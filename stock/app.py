import logging
import os
import atexit
import uuid

import redis

from msgspec import msgpack, Struct
from flask import Flask, jsonify, abort, Response


DB_ERROR_STR = "DB error"

app = Flask("stock-service")

db: redis.Redis = redis.Redis(host=os.environ['REDIS_HOST'],
                              port=int(os.environ['REDIS_PORT']),
                              password=os.environ['REDIS_PASSWORD'],
                              db=int(os.environ['REDIS_DB']))


def close_db_connection():
    db.close()


atexit.register(close_db_connection)


class StockValue(Struct):
    stock: int
    price: int


def get_item_from_db(item_id: str) -> StockValue | None:
    # get serialized data
    try:
        entry: bytes = db.get(item_id)
    except redis.exceptions.RedisError:
        return abort(400, DB_ERROR_STR)
    # deserialize data if it exists else return null
    entry: StockValue | None = msgpack.decode(entry, type=StockValue) if entry else None
    if entry is None:
        # if item does not exist in the database; abort
        abort(400, f"Item: {item_id} not found!")
    return entry


@app.post('/item/create/<price>')
def create_item(price: int):
    key = str(uuid.uuid4())
    app.logger.debug(f"Item: {key} created")
    value = msgpack.encode(StockValue(stock=0, price=int(price)))
    try:
        db.set(key, value)
    except redis.exceptions.RedisError:
        return abort(400, DB_ERROR_STR)
    return jsonify({'item_id': key})


@app.post('/batch_init/<n>/<starting_stock>/<item_price>')
def batch_init_users(n: int, starting_stock: int, item_price: int):
    n = int(n)
    starting_stock = int(starting_stock)
    item_price = int(item_price)
    kv_pairs: dict[str, bytes] = {f"{i}": msgpack.encode(StockValue(stock=starting_stock, price=item_price))
                                  for i in range(n)}
    try:
        db.mset(kv_pairs)
    except redis.exceptions.RedisError:
        return abort(400, DB_ERROR_STR)
    return jsonify({"msg": "Batch init for stock successful"})


@app.get('/find/<item_id>')
def find_item(item_id: str):
    item_entry: StockValue = get_item_from_db(item_id)
    return jsonify(
        {
            "stock": item_entry.stock,
            "price": item_entry.price
        }
    )


@app.post('/add/<item_id>/<amount>')
def add_stock(item_id: str, amount: int):
    item_entry: StockValue = get_item_from_db(item_id)
    # update stock, serialize and update database
    item_entry.stock += int(amount)
    try:
        db.set(item_id, msgpack.encode(item_entry))
    except redis.exceptions.RedisError:
        return abort(400, DB_ERROR_STR)
    return Response(f"Item: {item_id} stock updated to: {item_entry.stock}", status=200)


@app.post('/subtract/<item_id>/<amount>')
def remove_stock(item_id: str, amount: int):
    item_entry: StockValue = get_item_from_db(item_id)
    # update stock, serialize and update database
    item_entry.stock -= int(amount)
    app.logger.debug(f"Item: {item_id} stock updated to: {item_entry.stock}")
    if item_entry.stock < 0:
        abort(400, f"Item: {item_id} stock cannot get reduced below zero!")
    try:
        db.set(item_id, msgpack.encode(item_entry))
    except redis.exceptions.RedisError:
        return abort(400, DB_ERROR_STR)
    return Response(f"Item: {item_id} stock updated to: {item_entry.stock}", status=200)

# Simple 2PC

@app.post('/prepare/<item_id>/<quantity>/<txn_id>')
def prepare_stock(item_id: str, quantity: int, txn_id: str):
    """
    Prepares an item to be deducted from the stock by recording this transaction 
    in the database. The key we use is transaction+item, and we record how many
    of that item will be removed.
    """
    item_entry: StockValue = get_item_from_db(item_id)
    if item_entry.stock < int(quantity):
        return abort(400, f"Insufficient stock for item: {item_id}")
    db.set(f"pending:{txn_id}:{item_id}", msgpack.encode({
        "item_id": item_id,
        "quantity": int(quantity)
    }))
    return Response("prepared", status=200)

@app.post('/commit/<item_id>/<txn_id>')
def commit_stock(item_id: str, txn_id: str):
    """
    Commits the transaction that was prepared. Actually applies what was
    stated in the database for a transaction+item.
    """
    pending_key = f"pending:{txn_id}:{item_id}"
    raw = db.get(pending_key)
    if not raw:
        return Response("already committed", status=200)
    pending = msgpack.decode(raw, type=dict)
    item_entry: StockValue = get_item_from_db(item_id)
    item_entry.stock -= pending["quantity"]
    db.set(item_id, msgpack.encode(item_entry))
    db.delete(pending_key)
    return Response("committed", status=200)

@app.post('/abort/<item_id>/<txn_id>')
def abort_stock(item_id: str, txn_id: str):
    """
    Abortion process, simply remove the corresponding row from the database.
    """
    db.delete(f"pending:{txn_id}:{item_id}")
    return Response("aborted", status=200)

# Stock Service:
# Publishes: StockReserved, StockFailed
# Subscribes to: ReserveStock, CompensateStock

def handle_reserve_stock_event(event_data: dict):
    txn_id = event_data["txn_id"]
    items = event_data["items"]
    
    try:
        for item_data in items:
            item_id = item_data["item_id"]
            quantity = item_data["quantity"]
            
            item_entry: StockValue = get_item_from_db(item_id)
            if item_entry.stock < int(quantity):
                # Publish: StockFailed
                # event_bus.publish("StockFailed", {"txn_id": txn_id, "reason": f"Insufficient stock for item {item_id}"})
                app.logger.info(f"Stock reservation failed for txn {txn_id}: Insufficient stock for item {item_id}")
                return
        
        for item_data in items:
            item_id = item_data["item_id"]
            quantity = item_data["quantity"]
            
            db.set(f"reserved:{txn_id}:{item_id}", msgpack.encode({
                "item_id": item_id,
                "quantity": int(quantity)
            }))
            
            item_entry: StockValue = get_item_from_db(item_id)
            item_entry.stock -= int(quantity)
            db.set(item_id, msgpack.encode(item_entry))
        
        # Publish: StockReserved
        # event_bus.publish("StockReserved", {"txn_id": txn_id})
        app.logger.info(f"Stock reserved and committed for txn {txn_id}")
    except Exception as e:
        # Publish: StockFailed
        # event_bus.publish("StockFailed", {"txn_id": txn_id, "reason": str(e)})
        app.logger.error(f"Stock reservation failed for txn {txn_id}: {str(e)}")

def handle_compensate_stock_event(event_data: dict):
    txn_id = event_data["txn_id"]
    items = event_data.get("items", [])
    
    try:
        for item_data in items:
            item_id = item_data["item_id"]
            reserved_key = f"reserved:{txn_id}:{item_id}"
            raw = db.get(reserved_key)
            if not raw:
                continue
            
            reserved = msgpack.decode(raw, type=dict)
            item_entry: StockValue = get_item_from_db(item_id)
            item_entry.stock += reserved["quantity"] 
            db.set(item_id, msgpack.encode(item_entry))
            db.delete(reserved_key)
        
        app.logger.info(f"Stock compensated for txn {txn_id}")
    except Exception as e:
        app.logger.error(f"Stock compensation failed for txn {txn_id}: {str(e)}")

# event_bus.subscribe("ReserveStock", handle_reserve_stock_event)
# event_bus.subscribe("CompensateStock", handle_compensate_stock_event)

if __name__ == '__main__':
    app.run(host="0.0.0.0", port=8000, debug=True)
else:
    gunicorn_logger = logging.getLogger('gunicorn.error')
    app.logger.handlers = gunicorn_logger.handlers
    app.logger.setLevel(gunicorn_logger.level)
