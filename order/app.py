import logging
import os
import atexit
import random
import uuid
from collections import defaultdict

import redis
import requests

from msgspec import msgpack, Struct
from flask import Flask, jsonify, abort, Response


DB_ERROR_STR = "DB error"
REQ_ERROR_STR = "Requests error"

GATEWAY_URL = os.environ['GATEWAY_URL']

app = Flask("order-service")

db: redis.Redis = redis.Redis(host=os.environ['REDIS_HOST'],
                              port=int(os.environ['REDIS_PORT']),
                              password=os.environ['REDIS_PASSWORD'],
                              db=int(os.environ['REDIS_DB']))


def close_db_connection():
    db.close()

def recover_pending_transactions():
    for key in db.scan_iter("txn:*"):
        txn = msgpack.decode(db.get(key), type=dict)
        status = txn["status"]
        txn_id = key.decode().split(":")[1]
        items = dict(txn.get("items", []))
        if status == "COMMITTING":
            commit_stock(txn_id, items)
            commit_payment(txn_id)
            db.set(key, msgpack.encode({"status": "DONE"}))
        elif status in ("PREPARING", "ABORTING"):
            abort_stock(txn_id, items)
            abort_payment(txn_id)
            db.set(key, msgpack.encode({"status": "ABORTED"}))

atexit.register(close_db_connection)

# upon (re)starting, check for transactions that were incomplete and finalise them.
recover_pending_transactions()

class OrderValue(Struct):
    paid: bool
    items: list[tuple[str, int]]
    user_id: str
    total_cost: int


def get_order_from_db(order_id: str) -> OrderValue | None:
    try:
        # get serialized data
        entry: bytes = db.get(order_id)
    except redis.exceptions.RedisError:
        return abort(400, DB_ERROR_STR)
    # deserialize data if it exists else return null
    entry: OrderValue | None = msgpack.decode(entry, type=OrderValue) if entry else None
    if entry is None:
        # if order does not exist in the database; abort
        abort(400, f"Order: {order_id} not found!")
    return entry


@app.post('/create/<user_id>')
def create_order(user_id: str):
    key = str(uuid.uuid4())
    value = msgpack.encode(OrderValue(paid=False, items=[], user_id=user_id, total_cost=0))
    try:
        db.set(key, value)
    except redis.exceptions.RedisError:
        return abort(400, DB_ERROR_STR)
    return jsonify({'order_id': key})


@app.post('/batch_init/<n>/<n_items>/<n_users>/<item_price>')
def batch_init_users(n: int, n_items: int, n_users: int, item_price: int):

    n = int(n)
    n_items = int(n_items)
    n_users = int(n_users)
    item_price = int(item_price)

    def generate_entry() -> OrderValue:
        user_id = random.randint(0, n_users - 1)
        item1_id = random.randint(0, n_items - 1)
        item2_id = random.randint(0, n_items - 1)
        value = OrderValue(paid=False,
                           items=[(f"{item1_id}", 1), (f"{item2_id}", 1)],
                           user_id=f"{user_id}",
                           total_cost=2*item_price)
        return value

    kv_pairs: dict[str, bytes] = {f"{i}": msgpack.encode(generate_entry())
                                  for i in range(n)}
    try:
        db.mset(kv_pairs)
    except redis.exceptions.RedisError:
        return abort(400, DB_ERROR_STR)
    return jsonify({"msg": "Batch init for orders successful"})


@app.get('/find/<order_id>')
def find_order(order_id: str):
    order_entry: OrderValue = get_order_from_db(order_id)
    return jsonify(
        {
            "order_id": order_id,
            "paid": order_entry.paid,
            "items": order_entry.items,
            "user_id": order_entry.user_id,
            "total_cost": order_entry.total_cost
        }
    )


def send_post_request(url: str):
    try:
        response = requests.post(url)
    except requests.exceptions.RequestException:
        abort(400, REQ_ERROR_STR)
    else:
        return response


def send_get_request(url: str):
    try:
        response = requests.get(url)
    except requests.exceptions.RequestException:
        abort(400, REQ_ERROR_STR)
    else:
        return response


@app.post('/addItem/<order_id>/<item_id>/<quantity>')
def add_item(order_id: str, item_id: str, quantity: int):
    order_entry: OrderValue = get_order_from_db(order_id)
    item_reply = send_get_request(f"{GATEWAY_URL}/stock/find/{item_id}")
    if item_reply.status_code != 200:
        # Request failed because item does not exist
        abort(400, f"Item: {item_id} does not exist!")
    item_json: dict = item_reply.json()
    order_entry.items.append((item_id, int(quantity)))
    order_entry.total_cost += int(quantity) * item_json["price"]
    try:
        db.set(order_id, msgpack.encode(order_entry))
    except redis.exceptions.RedisError:
        return abort(400, DB_ERROR_STR)
    return Response(f"Item: {item_id} added to: {order_id} price updated to: {order_entry.total_cost}",
                    status=200)

# Simple 2PC
def prepare_stock(txn_id: str, items: dict[str, int]) -> bool:
    for item_id, quantity in items.items():
        response = send_post_request(f"{GATEWAY_URL}/stock/prepare/{item_id}/{quantity}/{txn_id}")
        if response.status_code != 200:
            return False
    return True

def prepare_payment(txn_id: str, user_id: str, amount: int) -> bool:
    response = send_post_request(f"{GATEWAY_URL}/payment/prepare/{user_id}/{amount}/{txn_id}")
    return response.status_code == 200

def commit_stock(txn_id: str, items: dict[str, int]):
    for item_id in items:
        send_post_request(f"{GATEWAY_URL}/stock/commit/{item_id}/{txn_id}")

def commit_payment(txn_id: str):
    send_post_request(f"{GATEWAY_URL}/payment/commit/{txn_id}")

def abort_stock(txn_id: str, items: dict[str, int]):
    for item_id in items:
        send_post_request(f"{GATEWAY_URL}/stock/abort/{item_id}/{txn_id}")

def abort_payment(txn_id: str):
    send_post_request(f"{GATEWAY_URL}/payment/abort/{txn_id}")

@app.post('/checkout/2pc/<order_id>')
def checkout_2pc(order_id: str):
    order_entry: OrderValue = get_order_from_db(order_id)

    # Check if order has already been processed
    if order_entry.paid:
        return abort(400, "Order already paid")
    
    items_quantities: dict[str, int] = defaultdict(int)
    for item_id, quantity in order_entry.items:
        items_quantities[item_id] += quantity

    txn_id = str(uuid.uuid4())

    # write intent before doing anything
    db.set(f"txn:{txn_id}", msgpack.encode({
        "status": "PREPARING",
        "order_id": order_id,
        "user_id": order_entry.user_id,
        "total_cost": order_entry.total_cost,
        "items": list(items_quantities.items())
    }))

    # phase 1
    stock_ok = prepare_stock(txn_id, items_quantities)
    payment_ok = prepare_payment(txn_id, order_entry.user_id, order_entry.total_cost)

    if stock_ok and payment_ok:
        # write COMMITTING before sending commits
        db.set(f"txn:{txn_id}", msgpack.encode({
            "status": "COMMITTING",
            "order_id": order_id,
            "user_id": order_entry.user_id,
            "total_cost": order_entry.total_cost,
            "items": list(items_quantities.items())
        }))
        commit_stock(txn_id, items_quantities)
        commit_payment(txn_id)
        order_entry.paid = True
        db.set(order_id, msgpack.encode(order_entry))
        db.set(f"txn:{txn_id}", msgpack.encode({"status": "DONE"}))
        return Response("Checkout successful", status=200)
    else:
        db.set(f"txn:{txn_id}", msgpack.encode({"status": "ABORTING"}))
        abort_stock(txn_id, items_quantities)
        abort_payment(txn_id)
        db.set(f"txn:{txn_id}", msgpack.encode({"status": "ABORTED"}))
        return abort(400, "Checkout failed")

# SAGA Order Service:
# Publishes: ReservePayment, CompensatePayment, ReserveStock, CompensateStock
# Subscribes to: PaymentReserved, PaymentFailed, StockReserved, StockFailed
saga_states = {}  # In-memory state tracking, could be persisted in Redis

def handle_payment_reserved_event(event_data: dict):
    txn_id = event_data["txn_id"]
    
    try:
        # Get saga state from Redis
        saga_state_raw = db.get(f"saga:{txn_id}")
        if not saga_state_raw:
            app.logger.error(f"Saga state not found for txn {txn_id}")
            return
        
        saga_state = msgpack.decode(saga_state_raw, type=dict)
        items = saga_state["items"]
        
        saga_state["status"] = "RESERVING_STOCK"
        db.set(f"saga:{txn_id}", msgpack.encode(saga_state))
        
        # Publish: ReserveStock
        items_list = [{"item_id": item_id, "quantity": quantity} for item_id, quantity in items]
        # event_bus.publish("ReserveStock", {"txn_id": txn_id, "items": items_list})
        app.logger.info(f"Published ReserveStock for txn {txn_id}")
        
    except Exception as e:
        app.logger.error(f"Error handling PaymentReserved for txn {txn_id}: {str(e)}")

def handle_payment_failed_event(event_data: dict):
    txn_id = event_data["txn_id"]
    reason = event_data.get("reason", "Unknown")
    
    try:
        saga_state = {"status": "FAILED_AT_PAYMENT", "reason": reason}
        db.set(f"saga:{txn_id}", msgpack.encode(saga_state))
        app.logger.info(f"Saga {txn_id} failed at payment: {reason}")
    except Exception as e:
        app.logger.error(f"Error handling PaymentFailed for txn {txn_id}: {str(e)}")

def handle_stock_reserved_event(event_data: dict):
    txn_id = event_data["txn_id"]
    
    try:
        # Get saga state from Redis
        saga_state_raw = db.get(f"saga:{txn_id}")
        if not saga_state_raw:
            app.logger.error(f"Saga state not found for txn {txn_id}")
            return
        
        saga_state = msgpack.decode(saga_state_raw, type=dict)
        order_id = saga_state["order_id"]
        
        saga_state["status"] = "COMPLETED"
        db.set(f"saga:{txn_id}", msgpack.encode(saga_state))
        
        order_entry: OrderValue = get_order_from_db(order_id)
        order_entry.paid = True
        db.set(order_id, msgpack.encode(order_entry))
        
        # Note: reserved keys are in Payment/Stock services, not here
        # They can be cleaned up by those services or left for garbage collection
        
        app.logger.info(f"Saga {txn_id} completed successfully")
        
    except Exception as e:
        app.logger.error(f"Error handling StockReserved for txn {txn_id}: {str(e)}")

def handle_stock_failed_event(event_data: dict):
    txn_id = event_data["txn_id"]
    reason = event_data.get("reason", "Unknown")
    
    try:
        # Get saga state from Redis
        saga_state_raw = db.get(f"saga:{txn_id}")
        if not saga_state_raw:
            app.logger.error(f"Saga state not found for txn {txn_id}")
            return
        
        saga_state = msgpack.decode(saga_state_raw, type=dict)
        
        saga_state["status"] = "COMPENSATING"
        saga_state["reason"] = reason
        db.set(f"saga:{txn_id}", msgpack.encode(saga_state))
        
        # Publish: CompensatePayment
        # event_bus.publish("CompensatePayment", {"txn_id": txn_id})
        app.logger.info(f"Published CompensatePayment for txn {txn_id}")
        
        saga_state["status"] = "COMPENSATED"
        db.set(f"saga:{txn_id}", msgpack.encode(saga_state))
        
    except Exception as e:
        app.logger.error(f"Error handling StockFailed for txn {txn_id}: {str(e)}")

# event_bus.subscribe("PaymentReserved", handle_payment_reserved_event)
# event_bus.subscribe("PaymentFailed", handle_payment_failed_event)
# event_bus.subscribe("StockReserved", handle_stock_reserved_event)
# event_bus.subscribe("StockFailed", handle_stock_failed_event)

@app.post('/checkout/saga/<order_id>')
def checkout_saga(order_id: str):
    order_entry: OrderValue = get_order_from_db(order_id)

    # Check if order has already been processed
    if order_entry.paid:
        return abort(400, "Order already paid")
    
    items_quantities: dict[str, int] = defaultdict(int)
    for item_id, quantity in order_entry.items:
        items_quantities[item_id] += quantity

    txn_id = str(uuid.uuid4())

    # Record saga state
    db.set(f"saga:{txn_id}", msgpack.encode({
        "status": "STARTED",
        "order_id": order_id,
        "user_id": order_entry.user_id,
        "total_cost": order_entry.total_cost,
        "items": list(items_quantities.items())
    }))

    # Step 1: Publish ReservePayment event
    db.set(f"saga:{txn_id}", msgpack.encode({
        "status": "RESERVING_PAYMENT",
        "order_id": order_id,
        "user_id": order_entry.user_id,
        "total_cost": order_entry.total_cost,
        "items": list(items_quantities.items())
    }))
    
    # Publish: ReservePayment
    # event_bus.publish("ReservePayment", {
    #     "txn_id": txn_id,
    #     "user_id": order_entry.user_id,
    #     "amount": order_entry.total_cost
    # })
    app.logger.info(f"Published ReservePayment for txn {txn_id}")

    return Response("Checkout initiated via SAGA", status=200)

if __name__ == '__main__':
    app.run(host="0.0.0.0", port=8000, debug=True)
else:
    gunicorn_logger = logging.getLogger('gunicorn.error')
    app.logger.handlers = gunicorn_logger.handlers
    app.logger.setLevel(gunicorn_logger.level)
