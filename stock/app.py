import logging
import os
import uuid

import nats
from redis.asyncio import Redis
from redis.exceptions import RedisError

from msgspec import msgpack, Struct
from quart import Quart, jsonify, abort, Response

from common.messages import CheckoutRequest, CheckoutResult


DB_ERROR_STR = "DB error"

app = Quart("stock-service")

db: Redis = Redis(host=os.environ['REDIS_HOST'],
                  port=int(os.environ['REDIS_PORT']),
                  password=os.environ['REDIS_PASSWORD'],
                  db=int(os.environ['REDIS_DB']))

nc: nats.NATS | None = None
js = None


class StockValue(Struct):
    stock: int
    price: int


async def get_item_from_db(item_id: str) -> StockValue:
    # get serialized data
    try:
        entry: bytes = await db.get(item_id)
    except RedisError:
        raise RedisError(DB_ERROR_STR)
    # deserialize data if it exists else return null
    entry: StockValue | None = msgpack.decode(entry, type=StockValue) if entry else None
    if entry is None:
        # if item does not exist in the database; raise
        raise ValueError(f"Item: {item_id} not found!")
    return entry


async def ensure_stream():
    try:
        await js.add_stream(name="CHECKOUT", subjects=["checkout.>"])
    except Exception:
        pass  # stream already exists


async def handle_checkout_stock(msg):
    req: CheckoutRequest = msgpack.decode(msg.data, type=CheckoutRequest)
    # track items already subtracted in case we need to roll back
    subtracted: list[tuple[str, int]] = []

    for item_id, quantity in req.items.items():
        try:
            item_entry = await get_item_from_db(item_id)
        except (ValueError, RedisError) as e:
            await rollback_stock(subtracted)
            await js.publish("checkout.result", msgpack.encode(
                CheckoutResult(order_id=req.order_id, success=False, error=str(e))
            ))
            return

        # update stock, serialize and update database
        item_entry.stock -= quantity
        app.logger.debug(f"Item: {item_id} stock updated to: {item_entry.stock}")
        if item_entry.stock < 0:
            await rollback_stock(subtracted)
            await js.publish("checkout.result", msgpack.encode(
                CheckoutResult(order_id=req.order_id, success=False, error=f"Item: {item_id} stock cannot get reduced below zero!")
            ))
            return

        try:
            await db.set(item_id, msgpack.encode(item_entry))
        except RedisError as e:
            await rollback_stock(subtracted)
            await js.publish("checkout.result", msgpack.encode(
                CheckoutResult(order_id=req.order_id, success=False, error=str(e))
            ))
            return

        subtracted.append((item_id, quantity))

    # all items subtracted successfully
    await js.publish("checkout.result", msgpack.encode(
        CheckoutResult(order_id=req.order_id, success=True, error="")
    ))


async def rollback_stock(subtracted: list[tuple[str, int]]):
    for item_id, quantity in subtracted:
        try:
            item_entry = await get_item_from_db(item_id)
            item_entry.stock += quantity
            await db.set(item_id, msgpack.encode(item_entry))
        except Exception as e:
            app.logger.error(f"Rollback failed for item {item_id}: {e}")


@app.before_serving
async def startup():
    global nc, js
    nc = await nats.connect(os.environ['NATS_URL'])
    js = nc.jetstream()
    await ensure_stream()
    await js.subscribe(
        "checkout.stock",
        durable="stock-checkout",
        queue="stock-checkout",
        cb=handle_checkout_stock,
    )


@app.after_serving
async def shutdown():
    await nc.drain()
    await db.aclose()


@app.post('/item/create/<price>')
async def create_item(price: int):
    key = str(uuid.uuid4())
    app.logger.debug(f"Item: {key} created")
    value = msgpack.encode(StockValue(stock=0, price=int(price)))
    try:
        await db.set(key, value)
    except RedisError:
        return abort(400, DB_ERROR_STR)
    return jsonify({'item_id': key})


@app.post('/batch_init/<n>/<starting_stock>/<item_price>')
async def batch_init_users(n: int, starting_stock: int, item_price: int):
    n = int(n)
    starting_stock = int(starting_stock)
    item_price = int(item_price)
    kv_pairs: dict[str, bytes] = {f"{i}": msgpack.encode(StockValue(stock=starting_stock, price=item_price))
                                  for i in range(n)}
    try:
        await db.mset(kv_pairs)
    except RedisError:
        return abort(400, DB_ERROR_STR)
    return jsonify({"msg": "Batch init for stock successful"})


@app.get('/find/<item_id>')
async def find_item(item_id: str):
    try:
        item_entry: StockValue = await get_item_from_db(item_id)
    except (ValueError, RedisError) as e:
        return abort(400, str(e))
    return jsonify(
        {
            "stock": item_entry.stock,
            "price": item_entry.price
        }
    )


@app.post('/add/<item_id>/<amount>')
async def add_stock(item_id: str, amount: int):
    try:
        item_entry: StockValue = await get_item_from_db(item_id)
    except (ValueError, RedisError) as e:
        return abort(400, str(e))
    # update stock, serialize and update database
    item_entry.stock += int(amount)
    try:
        await db.set(item_id, msgpack.encode(item_entry))
    except RedisError:
        return abort(400, DB_ERROR_STR)
    return Response(f"Item: {item_id} stock updated to: {item_entry.stock}", status=200)


@app.post('/subtract/<item_id>/<amount>')
async def remove_stock(item_id: str, amount: int):
    try:
        item_entry: StockValue = await get_item_from_db(item_id)
    except (ValueError, RedisError) as e:
        return abort(400, str(e))
    # update stock, serialize and update database
    item_entry.stock -= int(amount)
    app.logger.debug(f"Item: {item_id} stock updated to: {item_entry.stock}")
    if item_entry.stock < 0:
        abort(400, f"Item: {item_id} stock cannot get reduced below zero!")
    try:
        await db.set(item_id, msgpack.encode(item_entry))
    except RedisError:
        return abort(400, DB_ERROR_STR)
    return Response(f"Item: {item_id} stock updated to: {item_entry.stock}", status=200)


if __name__ == '__main__':
    app.run(host="0.0.0.0", port=8000, debug=True)
else:
    logging.basicConfig(level=logging.INFO)
