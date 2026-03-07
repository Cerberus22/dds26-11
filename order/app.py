import logging
import os
import random
import uuid
from collections import defaultdict

import aiohttp
import nats
from redis.asyncio import Redis
from redis.exceptions import RedisError

from msgspec import msgpack, Struct
from quart import Quart, jsonify, abort, Response

from common.messages import CheckoutRequest, CheckoutResult


DB_ERROR_STR = "DB error"
REQ_ERROR_STR = "Requests error"

GATEWAY_URL = os.environ['GATEWAY_URL']
NATS_URL = os.environ['NATS_URL']

app = Quart("order-service")

db: Redis = Redis(host=os.environ['REDIS_HOST'],
                  port=int(os.environ['REDIS_PORT']),
                  password=os.environ['REDIS_PASSWORD'],
                  db=int(os.environ['REDIS_DB']))

nc: nats.NATS | None = None
js = None
session: aiohttp.ClientSession | None = None


class OrderValue(Struct):
    paid: bool
    items: list[tuple[str, int]]
    user_id: str
    total_cost: int


async def get_order_from_db(order_id: str) -> OrderValue | None:
    try:
        # get serialized data
        entry: bytes = await db.get(order_id)
    except RedisError:
        return abort(400, DB_ERROR_STR)
    # deserialize data if it exists else return null
    entry: OrderValue | None = msgpack.decode(entry, type=OrderValue) if entry else None
    if entry is None:
        # if order does not exist in the database; abort
        abort(400, f"Order: {order_id} not found!")
    return entry


async def ensure_stream():
    try:
        await js.add_stream(name="CHECKOUT", subjects=["checkout.>"])
    except Exception:
        pass  # stream already exists
    
    try:
        await js.add_stream(name="ORDER", subjects=["order.>"])
    except Exception:
        pass  # stream already exists
    
    try:
        await js.add_stream(name="PAYMENT", subjects=["payment.>"])
    except Exception:
        pass  # stream already exists
    
    try:
        await js.add_stream(name="STOCK", subjects=["stock.>"])
    except Exception:
        pass  # stream already exists


async def handle_payment_result(msg):
    result: CheckoutResult = msgpack.decode(msg.data, type=CheckoutResult)
    
    if not result.success:
        app.logger.warning(f"Payment failed for order {result.order_id}: {result.error}")
        await js.publish("order.result", msgpack.encode(
            CheckoutResult(order_id=result.order_id, success=False, error=result.error)
        ))
        return
    
    app.logger.info(f"Payment succeeded for order {result.order_id}, waiting for stock result")
    # payment succeeded, now waiting for stock service result


async def handle_stock_result(msg):
    result: CheckoutResult = msgpack.decode(msg.data, type=CheckoutResult)
    
    if not result.success:
        app.logger.warning(f"Stock checkout failed for order {result.order_id}: {result.error}")
        await js.publish("order.result", msgpack.encode(
            CheckoutResult(order_id=result.order_id, success=False, error=result.error)
        ))
        return
    
    # stock succeeded, mark order as paid
    try:
        entry: bytes = await db.get(result.order_id)
        if not entry:
            app.logger.error(f"Order {result.order_id} not found in DB on checkout result")
            await js.publish("order.result", msgpack.encode(
                CheckoutResult(order_id=result.order_id, success=False, error=f"Order {result.order_id} not found")
            ))
            return
        order_entry: OrderValue = msgpack.decode(entry, type=OrderValue)
        order_entry.paid = True
        await db.set(result.order_id, msgpack.encode(order_entry))
        app.logger.info(f"Order {result.order_id} marked as paid")
        await js.publish("order.result", msgpack.encode(
            CheckoutResult(order_id=result.order_id, success=True, error="")
        ))
    except RedisError as e:
        app.logger.error(f"DB error marking order {result.order_id} as paid: {e}")
        await js.publish("order.result", msgpack.encode(
            CheckoutResult(order_id=result.order_id, success=False, error=str(e))
        ))


@app.before_serving
async def startup():
    global nc, js, session
    session = aiohttp.ClientSession()
    nc = await nats.connect(NATS_URL)
    js = nc.jetstream()
    await ensure_stream()
    await js.subscribe(
        "payment.result",
        durable="order-payment-result",
        queue="order-payment-result",
        cb=handle_payment_result,
    )
    await js.subscribe(
        "stock.result",
        durable="order-stock-result",
        queue="order-stock-result",
        cb=handle_stock_result,
    )


@app.after_serving
async def shutdown():
    await session.close()
    await nc.drain()
    await db.aclose()


@app.post('/create/<user_id>')
async def create_order(user_id: str):
    key = str(uuid.uuid4())
    value = msgpack.encode(OrderValue(paid=False, items=[], user_id=user_id, total_cost=0))
    try:
        await db.set(key, value)
    except RedisError:
        return abort(400, DB_ERROR_STR)
    return jsonify({'order_id': key})


@app.post('/batch_init/<n>/<n_items>/<n_users>/<item_price>')
async def batch_init_users(n: int, n_items: int, n_users: int, item_price: int):

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
        await db.mset(kv_pairs)
    except RedisError:
        return abort(400, DB_ERROR_STR)
    return jsonify({"msg": "Batch init for orders successful"})


@app.get('/find/<order_id>')
async def find_order(order_id: str):
    order_entry: OrderValue = await get_order_from_db(order_id)
    return jsonify(
        {
            "order_id": order_id,
            "paid": order_entry.paid,
            "items": order_entry.items,
            "user_id": order_entry.user_id,
            "total_cost": order_entry.total_cost
        }
    )


async def send_post_request(url: str):
    try:
        response = await session.post(url)
    except aiohttp.ClientError:
        abort(400, REQ_ERROR_STR)
    else:
        return response


async def send_get_request(url: str):
    try:
        response = await session.get(url)
    except aiohttp.ClientError:
        abort(400, REQ_ERROR_STR)
    else:
        return response


async def rollback_stock(removed_items: list[tuple[str, int]]):
    for item_id, quantity in removed_items:
        await send_post_request(f"{GATEWAY_URL}/stock/add/{item_id}/{quantity}")


@app.post('/addItem/<order_id>/<item_id>/<quantity>')
async def add_item(order_id: str, item_id: str, quantity: int):
    order_entry: OrderValue = await get_order_from_db(order_id)
    item_reply = await send_get_request(f"{GATEWAY_URL}/stock/find/{item_id}")
    if item_reply.status != 200:
        # Request failed because item does not exist
        abort(400, f"Item: {item_id} does not exist!")
    item_json: dict = await item_reply.json()
    order_entry.items.append((item_id, int(quantity)))
    order_entry.total_cost += int(quantity) * item_json["price"]
    try:
        await db.set(order_id, msgpack.encode(order_entry))
    except RedisError:
        return abort(400, DB_ERROR_STR)
    return Response(f"Item: {item_id} added to: {order_id} price updated to: {order_entry.total_cost}",
                    status=200)


@app.post('/checkout/<order_id>')
async def checkout(order_id: str):
    app.logger.debug(f"Checking out {order_id}")
    order_entry: OrderValue = await get_order_from_db(order_id)
    # get the quantity per item
    items_quantities: dict[str, int] = defaultdict(int)
    for item_id, quantity in order_entry.items:
        items_quantities[item_id] += quantity

    msg = CheckoutRequest(
        order_id=order_id,
        user_id=order_entry.user_id,
        total_cost=order_entry.total_cost,
        items=dict(items_quantities),
    )
    
    await js.publish("checkout.payment", msgpack.encode(msg))
    return Response("Checkout initiated", status=202)


if __name__ == '__main__':
    app.run(host="0.0.0.0", port=8000, debug=True)
else:
    logging.basicConfig(level=logging.DEBUG)
