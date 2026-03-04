import logging
import os
import random
import asyncio
import uuid

import aiohttp
from msgspec import ValidationError
from redis.asyncio import Redis
from redis.exceptions import RedisError

from msgspec import msgpack, Struct
from quart import Quart, jsonify, abort, Response

import aiokafka
from messages import *

DB_ERROR_STR = "DB error"
REQ_ERROR_STR = "Requests error"

GATEWAY_URL = os.environ['GATEWAY_URL']
KAFKA_BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:19092")
PAYMENT_CREDIT_TOPIC = os.environ.get("PAYMENT_CREDIT_TOPIC", "payment-credit-check")
ORDER_CHECKOUT_RESULT_TOPIC = os.environ.get("ORDER_CHECKOUT_RESULT_TOPIC", "order-checkout-result")
ORDER_CHECKOUT_TIMEOUT_SEC = float(os.environ.get("ORDER_CHECKOUT_TIMEOUT_SEC", "60"))

app = Quart("order-service")

db: Redis = Redis(host=os.environ['REDIS_HOST'],
                  port=int(os.environ['REDIS_PORT']),
                  password=os.environ['REDIS_PASSWORD'],
                  db=int(os.environ['REDIS_DB']))

session: aiohttp.ClientSession | None = None
producer: aiokafka.AIOKafkaProducer | None = None
result_consumer: aiokafka.AIOKafkaConsumer | None = None
consumer_task: asyncio.Task | None = None
pending_sagas: dict[str, asyncio.Future] = {}


@app.before_serving
async def startup():
    global session, producer, result_consumer, consumer_task
    session = aiohttp.ClientSession()
    producer = aiokafka.AIOKafkaProducer(bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS)
    await producer.start()
    result_consumer = aiokafka.AIOKafkaConsumer(
        ORDER_CHECKOUT_RESULT_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        enable_auto_commit=True,
    )
    await result_consumer.start()
    consumer_task = asyncio.create_task(consume_checkout_results())


@app.after_serving
async def shutdown():
    if consumer_task is not None:
        consumer_task.cancel()
        try:
            await consumer_task
        except asyncio.CancelledError:
            pass
    if result_consumer is not None:
        await result_consumer.stop()
    if producer is not None:
        await producer.stop()
    if session is not None:
        await session.close()
    await db.aclose()


def decode_checkout_result(payload: bytes) -> EverythingGood | EverythingBad:
    try:
        return msgpack.decode(payload, type=EverythingBad)
    except ValidationError:
        return msgpack.decode(payload, type=EverythingGood)


async def consume_checkout_results():
    async for message in result_consumer:
        result = decode_checkout_result(message.value)
        future = pending_sagas.get(result.saga_id)
        if future is not None and not future.done():
            future.set_result(result)

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
        value = OrderValue(paid=None,
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


async def send_get_request(url: str):
    try:
        response = await session.get(url)
    except aiohttp.ClientError:
        abort(400, REQ_ERROR_STR)
    else:
        return response


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
    app.logger.debug(f"Checking out {order_id} through kafka saga")
    order_entry: OrderValue = await get_order_from_db(order_id)
    saga_id = str(uuid.uuid4())
    checkout_future = asyncio.get_running_loop().create_future()
    pending_sagas[saga_id] = checkout_future

    event = DoesUserHaveEnoughCreditForOrder(
        saga_id=saga_id,
        user_id=order_entry.user_id,
        order=order_entry,
    )
    await producer.send_and_wait(PAYMENT_CREDIT_TOPIC, msgpack.encode(event))

    try:
        result = await asyncio.wait_for(checkout_future, timeout=ORDER_CHECKOUT_TIMEOUT_SEC)
    except TimeoutError:
        abort(504, "Checkout timed out")
    finally:
        pending_sagas.pop(saga_id, None)

    if isinstance(result, EverythingBad):
        abort(400, result.reason)

    order_entry.paid = True
    try:
        await db.set(order_id, msgpack.encode(order_entry))
    except RedisError:
        return abort(400, DB_ERROR_STR)

    app.logger.debug("Checkout successful")
    return Response("Checkout successful", status=200)


if __name__ == '__main__':
    app.run(host="0.0.0.0", port=8000, debug=True)
else:
    logging.basicConfig(level=logging.INFO)
