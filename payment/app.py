import logging
import os
import asyncio
import uuid

from redis.asyncio import Redis
from redis.exceptions import RedisError

from msgspec import msgpack, Struct, ValidationError
from quart import Quart, jsonify, abort, Response

import aiokafka

from messages import *

DB_ERROR_STR = "DB error"
KAFKA_BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:19092")
PAYMENT_CREDIT_TOPIC = os.environ.get("PAYMENT_CREDIT_TOPIC", "payment-credit-check")
STOCK_CHECK_TOPIC = os.environ.get("STOCK_CHECK_TOPIC", "stock-check")
STOCK_CHECK_RESULT_TOPIC = os.environ.get("STOCK_CHECK_RESULT_TOPIC", "stock-check-result")
ORDER_CHECKOUT_RESULT_TOPIC = os.environ.get("ORDER_CHECKOUT_RESULT_TOPIC", "order-checkout-result")


app = Quart("payment-service")

db: Redis = Redis(host=os.environ['REDIS_HOST'],
                  port=int(os.environ['REDIS_PORT']),
                  password=os.environ['REDIS_PASSWORD'],
                  db=int(os.environ['REDIS_DB']))

producer: aiokafka.AIOKafkaProducer | None = None
payment_request_consumer: aiokafka.AIOKafkaConsumer | None = None
stock_result_consumer: aiokafka.AIOKafkaConsumer | None = None
payment_task: asyncio.Task | None = None
stock_result_task: asyncio.Task | None = None
pending_payments: dict[str, tuple[str, int]] = {}


@app.before_serving
async def startup():
    global producer, payment_request_consumer, stock_result_consumer, payment_task, stock_result_task
    producer = aiokafka.AIOKafkaProducer(bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS)
    await producer.start()

    payment_request_consumer = aiokafka.AIOKafkaConsumer(
        PAYMENT_CREDIT_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        group_id="payment-credit-group",
        enable_auto_commit=True,
    )
    await payment_request_consumer.start()

    stock_result_consumer = aiokafka.AIOKafkaConsumer(
        STOCK_CHECK_RESULT_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        group_id="payment-stock-result-group",
        enable_auto_commit=True,
    )
    await stock_result_consumer.start()

    payment_task = asyncio.create_task(handle_credit_requests())
    stock_result_task = asyncio.create_task(handle_stock_results())


@app.after_serving
async def shutdown():
    if payment_task is not None:
        payment_task.cancel()
        try:
            await payment_task
        except asyncio.CancelledError:
            pass
    if stock_result_task is not None:
        stock_result_task.cancel()
        try:
            await stock_result_task
        except asyncio.CancelledError:
            pass
    if payment_request_consumer is not None:
        await payment_request_consumer.stop()
    if stock_result_consumer is not None:
        await stock_result_consumer.stop()
    if producer is not None:
        await producer.stop()
    await db.aclose()


def decode_checkout_result(payload: bytes) -> EverythingGood | EverythingBad:
    try:
        return msgpack.decode(payload, type=EverythingBad)
    except ValidationError:
        return msgpack.decode(payload, type=EverythingGood)


async def handle_credit_requests():
    async for message in payment_request_consumer:
        checkout_request = msgpack.decode(message.value, type=DoesUserHaveEnoughCreditForOrder)
        saga_id = checkout_request.saga_id
        user_id = checkout_request.user_id
        total_cost = checkout_request.order.total_cost
        try:
            user_entry = await get_user_from_db(user_id)
            if user_entry.credit < total_cost:
                bad = EverythingBad(saga_id=saga_id, reason="User out of credit")
                await producer.send_and_wait(ORDER_CHECKOUT_RESULT_TOPIC, msgpack.encode(bad))
                continue

            user_entry.credit -= total_cost
            await db.set(user_id, msgpack.encode(user_entry))
            pending_payments[saga_id] = (user_id, total_cost)

            stock_request = EnoughStock(saga_id=saga_id, order=checkout_request.order)
            await producer.send_and_wait(STOCK_CHECK_TOPIC, msgpack.encode(stock_request))
        except Exception as ex:
            bad = EverythingBad(saga_id=saga_id, reason=str(ex))
            await producer.send_and_wait(ORDER_CHECKOUT_RESULT_TOPIC, msgpack.encode(bad))


async def handle_stock_results():
    async for message in stock_result_consumer:
        result = decode_checkout_result(message.value)
        saga_id = result.saga_id

        if isinstance(result, EverythingGood):
            pending_payments.pop(saga_id, None)
            continue

        pending = pending_payments.pop(saga_id, None)
        if pending is not None:
            user_id, amount = pending
            try:
                user_entry = await get_user_from_db(user_id)
                user_entry.credit += amount
                await db.set(user_id, msgpack.encode(user_entry))
            except Exception:
                app.logger.exception("Failed to compensate payment for saga %s", saga_id)

        await producer.send_and_wait(ORDER_CHECKOUT_RESULT_TOPIC, msgpack.encode(result))


async def get_user_from_db(user_id: str) -> UserValue | None:
    try:
        # get serialized data
        entry: bytes = await db.get(user_id)
    except RedisError:
        return abort(400, DB_ERROR_STR)
    # deserialize data if it exists else return null
    entry: UserValue | None = msgpack.decode(entry, type=UserValue) if entry else None
    if entry is None:
        # if user does not exist in the database; abort
        abort(400, f"User: {user_id} not found!")
    return entry


@app.post('/create_user')
async def create_user():
    key = str(uuid.uuid4())
    value = msgpack.encode(UserValue(credit=0))
    try:
        await db.set(key, value)
    except RedisError:
        return abort(400, DB_ERROR_STR)
    return jsonify({'user_id': key})


@app.post('/batch_init/<n>/<starting_money>')
async def batch_init_users(n: int, starting_money: int):
    n = int(n)
    starting_money = int(starting_money)
    kv_pairs: dict[str, bytes] = {f"{i}": msgpack.encode(UserValue(credit=starting_money))
                                  for i in range(n)}
    try:
        await db.mset(kv_pairs)
    except RedisError:
        return abort(400, DB_ERROR_STR)
    return jsonify({"msg": "Batch init for users successful"})


@app.get('/find_user/<user_id>')
async def find_user(user_id: str):
    user_entry: UserValue = await get_user_from_db(user_id)
    return jsonify(
        {
            "user_id": user_id,
            "credit": user_entry.credit
        }
    )


@app.post('/add_funds/<user_id>/<amount>')
async def add_credit(user_id: str, amount: int):
    user_entry: UserValue = await get_user_from_db(user_id)
    # update credit, serialize and update database
    user_entry.credit += int(amount)
    try:
        await db.set(user_id, msgpack.encode(user_entry))
    except RedisError:
        return abort(400, DB_ERROR_STR)
    return Response(f"User: {user_id} credit updated to: {user_entry.credit}", status=200)


@app.post('/pay/<user_id>/<amount>')
async def remove_credit(user_id: str, amount: int):
    app.logger.debug(f"Removing {amount} credit from user: {user_id}")
    user_entry: UserValue = await get_user_from_db(user_id)
    # update credit, serialize and update database
    user_entry.credit -= int(amount)
    if user_entry.credit < 0:
        abort(400, f"User: {user_id} credit cannot get reduced below zero!")
    try:
        await db.set(user_id, msgpack.encode(user_entry))
    except RedisError:
        return abort(400, DB_ERROR_STR)
    return Response(f"User: {user_id} credit updated to: {user_entry.credit}", status=200)


if __name__ == '__main__':
    app.run(host="0.0.0.0", port=8000, debug=True)
else:
    logging.basicConfig(level=logging.INFO)
