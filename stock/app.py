import logging
import os
import asyncio
import uuid
from collections import defaultdict

from redis.asyncio import Redis
from redis.exceptions import RedisError

from msgspec import msgpack, Struct
from quart import Quart, jsonify, abort, Response

import aiokafka

from messages import *

DB_ERROR_STR = "DB error"
KAFKA_BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:19092")
STOCK_CHECK_TOPIC = os.environ.get("STOCK_CHECK_TOPIC", "stock-check")
STOCK_CHECK_RESULT_TOPIC = os.environ.get("STOCK_CHECK_RESULT_TOPIC", "stock-check-result")
ORDER_CHECKOUT_RESULT_TOPIC = os.environ.get("ORDER_CHECKOUT_RESULT_TOPIC", "order-checkout-result")

app = Quart("stock-service")

db: Redis = Redis(host=os.environ['REDIS_HOST'],
                  port=int(os.environ['REDIS_PORT']),
                  password=os.environ['REDIS_PASSWORD'],
                  db=int(os.environ['REDIS_DB']))

producer: aiokafka.AIOKafkaProducer | None = None
stock_request_consumer: aiokafka.AIOKafkaConsumer | None = None
stock_task: asyncio.Task | None = None


@app.before_serving
async def startup():
    global producer, stock_request_consumer, stock_task
    producer = aiokafka.AIOKafkaProducer(bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS)
    await producer.start()
    stock_request_consumer = aiokafka.AIOKafkaConsumer(
        STOCK_CHECK_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        group_id="stock-check-group",
        enable_auto_commit=True,
    )
    await stock_request_consumer.start()
    stock_task = asyncio.create_task(handle_stock_requests())


@app.after_serving
async def shutdown():
    if stock_task is not None:
        stock_task.cancel()
        try:
            await stock_task
        except asyncio.CancelledError:
            pass
    if stock_request_consumer is not None:
        await stock_request_consumer.stop()
    if producer is not None:
        await producer.stop()
    await db.aclose()


async def handle_stock_requests():
    async for message in stock_request_consumer:
        request = msgpack.decode(message.value, type=EnoughStock)
        saga_id = request.saga_id

        try:
            items_quantities: dict[str, int] = defaultdict(int)
            for item_id, quantity in request.order.items:
                items_quantities[item_id] += quantity

            current_entries: dict[str, StockValue] = {}
            for item_id, quantity in items_quantities.items():
                item_entry: StockValue = await get_item_from_db(item_id)
                if item_entry.stock < quantity:
                    bad = EverythingBad(saga_id=saga_id, reason=f"Out of stock on item_id: {item_id}")
                    await producer.send_and_wait(STOCK_CHECK_RESULT_TOPIC, msgpack.encode(bad))
                    await producer.send_and_wait(ORDER_CHECKOUT_RESULT_TOPIC, msgpack.encode(bad))
                    break
                current_entries[item_id] = item_entry
            else: # this case only runs if the loop above didnt break
                for item_id, quantity in items_quantities.items():
                    item_entry = current_entries[item_id]
                    item_entry.stock -= quantity
                    await db.set(item_id, msgpack.encode(item_entry))

                good = EverythingGood(saga_id=saga_id)
                await producer.send_and_wait(STOCK_CHECK_RESULT_TOPIC, msgpack.encode(good))
                await producer.send_and_wait(ORDER_CHECKOUT_RESULT_TOPIC, msgpack.encode(good))
        except Exception as ex:
            bad = EverythingBad(saga_id=saga_id, reason=str(ex))
            await producer.send_and_wait(STOCK_CHECK_RESULT_TOPIC, msgpack.encode(bad))
            await producer.send_and_wait(ORDER_CHECKOUT_RESULT_TOPIC, msgpack.encode(bad))


async def get_item_from_db(item_id: str) -> StockValue | None:
    # get serialized data
    try:
        entry: bytes = await db.get(item_id)
    except RedisError:
        return abort(400, DB_ERROR_STR)
    # deserialize data if it exists else return null
    entry: StockValue | None = msgpack.decode(entry, type=StockValue) if entry else None
    if entry is None:
        # if item does not exist in the database; abort
        abort(400, f"Item: {item_id} not found!")
    return entry


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
    item_entry: StockValue = await get_item_from_db(item_id)
    return jsonify(
        {
            "stock": item_entry.stock,
            "price": item_entry.price
        }
    )


@app.post('/add/<item_id>/<amount>')
async def add_stock(item_id: str, amount: int):
    item_entry: StockValue = await get_item_from_db(item_id)
    # update stock, serialize and update database
    item_entry.stock += int(amount)
    try:
        await db.set(item_id, msgpack.encode(item_entry))
    except RedisError:
        return abort(400, DB_ERROR_STR)
    return Response(f"Item: {item_id} stock updated to: {item_entry.stock}", status=200)


@app.post('/subtract/<item_id>/<amount>')
async def remove_stock(item_id: str, amount: int):
    item_entry: StockValue = await get_item_from_db(item_id)
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
