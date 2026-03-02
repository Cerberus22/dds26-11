import logging
import os
import atexit
import uuid

import redis
import pika
import threading

from msgspec import msgpack, Struct
from flask import Flask, jsonify, abort, Response

import MqAbstraction


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

# Setup for RabbitMQ consumer thread
declare_queues = ['stock_queue', 'order_queue', 'payment_queue']
consumer_rabbitmq_channel = None
def queue_receive_callback(ch, method, properties, body):
    message = msgpack.decode(body, type=MqAbstraction.Message)

    match message:
        case MqAbstraction.ModifyStockRequest(message_id=message_id, to_modify=to_modify):
            reply = None
            try:
                # Read all affected items first
                items: dict[str, StockValue] = {}
                for item_id, delta in to_modify.items():
                    items[item_id] = get_item_from_db(item_id)

                # Apply changes in-memory and validate
                for item_id, delta in to_modify.items():
                    items[item_id].stock += int(delta)
                    if items[item_id].stock < 0:
                        raise ValueError(f"Item {item_id} would go negative")

                # Persist all changes
                for item_id, entry in items.items():
                    db.set(item_id, msgpack.encode(entry))

                reply = MqAbstraction.ModifyStockReply(message_id=message_id, success=True)
            except Exception as e:
                app.logger.exception("Failed to modify stock: %s", e)
                # On any error return success=False
                reply = MqAbstraction.ModifyStockReply(message_id=message_id, success=False)

            # Publish the reply back to the requester
            consumer_rabbitmq_channel.basic_publish(
                exchange='',
                routing_key=properties.reply_to,
                body=msgpack.encode(reply),
                properties=pika.BasicProperties(
                    delivery_mode=pika.DeliveryMode.Persistent,
                    content_type='application/msgpack',
                    correlation_id=properties.correlation_id
                )
            )
        case _:
            app.logger.error(f"Received unknown message type: {message}")

reply_to_queue, consumer_rabbitmq_channel = MqAbstraction.spawn_rabbitmq_consumer_thread('stock_queue', queue_receive_callback, declare_queues)


# Setup for REST thread
producer_rabbitmq_channel = MqAbstraction.setup_rabbit_mq_producer(declare_queues)

def publish_to_queue(queue_name: str, message):
    producer_rabbitmq_channel.basic_publish(
        exchange='',
        routing_key=queue_name,
        body=message,
        properties=pika.BasicProperties(
            delivery_mode=pika.DeliveryMode.Persistent,
            content_type='application/msgpack'
        )
    )


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


if __name__ == '__main__':
    app.run(host="0.0.0.0", port=8000, debug=True)
else:
    gunicorn_logger = logging.getLogger('gunicorn.error')
    app.logger.handlers = gunicorn_logger.handlers
    app.logger.setLevel(gunicorn_logger.level)
