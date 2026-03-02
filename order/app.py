import logging
import os
import atexit
import random
import uuid
from collections import defaultdict
import threading

import redis
import requests
import pika

from msgspec import msgpack, Struct
from flask import Flask, jsonify, abort, Response

import MqAbstraction


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


atexit.register(close_db_connection)


class OrderValue(Struct):
    paid: bool
    items: list[tuple[str, int]]
    user_id: str
    total_cost: int

# Setup for RabbitMQ consumer thread
declare_queues = ['order_queue', 'stock_queue', 'payment_queue']
pending_responses: dict[str, dict] = {} # TODO: THIS SHOULD BE IN REDIS so it doesnt get lost on crash

def queue_receive_callback(ch, method, properties, body):
    message = msgpack.decode(body, type=MqAbstraction.Message)

    if properties.correlation_id in pending_responses:
        pending_responses[properties.correlation_id]['response'] = message
        pending_responses[properties.correlation_id]['event'].set()

    match message:
        case MqAbstraction.HasMoreThanXCreditReply(message_id=message_id, has_more_credit=has_more_credit):
            app.logger.debug(f"Received HasMoreThanXCreditReply with message_id: {message_id}, corr_id: {properties.correlation_id}, has_more_credit: {has_more_credit}")
        case MqAbstraction.ModifyStockReply(message_id=message_id, success=success):
            app.logger.debug(f"Received ModifyStockReply with message_id: {message_id}, corr_id: {properties.correlation_id}, success: {success}")
        case MqAbstraction.SubtractCreditReply(message_id=message_id, success=success):
            app.logger.debug(f"Received SubtractCreditReply with message_id: {message_id}, corr_id: {properties.correlation_id}, success: {success}")
reply_to_queue, consumer_rabbitmq_channel = MqAbstraction.spawn_rabbitmq_consumer_thread('order_queue', queue_receive_callback, declare_queues)

# Setup for RabbitMQ producer
producer_rabbitmq_channel = MqAbstraction.setup_rabbit_mq_producer(declare_queues)

def publish_request_to_queue(queue_name: str, message):
    body = msgpack.encode(message)
    corr_id = str(uuid.uuid4())
    pending = {'event': threading.Event(), 'response': None}
    pending_responses[corr_id] = pending
    producer_rabbitmq_channel.basic_publish(
        exchange='',
        routing_key=queue_name,
        body=body,
        properties=pika.BasicProperties(
            delivery_mode=pika.DeliveryMode.Persistent,
            content_type='application/msgpack',
            reply_to=reply_to_queue,
            correlation_id=corr_id
        )
    )
    pending['event'].wait()
    pending_responses.pop(corr_id)
    return pending.get('response') 


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


@app.post('/checkout/<order_id>')
def checkout(order_id: str):
    app.logger.debug(f"Checking out {order_id}")
    order_entry: OrderValue = get_order_from_db(order_id)
    # get the quantity per item
    delta_stock: dict[str, int] = defaultdict(int)
    for item_id, quantity in order_entry.items:
        delta_stock[item_id] -= quantity

    # Check if user has enough credit
    credit_req = MqAbstraction.HasMoreThanXCreditRequest(message_id=str(uuid.uuid4()), user_id=order_entry.user_id, credit_threshold=float(order_entry.total_cost))
    credit_reply = publish_request_to_queue('payment_queue', credit_req)
    if not credit_reply.has_more_credit:
        abort(400, "User does not have enough credit")

    # Subtract stock
    stock_req = MqAbstraction.ModifyStockRequest(message_id=str(uuid.uuid4()), to_modify=delta_stock)
    stock_reply = publish_request_to_queue('stock_queue', stock_req)
    if not stock_reply.success:
        abort(400, "Not enough stock for one or more items in the order")

    # Reduce user credit
    pay_req = MqAbstraction.SubtractCreditRequest(message_id=str(uuid.uuid4()), user_id=order_entry.user_id, amount=float(order_entry.total_cost))
    pay_reply = publish_request_to_queue('payment_queue', pay_req)
    if not pay_reply.success:
        abort(400, "Payment failed")
        compensate_stock: dict[str, int] = {item_id: -quantity for item_id, quantity in order_entry.items}
        stock_req = MqAbstraction.ModifyStockRequest(message_id=str(uuid.uuid4()), to_modify=compensate_stock)
        publish_request_to_queue('stock_queue', stock_req)

    order_entry.paid = True


    try:
        db.set(order_id, msgpack.encode(order_entry))
    except redis.exceptions.RedisError:
        return abort(400, DB_ERROR_STR)
    app.logger.debug("Checkout successful")
    return Response("Checkout successful", status=200)


if __name__ == '__main__':
    app.run(host="0.0.0.0", port=8000, debug=True)
else:
    gunicorn_logger = logging.getLogger('gunicorn.error')
    app.logger.handlers = gunicorn_logger.handlers
    app.logger.setLevel(gunicorn_logger.level)
