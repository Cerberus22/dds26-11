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


app = Flask("payment-service")

db: redis.Redis = redis.Redis(host=os.environ['REDIS_HOST'],
                              port=int(os.environ['REDIS_PORT']),
                              password=os.environ['REDIS_PASSWORD'],
                              db=int(os.environ['REDIS_DB']))


def close_db_connection():
    db.close()


atexit.register(close_db_connection)

# Setup for RabbitMQ consumer thread
declare_queues = ['payment_queue', 'order_queue']
consumer_rabbitmq_channel = None
def queue_receive_callback(ch, method, properties, body):
    message = msgpack.decode(body, type=MqAbstraction.Message)

    match message:
        case MqAbstraction.HasMoreThanXCreditRequest(message_id=message_id, user_id=user_id, credit_threshold=credit_threshold):
            app.logger.debug(f"Received HasMoreThanXCreditRequest with message_id: {message_id}, user_id: {user_id}, credit_threshold: {credit_threshold}")
            user_entry: UserValue | None = None
            reply = None
            try: # Try to get the user from db, if it fails reply with an error message
                user_entry: UserValue = get_user_from_db(user_id)
                has_more_credit = user_entry.credit >= credit_threshold
                reply = MqAbstraction.HasMoreThanXCreditReply(
                    message_id=message_id,
                    has_more_credit=has_more_credit
                )
            except Exception:
                reply = MqAbstraction.ErrorMessage(
                    message_id=message_id,
                    error_num=1
                )

            # Publish either the response, or the error message
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
        case MqAbstraction.SubtractCreditRequest(message_id=message_id, user_id=user_id, amount=amount):
            app.logger.debug(f"Received SubtractCreditRequest with message_id: {message_id}, user_id: {user_id}, amount: {amount}")
            user_entry: UserValue | None = None
            reply = None
            try: # Try to get the user from db, if it fails reply with an error message
                user_entry: UserValue = get_user_from_db(user_id)
                success = False
                if user_entry.credit >= amount:
                    success = True
                    user_entry.credit -= int(amount)
                    db.set(user_id, msgpack.encode(user_entry))

                reply = MqAbstraction.SubtractCreditReply(
                    message_id=message_id,
                    success=success
                )
            except Exception:
                reply = MqAbstraction.ErrorMessage(
                    message_id=message_id,
                    error_num=2
                )
                
            # Publish either the response, or the error message
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

reply_to_queue, consumer_rabbitmq_channel = MqAbstraction.spawn_rabbitmq_consumer_thread('payment_queue', queue_receive_callback, declare_queues)

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


class UserValue(Struct):
    credit: int


def get_user_from_db(user_id: str) -> UserValue | None:
    try:
        # get serialized data
        entry: bytes = db.get(user_id)
    except redis.exceptions.RedisError:
        return abort(400, DB_ERROR_STR)
    # deserialize data if it exists else return null
    entry: UserValue | None = msgpack.decode(entry, type=UserValue) if entry else None
    if entry is None:
        # if user does not exist in the database; abort
        abort(400, f"User: {user_id} not found!")
    return entry


@app.post('/create_user')
def create_user():
    key = str(uuid.uuid4())
    value = msgpack.encode(UserValue(credit=0))
    try:
        db.set(key, value)
    except redis.exceptions.RedisError:
        return abort(400, DB_ERROR_STR)
    return jsonify({'user_id': key})


@app.post('/batch_init/<n>/<starting_money>')
def batch_init_users(n: int, starting_money: int):
    n = int(n)
    starting_money = int(starting_money)
    kv_pairs: dict[str, bytes] = {f"{i}": msgpack.encode(UserValue(credit=starting_money))
                                  for i in range(n)}
    try:
        db.mset(kv_pairs)
    except redis.exceptions.RedisError:
        return abort(400, DB_ERROR_STR)
    return jsonify({"msg": "Batch init for users successful"})


@app.get('/find_user/<user_id>')
def find_user(user_id: str):
    user_entry: UserValue = get_user_from_db(user_id)
    return jsonify(
        {
            "user_id": user_id,
            "credit": user_entry.credit
        }
    )


@app.post('/add_funds/<user_id>/<amount>')
def add_credit(user_id: str, amount: int):
    user_entry: UserValue = get_user_from_db(user_id)
    # update credit, serialize and update database
    user_entry.credit += int(amount)
    try:
        db.set(user_id, msgpack.encode(user_entry))
    except redis.exceptions.RedisError:
        return abort(400, DB_ERROR_STR)
    return Response(f"User: {user_id} credit updated to: {user_entry.credit}", status=200)


@app.post('/pay/<user_id>/<amount>')
def remove_credit(user_id: str, amount: int):
    app.logger.debug(f"Removing {amount} credit from user: {user_id}")
    user_entry: UserValue = get_user_from_db(user_id)
    # update credit, serialize and update database
    user_entry.credit -= int(amount)
    if user_entry.credit < 0:
        abort(400, f"User: {user_id} credit cannot get reduced below zero!")
    try:
        db.set(user_id, msgpack.encode(user_entry))
    except redis.exceptions.RedisError:
        return abort(400, DB_ERROR_STR)
    return Response(f"User: {user_id} credit updated to: {user_entry.credit}", status=200)


if __name__ == '__main__':
    app.run(host="0.0.0.0", port=8000, debug=True)
else:
    gunicorn_logger = logging.getLogger('gunicorn.error')
    app.logger.handlers = gunicorn_logger.handlers
    app.logger.setLevel(gunicorn_logger.level)
