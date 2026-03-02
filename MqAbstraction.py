from typing import Union
from msgspec import Struct

import threading
import pika
import os
from functools import reduce
import operator


_message_registry: list[type] = []
def Message(cls: type) -> type:
    _message_registry.append(cls)
    return cls

@Message
class HasMoreThanXCreditRequest(Struct, tag=True):
    message_id: str
    user_id: str
    credit_threshold: float

@Message
class HasMoreThanXCreditReply(Struct, tag=True):
    message_id: str
    has_more_credit: bool

@Message
class ModifyStockRequest(Struct, tag=True):
    message_id: str
    to_modify: dict[str, int]

@Message
class ModifyStockReply(Struct, tag=True):
    message_id: str
    success: bool

@Message
class SubtractCreditRequest(Struct, tag=True):
    message_id: str
    user_id: str
    amount: float

@Message
class SubtractCreditReply(Struct, tag=True):
    message_id: str
    success: bool

@Message
class ErrorMessage(Struct, tag=True):
    message_id: str
    error_num: int

Message = reduce(operator.or_, _message_registry)


def spawn_rabbitmq_consumer_thread(listening_queue: str, callback, declare_queues: list[str] = []):
    consumer_rabbitmq_connection = pika.BlockingConnection(pika.ConnectionParameters(
        host=os.environ['RABBITMQ_HOST'],
        port=int(os.environ['RABBITMQ_PORT']),
        credentials=pika.PlainCredentials(
            username=os.environ['RABBITMQ_USER'],
            password=os.environ['RABBITMQ_PASSWORD']
        ))
    )

    consumer_rabbitmq_channel = consumer_rabbitmq_connection.channel()
    for q in declare_queues:
        consumer_rabbitmq_channel.queue_declare(queue=q, durable=True)

    result = consumer_rabbitmq_channel.queue_declare(queue='', exclusive=True)
    reply_to_queue = result.method.queue

    consumer_rabbitmq_channel.basic_consume(
        queue=listening_queue, 
        on_message_callback=callback, 
        auto_ack=True # TODO, make this false and add manual ack after processing message
    )
    consumer_rabbitmq_channel.basic_consume(
        queue=reply_to_queue,
        on_message_callback=callback,
        auto_ack=True
    )
    consumer_rabbitmq_channel.basic_qos(prefetch_count=1)

    thread = threading.Thread(target=consumer_rabbitmq_channel.start_consuming, daemon=True)
    thread.start()

    return reply_to_queue, consumer_rabbitmq_channel


def setup_rabbit_mq_producer(declare_queues: list[str] = []):
    rabbitmq_connection = pika.BlockingConnection(pika.ConnectionParameters(
        host=os.environ['RABBITMQ_HOST'],
        port=int(os.environ['RABBITMQ_PORT']),
        credentials=pika.PlainCredentials(
            username=os.environ['RABBITMQ_USER'],
            password=os.environ['RABBITMQ_PASSWORD']
        ))
    )

    rabbitmq_channel = rabbitmq_connection.channel()
    for q in declare_queues:
        rabbitmq_channel.queue_declare(queue=q, durable=True)
    return rabbitmq_channel



class AbstractMQ():
    def __init__(self, config):
        self.config = config

    def connect(self):
        raise NotImplementedError("Subclasses must implement this method")

    def publish(self, topic, message):
        raise NotImplementedError("Subclasses must implement this method")

    def subscribe(self, topic, callback):
        raise NotImplementedError("Subclasses must implement this method")

    def disconnect(self):
        raise NotImplementedError("Subclasses must implement this method")