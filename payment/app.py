import logging
import os
import atexit
import uuid
import threading
import time

import redis

from msgspec import msgpack, Struct
from flask import Flask, jsonify, abort, Response

# Assuming EventBus is implemented
from events import EventBus

DB_ERROR_STR = "DB error"


app = Flask("payment-service")

db: redis.Redis = redis.Redis(host=os.environ['REDIS_HOST'],
                              port=int(os.environ['REDIS_PORT']),
                              password=os.environ['REDIS_PASSWORD'],
                              db=int(os.environ['REDIS_DB']))

# Initialize event bus
event_bus = EventBus(db)


def close_db_connection():
    db.close()


atexit.register(close_db_connection)


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

# Simple 2PC

@app.post('/prepare/<user_id>/<amount>/<txn_id>')
def prepare_payment(user_id: str, amount: int, txn_id: str):
    user_entry: UserValue = get_user_from_db(user_id)
    if user_entry.credit < int(amount):
        return abort(400, f"Insufficient credit for user: {user_id} for transaction {txn_id}")
    # deduct credit in the prepare phase, keep log of it in case of an abort
    user_entry.credit -= int(amount)
    db.set(f"pending:{txn_id}", msgpack.encode({
        "user_id": user_id,
        "amount": int(amount)
    }))
    return Response(f"Payment prepared for transaction {txn_id}", status=200)

@app.post('/commit/<txn_id>')
def commit_payment(txn_id: str):
    pending_key = f"pending:{txn_id}"
    raw = db.get(pending_key)
    if not raw:
        return Response(f"Payment already committed for transaction {txn_id}", status=200)
    db.delete(pending_key)
    return Response(f"Payment committed for transaction {txn_id}", status=200)

@app.post('/abort/<txn_id>')
def abort_payment(txn_id: str):
    pending_key = f"pending:{txn_id}"
    raw = db.get(pending_key)
    if not raw:
        return Response(f"Payment already aborted for transaction {txn_id}", status=200)
    # undo the deduction
    pending = msgpack.decode(raw, type=dict)
    user_entry: UserValue = get_user_from_db(pending["user_id"])
    user_entry.credit += pending["amount"]
    db.set(pending["user_id"], msgpack.encode(user_entry))
    db.delete(pending_key)
    return Response(f"Payment aborted for transaction {txn_id}", status=200)

# Payment Service:
# Publishes: PaymentReserved, PaymentFailed
# Subscribes to: ReservePayment, CompensatePayment

def handle_reserve_payment_event(event_data: dict):
    txn_id = event_data["txn_id"]
    user_id = event_data["user_id"]
    amount = event_data["amount"]
    
    try:
        user_entry: UserValue = get_user_from_db(user_id)
        if user_entry.credit < int(amount):
            # Publish: PaymentFailed
            # event_bus.publish("PaymentFailed", {"txn_id": txn_id, "reason": "Insufficient credit"})
            app.logger.info(f"Payment reservation failed for txn {txn_id}: Insufficient credit")
            return
        
        db.set(f"reserved:{txn_id}", msgpack.encode({
            "user_id": user_id,
            "amount": int(amount)
        }))
        
        user_entry.credit -= int(amount)
        db.set(user_id, msgpack.encode(user_entry))
        
        # Publish: PaymentReserved
        # event_bus.publish("PaymentReserved", {"txn_id": txn_id})
        app.logger.info(f"Payment reserved and committed for txn {txn_id}")
    except Exception as e:
        # Publish: PaymentFailed
        # event_bus.publish("PaymentFailed", {"txn_id": txn_id, "reason": str(e)})
        app.logger.error(f"Payment reservation failed for txn {txn_id}: {str(e)}")

def handle_compensate_payment_event(event_data: dict):
    txn_id = event_data["txn_id"]
    
    try:
        reserved_key = f"reserved:{txn_id}"
        raw = db.get(reserved_key)
        if not raw:
            app.logger.info(f"Payment already compensated for txn {txn_id}")
            return
        
        reserved = msgpack.decode(raw, type=dict)
        user_entry: UserValue = get_user_from_db(reserved["user_id"])
        user_entry.credit += reserved["amount"]  # Restore credit
        db.set(reserved["user_id"], msgpack.encode(user_entry))
        db.delete(reserved_key)
        
        app.logger.info(f"Payment compensated for txn {txn_id}")
    except Exception as e:
        app.logger.error(f"Payment compensation failed for txn {txn_id}: {str(e)}")

# event_bus.subscribe("ReservePayment", handle_reserve_payment_event)
# event_bus.subscribe("CompensatePayment", handle_compensate_payment_event)

if __name__ == '__main__':
    app.run(host="0.0.0.0", port=8000, debug=True)
else:
    gunicorn_logger = logging.getLogger('gunicorn.error')
    app.logger.handlers = gunicorn_logger.handlers
    app.logger.setLevel(gunicorn_logger.level)
