import logging
import os
import uuid
import threading
import time

from dds_db.transaction import async_transactional
import nats
from redis.asyncio import Redis
from redis.exceptions import RedisError

from dds_db import db
from msgspec import msgpack, Struct
from quart import Quart, jsonify, abort, Response

import atexit

from common.messages import CheckoutRequest, CheckoutResult

# Assuming EventBus is implemented
# from events import EventBus


DB_ERROR_STR = "DB error"


app = Quart("payment-service")

# db: Redis = Redis(host=os.environ['REDIS_HOST'],
#                   port=int(os.environ['REDIS_PORT']),
#                   password=os.environ['REDIS_PASSWORD'],
#                   db=int(os.environ['REDIS_DB']))

# Initialize event bus
# event_bus = EventBus(db)


# def close_db_connection():
#     db.close()


# atexit.register(close_db_connection)
nc: nats.NATS | None = None
js = None

# app = Flask("payment-service")


class UserValue(Struct):
    credit: int


async def get_user_from_db(user_id: str) -> UserValue:
    try:
        # get serialized data
        entry: bytes = db.get(user_id)
    except RedisError:
        raise RedisError(DB_ERROR_STR)
    # deserialize data if it exists else return null
    entry: UserValue | None = msgpack.decode(entry, type=UserValue) if entry else None
    if entry is None:
        # if user does not exist in the database; raise
        raise ValueError(f"User: {user_id} not found!")
    return entry

async def get_user_from_db_for_update(user_id: str) -> UserValue:
    """The same as the above get_user_from_db but with a lock that allows writing to the data without upgrading it."""
    try:
        entry: bytes = db.get_for_update(user_id)
    except RedisError:
        raise RedisError(DB_ERROR_STR)
    entry: UserValue | None = msgpack.decode(entry, type=UserValue) if entry else None
    if entry is None:
        raise ValueError(f"User: {user_id} not found!")
    return entry


async def ensure_stream():
    try:
        await js.add_stream(name="CHECKOUT", subjects=["checkout.>"])
    except Exception:
        pass  # stream already exists


async def handle_checkout_payment(msg):
    req: CheckoutRequest = msgpack.decode(msg.data, type=CheckoutRequest)
    try:
        user_entry = await get_user_from_db(req.user_id)
    except (ValueError, RedisError) as e:
        app.logger.warning(f"Payment checkout failed for order {req.order_id}: {e}")
        return

    # update credit, serialize and update database
    user_entry.credit -= req.total_cost
    if user_entry.credit < 0:
        app.logger.warning(f"User: {req.user_id} credit cannot get reduced below zero!")
        return

    try:
        db.set(req.user_id, msgpack.encode(user_entry))
    except RedisError as e:
        app.logger.error(f"DB error during payment for order {req.order_id}: {e}")
        return

    # forward to stock service
    await js.publish("checkout.stock", msgpack.encode(req))


@app.before_serving
async def startup():
    global nc, js
    nc = await nats.connect(os.environ['NATS_URL'])
    js = nc.jetstream()
    await ensure_stream()
    await js.subscribe(
        "checkout.payment",
        durable="payment-checkout",
        queue="payment-checkout",
        cb=handle_checkout_payment,
    )
    await js.subscribe("checkout.2pc.payment.prepare", durable="payment-2pc-prepare", queue="payment-2pc-prepare", cb=handle_2pc_prepare,)
    await js.subscribe("checkout.2pc.payment.commit", durable="payment-2pc-commit", queue="payment-2pc-commit", cb=handle_2pc_commit,)
    await js.subscribe("checkout.2pc.payment.abort", durable="payment-2pc-abort", queue="payment-2pc-abort", cb=handle_2pc_abort,)

@app.after_serving
async def shutdown():
    await nc.drain()
    db.close()


@app.post('/create_user')
async def create_user():
    key = str(uuid.uuid4())
    value = msgpack.encode(UserValue(credit=0))
    try:
        db.set(key, value)
    except RedisError:
        return await abort(400, DB_ERROR_STR)
    return jsonify({'user_id': key})


@app.post('/batch_init/<n>/<starting_money>')
async def batch_init_users(n: int, starting_money: int):
    n = int(n)
    starting_money = int(starting_money)
    kv_pairs: dict[str, bytes] = {f"{i}": msgpack.encode(UserValue(credit=starting_money))
                                  for i in range(n)}
    try:
        db.mset(kv_pairs)
    except RedisError:
        return await abort(400, DB_ERROR_STR)
    return jsonify({"msg": "Batch init for users successful"})


@app.get('/find_user/<user_id>')
async def find_user(user_id: str):
    try:
        user_entry: UserValue = await get_user_from_db(user_id)
    except (ValueError, RedisError) as e:
        return await abort(400, str(e))
    return jsonify(
        {
            "user_id": user_id,
            "credit": user_entry.credit
        }
    )

@async_transactional
@app.post('/add_funds/<user_id>/<amount>')
async def add_credit(user_id: str, amount: int):
    try:
        user_entry: UserValue = await get_user_from_db(user_id)
    except (ValueError, RedisError) as e:
        return await abort(400, str(e))
    # update credit, serialize and update database
    user_entry.credit += int(amount)
    try:
        db.set(user_id, msgpack.encode(user_entry))
    except RedisError:
        return await abort(400, DB_ERROR_STR)
    return Response(f"User: {user_id} credit updated to: {user_entry.credit}", status=200)

@async_transactional
@app.post('/pay/<user_id>/<amount>')
async def remove_credit(user_id: str, amount: int):
    app.logger.debug(f"Removing {amount} credit from user: {user_id}")
    try:
        user_entry: UserValue = await get_user_from_db(user_id)
    except (ValueError, RedisError) as e:
        return await abort(400, str(e))
    # update credit, serialize and update database
    user_entry.credit -= int(amount)
    if user_entry.credit < 0:
        return await abort(400, f"User: {user_id} credit cannot get reduced below zero!")
    try:
        db.set(user_id, msgpack.encode(user_entry))
    except RedisError:
        return await abort(400, DB_ERROR_STR)
    return Response(f"User: {user_id} credit updated to: {user_entry.credit}", status=200)

# 2PC
async def handle_2pc_prepare(msg):
    req: CheckoutRequest = msgpack.decode(msg.data, type=CheckoutRequest)
    txn_id = uuid.UUID(req.txn_id)
    db.begin_txn(txn_id)

    try:
        user_entry = await get_user_from_db_for_update(req.user_id)
    except (ValueError, RedisError) as e:
        # vote no if error in db or user not found
        await js.publish(
            "checkout.2pc.payment.vote",
            msgpack.encode(
                CheckoutResult(
                    txn_id=req.txn_id,
                    order_id=req.order_id,
                    success=False,
                    error=str(e),
                )
            ),
        )
        db.end_txn(txn_id)
        return

    # vote no if not enough credit
    if user_entry.credit < req.total_cost:
        await js.publish("checkout.2pc.payment.vote", msgpack.encode(
            CheckoutResult(txn_id=req.txn_id, order_id=req.order_id, success=False, error="Insufficient credit")
        ))
        db.end_txn(txn_id)
        return

    # add to pending transactions
    db.set(
        f"pending:{req.txn_id}",
        msgpack.encode({
            "user_id": req.user_id,
            "amount": req.total_cost,
        }),
    )

    # vote yes
    await js.publish("checkout.2pc.payment.vote", msgpack.encode(
        CheckoutResult(txn_id=req.txn_id, order_id=req.order_id, success=True, error="")
    ))
    db.detach_txn()

async def handle_2pc_commit(msg):
    req: CheckoutRequest = msgpack.decode(msg.data, type=CheckoutRequest)
    txn_id = uuid.UUID(req.txn_id)
    db.begin_txn(txn_id)

    # execute pending transaction
    raw = db.get(f"pending:{req.txn_id}")
    if raw:
        pending = msgpack.decode(raw, type=dict)
        user_entry = await get_user_from_db_for_update(pending["user_id"])
        user_entry.credit -= pending["amount"]
        db.set(pending["user_id"], msgpack.encode(user_entry))
        db.delete(f"pending:{req.txn_id}")

    # release locks
    db.end_txn(txn_id)

    # ack the commit
    await js.publish("checkout.2pc.payment.ack", msgpack.encode(
        CheckoutResult(txn_id=req.txn_id, order_id=req.order_id, success=True, error="")
    ))

async def handle_2pc_abort(msg):
    req: CheckoutRequest = msgpack.decode(msg.data, type=CheckoutRequest)
    txn_id = uuid.UUID(req.txn_id)

    # delete pending txn and release locks to abort
    db.delete(f"pending:{req.txn_id}")
    db.end_txn(txn_id)

    # ack the abort
    await js.publish("checkout.2pc.payment.ack", msgpack.encode(
        CheckoutResult(txn_id=req.txn_id, order_id=req.order_id, success=True, error="")
    ))

if __name__ == '__main__':
    app.run(host="0.0.0.0", port=8000, debug=True)
else:
    logging.basicConfig(level=logging.INFO)
