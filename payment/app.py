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
        entry: bytes = await db.get(user_id)
    except RedisError:
        raise RedisError(DB_ERROR_STR)
    # deserialize data if it exists else return null
    entry: UserValue | None = msgpack.decode(entry, type=UserValue) if entry else None
    if entry is None:
        # if user does not exist in the database; raise
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
        await db.set(req.user_id, msgpack.encode(user_entry))
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
    await db.raw.aclose()


@app.post('/create_user')
async def create_user():
    key = str(uuid.uuid4())
    value = msgpack.encode(UserValue(credit=0))
    try:
        await db.set(key, value)
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
        await db.mset(kv_pairs)
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
        await db.set(user_id, msgpack.encode(user_entry))
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
        await db.set(user_id, msgpack.encode(user_entry))
    except RedisError:
        return await abort(400, DB_ERROR_STR)
    return Response(f"User: {user_id} credit updated to: {user_entry.credit}", status=200)

# 2PC
@async_transactional
async def handle_2pc_prepare(msg):
    req: CheckoutRequest = msgpack.decode(msg.data, type=CheckoutRequest)

    try:
        user_entry = await get_user_from_db(req.user_id)
    except (ValueError, RedisError) as e:
        await js.publish("checkout.2pc.payment.vote", msgpack.encode(
            CheckoutResult(txn_id=req.txn_id, order_id=req.order_id, success=False, error=str(e))
        ))
        return

    if user_entry.credit < req.total_cost:
        await js.publish("checkout.2pc.payment.vote", msgpack.encode(
            CheckoutResult(
                txn_id=req.txn_id,
                order_id=req.order_id,
                success=False,
                error="Insufficient credit"
            )
        ))
        return

    # reserve payment (do NOT deduct yet)
    await db.set(
        f"pending:{req.txn_id}",
        msgpack.encode({
            "user_id": req.user_id,
            "amount": req.total_cost
        })
    )

    await js.publish("checkout.2pc.payment.vote", msgpack.encode(
        CheckoutResult(txn_id=req.txn_id, order_id=req.order_id, success=True, error="")
    ))
    
@async_transactional
async def handle_2pc_commit(msg):
    req: CheckoutRequest = msgpack.decode(msg.data, type=CheckoutRequest)

    raw = await db.raw.get(f"pending:{req.txn_id}")
    if not raw:
        return

    pending = msgpack.decode(raw, type=dict)
    user_entry = await get_user_from_db(pending["user_id"])
    user_entry.credit -= pending["amount"]
    await db.set(pending["user_id"], msgpack.encode(user_entry))
    await db.raw.delete(f"pending:{req.txn_id}")

async def handle_2pc_abort(msg):
    req: CheckoutRequest = msgpack.decode(msg.data, type=CheckoutRequest)

    await db.raw.delete(f"pending:{req.txn_id}")

# SAGA
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
    logging.basicConfig(level=logging.INFO)
