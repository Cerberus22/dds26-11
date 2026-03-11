import logging
import os
import uuid

from dds_db.transaction import async_transactional
import nats
from redis.exceptions import RedisError

from dds_db import db
from msgspec import msgpack

import asyncio

from common.messages import *

DB_ERROR_STR = "DB error"

NATS_URL = os.environ['NATS_URL']
MESSAGE_STREAM_SUBJECTS = ["order.*", "payment.*", "stock.*", "checkout.>", "inbox.>"]

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

# Global logger
logger = None


async def get_user_from_db(user_id: str) -> UserValue | None:
    try:
        # get serialized data
        entry: bytes = await db.get(user_id)
    except RedisError as e:
        logger.error(f"DB error fetching user {user_id}: {e}")
        return None
    # deserialize data if it exists else return null
    entry: UserValue | None = msgpack.decode(entry, type=UserValue) if entry else None
    if entry is None:
        logger.warning(f"User: {user_id} not found!")
        return None
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
        await js.add_stream(name="MESSAGES", subjects=MESSAGE_STREAM_SUBJECTS)
    except Exception:
        pass  # stream already exists


async def publish_reply(request_id: str, response):
    await js.publish(f"inbox.{request_id}", msgpack.encode(response))


async def handle_checkout_payment(msg):
    req: CheckoutRequest = msgpack.decode(msg.data, type=CheckoutRequest)
    try:
        user_entry = await get_user_from_db(req.user_id)
        if user_entry is None:
            logger.warning(f"Payment checkout failed for order {req.order_id}: User {req.user_id} not found")
            return
    except (ValueError, RedisError) as e:
        logger.warning(f"Payment checkout failed for order {req.order_id}: {e}")
        return

    # update credit, serialize and update database
    user_entry.credit -= req.total_cost
    if user_entry.credit < 0:
        logger.warning(f"User: {req.user_id} credit cannot get reduced below zero!")
        return

    try:
        db.set(req.user_id, msgpack.encode(user_entry))
    except RedisError as e:
        logger.error(f"DB error during payment for order {req.order_id}: {e}")
        return

    # forward to stock service
    next_req = CheckoutRequest(
        message_id=str(uuid.uuid4()),
        request_id=req.request_id,
        order_id=req.order_id,
        user_id=req.user_id,
        total_cost=req.total_cost,
        items=req.items,
    )
    await js.publish("checkout.stock", msgpack.encode(next_req))


async def handle_create_user(msg):
    """Handle user creation request."""
    try:
        req: PaymentCreateUserRequest = msgpack.decode(msg.data, type=PaymentCreateUserRequest)
    except Exception as e:
        logger.error(f"Failed to decode create user message: {e}")
        return
    
    request_id = req.request_id
    key = str(uuid.uuid4())
    value = msgpack.encode(UserValue(credit=0))
    
    try:
        await db.set(key, value)
        result = PaymentCreateUserResult(message_id=str(uuid.uuid4()), request_id=request_id, user_id=key, error="")
        await publish_reply(request_id, result)
    except RedisError as e:
        result = PaymentCreateUserResult(message_id=str(uuid.uuid4()), request_id=request_id, user_id="", error=DB_ERROR_STR)
        await publish_reply(request_id, result)


async def handle_batch_init_users(msg):
    """Handle batch initialization of users."""
    try:
        req: PaymentBatchInitRequest = msgpack.decode(msg.data, type=PaymentBatchInitRequest)
    except Exception as e:
        logger.error(f"Failed to decode batch init message: {e}")
        return
    
    request_id = req.request_id
    kv_pairs: dict[str, bytes] = {f"{i}": msgpack.encode(UserValue(credit=req.starting_money))
                                  for i in range(req.n)}
    try:
        await db.mset(kv_pairs)
        result = PaymentBatchInitResult(message_id=str(uuid.uuid4()), request_id=request_id, success=True, error="")
        await publish_reply(request_id, result)
    except RedisError as e:
        result = PaymentBatchInitResult(message_id=str(uuid.uuid4()), request_id=request_id, success=False, error=DB_ERROR_STR)
        await publish_reply(request_id, result)


async def handle_find_user(msg):
    """Handle user lookup request."""
    try:
        req: PaymentFindUserRequest = msgpack.decode(msg.data, type=PaymentFindUserRequest)
    except Exception as e:
        logger.error(f"Failed to decode find user message: {e}")
        return
    
    request_id = req.request_id
    user_entry = await get_user_from_db(req.user_id)
    
    result = PaymentFindUserResult(
        message_id=str(uuid.uuid4()),
        request_id=request_id,
        user_id=req.user_id,
        user=user_entry,
        error="" if user_entry else f"User: {req.user_id} not found!"
    )

    await publish_reply(request_id, result)


async def handle_add_funds(msg):
    """Handle adding funds to user account."""
    try:
        req: PaymentAddFundsRequest = msgpack.decode(msg.data, type=PaymentAddFundsRequest)
    except Exception as e:
        logger.error(f"Failed to decode add funds message: {e}")
        return
    
    request_id = req.request_id
    user_entry = await get_user_from_db(req.user_id)
    
    if user_entry is None:
        result = PaymentAddFundsResult(
            message_id=str(uuid.uuid4()),
            request_id=request_id,
            user_id=req.user_id,
            credit=0,
            error=f"User: {req.user_id} not found!"
        )
        await publish_reply(request_id, result)
        return
    
    # Update credit
    user_entry.credit += req.amount
    
    try:
        await db.set(req.user_id, msgpack.encode(user_entry))
        result = PaymentAddFundsResult(
            message_id=str(uuid.uuid4()),
            request_id=request_id,
            user_id=req.user_id,
            credit=user_entry.credit,
            error=""
        )
        await publish_reply(request_id, result)
    except RedisError as e:
        result = PaymentAddFundsResult(
            message_id=str(uuid.uuid4()),
            request_id=request_id,
            user_id=req.user_id,
            credit=0,
            error=DB_ERROR_STR
        )
        await publish_reply(request_id, result)


async def handle_remove_credit(msg):
    """Handle removing credit from user account."""
    try:
        req: PaymentRemoveCreditRequest = msgpack.decode(msg.data, type=PaymentRemoveCreditRequest)
    except Exception as e:
        logger.error(f"Failed to decode remove credit message: {e}")
        return
    
    request_id = req.request_id
    user_entry = await get_user_from_db(req.user_id)
    
    if user_entry is None:
        result = PaymentRemoveCreditResult(
            message_id=str(uuid.uuid4()),
            request_id=request_id,
            user_id=req.user_id,
            credit=0,
            error=f"User: {req.user_id} not found!"
        )
        await publish_reply(request_id, result)
        return
    
    # Update credit
    user_entry.credit -= req.amount
    
    if user_entry.credit < 0:
        result = PaymentRemoveCreditResult(
            message_id=str(uuid.uuid4()),
            request_id=request_id,
            user_id=req.user_id,
            credit=0,
            error=f"User: {req.user_id} credit cannot get reduced below zero!"
        )
        await publish_reply(request_id, result)
        return
    
    try:
        await db.set(req.user_id, msgpack.encode(user_entry))
        result = PaymentRemoveCreditResult(
            message_id=str(uuid.uuid4()),
            request_id=request_id,
            user_id=req.user_id,
            credit=user_entry.credit,
            error=""
        )
        await publish_reply(request_id, result)
    except RedisError as e:
        result = PaymentRemoveCreditResult(
            message_id=str(uuid.uuid4()),
            request_id=request_id,
            user_id=req.user_id,
            credit=0,
            error=DB_ERROR_STR
        )
        await publish_reply(request_id, result)


async def startup():
    global nc, js, logger
    logger = logging.getLogger("payment-service")
    nc = await nats.connect(NATS_URL)
    js = nc.jetstream()
    await ensure_stream()
    
    # Subscribe to payment RPC subjects (JetStream request/reply)
    await js.subscribe(
        "payment.create_user",
        durable="payment-create-user",
        queue="payment-create-user",
        cb=handle_create_user,
    )
    await js.subscribe(
        "payment.batch_init",
        durable="payment-batch-init",
        queue="payment-batch-init",
        cb=handle_batch_init_users,
    )
    await js.subscribe(
        "payment.find_user",
        durable="payment-find-user",
        queue="payment-find-user",
        cb=handle_find_user,
    )
    await js.subscribe(
        "payment.add_funds",
        durable="payment-add-funds",
        queue="payment-add-funds",
        cb=handle_add_funds,
    )
    await js.subscribe(
        "payment.remove_credit",
        durable="payment-remove-credit",
        queue="payment-remove-credit",
        cb=handle_remove_credit,
    )

    # Subscribe to checkout messages
    await js.subscribe(
        "checkout.payment",
        durable="payment-checkout",
        queue="payment-checkout",
        cb=handle_checkout_payment,
    )
    await js.subscribe("checkout.2pc.payment.prepare", durable="payment-2pc-prepare", queue="payment-2pc-prepare", cb=handle_2pc_prepare,)
    await js.subscribe("checkout.2pc.payment.commit", durable="payment-2pc-commit", queue="payment-2pc-commit", cb=handle_2pc_commit,)
    await js.subscribe("checkout.2pc.payment.abort", durable="payment-2pc-abort", queue="payment-2pc-abort", cb=handle_2pc_abort,)

async def shutdown():
    await nc.drain()
    db.close()


async def main():
    logger = logging.getLogger("payment-service")
    logging.basicConfig(level=logging.INFO)
    await startup()
    # Keep the service running
    await asyncio.sleep(float('inf'))

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
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Shutting down...")
else:
    logging.basicConfig(level=logging.INFO)
