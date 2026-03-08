import logging
import os
import uuid

import nats
from redis.asyncio import Redis
from redis.exceptions import RedisError

from msgspec import msgpack
import asyncio

from common.messages import *

DB_ERROR_STR = "DB error"

NATS_URL = os.environ['NATS_URL']
MESSAGE_STREAM_SUBJECTS = ["order.*", "payment.*", "stock.*", "checkout.>", "inbox.>"]

db: Redis = Redis(host=os.environ['REDIS_HOST'],
                  port=int(os.environ['REDIS_PORT']),
                  password=os.environ['REDIS_PASSWORD'],
                  db=int(os.environ['REDIS_DB']))

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
        await db.set(req.user_id, msgpack.encode(user_entry))
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


async def shutdown():
    await nc.drain()
    await db.aclose()


async def main():
    logger = logging.getLogger("payment-service")
    logging.basicConfig(level=logging.INFO)
    await startup()
    # Keep the service running
    await asyncio.sleep(float('inf'))


if __name__ == '__main__':
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Shutting down...")
else:
    logging.basicConfig(level=logging.INFO)
