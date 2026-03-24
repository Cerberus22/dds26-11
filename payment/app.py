import asyncio
import logging
import os
from random import random
import uuid

import nats
from msgspec import Struct, msgpack
from redis.asyncio import Redis
from redis.exceptions import RedisError, WatchError

from common.messages import *


DB_ERROR_STR = "DB error"

NATS_URL = os.environ["NATS_URL"]

db: Redis = Redis(
    host=os.environ["REDIS_HOST"],
    port=int(os.environ["REDIS_PORT"]),
    password=os.environ["REDIS_PASSWORD"],
    db=int(os.environ["REDIS_DB"]),
)

nc: nats.NATS | None = None
js = None
logger = None


def _saga_compensation_key(saga_id: str) -> str:
    return f"saga:compensation:{saga_id}"


def _saga_commit_key(saga_id: str) -> str:
    return f"saga:commit:{saga_id}"


class PaymentCompensation(Struct):
    user_id: str
    delta: int
    order_id: str


async def get_user_from_db(user_id: str) -> UserValue | None:
    try:
        entry: bytes = await db.get(user_id)
    except RedisError as e:
        logger.error(f"DB error fetching user {user_id}: {e}")
        return None
    entry: UserValue | None = msgpack.decode(entry, type=UserValue) if entry else None
    if entry is None:
        logger.warning(f"User: {user_id} not found!")
        return None
    return entry


async def ensure_stream():
    for stream_name, subjects in [
        ("CHECKOUT", ["checkout.>"]),
        ("PAYMENT", ["payment.>"]),
        ("STOCK", ["stock.>"]),
        ("INBOX", ["inbox.>"]),
    ]:
        try:
            await js.add_stream(name=stream_name, subjects=subjects)
        except Exception:
            pass


async def publish_reply(request_id: str, response):
    await js.publish(f"inbox.{request_id}", msgpack.encode(response))


async def handle_reserve_payment(msg):
    req: CheckoutRequest = msgpack.decode(msg.data, type=CheckoutRequest)
    commit_bytes = await db.get(_saga_commit_key(req.saga_id))
    if commit_bytes:
        commit_data = msgpack.decode(commit_bytes)
        logger.info(f"Duplicate payment.reserve for saga {req.saga_id}, republishing stored result: success={commit_data['success']}")
        result = commit_data["data"]
        await publish_payment_reserved(req, result["success"], result["error"])
        await msg.ack()
        return

    for attempt in range(3):
        pipe = db.pipeline()
        try:
            await pipe.watch(req.user_id)
            user_entry = await get_user_from_db(req.user_id)

            if user_entry is None:
                await pipe.unwatch()
                await pipe.aclose()
                result = CheckoutResult(
                    saga_id=req.saga_id,
                    message_id=str(uuid.uuid4()),
                    request_id=req.request_id,
                    order_id=req.order_id,
                    success=False,
                    error=f"User: {req.user_id} not found!",
                )
                commit_data = {"success": False, "data": result}
                await db.set(_saga_commit_key(req.saga_id), msgpack.encode(commit_data))
                await publish_payment_reserved(req, False, result.error)
                await msg.ack()
                return

            if user_entry.credit < req.total_cost:
                await pipe.unwatch()
                await pipe.aclose()
                result = CheckoutResult(
                    saga_id=req.saga_id,
                    message_id=str(uuid.uuid4()),
                    request_id=req.request_id,
                    order_id=req.order_id,
                    success=False,
                    error=f"User: {req.user_id} insufficient credit!",
                )
                commit_data = {"success": False, "data": result}
                await db.set(_saga_commit_key(req.saga_id), msgpack.encode(commit_data))
                await publish_payment_reserved(req, False, result.error)
                await msg.ack()
                return

            compensation = PaymentCompensation(
                user_id=req.user_id,
                delta=req.total_cost,
                order_id=req.order_id,
            )
            user_entry.credit -= req.total_cost

            pipe.multi()
            pipe.set(_saga_compensation_key(req.saga_id), msgpack.encode(compensation))
            pipe.set(req.user_id, msgpack.encode(user_entry))
            result = CheckoutResult(
                saga_id=req.saga_id,
                message_id=str(uuid.uuid4()),
                request_id=req.request_id,
                order_id=req.order_id,
                success=True,
                error="",
            )
            commit_data = {"success": True, "data": result}
            pipe.set(_saga_commit_key(req.saga_id), msgpack.encode(commit_data))
            await pipe.execute()
            await pipe.aclose()
            await publish_payment_reserved(req, True, "")
            await msg.ack()
            return

        except WatchError:
            await pipe.aclose()
            continue

        except RedisError as e:
            await pipe.aclose()
            if attempt < 2:
                continue
            result = CheckoutResult(
                saga_id=req.saga_id,
                message_id=str(uuid.uuid4()),
                request_id=req.request_id,
                order_id=req.order_id,
                success=False,
                error=str(e),
            )
            commit_data = {"success": False, "data": result}
            await db.set(_saga_commit_key(req.saga_id), msgpack.encode(commit_data))
            await publish_payment_reserved(req, False, str(e))
            await msg.ack()
            return

    else:
        result = CheckoutResult(
            saga_id=req.saga_id,
            message_id=str(uuid.uuid4()),
            request_id=req.request_id,
            order_id=req.order_id,
            success=False,
            error="Exhausted retries due to contention",
        )
        commit_data = {"success": False, "data": result}
        await db.set(_saga_commit_key(req.saga_id), msgpack.encode(commit_data))
        await publish_payment_reserved(req, False, result.error)
        await msg.ack()
        return


async def publish_payment_reserved(req: CheckoutRequest, success: bool, error: str):
    await js.publish(
        "orchestrator.payment_reserved",
        msgpack.encode(
            CheckoutResult(
                saga_id=req.saga_id,
                message_id=str(uuid.uuid4()),
                request_id=req.request_id,
                order_id=req.order_id,
                success=success,
                error=error,
            )
        ),
    )

async def publish_payment_compensated(req: CheckoutRequest, success: bool, error: str):
    await js.publish(
        "orchestrator.payment_compensated",
        msgpack.encode(
            CheckoutResult(
                saga_id=req.saga_id,
                message_id=str(uuid.uuid4()),
                request_id=req.request_id,
                order_id=req.order_id,
                success=success,
                error=error,
            )
        ),
    )

async def handle_compensate_payment(msg):
    """
    Handle compensation request from the orchestrator. Receive a CheckoutRequest.
    """
    result: CheckoutRequest = msgpack.decode(msg.data, type=CheckoutRequest)
    # if not result.success: # we compensate based on command by the orchestrator
    comp_key = _saga_compensation_key(result.saga_id)
    try:
        async with db.pipeline() as pipe:
            await pipe.watch(comp_key)
            comp_bytes = await pipe.get(comp_key)

            # No comp data, failure
            if comp_bytes is None:
                await pipe.aclose()
                logger.warning(f"No compensation data for saga {result.saga_id}, cannot rollback payment. THIS IS BAD.")
                await publish_payment_compensated(result, False, "No compensation data found!")
                msg.ack()
                return

            comp: PaymentCompensation = msgpack.decode(comp_bytes, type=PaymentCompensation)
            user_entry = await get_user_from_db(comp.user_id)

            # User not found, failure
            if user_entry is None:
                await pipe.aclose()
                logger.error(f"User {comp.user_id} not found, cannot rollback payment.")
                await publish_payment_compensated(result, False, f"User {comp.user_id} not found!")
                msg.ack()
                return

            user_entry.credit += comp.delta

            pipe.multi()
            pipe.set(comp.user_id, msgpack.encode(user_entry))
            pipe.delete(comp_key)

            await pipe.execute()
            await pipe.aclose()
            # Success!
            await publish_payment_compensated(result, True, "")
            await msg.ack()

    # no ack on failure to trigger retry
    except WatchError:
        await pipe.aclose()
        logger.error(f"Rollback failed for saga {result.saga_id}: WatchError")
    except (ValueError, RedisError) as e:
        await pipe.aclose()
        logger.error(f"Rollback failed for saga {result.saga_id}: {e}")
    
# basic functionality handlers
async def handle_create_user(msg):
    try:
        req: PaymentCreateUserRequest = msgpack.decode(msg.data, type=PaymentCreateUserRequest)
    except Exception as e:
        logger.error(f"Failed to decode create user message: {e}")
        return

    key = str(uuid.uuid4())
    value = msgpack.encode(UserValue(credit=0))

    try:
        await db.set(key, value)
        result = PaymentCreateUserResult(message_id=str(uuid.uuid4()), request_id=req.request_id, user_id=key, error="")
        await publish_reply(req.request_id, result)
    except RedisError:
        result = PaymentCreateUserResult(message_id=str(uuid.uuid4()), request_id=req.request_id, user_id="", error=DB_ERROR_STR)
        await publish_reply(req.request_id, result)

async def handle_batch_init_users(msg):
    try:
        req: PaymentBatchInitRequest = msgpack.decode(msg.data, type=PaymentBatchInitRequest)
    except Exception as e:
        logger.error(f"Failed to decode batch init message: {e}")
        return

    kv_pairs: dict[str, bytes] = {f"{i}": msgpack.encode(UserValue(credit=req.starting_money)) for i in range(req.n)}
    try:
        await db.mset(kv_pairs)
        result = PaymentBatchInitResult(message_id=str(uuid.uuid4()), request_id=req.request_id, success=True, error="")
        await publish_reply(req.request_id, result)
    except RedisError:
        result = PaymentBatchInitResult(message_id=str(uuid.uuid4()), request_id=req.request_id, success=False, error=DB_ERROR_STR)
        await publish_reply(req.request_id, result)

async def handle_find_user(msg):
    try:
        req: PaymentFindUserRequest = msgpack.decode(msg.data, type=PaymentFindUserRequest)
    except Exception as e:
        logger.error(f"Failed to decode find user message: {e}")
        return

    user_entry = await get_user_from_db(req.user_id)
    result = PaymentFindUserResult(
        message_id=str(uuid.uuid4()),
        request_id=req.request_id,
        user_id=req.user_id,
        user=user_entry,
        error="" if user_entry else f"User: {req.user_id} not found!",
    )
    await publish_reply(req.request_id, result)

async def handle_add_funds(msg):
    try:
        req: PaymentAddFundsRequest = msgpack.decode(msg.data, type=PaymentAddFundsRequest)
    except Exception as e:
        logger.error(f"Failed to decode add funds message: {e}")
        return

    user_entry = await get_user_from_db(req.user_id)
    if user_entry is None:
        result = PaymentAddFundsResult(
            message_id=str(uuid.uuid4()),
            request_id=req.request_id,
            user_id=req.user_id,
            credit=0,
            error=f"User: {req.user_id} not found!",
        )
        await publish_reply(req.request_id, result)
        return

    user_entry.credit += req.amount
    try:
        await db.set(req.user_id, msgpack.encode(user_entry))
        result = PaymentAddFundsResult(
            message_id=str(uuid.uuid4()),
            request_id=req.request_id,
            user_id=req.user_id,
            credit=user_entry.credit,
            error="",
        )
        await publish_reply(req.request_id, result)
    except RedisError:
        result = PaymentAddFundsResult(
            message_id=str(uuid.uuid4()),
            request_id=req.request_id,
            user_id=req.user_id,
            credit=0,
            error=DB_ERROR_STR,
        )
        await publish_reply(req.request_id, result)

async def handle_remove_credit(msg):
    try:
        req: PaymentRemoveCreditRequest = msgpack.decode(msg.data, type=PaymentRemoveCreditRequest)
    except Exception as e:
        logger.error(f"Failed to decode remove credit message: {e}")
        return

    user_entry = await get_user_from_db(req.user_id)
    if user_entry is None:
        result = PaymentRemoveCreditResult(
            message_id=str(uuid.uuid4()),
            request_id=req.request_id,
            user_id=req.user_id,
            credit=0,
            error=f"User: {req.user_id} not found!",
        )
        await publish_reply(req.request_id, result)
        return

    user_entry.credit -= req.amount
    if user_entry.credit < 0:
        result = PaymentRemoveCreditResult(
            message_id=str(uuid.uuid4()),
            request_id=req.request_id,
            user_id=req.user_id,
            credit=0,
            error=f"User: {req.user_id} credit cannot get reduced below zero!",
        )
        await publish_reply(req.request_id, result)
        return

    try:
        await db.set(req.user_id, msgpack.encode(user_entry))
        result = PaymentRemoveCreditResult(
            message_id=str(uuid.uuid4()),
            request_id=req.request_id,
            user_id=req.user_id,
            credit=user_entry.credit,
            error="",
        )
        await publish_reply(req.request_id, result)
    except RedisError:
        result = PaymentRemoveCreditResult(
            message_id=str(uuid.uuid4()),
            request_id=req.request_id,
            user_id=req.user_id,
            credit=0,
            error=DB_ERROR_STR,
        )
        await publish_reply(req.request_id, result)

async def startup():
    global nc, js, logger
    logger = logging.getLogger("payment-service")
    nc = await nats.connect(NATS_URL)
    js = nc.jetstream()
    await ensure_stream()

    await js.subscribe("payment.create_user", durable="payment-create-user", queue="payment-create-user", cb=handle_create_user)
    await js.subscribe("payment.batch_init", durable="payment-batch-init", queue="payment-batch-init", cb=handle_batch_init_users)
    await js.subscribe("payment.find_user", durable="payment-find-user", queue="payment-find-user", cb=handle_find_user)
    await js.subscribe("payment.add_funds", durable="payment-add-funds", queue="payment-add-funds", cb=handle_add_funds)
    await js.subscribe("payment.remove_credit", durable="payment-remove-credit", queue="payment-remove-credit", cb=handle_remove_credit)

    await js.subscribe("payment.reserve", durable="payment-reserve", queue="payment-reserve", cb=handle_reserve_payment)
    await js.subscribe("payment.compensate", durable="payment-compensate", queue="payment-compensate", cb=handle_compensate_payment)

async def shutdown():
    await nc.drain()
    await db.aclose()


async def main():
    logging.basicConfig(level=logging.INFO)
    await startup()
    await asyncio.sleep(float("inf"))


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Shutting down...")
else:
    logging.basicConfig(level=logging.DEBUG)
