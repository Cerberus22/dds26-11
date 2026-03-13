import asyncio
import logging
import os
import uuid

import nats
from msgspec import Struct, msgpack
from redis.asyncio import Redis
from redis.exceptions import RedisError

from common.messages import *


DB_ERROR_STR = "DB error"

NATS_URL = os.environ["NATS_URL"]

db: Redis = Redis(
    host=os.environ["REDIS_HOST"],
    port=int(os.environ["REDIS_PORT"]),
    password=os.environ["REDIS_PASSWORD"],
    db=int(os.environ["REDIS_DB"]),
)

# KEYS[1] = user_id
# KEYS[2] = saga_compensation_key
# KEYS[3] = saga_commit_key
# ARGV[1] = total_cost
# ARGV[2] = encoded CheckoutRequest bytes
# ARGV[3] = user_id (stored in compensation hash)
# ARGV[4] = order_id (stored in compensation hash)
DEDUCT_CREDIT_SCRIPT = db.register_script("""
local credit = tonumber(redis.call('GET', KEYS[1]))

if credit == nil then
    return {-2, 0}
end

local cost = tonumber(ARGV[1])

if credit < cost then
    return {-1, credit}
end

local new_credit = credit - cost
redis.call('SET', KEYS[1], new_credit)
redis.call('HSET', KEYS[2], 'user_id', ARGV[3], 'previous_credit', credit, 'order_id', ARGV[4])
redis.call('SET', KEYS[3], ARGV[2])
return {0, new_credit}
""")

nc: nats.NATS | None = None
js = None
logger = None


def _saga_started_key(saga_id: str) -> str:
    return f"saga:started:{saga_id}"


def _saga_compensation_key(saga_id: str) -> str:
    return f"saga:compensation:{saga_id}"


def _saga_commit_key(saga_id: str) -> str:
    return f"saga:commit:{saga_id}"


def _saga_outbox_key(saga_id: str) -> str:
    return f"saga:outbox:{saga_id}"


async def get_user_from_db(user_id: str) -> UserValue | None:
    try:
        entry: bytes = await db.get(user_id)
    except RedisError as e:
        logger.error(f"DB error fetching user {user_id}: {e}")
        return None
    if entry is None:
        logger.warning(f"User: {user_id} not found!")
        return None
    try:
        credit = int(entry)
    except (TypeError, ValueError) as e:
        logger.error(f"Failed to parse credit for user {user_id}: {e}")
        return None
    return UserValue(credit=credit)


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


async def handle_checkout_payment(msg):
    req: CheckoutRequest = msgpack.decode(msg.data, type=CheckoutRequest)

    if await db.exists(_saga_commit_key(req.saga_id)):
        logger.info(f"Duplicate checkout.payment for saga {req.saga_id}, skipping")
        return

    try:
        await db.set(_saga_started_key(req.saga_id), req.order_id)
    except RedisError as e:
        await js.publish(
            "payment.result",
            msgpack.encode(CheckoutResult(
                saga_id=req.saga_id,
                message_id=str(uuid.uuid4()),
                request_id=req.request_id,
                order_id=req.order_id,
                success=False,
                error=str(e),
            )),
        )
        return

    try:
        result = await DEDUCT_CREDIT_SCRIPT(
            keys=[
                req.user_id,
                _saga_compensation_key(req.saga_id),
                _saga_commit_key(req.saga_id),
            ],
            args=[
                req.total_cost,
                msgpack.encode(req),
                req.user_id,
                req.order_id,
            ],
        )
    except RedisError as e:
        await db.delete(_saga_started_key(req.saga_id))
        await js.publish(
            "payment.result",
            msgpack.encode(CheckoutResult(
                saga_id=req.saga_id,
                message_id=str(uuid.uuid4()),
                request_id=req.request_id,
                order_id=req.order_id,
                success=False,
                error=str(e),
            )),
        )
        return

    status = result[0]

    if status == -2:
        await db.delete(_saga_started_key(req.saga_id))
        await js.publish(
            "payment.result",
            msgpack.encode(CheckoutResult(
                saga_id=req.saga_id,
                message_id=str(uuid.uuid4()),
                request_id=req.request_id,
                order_id=req.order_id,
                success=False,
                error=f"User: {req.user_id} not found!",
            )),
        )
        return

    if status == -1:
        await db.delete(_saga_started_key(req.saga_id))
        await js.publish(
            "payment.result",
            msgpack.encode(CheckoutResult(
                saga_id=req.saga_id,
                message_id=str(uuid.uuid4()),
                request_id=req.request_id,
                order_id=req.order_id,
                success=False,
                error=f"User: {req.user_id} insufficient credit!",
            )),
        )
        return

    # status == 0: credit deducted, compensation hash and commit key all written atomically
    try:
        await js.publish("checkout.stock", msgpack.encode(req))
        await db.set(_saga_outbox_key(req.saga_id), b"1")
    except Exception as e:
        logger.error(f"Failed to publish for saga {req.saga_id}: {e}")


async def handle_stock_result(msg):
    result: CheckoutResult = msgpack.decode(msg.data, type=CheckoutResult)

    if await db.exists(_saga_commit_key(result.saga_id) + ":stock-ack"):
        logger.info(f"Duplicate stock.result for saga {result.saga_id}, skipping")
        return

    if result.success:
        try:
            await db.set(_saga_commit_key(result.saga_id) + ":stock-ack", b"1")
        except RedisError:
            pass
        await _cleanup_saga(result.saga_id)
        return

    await _rollback_payment(result.saga_id)


async def _rollback_payment(saga_id: str):
    comp_key = _saga_compensation_key(saga_id)
    try:
        mapping = await db.hgetall(comp_key)
        if not mapping:
            logger.warning(f"No compensation data for saga {saga_id}, cannot rollback payment")
            return
        user_id = mapping[b"user_id"].decode()
        previous_credit = int(mapping[b"previous_credit"])
        await db.set(user_id, previous_credit)
    except (ValueError, RedisError) as e:
        logger.error(f"Rollback failed for saga {saga_id}: {e}")
    finally:
        await _cleanup_saga(saga_id)


async def _cleanup_saga(saga_id: str):
    try:
        await db.delete(
            _saga_started_key(saga_id),
            _saga_compensation_key(saga_id),
            _saga_commit_key(saga_id),
            _saga_outbox_key(saga_id),
            _saga_commit_key(saga_id) + ":stock-ack",
        )
    except RedisError as e:
        logger.warning(f"Could not clean up saga keys for {saga_id}: {e}")


async def recover_sagas():
    logger.info("Payment service: scanning for incomplete sagas...")
    try:
        cursor = 0
        while True:
            cursor, keys = await db.scan(cursor, match="saga:started:*", count=100)
            for key in keys:
                saga_id = key.decode().removeprefix("saga:started:")
                commit_exists = await db.exists(_saga_commit_key(saga_id))
                outbox_exists = await db.exists(_saga_outbox_key(saga_id))

                if not commit_exists:
                    await _cleanup_saga(saga_id)
                elif not outbox_exists:
                    try:
                        outbox_bytes = await db.get(_saga_commit_key(saga_id))
                        if outbox_bytes:
                            req: CheckoutRequest = msgpack.decode(outbox_bytes, type=CheckoutRequest)
                            await js.publish(
                                "payment.result",
                                msgpack.encode(CheckoutResult(
                                    saga_id=saga_id,
                                    message_id=str(uuid.uuid4()),
                                    request_id=req.request_id,
                                    order_id=req.order_id,
                                    success=True,
                                    error="",
                                )),
                            )
                            await js.publish("checkout.stock", msgpack.encode(req))
                            await db.set(_saga_outbox_key(saga_id), b"1")
                    except Exception as e:
                        logger.error(f"Recovery: failed to re-publish saga {saga_id}: {e}")

            if cursor == 0:
                break
    except RedisError as e:
        logger.error(f"Recovery scan failed: {e}")


async def handle_create_user(msg):
    try:
        req: PaymentCreateUserRequest = msgpack.decode(msg.data, type=PaymentCreateUserRequest)
    except Exception as e:
        logger.error(f"Failed to decode create user message: {e}")
        return

    key = str(uuid.uuid4())

    try:
        await db.set(key, 0)
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

    kv_pairs: dict[str, int] = {f"{i}": req.starting_money for i in range(req.n)}
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

    new_credit = user_entry.credit + req.amount
    try:
        await db.set(req.user_id, new_credit)
        result = PaymentAddFundsResult(
            message_id=str(uuid.uuid4()),
            request_id=req.request_id,
            user_id=req.user_id,
            credit=new_credit,
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

    new_credit = user_entry.credit - req.amount
    if new_credit < 0:
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
        await db.set(req.user_id, new_credit)
        result = PaymentRemoveCreditResult(
            message_id=str(uuid.uuid4()),
            request_id=req.request_id,
            user_id=req.user_id,
            credit=new_credit,
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
    await recover_sagas()

    await js.subscribe("payment.create_user", durable="payment-create-user", queue="payment-create-user", cb=handle_create_user)
    await js.subscribe("payment.batch_init", durable="payment-batch-init", queue="payment-batch-init", cb=handle_batch_init_users)
    await js.subscribe("payment.find_user", durable="payment-find-user", queue="payment-find-user", cb=handle_find_user)
    await js.subscribe("payment.add_funds", durable="payment-add-funds", queue="payment-add-funds", cb=handle_add_funds)
    await js.subscribe("payment.remove_credit", durable="payment-remove-credit", queue="payment-remove-credit", cb=handle_remove_credit)

    await js.subscribe("checkout.payment", durable="payment-checkout", queue="payment-checkout", cb=handle_checkout_payment)
    await js.subscribe("stock.result", durable="payment-stock-result", queue="payment-stock-result", cb=handle_stock_result)


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
