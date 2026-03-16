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

# KEYS[1] = user_id
# KEYS[2] = saga_compensation_key
# KEYS[3] = saga_commit_key
# ARGV[1] = total_cost
# ARGV[2] = user_id (stored in compensation hash)
# ARGV[3] = order_id (stored in compensation hash)
DEDUCT_CREDIT_SCRIPT = db.register_script("""
local credit = tonumber(redis.call('GET', KEYS[1]))

if credit == nil then
    redis.call('SET', KEYS[3], -2)
    return {-2, 0}
end

local cost = tonumber(ARGV[1])

if credit < cost then
    redis.call('SET', KEYS[3], -1)
    return {-1, credit}
end

local new_credit = credit - cost
redis.call('SET', KEYS[1], new_credit)
redis.call('HSET', KEYS[2], 'user_id', ARGV[2], 'delta', cost, 'order_id', ARGV[3])
redis.call('SET', KEYS[3], 0)
return {0, new_credit}
""")

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
    commit_bytes = await db.get(_saga_commit_key(req.saga_id))
    if commit_bytes:
        commit_data = msgpack.decode(commit_bytes)
        logger.info(f"Duplicate checkout.payment for saga {req.saga_id}, republishing stored event: success={commit_data['success']}")
        if commit_data["success"]:
            # republish the original CheckoutRequest to checkout.stock
            await js.publish("checkout.stock", msgpack.encode(commit_data["data"]))
        else:
            # republish the CheckoutResult to payment.result
            result = commit_data["data"]
            await js.publish(
            "payment.result",
            msgpack.encode(CheckoutResult(
                saga_id=req.saga_id,
                message_id=str(uuid.uuid4()),
                request_id=req.request_id,
                order_id=req.order_id,
                success=result.success,
                error=result.error,
            )),
        )
        await msg.ack()
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
                req.user_id,
                req.order_id,
            ],
        )
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

    status = result[0]

    if status == -2:
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
        await msg.ack()
        return

    if status == -1:
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
        await msg.ack()
        return

    # status == 0: credit deducted, compensation hash and commit key all written atomically
    try:
        await js.publish("checkout.stock", msgpack.encode(req))
        await msg.ack()
    except Exception as e:
        logger.error(f"Failed to publish for saga {req.saga_id}: {e}")


async def handle_stock_result(msg):
    result: CheckoutResult = msgpack.decode(msg.data, type=CheckoutResult)
    if not result.success:
        comp_key = _saga_compensation_key(result.saga_id)
        try:
            async with db.pipeline() as pipe:
                comp_bytes = await pipe.hgetall(comp_key)

                if comp_bytes is None:
                    await pipe.aclose()
                    logger.warning(f"No compensation data for saga {result.saga_id}, cannot rollback payment. THIS IS BAD.")
                    msg.ack()
                    return
                
                comp_dict = {k.decode(): v.decode() for k, v in comp_bytes.items()}
                comp = PaymentCompensation(
                    user_id=comp_dict["user_id"],
                    delta=int(comp_dict["delta"]),
                    order_id=comp_dict["order_id"]
                )
                user_entry = await get_user_from_db(comp.user_id)

                if user_entry is None:
                    await pipe.aclose()
                    logger.error(f"User {comp.user_id} not found, cannot rollback payment.")
                    msg.ack()
                    return

                user_entry.credit += comp.delta

                pipe.multi()
                pipe.set(comp.user_id, msgpack.encode(user_entry))
                pipe.delete(comp_key)

                await pipe.execute()
                await pipe.aclose()
                await msg.ack()

        except WatchError:
            await pipe.aclose()
            logger.error(f"Rollback failed for saga {result.saga_id}: WatchError")
        except (ValueError, RedisError) as e:
            await pipe.aclose()
            logger.error(f"Rollback failed for saga {result.saga_id}: {e}")
    

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
