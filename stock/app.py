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


class StockCompensation(Struct):
    order_id: str
    previous_stock: dict[str, int]


async def get_item_from_db(item_id: str) -> StockValue | None:
    try:
        entry: bytes = await db.get(item_id)
    except RedisError as e:
        logger.error(f"DB error fetching item {item_id}: {e}")
        return None
    entry: StockValue | None = msgpack.decode(entry, type=StockValue) if entry else None
    if entry is None:
        logger.warning(f"Item: {item_id} not found!")
        return None
    return entry


async def ensure_stream():
    for stream_name, subjects in [
        ("CHECKOUT", ["checkout.>"]),
        ("STOCK", ["stock.>"]),
        ("INBOX", ["inbox.>"]),
    ]:
        try:
            await js.add_stream(name=stream_name, subjects=subjects)
        except Exception:
            pass


async def publish_reply(request_id: str, response):
    await js.publish(f"inbox.{request_id}", msgpack.encode(response))


async def handle_checkout_stock(msg):
    req: CheckoutRequest = msgpack.decode(msg.data, type=CheckoutRequest)

    if await db.exists(_saga_commit_key(req.saga_id)):
        logger.info(f"Duplicate checkout.stock for saga {req.saga_id}, skipping")
        return

    try:
        await db.set(_saga_started_key(req.saga_id), req.order_id)
    except RedisError as e:
        await js.publish(
            "stock.result",
            msgpack.encode(
                CheckoutResult(
                    saga_id=req.saga_id,
                    message_id=str(uuid.uuid4()),
                    request_id=req.request_id,
                    order_id=req.order_id,
                    success=False,
                    error=str(e),
                )
            ),
        )
        return

    updated_items: dict[str, bytes] = {}
    previous_stock: dict[str, int] = {}
    for item_id, quantity in req.items.items():
        item_entry = await get_item_from_db(item_id)
        if item_entry is None:
            await db.delete(_saga_started_key(req.saga_id))
            await js.publish(
                "stock.result",
                msgpack.encode(
                    CheckoutResult(
                        saga_id=req.saga_id,
                        message_id=str(uuid.uuid4()),
                        request_id=req.request_id,
                        order_id=req.order_id,
                        success=False,
                        error=f"Item: {item_id} not found!",
                    )
                ),
            )
            return

        if item_entry.stock < quantity:
            await db.delete(_saga_started_key(req.saga_id))
            await js.publish(
                "stock.result",
                msgpack.encode(
                    CheckoutResult(
                        saga_id=req.saga_id,
                        message_id=str(uuid.uuid4()),
                        request_id=req.request_id,
                        order_id=req.order_id,
                        success=False,
                        error=f"Item: {item_id} stock cannot get reduced below zero!",
                    )
                ),
            )
            return

        previous_stock[item_id] = item_entry.stock
        item_entry.stock -= quantity
        updated_items[item_id] = msgpack.encode(item_entry)

    compensation = StockCompensation(order_id=req.order_id, previous_stock=previous_stock)
    try:
        pipe = db.pipeline(transaction=True)
        pipe.set(_saga_compensation_key(req.saga_id), msgpack.encode(compensation))
        for item_id, encoded in updated_items.items():
            pipe.set(item_id, encoded)
        pipe.set(_saga_commit_key(req.saga_id), msgpack.encode(req))
        await pipe.execute()
    except RedisError as e:
        await db.delete(_saga_started_key(req.saga_id))
        await js.publish(
            "stock.result",
            msgpack.encode(
                CheckoutResult(
                    saga_id=req.saga_id,
                    message_id=str(uuid.uuid4()),
                    request_id=req.request_id,
                    order_id=req.order_id,
                    success=False,
                    error=str(e),
                )
            ),
        )
        return

    try:
        await js.publish(
            "stock.result",
            msgpack.encode(
                CheckoutResult(
                    saga_id=req.saga_id,
                    message_id=str(uuid.uuid4()),
                    request_id=req.request_id,
                    order_id=req.order_id,
                    success=True,
                    error="",
                )
            ),
        )
        await db.set(_saga_outbox_key(req.saga_id), b"1")
    except Exception as e:
        logger.error(f"Failed to publish stock.result for saga {req.saga_id}: {e}")


async def _rollback_stock_items(previous_stock: dict[str, int]):
    for item_id, stock_value in previous_stock.items():
        try:
            item_entry = await get_item_from_db(item_id)
            if item_entry is not None:
                item_entry.stock = stock_value
                await db.set(item_id, msgpack.encode(item_entry))
        except Exception as e:
            logger.error(f"Rollback failed for item {item_id}: {e}")


async def handle_create_item(msg):
    try:
        req: StockCreateItemRequest = msgpack.decode(msg.data, type=StockCreateItemRequest)
    except Exception as e:
        logger.error(f"Failed to decode create item message: {e}")
        return

    key = str(uuid.uuid4())
    value = msgpack.encode(StockValue(stock=0, price=req.price))

    try:
        await db.set(key, value)
        result = StockCreateItemResult(message_id=str(uuid.uuid4()), request_id=req.request_id, item_id=key, error="")
        await publish_reply(req.request_id, result)
    except RedisError:
        result = StockCreateItemResult(message_id=str(uuid.uuid4()), request_id=req.request_id, item_id="", error=DB_ERROR_STR)
        await publish_reply(req.request_id, result)


async def handle_batch_init_items(msg):
    try:
        req: StockBatchInitRequest = msgpack.decode(msg.data, type=StockBatchInitRequest)
    except Exception as e:
        logger.error(f"Failed to decode batch init message: {e}")
        return

    kv_pairs: dict[str, bytes] = {f"{i}": msgpack.encode(StockValue(stock=req.starting_stock, price=req.item_price)) for i in range(req.n)}
    try:
        await db.mset(kv_pairs)
        result = StockBatchInitResult(message_id=str(uuid.uuid4()), request_id=req.request_id, success=True, error="")
        await publish_reply(req.request_id, result)
    except RedisError:
        result = StockBatchInitResult(message_id=str(uuid.uuid4()), request_id=req.request_id, success=False, error=DB_ERROR_STR)
        await publish_reply(req.request_id, result)


async def handle_find_item(msg):
    try:
        req: StockFindItemRequest = msgpack.decode(msg.data, type=StockFindItemRequest)
    except Exception as e:
        logger.error(f"Failed to decode find item message: {e}")
        return

    item_entry = await get_item_from_db(req.item_id)
    result = StockFindItemResult(
        message_id=str(uuid.uuid4()),
        request_id=req.request_id,
        item_id=req.item_id,
        item=item_entry,
        error="" if item_entry else f"Item: {req.item_id} not found!",
    )
    await publish_reply(req.request_id, result)


async def handle_add_amount(msg):
    try:
        req: StockAddAmountRequest = msgpack.decode(msg.data, type=StockAddAmountRequest)
    except Exception as e:
        logger.error(f"Failed to decode add amount message: {e}")
        return

    item_entry = await get_item_from_db(req.item_id)
    if item_entry is None:
        result = StockAddAmountResult(
            message_id=str(uuid.uuid4()),
            request_id=req.request_id,
            item_id=req.item_id,
            stock=0,
            error=f"Item: {req.item_id} not found!",
        )
        await publish_reply(req.request_id, result)
        return

    item_entry.stock += req.amount
    try:
        await db.set(req.item_id, msgpack.encode(item_entry))
        result = StockAddAmountResult(
            message_id=str(uuid.uuid4()),
            request_id=req.request_id,
            item_id=req.item_id,
            stock=item_entry.stock,
            error="",
        )
        await publish_reply(req.request_id, result)
    except RedisError:
        result = StockAddAmountResult(
            message_id=str(uuid.uuid4()),
            request_id=req.request_id,
            item_id=req.item_id,
            stock=0,
            error=DB_ERROR_STR,
        )
        await publish_reply(req.request_id, result)


async def handle_subtract_amount(msg):
    try:
        req: StockSubtractAmountRequest = msgpack.decode(msg.data, type=StockSubtractAmountRequest)
    except Exception as e:
        logger.error(f"Failed to decode subtract amount message: {e}")
        return

    item_entry = await get_item_from_db(req.item_id)
    if item_entry is None:
        result = StockSubtractAmountResult(
            message_id=str(uuid.uuid4()),
            request_id=req.request_id,
            item_id=req.item_id,
            stock=0,
            error=f"Item: {req.item_id} not found!",
        )
        await publish_reply(req.request_id, result)
        return

    item_entry.stock -= req.amount
    if item_entry.stock < 0:
        result = StockSubtractAmountResult(
            message_id=str(uuid.uuid4()),
            request_id=req.request_id,
            item_id=req.item_id,
            stock=0,
            error=f"Item: {req.item_id} stock cannot get reduced below zero!",
        )
        await publish_reply(req.request_id, result)
        return

    try:
        await db.set(req.item_id, msgpack.encode(item_entry))
        result = StockSubtractAmountResult(
            message_id=str(uuid.uuid4()),
            request_id=req.request_id,
            item_id=req.item_id,
            stock=item_entry.stock,
            error="",
        )
        await publish_reply(req.request_id, result)
    except RedisError:
        result = StockSubtractAmountResult(
            message_id=str(uuid.uuid4()),
            request_id=req.request_id,
            item_id=req.item_id,
            stock=0,
            error=DB_ERROR_STR,
        )
        await publish_reply(req.request_id, result)


async def _cleanup_saga(saga_id: str):
    try:
        await db.delete(
            _saga_started_key(saga_id),
            _saga_compensation_key(saga_id),
            _saga_commit_key(saga_id),
            _saga_outbox_key(saga_id),
        )
    except RedisError as e:
        logger.warning(f"Could not clean up saga keys for {saga_id}: {e}")


async def recover_sagas():
    logger.info("Stock service: scanning for incomplete sagas...")
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
                                "stock.result",
                                msgpack.encode(
                                    CheckoutResult(
                                        saga_id=saga_id,
                                        message_id=str(uuid.uuid4()),
                                        request_id=req.request_id,
                                        order_id=req.order_id,
                                        success=True,
                                        error="",
                                    )
                                ),
                            )
                            await db.set(_saga_outbox_key(saga_id), b"1")
                    except Exception as e:
                        logger.error(f"Recovery: failed to re-publish saga {saga_id}: {e}")

            if cursor == 0:
                break
    except RedisError as e:
        logger.error(f"Recovery scan failed: {e}")


async def startup():
    global nc, js, logger
    logger = logging.getLogger("stock-service")
    nc = await nats.connect(NATS_URL)
    js = nc.jetstream()
    await ensure_stream()
    await recover_sagas()

    await js.subscribe("stock.create_item", durable="stock-create-item", queue="stock-create-item", cb=handle_create_item)
    await js.subscribe("stock.batch_init", durable="stock-batch-init", queue="stock-batch-init", cb=handle_batch_init_items)
    await js.subscribe("stock.find", durable="stock-find", queue="stock-find", cb=handle_find_item)
    await js.subscribe("stock.add", durable="stock-add", queue="stock-add", cb=handle_add_amount)
    await js.subscribe("stock.subtract", durable="stock-subtract", queue="stock-subtract", cb=handle_subtract_amount)

    await js.subscribe("checkout.stock", durable="stock-checkout", queue="stock-checkout", cb=handle_checkout_stock)


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
