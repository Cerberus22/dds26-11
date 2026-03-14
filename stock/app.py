import asyncio
import logging
import os
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

class StockCompensation(Struct):
    order_id: str
    deltas: dict[str, int]

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
    commit_bytes = await db.get(_saga_commit_key(req.saga_id))
    if commit_bytes:
        result: CheckoutResult = msgpack.decode(commit_bytes, type=CheckoutResult)
        logger.info(f"Duplicate checkout.stock for saga {req.saga_id}, republishing stored result: success={result.success}")
        await publish_stock_result(req, result.success, result.error)
        msg.ack()
        return

    item_ids = list(req.items.keys())

    for attempt in range(3):
        pipe = db.pipeline()
        try:
            await pipe.watch(*item_ids)
            updated_items = {}
            deltas = {}
            for item_id, quantity in req.items.items():
                item_entry = await get_item_from_db(item_id)
                if item_entry is None:
                    pipe.unwatch()
                    result = CheckoutResult(
                        saga_id=req.saga_id,
                        message_id=str(uuid.uuid4()),
                        request_id=req.request_id,
                        order_id=req.order_id,
                        success=False,
                        error=f"Item: {item_id} not found!",
                    )
                    await db.set(_saga_commit_key(req.saga_id), msgpack.encode(result))
                    await publish_stock_result(req, result.success, result.error)
                    msg.ack()
                    return
                if item_entry.stock < quantity:
                    pipe.unwatch()
                    result = CheckoutResult(
                        saga_id=req.saga_id,
                        message_id=str(uuid.uuid4()),
                        request_id=req.request_id,
                        order_id=req.order_id,
                        success=False,
                        error=f"Item: {item_id} insufficient stock!",
                    )
                    await db.set(_saga_commit_key(req.saga_id), msgpack.encode(result))
                    await publish_stock_result(req, result.success, result.error)
                    msg.ack()
                    return
                deltas[item_id] = -quantity
                item_entry.stock -= quantity
                updated_items[item_id] = msgpack.encode(item_entry)
            compensation = StockCompensation(order_id=req.order_id, deltas=deltas)
            pipe.multi()
            pipe.set(_saga_compensation_key(req.saga_id), msgpack.encode(compensation))
            for item_id, encoded in updated_items.items():
                pipe.set(item_id, encoded)
            result = CheckoutResult(
                saga_id=req.saga_id,
                message_id=str(uuid.uuid4()),
                request_id=req.request_id,
                order_id=req.order_id,
                success=True,
                error="",
            )
            pipe.set(_saga_commit_key(req.saga_id), msgpack.encode(result))
            await pipe.execute()
            await publish_stock_result(req, result.success, result.error)
            msg.ack()
            return
        except WatchError:     
            continue
        except RedisError as e:
            if attempt < 2: continue
            result = CheckoutResult(
                saga_id=req.saga_id,
                message_id=str(uuid.uuid4()),
                request_id=req.request_id,
                order_id=req.order_id,
                success=False,
                error=str(e),
            )
            await db.set(_saga_commit_key(req.saga_id), msgpack.encode(result))
            await publish_stock_result(req, result.success, result.error)
            msg.ack()
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
        await db.set(_saga_commit_key(req.saga_id), msgpack.encode(result))
        await publish_stock_result(req, result.success, result.error)
        msg.ack()
        return

async def publish_stock_result(req, success, error):
    await js.publish("stock.result", msgpack.encode(CheckoutResult(
        saga_id=req.saga_id, message_id=str(uuid.uuid4()),
        request_id=req.request_id, order_id=req.order_id,
        success=success, error=error
    )))

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

async def startup():
    global nc, js, logger
    logger = logging.getLogger("stock-service")
    nc = await nats.connect(NATS_URL)
    js = nc.jetstream()
    await ensure_stream()

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

async def handle_stock_result(msg):
    result: CheckoutResult = msgpack.decode(msg.data, type=CheckoutResult)
    if not result.success:
        comp_key = _saga_compensation_key(result.saga_id)
        try:
            async with db.pipeline() as pipe:
                await pipe.watch(comp_key)
                comp_bytes = await pipe.get(comp_key)
                
                if comp_bytes is None:
                    logger.warning(f"No compensation data for saga {result.saga_id}, cannot rollback stock. THIS IS BAD.")
                    msg.ack()
                    return

                comp: StockCompensation = msgpack.decode(comp_bytes, type=StockCompensation)
                
                item_entries = {}
                for item_id in comp.deltas:
                    item_entry = await get_item_from_db(item_id)
                    if item_entry is not None:
                        item_entries[item_id] = item_entry

                pipe.multi()
                for item_id, delta in comp.deltas.items():
                    if item_id in item_entries:
                        item_entries[item_id].stock -= delta
                        pipe.set(item_id, msgpack.encode(item_entries[item_id]))
                pipe.delete(comp_key)
                
                await pipe.execute()
                msg.ack()

        except WatchError:
            logger.error(f"Rollback failed for saga {result.saga_id}: WatchError")
        except (ValueError, RedisError) as e:
            logger.error(f"Rollback failed for saga {result.saga_id}: {e}")