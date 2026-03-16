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

# KEYS: [item_id_1, item_id_2, ..., saga_compensation_key, saga_commit_key]
# ARGV: [qty_1, qty_2, ..., <same count as items>, encoded_req, order_id, num_items]
#
# Layout:
#   KEYS[1..num_items]             = item keys (Redis Hashes with 'stock' and 'price' fields)
#   KEYS[num_items+1]              = saga_compensation_key
#   KEYS[num_items+2]              = saga_commit_key
#   ARGV[1..num_items]             = quantities to deduct per item
#   ARGV[num_items+1]              = encoded CheckoutRequest bytes
#   ARGV[num_items+2]              = order_id
#   ARGV[num_items+3]              = num_items (so Lua knows the split point)
DEDUCT_STOCK_SCRIPT = db.register_script("""
local num_items = tonumber(ARGV[#ARGV])
local comp_key  = KEYS[num_items + 1]
local commit_key = KEYS[num_items + 2]

-- Pass 1: validate all items before touching anything
for i = 1, num_items do
    local stock = tonumber(redis.call('HGET', KEYS[i], 'stock'))
    if stock == nil then
        return {-2, i}  -- item not found, return its index
    end
    local qty = tonumber(ARGV[i])
    if stock < qty then
        return {-1, i}  -- insufficient stock, return its index
    end
end

-- Pass 2: all checks passed — deduct and record previous stock in compensation hash
for i = 1, num_items do
    local stock = tonumber(redis.call('HGET', KEYS[i], 'stock'))
    local qty   = tonumber(ARGV[i])
    redis.call('HSET', KEYS[i], 'stock', stock - qty)
    redis.call('HSET', comp_key, KEYS[i], stock)  -- previous stock per item_id
end

redis.call('HSET', comp_key, '__order_id__', ARGV[num_items + 2])
redis.call('SET',  commit_key, ARGV[num_items + 1])
return {0, 0}
""")

nc: nats.NATS | None = None
js = None
logger = None

def _saga_compensation_key(saga_id: str) -> str:
    return f"saga:compensation:{saga_id}"


def _saga_commit_key(saga_id: str) -> str:
    return f"saga:commit:{saga_id}"


def _saga_outbox_key(saga_id: str) -> str:
    return f"saga:outbox:{saga_id}"


# ---------------------------------------------------------------------------
# Hash helpers
# ---------------------------------------------------------------------------

def _stock_value_to_mapping(sv: StockValue) -> dict[str, int]:
    """Convert a StockValue to a flat dict suitable for HSET."""
    return {"stock": sv.stock, "price": sv.price}


def _mapping_to_stock_value(mapping: dict) -> StockValue | None:
    """Convert a Redis HGETALL result (bytes keys/values) to StockValue."""
    if not mapping:
        return None
    return StockValue(
        stock=int(mapping[b"stock"]),
        price=int(mapping[b"price"]),
    )

class StockCompensation(Struct):
    order_id: str
    deltas: dict[str, int]

async def get_item_from_db(item_id: str) -> StockValue | None:
    try:
        mapping = await db.hgetall(item_id)
    except RedisError as e:
        logger.error(f"DB error fetching item {item_id}: {e}")
        return None
    entry = _mapping_to_stock_value(mapping)
    if entry is None:
        logger.warning(f"Item: {item_id} not found!")
    return entry


async def _set_item_in_db(pipe_or_db, item_id: str, sv: StockValue):
    """Store a StockValue as a Redis Hash. Works with both db and pipeline."""
    await pipe_or_db.hset(item_id, mapping=_stock_value_to_mapping(sv))

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


# ---------------------------------------------------------------------------
# Handlers
# ---------------------------------------------------------------------------

async def handle_checkout_stock(msg):
    req: CheckoutRequest = msgpack.decode(msg.data, type=CheckoutRequest)
    commit_bytes = await db.get(_saga_commit_key(req.saga_id))
    if commit_bytes:
        result: CheckoutResult = msgpack.decode(commit_bytes, type=CheckoutResult)
        logger.info(f"Duplicate checkout.stock for saga {req.saga_id}, republishing stored result: success={result.success}")
        await publish_stock_result(req, result.success, result.error)
        await msg.ack()
        return

    try:
        await db.set(_saga_started_key(req.saga_id), req.order_id)
    except RedisError as e:
        await js.publish(
            "stock.result",
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

    # Build keys and args for the Lua script:
    #   KEYS = [item_id_1, ..., item_id_n, compensation_key, commit_key]
    #   ARGV = [qty_1, ..., qty_n, encoded_req, order_id, n]
    items = list(req.items.items())  # stable ordering
    item_keys = [item_id for item_id, _ in items]
    quantities = [qty for _, qty in items]
    num_items = len(items)

    keys = item_keys + [
        _saga_compensation_key(req.saga_id),
        _saga_commit_key(req.saga_id),
    ]
    args = quantities + [
        msgpack.encode(req),
        req.order_id,
        num_items,
    ]

    try:
        result = await DEDUCT_STOCK_SCRIPT(keys=keys, args=args)
    except RedisError as e:
        await db.delete(_saga_started_key(req.saga_id))
        await js.publish(
            "stock.result",
            msgpack.encode(CheckoutResult(
                saga_id=req.saga_id,
                message_id=str(uuid.uuid4()),
                request_id=req.request_id,
                order_id=req.order_id,
                success=False,
                error=str(e),
            )),
        )
        await db.set(_saga_commit_key(req.saga_id), msgpack.encode(result))
        await publish_stock_result(req, result.success, result.error)
        await msg.ack()
        return

    status, index = result[0], result[1]

    if status == -2:
        # index is 1-based from Lua
        item_id = item_keys[index - 1]
        await db.delete(_saga_started_key(req.saga_id))
        await js.publish(
            "stock.result",
            msgpack.encode(CheckoutResult(
                saga_id=req.saga_id,
                message_id=str(uuid.uuid4()),
                request_id=req.request_id,
                order_id=req.order_id,
                success=False,
                error=f"Item: {item_id} not found!",
            )),
        )
        return

    if status == -1:
        item_id = item_keys[index - 1]
        await db.delete(_saga_started_key(req.saga_id))
        await js.publish(
            "stock.result",
            msgpack.encode(CheckoutResult(
                saga_id=req.saga_id,
                message_id=str(uuid.uuid4()),
                request_id=req.request_id,
                order_id=req.order_id,
                success=False,
                error=f"Item: {item_id} stock cannot get reduced below zero!",
            )),
        )
        return

    # status == 0: all items deducted, compensation hash and commit key written atomically
    try:
        await js.publish(
            "stock.result",
            msgpack.encode(CheckoutResult(
                saga_id=req.saga_id,
                message_id=str(uuid.uuid4()),
                request_id=req.request_id,
                order_id=req.order_id,
                success=True,
                error="",
            )),
        )
        await db.set(_saga_outbox_key(req.saga_id), b"1")
    except Exception as e:
        logger.error(f"Failed to publish stock.result for saga {req.saga_id}: {e}")


async def _rollback_stock_items(saga_id: str):
    comp_key = _saga_compensation_key(saga_id)
    try:
        mapping = await db.hgetall(comp_key)
        if not mapping:
            logger.warning(f"No compensation data for saga {saga_id}, cannot rollback stock")
            return
        for raw_key, raw_value in mapping.items():
            item_id = raw_key.decode()
            if item_id == "__order_id__":
                continue
            previous_stock = int(raw_value)
            try:
                item_entry = await get_item_from_db(item_id)
                if item_entry is not None:
                    item_entry.stock = previous_stock
                    await db.hset(item_id, mapping=_stock_value_to_mapping(item_entry))
            except Exception as e:
                logger.error(f"Rollback failed for item {item_id}: {e}")
    except RedisError as e:
        logger.error(f"Failed to read compensation for saga {saga_id}: {e}")


async def handle_create_item(msg):
    try:
        req: StockCreateItemRequest = msgpack.decode(msg.data, type=StockCreateItemRequest)
    except Exception as e:
        logger.error(f"Failed to decode create item message: {e}")
        return

    key = str(uuid.uuid4())
    sv = StockValue(stock=0, price=req.price)

    try:
        await db.hset(key, mapping=_stock_value_to_mapping(sv))
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

    sv = StockValue(stock=req.starting_stock, price=req.item_price)
    mapping = _stock_value_to_mapping(sv)

    try:
        pipe = db.pipeline(transaction=False)  # non-transactional for speed
        for i in range(req.n):
            pipe.hset(str(i), mapping=mapping)
        await pipe.execute()
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
        await db.hset(req.item_id, mapping=_stock_value_to_mapping(item_entry))
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
        await db.hset(req.item_id, mapping=_stock_value_to_mapping(item_entry))
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
                    await pipe.aclose()
                    logger.warning(f"No compensation data for saga {result.saga_id}, cannot rollback stock. THIS IS BAD.")
                    await msg.ack()
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
                await pipe.aclose()
                await msg.ack()

        except WatchError:
            await pipe.aclose()
            logger.error(f"Rollback failed for saga {result.saga_id}: WatchError")
        except (ValueError, RedisError) as e:
            await pipe.aclose()
            logger.error(f"Rollback failed for saga {result.saga_id}: {e}")
