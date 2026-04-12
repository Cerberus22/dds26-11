import asyncio
import hashlib
import logging
import os
import uuid
from collections import defaultdict
import nats
from nats.js.api import StorageType
from msgspec import Struct, msgpack
from redis.asyncio import Redis
from redis.exceptions import RedisError

from common.messages import *


DB_ERROR_STR = "DB error"

NATS_URL = os.environ["NATS_URL"]


# Redis sharding setup
REDIS_SHARDS = os.environ.get("REDIS_SHARDS", "stock-db-0:6379,stock-db-1:6379,stock-db-2:6379").split(",")
NUM_SHARDS = len(REDIS_SHARDS)
redis_connections = [
    Redis(host=host.split(":")[0], port=int(host.split(":")[1]), password="redis", db=0)
    for host in REDIS_SHARDS
]


def get_shard_idx(key: str) -> int:
    return int(hashlib.sha256(key.encode()).hexdigest(), 16) % NUM_SHARDS


def get_redis_for_item(item_id: str) -> Redis:
    return redis_connections[get_shard_idx(item_id)]


# Compensation and commit keys for a saga always go to one shard derived from saga_id,
# so they are never split across nodes regardless of which item shards are involved
def get_redis_for_saga(saga_id: str) -> Redis:
    return redis_connections[get_shard_idx(saga_id)]


# Per-shard deduct: checks the per-shard commit key first (idempotency), validates all
# items, deducts, and records the shard commit atomically.
# KEYS: [item_id_1, ..., item_id_n, shard_commit_key]
# ARGV: [qty_1, ..., qty_n, n]
# Returns: {1, 0} already committed | {0, 0} success | {-2, i} item i not found | {-1, i} item i insufficient
DEDUCT_STOCK_LUA = """
local num_items  = tonumber(ARGV[#ARGV])
local commit_key = KEYS[num_items + 1]

if redis.call('EXISTS', commit_key) == 1 then
    return {1, 0}
end

for i = 1, num_items do
    local stock = tonumber(redis.call('HGET', KEYS[i], 'stock'))
    if stock == nil then return {-2, i} end
    if stock < tonumber(ARGV[i]) then return {-1, i} end
end

for i = 1, num_items do
    local stock = tonumber(redis.call('HGET', KEYS[i], 'stock'))
    redis.call('HSET', KEYS[i], 'stock', stock - tonumber(ARGV[i]))
    redis.call('HSET', commit_key, KEYS[i], ARGV[i])
end

return {0, 0}
"""

# Per-shard rollback: adds quantities back and deletes the shard commit key atomically.
# KEYS: [item_id_1, ..., item_id_n, shard_commit_key]
# ARGV: [qty_1, ..., qty_n, n]
ROLLBACK_STOCK_LUA = """
local num_items  = tonumber(ARGV[#ARGV])
local commit_key = KEYS[num_items + 1]

for i = 1, num_items do
    local stock = tonumber(redis.call('HGET', KEYS[i], 'stock'))
    if stock ~= nil then
        redis.call('HSET', KEYS[i], 'stock', stock + tonumber(ARGV[i]))
    end
end

redis.call('DEL', commit_key)
return 1
"""

# Registered at startup against a single connection; called with client= to target the right shard
_deduct_stock_script = None
_rollback_stock_script = None

nc: nats.NATS | None = None
js = None
logger = None


def _saga_commit_key(saga_id: str) -> str:
    return f"saga:commit:{saga_id}"


# Written on each item shard alongside the deduction — ensures a retry skips already-done shards
def _saga_shard_commit_key(saga_id: str) -> str:
    return f"saga:shard_commit:{saga_id}"


def _stock_value_to_mapping(sv: StockValue) -> dict[str, int]:
    return {"stock": sv.stock, "price": sv.price}


def _mapping_to_stock_value(mapping: dict) -> StockValue | None:
    if not mapping:
        return None
    return StockValue(
        stock=int(mapping[b"stock"]),
        price=int(mapping[b"price"]),
    )


async def get_item_from_db(item_id: str) -> StockValue | None:
    db = get_redis_for_item(item_id)
    try:
        mapping = await db.hgetall(item_id)
    except RedisError as e:
        logger.error(f"DB error fetching item {item_id}: {e}")
        return None
    entry = _mapping_to_stock_value(mapping)
    if entry is None:
        logger.warning(f"Item: {item_id} not found!")
    return entry


async def ensure_stream():
    for stream_name, subjects in [
        ("CHECKOUT", ["checkout.>"]),
        ("STOCK", ["stock.>"]),
    ]:
        try:
            await js.add_stream(name=stream_name, subjects=subjects,
                                max_msgs=500_000, storage=StorageType.MEMORY)
        except nats.js.errors.BadRequestError:
            pass  # stream already exists
        except Exception as e:
            logger.error(f"Failed to create stream {stream_name}: {e}")
            raise


async def publish_reply(request_id: str, response):
    await nc.publish(f"inbox.{request_id}", msgpack.encode(response))


async def _rollback_completed(saga_id: str, completed: list[tuple[int, list[tuple[str, int]]]]):
    """Roll back all per-shard deductions that already succeeded, deleting their shard commit keys."""
    shard_commit_key = _saga_shard_commit_key(saga_id)
    for shard_idx, shard_items in completed:
        db = redis_connections[shard_idx]
        item_keys = [item_id for item_id, _ in shard_items]
        quantities = [qty for _, qty in shard_items]
        try:
            await _rollback_stock_script(
                keys=item_keys + [shard_commit_key],
                args=quantities + [len(shard_items)],
                client=db,
            )
        except RedisError as e:
            logger.error(f"Rollback failed for shard {shard_idx}: {e}")


# ---------------------------------------------------------------------------
# Handlers
# ---------------------------------------------------------------------------

async def handle_checkout_stock(msg):
    req: CheckoutRequest = msgpack.decode(msg.data, type=CheckoutRequest)

    comp_shard = get_redis_for_saga(req.saga_id)
    commit_key = _saga_commit_key(req.saga_id)
    shard_commit_key = _saga_shard_commit_key(req.saga_id)

    # Saga-level idempotency: if the saga already fully committed, republish the stored result
    try:
        commit_val = await comp_shard.get(commit_key)
    except RedisError as e:
        logger.error(f"Redis unavailable for checkout stock saga={req.saga_id}: {e}")
        await msg.nak(delay=min(2 ** msg.metadata.num_delivered, 30))
        return
    if commit_val is not None:
        status = int(commit_val)
        logger.info(f"Duplicate checkout.stock for saga {req.saga_id}, status={status}")
        error = "" if status == 0 else ("Item not found" if status == -2 else "Insufficient stock")
        await js.publish(
            "stock.result",
            msgpack.encode(CheckoutResult(
                saga_id=req.saga_id,
                message_id=str(uuid.uuid4()),
                request_id=req.request_id,
                order_id=req.order_id,
                success=status == 0,
                error=error,
                user_id=req.user_id,
            )),
        )
        await msg.ack()
        return

    # Group items by which shard their item_id hashes to
    items_by_shard: dict[int, list[tuple[str, int]]] = defaultdict(list)
    for item_id, qty in req.items.items():
        items_by_shard[get_shard_idx(item_id)].append((item_id, qty))

    # Sequential per-shard writes: if any shard fails, roll back all completed shards.
    # The Lua script writes a per-shard commit key atomically with the deduction, so
    # a retry (status == 1) safely skips shards that already committed.
    completed: list[tuple[int, list[tuple[str, int]]]] = []
    all_deltas: dict[str, int] = {}

    for shard_idx, shard_items in items_by_shard.items():
        db = redis_connections[shard_idx]
        item_keys = [item_id for item_id, _ in shard_items]
        quantities = [qty for _, qty in shard_items]

        try:
            result = await _deduct_stock_script(
                keys=item_keys + [shard_commit_key],
                args=quantities + [len(shard_items)],
                client=db,
            )
        except RedisError as e:
            await _rollback_completed(req.saga_id, completed)
            await msg.nak(delay=min(2 ** msg.metadata.num_delivered, 30))
            return

        status, index = result[0], result[1]

        if status == 1:
            # This shard already committed in a previous attempt — skip it
            logger.info(f"Shard {shard_idx} already committed for saga {req.saga_id}, skipping")
            completed.append((shard_idx, shard_items))
            for item_id, qty in shard_items:
                all_deltas[item_id] = qty
            continue

        if status != 0:
            await _rollback_completed(req.saga_id, completed)
            item_id = item_keys[index - 1]
            error = f"Item: {item_id} not found!" if status == -2 else f"Item: {item_id} stock cannot get reduced below zero!"
            await comp_shard.set(commit_key, status)
            await js.publish(
                "stock.result",
                msgpack.encode(CheckoutResult(
                    saga_id=req.saga_id,
                    message_id=str(uuid.uuid4()),
                    request_id=req.request_id,
                    order_id=req.order_id,
                    success=False,
                    error=error,
                    user_id=req.user_id,
                )),
            )
            await msg.ack()
            return

        completed.append((shard_idx, shard_items))
        for item_id, qty in shard_items:
            all_deltas[item_id] = qty

    # All shards succeeded — write saga commit key to the saga shard
    try:
        await comp_shard.set(commit_key, 0)
    except RedisError as e:
        # Compensation write failed — roll back all item shards
        await _rollback_completed(req.saga_id, completed)
        await msg.nak(delay=min(2 ** msg.metadata.num_delivered, 30))
        return

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
                user_id=req.user_id,
            )),
        )
        await msg.ack()
    except Exception as e:
        logger.error(f"Failed to publish stock.result for saga {req.saga_id}: {e}")


async def handle_create_item(msg):
    try:
        req: StockCreateItemRequest = msgpack.decode(msg.data, type=StockCreateItemRequest)
    except Exception as e:
        logger.error(f"Failed to decode create item message: {e}")
        await msg.ack()
        return

    key = str(uuid.uuid4())
    db = get_redis_for_item(key)
    sv = StockValue(stock=0, price=req.price)

    try:
        await db.hset(key, mapping=_stock_value_to_mapping(sv))
        result = StockCreateItemResult(message_id=str(uuid.uuid4()), request_id=req.request_id, item_id=key, error="")
        await publish_reply(req.request_id, result)
    except RedisError as e:
        logger.error(f"Redis unavailable for create item {key}: {e}")
        await msg.nak(delay=min(2 ** msg.metadata.num_delivered, 30))
        return
    await msg.ack()


async def handle_batch_init_items(msg):
    try:
        req: StockBatchInitRequest = msgpack.decode(msg.data, type=StockBatchInitRequest)
    except Exception as e:
        logger.error(f"Failed to decode batch init message: {e}")
        await msg.ack()
        return

    sv = StockValue(stock=req.starting_stock, price=req.item_price)
    mapping = _stock_value_to_mapping(sv)

    # Group items by shard and pipeline writes per shard
    items_by_shard: dict[int, list[str]] = defaultdict(list)
    for i in range(req.n):
        item_id = str(i)
        items_by_shard[get_shard_idx(item_id)].append(item_id)

    for shard_idx, item_ids in items_by_shard.items():
        db = redis_connections[shard_idx]
        try:
            pipe = db.pipeline(transaction=False)
            for item_id in item_ids:
                pipe.hset(item_id, mapping=mapping)
            await pipe.execute()
        except RedisError as e:
            logger.error(f"Redis unavailable for batch init stock shard {shard_idx}: {e}")
            await msg.nak(delay=min(2 ** msg.metadata.num_delivered, 30))
            return

    result = StockBatchInitResult(message_id=str(uuid.uuid4()), request_id=req.request_id, success=True, error="")
    await publish_reply(req.request_id, result)
    await msg.ack()

async def handle_find_item(msg):
    try:
        req: StockFindItemRequest = msgpack.decode(msg.data, type=StockFindItemRequest)
    except Exception as e:
        logger.error(f"Failed to decode find item message: {e}")
        await msg.ack()
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
    await msg.ack()


async def handle_add_amount(msg):
    try:
        req: StockAddAmountRequest = msgpack.decode(msg.data, type=StockAddAmountRequest)
    except Exception as e:
        logger.error(f"Failed to decode add amount message: {e}")
        await msg.ack()
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
        await msg.ack()
        return

    db = get_redis_for_item(req.item_id)
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
    except RedisError as e:
        logger.error(f"Redis unavailable for add amount item={req.item_id}: {e}")
        await msg.nak(delay=min(2 ** msg.metadata.num_delivered, 30))
        return
    await msg.ack()


async def handle_subtract_amount(msg):
    try:
        req: StockSubtractAmountRequest = msgpack.decode(msg.data, type=StockSubtractAmountRequest)
    except Exception as e:
        logger.error(f"Failed to decode subtract amount message: {e}")
        await msg.ack()
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
        await msg.ack()
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
        await msg.ack()
        return

    db = get_redis_for_item(req.item_id)
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
    except RedisError as e:
        logger.error(f"Redis unavailable for subtract amount item={req.item_id}: {e}")
        await msg.nak(delay=min(2 ** msg.metadata.num_delivered, 30))
        return
    await msg.ack()


async def startup():
    global nc, js, logger, _deduct_stock_script, _rollback_stock_script
    logger = logging.getLogger("stock-service")
    nc = await nats.connect(NATS_URL)
    js = nc.jetstream()
    await ensure_stream()

    _deduct_stock_script = redis_connections[0].register_script(DEDUCT_STOCK_LUA)
    _rollback_stock_script = redis_connections[0].register_script(ROLLBACK_STOCK_LUA)
    
    await js.subscribe("stock.create_item", durable="stock-create-item", queue="stock-create-item", cb=handle_create_item, manual_ack=True)
    await js.subscribe("stock.batch_init", durable="stock-batch-init", queue="stock-batch-init", cb=handle_batch_init_items, manual_ack=True)
    await js.subscribe("stock.find", durable="stock-find", queue="stock-find", cb=handle_find_item, manual_ack=True)
    await js.subscribe("stock.add", durable="stock-add", queue="stock-add", cb=handle_add_amount, manual_ack=True)
    await js.subscribe("stock.subtract", durable="stock-subtract", queue="stock-subtract", cb=handle_subtract_amount, manual_ack=True)

    await js.subscribe("checkout.stock", durable="stock-checkout", queue="stock-checkout", cb=handle_checkout_stock, manual_ack=True)


async def shutdown():
    await nc.drain()
    for db in redis_connections:
        await db.aclose()


async def main():
    logging.basicConfig(level=logging.WARNING, format="%(asctime)s:%(levelname)s:%(name)s:%(message)s")
    await startup()
    await asyncio.sleep(float("inf"))


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Shutting down...")
else:
    logging.basicConfig(level=logging.WARNING, format="%(asctime)s:%(levelname)s:%(name)s:%(message)s")
