import asyncio
import logging
import os
import random
import uuid
from collections import defaultdict

import nats
from msgspec import Struct, msgpack
from redis.asyncio import Redis
from redis.exceptions import RedisError

import hashlib

from common.messages import *


DB_ERROR_STR = "DB error"

NATS_URL = os.environ["NATS_URL"]
MESSAGE_TIMEOUT = 30.0


# Redis sharding setup
REDIS_SHARDS = os.environ.get("REDIS_SHARDS", "order-db-0:6379,order-db-1:6379").split(",")
NUM_SHARDS = len(REDIS_SHARDS)
redis_connections = [
    Redis(host=host.split(":")[0], port=int(host.split(":")[1]), password="redis", db=0)
    for host in REDIS_SHARDS
]

# uuids are mapped to shard deterministically using a hash function to ensure the same order_id
# always maps to the same shard, even across restarts
def get_redis_for_order(order_id: str) -> Redis:
    h = int(hashlib.sha256(order_id.encode()).hexdigest(), 16)
    shard_num = h % NUM_SHARDS
    return redis_connections[shard_num]

nc: nats.NATS | None = None
js = None
logger = None

def _saga_compensation_key(saga_id: str) -> str:
    return f"saga:compensation:{saga_id}"


def _saga_commit_key(saga_id: str) -> str:
    return f"saga:commit:{saga_id}"

class OrderCompensation(Struct):
    order_id: str
    original_paid: bool

async def get_stock_item(item_id: str) -> StockFindItemResult:
    request_id = str(uuid.uuid4())
    message = StockFindItemRequest(
        message_id=str(uuid.uuid4()),
        request_id=request_id,
        item_id=item_id,
    )
    reply_subject = f"inbox.{request_id}"
    sub = await js.subscribe(reply_subject)
    try:
        await js.publish("stock.find", msgpack.encode(message))
        response_msg = await sub.next_msg(timeout=MESSAGE_TIMEOUT)
        response = msgpack.decode(response_msg.data, type=StockFindItemResult)
        if response.request_id != request_id:
            raise ValueError("Correlation mismatch for stock.find")
        return response
    finally:
        await sub.unsubscribe()


async def get_order_from_db(order_id: str) -> OrderValue | None:
    db = get_redis_for_order(order_id)
    try:
        entry: bytes = await db.get(order_id)
    except RedisError as e:
        logger.error(f"DB error fetching order {order_id}: {e}")
        return None
    entry: OrderValue | None = msgpack.decode(entry, type=OrderValue) if entry else None
    if entry is None:
        logger.warning(f"Order: {order_id} not found!")
        return None
    return entry


async def ensure_stream():
    for stream_name, subjects in [
        ("CHECKOUT", ["checkout.>"]),
        ("ORDER", ["order.>"]),
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


async def _publish_checkout_gateway_result(result: CheckoutResult):
    if result.request_id:
        gateway_result = CheckoutResult(
            saga_id=result.saga_id,
            message_id=str(uuid.uuid4()),
            request_id=result.request_id,
            order_id=result.order_id,
            success=result.success,
            error=result.error,
        )
        await publish_reply(result.request_id, gateway_result)


async def handle_checkout_order(msg):
    checkout_order: CheckoutOrderRequest = msgpack.decode(msg.data, type=CheckoutOrderRequest)
    order_id = checkout_order.order_id
    logger.debug(f"Checking out {order_id}")

    db = get_redis_for_order(order_id)

    commit_bytes = await db.get(_saga_commit_key(order_id))
    if commit_bytes:
        commit_data = msgpack.decode(commit_bytes)
        logger.info(f"Duplicate checkout.order for order {order_id}, republishing stored event: success={commit_data['success']}")
        if commit_data["success"]:
            await js.publish("checkout.payment", msgpack.encode(commit_data["data"]))
        else:
            result = commit_data["data"]
            await publish_reply(checkout_order.request_id, result)
        await msg.ack()
        return
    
    saga_id = str(uuid.uuid4())

    entry: bytes = await db.get(order_id)
    if not entry:
        result = CheckoutResult(
            saga_id=saga_id,
            message_id=str(uuid.uuid4()),
            request_id=checkout_order.request_id,
            order_id=order_id,
            success=False,
            error=f"Order {order_id} not found",
        )
        commit_data = {
            "success": False,
            "data": result,
        }
        await db.set(_saga_commit_key(order_id), msgpack.encode(commit_data))
        await publish_reply(checkout_order.request_id, result)
        await msg.ack()
        return

    order_entry: OrderValue = msgpack.decode(entry, type=OrderValue)
    items_quantities: dict[str, int] = defaultdict(int)
    for item_id, quantity in order_entry.items:
        items_quantities[item_id] += quantity

    checkout_req = CheckoutRequest(
        saga_id=saga_id,
        message_id=str(uuid.uuid4()),
        request_id=checkout_order.request_id,
        order_id=order_id,
        user_id=order_entry.user_id,
        total_cost=order_entry.total_cost,
        items=dict(items_quantities),
    )

    compensation = OrderCompensation(order_id=order_id, original_paid=order_entry.paid)
    order_entry.paid = True
    try:
        pipe = db.pipeline()
        pipe.multi()
        pipe.set(_saga_compensation_key(saga_id), msgpack.encode(compensation))
        pipe.set(order_id, msgpack.encode(order_entry))
        commit_data = {
            "success": True,
            "data": checkout_req,
        }
        pipe.set(_saga_commit_key(saga_id), msgpack.encode(commit_data))
        await pipe.execute()
        await pipe.aclose()
        await js.publish("checkout.payment", msgpack.encode(checkout_req))
        await msg.ack()
        return  # transaction committed — exit
    except RedisError as e:
        await pipe.aclose()
        result = CheckoutResult(
            saga_id=saga_id,
            message_id=str(uuid.uuid4()),
            request_id=checkout_order.request_id,
            order_id=order_id,
            success=False,
            error=f"{DB_ERROR_STR}: {e}",
        )
        commit_data = {
            "success": False,
            "data": result,
        }
        await db.set(_saga_commit_key(order_id), msgpack.encode(commit_data))
        await publish_reply(checkout_order.request_id, result)
        await msg.ack()
        return


async def handle_payment_result(msg):
    result: CheckoutResult = msgpack.decode(msg.data, type=CheckoutResult)
    # don't care about payment success ack (we will get stock result later)
    if not result.success:
        logger.warning(f"Payment failed for saga {result.saga_id}, order {result.order_id}: {result.error}")
        comp_key = _saga_compensation_key(result.saga_id)
        try:
            db = get_redis_for_order(result.order_id)
            comp_bytes = await db.get(comp_key)
            if comp_bytes is None:
                logger.warning(f"No compensation data for saga {result.saga_id}, cannot rollback order. THIS IS BAD.")
            else:
                comp: OrderCompensation = msgpack.decode(comp_bytes, type=OrderCompensation)
                db_comp = get_redis_for_order(comp.order_id)
                entry_bytes: bytes = await db_comp.get(comp.order_id)
                if entry_bytes:
                    order_entry: OrderValue = msgpack.decode(entry_bytes, type=OrderValue)
                    order_entry.paid = comp.original_paid
                    await db_comp.set(comp.order_id, msgpack.encode(order_entry))
                    logger.info(f"Order {comp.order_id} rolled back to paid={comp.original_paid} for saga {result.saga_id}")
                    await msg.ack()
            await _publish_checkout_gateway_result(result)
        # redis error - will retry later
        except (ValueError, RedisError) as e:
            logger.error(f"Rollback failed for saga {result.saga_id}: {e}")
        return

async def handle_stock_result(msg):
    result: CheckoutResult = msgpack.decode(msg.data, type=CheckoutResult)

    if result.success:
        logger.info(f"Stock succeeded for saga {result.saga_id}, order {result.order_id}")
        await _publish_checkout_gateway_result(result)
        await msg.ack()
        return

    logger.warning(f"Stock checkout failed for saga {result.saga_id}, order {result.order_id}: {result.error}")
    comp_key = _saga_compensation_key(result.saga_id)
    try:
        db = get_redis_for_order(result.order_id)
        comp_bytes = await db.get(comp_key)
        if comp_bytes is None:
            logger.warning(f"No compensation data for saga {result.saga_id}, cannot rollback order. THIS IS BAD.")
        else:
            comp: OrderCompensation = msgpack.decode(comp_bytes, type=OrderCompensation)
            db_comp = get_redis_for_order(comp.order_id)
            entry_bytes: bytes = await db_comp.get(comp.order_id)
            if entry_bytes:
                order_entry: OrderValue = msgpack.decode(entry_bytes, type=OrderValue)
                order_entry.paid = comp.original_paid
                await db_comp.set(comp.order_id, msgpack.encode(order_entry))
                logger.info(f"Order {comp.order_id} rolled back to paid={comp.original_paid} for saga {result.saga_id}")
                await msg.ack()
        await _publish_checkout_gateway_result(result)
    # redis error - will retry later
    except (ValueError, RedisError) as e:
        logger.error(f"Rollback failed for saga {result.saga_id}: {e}")

async def handle_order_create(msg):
    try:
        req: OrderCreateRequest = msgpack.decode(msg.data, type=OrderCreateRequest)
    except Exception as e:
        logger.error(f"Failed to decode order create message: {e}")
        return

    request_id = req.request_id
    key = str(uuid.uuid4())
    value = msgpack.encode(OrderValue(paid=False, items=[], user_id=req.user_id, total_cost=0))

    db = get_redis_for_order(key)
    try:
        await db.set(key, value)
        result = OrderCreateResult(message_id=str(uuid.uuid4()), request_id=request_id, order_id=key, error="")
        await publish_reply(request_id, result)
    except RedisError:
        result = OrderCreateResult(message_id=str(uuid.uuid4()), request_id=request_id, order_id="", error=DB_ERROR_STR)
        await publish_reply(request_id, result)


async def handle_order_batch_init(msg):
    try:
        req: OrderBatchInitRequest = msgpack.decode(msg.data, type=OrderBatchInitRequest)
    except Exception as e:
        logger.error(f"Failed to decode batch init message: {e}")
        return

    request_id = req.request_id

    def generate_entry() -> OrderValue:
        user_id = random.randint(0, req.n_users - 1)
        item1_id = random.randint(0, req.n_items - 1)
        item2_id = random.randint(0, req.n_items - 1)
        return OrderValue(
            paid=False,
            items=[(f"{item1_id}", 1), (f"{item2_id}", 1)],
            user_id=f"{user_id}",
            total_cost=2 * req.item_price,
        )

    kv_pairs: dict[str, bytes] = {f"{i}": msgpack.encode(generate_entry()) for i in range(req.n)}
    # Partition batch by order_id
    for order_id, value in kv_pairs.items():
        db = get_redis_for_order(order_id)
        try:
            await db.set(order_id, value)
        except RedisError:
            result = OrderBatchInitResult(message_id=str(uuid.uuid4()), request_id=request_id, success=False, error=DB_ERROR_STR)
            await publish_reply(request_id, result)
            return
    result = OrderBatchInitResult(message_id=str(uuid.uuid4()), request_id=request_id, success=True, error="")
    await publish_reply(request_id, result)


async def handle_order_find(msg):
    try:
        req: OrderFindRequest = msgpack.decode(msg.data, type=OrderFindRequest)
    except Exception as e:
        logger.error(f"Failed to decode order find message: {e}")
        return

    order_entry = await get_order_from_db(req.order_id)
    result = OrderFindResult(
        message_id=str(uuid.uuid4()),
        request_id=req.request_id,
        order_id=req.order_id,
        order=order_entry,
        error="" if order_entry else f"Order: {req.order_id} not found!",
    )
    await publish_reply(req.request_id, result)


async def handle_order_add_item(msg):
    try:
        req: OrderAddItemRequest = msgpack.decode(msg.data, type=OrderAddItemRequest)
    except Exception as e:
        logger.error(f"Failed to decode order add item message: {e}")
        return

    order_entry = await get_order_from_db(req.order_id)
    if order_entry is None:
        result = OrderAddItemResult(
            message_id=str(uuid.uuid4()),
            request_id=req.request_id,
            order_id=req.order_id,
            total_cost=0,
            error=f"Order: {req.order_id} not found!",
        )
        await publish_reply(req.request_id, result)
        return

    try:
        stock_result = await get_stock_item(req.item_id)
    except Exception:
        result = OrderAddItemResult(
            message_id=str(uuid.uuid4()),
            request_id=req.request_id,
            order_id=req.order_id,
            total_cost=0,
            error="Stock service timeout",
        )
        await publish_reply(req.request_id, result)
        return

    if stock_result.error or stock_result.item is None:
        result = OrderAddItemResult(
            message_id=str(uuid.uuid4()),
            request_id=req.request_id,
            order_id=req.order_id,
            total_cost=0,
            error=f"Item: {req.item_id} does not exist!",
        )
        await publish_reply(req.request_id, result)
        return

    order_entry.items.append((req.item_id, req.quantity))
    order_entry.total_cost += req.quantity * stock_result.item.price
    db = get_redis_for_order(req.order_id)
    try:
        await db.set(req.order_id, msgpack.encode(order_entry))
        result = OrderAddItemResult(
            message_id=str(uuid.uuid4()),
            request_id=req.request_id,
            order_id=req.order_id,
            total_cost=order_entry.total_cost,
            error="",
        )
        await publish_reply(req.request_id, result)
    except RedisError:
        result = OrderAddItemResult(
            message_id=str(uuid.uuid4()),
            request_id=req.request_id,
            order_id=req.order_id,
            total_cost=0,
            error=DB_ERROR_STR,
        )
        await publish_reply(req.request_id, result)


async def startup():
    global nc, js, logger
    logger = logging.getLogger("order-service")
    nc = await nats.connect(NATS_URL)
    js = nc.jetstream()
    await ensure_stream()

    await js.subscribe("order.create", durable="order-create", queue="order-create", cb=handle_order_create)
    await js.subscribe("order.batch_init", durable="order-batch-init", queue="order-batch-init", cb=handle_order_batch_init)
    await js.subscribe("order.find", durable="order-find", queue="order-find", cb=handle_order_find)
    await js.subscribe("order.add_item", durable="order-add-item", queue="order-add-item", cb=handle_order_add_item)
    await js.subscribe("checkout.order", durable="checkout-order", queue="checkout-order", cb=handle_checkout_order)
    await js.subscribe("payment.result", durable="order-payment-result", queue="order-payment-result", cb=handle_payment_result)
    await js.subscribe("stock.result", durable="order-stock-result", queue="order-stock-result", cb=handle_stock_result)


async def shutdown():
    await nc.drain()
    for db in redis_connections:
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
