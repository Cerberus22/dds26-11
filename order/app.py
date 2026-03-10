import logging
import os
import random
import uuid
from collections import defaultdict

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
MESSAGE_TIMEOUT = 30.0


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
    try:
        # get serialized data
        entry: bytes = await db.get(order_id)
    except RedisError as e:
        logger.error(f"DB error fetching order {order_id}: {e}")
        return None
    # deserialize data if it exists else return null
    entry: OrderValue | None = msgpack.decode(entry, type=OrderValue) if entry else None
    if entry is None:
        logger.warning(f"Order: {order_id} not found!")
        return None
    return entry


async def ensure_stream():
    try:
        await js.add_stream(name="MESSAGES", subjects=MESSAGE_STREAM_SUBJECTS)
    except Exception:
        pass  # stream already exists


async def publish_reply(request_id: str, response):
    await js.publish(f"inbox.{request_id}", msgpack.encode(response))


async def handle_checkout_initiate(msg):
    initiate: CheckoutInitiateRequest = msgpack.decode(msg.data, type=CheckoutInitiateRequest)
    order_id = initiate.order_id
    logger.debug(f"Checking out {order_id}")
    try:
        entry: bytes = await db.get(order_id)
    except RedisError as e:
        logger.error(f"DB error fetching order {order_id}: {e}")
        result = CheckoutResult(
            message_id=str(uuid.uuid4()),
            request_id=initiate.request_id,
            order_id=order_id,
            success=False,
            error=DB_ERROR_STR,
        )
        await publish_reply(initiate.request_id, result)
        return
    if not entry:
        logger.error(f"Order {order_id} not found")
        result = CheckoutResult(
            message_id=str(uuid.uuid4()),
            request_id=initiate.request_id,
            order_id=order_id,
            success=False,
            error=f"Order {order_id} not found",
        )
        await publish_reply(initiate.request_id, result)
        return
    order_entry: OrderValue = msgpack.decode(entry, type=OrderValue)
    items_quantities: dict[str, int] = defaultdict(int)
    for item_id, quantity in order_entry.items:
        items_quantities[item_id] += quantity
    req = CheckoutRequest(
        message_id=str(uuid.uuid4()),
        request_id=initiate.request_id,
        order_id=order_id,
        user_id=order_entry.user_id,
        total_cost=order_entry.total_cost,
        items=dict(items_quantities),
    )
    await js.publish("checkout.payment", msgpack.encode(req))


async def handle_checkout_result(msg):
    result: CheckoutResult = msgpack.decode(msg.data, type=CheckoutResult)
    if not result.success:
        logger.warning(f"Checkout failed for order {result.order_id}: {result.error}")
        gateway_result = CheckoutResult(
            message_id=str(uuid.uuid4()),
            request_id=result.request_id,
            order_id=result.order_id,
            success=False,
            error=result.error,
        )
        await publish_reply(result.request_id, gateway_result)
        return
    try:
        entry: bytes = await db.get(result.order_id)
        if not entry:
            logger.error(f"Order {result.order_id} not found in DB on checkout result")
            gateway_result = CheckoutResult(
                message_id=str(uuid.uuid4()),
                request_id=result.request_id,
                order_id=result.order_id,
                success=False,
                error=f"Order {result.order_id} not found",
            )
            await publish_reply(result.request_id, gateway_result)
            return
        order_entry: OrderValue = msgpack.decode(entry, type=OrderValue)
        order_entry.paid = True
        await db.set(result.order_id, msgpack.encode(order_entry))
        gateway_result = CheckoutResult(
            message_id=str(uuid.uuid4()),
            request_id=result.request_id,
            order_id=result.order_id,
            success=True,
            error="",
        )
        await publish_reply(result.request_id, gateway_result)
    except RedisError as e:
        logger.error(f"DB error marking order {result.order_id} as paid: {e}")
        gateway_result = CheckoutResult(
            message_id=str(uuid.uuid4()),
            request_id=result.request_id,
            order_id=result.order_id,
            success=False,
            error=DB_ERROR_STR,
        )
        await publish_reply(result.request_id, gateway_result)


async def handle_order_create(msg):
    """Handle order creation request."""
    try:
        req: OrderCreateRequest = msgpack.decode(msg.data, type=OrderCreateRequest)
    except Exception as e:
        logger.error(f"Failed to decode order create message: {e}")
        return
    
    request_id = req.request_id
    key = str(uuid.uuid4())
    value = msgpack.encode(OrderValue(paid=False, items=[], user_id=req.user_id, total_cost=0))
    
    try:
        await db.set(key, value)
        result = OrderCreateResult(message_id=str(uuid.uuid4()), request_id=request_id, order_id=key, error="")
        await publish_reply(request_id, result)
    except RedisError as e:
        result = OrderCreateResult(message_id=str(uuid.uuid4()), request_id=request_id, order_id="", error=DB_ERROR_STR)
        await publish_reply(request_id, result)


async def handle_order_batch_init(msg):
    """Handle batch initialization of orders."""
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
        value = OrderValue(paid=False,
                           items=[(f"{item1_id}", 1), (f"{item2_id}", 1)],
                           user_id=f"{user_id}",
                           total_cost=2*req.item_price)
        return value

    kv_pairs: dict[str, bytes] = {f"{i}": msgpack.encode(generate_entry())
                                  for i in range(req.n)}
    try:
        await db.mset(kv_pairs)
        result = OrderBatchInitResult(message_id=str(uuid.uuid4()), request_id=request_id, success=True, error="")
        await publish_reply(request_id, result)
    except RedisError as e:
        result = OrderBatchInitResult(message_id=str(uuid.uuid4()), request_id=request_id, success=False, error=DB_ERROR_STR)
        await publish_reply(request_id, result)


async def handle_order_find(msg):
    """Handle order lookup request."""
    try:
        req: OrderFindRequest = msgpack.decode(msg.data, type=OrderFindRequest)
    except Exception as e:
        logger.error(f"Failed to decode order find message: {e}")
        return
    
    request_id = req.request_id
    order_entry = await get_order_from_db(req.order_id)
    
    result = OrderFindResult(
        message_id=str(uuid.uuid4()),
        request_id=request_id,
        order_id=req.order_id,
        order=order_entry,
        error="" if order_entry else f"Order: {req.order_id} not found!"
    )

    await publish_reply(request_id, result)


async def handle_order_add_item(msg):
    """Handle adding item to order request."""
    try:
        req: OrderAddItemRequest = msgpack.decode(msg.data, type=OrderAddItemRequest)
    except Exception as e:
        logger.error(f"Failed to decode order add item message: {e}")
        return
    
    request_id = req.request_id
    order_entry = await get_order_from_db(req.order_id)
    
    if order_entry is None:
        result = OrderAddItemResult(
            message_id=str(uuid.uuid4()),
            request_id=request_id,
            order_id=req.order_id,
            total_cost=0,
            error=f"Order: {req.order_id} not found!"
        )
        await publish_reply(request_id, result)
        return

    try:
        stock_result = await get_stock_item(req.item_id)
    except Exception:
        result = OrderAddItemResult(
            message_id=str(uuid.uuid4()),
            request_id=request_id,
            order_id=req.order_id,
            total_cost=0,
            error="Stock service timeout"
        )
        await publish_reply(request_id, result)
        return

    if stock_result.error or stock_result.item is None:
        result = OrderAddItemResult(
            message_id=str(uuid.uuid4()),
            request_id=request_id,
            order_id=req.order_id,
            total_cost=0,
            error=f"Item: {req.item_id} does not exist!"
        )
        await publish_reply(request_id, result)
        return
    
    # Add item to order
    order_entry.items.append((req.item_id, req.quantity))
    order_entry.total_cost += req.quantity * stock_result.item.price
    
    try:
        await db.set(req.order_id, msgpack.encode(order_entry))
        result = OrderAddItemResult(
            message_id=str(uuid.uuid4()),
            request_id=request_id,
            order_id=req.order_id,
            total_cost=order_entry.total_cost,
            error=""
        )
        await publish_reply(request_id, result)
    except RedisError as e:
        result = OrderAddItemResult(
            message_id=str(uuid.uuid4()),
            request_id=request_id,
            order_id=req.order_id,
            total_cost=0,
            error=DB_ERROR_STR
        )
        await publish_reply(request_id, result)


async def startup():
    global nc, js, logger
    logger = logging.getLogger("order-service")
    nc = await nats.connect(NATS_URL)
    js = nc.jetstream()
    await ensure_stream()
    
    # Subscribe to order RPC subjects (JetStream request/reply)
    await js.subscribe(
        "order.create",
        durable="order-create",
        queue="order-create",
        cb=handle_order_create,
    )
    await js.subscribe(
        "order.batch_init",
        durable="order-batch-init",
        queue="order-batch-init",
        cb=handle_order_batch_init,
    )
    await js.subscribe(
        "order.find",
        durable="order-find",
        queue="order-find",
        cb=handle_order_find,
    )
    await js.subscribe(
        "order.add_item",
        durable="order-add-item",
        queue="order-add-item",
        cb=handle_order_add_item,
    )

    # Subscribe to checkout subjects
    await js.subscribe(
        "checkout.initiate",
        durable="order-initiate",
        queue="order-initiate",
        cb=handle_checkout_initiate,
    )
    await js.subscribe(
        "checkout.result",
        durable="order-checkout",
        queue="order-checkout",
        cb=handle_checkout_result,
    )


async def shutdown():
    await nc.drain()
    await db.aclose()


async def main():
    logger = logging.getLogger("order-service")
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
