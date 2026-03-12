import logging
import os
import uuid

import nats
from redis.exceptions import RedisError

from dds_db import db
from msgspec import msgpack

from dds_db.transaction import async_transactional
import asyncio

from common.messages import *

DB_ERROR_STR = "DB error"

NATS_URL = os.environ['NATS_URL']
MESSAGE_STREAM_SUBJECTS = ["order.*", "payment.*", "stock.*", "checkout.>", "inbox.>"]

# db: Redis = Redis(host=os.environ['REDIS_HOST'],
#                   port=int(os.environ['REDIS_PORT']),
#                   password=os.environ['REDIS_PASSWORD'],
#                   db=int(os.environ['REDIS_DB']))

nc: nats.NATS | None = None
js = None

# Global logger
logger = None


async def get_item_from_db(item_id: str) -> StockValue | None:
    try:
        # get serialized data
        entry: bytes = await db.async_get(item_id)
    except RedisError as e:
        logger.error(f"DB error fetching item {item_id}: {e}")
        return None
    # deserialize data if it exists else return null
    entry: StockValue | None = msgpack.decode(entry, type=StockValue) if entry else None
    if entry is None:
        logger.warning(f"Item: {item_id} not found!")
        return None
    return entry

async def get_item_from_db_for_update(item_id: str) -> StockValue:
    """The same as the above get_item_from_db but with a lock that allows writing to the data without upgrading it."""
    try:
        entry: bytes = await db.async_get_for_update(item_id)
    except RedisError:
        raise RedisError(DB_ERROR_STR)
    entry: StockValue | None = msgpack.decode(entry, type=StockValue) if entry else None
    if entry is None:
        raise ValueError(f"Item: {item_id} not found!")
    return entry

async def ensure_stream():
    try:
        await js.add_stream(name="MESSAGES", subjects=MESSAGE_STREAM_SUBJECTS)
    except Exception:
        pass  # stream already exists


async def publish_reply(request_id: str, response):
    await js.publish(f"inbox.{request_id}", msgpack.encode(response))


async def handle_checkout_stock(msg):
    req: CheckoutRequest = msgpack.decode(msg.data, type=CheckoutRequest)
    # track items already subtracted in case we need to roll back
    subtracted: list[tuple[str, int]] = []

    for item_id, quantity in req.items.items():
        try:
            item_entry = await get_item_from_db(item_id)
            if item_entry is None:
                await rollback_stock(subtracted)
                await js.publish("checkout.result", msgpack.encode(
                    CheckoutResult(
                        message_id=str(uuid.uuid4()),
                        request_id=req.request_id,
                        order_id=req.order_id,
                        success=False,
                        error=f"Item: {item_id} not found!"
                    )
                ))
                return
        except (ValueError, RedisError) as e:
            await rollback_stock(subtracted)
            await js.publish("checkout.result", msgpack.encode(
                CheckoutResult(
                    message_id=str(uuid.uuid4()),
                    request_id=req.request_id,
                    order_id=req.order_id,
                    success=False,
                    error=str(e)
                )
            ))
            return

        # update stock, serialize and update database
        item_entry.stock -= quantity
        logger.debug(f"Item: {item_id} stock updated to: {item_entry.stock}")
        if item_entry.stock < 0:
            await rollback_stock(subtracted)
            await js.publish("checkout.result", msgpack.encode(
                CheckoutResult(
                    message_id=str(uuid.uuid4()),
                    request_id=req.request_id,
                    order_id=req.order_id,
                    success=False,
                    error=f"Item: {item_id} stock cannot get reduced below zero!"
                )
            ))
            return

        try:
            await db.async_set(item_id, msgpack.encode(item_entry))
        except RedisError as e:
            await rollback_stock(subtracted)
            await js.publish("checkout.result", msgpack.encode(
                CheckoutResult(
                    message_id=str(uuid.uuid4()),
                    request_id=req.request_id,
                    order_id=req.order_id,
                    success=False,
                    error=str(e)
                )
            ))
            return

        subtracted.append((item_id, quantity))

    # all items subtracted successfully
    await js.publish("checkout.result", msgpack.encode(
        CheckoutResult(
            message_id=str(uuid.uuid4()),
            request_id=req.request_id,
            order_id=req.order_id,
            success=True,
            error=""
        )
    ))


async def rollback_stock(subtracted: list[tuple[str, int]]):
    for item_id, quantity in subtracted:
        try:
            item_entry = await get_item_from_db(item_id)
            if item_entry is not None:
                item_entry.stock += quantity
                await db.async_set(item_id, msgpack.encode(item_entry))
        except Exception as e:
            logger.error(f"Rollback failed for item {item_id}: {e}")


async def handle_create_item(msg):
    """Handle item creation request."""
    try:
        req: StockCreateItemRequest = msgpack.decode(msg.data, type=StockCreateItemRequest)
    except Exception as e:
        logger.error(f"Failed to decode create item message: {e}")
        return
    
    request_id = req.request_id
    key = str(uuid.uuid4())
    logger.debug(f"Item: {key} created")
    value = msgpack.encode(StockValue(stock=0, price=req.price))
    
    try:
        await db.async_set(key, value)
        result = StockCreateItemResult(message_id=str(uuid.uuid4()), request_id=request_id, item_id=key, error="")
        await publish_reply(request_id, result)
    except RedisError as e:
        result = StockCreateItemResult(message_id=str(uuid.uuid4()), request_id=request_id, item_id="", error=DB_ERROR_STR)
        await publish_reply(request_id, result)


async def handle_batch_init_items(msg):
    """Handle batch initialization of items."""
    try:
        req: StockBatchInitRequest = msgpack.decode(msg.data, type=StockBatchInitRequest)
    except Exception as e:
        logger.error(f"Failed to decode batch init message: {e}")
        return
    
    request_id = req.request_id
    kv_pairs: dict[str, bytes] = {f"{i}": msgpack.encode(StockValue(stock=req.starting_stock, price=req.item_price))
                                  for i in range(req.n)}
    try:
        await db.async_mset(kv_pairs)
        result = StockBatchInitResult(message_id=str(uuid.uuid4()), request_id=request_id, success=True, error="")
        await publish_reply(request_id, result)
    except RedisError as e:
        result = StockBatchInitResult(message_id=str(uuid.uuid4()), request_id=request_id, success=False, error=DB_ERROR_STR)
        await publish_reply(request_id, result)


async def handle_find_item(msg):
    """Handle item lookup request."""
    try:
        req: StockFindItemRequest = msgpack.decode(msg.data, type=StockFindItemRequest)
    except Exception as e:
        logger.error(f"Failed to decode find item message: {e}")
        return
    
    request_id = req.request_id
    item_entry = await get_item_from_db(req.item_id)
    
    result = StockFindItemResult(
        message_id=str(uuid.uuid4()),
        request_id=request_id,
        item_id=req.item_id,
        item=item_entry,
        error="" if item_entry else f"Item: {req.item_id} not found!"
    )

    await publish_reply(request_id, result)


async def handle_add_amount(msg):
    """Handle adding stock to an item."""
    try:
        req: StockAddAmountRequest = msgpack.decode(msg.data, type=StockAddAmountRequest)
    except Exception as e:
        logger.error(f"Failed to decode add amount message: {e}")
        return
    
    request_id = req.request_id
    item_entry = await get_item_from_db(req.item_id)
    
    if item_entry is None:
        result = StockAddAmountResult(
            message_id=str(uuid.uuid4()),
            request_id=request_id,
            item_id=req.item_id,
            stock=0,
            error=f"Item: {req.item_id} not found!"
        )
        await publish_reply(request_id, result)
        return
    
    # Update stock
    item_entry.stock += req.amount
    
    try:
        await db.async_set(req.item_id, msgpack.encode(item_entry))
        result = StockAddAmountResult(
            message_id=str(uuid.uuid4()),
            request_id=request_id,
            item_id=req.item_id,
            stock=item_entry.stock,
            error=""
        )
        await publish_reply(request_id, result)
    except RedisError as e:
        result = StockAddAmountResult(
            message_id=str(uuid.uuid4()),
            request_id=request_id,
            item_id=req.item_id,
            stock=0,
            error=DB_ERROR_STR
        )
        await publish_reply(request_id, result)


async def handle_subtract_amount(msg):
    """Handle subtracting stock from an item."""
    try:
        req: StockSubtractAmountRequest = msgpack.decode(msg.data, type=StockSubtractAmountRequest)
    except Exception as e:
        logger.error(f"Failed to decode subtract amount message: {e}")
        return
    
    request_id = req.request_id
    item_entry = await get_item_from_db(req.item_id)
    
    if item_entry is None:
        result = StockSubtractAmountResult(
            message_id=str(uuid.uuid4()),
            request_id=request_id,
            item_id=req.item_id,
            stock=0,
            error=f"Item: {req.item_id} not found!"
        )
        await publish_reply(request_id, result)
        return
    
    # Update stock
    item_entry.stock -= req.amount
    logger.debug(f"Item: {req.item_id} stock updated to: {item_entry.stock}")
    
    if item_entry.stock < 0:
        result = StockSubtractAmountResult(
            message_id=str(uuid.uuid4()),
            request_id=request_id,
            item_id=req.item_id,
            stock=0,
            error=f"Item: {req.item_id} stock cannot get reduced below zero!"
        )
        await publish_reply(request_id, result)
        return
    
    try:
        await db.async_set(req.item_id, msgpack.encode(item_entry))
        result = StockSubtractAmountResult(
            message_id=str(uuid.uuid4()),
            request_id=request_id,
            item_id=req.item_id,
            stock=item_entry.stock,
            error=""
        )
        await publish_reply(request_id, result)
    except RedisError as e:
        result = StockSubtractAmountResult(
            message_id=str(uuid.uuid4()),
            request_id=request_id,
            item_id=req.item_id,
            stock=0,
            error=DB_ERROR_STR
        )
        await publish_reply(request_id, result)


async def startup():
    global nc, js, logger
    logger = logging.getLogger("stock-service")
    nc = await nats.connect(NATS_URL)
    js = nc.jetstream()
    await ensure_stream()
    
    # Subscribe to stock RPC subjects (JetStream request/reply)
    await js.subscribe(
        "stock.create_item",
        durable="stock-create-item",
        queue="stock-create-item",
        cb=handle_create_item,
    )
    await js.subscribe(
        "stock.batch_init",
        durable="stock-batch-init",
        queue="stock-batch-init",
        cb=handle_batch_init_items,
    )
    await js.subscribe(
        "stock.find",
        durable="stock-find",
        queue="stock-find",
        cb=handle_find_item,
    )
    await js.subscribe(
        "stock.add",
        durable="stock-add",
        queue="stock-add",
        cb=handle_add_amount,
    )
    await js.subscribe(
        "stock.subtract",
        durable="stock-subtract",
        queue="stock-subtract",
        cb=handle_subtract_amount,
    )

    # Subscribe to checkout messages
    await js.subscribe(
        "checkout.stock",
        durable="stock-checkout",
        queue="stock-checkout",
        cb=handle_checkout_stock,
    
    )
    await js.subscribe("checkout.2pc.stock.prepare", durable="stock-2pc-prepare", queue="stock-2pc-prepare", cb=handle_2pc_prepare,)
    await js.subscribe("checkout.2pc.stock.commit", durable="stock-2pc-commit", queue="stock-2pc-commit", cb=handle_2pc_commit,)
    await js.subscribe("checkout.2pc.stock.abort", durable="stock-2pc-abort", queue="stock-2pc-abort", cb=handle_2pc_abort,)


async def shutdown():
    await nc.drain()
    await db.async_close()

async def main():
    logger = logging.getLogger("stock-service")
    logging.basicConfig(level=logging.INFO)
    await startup()
    # Keep the service running
    await asyncio.sleep(float('inf'))

# 2PC
async def handle_2pc_prepare(msg):
    req: CheckoutRequest = msgpack.decode(msg.data, type=CheckoutRequest)
    txn_id = uuid.UUID(req.txn_id)
    await db.async_begin_txn(txn_id)
    logger.info(f"[2PC Stock Prepare] Preparing stock for transaction {txn_id}")

    try:
        # validate all items before storing intents, so that there are no partial reservations
        for item_id, quantity in req.items.items():
            try:
                item_entry = await get_item_from_db_for_update(item_id)

            # if item cannot be found or db error, vote no and return
            except Exception as e:
                await js.publish("checkout.2pc.stock.vote", msgpack.encode(
                    CheckoutResult(txn_id=req.txn_id, message_id=req.message_id, request_id=req.request_id, order_id=req.order_id, success=False, error=str(e))
                ))
                await db.async_end_txn(txn_id)
                logger.warning(f"[2PC Stock Prepare] Voting NO for transaction {txn_id} due to error: {e}")
                return

            # vote no if not enough stock
            if item_entry.stock < quantity:
                await js.publish("checkout.2pc.stock.vote", msgpack.encode(
                    CheckoutResult(txn_id=req.txn_id, message_id=req.message_id, request_id=req.request_id, order_id=req.order_id, success=False, error=f"Insufficient stock for {item_id}")
                ))
                await db.async_end_txn(txn_id)
                logger.warning(f"[2PC Stock Prepare] Voting NO for transaction {txn_id} due to insufficient stock for item {item_id}")
                return

        # all items validated, store intents for commit phase
        for item_id, quantity in req.items.items():
            logger.info(f"[2PC Stock Prepare] Storing pending intent for item {item_id} with quantity {quantity} in transaction {txn_id}")
            await db.async_set(f"pending:{req.txn_id}:{item_id}", msgpack.encode({
                "item_id": item_id,
                "quantity": quantity,
            }))

        # vote yes
        await js.publish("checkout.2pc.stock.vote", msgpack.encode(
            CheckoutResult(txn_id=req.txn_id, message_id=req.message_id, request_id=req.request_id, order_id=req.order_id, success=True, error="")
        ))
        logger.info(f"[2PC Stock Prepare] Voting YES for transaction {txn_id}")
        await db.async_detach_txn()

    except Exception:
        # clean up locks
        logger.exception(f"[2PC Stock Prepare] Error during prepare for transaction {txn_id}, voting NO")
        await db.async_end_txn(txn_id)
        raise


async def handle_2pc_commit(msg):
    req: CheckoutRequest = msgpack.decode(msg.data, type=CheckoutRequest)
    txn_id = uuid.UUID(req.txn_id)
    await db.async_begin_txn(txn_id)
    logger.info(f"[2PC Stock Commit] Committing transaction {txn_id}")
    logger.info(f"[2PC Stock Commit] Items to commit for transaction {txn_id}: {req.items}")

    # execute pending transactions
    for item_id in req.items:
        raw = await db.async_get_for_update(f"pending:{req.txn_id}:{item_id}")
        logger.info(f"[2PC Stock Commit] Processing pending intent for item {item_id} in transaction {txn_id}")    
        if not raw:
            logger.error(f"[2PC Stock Commit] Missing pending intent for item {item_id} in transaction {txn_id}, skipping")
            continue
        
        pending = msgpack.decode(raw, type=dict)
        item_entry = await get_item_from_db_for_update(pending["item_id"])
        item_entry.stock -= pending["quantity"]
        logger.info(f"[2PC Stock Commit] New stock for item {item_id} is {item_entry.stock}") 
        await db.async_set(pending["item_id"], msgpack.encode(item_entry))
        await db.async_delete(f"pending:{req.txn_id}:{item_id}")

    # release locks
    await db.async_end_txn(txn_id)

    # ack to coordinator
    logger.info(f"[2PC Stock Commit] Transaction {txn_id} committed successfully, sending ACK")
    await js.publish("checkout.2pc.stock.ack", msgpack.encode(
        CheckoutResult(txn_id=req.txn_id, message_id=req.message_id, request_id=req.request_id, order_id=req.order_id, success=True, error="")
    ))


async def handle_2pc_abort(msg):
    req: CheckoutRequest = msgpack.decode(msg.data, type=CheckoutRequest)
    txn_id = uuid.UUID(req.txn_id)
    logger.info(f"[2PC Stock Abort] Aborting transaction {txn_id}")

    # delete pending intents and release locks to abort, no rollback needed
    for item_id in req.items:
        await db.async_delete(f"pending:{req.txn_id}:{item_id}")

    await db.async_end_txn(txn_id)

    # ack to coordinator
    await js.publish("checkout.2pc.stock.ack", msgpack.encode(
        CheckoutResult(txn_id=req.txn_id, message_id=req.message_id, request_id=req.request_id, order_id=req.order_id, success=True, error="")
    ))

if __name__ == '__main__':
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Shutting down...")
else:
    logging.basicConfig(level=logging.INFO)
