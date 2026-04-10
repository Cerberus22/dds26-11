import asyncio
import logging
import os
import time
import uuid
from collections import defaultdict

import nats
from msgspec import Struct, msgpack
from redis.asyncio import Redis
from redis.exceptions import RedisError
from nats.js.api import RetentionPolicy

from common.messages import *


DB_ERROR_STR = "DB error"
NATS_URL = os.environ["NATS_URL"]
MESSAGE_TIMEOUT = 30.0

db: Redis = Redis(
    host=os.environ["REDIS_HOST"],
    port=int(os.environ["REDIS_PORT"]),
    password=os.environ["REDIS_PASSWORD"],
    db=int(os.environ["REDIS_DB"]),
)

nc: nats.NATS | None = None
js = None
logger = None

# Saga steps:
# 1. Receive checkout request from order service
# 2. Send command to payment service to reserve funds, if fail notify order service and end saga 
# 3. Send command to stock service to reserve inventory, if fail send compensate payment, notify order service and end saga
# 4. Notify order service (success) and end saga

ACTIVE_SAGAS_KEY = "orchestrator:active_sagas"
SAGA_TIMEOUT_SECONDS = 60

# Saga-DB functions
def _saga_state_key(saga_id: str) -> str:
    return f"orchestrator:saga:{saga_id}"


async def _get_saga(saga_id: str) -> SagaState | None:
    raw = await db.get(_saga_state_key(saga_id))
    if raw is None:
        return None
    return msgpack.decode(raw, type=SagaState)

def _order_saga_key(order_id: str) -> str:
    return f"orchestrator:order_saga:{order_id}"

async def _save_saga(saga: SagaState):
    saga.updated_at = time.time()
    await db.set(_saga_state_key(saga.saga_id), msgpack.encode(saga))

# Stream helpers
async def ensure_stream():
    for stream_name, subjects in [
        ("ORCHESTRATOR", ["orchestrator.>"]),
        ("PAYMENT", ["payment.>"]),
        ("STOCK", ["stock.>"]),
        ("INBOX", ["inbox.>"]),
    ]:
        try:
            await js.add_stream(name=stream_name, subjects=subjects, retention=RetentionPolicy.INTEREST)
        except Exception:
            pass

# Starting here: Checkout request initiated
async def handle_checkout_request(msg):
    req: StartCheckoutRequest = msgpack.decode(msg.data, type=StartCheckoutRequest)

    try: 
        # Idempotency: check if this order already has an active saga
        saga_id = str(uuid.uuid4())
        # set the saga to nx so that for concurrent req, only one succeeds 
        was_set = await db.set(_order_saga_key(req.order_id), saga_id, nx=True)
        if not was_set:
            # Another saga already claimed this order, resume it
            logger.info(f"Checkout request for order {req.order_id} already has an active saga, resuming it")
            existing_saga_id = await db.get(_order_saga_key(req.order_id))
            existing = await _get_saga(existing_saga_id.decode())
            if existing:
                existing.extra_request_ids.append(req.request_id)
                await _save_saga(existing)
                await _resume_saga(existing)
            await msg.ack()
            return

        # no active saga, make one
        logger.info(f"Starting new saga {saga_id} for order {req.order_id}")
        saga = SagaState(
            saga_id=saga_id,
            request_id=req.request_id,
            extra_request_ids=[],
            order_id=req.order_id,
            user_id=req.user_id,
            total_cost=req.total_cost,
            items=req.items,
            status="STARTED",
            payment_reserved=False,
            stock_reserved=False,
            error="",
        )

        # Save initial saga state and index by order_id for idempotency
        pipe = db.pipeline()
        pipe.multi()
        pipe.set(_saga_state_key(saga_id), msgpack.encode(saga))
        # pipe.set(_order_saga_key(req.order_id), saga_id)
        pipe.sadd(ACTIVE_SAGAS_KEY, saga_id)
        await pipe.execute()
        await pipe.aclose()

        # Initiate step 2: reserve payment
        await _command_reserve_payment(saga)
        await msg.ack()

    except RedisError as e:
        logger.error(f"Redis unavailable for checkout of order {req.order_id}: {e}")
        await msg.nak(delay=min(2 ** msg.metadata.num_delivered, 30))
        return


# Step 2: Payment service replies 
async def handle_payment_reserved(msg):
    """
    msg: Received on `orchestrator.payment_reserved`, CheckoutResult with saga_id, success, error.
    Goal: if success, move to reserve stock step; if fail, update saga and notify order service.
    """
    result: CheckoutResult = msgpack.decode(msg.data, type=CheckoutResult)
    try:
        saga = await _get_saga(result.saga_id)
        if saga is None:
            logger.warning(f"No saga state for {result.saga_id} on payment_reserved")
            await msg.ack()
            return

        if not result.success:
            # Payment failed: nothing to compensate, just fail the saga
            saga.status = "FAILED"
            saga.error = result.error
            await _save_saga(saga)
            await _notify_order_service(saga, success=False)
            await msg.ack()
            return

        # Payment succeeded: move to stock reservation
        saga.payment_reserved = True
        saga.status = "PAYMENT_RESERVED"
        await _save_saga(saga)
        # Initiate step 3
        await _command_reserve_stock(saga)
        await msg.ack()

    except RedisError as e:
        logger.error(f"Redis unavailable for payment reservation of saga {result.saga_id}: {e}")
        await msg.nak(delay=min(2 ** msg.metadata.num_delivered, 30))
        return

# Step 3: Stock service replies
async def handle_stock_reserved(msg):
    """
    msg: Received on `orchestrator.stock_reserved`, CheckoutResult with saga_id, success, error.
    Goal: if success, complete saga and notify order service; 
    if fail, compensate payment, update saga and notify order service.
    """
    result: CheckoutResult = msgpack.decode(msg.data, type=CheckoutResult)

    try:
        saga = await _get_saga(result.saga_id)
        if saga is None:
            logger.warning(f"No saga state for {result.saga_id} on stock_reserved")
            await msg.ack()
            return

        if result.success:
            # Both payment and stock succeeded: saga complete
            saga.stock_reserved = True
            saga.status = "COMPLETED"
            await _save_saga(saga)
            # Initiate step 4: notify order service of success
            await _notify_order_service(saga, success=True)
            await msg.ack()
            return

        # Stock failed: need to compensate payment
        saga.status = "COMPENSATING"
        saga.error = result.error
        await _save_saga(saga)
        await _command_compensate_payment(saga)
        await msg.ack()

    except RedisError as e:
        logger.error(f"Redis unavailable for stock reservation of saga {result.saga_id}: {e}")
        await msg.nak(delay=min(2 ** msg.metadata.num_delivered, 30))
        return

# Compensation reply from payment service
async def handle_payment_compensated(msg):
    """
    msg: Received on `orchestrator.payment_compensated`, CheckoutResult with saga_id, success, error.
    Goal: Update saga status based on compensation result and notify order service.
    """
    result: CheckoutResult = msgpack.decode(msg.data, type=CheckoutResult)
    try: 
        saga = await _get_saga(result.saga_id)
        if saga is None:
            logger.warning(f"No saga state for {result.saga_id} on payment_compensated")
            await msg.ack()
            return

        if not result.success:
            logger.error(
                f"Payment compensation FAILED for saga {saga.saga_id}: {result.error}. "
                "Major failure!!!"
            )

        logger.info(f"Payment compensation {'succeeded' if result.success else 'failed'} for saga {saga.saga_id}, notifying order service of failure")
        saga.payment_reserved = False
        saga.status = "FAILED"
        await _save_saga(saga)
        await _notify_order_service(saga, success=False)
        await msg.ack()

    except RedisError as e:
        logger.error(f"Redis unavailable for payment compensation of saga {result.saga_id}: {e}")
        await msg.nak(delay=min(2 ** msg.metadata.num_delivered, 30))
        return

# commands to other services
async def _command_reserve_payment(saga: SagaState):
    """Tell payment service to deduct credit."""
    logger.info(f"Commanding payment reserve for saga {saga.saga_id}, order {saga.order_id}")
    req = CheckoutRequest(
        saga_id=saga.saga_id,
        message_id=str(uuid.uuid4()),
        request_id=saga.request_id,
        order_id=saga.order_id,
        user_id=saga.user_id,
        total_cost=saga.total_cost,
        items=saga.items,
    )
    await js.publish("payment.reserve", msgpack.encode(req))


async def _command_reserve_stock(saga: SagaState):
    """Tell stock service to decrement stock."""
    logger.info(f"Commanding stock reserve for saga {saga.saga_id}, order {saga.order_id}")
    req = CheckoutRequest(
        saga_id=saga.saga_id,
        message_id=str(uuid.uuid4()),
        request_id=saga.request_id,
        order_id=saga.order_id,
        user_id=saga.user_id,
        total_cost=saga.total_cost,
        items=saga.items,
    )
    await js.publish("stock.reserve", msgpack.encode(req))


async def _command_compensate_payment(saga: SagaState):
    """Tell payment service to refund credit."""
    logger.info(f"Commanding payment compensation for saga {saga.saga_id}, order {saga.order_id}")
    req = CheckoutRequest(
        saga_id=saga.saga_id,
        message_id=str(uuid.uuid4()),
        request_id=saga.request_id,
        order_id=saga.order_id,
        user_id=saga.user_id,
        total_cost=saga.total_cost,
        items=saga.items,
    )
    await js.publish("payment.compensate", msgpack.encode(req))


async def _notify_order_service(saga: SagaState, success: bool):
    """Send the final checkout result back to the order service."""
    for req_id in [saga.request_id] + saga.extra_request_ids:
        logger.info(f"Notifying order service of {'success' if success else 'failure'} for saga {saga.saga_id}, order {saga.order_id}")
        result = CheckoutResult(
            saga_id=saga.saga_id,
            message_id=str(uuid.uuid4()),
            request_id=req_id,
            order_id=saga.order_id,
            success=success,
            error=saga.error,
        )
        # publish (to which the order service is subscribed), order service will notify the gateway
        await js.publish("orchestrator.result", msgpack.encode(result))
        # delete the saga state so that if the order is retried later, a fresh saga is started
        # it is okay if failure here, the recovery loop will catch that and clean up
        pipe = db.pipeline()
        pipe.multi()
        pipe.delete(_order_saga_key(saga.order_id))
        pipe.delete(_saga_state_key(saga.saga_id))
        pipe.srem(ACTIVE_SAGAS_KEY, saga.saga_id)
        await pipe.execute()
        await pipe.aclose()

async def _resume_saga(saga: SagaState):
    """
    Idempotent recovery: re-issue command based on saga status.
    """
    logger.info(f"Resuming saga {saga.saga_id} at status {saga.status}")
    if saga.status == "STARTED":
        await _command_reserve_payment(saga)
    elif saga.status == "PAYMENT_RESERVED":
        await _command_reserve_stock(saga)
    elif saga.status == "COMPENSATING":
        await _command_compensate_payment(saga)
    elif saga.status in ("COMPLETED", "FAILED"):
        await _notify_order_service(saga, success=(saga.status == "COMPLETED"))

# Recovery logic: run after startup and then periodically to find and resume stuck sagas
async def recover_sagas():
    # with replicas, prevent multiple instances from doing recovery at the same time by acquiring a lock
    acquired = await db.set("orchestrator:recovery_lock", "1", nx=True, ex=25)
    if not acquired:
        logger.debug("Another instance is running recovery sweep, skipping")
        return
    
    saga_ids = await db.smembers(ACTIVE_SAGAS_KEY)
    if not saga_ids:
        return

    logger.info(f"Recovery sweep: checking {len(saga_ids)} active sagas")

    for saga_id_bytes in saga_ids:
        saga_id = saga_id_bytes.decode()
        saga = await _get_saga(saga_id)

        if saga is None:
            # no saga state found, remove it
            await db.srem(ACTIVE_SAGAS_KEY, saga_id)
            continue

        if saga.status in ("COMPLETED", "FAILED"):
            # finished but not cleaned up, notify and delete
            await _notify_order_service(saga, success=(saga.status == "COMPLETED"))
            continue

        # check timestamp
        if saga.updated_at and (time.time() - saga.updated_at) < SAGA_TIMEOUT_SECONDS:
            continue  # still fresh, let it be

        logger.warning(f"Recovering stuck saga {saga_id}, status={saga.status}")
        await _resume_saga(saga)

async def recovery_loop():
    while True:
        try:
            await recover_sagas()
        except RedisError as e:
            logger.error(f"Redis unavailable during recovery sweep: {e}")
        except Exception as e:
            logger.error(f"Recovery sweep failed: {e}")
        await asyncio.sleep(30) # sweep every 30 seconds

# setup
async def startup():
    global nc, js, logger
    logger = logging.getLogger("orchestrator-service")
    nc = await nats.connect(NATS_URL)
    js = nc.jetstream()
    await ensure_stream()

    # Entry point from order service
    await js.subscribe(
        "orchestrator.checkout",
        durable="orch-checkout",
        queue="orch-checkout",
        cb=handle_checkout_request,
        manual_ack=True,
    )
    # Replies from payment service
    await js.subscribe(
        "orchestrator.payment_reserved",
        durable="orch-payment-reserved",
        queue="orch-payment-reserved",
        cb=handle_payment_reserved,
        manual_ack=True,
    )
    await js.subscribe(
        "orchestrator.payment_compensated",
        durable="orch-payment-compensated",
        queue="orch-payment-compensated",
        cb=handle_payment_compensated,
        manual_ack=True,
    )
    # Reply from stock service
    await js.subscribe(
        "orchestrator.stock_reserved",
        durable="orch-stock-reserved",
        queue="orch-stock-reserved",
        cb=handle_stock_reserved,
        manual_ack=True,
    )


async def shutdown():
    await nc.drain()
    await db.aclose()


async def main():
    logging.basicConfig(level=logging.WARNING)
    await startup()

    await recover_sagas()
    asyncio.create_task(recovery_loop())

    await asyncio.sleep(float("inf"))


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Shutting down...")
else:
    logging.basicConfig(level=logging.WARNING)