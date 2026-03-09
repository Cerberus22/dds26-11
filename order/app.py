import logging
import os
import random
import uuid
from collections import defaultdict

import aiohttp
import nats
from redis.asyncio import Redis
from redis.exceptions import RedisError

from msgspec import msgpack, Struct
from quart import Quart, jsonify, abort, Response

from common.messages import CheckoutRequest, CheckoutResult


DB_ERROR_STR = "DB error"
REQ_ERROR_STR = "Requests error"

GATEWAY_URL = os.environ['GATEWAY_URL']
NATS_URL = os.environ['NATS_URL']

app = Quart("order-service")

db: Redis = Redis(host=os.environ['REDIS_HOST'],
                  port=int(os.environ['REDIS_PORT']),
                  password=os.environ['REDIS_PASSWORD'],
                  db=int(os.environ['REDIS_DB']))

def _saga_started_key(saga_id: str) -> str:
    return f"saga:started:{saga_id}"

def _saga_compensation_key(saga_id: str) -> str:
    return f"saga:compensation:{saga_id}"

def _saga_commit_key(saga_id: str) -> str:
    return f"saga:commit:{saga_id}"

def _saga_outbox_key(saga_id: str) -> str:
    return f"saga:outbox:{saga_id}"

nc: nats.NATS | None = None
js = None
session: aiohttp.ClientSession | None = None

class OrderCompensation(Struct):
    order_id: str
    original_paid: bool


class OrderValue(Struct):
    paid: bool
    items: list[tuple[str, int]]
    user_id: str
    total_cost: int


async def get_order_from_db(order_id: str) -> OrderValue | None:
    try:
        # get serialized data
        entry: bytes = await db.get(order_id)
    except RedisError:
        return abort(400, DB_ERROR_STR)
    # deserialize data if it exists else return null
    entry: OrderValue | None = msgpack.decode(entry, type=OrderValue) if entry else None
    if entry is None:
        # if order does not exist in the database; abort
        abort(400, f"Order: {order_id} not found!")
    return entry


async def ensure_stream():
    try:
        await js.add_stream(name="CHECKOUT", subjects=["checkout.>"])
    except Exception:
        pass  # stream already exists
    
    try:
        await js.add_stream(name="ORDER", subjects=["order.>"])
    except Exception:
        pass  # stream already exists
    
    try:
        await js.add_stream(name="PAYMENT", subjects=["payment.>"])
    except Exception:
        pass  # stream already exists
    
    try:
        await js.add_stream(name="STOCK", subjects=["stock.>"])
    except Exception:
        pass  # stream already exists


async def handle_payment_result(msg):
    result: CheckoutResult = msgpack.decode(msg.data, type=CheckoutResult)

    # check for duplicate messages
    if await db.exists(_saga_commit_key(result.saga_id) + ":payment-ack"):
        app.logger.info(f"Duplicate payment.result for saga {result.saga_id}, skipping")
        return

    if not result.success:
        app.logger.warning(f"Payment failed for saga {result.saga_id}, order {result.order_id}: {result.error}")
        await _rollback_order(result.saga_id, result.order_id)
        return

    app.logger.info(f"Payment succeeded for saga {result.saga_id}, order {result.order_id}, waiting for stock result")
    # mark that we have acked this payment success so we don't re-rollback on duplicate
    try:
        await db.set(_saga_commit_key(result.saga_id) + ":payment-ack", b"1")
    except RedisError:
        pass


async def handle_stock_result(msg):
    result: CheckoutResult = msgpack.decode(msg.data, type=CheckoutResult)

    # check for duplicate messages
    if await db.exists(_saga_commit_key(result.saga_id) + ":stock-ack"):
        app.logger.info(f"Duplicate stock.result for saga {result.saga_id}, skipping")
        return

    if result.success:
        # stock succeeded - saga is fully complete, clean up saga keys
        app.logger.info(f"Stock succeeded for saga {result.saga_id}, order {result.order_id}")
        await _cleanup_saga(result.saga_id)
        return

    # stock failed - rollback: unmark the order as paid
    app.logger.warning(f"Stock checkout failed for saga {result.saga_id}, order {result.order_id}: {result.error}")
    await _rollback_order(result.saga_id, result.order_id)

async def _rollback_order(saga_id: str, order_id: str):
    # rollback order from its compensation data, then clean up saga keys
    comp_key = _saga_compensation_key(saga_id)
    try:
        comp_bytes = await db.get(comp_key)
        if comp_bytes is None:
            app.logger.warning(f"No compensation data for saga {saga_id}, cannot rollback order {order_id}")
            return
        comp: OrderCompensation = msgpack.decode(comp_bytes, type=OrderCompensation)
        entry_bytes: bytes = await db.get(comp.order_id)
        if entry_bytes:
            order_entry: OrderValue = msgpack.decode(entry_bytes, type=OrderValue)
            order_entry.paid = comp.original_paid
            await db.set(comp.order_id, msgpack.encode(order_entry))
            app.logger.info(f"Order {comp.order_id} rolled back to paid={comp.original_paid} for saga {saga_id}")
    except RedisError as e:
        app.logger.error(f"DB error rolling back order {order_id} for saga {saga_id}: {e}")
    finally:
        await _cleanup_saga(saga_id)


async def _cleanup_saga(saga_id: str):
    # clean up all saga keys so we don't try to recover or rollback this saga again
    try:
        await db.delete(
            _saga_started_key(saga_id),
            _saga_compensation_key(saga_id),
            _saga_commit_key(saga_id),
            _saga_outbox_key(saga_id),
            _saga_commit_key(saga_id) + ":payment-ack",
            _saga_commit_key(saga_id) + ":stock-ack",
        )
    except RedisError as e:
        app.logger.warning(f"Could not clean up saga keys for {saga_id}: {e}")

async def recover_sagas():
    app.logger.info("Order service: scanning for incomplete sagas...")
    try:
        cursor = 0
        while True:
            cursor, keys = await db.scan(cursor, match="saga:started:*", count=100)
            for key in keys:
                saga_id = key.decode().removeprefix("saga:started:")
                commit_exists = await db.exists(_saga_commit_key(saga_id))
                outbox_exists = await db.exists(_saga_outbox_key(saga_id))

                if not commit_exists:
                    app.logger.warning(f"Recovery: saga {saga_id} has no commit, cleaning up (order was not mutated)")
                    await _cleanup_saga(saga_id)

                elif not outbox_exists:
                    # crashed after commit but before/during NATS publish -> re-publish
                    app.logger.warning(f"Recovery: saga {saga_id} committed but outbox missing, re-publishing")
                    try:
                        outbox_bytes = await db.get(_saga_commit_key(saga_id))
                        if outbox_bytes:
                            req: CheckoutRequest = msgpack.decode(outbox_bytes, type=CheckoutRequest)
                            await js.publish("checkout.payment", msgpack.encode(req))
                            await db.set(_saga_outbox_key(saga_id), b"1")
                            app.logger.info(f"Recovery: re-published checkout.payment for saga {saga_id}")
                    except Exception as e:
                        app.logger.error(f"Recovery: failed to re-publish saga {saga_id}: {e}")

            if cursor == 0:
                break
    except RedisError as e:
        app.logger.error(f"Recovery scan failed: {e}")


@app.before_serving
async def startup():
    global nc, js, session
    session = aiohttp.ClientSession()
    nc = await nats.connect(NATS_URL)
    js = nc.jetstream()
    await ensure_stream()
    # run crash recovery before accepting messages
    await recover_sagas()
    await js.subscribe(
        "payment.result",
        durable="order-payment-result",
        queue="order-payment-result",
        cb=handle_payment_result,
    )
    await js.subscribe(
        "stock.result",
        durable="order-stock-result",
        queue="order-stock-result",
        cb=handle_stock_result,
    )


@app.after_serving
async def shutdown():
    await session.close()
    await nc.drain()
    await db.aclose()


@app.post('/create/<user_id>')
async def create_order(user_id: str):
    key = str(uuid.uuid4())
    value = msgpack.encode(OrderValue(paid=False, items=[], user_id=user_id, total_cost=0))
    try:
        await db.set(key, value)
    except RedisError:
        return abort(400, DB_ERROR_STR)
    return jsonify({'order_id': key})


@app.post('/batch_init/<n>/<n_items>/<n_users>/<item_price>')
async def batch_init_users(n: int, n_items: int, n_users: int, item_price: int):

    n = int(n)
    n_items = int(n_items)
    n_users = int(n_users)
    item_price = int(item_price)

    def generate_entry() -> OrderValue:
        user_id = random.randint(0, n_users - 1)
        item1_id = random.randint(0, n_items - 1)
        item2_id = random.randint(0, n_items - 1)
        value = OrderValue(paid=False,
                           items=[(f"{item1_id}", 1), (f"{item2_id}", 1)],
                           user_id=f"{user_id}",
                           total_cost=2*item_price)
        return value

    kv_pairs: dict[str, bytes] = {f"{i}": msgpack.encode(generate_entry())
                                  for i in range(n)}
    try:
        await db.mset(kv_pairs)
    except RedisError:
        return abort(400, DB_ERROR_STR)
    return jsonify({"msg": "Batch init for orders successful"})


@app.get('/find/<order_id>')
async def find_order(order_id: str):
    order_entry: OrderValue = await get_order_from_db(order_id)
    return jsonify(
        {
            "order_id": order_id,
            "paid": order_entry.paid,
            "items": order_entry.items,
            "user_id": order_entry.user_id,
            "total_cost": order_entry.total_cost
        }
    )


async def send_post_request(url: str):
    try:
        response = await session.post(url)
    except aiohttp.ClientError:
        abort(400, REQ_ERROR_STR)
    else:
        return response


async def send_get_request(url: str):
    try:
        response = await session.get(url)
    except aiohttp.ClientError:
        abort(400, REQ_ERROR_STR)
    else:
        return response


async def rollback_stock(removed_items: list[tuple[str, int]]):
    for item_id, quantity in removed_items:
        await send_post_request(f"{GATEWAY_URL}/stock/add/{item_id}/{quantity}")


@app.post('/addItem/<order_id>/<item_id>/<quantity>')
async def add_item(order_id: str, item_id: str, quantity: int):
    order_entry: OrderValue = await get_order_from_db(order_id)
    item_reply = await send_get_request(f"{GATEWAY_URL}/stock/find/{item_id}")
    if item_reply.status != 200:
        # Request failed because item does not exist
        abort(400, f"Item: {item_id} does not exist!")
    item_json: dict = await item_reply.json()
    order_entry.items.append((item_id, int(quantity)))
    order_entry.total_cost += int(quantity) * item_json["price"]
    try:
        await db.set(order_id, msgpack.encode(order_entry))
    except RedisError:
        return abort(400, DB_ERROR_STR)
    return Response(f"Item: {item_id} added to: {order_id} price updated to: {order_entry.total_cost}",
                    status=200)


@app.post('/checkout/<order_id>')
async def checkout(order_id: str):
    app.logger.debug(f"Checking out {order_id}")
    order_entry: OrderValue = await get_order_from_db(order_id)
    items_quantities: dict[str, int] = defaultdict(int)
    for item_id, quantity in order_entry.items:
        items_quantities[item_id] += quantity

    saga_id = str(uuid.uuid4())
    app.logger.debug(f"Starting saga {saga_id} for order {order_id}")

    # 1. write saga:started - marks our intent
    try:
        await db.set(_saga_started_key(saga_id), order_id)
    except RedisError:
        return abort(400, DB_ERROR_STR)

    # 2. build the outbox message
    msg = CheckoutRequest(
        saga_id=saga_id,
        order_id=order_id,
        user_id=order_entry.user_id,
        total_cost=order_entry.total_cost,
        items=dict(items_quantities),
    )

    # 3. pipeline: saga:compensation + order.paid=True + saga:commit atomically.
    compensation = OrderCompensation(order_id=order_id, original_paid=order_entry.paid)
    order_entry.paid = True
    try:
        pipe = db.pipeline(transaction=True)
        pipe.set(_saga_compensation_key(saga_id), msgpack.encode(compensation))
        pipe.set(order_id, msgpack.encode(order_entry))
        # saga:commit stores the outbox message so recovery can re-publish it
        pipe.set(_saga_commit_key(saga_id), msgpack.encode(msg))
        await pipe.execute()
    except RedisError:
        await db.delete(_saga_started_key(saga_id))
        return abort(400, DB_ERROR_STR)

    # 4. publish to NATS, then mark outbox as sent
    try:
        await js.publish("checkout.payment", msgpack.encode(msg))
        await db.set(_saga_outbox_key(saga_id), b"1")
    except Exception as e:
        # NATS publish failed - recovery on restart will re-publish (commit exists, outbox missing)
        app.logger.error(f"Failed to publish checkout.payment for saga {saga_id}: {e}")
        # still return 202: the saga is committed and will be recovered on restart
        return Response("Checkout initiated (pending delivery)", status=202)

    return Response("Checkout initiated", status=202)


if __name__ == '__main__':
    app.run(host="0.0.0.0", port=8000, debug=True)
else:
    logging.basicConfig(level=logging.DEBUG)
