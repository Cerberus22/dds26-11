import logging
import os
import random
import time
import uuid
from collections import defaultdict

import aiohttp
import nats
from redis.asyncio import Redis
from redis.exceptions import RedisError

from dds_db.transaction import async_transactional
from dds_db import db, transactional
from msgspec import msgpack, Struct
from quart import Quart, jsonify, abort, Response

from common.messages import CheckoutRequest, CheckoutResult

import asyncio

# decide_locks: dict[str, asyncio.Lock] = defaultdict(asyncio.Lock)

DB_ERROR_STR = "DB error"
REQ_ERROR_STR = "Requests error"

GATEWAY_URL = os.environ['GATEWAY_URL']
NATS_URL = os.environ['NATS_URL']

app = Quart("order-service")

# db: Redis = Redis(host=os.environ['REDIS_HOST'],
#                   port=int(os.environ['REDIS_PORT']),
#                   password=os.environ['REDIS_PASSWORD'],
#                   db=int(os.environ['REDIS_DB']))

# def close_db_connection():
#     db.close()

async def recover_pending_transactions():
    async for key in db.raw.scan_iter("txn:*"):
        txn = msgpack.decode(await db.get(key), type=dict)
        status = txn["status"]
        txn_id = key.decode().split(":")[1]
        items = dict(txn.get("items", []))
        if status == "COMMITTING":
            await js.publish("checkout.2pc.stock.commit", msgpack.encode(
                CheckoutRequest(txn_id=txn_id, order_id=txn["order_id"], user_id="", total_cost=0, items=items)
            ))
            await js.publish("checkout.2pc.payment.commit", msgpack.encode(
                CheckoutRequest(txn_id=txn_id, order_id=txn["order_id"], user_id="", total_cost=0, items=items)
            ))
            await db.set(key, msgpack.encode({"status": "DONE"}))
        elif status in ("PREPARING", "ABORTING"):
            await js.publish("checkout.2pc.stock.abort", msgpack.encode(
                CheckoutRequest(txn_id=txn_id, order_id=txn["order_id"], user_id="", total_cost=0, items=items)
            ))
            await js.publish("checkout.2pc.payment.abort", msgpack.encode(
                CheckoutRequest(txn_id=txn_id, order_id=txn["order_id"], user_id="", total_cost=0, items=items)
            ))
            await db.set(key, msgpack.encode({"status": "ABORTED"}))

# atexit.register(close_db_connection)

nc: nats.NATS | None = None
js = None
session: aiohttp.ClientSession | None = None

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


# Handle checkout votes for 2PC
# track votes per txn
# txn_votes: dict[str, dict] = defaultdict(dict)

async def handle_stock_vote(msg):
    result: CheckoutResult = msgpack.decode(msg.data, type=CheckoutResult)
    txn_id = result.txn_id

    txn_key = f"txn:{txn_id}"
    txn_raw = await db.get(txn_key)

    if txn_raw is None:
        return

    txn = msgpack.decode(txn_raw, type=dict)
    if txn["status"] != "PREPARING":
        return
    
    txn["stock_vote"] = result.success
    await db.set(txn_key, msgpack.encode(txn))
    await maybe_decide(txn_id)

async def handle_payment_vote(msg):
    result: CheckoutResult = msgpack.decode(msg.data, type=CheckoutResult)
    txn_id = result.txn_id

    txn_key = f"txn:{txn_id}"
    txn_raw = await db.get(txn_key)

    if txn_raw is None:
        return

    txn = msgpack.decode(txn_raw, type=dict)
    if txn["status"] != "PREPARING":
        return
    
    txn["payment_vote"] = result.success
    await db.set(txn_key, msgpack.encode(txn))
    await maybe_decide(txn_id)

async def maybe_decide(txn_id: str):
    txn_key = f"txn:{txn_id}"
    txn_raw = await db.get(txn_key)

    if txn_raw is None:
        return

    txn = msgpack.decode(txn_raw, type=dict)

    if txn["status"] != "PREPARING":
        return

    stock_vote = txn.get("stock_vote")
    payment_vote = txn.get("payment_vote")

    if stock_vote is None or payment_vote is None:
        return

    if stock_vote and payment_vote:
        await commit_2pc(txn_id, txn)
    else:
        await abort_2pc(txn_id, txn)

async def handle_checkout_result(msg):
    result: CheckoutResult = msgpack.decode(msg.data, type=CheckoutResult)
    if not result.success:
        app.logger.warning(f"Checkout failed for order {result.order_id}: {result.error}")
        return
    try:
        entry: bytes = await db.get(result.order_id)
        if not entry:
            app.logger.error(f"Order {result.order_id} not found in DB on checkout result")
            return
        order_entry: OrderValue = msgpack.decode(entry, type=OrderValue)
        order_entry.paid = True
        await db.set(result.order_id, msgpack.encode(order_entry))
        app.logger.info(f"Order {result.order_id} marked as paid")
    except RedisError as e:
        app.logger.error(f"DB error marking order {result.order_id} as paid: {e}")

@app.before_serving
async def startup():
    global nc, js, session
    session = aiohttp.ClientSession()
    nc = await nats.connect(NATS_URL)
    js = nc.jetstream()
    await ensure_stream()
    await js.subscribe(
        "checkout.result",
        durable="order-checkout",
        queue="order-checkout",
        cb=handle_checkout_result,
    
    )
    await js.subscribe(
        "checkout.2pc.stock.vote", 
        durable="order-stock-vote", 
        queue="order-stock-vote",
        cb=handle_stock_vote,
    
    )
    await js.subscribe(
        "checkout.2pc.payment.vote", 
        durable="order-payment-vote",
        queue="order-payment-vote", 
        cb=handle_payment_vote,
    
    )

    # upon (re)starting, check for transactions that were incomplete and finalise them.
    await recover_pending_transactions()


@app.after_serving
async def shutdown():
    await session.close()
    await nc.drain()
    await db.raw.aclose()


@app.post('/create/<user_id>')
@async_transactional
async def create_order(user_id: str):
    key = str(uuid.uuid4())
    value = msgpack.encode(OrderValue(paid=False, items=[], user_id=user_id, total_cost=0))
    try:
        await db.set(key, value)
    except RedisError:
        return await abort(400, DB_ERROR_STR)
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
        return await abort(400, DB_ERROR_STR)
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
        await abort(400, REQ_ERROR_STR)
    else:
        return response


async def send_get_request(url: str):
    try:
        response = await session.get(url)
    except aiohttp.ClientError:
        await abort(400, REQ_ERROR_STR)
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
        await abort(400, f"Item: {item_id} does not exist!")
    item_json: dict = await item_reply.json()
    order_entry.items.append((item_id, int(quantity)))
    order_entry.total_cost += int(quantity) * item_json["price"]
    try:
        await db.set(order_id, msgpack.encode(order_entry))
    except RedisError:
        return await abort(400, DB_ERROR_STR)
    return Response(f"Item: {item_id} added to: {order_id} price updated to: {order_entry.total_cost}",
                    status=200)

# 2PC
@app.post('/checkout/2pc/<order_id>')
async def checkout_2pc(order_id: str):
    order_entry: OrderValue = await get_order_from_db(order_id)
    if order_entry.paid:
        return Response(200, f"Order {order_id} already paid")

    items_quantities: dict[str, int] = defaultdict(int)
    for item_id, quantity in order_entry.items:
        items_quantities[item_id] += quantity

    txn_id = str(uuid.uuid4())

    await db.set(f"txn:{txn_id}", msgpack.encode({
        "status": "PREPARING",
        "order_id": order_id,
        "user_id": order_entry.user_id,
        "total_cost": order_entry.total_cost,
        "items": list(items_quantities.items()),
        "stock_vote": None,
        "payment_vote": None,
    }))

    msg = CheckoutRequest(
        txn_id=txn_id,
        order_id=order_id,
        user_id=order_entry.user_id,
        total_cost=order_entry.total_cost,
        items=dict(items_quantities),
    )

    # publish prepare to both services simultaneously
    await js.publish("checkout.2pc.stock.prepare", msgpack.encode(msg))
    await js.publish("checkout.2pc.payment.prepare", msgpack.encode(msg))

    return Response("Checkout initiated", status=202)

async def commit_2pc(txn_id: str, txn: dict):
    txn_key = f"txn:{txn_id}"

    # mark transaction as committing
    txn["status"] = "COMMITTING"
    await db.set(txn_key, msgpack.encode(txn))

    items = dict(txn.get("items", []))

    msg = CheckoutRequest(
        txn_id=txn_id,
        order_id=txn["order_id"],
        user_id=txn["user_id"],
        total_cost=txn["total_cost"],
        items=items,
    )

    # send commit decision
    await js.publish("checkout.2pc.stock.commit", msgpack.encode(msg))
    await js.publish("checkout.2pc.payment.commit", msgpack.encode(msg))

    # mark order as paid
    entry = await db.get(txn["order_id"])
    if entry:
        order_entry: OrderValue = msgpack.decode(entry, type=OrderValue)
        order_entry.paid = True
        await db.set(txn["order_id"], msgpack.encode(order_entry))

    # mark transaction done
    txn["status"] = "DONE"
    await db.set(txn_key, msgpack.encode(txn))


async def abort_2pc(txn_id: str, txn: dict):
    txn_key = f"txn:{txn_id}"

    txn["status"] = "ABORTING"
    await db.set(txn_key, msgpack.encode(txn))

    items = dict(txn.get("items", []))

    msg = CheckoutRequest(
        txn_id=txn_id,
        order_id=txn["order_id"],
        user_id=txn["user_id"],
        total_cost=txn["total_cost"],
        items=items,
    )

    # send abort decision
    await js.publish("checkout.2pc.stock.abort", msgpack.encode(msg))
    await js.publish("checkout.2pc.payment.abort", msgpack.encode(msg))

    txn["status"] = "ABORTED"
    await db.set(txn_key, msgpack.encode(txn))

if __name__ == '__main__':
    app.run(host="0.0.0.0", port=8000, debug=True)
else:
    logging.basicConfig(level=logging.INFO)
