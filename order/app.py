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

# import atexit

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


# def close_db_connection():
#     db.close()

async def recover_pending_transactions():
    async for key in db.scan_iter("txn:*"):
        txn = msgpack.decode(await db.get(key), type=dict)
        status = txn["status"]
        txn_id = key.decode().split(":")[1]
        items = dict(txn.get("items", []))
        if status == "COMMITTING":
            await js.publish("checkout.2pc.stock.commit", msgpack.encode(
                CheckoutRequest(order_id=txn["order_id"], user_id="", total_cost=0, items=items)
            ))
            await js.publish("checkout.2pc.payment.commit", msgpack.encode(
                CheckoutRequest(order_id=txn["order_id"], user_id="", total_cost=0, items=items)
            ))
            await db.set(key, msgpack.encode({"status": "DONE"}))
        elif status in ("PREPARING", "ABORTING"):
            await js.publish("checkout.2pc.stock.abort", msgpack.encode(
                CheckoutRequest(order_id=txn["order_id"], user_id="", total_cost=0, items=items)
            ))
            await js.publish("checkout.2pc.payment.abort", msgpack.encode(
                CheckoutRequest(order_id=txn["order_id"], user_id="", total_cost=0, items=items)
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
txn_votes: dict[str, dict] = defaultdict(dict)

async def handle_stock_vote(msg):
    result: CheckoutResult = msgpack.decode(msg.data, type=CheckoutResult)
    txn_id = result.order_id  # we'll use order_id as txn key for now
    txn_votes[txn_id]["stock"] = result.success

    await maybe_decide(txn_id, result.order_id)

async def handle_payment_vote(msg):
    result: CheckoutResult = msgpack.decode(msg.data, type=CheckoutResult)
    txn_id = result.order_id
    txn_votes[txn_id]["payment"] = result.success

    await maybe_decide(txn_id, result.order_id)

async def maybe_decide(txn_id: str, order_id: str):
    votes = txn_votes.get(txn_id, {})
    if "stock" not in votes or "payment" not in votes:
        return  # still waiting for the other vote

    if votes["stock"] and votes["payment"]:
        await db.set(f"txn:{txn_id}", msgpack.encode({"status": "COMMITTING", "order_id": order_id}))
        await js.publish("checkout.2pc.stock.commit", msgpack.encode(CheckoutRequest(
            order_id=order_id, user_id="", total_cost=0, items={}
        )))
        await js.publish("checkout.2pc.payment.commit", msgpack.encode(CheckoutRequest(
            order_id=order_id, user_id="", total_cost=0, items={}
        )))
        entry = await db.get(order_id)
        order_entry: OrderValue = msgpack.decode(entry, type=OrderValue)
        order_entry.paid = True
        await db.set(order_id, msgpack.encode(order_entry))
        await db.set(f"txn:{txn_id}", msgpack.encode({"status": "DONE"}))
    else:
        await db.set(f"txn:{txn_id}", msgpack.encode({"status": "ABORTING", "order_id": order_id}))
        await js.publish("checkout.2pc.stock.abort", msgpack.encode(CheckoutRequest(
            order_id=order_id, user_id="", total_cost=0, items={}
        )))
        await js.publish("checkout.2pc.payment.abort", msgpack.encode(CheckoutRequest(
            order_id=order_id, user_id="", total_cost=0, items={}
        )))
        await db.set(f"txn:{txn_id}", msgpack.encode({"status": "ABORTED"}))

    del txn_votes[txn_id]

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
        cb=handle_stock_vote,
    
    )
    await js.subscribe(
        "checkout.2pc.payment.vote", 
        durable="order-payment-vote", 
        cb=handle_payment_vote,
    
    )
    await js.subscribe("checkout.2pc.stock.prepare", durable="stock-2pc-prepare", cb=handle_2pc_prepare,)
    await js.subscribe("checkout.2pc.stock.commit", durable="stock-2pc-commit", cb=handle_2pc_commit,)
    await js.subscribe("checkout.2pc.stock.abort", durable="stock-2pc-abort", cb=handle_2pc_abort,)
    
    # upon (re)starting, check for transactions that were incomplete and finalise them.
    recover_pending_transactions()


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

# Simple 2PC
async def handle_2pc_prepare(msg):
    req: CheckoutRequest = msgpack.decode(msg.data, type=CheckoutRequest)
    for item_id, quantity in req.items.items():
        try:
            item_entry = await get_item_from_db(item_id)
        except (ValueError, RedisError) as e:
            await js.publish("checkout.2pc.stock.vote", msgpack.encode(
                CheckoutResult(order_id=req.order_id, success=False, error=str(e))
            ))
            return
        if item_entry.stock < quantity:
            await js.publish("checkout.2pc.stock.vote", msgpack.encode(
                CheckoutResult(order_id=req.order_id, success=False, error=f"Insufficient stock for {item_id}")
            ))
            return
        item_entry.stock -= quantity
        await db.set(item_id, msgpack.encode(item_entry))
        await db.set(f"pending:{req.order_id}:{item_id}", msgpack.encode({
            "item_id": item_id, "quantity": quantity
        }))

    await js.publish("checkout.2pc.stock.vote", msgpack.encode(
        CheckoutResult(order_id=req.order_id, success=True, error="")
    ))

async def handle_2pc_commit(msg):
    req: CheckoutRequest = msgpack.decode(msg.data, type=CheckoutRequest)
    async for key in db.scan_iter(f"pending:{req.order_id}:*"):
        await db.delete(key)

async def handle_2pc_abort(msg):
    req: CheckoutRequest = msgpack.decode(msg.data, type=CheckoutRequest)
    async for key in db.scan_iter(f"pending:{req.order_id}:*"):
        raw = await db.get(key)
        if not raw:
            continue
        pending = msgpack.decode(raw, type=dict)
        item_entry = await get_item_from_db(pending["item_id"])
        item_entry.stock += pending["quantity"]
        await db.set(pending["item_id"], msgpack.encode(item_entry))
        await db.delete(key)

@app.post('/checkout/2pc/<order_id>')
async def checkout_2pc(order_id: str):
    order_entry: OrderValue = await get_order_from_db(order_id)
    if order_entry.paid:
        return abort(400, f"Order {order_id} already paid")

    items_quantities: dict[str, int] = defaultdict(int)
    for item_id, quantity in order_entry.items:
        items_quantities[item_id] += quantity

    txn_id = str(uuid.uuid4())

    await db.set(f"txn:{txn_id}", msgpack.encode({
        "status": "PREPARING",
        "order_id": order_id,
        "user_id": order_entry.user_id,
        "total_cost": order_entry.total_cost,
        "items": list(items_quantities.items())
    }))

    msg = CheckoutRequest(
        order_id=order_id,
        user_id=order_entry.user_id,
        total_cost=order_entry.total_cost,
        items=dict(items_quantities),
    )

    # publish prepare to both services simultaneously
    await js.publish("checkout.2pc.stock.prepare", msgpack.encode(msg))
    await js.publish("checkout.2pc.payment.prepare", msgpack.encode(msg))

    return Response("Checkout initiated", status=202)

# SAGA Order Service:
# Publishes: ReservePayment, CompensatePayment, ReserveStock, CompensateStock
# Subscribes to: PaymentReserved, PaymentFailed, StockReserved, StockFailed
saga_states = {}  # In-memory state tracking, could be persisted in Redis

def handle_payment_reserved_event(event_data: dict):
    txn_id = event_data["txn_id"]
    
    try:
        # Get saga state from Redis
        saga_state_raw = db.get(f"saga:{txn_id}")
        if not saga_state_raw:
            app.logger.error(f"Saga state not found for txn {txn_id}")
            return
        
        saga_state = msgpack.decode(saga_state_raw, type=dict)
        items = saga_state["items"]
        
        saga_state["status"] = "RESERVING_STOCK"
        db.set(f"saga:{txn_id}", msgpack.encode(saga_state))
        
        # Publish: ReserveStock
        items_list = [{"item_id": item_id, "quantity": quantity} for item_id, quantity in items]
        # event_bus.publish("ReserveStock", {"txn_id": txn_id, "items": items_list})
        app.logger.info(f"Published ReserveStock for txn {txn_id}")
        
    except Exception as e:
        app.logger.error(f"Error handling PaymentReserved for txn {txn_id}: {str(e)}")

def handle_payment_failed_event(event_data: dict):
    txn_id = event_data["txn_id"]
    reason = event_data.get("reason", "Unknown")
    
    try:
        saga_state = {"status": "FAILED_AT_PAYMENT", "reason": reason}
        db.set(f"saga:{txn_id}", msgpack.encode(saga_state))
        app.logger.info(f"Saga {txn_id} failed at payment: {reason}")
    except Exception as e:
        app.logger.error(f"Error handling PaymentFailed for txn {txn_id}: {str(e)}")

def handle_stock_reserved_event(event_data: dict):
    txn_id = event_data["txn_id"]
    
    try:
        # Get saga state from Redis
        saga_state_raw = db.get(f"saga:{txn_id}")
        if not saga_state_raw:
            app.logger.error(f"Saga state not found for txn {txn_id}")
            return
        
        saga_state = msgpack.decode(saga_state_raw, type=dict)
        order_id = saga_state["order_id"]
        
        saga_state["status"] = "COMPLETED"
        db.set(f"saga:{txn_id}", msgpack.encode(saga_state))
        
        order_entry: OrderValue = get_order_from_db(order_id)
        order_entry.paid = True
        db.set(order_id, msgpack.encode(order_entry))
        
        # Note: reserved keys are in Payment/Stock services, not here
        # They can be cleaned up by those services or left for garbage collection
        
        app.logger.info(f"Saga {txn_id} completed successfully")
        
    except Exception as e:
        app.logger.error(f"Error handling StockReserved for txn {txn_id}: {str(e)}")

def handle_stock_failed_event(event_data: dict):
    txn_id = event_data["txn_id"]
    reason = event_data.get("reason", "Unknown")
    
    try:
        # Get saga state from Redis
        saga_state_raw = db.get(f"saga:{txn_id}")
        if not saga_state_raw:
            app.logger.error(f"Saga state not found for txn {txn_id}")
            return
        
        saga_state = msgpack.decode(saga_state_raw, type=dict)
        
        saga_state["status"] = "COMPENSATING"
        saga_state["reason"] = reason
        db.set(f"saga:{txn_id}", msgpack.encode(saga_state))
        
        # Publish: CompensatePayment
        # event_bus.publish("CompensatePayment", {"txn_id": txn_id})
        app.logger.info(f"Published CompensatePayment for txn {txn_id}")
        
        saga_state["status"] = "COMPENSATED"
        db.set(f"saga:{txn_id}", msgpack.encode(saga_state))
        
    except Exception as e:
        app.logger.error(f"Error handling StockFailed for txn {txn_id}: {str(e)}")

# event_bus.subscribe("PaymentReserved", handle_payment_reserved_event)
# event_bus.subscribe("PaymentFailed", handle_payment_failed_event)
# event_bus.subscribe("StockReserved", handle_stock_reserved_event)
# event_bus.subscribe("StockFailed", handle_stock_failed_event)

@app.post('/checkout/saga/<order_id>')
def checkout_saga(order_id: str):
    order_entry: OrderValue = get_order_from_db(order_id)

    # Check if order has already been processed
    if order_entry.paid:
        return abort(400, "Order already paid")
    
    items_quantities: dict[str, int] = defaultdict(int)
    for item_id, quantity in order_entry.items:
        items_quantities[item_id] += quantity

    txn_id = str(uuid.uuid4())

    # Record saga state
    db.set(f"saga:{txn_id}", msgpack.encode({
        "status": "STARTED",
        "order_id": order_id,
        "user_id": order_entry.user_id,
        "total_cost": order_entry.total_cost,
        "items": list(items_quantities.items())
    }))

    # Step 1: Publish ReservePayment event
    db.set(f"saga:{txn_id}", msgpack.encode({
        "status": "RESERVING_PAYMENT",
        "order_id": order_id,
        "user_id": order_entry.user_id,
        "total_cost": order_entry.total_cost,
        "items": list(items_quantities.items())
    }))
    
    # Publish: ReservePayment
    # event_bus.publish("ReservePayment", {
    #     "txn_id": txn_id,
    #     "user_id": order_entry.user_id,
    #     "amount": order_entry.total_cost
    # })
    app.logger.info(f"Published ReservePayment for txn {txn_id}")

    return Response("Checkout initiated via SAGA", status=200)

if __name__ == '__main__':
    app.run(host="0.0.0.0", port=8000, debug=True)
else:
    logging.basicConfig(level=logging.INFO)
