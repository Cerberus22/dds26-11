import logging
import os
import uuid

import nats
from redis.asyncio import Redis
from redis.exceptions import RedisError

from msgspec import msgpack, Struct
from quart import Quart, jsonify, abort, Response

from common.messages import CheckoutRequest, CheckoutResult

DB_ERROR_STR = "DB error"


app = Quart("payment-service")

db: Redis = Redis(host=os.environ['REDIS_HOST'],
                  port=int(os.environ['REDIS_PORT']),
                  password=os.environ['REDIS_PASSWORD'],
                  db=int(os.environ['REDIS_DB']))

nc: nats.NATS | None = None
js = None

# store payment transactions for potential rollback
payment_transactions: dict[str, tuple[str, int]] = {}  # {order_id: (user_id, amount)}


class UserValue(Struct):
    credit: int


async def get_user_from_db(user_id: str) -> UserValue:
    try:
        # get serialized data
        entry: bytes = await db.get(user_id)
    except RedisError:
        raise RedisError(DB_ERROR_STR)
    # deserialize data if it exists else return null
    entry: UserValue | None = msgpack.decode(entry, type=UserValue) if entry else None
    if entry is None:
        # if user does not exist in the database; raise
        raise ValueError(f"User: {user_id} not found!")
    return entry


async def ensure_stream():
    try:
        await js.add_stream(name="CHECKOUT", subjects=["checkout.>"])
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


async def handle_checkout_payment(msg):
    req: CheckoutRequest = msgpack.decode(msg.data, type=CheckoutRequest)
    try:
        user_entry = await get_user_from_db(req.user_id)
    except (ValueError, RedisError) as e:
        app.logger.warning(f"Payment checkout failed for order {req.order_id}: {e}")
        await js.publish("payment.result", msgpack.encode(
            CheckoutResult(order_id=req.order_id, success=False, error=str(e))
        ))
        return

    # check if user has sufficient credit
    if user_entry.credit < req.total_cost:
        app.logger.warning(f"User: {req.user_id} insufficient credit for order {req.order_id}")
        await js.publish("payment.result", msgpack.encode(
            CheckoutResult(order_id=req.order_id, success=False, error=f"User: {req.user_id} insufficient credit!")
        ))
        return

    # update credit, serialize and update database
    user_entry.credit -= req.total_cost
    app.logger.debug(f"User: {req.user_id} credit updated to: {user_entry.credit}")

    try:
        await db.set(req.user_id, msgpack.encode(user_entry))
    except RedisError as e:
        app.logger.error(f"DB error during payment for order {req.order_id}: {e}")
        await js.publish("payment.result", msgpack.encode(
            CheckoutResult(order_id=req.order_id, success=False, error=str(e))
        ))
        return

    # store transaction for potential rollback
    payment_transactions[req.order_id] = (req.user_id, req.total_cost)
    app.logger.debug(f"Stored payment transaction for order {req.order_id}")
    
    # payment successful - publish success to payment stream
    await js.publish("payment.result", msgpack.encode(
        CheckoutResult(order_id=req.order_id, success=True, error="")
    ))
    
    # forward to stock service
    await js.publish("checkout.stock", msgpack.encode(req))


async def rollback_payment(order_id: str):
    if order_id not in payment_transactions:
        app.logger.error(f"Cannot rollback order {order_id}: no payment transaction found")
        return
    
    user_id, amount = payment_transactions[order_id]
    
    try:
        user_entry = await get_user_from_db(user_id)
        # restore the credit
        user_entry.credit += amount
        await db.set(user_id, msgpack.encode(user_entry))
        app.logger.info(f"Successfully rolled back payment for order {order_id}. User {user_id} credit restored to: {user_entry.credit}")
        # remove from cache
        del payment_transactions[order_id]
    except Exception as e:
        app.logger.error(f"Rollback failed for order {order_id}, user {user_id}: {e}")


async def handle_stock_result(msg):
    result: CheckoutResult = msgpack.decode(msg.data, type=CheckoutResult)
    
    if result.success:
        # stock succeeded, remove from rollback cache
        if result.order_id in payment_transactions:
            del payment_transactions[result.order_id]
            app.logger.info(f"Stock succeeded for order {result.order_id}, payment confirmed")
        return
    
    # stock failed, rollback payment
    app.logger.info(f"Stock failed for order {result.order_id}: {result.error}. Rolling back payment.")
    await rollback_payment(result.order_id)


@app.before_serving
async def startup():
    global nc, js
    nc = await nats.connect(os.environ['NATS_URL'])
    js = nc.jetstream()
    await ensure_stream()
    await js.subscribe(
        "checkout.payment",
        durable="payment-checkout",
        queue="payment-checkout",
        cb=handle_checkout_payment,
    )
    await js.subscribe(
        "stock.result",
        durable="payment-stock-result",
        queue="payment-stock-result",
        cb=handle_stock_result,
    )


@app.after_serving
async def shutdown():
    await nc.drain()
    await db.aclose()


@app.post('/create_user')
async def create_user():
    key = str(uuid.uuid4())
    value = msgpack.encode(UserValue(credit=0))
    try:
        await db.set(key, value)
    except RedisError:
        return abort(400, DB_ERROR_STR)
    return jsonify({'user_id': key})


@app.post('/batch_init/<n>/<starting_money>')
async def batch_init_users(n: int, starting_money: int):
    n = int(n)
    starting_money = int(starting_money)
    kv_pairs: dict[str, bytes] = {f"{i}": msgpack.encode(UserValue(credit=starting_money))
                                  for i in range(n)}
    try:
        await db.mset(kv_pairs)
    except RedisError:
        return abort(400, DB_ERROR_STR)
    return jsonify({"msg": "Batch init for users successful"})


@app.get('/find_user/<user_id>')
async def find_user(user_id: str):
    try:
        user_entry: UserValue = await get_user_from_db(user_id)
    except (ValueError, RedisError) as e:
        return abort(400, str(e))
    return jsonify(
        {
            "user_id": user_id,
            "credit": user_entry.credit
        }
    )


@app.post('/add_funds/<user_id>/<amount>')
async def add_credit(user_id: str, amount: int):
    try:
        user_entry: UserValue = await get_user_from_db(user_id)
    except (ValueError, RedisError) as e:
        return abort(400, str(e))
    # update credit, serialize and update database
    user_entry.credit += int(amount)
    try:
        await db.set(user_id, msgpack.encode(user_entry))
    except RedisError:
        return abort(400, DB_ERROR_STR)
    return Response(f"User: {user_id} credit updated to: {user_entry.credit}", status=200)


@app.post('/pay/<user_id>/<amount>')
async def remove_credit(user_id: str, amount: int):
    app.logger.debug(f"Removing {amount} credit from user: {user_id}")
    try:
        user_entry: UserValue = await get_user_from_db(user_id)
    except (ValueError, RedisError) as e:
        return abort(400, str(e))
    # update credit, serialize and update database
    user_entry.credit -= int(amount)
    if user_entry.credit < 0:
        abort(400, f"User: {user_id} credit cannot get reduced below zero!")
    try:
        await db.set(user_id, msgpack.encode(user_entry))
    except RedisError:
        return abort(400, DB_ERROR_STR)
    return Response(f"User: {user_id} credit updated to: {user_entry.credit}", status=200)


if __name__ == '__main__':
    app.run(host="0.0.0.0", port=8000, debug=True)
else:
    logging.basicConfig(level=logging.DEBUG)
