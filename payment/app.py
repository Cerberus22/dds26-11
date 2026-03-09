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


def _saga_started_key(saga_id: str) -> str:
    return f"saga:started:{saga_id}"

def _saga_compensation_key(saga_id: str) -> str:
    return f"saga:compensation:{saga_id}"

def _saga_commit_key(saga_id: str) -> str:
    return f"saga:commit:{saga_id}"

def _saga_outbox_key(saga_id: str) -> str:
    return f"saga:outbox:{saga_id}"

class PaymentCompensation(Struct):
    user_id: str
    previous_credit: int 
    order_id: str


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

    # check for duplicate messages 
    if await db.exists(_saga_commit_key(req.saga_id)):
        app.logger.info(f"Duplicate checkout.payment for saga {req.saga_id}, skipping")
        return

    # 1. write saga:started
    try:
        await db.set(_saga_started_key(req.saga_id), req.order_id)
    except RedisError as e:
        app.logger.error(f"Failed to write saga:started for {req.saga_id}: {e}")
        await js.publish("payment.result", msgpack.encode(
            CheckoutResult(saga_id=req.saga_id, order_id=req.order_id, success=False, error=str(e))
        ))
        return

    try:
        user_entry = await get_user_from_db(req.user_id)
    except (ValueError, RedisError) as e:
        app.logger.warning(f"Payment checkout failed for saga {req.saga_id}, order {req.order_id}: {e}")
        await db.delete(_saga_started_key(req.saga_id))
        await js.publish("payment.result", msgpack.encode(
            CheckoutResult(saga_id=req.saga_id, order_id=req.order_id, success=False, error=str(e))
        ))
        return

    # 2. validate user existence and credit
    if user_entry.credit < req.total_cost:
        app.logger.warning(f"User: {req.user_id} insufficient credit for saga {req.saga_id}, order {req.order_id}")
        await db.delete(_saga_started_key(req.saga_id))
        await js.publish("payment.result", msgpack.encode(
            CheckoutResult(order_id=req.order_id, success=False,
                           error=f"User: {req.user_id} insufficient credit!", saga_id=req.saga_id)
        ))
        return

    # 3. pipeline: saga:compensation + update db (deduct credit) + saga:commit atomically.
    #    saga:compensation stores the exact pre-deduction credit (snapshot) so that rollback can restore to the correct value.
    #    saga:commit stores the CheckoutRequest so recovery can re-publish checkout.stock.
    compensation = PaymentCompensation(
        user_id=req.user_id,
        previous_credit=user_entry.credit, 
        order_id=req.order_id,
    )
    user_entry.credit -= req.total_cost
    app.logger.debug(f"User: {req.user_id} credit updated to: {user_entry.credit}")
    try:
        pipe = db.pipeline(transaction=True)
        pipe.set(_saga_compensation_key(req.saga_id), msgpack.encode(compensation))
        pipe.set(req.user_id, msgpack.encode(user_entry))
        pipe.set(_saga_commit_key(req.saga_id), msgpack.encode(req))
        await pipe.execute()
    except RedisError as e:
        app.logger.error(f"DB error during payment pipeline for saga {req.saga_id}: {e}")
        await db.delete(_saga_started_key(req.saga_id))
        await js.publish("payment.result", msgpack.encode(
            CheckoutResult(order_id=req.order_id, success=False, error=str(e), saga_id=req.saga_id)
        ))
        return

    # 4. publish payment.result (success) + forward to stock + mark outbox
    try:
        await js.publish("payment.result", msgpack.encode(
            CheckoutResult(order_id=req.order_id, success=True, error="", saga_id=req.saga_id)
        ))
        await js.publish("checkout.stock", msgpack.encode(req))
        await db.set(_saga_outbox_key(req.saga_id), b"1")
    except Exception as e:
        app.logger.error(f"Failed to publish for saga {req.saga_id}: {e}")
        # commit exists, outbox missing - recovery will re-publish on restart


async def handle_stock_result(msg):
    result: CheckoutResult = msgpack.decode(msg.data, type=CheckoutResult)

    # check for duplicate messages
    if await db.exists(_saga_commit_key(result.saga_id) + ":stock-ack"):
        app.logger.info(f"Duplicate stock.result for saga {result.saga_id}, skipping")
        return

    if result.success:
        # stock succeeded - saga complete, clean up
        app.logger.info(f"Stock succeeded for saga {result.saga_id}, order {result.order_id}. Payment confirmed.")
        # mark stock-ack before cleanup
        try:
            await db.set(_saga_commit_key(result.saga_id) + ":stock-ack", b"1")
        except RedisError:
            pass
        await _cleanup_saga(result.saga_id)
        return

    # stock failed - rollback the payment
    app.logger.info(f"Stock failed for saga {result.saga_id}, order {result.order_id}: {result.error}. Rolling back payment.")
    await _rollback_payment(result.saga_id, result.order_id)

async def _rollback_payment(saga_id: str, order_id: str):
    comp_key = _saga_compensation_key(saga_id)
    try:
        comp_bytes = await db.get(comp_key)
        if comp_bytes is None:
            app.logger.warning(f"No compensation data for saga {saga_id}, cannot rollback payment for order {order_id}")
            return
        comp: PaymentCompensation = msgpack.decode(comp_bytes, type=PaymentCompensation)
        user_entry = await get_user_from_db(comp.user_id)
        user_entry.credit = comp.previous_credit
        await db.set(comp.user_id, msgpack.encode(user_entry))
        app.logger.info(f"Payment rolled back for saga {saga_id}: user {comp.user_id} credit restored to {comp.previous_credit}")
    except (ValueError, RedisError) as e:
        app.logger.error(f"Rollback failed for saga {saga_id}: {e}")
    finally:
        await _cleanup_saga(saga_id)


async def _cleanup_saga(saga_id: str):
    try:
        await db.delete(
            _saga_started_key(saga_id),
            _saga_compensation_key(saga_id),
            _saga_commit_key(saga_id),
            _saga_outbox_key(saga_id),
            _saga_commit_key(saga_id) + ":stock-ack",
        )
    except RedisError as e:
        app.logger.warning(f"Could not clean up saga keys for {saga_id}: {e}")


async def recover_sagas():
    app.logger.info("Payment service: scanning for incomplete sagas...")
    try:
        cursor = 0
        while True:
            cursor, keys = await db.scan(cursor, match="saga:started:*", count=100)
            for key in keys:
                saga_id = key.decode().removeprefix("saga:started:")
                commit_exists = await db.exists(_saga_commit_key(saga_id))
                outbox_exists = await db.exists(_saga_outbox_key(saga_id))

                if not commit_exists:
                    # crashed before committing - credit was never deducted, just clean up
                    app.logger.warning(f"Recovery: saga {saga_id} has no commit, cleaning up (no credit deducted)")
                    await _cleanup_saga(saga_id)

                elif not outbox_exists:
                    # crashed after commit but before/during NATS publish - re-publish
                    app.logger.warning(f"Recovery: saga {saga_id} committed but outbox missing, re-publishing")
                    try:
                        outbox_bytes = await db.get(_saga_commit_key(saga_id))
                        if outbox_bytes:
                            req: CheckoutRequest = msgpack.decode(outbox_bytes, type=CheckoutRequest)
                            await js.publish("payment.result", msgpack.encode(
                                CheckoutResult(order_id=req.order_id, success=True, error="", saga_id=saga_id)
                            ))
                            await js.publish("checkout.stock", msgpack.encode(req))
                            await db.set(_saga_outbox_key(saga_id), b"1")
                            app.logger.info(f"Recovery: re-published checkout.stock for saga {saga_id}")
                    except Exception as e:
                        app.logger.error(f"Recovery: failed to re-publish saga {saga_id}: {e}")

            if cursor == 0:
                break
    except RedisError as e:
        app.logger.error(f"Recovery scan failed: {e}")


@app.before_serving
async def startup():
    global nc, js
    nc = await nats.connect(os.environ['NATS_URL'])
    js = nc.jetstream()
    await ensure_stream()
    # run crash recovery before accepting messages
    await recover_sagas()
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
