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

app = Quart("stock-service")

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


class StockCompensation(Struct):
    order_id: str
    previous_stock: dict[str, int]


class StockValue(Struct):
    stock: int
    price: int


async def get_item_from_db(item_id: str) -> StockValue:
    # get serialized data
    try:
        entry: bytes = await db.get(item_id)
    except RedisError:
        raise RedisError(DB_ERROR_STR)
    # deserialize data if it exists else return null
    entry: StockValue | None = msgpack.decode(entry, type=StockValue) if entry else None
    if entry is None:
        # if item does not exist in the database; raise
        raise ValueError(f"Item: {item_id} not found!")
    return entry


async def ensure_stream():
    try:
        await js.add_stream(name="CHECKOUT", subjects=["checkout.>"])
    except Exception:
        pass  # stream already exists
    
    try:
        await js.add_stream(name="STOCK", subjects=["stock.>"])
    except Exception:
        pass  # stream already exists

async def handle_checkout_stock(msg):
    req: CheckoutRequest = msgpack.decode(msg.data, type=CheckoutRequest)

    # check for duplicate messages
    if await db.exists(_saga_commit_key(req.saga_id)):
        app.logger.info(f"Duplicate checkout.stock for saga {req.saga_id}, skipping")
        return

    # 1. write saga:started
    try:
        await db.set(_saga_started_key(req.saga_id), req.order_id)
    except RedisError as e:
        app.logger.error(f"Failed to write saga:started for {req.saga_id}: {e}")
        await js.publish("stock.result", msgpack.encode(
            CheckoutResult(saga_id=req.saga_id, order_id=req.order_id, success=False, error=str(e))
        ))
        return

    # 2. validate all items and compute new values - read-only, no DB writes yet.
    # do preparation before the next step.
    updated_items: dict[str, bytes] = {}
    previous_stock: dict[str, int] = {}
    for item_id, quantity in req.items.items():
        try:
            item_entry = await get_item_from_db(item_id)
        except (ValueError, RedisError) as e:
            await db.delete(_saga_started_key(req.saga_id))
            await js.publish("stock.result", msgpack.encode(
                CheckoutResult(saga_id=req.saga_id, order_id=req.order_id, success=False, error=str(e))
            ))
            return
        if item_entry.stock < quantity:
            await db.delete(_saga_started_key(req.saga_id))
            await js.publish("stock.result", msgpack.encode(
                CheckoutResult(saga_id=req.saga_id, order_id=req.order_id, success=False,
                               error=f"Item: {item_id} stock cannot get reduced below zero!")
            ))
            return
        previous_stock[item_id] = item_entry.stock
        item_entry.stock -= quantity
        app.logger.debug(f"Item: {item_id} stock will be updated to: {item_entry.stock}")
        updated_items[item_id] = msgpack.encode(item_entry)

    # 3. pipeline: saga:compensation + all item updates + saga:commit atomically.
    #    saga:compensation stores the exact pre-mutation values (snapshot) so that rollback can restore to the correct value.
    #    saga:commit stores the saga_id
    compensation = StockCompensation(order_id=req.order_id, previous_stock=previous_stock)
    success_result = CheckoutResult(saga_id=req.saga_id, order_id=req.order_id, success=True, error="")
    try:
        pipe = db.pipeline(transaction=True)
        pipe.set(_saga_compensation_key(req.saga_id), msgpack.encode(compensation))
        for item_id, encoded in updated_items.items():
            pipe.set(item_id, encoded)
        pipe.set(_saga_commit_key(req.saga_id), b"1")
        await pipe.execute()
    except RedisError as e:
        app.logger.error(f"Failed to commit stock pipeline for saga {req.saga_id}: {e}")
        await db.delete(_saga_started_key(req.saga_id))
        await js.publish("stock.result", msgpack.encode(
            CheckoutResult(saga_id=req.saga_id, order_id=req.order_id, success=False, error=str(e))
        ))
        return

    # 4. publish result, then mark outbox as sent
    try:
        await js.publish("stock.result", msgpack.encode(success_result))
        await db.set(_saga_outbox_key(req.saga_id), b"1")
    except Exception as e:
        app.logger.error(f"Failed to publish stock.result for saga {req.saga_id}: {e}")
        # commit exists, outbox missing - recovery will re-publish on restart


async def rollback_stock(previous_stock: dict[str, int]):
    await _rollback_stock_items(previous_stock)


async def _rollback_stock_items(previous_stock: dict[str, int]):
    for item_id, stock_value in previous_stock.items():
        try:
            item_entry = await get_item_from_db(item_id)
            item_entry.stock = stock_value
            await db.set(item_id, msgpack.encode(item_entry))
        except Exception as e:
            app.logger.error(f"Rollback failed for item {item_id}: {e}")


async def _cleanup_saga(saga_id: str):
    try:
        await db.delete(
            _saga_started_key(saga_id),
            _saga_compensation_key(saga_id),
            _saga_commit_key(saga_id),
            _saga_outbox_key(saga_id),
        )
    except RedisError as e:
        app.logger.warning(f"Could not clean up saga keys for {saga_id}: {e}")


async def recover_sagas():
    app.logger.info("Stock service: scanning for incomplete sagas...")
    try:
        cursor = 0
        while True:
            cursor, keys = await db.scan(cursor, match="saga:started:*", count=100)
            for key in keys:
                saga_id = key.decode().removeprefix("saga:started:")
                commit_exists = await db.exists(_saga_commit_key(saga_id))
                outbox_exists = await db.exists(_saga_outbox_key(saga_id))

                if not commit_exists:
                    # crashed before the pipeline - no item DB writes happened, just clean up
                    app.logger.warning(f"Recovery: saga {saga_id} has no commit, cleaning up (no stock was written)")
                    await _cleanup_saga(saga_id)

                elif not outbox_exists:
                    # crashed after pipeline committed but before/during NATS publish - re-publish
                    app.logger.warning(f"Recovery: saga {saga_id} committed but outbox missing, re-publishing")
                    try:
                        comp_bytes = await db.get(_saga_compensation_key(saga_id))
                        if comp_bytes:
                            comp: StockCompensation = msgpack.decode(comp_bytes, type=StockCompensation)
                            success_result = CheckoutResult(
                                saga_id=saga_id, order_id=comp.order_id, success=True, error=""
                            )
                            await js.publish("stock.result", msgpack.encode(success_result))
                            await db.set(_saga_outbox_key(saga_id), b"1")
                            app.logger.info(f"Recovery: re-published stock.result for saga {saga_id}")
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
        "checkout.stock",
        durable="stock-checkout",
        queue="stock-checkout",
        cb=handle_checkout_stock,
    )


@app.after_serving
async def shutdown():
    await nc.drain()
    await db.aclose()


@app.post('/item/create/<price>')
async def create_item(price: int):
    key = str(uuid.uuid4())
    app.logger.debug(f"Item: {key} created")
    value = msgpack.encode(StockValue(stock=0, price=int(price)))
    try:
        await db.set(key, value)
    except RedisError:
        return abort(400, DB_ERROR_STR)
    return jsonify({'item_id': key})


@app.post('/batch_init/<n>/<starting_stock>/<item_price>')
async def batch_init_users(n: int, starting_stock: int, item_price: int):
    n = int(n)
    starting_stock = int(starting_stock)
    item_price = int(item_price)
    kv_pairs: dict[str, bytes] = {f"{i}": msgpack.encode(StockValue(stock=starting_stock, price=item_price))
                                  for i in range(n)}
    try:
        await db.mset(kv_pairs)
    except RedisError:
        return abort(400, DB_ERROR_STR)
    return jsonify({"msg": "Batch init for stock successful"})


@app.get('/find/<item_id>')
async def find_item(item_id: str):
    try:
        item_entry: StockValue = await get_item_from_db(item_id)
    except (ValueError, RedisError) as e:
        return abort(400, str(e))
    return jsonify(
        {
            "stock": item_entry.stock,
            "price": item_entry.price
        }
    )


@app.post('/add/<item_id>/<amount>')
async def add_stock(item_id: str, amount: int):
    try:
        item_entry: StockValue = await get_item_from_db(item_id)
    except (ValueError, RedisError) as e:
        return abort(400, str(e))
    # update stock, serialize and update database
    item_entry.stock += int(amount)
    try:
        await db.set(item_id, msgpack.encode(item_entry))
    except RedisError:
        return abort(400, DB_ERROR_STR)
    return Response(f"Item: {item_id} stock updated to: {item_entry.stock}", status=200)


@app.post('/subtract/<item_id>/<amount>')
async def remove_stock(item_id: str, amount: int):
    try:
        item_entry: StockValue = await get_item_from_db(item_id)
    except (ValueError, RedisError) as e:
        return abort(400, str(e))
    # update stock, serialize and update database
    item_entry.stock -= int(amount)
    app.logger.debug(f"Item: {item_id} stock updated to: {item_entry.stock}")
    if item_entry.stock < 0:
        abort(400, f"Item: {item_id} stock cannot get reduced below zero!")
    try:
        await db.set(item_id, msgpack.encode(item_entry))
    except RedisError:
        return abort(400, DB_ERROR_STR)
    return Response(f"Item: {item_id} stock updated to: {item_entry.stock}", status=200)


if __name__ == '__main__':
    app.run(host="0.0.0.0", port=8000, debug=True)
else:
    logging.basicConfig(level=logging.DEBUG)
