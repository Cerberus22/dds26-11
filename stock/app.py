import logging
import os
import uuid
from contextlib import asynccontextmanager

from redis.asyncio import Redis
from redis.exceptions import RedisError

from msgspec import msgpack, Struct
from fastapi import FastAPI, HTTPException
from fastapi.responses import PlainTextResponse


DB_ERROR_STR = "DB error"

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("stock-service")

db: Redis = Redis(host=os.environ['REDIS_HOST'],
                  port=int(os.environ['REDIS_PORT']),
                  password=os.environ['REDIS_PASSWORD'],
                  db=int(os.environ['REDIS_DB']))


@asynccontextmanager
async def lifespan(app: FastAPI):
    yield
    await db.aclose()


app = FastAPI(lifespan=lifespan)


class StockValue(Struct):
    stock: int
    price: int


async def get_item_from_db(item_id: str) -> StockValue | None:
    # get serialized data
    try:
        entry: bytes = await db.get(item_id)
    except RedisError:
        raise HTTPException(status_code=400, detail=DB_ERROR_STR)
    # deserialize data if it exists else return null
    entry: StockValue | None = msgpack.decode(entry, type=StockValue) if entry else None
    if entry is None:
        # if item does not exist in the database; abort
        raise HTTPException(status_code=400, detail=f"Item: {item_id} not found!")
    return entry


@app.post('/item/create/{price}')
async def create_item(price: int):
    key = str(uuid.uuid4())
    logger.debug(f"Item: {key} created")
    value = msgpack.encode(StockValue(stock=0, price=int(price)))
    try:
        await db.set(key, value)
    except RedisError:
        raise HTTPException(status_code=400, detail=DB_ERROR_STR)
    return {'item_id': key}


@app.post('/batch_init/{n}/{starting_stock}/{item_price}')
async def batch_init_users(n: int, starting_stock: int, item_price: int):
    n = int(n)
    starting_stock = int(starting_stock)
    item_price = int(item_price)
    kv_pairs: dict[str, bytes] = {f"{i}": msgpack.encode(StockValue(stock=starting_stock, price=item_price))
                                  for i in range(n)}
    try:
        await db.mset(kv_pairs)
    except RedisError:
        raise HTTPException(status_code=400, detail=DB_ERROR_STR)
    return {"msg": "Batch init for stock successful"}


@app.get('/find/{item_id}')
async def find_item(item_id: str):
    item_entry: StockValue = await get_item_from_db(item_id)
    return {
        "stock": item_entry.stock,
        "price": item_entry.price
    }


@app.post('/add/{item_id}/{amount}')
async def add_stock(item_id: str, amount: int):
    item_entry: StockValue = await get_item_from_db(item_id)
    # update stock, serialize and update database
    item_entry.stock += int(amount)
    try:
        await db.set(item_id, msgpack.encode(item_entry))
    except RedisError:
        raise HTTPException(status_code=400, detail=DB_ERROR_STR)
    return PlainTextResponse(f"Item: {item_id} stock updated to: {item_entry.stock}")


@app.post('/subtract/{item_id}/{amount}')
async def remove_stock(item_id: str, amount: int):
    item_entry: StockValue = await get_item_from_db(item_id)
    # update stock, serialize and update database
    item_entry.stock -= int(amount)
    logger.debug(f"Item: {item_id} stock updated to: {item_entry.stock}")
    if item_entry.stock < 0:
        raise HTTPException(status_code=400, detail=f"Item: {item_id} stock cannot get reduced below zero!")
    try:
        await db.set(item_id, msgpack.encode(item_entry))
    except RedisError:
        raise HTTPException(status_code=400, detail=DB_ERROR_STR)
    return PlainTextResponse(f"Item: {item_id} stock updated to: {item_entry.stock}")
