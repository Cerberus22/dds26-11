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
logger = logging.getLogger("payment-service")

db: Redis = Redis(host=os.environ['REDIS_HOST'],
                  port=int(os.environ['REDIS_PORT']),
                  password=os.environ['REDIS_PASSWORD'],
                  db=int(os.environ['REDIS_DB']))


@asynccontextmanager
async def lifespan(app: FastAPI):
    yield
    await db.aclose()


app = FastAPI(lifespan=lifespan)


class UserValue(Struct):
    credit: int


async def get_user_from_db(user_id: str) -> UserValue | None:
    try:
        # get serialized data
        entry: bytes = await db.get(user_id)
    except RedisError:
        raise HTTPException(status_code=400, detail=DB_ERROR_STR)
    # deserialize data if it exists else return null
    entry: UserValue | None = msgpack.decode(entry, type=UserValue) if entry else None
    if entry is None:
        # if user does not exist in the database; abort
        raise HTTPException(status_code=400, detail=f"User: {user_id} not found!")
    return entry


@app.post('/create_user')
async def create_user():
    key = str(uuid.uuid4())
    value = msgpack.encode(UserValue(credit=0))
    try:
        await db.set(key, value)
    except RedisError:
        raise HTTPException(status_code=400, detail=DB_ERROR_STR)
    return {'user_id': key}


@app.post('/batch_init/{n}/{starting_money}')
async def batch_init_users(n: int, starting_money: int):
    n = int(n)
    starting_money = int(starting_money)
    kv_pairs: dict[str, bytes] = {f"{i}": msgpack.encode(UserValue(credit=starting_money))
                                  for i in range(n)}
    try:
        await db.mset(kv_pairs)
    except RedisError:
        raise HTTPException(status_code=400, detail=DB_ERROR_STR)
    return {"msg": "Batch init for users successful"}


@app.get('/find_user/{user_id}')
async def find_user(user_id: str):
    user_entry: UserValue = await get_user_from_db(user_id)
    return {
        "user_id": user_id,
        "credit": user_entry.credit
    }


@app.post('/add_funds/{user_id}/{amount}')
async def add_credit(user_id: str, amount: int):
    user_entry: UserValue = await get_user_from_db(user_id)
    # update credit, serialize and update database
    user_entry.credit += int(amount)
    try:
        await db.set(user_id, msgpack.encode(user_entry))
    except RedisError:
        raise HTTPException(status_code=400, detail=DB_ERROR_STR)
    return PlainTextResponse(f"User: {user_id} credit updated to: {user_entry.credit}")


@app.post('/pay/{user_id}/{amount}')
async def remove_credit(user_id: str, amount: int):
    logger.debug(f"Removing {amount} credit from user: {user_id}")
    user_entry: UserValue = await get_user_from_db(user_id)
    # update credit, serialize and update database
    user_entry.credit -= int(amount)
    if user_entry.credit < 0:
        raise HTTPException(status_code=400, detail=f"User: {user_id} credit cannot get reduced below zero!")
    try:
        await db.set(user_id, msgpack.encode(user_entry))
    except RedisError:
        raise HTTPException(status_code=400, detail=DB_ERROR_STR)
    return PlainTextResponse(f"User: {user_id} credit updated to: {user_entry.credit}")
