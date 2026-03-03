import logging
import os
import random
import uuid
from collections import defaultdict
from contextlib import asynccontextmanager

import aiohttp
from redis.asyncio import Redis
from redis.exceptions import RedisError

from msgspec import msgpack, Struct
from fastapi import FastAPI, HTTPException
from fastapi.responses import PlainTextResponse


DB_ERROR_STR = "DB error"
REQ_ERROR_STR = "Requests error"

GATEWAY_URL = os.environ['GATEWAY_URL']

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("order-service")

db: Redis = Redis(host=os.environ['REDIS_HOST'],
                  port=int(os.environ['REDIS_PORT']),
                  password=os.environ['REDIS_PASSWORD'],
                  db=int(os.environ['REDIS_DB']))

session: aiohttp.ClientSession | None = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    global session
    session = aiohttp.ClientSession()
    yield
    await session.close()
    await db.aclose()


app = FastAPI(lifespan=lifespan)


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
        raise HTTPException(status_code=400, detail=DB_ERROR_STR)
    # deserialize data if it exists else return null
    entry: OrderValue | None = msgpack.decode(entry, type=OrderValue) if entry else None
    if entry is None:
        # if order does not exist in the database; abort
        raise HTTPException(status_code=400, detail=f"Order: {order_id} not found!")
    return entry


@app.post('/create/{user_id}')
async def create_order(user_id: str):
    key = str(uuid.uuid4())
    value = msgpack.encode(OrderValue(paid=False, items=[], user_id=user_id, total_cost=0))
    try:
        await db.set(key, value)
    except RedisError:
        raise HTTPException(status_code=400, detail=DB_ERROR_STR)
    return {'order_id': key}


@app.post('/batch_init/{n}/{n_items}/{n_users}/{item_price}')
async def batch_init_users(n: int, n_items: int, n_users: int, item_price: int):

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
        raise HTTPException(status_code=400, detail=DB_ERROR_STR)
    return {"msg": "Batch init for orders successful"}


@app.get('/find/{order_id}')
async def find_order(order_id: str):
    order_entry: OrderValue = await get_order_from_db(order_id)
    return {
        "order_id": order_id,
        "paid": order_entry.paid,
        "items": order_entry.items,
        "user_id": order_entry.user_id,
        "total_cost": order_entry.total_cost
    }


async def send_post_request(url: str):
    try:
        response = await session.post(url)
    except aiohttp.ClientError:
        raise HTTPException(status_code=400, detail=REQ_ERROR_STR)
    return response


async def send_get_request(url: str):
    try:
        response = await session.get(url)
    except aiohttp.ClientError:
        raise HTTPException(status_code=400, detail=REQ_ERROR_STR)
    return response


@app.post('/addItem/{order_id}/{item_id}/{quantity}')
async def add_item(order_id: str, item_id: str, quantity: int):
    order_entry: OrderValue = await get_order_from_db(order_id)
    item_reply = await send_get_request(f"{GATEWAY_URL}/stock/find/{item_id}")
    if item_reply.status != 200:
        # Request failed because item does not exist
        raise HTTPException(status_code=400, detail=f"Item: {item_id} does not exist!")
    item_json: dict = await item_reply.json()
    order_entry.items.append((item_id, int(quantity)))
    order_entry.total_cost += int(quantity) * item_json["price"]
    try:
        await db.set(order_id, msgpack.encode(order_entry))
    except RedisError:
        raise HTTPException(status_code=400, detail=DB_ERROR_STR)
    return PlainTextResponse(f"Item: {item_id} added to: {order_id} price updated to: {order_entry.total_cost}")


async def rollback_stock(removed_items: list[tuple[str, int]]):
    for item_id, quantity in removed_items:
        await send_post_request(f"{GATEWAY_URL}/stock/add/{item_id}/{quantity}")


@app.post('/checkout/{order_id}')
async def checkout(order_id: str):
    logger.debug(f"Checking out {order_id}")
    order_entry: OrderValue = await get_order_from_db(order_id)
    # get the quantity per item
    items_quantities: dict[str, int] = defaultdict(int)
    for item_id, quantity in order_entry.items:
        items_quantities[item_id] += quantity
    # The removed items will contain the items that we already have successfully subtracted stock from
    # for rollback purposes.
    removed_items: list[tuple[str, int]] = []
    for item_id, quantity in items_quantities.items():
        stock_reply = await send_post_request(f"{GATEWAY_URL}/stock/subtract/{item_id}/{quantity}")
        if stock_reply.status != 200:
            # If one item does not have enough stock we need to rollback
            await rollback_stock(removed_items)
            raise HTTPException(status_code=400, detail=f'Out of stock on item_id: {item_id}')
        removed_items.append((item_id, quantity))
    user_reply = await send_post_request(f"{GATEWAY_URL}/payment/pay/{order_entry.user_id}/{order_entry.total_cost}")
    if user_reply.status != 200:
        # If the user does not have enough credit we need to rollback all the item stock subtractions
        await rollback_stock(removed_items)
        raise HTTPException(status_code=400, detail="User out of credit")
    order_entry.paid = True
    try:
        await db.set(order_id, msgpack.encode(order_entry))
    except RedisError:
        raise HTTPException(status_code=400, detail=DB_ERROR_STR)
    logger.debug("Checkout successful")
    return PlainTextResponse("Checkout successful")
