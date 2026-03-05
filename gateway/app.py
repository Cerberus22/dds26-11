import logging
import os

import aiohttp
import nats
from msgspec import msgpack
from quart import Quart, request, Response

from common.messages import CheckoutInitiate

NATS_URL = os.environ['NATS_URL']
ORDER_URL = os.environ['ORDER_URL']
PAYMENT_URL = os.environ['PAYMENT_URL']
STOCK_URL = os.environ['STOCK_URL']

app = Quart("gateway")

nc: nats.NATS | None = None
js = None
session: aiohttp.ClientSession | None = None


async def ensure_stream():
    try:
        await js.add_stream(name="CHECKOUT", subjects=["checkout.>"])
    except Exception:
        pass  # stream already exists


@app.before_serving
async def startup():
    global nc, js, session
    session = aiohttp.ClientSession()
    nc = await nats.connect(NATS_URL)
    js = nc.jetstream()
    await ensure_stream()


@app.after_serving
async def shutdown():
    await session.close()
    await nc.drain()


@app.post('/orders/checkout/<order_id>')
async def checkout(order_id: str):
    msg = CheckoutInitiate(order_id=order_id)
    await js.publish("checkout.initiate", msgpack.encode(msg))
    return Response("Checkout initiated", status=202)


@app.route('/orders/<path:path>', methods=['GET', 'POST', 'PUT', 'DELETE'])
async def proxy_orders(path: str):
    return await _proxy(f"{ORDER_URL}/{path}")


@app.route('/payment/<path:path>', methods=['GET', 'POST', 'PUT', 'DELETE'])
async def proxy_payment(path: str):
    return await _proxy(f"{PAYMENT_URL}/{path}")


@app.route('/stock/<path:path>', methods=['GET', 'POST', 'PUT', 'DELETE'])
async def proxy_stock(path: str):
    return await _proxy(f"{STOCK_URL}/{path}")


async def _proxy(url: str) -> Response:
    method = request.method
    headers = {k: v for k, v in request.headers if k.lower() != 'host'}
    body = await request.get_data()

    try:
        async with session.request(method, url, headers=headers, data=body) as resp:
            content = await resp.read()
            return Response(content, status=resp.status,
                            content_type=resp.content_type)
    except aiohttp.ClientError as e:
        return Response(str(e), status=502)


if __name__ == '__main__':
    app.run(host="0.0.0.0", port=8000, debug=True)
else:
    logging.basicConfig(level=logging.INFO)
