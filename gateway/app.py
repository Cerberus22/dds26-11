import logging
import os
import uuid
import aiohttp
import nats
from msgspec import msgpack
from quart import Quart, request, Response, jsonify
import asyncio

from common.messages import *

NATS_URL = os.environ['NATS_URL']
MESSAGE_TIMEOUT = 30.0
MESSAGE_STREAM_SUBJECTS = ["order.*", "payment.*", "stock.*", "checkout.>", "inbox.>"]

app = Quart("gateway")
logger = logging.getLogger(__name__)

nc: nats.NATS | None = None
js = None
session: aiohttp.ClientSession | None = None


async def ensure_stream():
    try:
        await js.add_stream(name="MESSAGES", subjects=MESSAGE_STREAM_SUBJECTS)
    except Exception:
        pass  # stream already exists


async def publish_and_wait_for_response(subject: str, message, response_type):
    """Publish a message and wait for a response using request_id and message_id."""
    request_id = str(uuid.uuid4())
    message_id = str(uuid.uuid4())
    reply_subject = f"inbox.{request_id}"
    
    # Reconstruct message with request_id and message_id
    msg_dict = {field: getattr(message, field) for field in message.__annotations__}
    msg_dict['request_id'] = request_id
    msg_dict['message_id'] = message_id
    message = type(message)(**msg_dict)

    sub = await js.subscribe(reply_subject)
    try:
        await js.publish(subject, msgpack.encode(message))
        response_msg = await sub.next_msg(timeout=MESSAGE_TIMEOUT)
        result = msgpack.decode(response_msg.data, type=response_type)
        if getattr(result, "request_id", None) != request_id:
            raise TimeoutError(f"Correlation mismatch for {subject}")
        return result
    except nats.errors.TimeoutError:
        raise TimeoutError(f"No response for {subject} within {MESSAGE_TIMEOUT} seconds")
    finally:
        await sub.unsubscribe()


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
    msg = CheckoutInitiateRequest(
        message_id="",
        request_id="",
        order_id=order_id,
    )
    try:
        result = await publish_and_wait_for_response("checkout.initiate", msg, CheckoutResult)
        if result.success:
            return jsonify({"message": f"Order {order_id} checked out successfully"}), 200
        return jsonify({"error": result.error}), 400
    except TimeoutError:
        return jsonify({'error': 'Checkout timeout'}), 503
    except Exception as e:
        logger.error(f"Error during checkout for order {order_id}: {e}")
        return jsonify({'error': str(e)}), 500


# Order endpoints
@app.post('/orders/create/<user_id>')
async def create_order(user_id: str):
    msg = OrderCreateRequest(message_id="", request_id="", user_id=user_id)
    
    try:
        result = await publish_and_wait_for_response("order.create", msg, OrderCreateResult)
        if result.error:
            return jsonify({'error': result.error}), 400
        return jsonify({'order_id': result.order_id}), 201
    except TimeoutError:
        return jsonify({'error': 'Order service timeout'}), 503
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.post('/orders/batch_init/<n>/<n_items>/<n_users>/<item_price>')
async def batch_init_orders(n: str, n_items: str, n_users: str, item_price: str):
    msg = OrderBatchInitRequest(
        message_id="",
        request_id="",
        n=int(n),
        n_items=int(n_items),
        n_users=int(n_users),
        item_price=int(item_price),
    )
    
    try:
        result = await publish_and_wait_for_response("order.batch_init", msg, OrderBatchInitResult)
        if result.error:
            return jsonify({'error': result.error}), 400
        return jsonify({'msg': 'Batch init for orders successful'}), 201
    except TimeoutError:
        return jsonify({'error': 'Order service timeout'}), 503
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.get('/orders/find/<order_id>')
async def find_order(order_id: str):
    msg = OrderFindRequest(message_id="", request_id="", order_id=order_id)
    
    try:
        result = await publish_and_wait_for_response("order.find", msg, OrderFindResult)
        if result.error:
            return jsonify({'error': result.error}), 400
        return jsonify({
            'order_id': result.order_id,
            'paid': result.order.paid,
            'items': result.order.items,
            'user_id': result.order.user_id,
            'total_cost': result.order.total_cost
        }), 200
    except TimeoutError:
        return jsonify({'error': 'Order service timeout'}), 503
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.post('/orders/addItem/<order_id>/<item_id>/<quantity>')
async def add_item(order_id: str, item_id: str, quantity: str):
    try:
        add_item_msg = OrderAddItemRequest(
            message_id="",
            request_id="",
            order_id=order_id,
            item_id=item_id,
            quantity=int(quantity),
        )
        
        result = await publish_and_wait_for_response("order.add_item", add_item_msg, OrderAddItemResult)
        if result.error:
            return jsonify({'error': result.error}), 400
        
        return jsonify({
            'message': f'Item: {item_id} added to: {order_id}',
            'total_cost': result.total_cost
        }), 200
    except TimeoutError:
        return jsonify({'error': 'Service timeout'}), 503
    except Exception as e:
        return jsonify({'error': str(e)}), 500


# Payment endpoints
@app.post('/payment/create_user')
async def create_user():
    msg = PaymentCreateUserRequest(message_id="", request_id="")
    
    try:
        result = await publish_and_wait_for_response("payment.create_user", msg, PaymentCreateUserResult)
        if result.error:
            return jsonify({'error': result.error}), 400
        return jsonify({'user_id': result.user_id}), 201
    except TimeoutError:
        return jsonify({'error': 'Payment service timeout'}), 503
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.post('/payment/batch_init/<n>/<starting_money>')
async def batch_init_users(n: str, starting_money: str):
    msg = PaymentBatchInitRequest(
        message_id="",
        request_id="",
        n=int(n),
        starting_money=int(starting_money),
    )
    
    try:
        result = await publish_and_wait_for_response("payment.batch_init", msg, PaymentBatchInitResult)
        if result.error:
            return jsonify({'error': result.error}), 400
        return jsonify({'msg': 'Batch init for users successful'}), 201
    except TimeoutError:
        return jsonify({'error': 'Payment service timeout'}), 503
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.get('/payment/find_user/<user_id>')
async def find_user(user_id: str):
    msg = PaymentFindUserRequest(message_id="", request_id="", user_id=user_id)
    
    try:
        result = await publish_and_wait_for_response("payment.find_user", msg, PaymentFindUserResult)
        if result.error:
            return jsonify({'error': result.error}), 400
        return jsonify({
            'user_id': result.user_id,
            'credit': result.user.credit
        }), 200
    except TimeoutError:
        return jsonify({'error': 'Payment service timeout'}), 503
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.post('/payment/add_funds/<user_id>/<amount>')
async def add_credit(user_id: str, amount: str):
    msg = PaymentAddFundsRequest(
        message_id="",
        request_id="",
        user_id=user_id,
        amount=int(amount),
    )
    
    try:
        result = await publish_and_wait_for_response("payment.add_funds", msg, PaymentAddFundsResult)
        if result.error:
            return jsonify({'error': result.error}), 400
        return jsonify({
            'message': f'User: {user_id} credit updated to: {result.credit}',
            'credit': result.credit
        }), 200
    except TimeoutError:
        return jsonify({'error': 'Payment service timeout'}), 503
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.post('/payment/pay/<user_id>/<amount>')
async def remove_credit(user_id: str, amount: str):
    msg = PaymentRemoveCreditRequest(
        message_id="",
        request_id="",
        user_id=user_id,
        amount=int(amount),
    )
    
    try:
        result = await publish_and_wait_for_response("payment.remove_credit", msg, PaymentRemoveCreditResult)
        if result.error:
            return jsonify({'error': result.error}), 400
        return jsonify({
            'message': f'User: {user_id} credit updated to: {result.credit}',
            'credit': result.credit
        }), 200
    except TimeoutError:
        return jsonify({'error': 'Payment service timeout'}), 503
    except Exception as e:
        return jsonify({'error': str(e)}), 500


# Stock endpoints
@app.post('/stock/item/create/<price>')
async def create_item(price: str):
    msg = StockCreateItemRequest(message_id="", request_id="", price=int(price))
    
    try:
        result = await publish_and_wait_for_response("stock.create_item", msg, StockCreateItemResult)
        if result.error:
            return jsonify({'error': result.error}), 400
        return jsonify({'item_id': result.item_id}), 201
    except TimeoutError:
        return jsonify({'error': 'Stock service timeout'}), 503
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.post('/stock/batch_init/<n>/<starting_stock>/<item_price>')
async def batch_init_items(n: str, starting_stock: str, item_price: str):
    msg = StockBatchInitRequest(
        message_id="",
        request_id="",
        n=int(n),
        starting_stock=int(starting_stock),
        item_price=int(item_price),
    )
    
    try:
        result = await publish_and_wait_for_response("stock.batch_init", msg, StockBatchInitResult)
        if result.error:
            return jsonify({'error': result.error}), 400
        return jsonify({'msg': 'Batch init for stock successful'}), 201
    except TimeoutError:
        return jsonify({'error': 'Stock service timeout'}), 503
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.get('/stock/find/<item_id>')
async def find_item(item_id: str):
    msg = StockFindItemRequest(message_id="", request_id="", item_id=item_id)
    
    try:
        result = await publish_and_wait_for_response("stock.find", msg, StockFindItemResult)
        if result.error:
            return jsonify({'error': result.error}), 400
        return jsonify({
            'item_id': result.item_id,
            'stock': result.stock,
            'price': result.price
        }), 200
    except TimeoutError:
        return jsonify({'error': 'Stock service timeout'}), 503
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.post('/stock/add/<item_id>/<amount>')
async def add_stock(item_id: str, amount: str):
    msg = StockAddAmountRequest(
        message_id="",
        request_id="",
        item_id=item_id,
        amount=int(amount),
    )
    
    try:
        result = await publish_and_wait_for_response("stock.add", msg, StockAddAmountResult)
        if result.error:
            return jsonify({'error': result.error}), 400
        return jsonify({
            'message': f'Item: {item_id} stock updated to: {result.stock}',
            'stock': result.stock
        }), 200
    except TimeoutError:
        return jsonify({'error': 'Stock service timeout'}), 503
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.post('/stock/subtract/<item_id>/<amount>')
async def remove_stock(item_id: str, amount: str):
    msg = StockSubtractAmountRequest(
        message_id="",
        request_id="",
        item_id=item_id,
        amount=int(amount),
    )
    
    try:
        result = await publish_and_wait_for_response("stock.subtract", msg, StockSubtractAmountResult)
        if result.error:
            return jsonify({'error': result.error}), 400
        return jsonify({
            'message': f'Item: {item_id} stock updated to: {result.stock}',
            'stock': result.stock
        }), 200
    except TimeoutError:
        return jsonify({'error': 'Stock service timeout'}), 503
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.get('/wait')
async def wait_endpoint():
    await asyncio.sleep(5)
    return jsonify({}), 200

if __name__ == '__main__':
    app.run(host="0.0.0.0", port=8000, debug=True)
else:
    logging.basicConfig(level=logging.INFO)
