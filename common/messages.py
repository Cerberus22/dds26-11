from msgspec import Struct


# Data Classes
class OrderValue(Struct):
    paid: bool
    items: list[tuple[str, int]]
    user_id: str
    total_cost: int


class UserValue(Struct):
    credit: int


class StockValue(Struct):
    stock: int
    price: int


class CheckoutOrderRequest(Struct):
    message_id: str
    request_id: str
    order_id: str


class CheckoutRequest(Struct):
    saga_id: str  # idempotency key
    message_id: str
    request_id: str
    order_id: str
    user_id: str
    total_cost: int
    items: dict[str, int]  # {item_id: quantity}


class CheckoutResult(Struct):
    saga_id: str  # idempotency key
    message_id: str
    request_id: str
    order_id: str
    success: bool
    error: str  # empty string on success


# Order Service Messages
class OrderCreateRequest(Struct):
    message_id: str
    request_id: str
    user_id: str


class OrderCreateResult(Struct):
    message_id: str
    request_id: str
    order_id: str
    error: str


class OrderBatchInitRequest(Struct):
    message_id: str
    request_id: str
    n: int
    n_items: int
    n_users: int
    item_price: int


class OrderBatchInitResult(Struct):
    message_id: str
    request_id: str
    success: bool
    error: str


class OrderFindRequest(Struct):
    message_id: str
    request_id: str
    order_id: str


class OrderFindResult(Struct):
    message_id: str
    request_id: str
    order_id: str
    order: OrderValue | None
    error: str


class OrderAddItemRequest(Struct):
    message_id: str
    request_id: str
    order_id: str
    item_id: str
    quantity: int


class OrderAddItemResult(Struct):
    message_id: str
    request_id: str
    order_id: str
    total_cost: int
    error: str


# Payment Service Messages
class PaymentCreateUserRequest(Struct):
    message_id: str
    request_id: str


class PaymentCreateUserResult(Struct):
    message_id: str
    request_id: str
    user_id: str
    error: str


class PaymentBatchInitRequest(Struct):
    message_id: str
    request_id: str
    n: int
    starting_money: int


class PaymentBatchInitResult(Struct):
    message_id: str
    request_id: str
    success: bool
    error: str


class PaymentFindUserRequest(Struct):
    message_id: str
    request_id: str
    user_id: str


class PaymentFindUserResult(Struct):
    message_id: str
    request_id: str
    user_id: str
    user: UserValue | None
    error: str


class PaymentAddFundsRequest(Struct):
    message_id: str
    request_id: str
    user_id: str
    amount: int


class PaymentAddFundsResult(Struct):
    message_id: str
    request_id: str
    user_id: str
    credit: int
    error: str


class PaymentRemoveCreditRequest(Struct):
    message_id: str
    request_id: str
    user_id: str
    amount: int


class PaymentRemoveCreditResult(Struct):
    message_id: str
    request_id: str
    user_id: str
    credit: int
    error: str


# Stock Service Messages
class StockCreateItemRequest(Struct):
    message_id: str
    request_id: str
    price: int


class StockCreateItemResult(Struct):
    message_id: str
    request_id: str
    item_id: str
    error: str


class StockBatchInitRequest(Struct):
    message_id: str
    request_id: str
    n: int
    starting_stock: int
    item_price: int


class StockBatchInitResult(Struct):
    message_id: str
    request_id: str
    success: bool
    error: str


class StockFindItemRequest(Struct):
    message_id: str
    request_id: str
    item_id: str


class StockFindItemResult(Struct):
    message_id: str
    request_id: str
    item_id: str
    item: StockValue | None
    error: str


class StockAddAmountRequest(Struct):
    message_id: str
    request_id: str
    item_id: str
    amount: int


class StockAddAmountResult(Struct):
    message_id: str
    request_id: str
    item_id: str
    stock: int
    error: str


class StockSubtractAmountRequest(Struct):
    message_id: str
    request_id: str
    item_id: str
    amount: int


class StockSubtractAmountResult(Struct):
    message_id: str
    request_id: str
    item_id: str
    stock: int
    error: str
