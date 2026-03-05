from msgspec import Struct


class CheckoutInitiate(Struct):
    order_id: str


class CheckoutRequest(Struct):
    order_id: str
    user_id: str
    total_cost: int
    items: dict[str, int]  # {item_id: quantity}


class CheckoutResult(Struct):
    order_id: str
    success: bool
    error: str  # empty string on success
