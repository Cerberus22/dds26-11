from msgspec import Struct


class CheckoutRequest(Struct):
    txn_id: str
    order_id: str
    user_id: str
    total_cost: int
    items: dict[str, int]  # {item_id: quantity}


class CheckoutResult(Struct):
    txn_id: str
    order_id: str
    success: bool
    error: str  # empty string on success
