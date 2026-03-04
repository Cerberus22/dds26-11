from msgspec import Struct

class OrderValue(Struct):
    paid: bool | None
    items: list[tuple[str, int]]
    user_id: str
    total_cost: int

class StockValue(Struct):
    stock: int
    price: int

class UserValue(Struct):
    credit: int


_message_registry: list[type] = []
def Message(cls: type) -> type:
    _message_registry.append(cls)
    return cls

@Message
class DoesUserHaveEnoughCreditForOrder(Struct):
    saga_id: str
    user_id: str
    order: OrderValue

@Message
class EnoughStock(Struct):
    saga_id: str
    order: OrderValue

@Message
class EverythingGood(Struct):
    saga_id: str

@Message
class EverythingBad(Struct):
    saga_id: str
    reason: str