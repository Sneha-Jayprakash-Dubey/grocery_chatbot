from typing import Dict


def add_to_cart(cart: Dict[str, int], item: str, qty: int) -> Dict[str, int]:
    qty = max(int(qty), 1)
    cart[item] = int(cart.get(item, 0) + qty)
    return cart


def remove_from_cart(cart: Dict[str, int], item: str, qty: int) -> Dict[str, int]:
    if item not in cart:
        return cart
    qty = max(int(qty), 1)
    remaining = int(cart[item] - qty)
    if remaining <= 0:
        cart.pop(item, None)
    else:
        cart[item] = remaining
    return cart
