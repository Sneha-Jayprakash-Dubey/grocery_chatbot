import pickle
import re
from pathlib import Path
from typing import Dict, Optional

from .cart import add_to_cart, remove_from_cart
from .context import UserContext
from .inventory import InventoryManager
from .recommender import Recommender


NUM_WORDS = {
    "one": 1,
    "two": 2,
    "three": 3,
    "four": 4,
    "five": 5,
}


class GroceryChatbot:
    def __init__(self, intent_model_path: str, inventory: InventoryManager, recommender: Recommender):
        with open(intent_model_path, "rb") as fp:
            self.intent_model = pickle.load(fp)
        self.inventory = inventory
        self.recommender = recommender

    def predict_intent(self, text: str) -> str:
        return str(self.intent_model.predict([text])[0])

    def _extract_qty(self, text: str) -> int:
        match = re.search(r"\b(\d+)\b", text)
        if match:
            return max(int(match.group(1)), 1)
        for word, val in NUM_WORDS.items():
            if re.search(rf"\b{word}\b", text):
                return val
        return 1

    def _extract_product(self, text: str) -> Optional[str]:
        text_low = text.lower().strip()
        products = self.inventory.list_products()
        for p in products:
            name = p["name"].lower().strip()
            if re.search(rf"\b{re.escape(name)}\b", text_low):
                return name
            if name.endswith("s"):
                singular = name[:-1]
                if singular and re.search(rf"\b{re.escape(singular)}\b", text_low):
                    return name
            if re.search(rf"\b{re.escape(name + 's')}\b", text_low):
                return name
        return None

    def _is_context_followup(self, text: str) -> bool:
        text_low = text.lower()
        followup_tokens = ("more", "same", "again", "another", "it", "them")
        return any(re.search(rf"\b{tok}\b", text_low) for tok in followup_tokens)

    def handle(self, user_text: str, ctx: UserContext) -> Dict:
        text = user_text.strip()
        intent = self.predict_intent(text)
        ctx.last_intent = intent

        if intent == "greeting":
            return {"intent": intent, "reply": "Hello! Tell me what groceries you need."}

        if intent == "show_products":
            products = self.inventory.list_products()
            names = ", ".join(p["name"] for p in products[:12])
            return {"intent": intent, "reply": f"Available products: {names}"}

        if intent in {"add_to_cart", "remove_from_cart"}:
            qty = self._extract_qty(text)
            item = self._extract_product(text)

            # Context fallback for incomplete follow-ups like "add 2 more".
            if not item and self._is_context_followup(text) and ctx.last_item:
                item = ctx.last_item

            if not item:
                return {
                    "intent": intent,
                    "reply": "Which product should I update in the cart?",
                    "context": {"last_item": ctx.last_item},
                }

            prod = self.inventory.find_product_by_name(item)
            if not prod:
                return {"intent": intent, "reply": f"I could not find {item} in inventory."}

            if intent == "add_to_cart":
                add_to_cart(ctx.cart, item, qty)
                self.inventory.add_cart_item("default_user", int(prod["id"]), qty)
                ctx.last_item = item
                recs = self.recommender.recommend([item], k=3)
                rec_names = ", ".join(
                    f"{r['name']} ({r.get('similarity_score', 0.0):.2f})" for r in recs
                )
                return {
                    "intent": intent,
                    "reply": f"Added {qty} x {item}. You may also like: {rec_names}",
                    "cart": ctx.cart,
                }

            remove_from_cart(ctx.cart, item, qty)
            self.inventory.remove_cart_item("default_user", int(prod["id"]), qty)
            ctx.last_item = item
            return {
                "intent": intent,
                "reply": f"Removed {qty} x {item} from cart.",
                "cart": ctx.cart,
            }

        if intent == "budget_query":
            return {
                "intent": intent,
                "reply": "Share your budget amount like `budget 300` and I will optimize your cart.",
            }

        return {"intent": intent, "reply": "I did not understand that. Please rephrase."}


def default_intent_model_path() -> str:
    root = Path(__file__).resolve().parents[1]
    return str(root / "ml" / "models" / "intent_model.pkl")
