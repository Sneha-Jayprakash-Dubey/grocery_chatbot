import re
from pathlib import Path

from flask import Flask, jsonify, request

from .budget import BudgetOptimizer, default_model_path as budget_model_path
from .chatbot import GroceryChatbot, default_intent_model_path
from .context import UserContext
from .inventory import InventoryManager, default_db_path
from .recommender import Recommender, default_model_path as rec_model_path

ROOT = Path(__file__).resolve().parents[1]

inventory = InventoryManager(default_db_path())
inventory.init_db()
inventory.seed_products_from_csv(str(ROOT / "data" / "products.csv"))

chatbot = GroceryChatbot(
    intent_model_path=default_intent_model_path(),
    inventory=inventory,
    recommender=Recommender(rec_model_path()),
)
budget_optimizer = BudgetOptimizer(budget_model_path())
ctx = UserContext()

flask_app = Flask(__name__)


@flask_app.get("/health")
def health():
    return jsonify({"ok": True})


@flask_app.post("/chat")
def chat():
    payload = request.get_json(silent=True) or {}
    message = str(payload.get("message", "")).strip()
    if not message:
        return jsonify({"error": "message is required"}), 400
    response = chatbot.handle(message, ctx)
    return jsonify(response)


@flask_app.get("/cart")
def cart():
    rows = inventory.get_cart("default_user")
    total = round(sum(float(r["line_total"]) for r in rows), 2)
    return jsonify({"items": rows, "total": total})


@flask_app.post("/cart/checkout")
def checkout():
    result = inventory.place_order("default_user")
    return jsonify(result), (200 if result.get("ok") else 400)


@flask_app.post("/budget/optimize")
def optimize_budget():
    payload = request.get_json(silent=True) or {}
    budget = float(payload.get("budget", 0))
    if budget <= 0:
        return jsonify({"error": "budget must be > 0"}), 400
    preferred_category = payload.get("preferred_category")
    result = budget_optimizer.optimize_cart(
        inventory.list_products(),
        budget=budget,
        preferred_category=preferred_category,
    )
    return jsonify(result)


@flask_app.get("/admin")
def admin():
    return jsonify(
        {
            "products": inventory.list_products(),
            "low_stock_alerts": inventory.low_stock_alerts(),
        }
    )


@flask_app.post("/chat/demo-context")
def demo_context():
    first = chatbot.handle("add apples", ctx)
    second = chatbot.handle("add 2 more", ctx)
    return jsonify({"step1": first, "step2": second, "context": ctx.__dict__})


if __name__ == "__main__":
    flask_app.run(debug=True)
