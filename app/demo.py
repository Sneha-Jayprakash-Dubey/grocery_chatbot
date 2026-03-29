from pathlib import Path

from app.budget import BudgetOptimizer
from app.chatbot import GroceryChatbot
from app.context import UserContext
from app.inventory import InventoryManager
from app.recommender import Recommender


def main():
    root = Path(__file__).resolve().parents[1]
    db_path = str(root / "data" / "grocery_ai.db")
    products_path = str(root / "data" / "products.csv")
    intent_path = str(root / "ml" / "models" / "intent_model.pkl")
    rec_path = str(root / "ml" / "models" / "recommender.pkl")
    budget_path = str(root / "ml" / "models" / "regression.pkl")

    inv = InventoryManager(db_path)
    inv.init_db()
    inv.seed_products_from_csv(products_path)

    bot = GroceryChatbot(intent_path, inv, Recommender(rec_path))
    budget = BudgetOptimizer(budget_path)
    ctx = UserContext()

    print("Chat 1:", bot.handle("hello", ctx))
    print("Chat 2:", bot.handle("add apple", ctx))
    print("Chat 3:", bot.handle("add 2 more", ctx))
    print("Cart:", inv.get_cart("default_user"))
    print("Budget plan:", budget.optimize_cart(inv.list_products(), 300, "fruits"))
    print("Checkout:", inv.place_order("default_user"))
    print("Admin alerts:", inv.low_stock_alerts())


if __name__ == "__main__":
    main()
