import sqlite3
from pathlib import Path
from typing import Dict, List, Optional

import pandas as pd


class InventoryManager:
    def __init__(self, db_path: str):
        self.db_path = db_path

    def _conn(self):
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        return conn

    def init_db(self):
        with self._conn() as conn:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS products (
                    id INTEGER PRIMARY KEY,
                    name TEXT NOT NULL UNIQUE,
                    category TEXT NOT NULL,
                    price REAL NOT NULL,
                    stock INTEGER NOT NULL,
                    threshold INTEGER NOT NULL DEFAULT 5,
                    demand_score REAL NOT NULL DEFAULT 1.0
                )
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS cart (
                    user_id TEXT NOT NULL,
                    product_id INTEGER NOT NULL,
                    qty INTEGER NOT NULL,
                    PRIMARY KEY (user_id, product_id),
                    FOREIGN KEY (product_id) REFERENCES products(id)
                )
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS orders (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    user_id TEXT NOT NULL,
                    total REAL NOT NULL,
                    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
                )
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS order_items (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    order_id INTEGER NOT NULL,
                    product_id INTEGER NOT NULL,
                    qty INTEGER NOT NULL,
                    unit_price REAL NOT NULL,
                    FOREIGN KEY (order_id) REFERENCES orders(id),
                    FOREIGN KEY (product_id) REFERENCES products(id)
                )
                """
            )

    def seed_products_from_csv(self, csv_path: str):
        df = pd.read_csv(csv_path)
        with self._conn() as conn:
            for _, row in df.iterrows():
                conn.execute(
                    """
                    INSERT INTO products(id, name, category, price, stock, threshold, demand_score)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(id) DO UPDATE SET
                        name=excluded.name,
                        category=excluded.category,
                        price=excluded.price,
                        stock=excluded.stock,
                        threshold=excluded.threshold,
                        demand_score=excluded.demand_score
                    """,
                    (
                        int(row["product_id"]),
                        str(row["name"]).lower().strip(),
                        str(row["category"]).lower().strip(),
                        float(row["price"]),
                        int(row["stock"]),
                        int(row.get("threshold", 5)),
                        float(row.get("demand_score", 1.0)),
                    ),
                )

    def list_products(self) -> List[Dict]:
        with self._conn() as conn:
            rows = conn.execute(
                "SELECT id, name, category, price, stock, threshold, demand_score FROM products ORDER BY name"
            ).fetchall()
        return [dict(r) for r in rows]

    def find_product_by_name(self, name: str) -> Optional[Dict]:
        with self._conn() as conn:
            row = conn.execute(
                "SELECT id, name, category, price, stock, threshold, demand_score FROM products WHERE name = ?",
                (name.lower().strip(),),
            ).fetchone()
        return dict(row) if row else None

    def add_cart_item(self, user_id: str, product_id: int, qty: int):
        qty = max(int(qty), 1)
        with self._conn() as conn:
            row = conn.execute(
                "SELECT qty FROM cart WHERE user_id = ? AND product_id = ?",
                (user_id, product_id),
            ).fetchone()
            if row:
                conn.execute(
                    "UPDATE cart SET qty = qty + ? WHERE user_id = ? AND product_id = ?",
                    (qty, user_id, product_id),
                )
            else:
                conn.execute(
                    "INSERT INTO cart(user_id, product_id, qty) VALUES (?, ?, ?)",
                    (user_id, product_id, qty),
                )

    def remove_cart_item(self, user_id: str, product_id: int, qty: int):
        qty = max(int(qty), 1)
        with self._conn() as conn:
            row = conn.execute(
                "SELECT qty FROM cart WHERE user_id = ? AND product_id = ?",
                (user_id, product_id),
            ).fetchone()
            if not row:
                return
            remaining = int(row["qty"]) - qty
            if remaining <= 0:
                conn.execute(
                    "DELETE FROM cart WHERE user_id = ? AND product_id = ?",
                    (user_id, product_id),
                )
            else:
                conn.execute(
                    "UPDATE cart SET qty = ? WHERE user_id = ? AND product_id = ?",
                    (remaining, user_id, product_id),
                )

    def get_cart(self, user_id: str) -> List[Dict]:
        with self._conn() as conn:
            rows = conn.execute(
                """
                SELECT c.product_id, p.name, p.price, c.qty, (p.price * c.qty) AS line_total
                FROM cart c JOIN products p ON p.id = c.product_id
                WHERE c.user_id = ?
                ORDER BY p.name
                """,
                (user_id,),
            ).fetchall()
        return [dict(r) for r in rows]

    def place_order(self, user_id: str) -> Dict:
        with self._conn() as conn:
            conn.execute("BEGIN")
            cart_rows = conn.execute(
                "SELECT product_id, qty FROM cart WHERE user_id = ?",
                (user_id,),
            ).fetchall()
            if not cart_rows:
                conn.execute("ROLLBACK")
                return {"ok": False, "message": "Cart is empty"}

            total = 0.0
            for r in cart_rows:
                prod = conn.execute(
                    "SELECT stock, price FROM products WHERE id = ?",
                    (int(r["product_id"]),),
                ).fetchone()
                if not prod or int(prod["stock"]) < int(r["qty"]):
                    conn.execute("ROLLBACK")
                    return {"ok": False, "message": "Insufficient stock"}
                total += float(prod["price"]) * int(r["qty"])

            cur = conn.execute("INSERT INTO orders(user_id, total) VALUES (?, ?)", (user_id, total))
            order_id = int(cur.lastrowid)

            for r in cart_rows:
                prod = conn.execute(
                    "SELECT price FROM products WHERE id = ?",
                    (int(r["product_id"]),),
                ).fetchone()
                conn.execute(
                    "UPDATE products SET stock = stock - ? WHERE id = ?",
                    (int(r["qty"]), int(r["product_id"])),
                )
                conn.execute(
                    "INSERT INTO order_items(order_id, product_id, qty, unit_price) VALUES (?, ?, ?, ?)",
                    (order_id, int(r["product_id"]), int(r["qty"]), float(prod["price"])),
                )

            conn.execute("DELETE FROM cart WHERE user_id = ?", (user_id,))
            conn.execute("COMMIT")
        return {"ok": True, "order_id": order_id, "total": round(total, 2)}

    def low_stock_alerts(self) -> List[Dict]:
        with self._conn() as conn:
            rows = conn.execute(
                "SELECT id, name, stock, threshold FROM products WHERE stock < threshold ORDER BY stock ASC"
            ).fetchall()
        return [dict(r) for r in rows]


def default_db_path() -> str:
    root = Path(__file__).resolve().parents[1]
    return str(root / "data" / "grocery_ai.db")
