import pickle
from pathlib import Path
from typing import Iterable, List, Optional

import numpy as np
import pandas as pd


class BudgetOptimizer:
    def __init__(self, model_path: str):
        with open(model_path, "rb") as fp:
            self.model = pickle.load(fp)

    def optimize_cart(
        self,
        products: Iterable[dict],
        budget: float,
        preferred_category: Optional[str] = None,
    ) -> dict:
        df = pd.DataFrame(products).copy()
        if df.empty:
            return {"items": [], "total": 0.0, "remaining": float(budget)}
        if "product_id" not in df.columns and "id" in df.columns:
            df["product_id"] = df["id"]

        df["budget"] = np.float64(budget)
        df["preferred_category"] = (preferred_category or "none").lower().strip()
        df["price"] = df["price"].astype(np.float64)
        df["demand_score"] = df["demand_score"].astype(np.float64)
        df["category"] = df["category"].astype(str).str.lower().str.strip()

        features = df[["budget", "price", "demand_score", "category", "preferred_category"]]
        predicted_qty = self.model.predict(features).astype(np.float64)
        predicted_qty = np.clip(predicted_qty, 0.0, 6.0)

        safe_price = np.maximum(df["price"].to_numpy(dtype=np.float64), np.float64(1e-6))
        utility = predicted_qty * (1.0 + df["demand_score"].to_numpy(dtype=np.float64)) / safe_price
        df["utility"] = utility
        df["predicted_qty"] = np.maximum(np.rint(predicted_qty), 1).astype(int)

        chosen: List[dict] = []
        total = np.float64(0.0)
        for _, row in df.sort_values("utility", ascending=False).iterrows():
            qty = min(int(row["predicted_qty"]), int(row.get("stock", 10)))
            if qty <= 0:
                continue
            line_cost = np.float64(row["price"]) * np.float64(qty)
            if total + line_cost > np.float64(budget):
                max_affordable = int((np.float64(budget) - total) // np.float64(row["price"]))
                qty = min(max_affordable, qty)
                line_cost = np.float64(row["price"]) * np.float64(qty)
            if qty <= 0:
                continue
            chosen.append(
                {
                    "product_id": int(row["product_id"]),
                    "name": str(row["name"]),
                    "qty": int(qty),
                    "unit_price": float(row["price"]),
                    "line_total": float(line_cost),
                }
            )
            total += line_cost
            if total >= np.float64(budget):
                break

        return {
            "items": chosen,
            "total": float(round(total, 2)),
            "remaining": float(round(np.float64(budget) - total, 2)),
        }


def default_model_path() -> str:
    root = Path(__file__).resolve().parents[1]
    return str(root / "ml" / "models" / "regression.pkl")
