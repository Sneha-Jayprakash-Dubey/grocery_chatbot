import pickle
from pathlib import Path
import json

import numpy as np
import pandas as pd
from sklearn.compose import ColumnTransformer
from sklearn.linear_model import Ridge
from sklearn.metrics import r2_score, root_mean_squared_error
from sklearn.model_selection import KFold, cross_val_score, train_test_split
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import OneHotEncoder, StandardScaler


def build_training_rows(products: pd.DataFrame, n_users: int = 120) -> pd.DataFrame:
    rng = np.random.default_rng(42)
    categories = products["category"].unique().tolist()
    rows = []
    for user_id in range(1, n_users + 1):
        budget = rng.uniform(150, 1400)
        preferred_cat = rng.choice(categories)
        pref_weight = rng.uniform(0.8, 1.4)
        for _, p in products.iterrows():
            price = float(p["price"])
            demand = float(p["demand_score"])
            cat_bonus = 0.8 if p["category"] == preferred_cat else 0.0
            linear_signal = (
                0.0018 * budget
                - 0.014 * price
                + 1.7 * demand
                + cat_bonus
                + 0.12 * pref_weight
            )
            qty = linear_signal + rng.normal(0.0, 0.22)
            qty = float(np.clip(qty, 0.0, 6.0))
            rows.append(
                {
                    "user_id": user_id,
                    "budget": float(budget),
                    "preferred_category": preferred_cat,
                    "product_id": int(p["product_id"]),
                    "category": p["category"],
                    "price": price,
                    "demand_score": demand,
                    "target_qty": qty,
                }
            )
    return pd.DataFrame(rows)


def main():
    root = Path(__file__).resolve().parents[1]
    products_path = root / "data" / "products.csv"
    model_dir = root / "ml" / "models"
    model_dir.mkdir(parents=True, exist_ok=True)

    products = pd.read_csv(products_path)
    products["price"] = products["price"].astype(np.float64)
    products["demand_score"] = products["demand_score"].astype(np.float64)

    dataset = build_training_rows(products)
    features = ["budget", "price", "demand_score", "category", "preferred_category"]
    target = "target_qty"
    x = dataset[features]
    y = dataset[target].astype(np.float64)

    x_train, x_test, y_train, y_test = train_test_split(
        x, y, test_size=0.2, random_state=42
    )

    preprocessor = ColumnTransformer(
        transformers=[
            ("num", StandardScaler(), ["budget", "price", "demand_score"]),
            (
                "cat",
                OneHotEncoder(handle_unknown="ignore"),
                ["category", "preferred_category"],
            ),
        ],
        remainder="drop",
    )

    model = Pipeline(
        steps=[
            ("prep", preprocessor),
            ("reg", Ridge(alpha=1.0, random_state=42)),
        ]
    )

    model.fit(x_train, y_train)
    pred = model.predict(x_test).astype(np.float64)
    if not np.isfinite(pred).all():
        raise ValueError("Non-finite values found in regression predictions.")

    rmse = root_mean_squared_error(y_test, pred)
    r2 = r2_score(y_test, pred)

    cv = KFold(n_splits=5, shuffle=True, random_state=42)
    cv_rmse = -cross_val_score(model, x, y, cv=cv, scoring="neg_root_mean_squared_error")
    cv_r2 = cross_val_score(model, x, y, cv=cv, scoring="r2")

    print("=== Budget Model Evaluation ===")
    print(f"RMSE: {rmse:.4f}")
    print(f"R2: {r2:.4f}")
    print(f"5-Fold CV RMSE (mean): {cv_rmse.mean():.4f}")
    print(f"5-Fold CV RMSE (std): {cv_rmse.std():.4f}")
    print(f"5-Fold CV R2 (mean): {cv_r2.mean():.4f}")
    print(f"5-Fold CV R2 (std): {cv_r2.std():.4f}")

    with (model_dir / "regression.pkl").open("wb") as fp:
        pickle.dump(model, fp)
    dataset.to_csv(root / "data" / "budget_training.csv", index=False)
    metrics = {
        "rmse": float(rmse),
        "r2": float(r2),
        "cv_rmse_mean": float(cv_rmse.mean()),
        "cv_rmse_std": float(cv_rmse.std()),
        "cv_r2_mean": float(cv_r2.mean()),
        "cv_r2_std": float(cv_r2.std()),
        "num_samples": int(len(dataset)),
    }
    with (model_dir / "budget_metrics.json").open("w", encoding="utf-8") as fp:
        json.dump(metrics, fp, indent=2, ensure_ascii=True)

    print(f"Saved model to: {model_dir / 'regression.pkl'}")
    print(f"Saved metrics to: {model_dir / 'budget_metrics.json'}")


if __name__ == "__main__":
    main()
