# AI-Powered Grocery Assistant (Local ML Only)

This module adds a full local ML pipeline and runtime system:

- Intent classification (TF-IDF + Logistic Regression)
- Context-aware chatbot flow
- Content-based recommendation engine
- Budget optimization (regression + greedy cart builder)
- Inventory/cart/order logic with low-stock alerts

## Project layout

- `ml/generate_training_data.py`
- `ml/train_intent.py`
- `ml/train_recommender.py`
- `ml/train_budget.py`
- `ml/train_all.py`
- `ml/models/` (generated)
- `app/app.py` (Flask endpoints)
- `app/chatbot.py`, `app/context.py`, `app/recommender.py`, `app/budget.py`
- `app/inventory.py`, `app/cart.py`, `app/demo.py`
- `data/products.csv`
- `data/training_data.json`

## Setup

```bash
pip install -r requirements.txt
```

## Train all models

```bash
python ml/train_all.py
```

This prints:

- Intent: Accuracy, Precision, Recall, F1, 5-fold CV F1
- Recommender: Precision@5
- Budget: RMSE, R2, 5-fold CV RMSE and CV R2

It also saves evaluation artifacts to `ml/models/`:

- `intent_metrics.json`
- `recommender_metrics.json` (includes similarity score statistics)
- `budget_metrics.json`

## Run API

```bash
python -m app.app
```

Endpoints:

- `POST /chat` with `{"message":"add apple"}`
- `GET /cart`
- `POST /cart/checkout`
- `POST /budget/optimize` with `{"budget":300,"preferred_category":"fruits"}`
- `GET /admin`

## Context behavior

- Message: `add apples` sets `last_item=apple`
- Next message: `add 2 more` reuses `last_item` and updates cart

## Example local demo

```bash
python -m app.demo
```
