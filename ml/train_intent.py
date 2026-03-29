import json
import pickle
from pathlib import Path
import sys

import numpy as np
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import accuracy_score, classification_report, precision_recall_fscore_support
from sklearn.model_selection import StratifiedKFold, cross_val_score, train_test_split
from sklearn.pipeline import Pipeline

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from ml.text_utils import custom_preprocessor


def load_data(path: Path):
    with path.open("r", encoding="utf-8") as fp:
        rows = json.load(fp)
    x = [row["text"] for row in rows]
    y = [row["intent"] for row in rows]
    return x, y


def main():
    root = Path(__file__).resolve().parents[1]
    data_path = root / "data" / "training_data.json"
    model_dir = root / "ml" / "models"
    model_dir.mkdir(parents=True, exist_ok=True)

    if not data_path.exists():
        raise FileNotFoundError(
            f"{data_path} not found. Run `python ml/generate_training_data.py` first."
        )

    x, y = load_data(data_path)
    x_train, x_test, y_train, y_test = train_test_split(
        x, y, test_size=0.2, random_state=42, stratify=y
    )

    model = Pipeline(
        steps=[
            (
                "tfidf",
                TfidfVectorizer(
                    preprocessor=custom_preprocessor,
                    ngram_range=(1, 2),
                    min_df=1,
                    max_df=0.98,
                    dtype=np.float64,
                ),
            ),
            (
                "clf",
                LogisticRegression(
                    max_iter=2000,
                    solver="lbfgs",
                    class_weight="balanced",
                    random_state=42,
                ),
            ),
        ]
    )

    model.fit(x_train, y_train)
    y_pred = model.predict(x_test)
    y_train_pred = model.predict(x_train)

    accuracy = accuracy_score(y_test, y_pred)
    train_acc = accuracy_score(y_train, y_train_pred)
    precision, recall, f1, _ = precision_recall_fscore_support(
        y_test, y_pred, average="weighted", zero_division=0
    )

    cv = StratifiedKFold(n_splits=5, shuffle=True, random_state=42)
    cv_scores = cross_val_score(model, x, y, cv=cv, scoring="f1_weighted")

    print("=== Intent Model Evaluation ===")
    print(f"Train Accuracy: {train_acc:.4f}")
    print(f"Test Accuracy: {accuracy:.4f}")
    print(f"Precision: {precision:.4f}")
    print(f"Recall: {recall:.4f}")
    print(f"F1-score: {f1:.4f}")
    print(f"5-Fold CV F1 (mean): {cv_scores.mean():.4f}")
    print(f"5-Fold CV F1 (std): {cv_scores.std():.4f}")
    print("\nClassification Report:\n")
    print(classification_report(y_test, y_pred, digits=4, zero_division=0))

    with (model_dir / "intent_model.pkl").open("wb") as fp:
        pickle.dump(model, fp)
    with (model_dir / "vectorizer.pkl").open("wb") as fp:
        pickle.dump(model.named_steps["tfidf"], fp)
    metrics = {
        "train_accuracy": float(train_acc),
        "test_accuracy": float(accuracy),
        "precision_weighted": float(precision),
        "recall_weighted": float(recall),
        "f1_weighted": float(f1),
        "cv_f1_weighted_mean": float(cv_scores.mean()),
        "cv_f1_weighted_std": float(cv_scores.std()),
        "num_samples": int(len(x)),
        "num_classes": int(len(set(y))),
    }
    with (model_dir / "intent_metrics.json").open("w", encoding="utf-8") as fp:
        json.dump(metrics, fp, indent=2, ensure_ascii=True)

    print(f"Saved model to: {model_dir / 'intent_model.pkl'}")
    print(f"Saved vectorizer to: {model_dir / 'vectorizer.pkl'}")
    print(f"Saved metrics to: {model_dir / 'intent_metrics.json'}")


if __name__ == "__main__":
    main()
