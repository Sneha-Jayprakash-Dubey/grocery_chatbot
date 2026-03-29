import pickle
from pathlib import Path
import json

import numpy as np
import pandas as pd
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics.pairwise import cosine_similarity
from sklearn.preprocessing import normalize


def build_user_interactions(products: pd.DataFrame) -> pd.DataFrame:
    rng = np.random.default_rng(42)
    users = []
    categories = products["category"].unique().tolist()
    for user_id in range(1, 61):
        fav = rng.choice(categories)
        liked = products.loc[products["category"] == fav, "product_id"].sample(
            n=min(5, len(products.loc[products["category"] == fav])),
            random_state=user_id,
            replace=False,
        )
        random_extra = products["product_id"].sample(n=3, random_state=user_id * 2, replace=False)
        for pid in pd.concat([liked, random_extra]).unique():
            users.append({"user_id": user_id, "product_id": int(pid)})
    return pd.DataFrame(users)


def precision_at_k(products, similarity, interactions, k=5):
    hits = 0
    users_evaluated = 0
    prod_index = {pid: idx for idx, pid in enumerate(products["product_id"].tolist())}
    index_to_pid = {v: k_ for k_, v in prod_index.items()}

    for user_id, grp in interactions.groupby("user_id"):
        items = grp["product_id"].tolist()
        if len(items) < 2:
            continue
        holdout = items[-1]
        seed_items = items[:-1]

        valid_seed_idx = [prod_index[i] for i in seed_items if i in prod_index]
        if not valid_seed_idx or holdout not in prod_index:
            continue

        profile = similarity[valid_seed_idx].mean(axis=0)
        seen = set(seed_items)
        ranked_idx = np.argsort(profile)[::-1]
        recs = []
        for idx in ranked_idx:
            pid = index_to_pid[idx]
            if pid not in seen:
                recs.append(pid)
            if len(recs) >= k:
                break

        if holdout in recs:
            hits += 1
        users_evaluated += 1

    if users_evaluated == 0:
        return 0.0
    return hits / users_evaluated


def main():
    root = Path(__file__).resolve().parents[1]
    products_path = root / "data" / "products.csv"
    model_dir = root / "ml" / "models"
    model_dir.mkdir(parents=True, exist_ok=True)

    products = pd.read_csv(products_path)
    products["text"] = (
        products["name"].astype(str).str.lower().str.strip()
        + " "
        + products["category"].astype(str).str.lower().str.strip()
    )

    vectorizer = TfidfVectorizer(ngram_range=(1, 2), dtype=np.float64)
    matrix = vectorizer.fit_transform(products["text"])
    matrix = normalize(matrix, norm="l2", axis=1)
    similarity = cosine_similarity(matrix, matrix)

    interactions = build_user_interactions(products)
    precision_k = precision_at_k(products, similarity, interactions, k=5)
    sim_upper = similarity[np.triu_indices_from(similarity, k=1)]
    sim_mean = float(np.mean(sim_upper)) if sim_upper.size else 0.0
    sim_max = float(np.max(sim_upper)) if sim_upper.size else 0.0
    sim_min = float(np.min(sim_upper)) if sim_upper.size else 0.0

    print("=== Recommender Evaluation ===")
    print(f"Users in eval: {interactions['user_id'].nunique()}")
    print(f"Precision@5: {precision_k:.4f}")
    print(f"Similarity score stats (off-diagonal) -> min: {sim_min:.4f}, mean: {sim_mean:.4f}, max: {sim_max:.4f}")

    artifact = {
        "products": products.to_dict(orient="records"),
        "vectorizer": vectorizer,
        "matrix": matrix,
        "similarity": similarity,
    }
    with (model_dir / "recommender.pkl").open("wb") as fp:
        pickle.dump(artifact, fp)

    interactions.to_csv(root / "data" / "user_interactions.csv", index=False)
    metrics = {
        "users_evaluated": int(interactions["user_id"].nunique()),
        "precision_at_5": float(precision_k),
        "similarity_min_offdiag": sim_min,
        "similarity_mean_offdiag": sim_mean,
        "similarity_max_offdiag": sim_max,
        "num_products": int(len(products)),
    }
    with (model_dir / "recommender_metrics.json").open("w", encoding="utf-8") as fp:
        json.dump(metrics, fp, indent=2, ensure_ascii=True)
    print(f"Saved model to: {model_dir / 'recommender.pkl'}")
    print(f"Saved metrics to: {model_dir / 'recommender_metrics.json'}")


if __name__ == "__main__":
    main()
