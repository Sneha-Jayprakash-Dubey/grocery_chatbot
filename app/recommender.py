import pickle
from pathlib import Path
from typing import List

import numpy as np


class Recommender:
    def __init__(self, model_path: str):
        with open(model_path, "rb") as fp:
            artifact = pickle.load(fp)
        self.products = artifact["products"]
        self.similarity = artifact["similarity"]
        self.index_by_name = {
            p["name"].lower().strip(): idx for idx, p in enumerate(self.products)
        }

    def recommend(self, item_names: List[str], k: int = 5) -> List[dict]:
        idxs = [
            self.index_by_name[name.lower().strip()]
            for name in item_names
            if name.lower().strip() in self.index_by_name
        ]
        if not idxs:
            return self.products[:k]

        profile = np.mean(self.similarity[idxs], axis=0)
        ranked = np.argsort(profile)[::-1]
        existing = {self.products[i]["name"].lower().strip() for i in idxs}
        recs = []
        for idx in ranked:
            prod = self.products[int(idx)]
            if prod["name"].lower().strip() in existing:
                continue
            rec = dict(prod)
            rec["similarity_score"] = float(profile[int(idx)])
            recs.append(rec)
            if len(recs) >= k:
                break
        return recs


def default_model_path() -> str:
    root = Path(__file__).resolve().parents[1]
    return str(root / "ml" / "models" / "recommender.pkl")
