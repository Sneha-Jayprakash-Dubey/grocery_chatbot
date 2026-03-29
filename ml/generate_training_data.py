import itertools
import json
import random
from pathlib import Path


SEED = 42
TARGET_PER_INTENT = 120


def synthesize_intent_samples():
    random.seed(SEED)

    patterns = {
        "add_to_cart": {
            "verbs": ["add", "put", "include", "insert", "buy", "place"],
            "qty": ["", "1 ", "2 ", "3 ", "half kg of ", "one packet of "],
            "items": [
                "apple",
                "banana",
                "milk",
                "bread",
                "rice",
                "eggs",
                "tomato",
                "potato",
                "cheese",
                "butter",
            ],
            "tails": ["", " please", " in cart", " to my cart", " now", " quickly"],
        },
        "remove_from_cart": {
            "verbs": ["remove", "delete", "take out", "drop", "exclude", "cancel"],
            "qty": ["", "1 ", "2 ", "all "],
            "items": [
                "apple",
                "banana",
                "milk",
                "bread",
                "rice",
                "eggs",
                "tomato",
                "chips",
            ],
            "tails": ["", " from cart", " from my cart", " please", " now"],
        },
        "show_products": {
            "verbs": ["show", "list", "display", "what are", "give", "view"],
            "topics": [
                "products",
                "items",
                "grocery items",
                "vegetables",
                "fruits",
                "dairy products",
                "snacks",
            ],
            "tails": ["", " available", " today", " right now", " in store"],
        },
        "budget_query": {
            "verbs": ["budget", "under", "within", "cheap", "affordable", "cost"],
            "amounts": ["100", "200", "300", "500", "700", "1000"],
            "tails": [
                " rupees",
                " rs",
                "",
                " for groceries",
                " shopping plan",
                " cart total",
            ],
            "prefixes": [
                "show items",
                "suggest cart",
                "i need",
                "can you make",
                "plan",
                "find products",
            ],
        },
        "greeting": {
            "phrases": [
                "hello",
                "hi",
                "hey",
                "good morning",
                "good evening",
                "namaste",
                "yo",
                "hey there",
                "hello assistant",
                "hi bot",
                "good afternoon",
                "sup",
            ]
        },
    }

    samples = {k: [] for k in patterns}

    for v, q, i, t in itertools.product(
        patterns["add_to_cart"]["verbs"],
        patterns["add_to_cart"]["qty"],
        patterns["add_to_cart"]["items"],
        patterns["add_to_cart"]["tails"],
    ):
        text = f"{v} {q}{i}{t}".strip()
        samples["add_to_cart"].append(text)

    for v, q, i, t in itertools.product(
        patterns["remove_from_cart"]["verbs"],
        patterns["remove_from_cart"]["qty"],
        patterns["remove_from_cart"]["items"],
        patterns["remove_from_cart"]["tails"],
    ):
        text = f"{v} {q}{i}{t}".strip()
        samples["remove_from_cart"].append(text)

    for v, topic, t in itertools.product(
        patterns["show_products"]["verbs"],
        patterns["show_products"]["topics"],
        patterns["show_products"]["tails"],
    ):
        samples["show_products"].append(f"{v} {topic}{t}".strip())

    for p, v, a, t in itertools.product(
        patterns["budget_query"]["prefixes"],
        patterns["budget_query"]["verbs"],
        patterns["budget_query"]["amounts"],
        patterns["budget_query"]["tails"],
    ):
        samples["budget_query"].append(f"{p} {v} {a}{t}".strip())

    samples["greeting"] = patterns["greeting"]["phrases"] * 20

    records = []
    for intent, text_list in samples.items():
        random.shuffle(text_list)
        chosen = text_list[:TARGET_PER_INTENT]
        for text in chosen:
            records.append({"text": text, "intent": intent})

    random.shuffle(records)
    return records


def main():
    root = Path(__file__).resolve().parents[1]
    out_path = root / "data" / "training_data.json"
    out_path.parent.mkdir(parents=True, exist_ok=True)

    records = synthesize_intent_samples()
    with out_path.open("w", encoding="utf-8") as fp:
        json.dump(records, fp, indent=2, ensure_ascii=True)

    print(f"Wrote {len(records)} training samples to {out_path}")


if __name__ == "__main__":
    main()
