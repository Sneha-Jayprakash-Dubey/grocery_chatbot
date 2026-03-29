import re
import string

STOPWORDS = {
    "a", "an", "the", "is", "are", "was", "were", "am", "to", "of", "for",
    "in", "on", "at", "my", "me", "i", "you", "your", "please", "can", "could",
    "would", "should", "and", "or", "with", "this", "that", "it", "now", "today",
}


def custom_preprocessor(text: str) -> str:
    text = text.lower()
    text = text.translate(str.maketrans("", "", string.punctuation))
    tokens = re.findall(r"[a-z0-9]+", text)
    filtered = [tok for tok in tokens if tok not in STOPWORDS]
    return " ".join(filtered)
