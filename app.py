from flask import Flask, Response, jsonify, redirect, render_template, request, send_from_directory, session, url_for
from model import chatbot_response
from difflib import get_close_matches
from markupsafe import escape
from werkzeug.security import check_password_hash, generate_password_hash
import datetime
import json
import io
import csv
import os
import pickle
import random
import re
import secrets
import sqlite3
import threading
import time
from pathlib import Path
try:
    import libsql  # Turso/libSQL client (optional)
except Exception:
    libsql = None
try:
    from pywebpush import WebPushException, webpush
except Exception:  # pragma: no cover - optional dependency at runtime
    WebPushException = Exception
    webpush = None

def get_app_secret_key():
    env_key = os.getenv("FLASK_SECRET_KEY")
    if env_key:
        return env_key

    key_file = Path(__file__).resolve().parent / ".flask_secret_key"
    if key_file.exists():
        try:
            stored = key_file.read_text(encoding="utf-8").strip()
            if stored:
                return stored
        except Exception:
            pass

    generated = secrets.token_hex(32)
    try:
        key_file.write_text(generated, encoding="utf-8")
    except Exception:
        # Fallback: still work even if file write is unavailable.
        return generated
    return generated


app = Flask(__name__)
app.secret_key = get_app_secret_key()
app.config.update(
    SESSION_COOKIE_HTTPONLY=True,
    SESSION_COOKIE_SAMESITE="Lax",
)

STORE_LOCATION = "123 Green Valley Road, Fresh Market Square"
DELIVERY_FEE = 30
MIN_FREE_DELIVERY = 500
MIN_ORDER_FOR_DELIVERY = 200

DB_PATH = os.getenv("DATABASE_PATH", "orders.db")
TURSO_DATABASE_URL = os.getenv("TURSO_DATABASE_URL") or os.getenv("LIBSQL_URL")
TURSO_AUTH_TOKEN = os.getenv("TURSO_AUTH_TOKEN") or os.getenv("LIBSQL_AUTH_TOKEN")
ADMIN_PASSWORD = os.getenv("ADMIN_PASSWORD")
ADMIN_PASSWORD_HASH = os.getenv("ADMIN_PASSWORD_HASH")
LOCAL_DEV_ADMIN_PASSWORD = os.getenv("LOCAL_DEV_ADMIN_PASSWORD", "adminbot")
VAPID_PUBLIC_KEY = os.getenv("VAPID_PUBLIC_KEY", "")
VAPID_PRIVATE_KEY = os.getenv("VAPID_PRIVATE_KEY", "")
VAPID_CLAIMS_SUB = os.getenv("VAPID_CLAIMS_SUB", "mailto:admin@example.com")
CRON_SECRET = os.getenv("CRON_SECRET", "")
BUDGET_MODEL_PATH = os.getenv("BUDGET_MODEL_PATH", os.path.join("ml", "models", "regression.pkl"))
BUDGET_MODEL_CACHE = None
MIN_PICKUP_LEAD_MINUTES = 15
MAX_PICKUP_LEAD_HOURS = 72
PICKUP_SOON_WINDOW_MINUTES = 30
REMINDER_POLL_SECONDS = 60

_reminder_thread = None
_reminder_stop_event = threading.Event()
_reminder_lock = threading.Lock()

VALID_UNITS = {"kg", "g", "litre", "ml", "piece", "packet", "dozen"}
UNIT_ALIASES = {
    "kg": "kg",
    "kilo": "kg",
    "g": "g",
    "gm": "g",
    "gram": "g",
    "grams": "g",
    "litre": "litre",
    "liter": "litre",
    "ltr": "litre",
    "l": "litre",
    "ml": "ml",
    "piece": "piece",
    "pieces": "piece",
    "pc": "piece",
    "pcs": "piece",
    "packet": "packet",
    "pack": "packet",
    "packs": "packet",
    "pkt": "packet",
    "dozen": "dozen",
}

HINGLISH_MAP = {
    "aloo": "potato",
    "pyaz": "onion",
    "pyaaz": "onion",
    "tamatar": "tomato",
    "doodh": "milk",
    "kela": "banana",
    "seb": "apple",
    "anda": "egg",
    "chai patti": "tea",
    "atta": "flour",
    "aata": "flour",
    "chawal": "rice",
    "mirchi": "chilli",
    "dhaniya": "coriander",
    "adrak": "ginger",
    "matar": "peas",
    "bhindi": "okra",
    "baingan": "brinjal",
    "baigan": "brinjal",
    "karela": "bitter gourd",
    "dahi": "curd",
    "chaas": "buttermilk",
}

DEFAULT_CATALOG = {
    "fruits": [
        {"name": "apple", "price_per_unit": 120, "base_unit": "kg", "aliases": "seb"},
        {"name": "banana", "price_per_unit": 60, "base_unit": "dozen", "aliases": "kela"},
        {"name": "grapes", "price_per_unit": 90, "base_unit": "kg", "aliases": "angoor"},
        {"name": "kiwi", "price_per_unit": 240, "base_unit": "kg", "aliases": ""},
        {"name": "dragon fruit", "price_per_unit": 220, "base_unit": "kg", "aliases": "pitaya"},
        {"name": "chikoo", "price_per_unit": 70, "base_unit": "kg", "aliases": "sapota"},
        {"name": "papaya", "price_per_unit": 50, "base_unit": "kg", "aliases": "papita"},
        {"name": "watermelon", "price_per_unit": 35, "base_unit": "kg", "aliases": "tarbooj"},
        {"name": "pomegranate", "price_per_unit": 180, "base_unit": "kg", "aliases": "anar"},
        {"name": "guava", "price_per_unit": 80, "base_unit": "kg", "aliases": "amrood"},
        {"name": "pineapple", "price_per_unit": 90, "base_unit": "piece", "aliases": "ananas"},
        {"name": "orange", "price_per_unit": 80, "base_unit": "kg", "aliases": ""},
        {"name": "mango", "price_per_unit": 150, "base_unit": "kg", "aliases": "aam"},
    ],
    "vegetables": [
        {"name": "potato", "price_per_unit": 40, "base_unit": "kg", "aliases": "aloo"},
        {"name": "tomato", "price_per_unit": 50, "base_unit": "kg", "aliases": "tamatar"},
        {"name": "carrot", "price_per_unit": 60, "base_unit": "kg", "aliases": "gajar"},
        {"name": "onion", "price_per_unit": 30, "base_unit": "kg", "aliases": "pyaz,pyaaz"},
        {"name": "peas", "price_per_unit": 90, "base_unit": "kg", "aliases": "matar"},
        {"name": "okra", "price_per_unit": 70, "base_unit": "kg", "aliases": "bhindi"},
        {"name": "brinjal", "price_per_unit": 60, "base_unit": "kg", "aliases": "baingan,baigan"},
        {"name": "bitter gourd", "price_per_unit": 70, "base_unit": "kg", "aliases": "karela"},
        {"name": "cauliflower", "price_per_unit": 55, "base_unit": "kg", "aliases": "phool gobi"},
        {"name": "cabbage", "price_per_unit": 45, "base_unit": "kg", "aliases": "patta gobi"},
        {"name": "capsicum", "price_per_unit": 90, "base_unit": "kg", "aliases": "shimla mirch"},
        {"name": "cucumber", "price_per_unit": 40, "base_unit": "kg", "aliases": "kheera"},
        {"name": "spinach", "price_per_unit": 30, "base_unit": "kg", "aliases": "palak"},
        {"name": "bottle gourd", "price_per_unit": 35, "base_unit": "kg", "aliases": "lauki"},
        {"name": "green chilli", "price_per_unit": 120, "base_unit": "kg", "aliases": "hari mirch"},
        {"name": "ginger", "price_per_unit": 140, "base_unit": "kg", "aliases": "adrak"},
        {"name": "garlic", "price_per_unit": 160, "base_unit": "kg", "aliases": "lahsun"},
        {"name": "coriander", "price_per_unit": 20, "base_unit": "packet", "aliases": "dhaniya"},
    ],
    "dairy": [
        {"name": "milk", "price_per_unit": 60, "base_unit": "litre", "aliases": "doodh"},
        {"name": "amul milk", "price_per_unit": 62, "base_unit": "litre", "aliases": ""},
        {"name": "mother dairy milk", "price_per_unit": 61, "base_unit": "litre", "aliases": ""},
        {"name": "toned milk", "price_per_unit": 58, "base_unit": "litre", "aliases": ""},
        {"name": "curd", "price_per_unit": 40, "base_unit": "piece", "aliases": "dahi"},
        {"name": "dahi", "price_per_unit": 40, "base_unit": "piece", "aliases": "curd"},
        {"name": "chaas", "price_per_unit": 25, "base_unit": "piece", "aliases": "buttermilk"},
        {"name": "paneer", "price_per_unit": 95, "base_unit": "piece", "aliases": ""},
        {"name": "cheese", "price_per_unit": 120, "base_unit": "piece", "aliases": ""},
        {"name": "butter", "price_per_unit": 100, "base_unit": "piece", "aliases": ""},
        {"name": "ghee", "price_per_unit": 340, "base_unit": "litre", "aliases": ""},
        {"name": "lassi", "price_per_unit": 30, "base_unit": "piece", "aliases": ""},
    ],
    "biscuits": [
        {"name": "biscuits", "price_per_unit": 30, "base_unit": "packet", "aliases": "biscuit"},
        {"name": "parle g", "price_per_unit": 5, "base_unit": "packet", "aliases": "parle"},
        {"name": "monaco", "price_per_unit": 10, "base_unit": "packet", "aliases": ""},
        {"name": "good day", "price_per_unit": 30, "base_unit": "packet", "aliases": ""},
        {"name": "marie gold", "price_per_unit": 35, "base_unit": "packet", "aliases": "marie"},
        {"name": "bourbon", "price_per_unit": 35, "base_unit": "packet", "aliases": ""},
        {"name": "krackjack", "price_per_unit": 30, "base_unit": "packet", "aliases": ""},
        {"name": "oreo", "price_per_unit": 40, "base_unit": "packet", "aliases": ""},
        {"name": "hide and seek", "price_per_unit": 35, "base_unit": "packet", "aliases": "hide n seek"},
    ],
    "chocolates": [
        {"name": "chocolate", "price_per_unit": 50, "base_unit": "piece", "aliases": ""},
        {"name": "cadbury dairy milk", "price_per_unit": 40, "base_unit": "piece", "aliases": "dairy milk"},
        {"name": "5 star", "price_per_unit": 10, "base_unit": "piece", "aliases": "five star"},
        {"name": "amul dark chocolate", "price_per_unit": 120, "base_unit": "piece", "aliases": "amul dark"},
        {"name": "kitkat", "price_per_unit": 20, "base_unit": "piece", "aliases": ""},
        {"name": "munch", "price_per_unit": 10, "base_unit": "piece", "aliases": ""},
        {"name": "perk", "price_per_unit": 10, "base_unit": "piece", "aliases": ""},
        {"name": "fuse", "price_per_unit": 30, "base_unit": "piece", "aliases": ""},
    ],
    "toffees": [
        {"name": "toffee", "price_per_unit": 1, "base_unit": "piece", "aliases": "candy"},
        {"name": "eclairs", "price_per_unit": 1, "base_unit": "piece", "aliases": ""},
        {"name": "melody", "price_per_unit": 1, "base_unit": "piece", "aliases": ""},
        {"name": "kismi", "price_per_unit": 1, "base_unit": "piece", "aliases": ""},
        {"name": "pulse candy", "price_per_unit": 2, "base_unit": "piece", "aliases": "pulse"},
        {"name": "mango bite", "price_per_unit": 1, "base_unit": "piece", "aliases": ""},
        {"name": "alpenliebe", "price_per_unit": 2, "base_unit": "piece", "aliases": ""},
        {"name": "parle poppins", "price_per_unit": 5, "base_unit": "piece", "aliases": "poppins"},
        {"name": "coffee bite", "price_per_unit": 2, "base_unit": "piece", "aliases": ""},
    ],
    "snacks": [
        {"name": "chips", "price_per_unit": 20, "base_unit": "packet", "aliases": ""},
        {"name": "lays chips", "price_per_unit": 20, "base_unit": "packet", "aliases": "lays"},
        {"name": "kurkure", "price_per_unit": 20, "base_unit": "packet", "aliases": ""},
        {"name": "bingo chips", "price_per_unit": 20, "base_unit": "packet", "aliases": "bingo"},
        {"name": "wafers", "price_per_unit": 20, "base_unit": "packet", "aliases": ""},
        {"name": "nachos", "price_per_unit": 50, "base_unit": "packet", "aliases": ""},
        {"name": "namkeen", "price_per_unit": 30, "base_unit": "packet", "aliases": "mixture"},
        {"name": "sev", "price_per_unit": 30, "base_unit": "packet", "aliases": ""},
        {"name": "bhujia", "price_per_unit": 35, "base_unit": "packet", "aliases": ""},
        {"name": "makhana", "price_per_unit": 110, "base_unit": "packet", "aliases": "fox nuts"},
        {"name": "popcorn", "price_per_unit": 30, "base_unit": "packet", "aliases": ""},
    ],
    "staples": [
        {"name": "wheat flour", "price_per_unit": 45, "base_unit": "kg", "aliases": "atta,aata"},
        {"name": "rice", "price_per_unit": 70, "base_unit": "kg", "aliases": "chawal"},
        {"name": "basmati rice", "price_per_unit": 120, "base_unit": "kg", "aliases": ""},
        {"name": "poha", "price_per_unit": 60, "base_unit": "kg", "aliases": ""},
        {"name": "suji", "price_per_unit": 50, "base_unit": "kg", "aliases": "rava"},
        {"name": "besan", "price_per_unit": 80, "base_unit": "kg", "aliases": "gram flour"},
        {"name": "sugar", "price_per_unit": 48, "base_unit": "kg", "aliases": "chini"},
        {"name": "salt", "price_per_unit": 24, "base_unit": "kg", "aliases": "namak"},
        {"name": "mustard oil", "price_per_unit": 190, "base_unit": "litre", "aliases": "sarson tel"},
        {"name": "sunflower oil", "price_per_unit": 170, "base_unit": "litre", "aliases": ""},
        {"name": "groundnut oil", "price_per_unit": 185, "base_unit": "litre", "aliases": "peanut oil"},
        {"name": "tea", "price_per_unit": 120, "base_unit": "packet", "aliases": "chai patti"},
    ],
    "pulses": [
        {"name": "toor dal", "price_per_unit": 140, "base_unit": "kg", "aliases": "arhar dal"},
        {"name": "moong dal", "price_per_unit": 130, "base_unit": "kg", "aliases": ""},
        {"name": "masoor dal", "price_per_unit": 110, "base_unit": "kg", "aliases": ""},
        {"name": "chana dal", "price_per_unit": 95, "base_unit": "kg", "aliases": ""},
        {"name": "urad dal", "price_per_unit": 125, "base_unit": "kg", "aliases": ""},
        {"name": "rajma", "price_per_unit": 150, "base_unit": "kg", "aliases": ""},
        {"name": "chole", "price_per_unit": 105, "base_unit": "kg", "aliases": "kabuli chana"},
        {"name": "black chana", "price_per_unit": 90, "base_unit": "kg", "aliases": "kala chana"},
    ],
    "spices": [
        {"name": "turmeric powder", "price_per_unit": 35, "base_unit": "packet", "aliases": "haldi"},
        {"name": "red chilli powder", "price_per_unit": 45, "base_unit": "packet", "aliases": "lal mirch"},
        {"name": "coriander powder", "price_per_unit": 40, "base_unit": "packet", "aliases": "dhaniya powder"},
        {"name": "cumin", "price_per_unit": 55, "base_unit": "packet", "aliases": "jeera"},
        {"name": "garam masala", "price_per_unit": 60, "base_unit": "packet", "aliases": ""},
        {"name": "mustard seeds", "price_per_unit": 35, "base_unit": "packet", "aliases": "rai"},
        {"name": "ajwain", "price_per_unit": 30, "base_unit": "packet", "aliases": "carom seeds"},
        {"name": "hing", "price_per_unit": 30, "base_unit": "packet", "aliases": "asafoetida"},
        {"name": "black pepper", "price_per_unit": 80, "base_unit": "packet", "aliases": "kali mirch"},
        {"name": "kasuri methi", "price_per_unit": 30, "base_unit": "packet", "aliases": ""},
    ],
    "beverages": [
        {"name": "tea", "price_per_unit": 120, "base_unit": "packet", "aliases": "chai patti"},
        {"name": "coffee", "price_per_unit": 180, "base_unit": "packet", "aliases": ""},
        {"name": "fruit juice", "price_per_unit": 110, "base_unit": "piece", "aliases": "juice"},
        {"name": "soft drink", "price_per_unit": 45, "base_unit": "piece", "aliases": "cold drink"},
        {"name": "mineral water", "price_per_unit": 20, "base_unit": "piece", "aliases": "water bottle"},
        {"name": "energy drink", "price_per_unit": 125, "base_unit": "piece", "aliases": ""},
    ],
    "bakery": [
        {"name": "bread", "price_per_unit": 45, "base_unit": "piece", "aliases": ""},
        {"name": "brown bread", "price_per_unit": 55, "base_unit": "piece", "aliases": ""},
        {"name": "rusk", "price_per_unit": 40, "base_unit": "packet", "aliases": "toast"},
        {"name": "pav", "price_per_unit": 35, "base_unit": "packet", "aliases": ""},
        {"name": "bun", "price_per_unit": 30, "base_unit": "packet", "aliases": ""},
        {"name": "cake", "price_per_unit": 150, "base_unit": "piece", "aliases": ""},
    ],
    "eggs and meat": [
        {"name": "eggs", "price_per_unit": 72, "base_unit": "dozen", "aliases": "anda"},
        {"name": "chicken", "price_per_unit": 260, "base_unit": "kg", "aliases": ""},
        {"name": "fish", "price_per_unit": 280, "base_unit": "kg", "aliases": ""},
    ],
}

RECIPE_KITS = {
    "pasta": ["tomato", "onion", "cheese", "butter"],
    "paneer curry": ["paneer", "tomato", "onion", "butter"],
    "matar paneer": ["paneer", "peas", "tomato", "onion"],
    "sandwich": ["bread", "butter", "tomato", "onion"],
    "omelette": ["eggs", "onion", "tomato", "butter"],
    "fruit bowl": ["apple", "banana", "orange", "mango"],
}

DIETARY_BLOCKLIST = {
    "vegan": {"milk", "cheese", "butter", "paneer", "curd", "eggs"},
    "jain": {"onion", "potato", "carrot", "ginger","Onion," "garlic", "eggs", "chicken", "fish"},
    "diabetic": {"sugar", "chocolate", "juice", "biscuits"},
}


def get_language(state):
    return state.get("language", "english")


def reply_text(state, english, hindi=None, hinglish=None):
    lang = get_language(state)
    if lang == "hindi" and hindi:
        return hindi
    if lang == "hinglish" and hinglish:
        return hinglish
    return english


def detect_language_command(msg):
    direct = msg.strip().lower()
    if direct in {"english", "hindi", "hinglish"}:
        return direct

    match = re.search(r"\b(language|lang|reply in|speak)\s+(english|hindi|hinglish)\b", direct)
    if match:
        return match.group(2)
    return None


def get_db_connection():
    class _NormalizedCursor:
        def __init__(self, cursor):
            self._cursor = cursor

        def _normalize_row(self, row):
            if row is None:
                return None
            if isinstance(row, dict):
                return row
            if hasattr(row, "keys") and not isinstance(row, (tuple, list)):
                try:
                    return {k: row[k] for k in row.keys()}
                except Exception:
                    pass
            if isinstance(row, (tuple, list)):
                description = getattr(self._cursor, "description", None) or []
                if description:
                    mapped = {}
                    for i, value in enumerate(row):
                        if i >= len(description):
                            break
                        col = description[i]
                        if isinstance(col, (tuple, list)):
                            col_name = str(col[0])
                        else:
                            col_name = str(col)
                        mapped[col_name] = value
                    if mapped:
                        return mapped
            return row

        def fetchone(self):
            return self._normalize_row(self._cursor.fetchone())

        def fetchall(self):
            rows = self._cursor.fetchall()
            return [self._normalize_row(r) for r in rows]

        def __iter__(self):
            for row in self._cursor:
                yield self._normalize_row(row)

        def __getattr__(self, name):
            return getattr(self._cursor, name)

    class _NormalizedConnection:
        def __init__(self, conn):
            self._conn = conn

        def execute(self, *args, **kwargs):
            return _NormalizedCursor(self._conn.execute(*args, **kwargs))

        def executemany(self, *args, **kwargs):
            return _NormalizedCursor(self._conn.executemany(*args, **kwargs))

        def cursor(self, *args, **kwargs):
            return _NormalizedCursor(self._conn.cursor(*args, **kwargs))

        def __enter__(self):
            self._conn.__enter__()
            return self

        def __exit__(self, exc_type, exc, tb):
            return self._conn.__exit__(exc_type, exc, tb)

        def __getattr__(self, name):
            return getattr(self._conn, name)

    def _dict_row_factory(cursor, row):
        try:
            cols = [c[0] for c in cursor.description]
            return {cols[i]: row[i] for i in range(len(cols))}
        except Exception:
            return row

    if TURSO_DATABASE_URL and TURSO_AUTH_TOKEN:
        if libsql is None:
            raise RuntimeError(
                "Turso is configured but libsql package is missing. Add `libsql` to requirements."
            )
        # Embedded replica mode: local file for cache, remote primary for persistence.
        conn = libsql.connect(
            DB_PATH,
            sync_url=TURSO_DATABASE_URL,
            auth_token=TURSO_AUTH_TOKEN,
            sync_interval=30,
        )
        return _NormalizedConnection(conn)

    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = _dict_row_factory
    return _NormalizedConnection(conn)


def ensure_column(conn, table_name, column_name, ddl):
    existing = set()
    rows = conn.execute(f"PRAGMA table_info({table_name})").fetchall()
    for row in rows:
        name = None
        try:
            name = row["name"]
        except Exception:
            # Tuple fallback (PRAGMA table_info order: cid, name, type, ...)
            if isinstance(row, (tuple, list)):
                if len(row) > 1:
                    name = row[1]
                elif len(row) > 0:
                    name = row[0]
        if name:
            existing.add(str(name))
    if column_name not in existing:
        conn.execute(f"ALTER TABLE {table_name} ADD COLUMN {column_name} {ddl}")


def row_value(row, key, fallback_index=0, default=None):
    try:
        return row[key]
    except Exception:
        if isinstance(row, dict):
            return row.get(key, default)
        if isinstance(row, (tuple, list)) and len(row) > fallback_index:
            return row[fallback_index]
    return default


def is_unique_constraint_error(exc):
    text = str(exc).lower()
    return "unique" in text or ("constraint" in text and "failed" in text)


def seed_catalog_if_empty(conn):
    row = conn.execute("SELECT COUNT(*) AS c FROM categories").fetchone()
    count = row_value(row, "c", fallback_index=0, default=0) or 0
    if count > 0:
        return

    for category_name, product_list in DEFAULT_CATALOG.items():
        cur = conn.execute(
            "INSERT INTO categories(name) VALUES (?)",
            (category_name.lower().strip(),),
        )
        category_id = cur.lastrowid
        for p in product_list:
            conn.execute(
                """
                INSERT INTO products(category_id, name, price_per_unit, base_unit, aliases, is_active)
                VALUES (?, ?, ?, ?, ?, 1)
                """,
                (
                    category_id,
                    p["name"].lower().strip(),
                    float(p["price_per_unit"]),
                    p["base_unit"],
                    p.get("aliases", "").lower().strip(),
                ),
            )


def sync_default_catalog(conn):
    category_rows = conn.execute("SELECT id, name FROM categories").fetchall()
    category_ids = {}
    for row in category_rows:
        name = str(row_value(row, "name", fallback_index=1, default="")).lower().strip()
        cat_id = row_value(row, "id", fallback_index=0, default=None)
        if name and cat_id is not None:
            category_ids[name] = int(cat_id)

    for category_name, product_list in DEFAULT_CATALOG.items():
        normalized_category = category_name.lower().strip()
        category_id = category_ids.get(normalized_category)
        if category_id is None:
            cur = conn.execute("INSERT OR IGNORE INTO categories(name) VALUES (?)", (normalized_category,))
            category_id = cur.lastrowid
            if not category_id:
                row = conn.execute("SELECT id FROM categories WHERE name = ?", (normalized_category,)).fetchone()
                category_id = row_value(row, "id", fallback_index=0, default=None)
            if category_id is None:
                continue
            category_ids[normalized_category] = int(category_id)

        existing_rows = conn.execute("SELECT name FROM products WHERE category_id = ?", (category_id,)).fetchall()
        existing_names = {
            str(row_value(r, "name", fallback_index=0, default="")).lower().strip()
            for r in existing_rows
            if str(row_value(r, "name", fallback_index=0, default="")).strip()
        }

        for p in product_list:
            product_name = p["name"].lower().strip()
            if product_name in existing_names:
                continue
            conn.execute(
                """
                INSERT INTO products(category_id, name, price_per_unit, base_unit, aliases, is_active)
                VALUES (?, ?, ?, ?, ?, 1)
                """,
                (
                    category_id,
                    product_name,
                    float(p["price_per_unit"]),
                    p["base_unit"],
                    p.get("aliases", "").lower().strip(),
                ),
            )
            existing_names.add(product_name)


def init_db():
    with get_db_connection() as conn:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS categories (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT UNIQUE NOT NULL
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS users (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                username TEXT UNIQUE NOT NULL,
                password_hash TEXT NOT NULL,
                created_at TEXT NOT NULL
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS family_groups (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL,
                invite_code TEXT UNIQUE NOT NULL,
                created_by INTEGER NOT NULL,
                created_at TEXT NOT NULL,
                FOREIGN KEY (created_by) REFERENCES users(id) ON DELETE CASCADE
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS family_members (
                group_id INTEGER NOT NULL,
                user_id INTEGER NOT NULL,
                role TEXT NOT NULL DEFAULT 'member',
                joined_at TEXT NOT NULL,
                PRIMARY KEY (group_id, user_id),
                FOREIGN KEY (group_id) REFERENCES family_groups(id) ON DELETE CASCADE,
                FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS family_list_items (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                group_id INTEGER NOT NULL,
                item TEXT NOT NULL,
                qty REAL NOT NULL DEFAULT 1,
                unit TEXT,
                added_by INTEGER,
                last_updated_by INTEGER,
                is_checked INTEGER NOT NULL DEFAULT 0,
                updated_at TEXT NOT NULL,
                FOREIGN KEY (group_id) REFERENCES family_groups(id) ON DELETE CASCADE,
                FOREIGN KEY (added_by) REFERENCES users(id) ON DELETE SET NULL,
                FOREIGN KEY (last_updated_by) REFERENCES users(id) ON DELETE SET NULL
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS products (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                category_id INTEGER NOT NULL,
                name TEXT NOT NULL,
                price_per_unit REAL NOT NULL,
                base_unit TEXT NOT NULL,
                aliases TEXT DEFAULT '',
                is_active INTEGER NOT NULL DEFAULT 1,
                FOREIGN KEY (category_id) REFERENCES categories(id) ON DELETE CASCADE
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS orders (
                id TEXT PRIMARY KEY,
                created_at TEXT NOT NULL,
                method TEXT NOT NULL,
                address TEXT,
                subtotal INTEGER NOT NULL,
                delivery_fee INTEGER NOT NULL,
                total INTEGER NOT NULL
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS order_items (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                order_id TEXT NOT NULL,
                item TEXT NOT NULL,
                qty REAL NOT NULL,
                unit TEXT,
                item_id INTEGER,
                line_total INTEGER NOT NULL,
                FOREIGN KEY (order_id) REFERENCES orders(id) ON DELETE CASCADE
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS event_logs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                created_at TEXT NOT NULL,
                event_type TEXT NOT NULL,
                session_user_id TEXT NOT NULL,
                order_id TEXT,
                payload_json TEXT
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS push_subscriptions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER,
                session_user_id TEXT NOT NULL,
                endpoint TEXT UNIQUE NOT NULL,
                p256dh TEXT NOT NULL,
                auth TEXT NOT NULL,
                is_active INTEGER NOT NULL DEFAULT 1,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            )
            """
        )

        ensure_column(conn, "products", "aliases", "TEXT DEFAULT ''")
        ensure_column(conn, "products", "is_active", "INTEGER NOT NULL DEFAULT 1")
        ensure_column(conn, "family_list_items", "last_updated_by", "INTEGER")
        ensure_column(conn, "orders", "pickup_time", "TEXT")
        ensure_column(conn, "orders", "reminder_sent", "INTEGER NOT NULL DEFAULT 0")
        ensure_column(conn, "orders", "pickup_soon_sent", "INTEGER NOT NULL DEFAULT 0")
        ensure_column(conn, "orders", "status", "TEXT DEFAULT 'placed'")
        ensure_column(conn, "orders", "user_id", "INTEGER")
        ensure_column(conn, "orders", "session_user_id", "TEXT")
        ensure_column(conn, "order_items", "unit", "TEXT")
        ensure_column(conn, "order_items", "item_id", "INTEGER")
        ensure_column(conn, "event_logs", "payload_json", "TEXT")
        ensure_column(conn, "event_logs", "user_id", "INTEGER")
        ensure_column(conn, "push_subscriptions", "is_active", "INTEGER NOT NULL DEFAULT 1")
        ensure_column(conn, "push_subscriptions", "created_at", "TEXT")
        ensure_column(conn, "push_subscriptions", "updated_at", "TEXT")

        seed_catalog_if_empty(conn)
        sync_default_catalog(conn)
        conn.commit()


def get_user_state():
    state = session.get("cart_state")
    if not state:
        state = {
            "orders": [],
            "total": 0,
            "last_item": None,
            "waiting_for_method": False,
            "waiting_for_address": False,
            "waiting_for_pickup_time": False,
            "language": "english",
            "notifications": [],
            "dietary_preference": None,
            "budget_mode": None,
            "brand_preferences": {},
            "item_memory": {},
            "pending_add": None,
        }
        session["cart_state"] = state
    if "language" not in state:
        state["language"] = "english"
        session["cart_state"] = state
    if "last_item" not in state:
        state["last_item"] = None
        session["cart_state"] = state
    if "notifications" not in state:
        state["notifications"] = []
        session["cart_state"] = state
    if "dietary_preference" not in state:
        state["dietary_preference"] = None
        session["cart_state"] = state
    if "budget_mode" not in state:
        state["budget_mode"] = None
        session["cart_state"] = state
    if "brand_preferences" not in state:
        state["brand_preferences"] = {}
        session["cart_state"] = state
    if "item_memory" not in state:
        state["item_memory"] = {}
        session["cart_state"] = state
    if "pending_add" not in state:
        state["pending_add"] = None
        session["cart_state"] = state
    return state


def save_user_state(state):
    session["cart_state"] = state
    session.modified = True


def clear_user_state():
    state = get_user_state()
    save_user_state(
        {
            "orders": [],
            "total": 0,
            "last_item": None,
            "waiting_for_method": False,
            "waiting_for_address": False,
            "waiting_for_pickup_time": False,
            "language": state.get("language", "english"),
            "notifications": state.get("notifications", []),
            "dietary_preference": state.get("dietary_preference"),
            "budget_mode": state.get("budget_mode"),
            "brand_preferences": state.get("brand_preferences", {}),
            "item_memory": state.get("item_memory", {}),
            "pending_add": None,
        }
    )


def generate_order_id():
    return f"GRC-{random.randint(100000, 999999)}"


def save_order(order_id, method, address, subtotal, delivery_fee, total, items, pickup_time=None):
    created_at = datetime.datetime.now().isoformat(timespec="seconds")
    user_id = get_logged_in_user_id()
    session_user_id = get_session_user_id()
    with get_db_connection() as conn:
        conn.execute(
            """
            INSERT INTO orders (id, created_at, method, address, subtotal, delivery_fee, total, pickup_time, reminder_sent, status, user_id, session_user_id)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, 0, 'placed', ?, ?)
            """,
            (order_id, created_at, method, address, subtotal, delivery_fee, total, pickup_time, user_id, session_user_id),
        )
        conn.executemany(
            """
            INSERT INTO order_items (order_id, item, qty, unit, item_id, line_total)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            [
                (
                    order_id,
                    it["item"],
                    float(it["qty"]),
                    it["unit"],
                    it.get("item_id"),
                    int(it["line_total"]),
                )
                for it in items
            ],
        )
        conn.commit()


def save_order_with_retry(method, address, subtotal, delivery_fee, total, items, pickup_time=None, retries=5):
    for _ in range(retries):
        order_id = generate_order_id()
        try:
            save_order(order_id, method, address, subtotal, delivery_fee, total, items, pickup_time=pickup_time)
            return order_id
        except Exception as exc:
            if not is_unique_constraint_error(exc):
                raise
            continue
    raise RuntimeError("Could not generate a unique order ID after multiple attempts.")


def format_qty(qty):
    return str(int(qty)) if float(qty).is_integer() else f"{qty:.2f}".rstrip("0").rstrip(".")


def parse_pickup_time(raw_text):
    text = raw_text.strip().lower()
    now = datetime.datetime.now()
    target_date = now.date()

    if text.startswith("tomorrow "):
        target_date = now.date() + datetime.timedelta(days=1)
        text = text[len("tomorrow "):].strip()

    formats = ["%H:%M", "%I:%M %p", "%I %p"]
    parsed_time = None
    for fmt in formats:
        try:
            parsed_time = datetime.datetime.strptime(text, fmt).time()
            break
        except ValueError:
            continue

    if parsed_time is None:
        return None, "Invalid time format. Use HH:MM, HH:MM AM/PM, or 'tomorrow HH:MM'."

    pickup_dt = datetime.datetime.combine(target_date, parsed_time)
    lead_minutes = (pickup_dt - now).total_seconds() / 60.0
    max_lead_minutes = MAX_PICKUP_LEAD_HOURS * 60

    if lead_minutes < MIN_PICKUP_LEAD_MINUTES:
        return None, f"Pickup time must be at least {MIN_PICKUP_LEAD_MINUTES} minutes from now."
    if lead_minutes > max_lead_minutes:
        return None, f"Pickup time is too far. Please choose within next {MAX_PICKUP_LEAD_HOURS} hours."
    return pickup_dt, None


def queue_notification(title, message, order_id=None):
    state = get_user_state()
    notes = state.get("notifications", [])
    notes.append(
        {
            "title": str(title),
            "message": str(message),
            "order_id": order_id,
            "created_at": datetime.datetime.now().isoformat(timespec="seconds"),
        }
    )
    state["notifications"] = notes[-30:]
    save_user_state(state)


def pop_notifications():
    state = get_user_state()
    notes = state.get("notifications", [])
    state["notifications"] = []
    save_user_state(state)
    return notes


def get_session_user_id():
    session_user_id = session.get("session_user_id")
    if not session_user_id:
        session_user_id = secrets.token_hex(8)
        session["session_user_id"] = session_user_id
        session.modified = True
    return session_user_id


def get_logged_in_user_id():
    return session.get("user_id")


def get_logged_in_username():
    return session.get("username")


def log_event(event_type, payload=None, order_id=None):
    created_at = datetime.datetime.now().isoformat(timespec="seconds")
    session_user_id = get_session_user_id()
    payload_json = json.dumps(payload or {}, ensure_ascii=True)
    user_id = get_logged_in_user_id()
    with get_db_connection() as conn:
        conn.execute(
            """
            INSERT INTO event_logs (created_at, event_type, session_user_id, user_id, order_id, payload_json)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (created_at, event_type, session_user_id, user_id, order_id, payload_json),
        )
        conn.commit()


def fetch_event_logs(limit=500):
    with get_db_connection() as conn:
        rows = conn.execute(
            """
            SELECT id, created_at, event_type, session_user_id, user_id, order_id, payload_json
            FROM event_logs
            ORDER BY id DESC
            LIMIT ?
            """,
            (limit,),
        ).fetchall()
    return [dict(r) for r in rows]


def is_web_push_configured():
    return bool(VAPID_PUBLIC_KEY and VAPID_PRIVATE_KEY and webpush is not None)


def upsert_push_subscription(subscription):
    endpoint = (subscription.get("endpoint") or "").strip()
    keys = subscription.get("keys") or {}
    p256dh = (keys.get("p256dh") or "").strip()
    auth = (keys.get("auth") or "").strip()
    if not endpoint or not p256dh or not auth:
        return False, "Invalid subscription payload."

    now = datetime.datetime.now().isoformat(timespec="seconds")
    user_id = get_logged_in_user_id()
    session_user_id = get_session_user_id()

    with get_db_connection() as conn:
        conn.execute(
            """
            INSERT INTO push_subscriptions (user_id, session_user_id, endpoint, p256dh, auth, is_active, created_at, updated_at)
            VALUES (?, ?, ?, ?, ?, 1, ?, ?)
            ON CONFLICT(endpoint) DO UPDATE SET
                user_id=excluded.user_id,
                session_user_id=excluded.session_user_id,
                p256dh=excluded.p256dh,
                auth=excluded.auth,
                is_active=1,
                updated_at=excluded.updated_at
            """,
            (user_id, session_user_id, endpoint, p256dh, auth, now, now),
        )
        conn.commit()
    return True, "Subscription saved."


def deactivate_push_subscription(endpoint):
    if not endpoint:
        return
    with get_db_connection() as conn:
        conn.execute(
            "UPDATE push_subscriptions SET is_active = 0, updated_at = ? WHERE endpoint = ?",
            (datetime.datetime.now().isoformat(timespec="seconds"), endpoint),
        )
        conn.commit()


def fetch_push_subscriptions_for_order(order_id):
    with get_db_connection() as conn:
        order_row = conn.execute(
            "SELECT id, user_id, session_user_id FROM orders WHERE id = ?",
            (order_id,),
        ).fetchone()
        if not order_row:
            return []

        user_id = order_row["user_id"]
        session_user_id = order_row["session_user_id"]
        rows = []
        if user_id:
            rows = conn.execute(
                """
                SELECT endpoint, p256dh, auth
                FROM push_subscriptions
                WHERE is_active = 1 AND user_id = ?
                """,
                (user_id,),
            ).fetchall()

        if not rows and session_user_id:
            rows = conn.execute(
                """
                SELECT endpoint, p256dh, auth
                FROM push_subscriptions
                WHERE is_active = 1 AND session_user_id = ?
                """,
                (session_user_id,),
            ).fetchall()
    return [dict(r) for r in rows]


def send_web_push_notification(order_id, title, message):
    if not is_web_push_configured():
        return {"sent": 0, "failed": 0, "reason": "push_not_configured"}

    subs = fetch_push_subscriptions_for_order(order_id)
    if not subs:
        return {"sent": 0, "failed": 0, "reason": "no_subscriptions"}

    payload = json.dumps({"title": title, "message": message, "order_id": order_id}, ensure_ascii=True)
    sent = 0
    failed = 0

    for s in subs:
        sub_info = {
            "endpoint": s["endpoint"],
            "keys": {"p256dh": s["p256dh"], "auth": s["auth"]},
        }
        try:
            webpush(
                subscription_info=sub_info,
                data=payload,
                vapid_private_key=VAPID_PRIVATE_KEY,
                vapid_claims={"sub": VAPID_CLAIMS_SUB},
            )
            sent += 1
        except WebPushException as exc:
            failed += 1
            status_code = getattr(getattr(exc, "response", None), "status_code", None)
            if status_code in (404, 410):
                deactivate_push_subscription(s["endpoint"])
        except Exception:
            failed += 1

    return {"sent": sent, "failed": failed}


def process_late_pickup_reminders(send_push=True):
    now = datetime.datetime.now()
    reminders = []
    logged_order_ids = []
    with get_db_connection() as conn:
        rows = conn.execute(
            """
            SELECT id, pickup_time
            FROM orders
            WHERE method = 'Pickup'
              AND status = 'placed'
              AND pickup_time IS NOT NULL
              AND COALESCE(reminder_sent, 0) = 0
            ORDER BY created_at ASC
            """
        ).fetchall()

        for row in rows:
            try:
                pickup_dt = datetime.datetime.fromisoformat(row["pickup_time"])
            except (TypeError, ValueError):
                continue
            if now < pickup_dt + datetime.timedelta(minutes=5):
                continue

            order_id = row["id"]
            conn.execute("UPDATE orders SET reminder_sent = 1 WHERE id = ?", (order_id,))
            msg = f"Order {order_id}: You may be late for pickup. Please let us know if you are still coming."
            reminders.append({"order_id": order_id, "title": "Pickup Reminder", "message": msg})
            logged_order_ids.append(order_id)

        conn.commit()

    for order_id in logged_order_ids:
        log_event("pickup_late_reminder", {"message": "Customer may be late by 5+ minutes."}, order_id=order_id)

    if send_push:
        for reminder in reminders:
            send_web_push_notification(reminder["order_id"], reminder["title"], reminder["message"])
    return reminders


def process_pickup_soon_reminders(send_push=True):
    now = datetime.datetime.now()
    reminders = []
    logged_order_ids = []
    with get_db_connection() as conn:
        rows = conn.execute(
            """
            SELECT id, pickup_time
            FROM orders
            WHERE method = 'Pickup'
              AND status = 'placed'
              AND pickup_time IS NOT NULL
              AND COALESCE(pickup_soon_sent, 0) = 0
            ORDER BY created_at ASC
            """
        ).fetchall()

        for row in rows:
            try:
                pickup_dt = datetime.datetime.fromisoformat(row["pickup_time"])
            except (TypeError, ValueError):
                continue

            mins_to_pickup = (pickup_dt - now).total_seconds() / 60.0
            if mins_to_pickup < 0 or mins_to_pickup > PICKUP_SOON_WINDOW_MINUTES:
                continue

            order_id = row["id"]
            conn.execute("UPDATE orders SET pickup_soon_sent = 1 WHERE id = ?", (order_id,))
            rounded = int(max(0, round(mins_to_pickup)))
            msg = f"Order {order_id}: Pickup is in about {rounded} minutes."
            reminders.append({"order_id": order_id, "title": "Pickup Soon", "message": msg})
            logged_order_ids.append(order_id)

        conn.commit()

    for order_id in logged_order_ids:
        log_event("pickup_soon_reminder", {"message": f"Pickup within {PICKUP_SOON_WINDOW_MINUTES} minutes."}, order_id=order_id)

    if send_push:
        for reminder in reminders:
            send_web_push_notification(reminder["order_id"], reminder["title"], reminder["message"])
    return reminders


def _background_reminder_worker():
    while not _reminder_stop_event.is_set():
        try:
            process_pickup_soon_reminders(send_push=True)
            process_late_pickup_reminders(send_push=True)
        except Exception:
            # Keep worker alive even if one cycle fails.
            pass
        _reminder_stop_event.wait(REMINDER_POLL_SECONDS)


def start_background_reminder_worker():
    global _reminder_thread
    with _reminder_lock:
        if _reminder_thread and _reminder_thread.is_alive():
            return
        _reminder_stop_event.clear()
        _reminder_thread = threading.Thread(
            target=_background_reminder_worker,
            name="reminder-worker",
            daemon=True,
        )
        _reminder_thread.start()


def fetch_order_training_rows(limit=1000):
    with get_db_connection() as conn:
        rows = conn.execute(
            """
            SELECT
                o.id,
                o.created_at,
                o.method,
                o.status,
                o.subtotal,
                o.delivery_fee,
                o.total,
                o.pickup_time,
                COUNT(oi.id) AS item_lines,
                COALESCE(SUM(oi.qty), 0) AS total_qty,
                COALESCE(SUM(oi.line_total), 0) AS basket_value
            FROM orders o
            LEFT JOIN order_items oi ON oi.order_id = o.id
            GROUP BY o.id, o.created_at, o.method, o.status, o.subtotal, o.delivery_fee, o.total, o.pickup_time
            ORDER BY o.created_at DESC
            LIMIT ?
            """,
            (limit,),
        ).fetchall()
    return [dict(r) for r in rows]


def fetch_cancellation_dataset(limit=2000):
    with get_db_connection() as conn:
        rows = conn.execute(
            """
            SELECT
                o.id AS order_id,
                o.created_at,
                o.method,
                o.subtotal,
                o.delivery_fee,
                o.total,
                COUNT(oi.id) AS item_lines,
                COALESCE(SUM(oi.qty), 0) AS total_qty,
                CASE WHEN o.status = 'cancelled' THEN 1 ELSE 0 END AS cancelled_label
            FROM orders o
            LEFT JOIN order_items oi ON oi.order_id = o.id
            GROUP BY o.id, o.created_at, o.method, o.subtotal, o.delivery_fee, o.total, o.status
            ORDER BY o.created_at DESC
            LIMIT ?
            """,
            (limit,),
        ).fetchall()
    return [dict(r) for r in rows]


def fetch_recommendation_dataset(limit=5000):
    with get_db_connection() as conn:
        rows = conn.execute(
            """
            SELECT
                o.id AS order_id,
                COALESCE(CAST(o.user_id AS TEXT), 'guest') AS user_key,
                oi.item AS product_name,
                oi.qty,
                oi.line_total
            FROM orders o
            JOIN order_items oi ON oi.order_id = o.id
            WHERE o.status = 'placed'
            ORDER BY o.created_at DESC
            LIMIT ?
            """,
            (limit,),
        ).fetchall()
    return [dict(r) for r in rows]


def fetch_demand_dataset(limit_days=60):
    with get_db_connection() as conn:
        rows = conn.execute(
            """
            SELECT
                date(o.created_at) AS order_date,
                strftime('%H', o.created_at) AS order_hour,
                oi.item AS product_name,
                SUM(oi.qty) AS qty_sum,
                COUNT(DISTINCT o.id) AS order_count
            FROM orders o
            JOIN order_items oi ON oi.order_id = o.id
            WHERE o.status = 'placed'
              AND datetime(o.created_at) >= datetime('now', ?)
            GROUP BY date(o.created_at), strftime('%H', o.created_at), oi.item
            ORDER BY order_date DESC, order_hour DESC
            """,
            (f"-{limit_days} days",),
        ).fetchall()
    return [dict(r) for r in rows]


def fetch_nlp_dataset(limit=5000):
    rows = fetch_event_logs(limit=limit)
    samples = []
    for row in rows:
        if row["event_type"] != "chat_message":
            continue
        try:
            payload = json.loads(row.get("payload_json") or "{}")
        except json.JSONDecodeError:
            payload = {}
        raw = payload.get("raw_message", "")
        normalized = payload.get("normalized_message", "")
        if not raw:
            continue
        samples.append(
            {
                "event_id": row["id"],
                "created_at": row["created_at"],
                "session_user_id": row["session_user_id"],
                "raw_message": raw,
                "normalized_message": normalized,
            }
        )
    return samples


def fetch_late_pickup_dataset(limit=2000):
    with get_db_connection() as conn:
        rows = conn.execute(
            """
            SELECT
                o.id AS order_id,
                o.created_at,
                o.pickup_time,
                o.total,
                o.status,
                o.reminder_sent,
                CASE WHEN o.reminder_sent = 1 THEN 1 ELSE 0 END AS late_label
            FROM orders o
            WHERE o.method = 'Pickup'
            ORDER BY o.created_at DESC
            LIMIT ?
            """,
            (limit,),
        ).fetchall()
    return [dict(r) for r in rows]


def fetch_segmentation_dataset(limit=5000):
    with get_db_connection() as conn:
        rows = conn.execute(
            """
            SELECT
                COALESCE(CAST(o.user_id AS TEXT), 'guest') AS user_key,
                COUNT(DISTINCT o.id) AS total_orders,
                AVG(o.total) AS avg_order_value,
                SUM(o.total) AS lifetime_value,
                SUM(CASE WHEN o.status = 'cancelled' THEN 1 ELSE 0 END) AS cancelled_orders
            FROM orders o
            GROUP BY COALESCE(CAST(o.user_id AS TEXT), 'guest')
            ORDER BY total_orders DESC
            LIMIT ?
            """,
            (limit,),
        ).fetchall()
    return [dict(r) for r in rows]


def fetch_cancellation_summary(limit=200):
    with get_db_connection() as conn:
        rows = conn.execute(
            """
            SELECT
                COALESCE(u.username, 'guest') AS customer,
                COUNT(*) AS cancelled_orders,
                MAX(o.created_at) AS last_cancelled_at
            FROM orders o
            LEFT JOIN users u ON u.id = o.user_id
            WHERE o.status = 'cancelled'
            GROUP BY COALESCE(u.username, 'guest')
            ORDER BY cancelled_orders DESC, last_cancelled_at DESC
            LIMIT ?
            """,
            (limit,),
        ).fetchall()
    return [dict(r) for r in rows]


def cancel_order_for_session(order_id):
    with get_db_connection() as conn:
        row = conn.execute(
            """
            SELECT id, method, status
            FROM orders
            WHERE id = ?
            """,
            (order_id,),
        ).fetchone()
        if not row:
            return False, "Order not found."
        if row["status"] == "cancelled":
            return False, "This order is already cancelled."
        conn.execute("UPDATE orders SET status = 'cancelled' WHERE id = ?", (order_id,))
        conn.commit()
    return True, f"Order {order_id} cancelled successfully."


def add_order_to_history(order_id):
    history = session.get("order_history", [])
    if order_id not in history:
        history.append(order_id)
    session["order_history"] = history[-30:]
    session.modified = True


def fetch_orders_for_history(order_ids, limit=10):
    if not order_ids:
        return []

    selected = order_ids[-limit:]
    placeholders = ",".join(["?"] * len(selected))
    query = f"""
        SELECT id, created_at, method, total, status
        FROM orders
        WHERE id IN ({placeholders})
    """
    with get_db_connection() as conn:
        rows = conn.execute(query, tuple(selected)).fetchall()

    rows_by_id = {r["id"]: r for r in rows}
    ordered = []
    for oid in reversed(selected):
        r = rows_by_id.get(oid)
        if r:
            ordered.append(dict(r))
    return ordered


def fetch_orders_for_user_history(user_id, limit=10):
    with get_db_connection() as conn:
        rows = conn.execute(
            """
            SELECT id, created_at, method, total, status
            FROM orders
            WHERE user_id = ?
            ORDER BY created_at DESC
            LIMIT ?
            """,
            (user_id, limit),
        ).fetchall()
    return [dict(r) for r in rows]


def get_family_group_for_user(user_id):
    if not user_id:
        return None
    with get_db_connection() as conn:
        row = conn.execute(
            """
            SELECT fg.id, fg.name, fg.invite_code
            FROM family_members fm
            JOIN family_groups fg ON fg.id = fm.group_id
            WHERE fm.user_id = ?
            LIMIT 1
            """,
            (user_id,),
        ).fetchone()
    return dict(row) if row else None


def create_family_group(user_id, family_name):
    if not user_id:
        return None, "Please login first."
    existing = get_family_group_for_user(user_id)
    if existing:
        return existing, f"You are already in family '{existing['name']}'."

    created_at = datetime.datetime.now().isoformat(timespec="seconds")
    for _ in range(5):
        code = "FM" + "".join(random.choice("ABCDEFGHJKLMNPQRSTUVWXYZ23456789") for _ in range(6))
        try:
            with get_db_connection() as conn:
                cur = conn.execute(
                    "INSERT INTO family_groups(name, invite_code, created_by, created_at) VALUES (?, ?, ?, ?)",
                    (family_name, code, user_id, created_at),
                )
                group_id = cur.lastrowid
                conn.execute(
                    "INSERT INTO family_members(group_id, user_id, role, joined_at) VALUES (?, ?, 'owner', ?)",
                    (group_id, user_id, created_at),
                )
                conn.commit()
            return {"id": group_id, "name": family_name, "invite_code": code}, None
        except Exception as exc:
            if not is_unique_constraint_error(exc):
                return None, "Could not create family right now. Please try again."
            continue
    return None, "Could not create family right now. Please try again."


def join_family_group(user_id, invite_code):
    if not user_id:
        return None, "Please login first."
    existing = get_family_group_for_user(user_id)
    if existing:
        return existing, f"You are already in family '{existing['name']}'."

    code = (invite_code or "").strip().upper()
    if not code:
        return None, "Please provide a valid family invite code."

    with get_db_connection() as conn:
        grp = conn.execute(
            "SELECT id, name, invite_code FROM family_groups WHERE invite_code = ?",
            (code,),
        ).fetchone()
        if not grp:
            return None, "Invalid family invite code."
        try:
            conn.execute(
                "INSERT INTO family_members(group_id, user_id, role, joined_at) VALUES (?, ?, 'member', ?)",
                (int(grp["id"]), user_id, datetime.datetime.now().isoformat(timespec="seconds")),
            )
            conn.commit()
        except Exception:
            pass
    return {"id": int(grp["id"]), "name": grp["name"], "invite_code": grp["invite_code"]}, None


def add_family_list_item(user_id, item, qty=1.0, unit=None):
    grp = get_family_group_for_user(user_id)
    if not grp:
        return False, "You are not in a family group. Use 'create family <name>' or 'join family <code>'."

    item_name = normalize_text(item).strip()
    if not item_name:
        return False, "Please provide item name."
    qty = max(float(qty), 0.01)
    now = datetime.datetime.now().isoformat(timespec="seconds")

    with get_db_connection() as conn:
        row = conn.execute(
            """
            SELECT id, qty
            FROM family_list_items
            WHERE group_id = ? AND lower(item) = lower(?) AND COALESCE(unit,'') = COALESCE(?, '')
            LIMIT 1
            """,
            (int(grp["id"]), item_name, unit),
        ).fetchone()
        if row:
            conn.execute(
                "UPDATE family_list_items SET qty = ?, updated_at = ?, is_checked = 0, last_updated_by = ? WHERE id = ?",
                (float(row["qty"]) + qty, now, user_id, int(row["id"])),
            )
        else:
            conn.execute(
                """
                INSERT INTO family_list_items(group_id, item, qty, unit, added_by, last_updated_by, is_checked, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, 0, ?)
                """,
                (int(grp["id"]), item_name, qty, unit, user_id, user_id, now),
            )
        conn.commit()
    return True, f"Added {format_qty(qty)} {unit or ''} {item_name.title()} to family list.".replace("  ", " ").strip()


def remove_family_list_item(user_id, item, qty=1.0):
    grp = get_family_group_for_user(user_id)
    if not grp:
        return False, "You are not in a family group."

    item_name = normalize_text(item).strip()
    if not item_name:
        return False, "Please provide item name."
    qty = max(float(qty), 0.01)

    with get_db_connection() as conn:
        row = conn.execute(
            """
            SELECT id, qty, item
            FROM family_list_items
            WHERE group_id = ? AND lower(item) = lower(?)
            ORDER BY updated_at DESC
            LIMIT 1
            """,
            (int(grp["id"]), item_name),
        ).fetchone()
        if not row:
            return False, f"{item_name.title()} is not in family list."
        remaining = float(row["qty"]) - qty
        if remaining <= 0:
            conn.execute("DELETE FROM family_list_items WHERE id = ?", (int(row["id"]),))
        else:
            conn.execute(
                "UPDATE family_list_items SET qty = ?, updated_at = ?, last_updated_by = ? WHERE id = ?",
                (remaining, datetime.datetime.now().isoformat(timespec="seconds"), user_id, int(row["id"])),
            )
        conn.commit()
    return True, f"Updated family list for {item_name.title()}."


def fetch_family_list(user_id):
    grp = get_family_group_for_user(user_id)
    if not grp:
        return None, []
    with get_db_connection() as conn:
        rows = conn.execute(
            """
            SELECT
                fli.item,
                fli.qty,
                fli.unit,
                fli.updated_at,
                ua.username AS added_by,
                uu.username AS last_updated_by
            FROM family_list_items fli
            LEFT JOIN users ua ON ua.id = fli.added_by
            LEFT JOIN users uu ON uu.id = fli.last_updated_by
            WHERE fli.group_id = ? AND COALESCE(fli.is_checked, 0) = 0
            ORDER BY fli.updated_at DESC, fli.item ASC
            """,
            (int(grp["id"]),),
        ).fetchall()
    return grp, [dict(r) for r in rows]


def add_family_list_to_cart(state, user_id):
    group, rows = fetch_family_list(user_id)
    if not group:
        return False, "You are not in a family group yet."
    if not rows:
        return False, "Your family list is empty."

    orders = state["orders"]
    total = state["total"]
    added = []

    for r in rows:
        product = find_best_product_match(r["item"], include_inactive=False)
        if not product:
            continue
        qty = max(float(r.get("qty", 1.0)), 0.01)
        qty_in_base, err = convert_quantity_to_base(qty, r.get("unit"), product["base_unit"])
        if err:
            continue
        line_total = int(round(float(product["price_per_unit"]) * qty_in_base))
        orders.append(
            {
                "item_id": product["id"],
                "item": product["name"],
                "qty": qty_in_base,
                "unit": product["base_unit"],
                "price_per_unit": float(product["price_per_unit"]),
                "line_total": line_total,
            }
        )
        total += line_total
        added.append(product["name"])

    if not added:
        return False, "Family list items are not currently available in catalog."

    state["orders"] = orders
    state["total"] = total
    state["last_item"] = added[-1]
    state["pending_add"] = None
    save_user_state(state)
    return True, f"Added family list items to cart: {', '.join(a.title() for a in added)}. Total: Rs {total}"


def normalize_item_key(item_name):
    key = normalize_text(item_name or "").strip()
    if key.endswith("es") and len(key) > 3:
        key = key[:-2]
    elif key.endswith("s") and len(key) > 2:
        key = key[:-1]
    return key


def format_since_days(days):
    d = max(float(days), 0.0)
    if d < (1.0 / 24.0):
        return "just now"
    if d < 1.0:
        hours = max(1, int(round(d * 24.0)))
        return f"{hours} hour{'s' if hours != 1 else ''} ago"
    whole = int(round(d))
    return f"{whole} day{'s' if whole != 1 else ''} ago"


def fetch_family_order_timeline(user_id, limit=25):
    grp = get_family_group_for_user(user_id)
    if not grp:
        return None, []
    with get_db_connection() as conn:
        rows = conn.execute(
            """
            SELECT o.id AS order_id, o.created_at, u.username, oi.item, oi.qty, oi.unit, oi.line_total
            FROM orders o
            JOIN family_members fm ON fm.user_id = o.user_id
            JOIN order_items oi ON oi.order_id = o.id
            LEFT JOIN users u ON u.id = o.user_id
            WHERE fm.group_id = ?
              AND o.status = 'placed'
            ORDER BY o.created_at DESC, o.id DESC
            LIMIT ?
            """,
            (int(grp["id"]), int(limit)),
        ).fetchall()
    return grp, [dict(r) for r in rows]


def fetch_recent_family_item_activity(user_id, item_name, lookback_days=14, limit=5):
    grp = get_family_group_for_user(user_id)
    if not grp:
        return None, []
    item_key = normalize_item_key(item_name)
    if not item_key:
        return grp, []

    cutoff = (datetime.datetime.now() - datetime.timedelta(days=lookback_days)).isoformat(timespec="seconds")
    with get_db_connection() as conn:
        rows = conn.execute(
            """
            SELECT o.created_at, u.username, oi.item, oi.qty, oi.unit
            FROM orders o
            JOIN family_members fm ON fm.user_id = o.user_id
            JOIN order_items oi ON oi.order_id = o.id
            LEFT JOIN users u ON u.id = o.user_id
            WHERE fm.group_id = ?
              AND o.status = 'placed'
              AND o.created_at >= ?
            ORDER BY o.created_at DESC
            """,
            (int(grp["id"]), cutoff),
        ).fetchall()

    matches = []
    for r in rows:
        row_key = normalize_item_key(r["item"])
        if row_key == item_key or item_key in row_key or row_key in item_key:
            matches.append(dict(r))
        if len(matches) >= limit:
            break
    return grp, matches


def build_family_duplicate_hint(user_id, item_name, lookback_days=7):
    grp, recent = fetch_recent_family_item_activity(user_id, item_name, lookback_days=lookback_days, limit=3)
    if not grp or not recent:
        return None
    _, estimate = estimate_family_item_stock(user_id, item_name, lookback_days=60)
    estimate_line = ""
    if estimate:
        estimate_line = (
            f"\nEstimated stock left: {estimate['score']}% ({estimate['label']})"
            f" | last bought {format_since_days(estimate['days_since_last'])}."
        )
    lines = []
    for r in recent:
        try:
            dt = datetime.datetime.fromisoformat(r["created_at"])
            day_label = dt.strftime("%a %d %b")
        except Exception:
            day_label = str(r["created_at"])
        lines.append(
            f"- {day_label}: {r.get('username') or 'member'} bought {r['item'].title()} {format_qty(r['qty'])} {r['unit'] or ''}".replace("  ", " ").strip()
        )
    return (
        f"Family already bought {item_name.title()} recently.\n"
        + "\n".join(lines)
        + estimate_line
        + "\nIf you still need more stock, say 'add anyway'."
    )


def estimate_family_item_stock(user_id, item_name, lookback_days=60):
    grp, rows = fetch_recent_family_item_activity(user_id, item_name, lookback_days=lookback_days, limit=200)
    if not grp or not rows:
        return grp, None

    parsed = []
    for r in rows:
        try:
            dt = datetime.datetime.fromisoformat(r["created_at"])
        except Exception:
            continue
        parsed.append(
            {
                "created_at": dt,
                "qty": float(r.get("qty", 1.0)),
                "unit": r.get("unit"),
                "item": r.get("item"),
                "username": r.get("username"),
            }
        )
    if not parsed:
        return grp, None

    parsed.sort(key=lambda x: x["created_at"], reverse=True)
    latest = parsed[0]
    earliest = parsed[-1]
    span_days = max((latest["created_at"] - earliest["created_at"]).total_seconds() / 86400.0, 1.0)
    total_qty = sum(max(p["qty"], 0.0) for p in parsed)
    daily_use = total_qty / span_days
    daily_use = max(daily_use, 1e-6)

    latest_qty = max(latest["qty"], 0.0)
    estimated_coverage_days = latest_qty / daily_use
    days_since_last = max((datetime.datetime.now() - latest["created_at"]).total_seconds() / 86400.0, 0.0)

    if estimated_coverage_days <= 0:
        stock_left_ratio = 0.0
    else:
        stock_left_ratio = max(0.0, min(1.0, 1.0 - (days_since_last / estimated_coverage_days)))

    score = int(round(stock_left_ratio * 100))
    if score >= 67:
        label = "High"
    elif score >= 34:
        label = "Medium"
    else:
        label = "Low"

    return grp, {
        "item": latest["item"],
        "unit": latest.get("unit"),
        "score": score,
        "label": label,
        "days_since_last": round(days_since_last, 1),
        "estimated_coverage_days": round(estimated_coverage_days, 1),
        "latest_qty": latest_qty,
        "latest_buyer": latest.get("username") or "member",
        "latest_time": latest["created_at"].isoformat(timespec="seconds"),
        "samples": len(parsed),
    }


def fetch_family_stock_snapshot(user_id, max_items=8):
    grp = get_family_group_for_user(user_id)
    if not grp:
        return None, []
    timeline_grp, rows = fetch_family_order_timeline(user_id, limit=250)
    if not timeline_grp or not rows:
        return grp, []

    seen = []
    seen_keys = set()
    for r in rows:
        key = normalize_item_key(r.get("item", ""))
        if not key or key in seen_keys:
            continue
        seen_keys.add(key)
        seen.append(r["item"])
        if len(seen) >= max_items:
            break

    snapshot = []
    for item in seen:
        _, est = estimate_family_item_stock(user_id, item, lookback_days=60)
        if est:
            snapshot.append(est)

    snapshot.sort(key=lambda x: x["score"])
    return grp, snapshot


def fetch_monthly_insights(user_id, dt=None):
    if not user_id:
        return None
    current = dt or datetime.datetime.now()
    start = datetime.datetime(current.year, current.month, 1)
    if current.month == 12:
        end = datetime.datetime(current.year + 1, 1, 1)
    else:
        end = datetime.datetime(current.year, current.month + 1, 1)

    with get_db_connection() as conn:
        order_rows = conn.execute(
            """
            SELECT id, total
            FROM orders
            WHERE user_id = ?
              AND status = 'placed'
              AND created_at >= ?
              AND created_at < ?
            """,
            (user_id, start.isoformat(timespec="seconds"), end.isoformat(timespec="seconds")),
        ).fetchall()
        if not order_rows:
            return {
                "month_label": start.strftime("%B %Y"),
                "total_spend": 0.0,
                "top_category": None,
                "snacks_spend": 0.0,
                "order_count": 0,
            }

        order_ids = [r["id"] for r in order_rows]
        placeholders = ",".join("?" for _ in order_ids)
        item_rows = conn.execute(
            f"""
            SELECT oi.item, oi.item_id, oi.line_total
            FROM order_items oi
            WHERE oi.order_id IN ({placeholders})
            """,
            tuple(order_ids),
        ).fetchall()
        product_rows = conn.execute(
            """
            SELECT p.id, p.name, c.name AS category
            FROM products p
            JOIN categories c ON c.id = p.category_id
            """
        ).fetchall()

    categories_by_id = {int(r["id"]): r["category"] for r in product_rows}
    categories_by_name = {str(r["name"]).lower().strip(): r["category"] for r in product_rows}

    category_spend = {}
    for row in item_rows:
        item_id = row["item_id"]
        cat = None
        if item_id is not None and int(item_id) in categories_by_id:
            cat = categories_by_id[int(item_id)]
        if not cat:
            cat = categories_by_name.get(str(row["item"]).lower().strip(), "other")
        category_spend[cat] = category_spend.get(cat, 0.0) + float(row["line_total"])

    total_spend = float(sum(float(r["total"]) for r in order_rows))
    top_category = None
    if category_spend:
        top_category = max(category_spend.items(), key=lambda kv: kv[1])[0]
    snacks_spend = float(category_spend.get("snacks", 0.0))
    return {
        "month_label": start.strftime("%B %Y"),
        "total_spend": round(total_spend, 2),
        "top_category": top_category,
        "snacks_spend": round(snacks_spend, 2),
        "order_count": int(len(order_rows)),
    }


def resolve_cancellable_order_id():
    last_order_id = session.get("last_order_id")
    if last_order_id:
        with get_db_connection() as conn:
            row = conn.execute(
                "SELECT id FROM orders WHERE id = ? AND status = 'placed'",
                (last_order_id,),
            ).fetchone()
            if row:
                return row["id"]

    user_id = get_logged_in_user_id()
    with get_db_connection() as conn:
        if user_id:
            row = conn.execute(
                """
                SELECT id
                FROM orders
                WHERE user_id = ? AND status = 'placed'
                ORDER BY created_at DESC
                LIMIT 1
                """,
                (user_id,),
            ).fetchone()
            if row:
                return row["id"]

        for oid in reversed(session.get("order_history", [])):
            row = conn.execute(
                "SELECT id FROM orders WHERE id = ? AND status = 'placed'",
                (oid,),
            ).fetchone()
            if row:
                return row["id"]

        session_user_id = session.get("session_user_id")
        if session_user_id:
            row = conn.execute(
                """
                SELECT o.id
                FROM orders o
                JOIN event_logs e ON e.order_id = o.id
                WHERE e.session_user_id = ?
                  AND e.event_type = 'order_placed'
                  AND o.status = 'placed'
                ORDER BY o.created_at DESC
                LIMIT 1
                """,
                (session_user_id,),
            ).fetchone()
            if row:
                return row["id"]
    return None


def fetch_recent_orders(limit=100):
    with get_db_connection() as conn:
        orders = conn.execute(
            """
            SELECT o.id, o.created_at, o.method, o.address, o.subtotal, o.delivery_fee, o.total, o.status, u.username
            FROM orders o
            LEFT JOIN users u ON u.id = o.user_id
            ORDER BY o.created_at DESC
            LIMIT ?
            """,
            (limit,),
        ).fetchall()

        order_rows = []
        for row in orders:
            items = conn.execute(
                """
                SELECT item, qty, unit, line_total
                FROM order_items
                WHERE order_id = ?
                ORDER BY id ASC
                """,
                (row["id"],),
            ).fetchall()
            details = ", ".join(
                f"{i['item'].title()} {format_qty(i['qty'])} {i['unit']} (Rs {i['line_total']})"
                for i in items
            )
            order_rows.append(
                {
                    "id": row["id"],
                    "time": row["created_at"],
                    "method": row["method"],
                    "address": row["address"] or "-",
                    "subtotal": row["subtotal"],
                    "delivery_fee": row["delivery_fee"],
                    "total": row["total"],
                    "status": row["status"] or "placed",
                    "customer": row["username"] or "guest",
                    "details": details or "-",
                }
            )
    return order_rows


def is_admin_authenticated():
    return session.get("admin_authenticated", False)


def is_user_authenticated():
    return bool(get_logged_in_user_id())


def require_user_login():
    if not is_user_authenticated():
        return jsonify({"error": "Authentication required. Please login or register."}), 401
    return None


def validate_admin_password(password):
    if ADMIN_PASSWORD_HASH:
        return check_password_hash(ADMIN_PASSWORD_HASH, password)
    if ADMIN_PASSWORD:
        return password == ADMIN_PASSWORD
    # Local development fallback so admin access works without extra env setup.
    return password == LOCAL_DEV_ADMIN_PASSWORD


def require_admin_json():
    if not is_admin_authenticated():
        return jsonify({"error": "Unauthorized"}), 401
    return None


def normalize_text(text):
    normalized = " ".join(text.lower().strip().split())
    normalized = normalized.replace("half", "0.5").replace("aadha", "0.5")
    for src, target in HINGLISH_MAP.items():
        normalized = re.sub(rf"\b{re.escape(src)}\b", target, normalized)
    return normalized


def canonical_unit(unit_token):
    if not unit_token:
        return None
    return UNIT_ALIASES.get(unit_token.lower().strip())


def parse_item_request(message):
    text = normalize_text(message)
    text = re.sub(r"^add\s+", "", text).strip()

    leading = re.match(
        r"^(?P<qty>\d+(?:\.\d+)?)\s*(?P<unit>[a-zA-Z]+)?\s+(?P<item>.+)$", text
    )
    if leading:
        qty = float(leading.group("qty"))
        unit = canonical_unit(leading.group("unit"))
        item = leading.group("item").strip()
        return item, qty, unit

    trailing = re.match(
        r"^(?P<item>.+?)\s+(?P<qty>\d+(?:\.\d+)?)\s*(?P<unit>[a-zA-Z]+)?$", text
    )
    if trailing:
        qty = float(trailing.group("qty"))
        unit = canonical_unit(trailing.group("unit"))
        item = trailing.group("item").strip()
        return item, qty, unit

    compact = re.match(
        r"^(?P<item>.+?)\s+(?P<qty>\d+(?:\.\d+)?)(?P<unit>[a-zA-Z]+)$", text
    )
    if compact:
        qty = float(compact.group("qty"))
        unit = canonical_unit(compact.group("unit"))
        item = compact.group("item").strip()
        return item, qty, unit

    return text, 1.0, None


def convert_quantity_to_base(qty, input_unit, base_unit):
    unit = input_unit or base_unit
    if unit == base_unit:
        return qty, None

    if base_unit == "kg" and unit == "g":
        return qty / 1000.0, None
    if base_unit == "g" and unit == "kg":
        return qty * 1000.0, None

    if base_unit == "litre" and unit == "ml":
        return qty / 1000.0, None
    if base_unit == "ml" and unit == "litre":
        return qty * 1000.0, None

    if base_unit == "piece" and unit == "dozen":
        return qty * 12.0, None
    if base_unit == "dozen" and unit == "piece":
        return qty / 12.0, None

    return None, f"This item is sold in {base_unit}. You entered {unit}."


def fetch_products(include_inactive=False):
    with get_db_connection() as conn:
        if include_inactive:
            rows = conn.execute(
                """
                SELECT p.id, p.name, p.price_per_unit, p.base_unit, p.aliases, p.is_active, c.name AS category
                FROM products p
                JOIN categories c ON c.id = p.category_id
                ORDER BY c.name, p.name
                """
            ).fetchall()
        else:
            rows = conn.execute(
                """
                SELECT p.id, p.name, p.price_per_unit, p.base_unit, p.aliases, p.is_active, c.name AS category
                FROM products p
                JOIN categories c ON c.id = p.category_id
                WHERE p.is_active = 1
                ORDER BY c.name, p.name
                """
            ).fetchall()
    return [dict(r) for r in rows]


def find_best_product_match(item_query, include_inactive=False):
    products = fetch_products(include_inactive=include_inactive)
    if not products:
        return None

    cleaned_query = normalize_text(item_query).replace("of ", "").strip()
    alias_to_product = {}
    terms = []

    for p in products:
        names = [p["name"]]
        if p["aliases"]:
            names.extend([a.strip() for a in p["aliases"].split(",") if a.strip()])
        for token in names:
            t = normalize_text(token)
            alias_to_product[t] = p
            terms.append(t)

    if cleaned_query in alias_to_product:
        return alias_to_product[cleaned_query]

    match = get_close_matches(cleaned_query, terms, n=1, cutoff=0.62)
    if match:
        return alias_to_product[match[0]]
    return None


def looks_like_product_request(message, qty, unit):
    if unit is not None:
        return True
    if qty != 1.0:
        return True
    if message.startswith("add "):
        return True
    if re.search(r"\d", message):
        return True
    return False


def is_context_followup_message(message):
    lowered = normalize_text(message)
    return bool(re.search(r"\b(more|same|again|another|it|them)\b", lowered))


def is_not_coming_message(message):
    m = message.strip().lower()
    exact = {
        "no",
        "nope",
        "nah",
        "not coming",
        "i am not coming",
        "im not coming",
        "i amnt coming",
        "can't come",
        "cant come",
        "will not come",
        "wont come",
    }
    if m in exact:
        return True
    patterns = [
        r"\bnot\s+coming\b",
        r"\bi\s*am\s*nt\s*coming\b",
        r"\bcan't\s+come\b",
        r"\bcant\s+come\b",
        r"\bwill\s+not\s+come\b",
        r"\bwon't\s+come\b",
    ]
    return any(re.search(p, m) for p in patterns)


def parse_remove_request(msg):
    if "undo" in msg:
        return {"mode": "last"}

    match = re.search(r"\bremove\b(.*)$", msg)
    if not match:
        return {"mode": "last"}

    payload = match.group(1).strip()
    if not payload or payload in {"last", "previous"}:
        return {"mode": "last"}

    item_query, qty, unit = parse_item_request(payload)
    return {"mode": "item", "item_query": item_query, "qty": qty, "unit": unit}


def remove_item_from_cart(orders, product, qty_to_remove):
    remaining = qty_to_remove
    removed_qty = 0.0
    removed_amount = 0

    for idx in range(len(orders) - 1, -1, -1):
        if remaining <= 1e-9:
            break

        row = orders[idx]
        same_item = row.get("item_id") == product.get("id") or row.get("item") == product.get("name")
        if not same_item:
            continue

        row_qty = float(row.get("qty", 0))
        if row_qty <= 0:
            continue

        take = min(row_qty, remaining)
        if abs(take - row_qty) <= 1e-9:
            removed_amount += int(row.get("line_total", 0))
            removed_qty += row_qty
            remaining -= row_qty
            orders.pop(idx)
        else:
            old_line_total = int(row.get("line_total", 0))
            unit_price = row.get("price_per_unit")
            if unit_price is None:
                unit_price = old_line_total / row_qty
            new_qty = row_qty - take
            new_line_total = int(round(float(unit_price) * new_qty))
            row["qty"] = new_qty
            row["line_total"] = new_line_total
            removed_amount += (old_line_total - new_line_total)
            removed_qty += take
            remaining -= take

    return removed_qty, removed_amount


def get_alternative_products(target_product, limit=3):
    all_active = fetch_products(include_inactive=False)
    if not all_active:
        return []

    same_category = [
        p["name"]
        for p in all_active
        if p.get("category") == target_product.get("category") and p["id"] != target_product["id"]
    ]
    if same_category:
        return same_category[:limit]

    fallback = [p["name"] for p in all_active if p["id"] != target_product["id"]]
    return fallback[:limit]


def list_categories():
    with get_db_connection() as conn:
        rows = conn.execute("SELECT id, name FROM categories ORDER BY name").fetchall()
    return [dict(r) for r in rows]


def products_for_category(category_name):
    with get_db_connection() as conn:
        rows = conn.execute(
            """
            SELECT p.id, p.name, p.price_per_unit, p.base_unit
            FROM products p
            JOIN categories c ON c.id = p.category_id
            WHERE lower(c.name) = ? AND p.is_active = 1
            ORDER BY p.name
            """,
            (category_name.lower().strip(),),
        ).fetchall()
    return [dict(r) for r in rows]


def detect_dietary_preference(message):
    text = normalize_text(message)
    if "vegan" in text:
        return "vegan"
    if "jain" in text:
        return "jain"
    if "diabetic" in text or "low sugar" in text:
        return "diabetic"
    return None


def filter_items_for_diet(items, dietary_preference):
    if not dietary_preference:
        return list(items)
    blocked = DIETARY_BLOCKLIST.get(dietary_preference, set())
    return [it for it in items if it not in blocked]


def infer_recipe_from_message(message):
    text = normalize_text(message)
    for recipe_name in RECIPE_KITS:
        if recipe_name in text:
            return recipe_name
    if "making pasta" in text:
        return "pasta"
    return None


def build_recipe_plan(recipe_name, dietary_preference=None):
    needed = RECIPE_KITS.get(recipe_name, [])
    needed = filter_items_for_diet(needed, dietary_preference)
    if not needed:
        return None

    available = fetch_products(include_inactive=False)
    available_by_name = {p["name"]: p for p in available}
    in_stock = [n for n in needed if n in available_by_name]
    missing = [n for n in needed if n not in available_by_name]

    lines = []
    for name in in_stock:
        p = available_by_name[name]
        lines.append(f"- {name.title()}: Rs {int(round(float(p['price_per_unit'])))} / {p['base_unit']}")
    if missing:
        lines.append(f"Missing right now: {', '.join(m.title() for m in missing)}")
    return lines


def add_recipe_to_cart(recipe_name, state):
    recipe_key = normalize_text(recipe_name).strip()
    if recipe_key not in RECIPE_KITS:
        return False, f"I do not have a recipe kit for {recipe_name}.", []

    desired = RECIPE_KITS[recipe_key]
    desired = filter_items_for_diet(desired, state.get("dietary_preference"))
    if not desired:
        return False, "No ingredients available after applying dietary preference.", []

    added_items = []
    orders = state["orders"]
    total = state["total"]
    for item_name in desired:
        product = find_best_product_match(item_name, include_inactive=False)
        if not product:
            continue
        qty_in_base = 1.0
        line_total = int(round(float(product["price_per_unit"]) * qty_in_base))
        orders.append(
            {
                "item_id": product["id"],
                "item": product["name"],
                "qty": qty_in_base,
                "unit": product["base_unit"],
                "price_per_unit": float(product["price_per_unit"]),
                "line_total": line_total,
            }
        )
        total += line_total
        state["last_item"] = product["name"]
        mem = state.get("item_memory", {})
        mem[product["name"]] = int(mem.get(product["name"], 0) + 1)
        state["item_memory"] = mem
        added_items.append(product["name"])

    state["orders"] = orders
    state["total"] = total
    save_user_state(state)
    if not added_items:
        return False, f"No ingredients from {recipe_name} are currently available.", []
    return True, f"Added ingredients for {recipe_name.title()}: {', '.join(i.title() for i in added_items)}.", added_items


def fetch_restock_suggestions(limit=5):
    user_id = get_logged_in_user_id()
    session_user_id = get_session_user_id()
    with get_db_connection() as conn:
        if user_id:
            rows = conn.execute(
                """
                SELECT oi.item, COUNT(*) AS times, MAX(o.created_at) AS last_time
                FROM orders o
                JOIN order_items oi ON oi.order_id = o.id
                WHERE o.status = 'placed' AND o.user_id = ?
                GROUP BY oi.item
                ORDER BY times DESC, last_time DESC
                LIMIT ?
                """,
                (user_id, limit),
            ).fetchall()
        else:
            rows = conn.execute(
                """
                SELECT oi.item, COUNT(*) AS times, MAX(o.created_at) AS last_time
                FROM orders o
                JOIN order_items oi ON oi.order_id = o.id
                WHERE o.status = 'placed' AND o.session_user_id = ?
                GROUP BY oi.item
                ORDER BY times DESC, last_time DESC
                LIMIT ?
                """,
                (session_user_id, limit),
            ).fetchall()
    return [dict(r) for r in rows]


def handle_lifestyle_context(message, state):
    text = normalize_text(message)

    brand_match = re.search(r"\bprefer\s+([a-z0-9]+)\s+(?:for\s+)?([a-z\s]+)$", text)
    if brand_match:
        brand = brand_match.group(1).strip()
        item = brand_match.group(2).strip()
        prefs = state.get("brand_preferences", {})
        prefs[item] = brand
        state["brand_preferences"] = prefs
        save_user_state(state)
        return f"Saved brand preference: {brand.title()} for {item.title()}."

    diet = detect_dietary_preference(text)
    if diet:
        state["dietary_preference"] = diet
        save_user_state(state)
        return f"Saved your preference: {diet.title()}. I will adapt suggestions accordingly."

    if "budget is tight" in text or "tight budget" in text:
        state["budget_mode"] = "tight"
        save_user_state(state)
        return "Understood. I will prioritize affordable essentials in suggestions this week."

    if any(k in text for k in ["guests", "party", "friends coming"]):
        return "Got it. For guests, I suggest quick picks: chips, biscuits, juice, fruits, and easy snacks."

    if any(k in text for k in ["i am sick", "not well", "fever", "cold"]):
        return "Take care. Suggested gentle items: curd, bananas, oats, bread, and light fruits."

    if any(k in text for k in ["running low", "restock", "reorder essentials"]):
        restock = fetch_restock_suggestions(limit=5)
        if not restock:
            return "I need a bit more order history to suggest restock items. Place a few orders and ask again."
        lines = "\n".join([f"- {r['item'].title()} (ordered {r['times']} times)" for r in restock])
        return f"Based on your history, consider restocking:\n{lines}"

    recipe = infer_recipe_from_message(text)
    if recipe:
        plan = build_recipe_plan(recipe, dietary_preference=state.get("dietary_preference"))
        if not plan:
            return "I could not build a recipe plan with current dietary settings."
        return {
            "reply": f"Great idea. For {recipe.title()}, add:\n" + "\n".join(plan),
            "actions": [
                {"label": f"Add All {recipe.title()}", "message": f"add recipe {recipe}"},
                {"label": "Show Cart", "message": "bill"},
            ],
        }

    if "what can i cook with" in text:
        ingredient = text.split("what can i cook with", 1)[1].strip()
        matches = [name for name, items in RECIPE_KITS.items() if ingredient and ingredient in " ".join(items)]
        if matches:
            return "You can make: " + ", ".join(m.title() for m in matches[:4]) + ". Say 'making <recipe>' to get an ingredient plan."
        return f"I do not have a recipe set for {ingredient} yet, but I can still help build a custom cart."

    return None


def load_budget_model():
    global BUDGET_MODEL_CACHE
    if BUDGET_MODEL_CACHE is not None:
        return BUDGET_MODEL_CACHE

    model_path = BUDGET_MODEL_PATH
    if not os.path.isabs(model_path):
        model_path = os.path.join(os.path.dirname(__file__), model_path)
    if not os.path.exists(model_path):
        return None

    try:
        with open(model_path, "rb") as fp:
            BUDGET_MODEL_CACHE = pickle.load(fp)
    except Exception:
        BUDGET_MODEL_CACHE = None
    return BUDGET_MODEL_CACHE


def parse_budget_request(raw_message):
    text = normalize_text(raw_message)
    match = re.search(r"\b(\d+(?:\.\d+)?)\b", text)
    if not match:
        return None, None
    budget = float(match.group(1))
    category = None
    for c in list_categories():
        if c["name"].lower() in text:
            category = c["name"].lower()
            break
    return budget, category


def optimize_budget_plan(budget, preferred_category=None):
    if budget <= 0:
        return {"error": "Budget must be greater than 0."}, 400

    products = fetch_products(include_inactive=False)
    if not products:
        return {"error": "No active products available for optimization."}, 404

    normalized = []
    for p in products:
        normalized.append(
            {
                "product_id": int(p["id"]),
                "name": str(p["name"]),
                "category": str(p["category"]).lower().strip(),
                "price": float(p["price_per_unit"]),
                "demand_score": 1.0,
                "stock": 10,
            }
        )

    if preferred_category:
        preferred = preferred_category.lower().strip()
        category_only = [p for p in normalized if p["category"] == preferred]
        if not category_only:
            return {
                "error": f"No active products found in category '{preferred_category}'. Try another category."
            }, 404
        normalized = category_only

    model = load_budget_model()
    chosen = []
    total = 0.0

    if model is not None:
        try:
            import numpy as np
            import pandas as pd

            df = pd.DataFrame(normalized)
            df["budget"] = np.float64(budget)
            df["preferred_category"] = (preferred_category or "none").lower().strip()
            features = df[["budget", "price", "demand_score", "category", "preferred_category"]]
            predicted_qty = model.predict(features).astype(np.float64)
            predicted_qty = np.clip(predicted_qty, 0.0, 6.0)
            safe_price = np.maximum(df["price"].to_numpy(dtype=np.float64), np.float64(1e-6))
            utility = predicted_qty * (1.0 + df["demand_score"].to_numpy(dtype=np.float64)) / safe_price
            df["utility"] = utility
            df["pred_qty"] = np.maximum(np.rint(predicted_qty), 1).astype(int)

            for _, row in df.sort_values("utility", ascending=False).iterrows():
                qty = int(row["pred_qty"])
                line_total = float(row["price"]) * qty
                if total + line_total > budget:
                    qty = int((budget - total) // float(row["price"]))
                    line_total = float(row["price"]) * qty
                if qty <= 0:
                    continue
                chosen.append(
                    {
                        "product_id": int(row["product_id"]),
                        "name": str(row["name"]),
                        "category": str(row["category"]),
                        "qty": qty,
                        "unit_price": round(float(row["price"]), 2),
                        "line_total": round(line_total, 2),
                    }
                )
                total += line_total
                if total >= budget:
                    break
        except Exception:
            chosen = []
            total = 0.0

    if not chosen:
        ranked = sorted(
            normalized,
            key=lambda x: (
                1 if preferred_category and x["category"] == preferred_category.lower().strip() else 0,
                -(x["demand_score"] / max(x["price"], 1e-6)),
            ),
            reverse=True,
        )
        for p in ranked:
            qty = int((budget - total) // p["price"])
            if qty <= 0:
                continue
            qty = min(qty, 2)
            line_total = p["price"] * qty
            chosen.append(
                {
                    "product_id": p["product_id"],
                    "name": p["name"],
                    "category": p["category"],
                    "qty": qty,
                    "unit_price": round(p["price"], 2),
                    "line_total": round(line_total, 2),
                }
            )
            total += line_total
            if total >= budget:
                break

    if preferred_category:
        preferred = preferred_category.lower().strip()
        chosen = [it for it in chosen if str(it.get("category", "")).lower().strip() == preferred]
        total = float(sum(float(it.get("line_total", 0.0)) for it in chosen))

    return {
        "budget": float(round(budget, 2)),
        "preferred_category": preferred_category,
        "items": chosen,
        "total": float(round(total, 2)),
        "remaining": float(round(budget - total, 2)),
    }, 200


@app.route("/")
def home():
    return render_template("index.html")


@app.route("/health", methods=["GET"])
def health():
    return jsonify({"ok": True, "service": "grocery_chatbot"})


@app.route("/cart", methods=["GET"])
def cart_view():
    guard = require_user_login()
    if guard:
        return guard

    state = get_user_state()
    orders = state.get("orders", [])
    total = state.get("total", 0)
    normalized_items = [
        {
            "item_id": it.get("item_id"),
            "item": it.get("item"),
            "qty": float(it.get("qty", 0)),
            "unit": it.get("unit"),
            "price_per_unit": float(it.get("price_per_unit", 0)),
            "line_total": int(it.get("line_total", 0)),
        }
        for it in orders
    ]
    return jsonify({"items": normalized_items, "total": int(total)})


@app.route("/budget", methods=["GET"])
def budget_home():
    budget = request.args.get("budget", type=float)
    preferred_category = request.args.get("preferred_category", default=None, type=str)
    if budget is not None:
        result, status = optimize_budget_plan(budget, preferred_category=preferred_category)
        return jsonify(result), status
    return jsonify(
        {
            "message": "Budget planner is ready. Use /budget?budget=300&preferred_category=fruits or POST /budget/optimize with JSON body.",
            "example_get": "/budget?budget=300&preferred_category=fruits",
            "example_post": {"budget": 300, "preferred_category": "fruits"},
        }
    )


@app.route("/budget/optimize", methods=["GET", "POST"])
def budget_optimize():
    if request.method == "GET":
        budget = request.args.get("budget", type=float)
        preferred_category = request.args.get("preferred_category", default=None, type=str)
    else:
        payload = request.get_json(silent=True) or {}
        try:
            budget = float(payload.get("budget", 0))
        except (TypeError, ValueError):
            budget = 0.0
        preferred_category = payload.get("preferred_category")

    if budget is None or budget <= 0:
        return jsonify(
            {
                "error": "Provide a valid budget > 0.",
                "example_get": "/budget/optimize?budget=300&preferred_category=fruits",
                "example_post": {"budget": 300, "preferred_category": "fruits"},
            }
        ), 400

    result, status = optimize_budget_plan(budget, preferred_category=preferred_category)
    return jsonify(result), status


@app.route("/sw.js")
def service_worker():
    return send_from_directory("static", "sw.js")


@app.route("/favicon.ico")
def favicon():
    icon_path = os.path.join(app.root_path, "static", "favicon.ico")
    if os.path.exists(icon_path):
        return send_from_directory("static", "favicon.ico")
    return ("", 204)


@app.route("/push/public-key", methods=["GET"])
def push_public_key():
    return jsonify({"publicKey": VAPID_PUBLIC_KEY, "enabled": is_web_push_configured()})


@app.route("/push/subscribe", methods=["POST"])
def push_subscribe():
    payload = request.get_json(silent=True) or {}
    subscription = payload.get("subscription") or payload
    ok, message = upsert_push_subscription(subscription)
    code = 200 if ok else 400
    return jsonify({"status": "ok" if ok else "error", "message": message, "enabled": is_web_push_configured()}), code


@app.route("/push/unsubscribe", methods=["POST"])
def push_unsubscribe():
    payload = request.get_json(silent=True) or {}
    endpoint = payload.get("endpoint")
    deactivate_push_subscription(endpoint)
    return jsonify({"status": "ok"})


@app.route("/internal/run-reminders", methods=["POST"])
def internal_run_reminders():
    auth_header = request.headers.get("X-Internal-Token", "")
    if not CRON_SECRET or auth_header != CRON_SECRET:
        return jsonify({"error": "Unauthorized"}), 401
    soon = process_pickup_soon_reminders(send_push=True)
    reminders = process_late_pickup_reminders(send_push=True)
    return jsonify({"processed": len(soon) + len(reminders), "pickup_soon": len(soon), "pickup_late": len(reminders)})


@app.route("/auth/register", methods=["POST"])
def auth_register():
    payload = request.get_json(silent=True) or {}
    username = (payload.get("username") or "").strip().lower()
    password = payload.get("password") or ""

    if not username or len(username) < 3:
        return jsonify({"error": "Username must be at least 3 characters."}), 400
    if len(password) < 6:
        return jsonify({"error": "Password must be at least 6 characters."}), 400

    created_at = datetime.datetime.now().isoformat(timespec="seconds")
    password_hash = generate_password_hash(password)

    try:
        with get_db_connection() as conn:
            cur = conn.execute(
                """
                INSERT INTO users (username, password_hash, created_at)
                VALUES (?, ?, ?)
                """,
                (username, password_hash, created_at),
            )
            conn.commit()
            user_id = cur.lastrowid
    except Exception as exc:
        if is_unique_constraint_error(exc):
            return jsonify({"error": "Username already exists."}), 409
        return jsonify({"error": f"Could not register user: {str(exc)}"}), 500

    session["user_id"] = user_id
    session["username"] = username
    session.modified = True
    log_event("user_registered", {"username": username})
    return jsonify({"status": "registered", "user": {"id": user_id, "username": username}})


@app.route("/auth/login", methods=["POST"])
def auth_login():
    payload = request.get_json(silent=True) or {}
    username = (payload.get("username") or "").strip().lower()
    password = payload.get("password") or ""

    if not username or not password:
        return jsonify({"error": "Username and password are required."}), 400

    with get_db_connection() as conn:
        user = conn.execute(
            "SELECT id, username, password_hash FROM users WHERE username = ?",
            (username,),
        ).fetchone()

    password_hash = row_value(user, "password_hash", fallback_index=2, default="") if user else ""
    if not user or not check_password_hash(password_hash, password):
        return jsonify({"error": "Invalid credentials."}), 401

    user_id = row_value(user, "id", fallback_index=0, default=None)
    username_value = row_value(user, "username", fallback_index=1, default=username)
    session["user_id"] = user_id
    session["username"] = username_value
    session.modified = True
    log_event("user_logged_in", {"username": username_value})
    return jsonify({"status": "logged_in", "user": {"id": user_id, "username": username_value}})


@app.route("/auth/logout", methods=["POST"])
def auth_logout():
    username = session.get("username")
    session.pop("user_id", None)
    session.pop("username", None)
    session.modified = True
    if username:
        log_event("user_logged_out", {"username": username})
    return jsonify({"status": "logged_out"})


@app.route("/auth/me", methods=["GET"])
def auth_me():
    user_id = session.get("user_id")
    username = session.get("username")
    return jsonify({"authenticated": bool(user_id), "user": {"id": user_id, "username": username} if user_id else None})


@app.route("/admin/login", methods=["GET", "POST"])
def admin_login():
    admin_configured = bool(ADMIN_PASSWORD_HASH or ADMIN_PASSWORD or LOCAL_DEV_ADMIN_PASSWORD)
    if request.method == "GET":
        config_msg = "" if admin_configured else "<p class='error'>Admin password is not configured on server. Set ADMIN_PASSWORD or ADMIN_PASSWORD_HASH.</p>"
        return """
        <style>
            body { font-family: sans-serif; padding: 32px; background: #f4f4f4; }
            .card { max-width: 420px; margin: 0 auto; background: white; padding: 24px; border-radius: 8px; }
            input, button { width: 100%; padding: 10px; margin-top: 10px; }
            .error { color: #b00020; margin-top: 8px; }
        </style>
        <div class='card'>
            <h2>Store Admin Login</h2>
            <form method='POST'>
                <label for='password'>Password</label>
                <input id='password' name='password' type='password' required />
                <button type='submit'>Login</button>
            </form>
            {config_msg}
        </div>
        """.replace("{config_msg}", config_msg)

    password = request.form.get("password", "")
    if validate_admin_password(password):
        session["admin_authenticated"] = True
        return redirect(url_for("admin_view"))

    return """
    <style>
        body { font-family: sans-serif; padding: 32px; background: #f4f4f4; }
        .card { max-width: 420px; margin: 0 auto; background: white; padding: 24px; border-radius: 8px; }
        input, button { width: 100%; padding: 10px; margin-top: 10px; }
        .error { color: #b00020; margin-top: 8px; }
    </style>
    <div class='card'>
        <h2>Store Admin Login</h2>
        <form method='POST'>
            <label for='password'>Password</label>
            <input id='password' name='password' type='password' required />
            <button type='submit'>Login</button>
        </form>
        <p class='error'>Invalid password.</p>
    </div>
    """, 401


@app.route("/admin/logout", methods=["POST"])
def admin_logout():
    session.pop("admin_authenticated", None)
    return redirect(url_for("admin_login"))


@app.route("/admin")
def admin_view():
    if not is_admin_authenticated():
        return redirect(url_for("admin_login"))

    orders = fetch_recent_orders(limit=200)
    rows = "".join(
        f"<tr><td>{escape(o['id'])}</td><td>{escape(o['customer'])}</td><td>{escape(o['time'])}</td><td>{escape(o['method'])}</td><td>{escape(o['address'])}</td><td>{escape(o['status'])}</td><td>Rs {o['subtotal']}</td><td>Rs {o['delivery_fee']}</td><td>Rs {o['total']}</td><td>{escape(o['details'])}</td></tr>"
        for o in orders
    )

    return f"""
    <style>
        body {{ font-family: sans-serif; padding: 20px; background: #f4f4f4; }}
        table {{ width: 100%; border-collapse: collapse; background: white; }}
        th, td {{ padding: 12px; border: 1px solid #ddd; text-align: left; vertical-align: top; }}
        th {{ background: #333; color: white; }}
        .actions {{ display: flex; gap: 8px; margin-bottom: 12px; }}
        .btn {{ padding: 8px 12px; border: 1px solid #333; background: white; cursor: pointer; }}
        .hint {{ margin-top: 12px; color: #444; }}
    </style>
    <h1>Store Admin - Recent Orders</h1>
    <div class='actions'>
        <a class='btn' href='/'>Back to Chat</a>
        <a class='btn' href='/admin/catalog'>Manage Catalog</a>
        <form action='/admin/logout' method='POST'>
            <button class='btn' type='submit'>Logout</button>
        </form>
    </div>
    <table>
        <tr><th>ID</th><th>Customer</th><th>Time</th><th>Method</th><th>Address</th><th>Status</th><th>Subtotal</th><th>Delivery Fee</th><th>Total</th><th>Details</th></tr>
        {rows or "<tr><td colspan='10'>No orders yet.</td></tr>"}
    </table>
    <div class='hint'>
        Catalog APIs: <code>/admin/api/catalog</code>, <code>/admin/api/categories</code>, <code>/admin/api/products</code>
    </div>
    """


@app.route("/admin/catalog")
def admin_catalog_page():
    if not is_admin_authenticated():
        return redirect(url_for("admin_login"))
    return render_template("admin_catalog.html")


@app.route("/admin/api/catalog", methods=["GET"])
def admin_catalog():
    guard = require_admin_json()
    if guard:
        return guard

    def map_category_row(row):
        try:
            return {"id": row["id"], "name": row["name"]}
        except Exception:
            return {"id": row[0], "name": row[1]}

    def map_product_row(row):
        try:
            return {
                "id": row["id"],
                "category_id": row["category_id"],
                "name": row["name"],
                "price_per_unit": row["price_per_unit"],
                "base_unit": row["base_unit"],
                "aliases": row["aliases"],
                "is_active": row["is_active"],
            }
        except Exception:
            return {
                "id": row[0],
                "category_id": row[1],
                "name": row[2],
                "price_per_unit": row[3],
                "base_unit": row[4],
                "aliases": row[5],
                "is_active": row[6],
            }

    with get_db_connection() as conn:
        categories = conn.execute("SELECT id, name FROM categories ORDER BY name").fetchall()
        products = conn.execute(
            """
            SELECT id, category_id, name, price_per_unit, base_unit, aliases, is_active
            FROM products
            ORDER BY name
            """
        ).fetchall()

    return jsonify(
        {
            "categories": [map_category_row(c) for c in categories],
            "products": [map_product_row(p) for p in products],
        }
    )


@app.route("/admin/api/categories", methods=["POST"])
def create_category():
    guard = require_admin_json()
    if guard:
        return guard

    name = (request.get_json(silent=True) or {}).get("name", "").lower().strip()
    if not name:
        return jsonify({"error": "Category name is required."}), 400

    try:
        with get_db_connection() as conn:
            cur = conn.execute("INSERT INTO categories(name) VALUES (?)", (name,))
            conn.commit()
        return jsonify({"id": cur.lastrowid, "name": name}), 201
    except Exception as exc:
        if is_unique_constraint_error(exc):
            return jsonify({"error": "Category already exists."}), 409
        return jsonify({"error": f"Could not create category: {str(exc)}"}), 500


@app.route("/admin/api/categories/<int:category_id>", methods=["PUT"])
def update_category(category_id):
    guard = require_admin_json()
    if guard:
        return guard

    name = (request.get_json(silent=True) or {}).get("name", "").lower().strip()
    if not name:
        return jsonify({"error": "Category name is required."}), 400

    try:
        with get_db_connection() as conn:
            exists = conn.execute("SELECT id FROM categories WHERE id = ?", (category_id,)).fetchone()
            if not exists:
                return jsonify({"error": "Category not found."}), 404
            try:
                conn.execute("UPDATE categories SET name = ? WHERE id = ?", (name, category_id))
                conn.commit()
            except Exception as exc:
                if is_unique_constraint_error(exc):
                    return jsonify({"error": "Category name already in use."}), 409
                return jsonify({"error": f"Could not update category: {str(exc)}"}), 500
    except Exception as exc:
        return jsonify({"error": f"Could not update category: {str(exc)}"}), 500

    return jsonify({"id": category_id, "name": name})


@app.route("/admin/api/products", methods=["POST"])
def create_product():
    guard = require_admin_json()
    if guard:
        return guard

    payload = request.get_json(silent=True) or {}
    name = payload.get("name", "").lower().strip()
    category_id = payload.get("category_id")
    aliases = payload.get("aliases", "").lower().strip()
    base_unit = canonical_unit(payload.get("base_unit", ""))

    try:
        price_per_unit = float(payload.get("price_per_unit", 0))
    except (TypeError, ValueError):
        return jsonify({"error": "price_per_unit must be numeric."}), 400

    if not name or not category_id or not base_unit:
        return jsonify({"error": "name, category_id, price_per_unit, base_unit are required."}), 400
    if base_unit not in VALID_UNITS:
        return jsonify({"error": f"Invalid base_unit. Use: {sorted(VALID_UNITS)}"}), 400
    if price_per_unit <= 0:
        return jsonify({"error": "price_per_unit must be greater than 0."}), 400

    try:
        with get_db_connection() as conn:
            exists = conn.execute("SELECT id FROM categories WHERE id = ?", (category_id,)).fetchone()
            if not exists:
                return jsonify({"error": "Category not found."}), 404
            cur = conn.execute(
                """
                INSERT INTO products(category_id, name, price_per_unit, base_unit, aliases, is_active)
                VALUES (?, ?, ?, ?, ?, 1)
                """,
                (category_id, name, price_per_unit, base_unit, aliases),
            )
            conn.commit()
    except Exception as exc:
        return jsonify({"error": f"Could not create product: {str(exc)}"}), 500
    return jsonify({"id": cur.lastrowid}), 201


@app.route("/admin/api/products/<int:product_id>", methods=["PUT"])
def update_product(product_id):
    guard = require_admin_json()
    if guard:
        return guard

    payload = request.get_json(silent=True) or {}
    name = payload.get("name", "").lower().strip()
    aliases = payload.get("aliases", "").lower().strip()
    category_id = payload.get("category_id")
    base_unit = canonical_unit(payload.get("base_unit", ""))

    try:
        price_per_unit = float(payload.get("price_per_unit", 0))
    except (TypeError, ValueError):
        return jsonify({"error": "price_per_unit must be numeric."}), 400

    if not name or not category_id or not base_unit:
        return jsonify({"error": "name, category_id, price_per_unit, base_unit are required."}), 400
    if base_unit not in VALID_UNITS:
        return jsonify({"error": f"Invalid base_unit. Use: {sorted(VALID_UNITS)}"}), 400
    if price_per_unit <= 0:
        return jsonify({"error": "price_per_unit must be greater than 0."}), 400

    with get_db_connection() as conn:
        exists = conn.execute("SELECT id FROM products WHERE id = ?", (product_id,)).fetchone()
        if not exists:
            return jsonify({"error": "Product not found."}), 404
        cat = conn.execute("SELECT id FROM categories WHERE id = ?", (category_id,)).fetchone()
        if not cat:
            return jsonify({"error": "Category not found."}), 404
        conn.execute(
            """
            UPDATE products
            SET category_id = ?, name = ?, price_per_unit = ?, base_unit = ?, aliases = ?, is_active = 1
            WHERE id = ?
            """,
            (category_id, name, price_per_unit, base_unit, aliases, product_id),
        )
        conn.commit()
    return jsonify({"id": product_id})


@app.route("/admin/api/products/<int:product_id>", methods=["DELETE"])
def delete_product(product_id):
    guard = require_admin_json()
    if guard:
        return guard

    with get_db_connection() as conn:
        exists = conn.execute("SELECT id FROM products WHERE id = ?", (product_id,)).fetchone()
        if not exists:
            return jsonify({"error": "Product not found."}), 404
        conn.execute("UPDATE products SET is_active = 0 WHERE id = ?", (product_id,))
        conn.commit()
    return jsonify({"status": "deactivated", "id": product_id})


@app.route("/admin/api/ml/events", methods=["GET"])
def admin_ml_events():
    guard = require_admin_json()
    if guard:
        return guard
    limit = request.args.get("limit", default=500, type=int)
    limit = max(1, min(limit, 5000))
    return jsonify({"events": fetch_event_logs(limit=limit)})


@app.route("/admin/api/ml/events.csv", methods=["GET"])
def admin_ml_events_csv():
    guard = require_admin_json()
    if guard:
        return guard
    limit = request.args.get("limit", default=1000, type=int)
    limit = max(1, min(limit, 10000))
    rows = fetch_event_logs(limit=limit)

    output = io.StringIO()
    writer = csv.DictWriter(
        output,
        fieldnames=["id", "created_at", "event_type", "session_user_id", "user_id", "order_id", "payload_json"],
    )
    writer.writeheader()
    for row in reversed(rows):
        writer.writerow(row)

    return Response(
        output.getvalue(),
        mimetype="text/csv",
        headers={"Content-Disposition": "attachment; filename=event_logs.csv"},
    )


@app.route("/admin/api/ml/orders", methods=["GET"])
def admin_ml_orders():
    guard = require_admin_json()
    if guard:
        return guard
    limit = request.args.get("limit", default=1000, type=int)
    limit = max(1, min(limit, 10000))
    return jsonify({"orders": fetch_order_training_rows(limit=limit)})


@app.route("/admin/api/ml/datasets/<dataset_name>", methods=["GET"])
def admin_ml_dataset(dataset_name):
    guard = require_admin_json()
    if guard:
        return guard

    limit = request.args.get("limit", default=2000, type=int)
    limit = max(1, min(limit, 20000))

    if dataset_name == "cancellation":
        return jsonify({"dataset": fetch_cancellation_dataset(limit=limit)})
    if dataset_name == "recommendations":
        return jsonify({"dataset": fetch_recommendation_dataset(limit=limit)})
    if dataset_name == "demand":
        days = request.args.get("days", default=60, type=int)
        days = max(1, min(days, 365))
        return jsonify({"dataset": fetch_demand_dataset(limit_days=days)})
    if dataset_name == "nlp":
        return jsonify({"dataset": fetch_nlp_dataset(limit=limit)})
    if dataset_name == "late_pickup":
        return jsonify({"dataset": fetch_late_pickup_dataset(limit=limit)})
    if dataset_name == "segmentation":
        return jsonify({"dataset": fetch_segmentation_dataset(limit=limit)})

    return jsonify(
        {
            "error": "Unknown dataset. Use one of: cancellation, recommendations, demand, nlp, late_pickup, segmentation"
        }
    ), 404


@app.route("/admin/api/cancellations", methods=["GET"])
def admin_cancellations():
    guard = require_admin_json()
    if guard:
        return guard
    limit = request.args.get("limit", default=200, type=int)
    limit = max(1, min(limit, 1000))
    return jsonify({"cancellations": fetch_cancellation_summary(limit=limit)})


@app.route("/notifications", methods=["GET"])
def customer_notifications():
    guard = require_user_login()
    if guard:
        return guard
    soon = process_pickup_soon_reminders(send_push=True)
    reminders = process_late_pickup_reminders(send_push=True)
    queued = pop_notifications()

    known_orders = set(session.get("order_history", []))
    last_order = session.get("last_order_id")
    if last_order:
        known_orders.add(last_order)

    dynamic = []
    for item in (soon + reminders):
        if item.get("order_id") in known_orders:
            dynamic.append({"title": item["title"], "message": item["message"], "order_id": item.get("order_id")})

    return jsonify({"notifications": queued + dynamic})


@app.route("/get", methods=["POST"])
def chat():
    guard = require_user_login()
    if guard:
        return guard

    req_json = request.get_json(silent=True) or {}
    raw_message = req_json.get("message", "")
    msg = normalize_text(raw_message)
    log_event("chat_message", {"raw_message": raw_message, "normalized_message": msg})
    process_pickup_soon_reminders(send_push=True)
    process_late_pickup_reminders(send_push=True)

    state = get_user_state()
    orders = state["orders"]
    total = state["total"]

    requested_language = detect_language_command(msg)
    if requested_language:
        state["language"] = requested_language
        save_user_state(state)
        labels = {"english": "English", "hindi": "Hindi", "hinglish": "Hinglish"}
        log_event("language_changed", {"language": requested_language})
        return jsonify({"reply": reply_text(state, f"Language changed to {labels[requested_language]}.", f"Bhasha {labels[requested_language]} me badal di gayi hai.", f"Language ab {labels[requested_language]} mode me set hai.")})

    if re.match(r"^\s*add anyway\b", msg):
        pending = state.get("pending_add")
        if not pending:
            return jsonify({"reply": "There is nothing pending to add. Please tell me the item, like 'add milk 2'."})
        product = find_best_product_match(str(pending.get("item", "")), include_inactive=False)
        if not product:
            state["pending_add"] = None
            save_user_state(state)
            return jsonify({"reply": "The previously requested item is not available now. Please try another item."})
        qty = float(pending.get("qty", 1.0))
        user_unit = pending.get("unit")
        qty_in_base, err = convert_quantity_to_base(qty, user_unit, product["base_unit"])
        if err:
            state["pending_add"] = None
            save_user_state(state)
            return jsonify({"reply": err})
        line_total = int(round(float(product["price_per_unit"]) * qty_in_base))
        orders.append(
            {
                "item_id": product["id"],
                "item": product["name"],
                "qty": qty_in_base,
                "unit": product["base_unit"],
                "price_per_unit": float(product["price_per_unit"]),
                "line_total": line_total,
            }
        )
        total += line_total
        state["orders"] = orders
        state["total"] = total
        state["last_item"] = product["name"]
        state["pending_add"] = None
        mem = state.get("item_memory", {})
        mem[product["name"]] = int(mem.get(product["name"], 0) + 1)
        state["item_memory"] = mem
        save_user_state(state)
        log_event(
            "cart_item_added_override",
            {
                "item": product["name"],
                "qty_base": qty_in_base,
                "unit": product["base_unit"],
                "line_total": line_total,
                "cart_total": total,
                "reason": "user_override_add_anyway",
            },
        )
        return jsonify({"reply": f"Added {product['name'].title()} {format_qty(qty_in_base)} {product['base_unit']}. Total: Rs {total}"})

    add_recipe_match = re.match(r"^\s*add recipe\s+(.+?)\s*$", msg)
    if add_recipe_match:
        recipe_name = add_recipe_match.group(1).strip()
        ok, info, added_items = add_recipe_to_cart(recipe_name, state)
        if not ok:
            return jsonify({"reply": info})
        log_event("recipe_added_to_cart", {"recipe": recipe_name, "items": added_items, "cart_total": state.get("total", 0)})
        return jsonify({"reply": f"{info}\nTotal: Rs {state.get('total', 0)}"})

    if msg.startswith("create family"):
        family_name = msg.replace("create family", "", 1).strip() or f"{get_logged_in_username()}'s Family"
        group, err = create_family_group(get_logged_in_user_id(), family_name)
        if err:
            return jsonify({"reply": err})
        return jsonify({"reply": f"Family created: {group['name']}\nInvite code: {group['invite_code']}\nShare this code so others can join."})

    if msg.startswith("join family"):
        invite = msg.replace("join family", "", 1).strip()
        group, err = join_family_group(get_logged_in_user_id(), invite)
        if err:
            return jsonify({"reply": err})
        return jsonify({"reply": f"You joined family '{group['name']}'. Invite code: {group['invite_code']}"})

    if msg in {"family code", "family invite", "my family"}:
        group = get_family_group_for_user(get_logged_in_user_id())
        if not group:
            return jsonify({"reply": "You are not in a family group. Use 'create family <name>' or 'join family <code>'."})
        return jsonify({"reply": f"Family: {group['name']}\nInvite code: {group['invite_code']}"})

    if msg.startswith("family add"):
        payload = msg.replace("family add", "", 1).strip()
        item, qty, unit = parse_item_request(payload)
        ok, info = add_family_list_item(get_logged_in_user_id(), item=item, qty=qty, unit=unit)
        if ok:
            return jsonify({"reply": info + "\nTip: Family list is shared. To buy now, say 'add family list' or add specific cart items."})
        return jsonify({"reply": info})

    if msg.startswith("family remove"):
        payload = msg.replace("family remove", "", 1).strip()
        item, qty, _ = parse_item_request(payload)
        ok, info = remove_family_list_item(get_logged_in_user_id(), item=item, qty=qty)
        return jsonify({"reply": info})

    if msg in {"family list", "shared list"}:
        group, rows = fetch_family_list(get_logged_in_user_id())
        if not group:
            return jsonify({"reply": "You are not in a family group yet."})
        if not rows:
            return jsonify({"reply": f"Family list for {group['name']} is empty."})
        lines = []
        for r in rows:
            item_text = f"- {r['item'].title()} {format_qty(r['qty'])} {r['unit'] or ''}".replace("  ", " ").strip()
            added_by = r.get("added_by") or "member"
            updated_by = r.get("last_updated_by") or added_by
            if updated_by != added_by:
                item_text += f" (added by {added_by}, updated by {updated_by})"
            else:
                item_text += f" (by {added_by})"
            lines.append(item_text)
        return jsonify({"reply": f"Shared Family List ({group['name']}):\n" + "\n".join(lines)})

    if msg in {"family orders", "family history", "shared history"}:
        group, rows = fetch_family_order_timeline(get_logged_in_user_id(), limit=30)
        if not group:
            return jsonify({"reply": "You are not in a family group yet."})
        if not rows:
            return jsonify({"reply": f"No placed family orders found for {group['name']} yet."})
        lines = []
        for r in rows:
            try:
                dt = datetime.datetime.fromisoformat(r["created_at"])
                day_label = dt.strftime("%a %d %b, %I:%M %p")
            except Exception:
                day_label = str(r["created_at"])
            lines.append(
                f"- {day_label} | {r.get('username') or 'member'} ordered {r['item'].title()} {format_qty(r['qty'])} {r['unit'] or ''}".replace("  ", " ").strip()
            )
        return jsonify({"reply": f"Family Purchase Timeline ({group['name']}):\n" + "\n".join(lines)})

    if msg in {"family stock score", "family stock scores", "family stock snapshot"}:
        group, snapshot = fetch_family_stock_snapshot(get_logged_in_user_id(), max_items=10)
        if not group:
            return jsonify({"reply": "You are not in a family group yet."})
        if not snapshot:
            _, list_rows = fetch_family_list(get_logged_in_user_id())
            if list_rows:
                names = ", ".join(r["item"].title() for r in list_rows[:8])
                return jsonify({"reply": f"No placed family orders yet for score prediction. Current shared list items: {names}. Place orders to unlock stock scores."})
            return jsonify({"reply": "Not enough family purchase history yet for stock scoring."})
        lines = []
        for s in snapshot:
            lines.append(
                f"- {s['item'].title()}: {s['score']}% ({s['label']}) | last buy {format_since_days(s['days_since_last'])} | est cover {s['estimated_coverage_days']} days"
            )
        return jsonify({"reply": f"Family Stock Scoreboard ({group['name']}):\n" + "\n".join(lines)})

    if msg in {"add family list", "add shared list"}:
        ok, info = add_family_list_to_cart(state, get_logged_in_user_id())
        return jsonify({"reply": info})

    family_stock_match = re.match(r"^\s*family\s+(?:stock|check)\s+(.+?)\s*$", msg)
    if family_stock_match:
        query_item = family_stock_match.group(1).strip()
        group, recent = fetch_recent_family_item_activity(get_logged_in_user_id(), query_item, lookback_days=14, limit=5)
        if not group:
            return jsonify({"reply": "You are not in a family group yet."})
        if not recent:
            return jsonify({"reply": f"No recent family purchase found for {query_item.title()} in last 14 days."})
        _, estimate = estimate_family_item_stock(get_logged_in_user_id(), query_item, lookback_days=60)
        lines = []
        for r in recent:
            try:
                dt = datetime.datetime.fromisoformat(r["created_at"])
                day_label = dt.strftime("%a %d %b")
            except Exception:
                day_label = str(r["created_at"])
            lines.append(f"- {day_label}: {r.get('username') or 'member'} bought {r['item'].title()} {format_qty(r['qty'])} {r['unit'] or ''}".replace("  ", " ").strip())
        est_text = ""
        if estimate:
            est_text = (
                f"\n\nEstimated stock left: {estimate['score']}% ({estimate['label']})"
                f"\nLast buy: {format_since_days(estimate['days_since_last'])}"
                f"\nBased on recent usage over ~{estimate['samples']} purchase records."
            )
        return jsonify({"reply": f"Recent family purchases for {query_item.title()}:\n" + "\n".join(lines) + est_text})

    if any(kw in msg for kw in ["monthly insight", "monthly insights", "insights", "this month spend"]):
        insight = fetch_monthly_insights(get_logged_in_user_id())
        if not insight:
            return jsonify({"reply": "Please login to view insights."})
        if insight["order_count"] == 0:
            return jsonify({"reply": f"No placed orders in {insight['month_label']} yet. Place an order to start insights."})
        return jsonify(
            {
                "reply": (
                    f"Monthly Insights ({insight['month_label']}):\n"
                    f"- Total Spend: Rs {int(round(insight['total_spend']))}\n"
                    f"- Orders: {insight['order_count']}\n"
                    f"- Snacks Spend: Rs {int(round(insight['snacks_spend']))}\n"
                    f"- Top Category: {(insight['top_category'] or 'N/A').title()}"
                )
            }
        )

    contextual_reply = handle_lifestyle_context(raw_message, state)
    if contextual_reply:
        if isinstance(contextual_reply, dict):
            return jsonify(contextual_reply)
        return jsonify({"reply": contextual_reply})

    if any(phrase in msg for phrase in ["my orders", "past orders", "order history", "show orders"]):
        user_id = get_logged_in_user_id()
        if user_id:
            orders_view = fetch_orders_for_user_history(user_id, limit=10)
        else:
            history_ids = session.get("order_history", [])
            orders_view = fetch_orders_for_history(history_ids, limit=10)
        if not orders_view:
            return jsonify({"reply": "No past orders found yet."})
        lines = "\n".join(
            [
                f"- {o['id']} | {o['created_at']} | {o['method']} | Rs {o['total']} | {o['status'] or 'placed'}"
                for o in orders_view
            ]
        )
        return jsonify({"reply": f"Your past orders:\n{lines}"})

    if is_not_coming_message(msg):
        cancel_order_id = resolve_cancellable_order_id()
        if cancel_order_id:
            ok, info = cancel_order_for_session(cancel_order_id)
            if ok:
                session["last_order_id"] = cancel_order_id
                session.modified = True
                log_event("order_cancelled_from_reminder", {"message": raw_message}, order_id=cancel_order_id)
                queue_notification("Order Cancelled", info, order_id=cancel_order_id)
                return jsonify({"reply": f"Understood. {info}"})
            return jsonify({"reply": info})
        return jsonify({"reply": "Understood. If this is about your pickup, type 'cancel order'."})

    if any(phrase in msg for phrase in ["cancel order", "cancel pickup", "cancel my order"]):
        cancel_order_id = resolve_cancellable_order_id()
        if not cancel_order_id:
            log_event("cancel_order_failed", {"reason": "missing_recent_order"})
            return jsonify({"reply": "I could not find an active order to cancel."})
        ok, info = cancel_order_for_session(cancel_order_id)
        if not ok:
            log_event("cancel_order_failed", {"reason": info}, order_id=cancel_order_id)
            return jsonify({"reply": info})
        session["last_order_id"] = cancel_order_id
        session.modified = True
        log_event("order_cancelled", {"message": info}, order_id=cancel_order_id)
        queue_notification("Order Cancelled", info, order_id=cancel_order_id)
        return jsonify({"reply": info})

    if state["waiting_for_pickup_time"]:
        pickup_dt, err = parse_pickup_time(raw_message)
        if err:
            return jsonify({"reply": err + " Example: 18:30"})
        order_id = save_order_with_retry(
            method="Pickup",
            address="",
            subtotal=total,
            delivery_fee=0,
            total=total,
            items=orders,
            pickup_time=pickup_dt.isoformat(timespec="minutes"),
        )
        session["last_order_id"] = order_id
        add_order_to_history(order_id)
        log_event(
            "order_placed",
            {"method": "Pickup", "total": total, "pickup_time": pickup_dt.isoformat(timespec="minutes")},
            order_id=order_id,
        )
        queue_notification(
            "Order Placed",
            f"Order {order_id} confirmed for pickup at {pickup_dt.strftime('%H:%M')}.",
            order_id=order_id,
        )
        clear_user_state()
        return jsonify({"reply": reply_text(state, f"Pickup scheduled.\nOrder ID: {order_id}\nPickup Time: {pickup_dt.strftime('%H:%M')}\nTotal: Rs {total}", f"Pickup schedule ho gaya.\nOrder ID: {order_id}\nPickup Time: {pickup_dt.strftime('%H:%M')}\nKul: Rs {total}", f"Pickup schedule ho gaya.\nOrder ID: {order_id}\nPickup Time: {pickup_dt.strftime('%H:%M')}\nTotal: Rs {total}")})

    if state["waiting_for_method"]:
        if "pickup" in msg:
            state["waiting_for_method"] = False
            state["waiting_for_pickup_time"] = True
            save_user_state(state)
            return jsonify({"reply": reply_text(state, "Please enter your pickup time (HH:MM or HH:MM AM/PM).", "Pickup time likhiye (HH:MM ya HH:MM AM/PM).", "Pickup time likho (HH:MM ya HH:MM AM/PM).")})
        if "delivery" in msg:
            if total < MIN_ORDER_FOR_DELIVERY:
                return jsonify({"reply": f"Delivery requires minimum Rs {MIN_ORDER_FOR_DELIVERY}. Current total: Rs {total}."})
            state["waiting_for_method"] = False
            state["waiting_for_address"] = True
            save_user_state(state)
            return jsonify({"reply": "Delivery selected. Please type your full delivery address."})

    if state["waiting_for_address"]:
        address = raw_message.strip()
        if not address:
            return jsonify({"reply": "Please provide a valid delivery address."})
        fee = 0 if total >= MIN_FREE_DELIVERY else DELIVERY_FEE
        final_amt = total + fee
        order_id = save_order_with_retry(
            method="Delivery",
            address=address,
            subtotal=total,
            delivery_fee=fee,
            total=final_amt,
            items=orders,
        )
        session["last_order_id"] = order_id
        add_order_to_history(order_id)
        log_event(
            "order_placed",
            {"method": "Delivery", "subtotal": total, "delivery_fee": fee, "total": final_amt},
            order_id=order_id,
        )
        queue_notification(
            "Order Placed",
            f"Order {order_id} confirmed for delivery. Total Rs {final_amt}.",
            order_id=order_id,
        )
        clear_user_state()
        return jsonify({"reply": f"Order placed.\nOrder ID: {order_id}\nSubtotal: Rs {total}\nDelivery Fee: Rs {fee}\nTotal: Rs {final_amt}"})

    if any(token in msg for token in ["budget", "under", "within", "affordable", "cheap", "optimize"]):
        budget_value, preferred_category = parse_budget_request(raw_message)
        if not budget_value or budget_value <= 0:
            return jsonify(
                {
                    "reply": "Please share a budget amount. Example: 'budget 300 fruits' or 'optimize under 500'."
                }
            )
        if not preferred_category and state.get("budget_mode") == "tight":
            preferred_category = "staples"
        result, status = optimize_budget_plan(budget_value, preferred_category=preferred_category)
        if status != 200:
            return jsonify({"reply": result.get("error", "Could not generate budget plan right now.")}), status
        if not result.get("items"):
            return jsonify({"reply": f"I could not build a plan under Rs {int(budget_value)}. Try increasing budget."})
        lines = "\n".join(
            [
                f"- {it['name'].title()} x{it['qty']} = Rs {int(round(it['line_total']))}"
                for it in result["items"][:10]
            ]
        )
        category_part = f" ({preferred_category.title()})" if preferred_category else ""
        return jsonify(
            {
                "reply": f"Budget Plan{category_part} for Rs {int(round(result['budget']))}:\n{lines}\n\nTotal: Rs {int(round(result['total']))}\nRemaining: Rs {int(round(result['remaining']))}"
            }
        )

    if any(token in msg for token in ["help", "madad", "kya hai", "kya kar", "what can you do"]):
        return jsonify(
            {
                "reply": reply_text(
                    state,
                    "Guide: 'Categories', 'Apple 2', 'Bill', 'Confirm', 'budget 300 fruits'. Recipe: 'I am making pasta tonight' then tap Add All. Family: 'create family Home', 'join family FMXXXXXX', 'family add milk 2', 'family list', 'add family list', 'family orders', 'family check milk', 'family stock score'. Duplicate prevention: if recently bought, bot asks before adding unless you say 'add anyway'. Insights: 'monthly insights'. Pickup time formats: 18:30, 6:30 PM, tomorrow 10:30 AM.",
                    "Guide: 'Categories', 'Apple 2', 'Bill', 'Confirm', 'budget 300 fruits'. Recipe: 'I am making pasta tonight' then tap Add All. Family: 'create family Home', 'join family FMXXXXXX', 'family add milk 2', 'family list', 'add family list', 'family orders', 'family check milk', 'family stock score'. Duplicate prevention: if recently bought, bot asks before adding unless you say 'add anyway'. Insights: 'monthly insights'. Pickup time formats: 18:30, 6:30 PM, tomorrow 10:30 AM.",
                    "Guide: 'Categories', 'Apple 2', 'Bill', 'Confirm', 'budget 300 fruits'. Recipe: 'I am making pasta tonight' then tap Add All. Family: 'create family Home', 'join family FMXXXXXX', 'family add milk 2', 'family list', 'add family list', 'family orders', 'family check milk', 'family stock score'. Duplicate prevention: if recently bought, bot asks before adding unless you say 'add anyway'. Insights: 'monthly insights'. Pickup time formats: 18:30, 6:30 PM, tomorrow 10:30 AM.",
                )
            }
        )

    if "categories" in msg:
        categories = list_categories()
        names = ", ".join(c["name"].title() for c in categories)
        return jsonify({"reply": f"Available categories: {names}"})

    if "remove" in msg or "undo" in msg:
        if not orders:
            return jsonify({"reply": "Cart is already empty."})
        remove_request = parse_remove_request(msg)
        if remove_request["mode"] == "last":
            removed = orders.pop()
            total -= removed["line_total"]
            state["orders"] = orders
            state["total"] = total
            state["last_item"] = removed.get("item")
            save_user_state(state)
            return jsonify({"reply": f"Removed {removed['item'].title()} {format_qty(removed['qty'])} {removed['unit']}. New total: Rs {total}"})
        remove_item_query = remove_request["item_query"]
        product = find_best_product_match(remove_item_query, include_inactive=True)
        if not product and is_context_followup_message(msg) and state.get("last_item"):
            remove_item_query = state["last_item"]
            product = find_best_product_match(remove_item_query, include_inactive=True)
        if not product:
            return jsonify({"reply": f"Could not find {remove_item_query} in catalog."})
        qty_in_base, err = convert_quantity_to_base(remove_request["qty"], remove_request["unit"], product["base_unit"])
        if err:
            return jsonify({"reply": err})
        removed_qty, removed_amount = remove_item_from_cart(orders, product, qty_in_base)
        if removed_qty <= 0:
            return jsonify({"reply": f"{product['name'].title()} is not in your cart."})
        total -= removed_amount
        state["orders"] = orders
        state["total"] = total
        state["last_item"] = product["name"]
        save_user_state(state)
        return jsonify({"reply": f"Removed {product['name'].title()} {format_qty(removed_qty)} {product['base_unit']}. New total: Rs {total}"})

    if any(word in msg for word in ["bill", "total", "cart"]):
        if not orders:
            return jsonify({"reply": "Cart is empty."})
        items = "\n".join([f"- {i['item'].title()} {format_qty(i['qty'])} {i['unit']} = Rs {i['line_total']}" for i in orders])
        return jsonify({"reply": f"Cart:\n{items}\n\nSubtotal: Rs {total}\nType 'Confirm' to checkout."})

    if "confirm" in msg or "checkout" in msg:
        if not orders:
            group, list_rows = fetch_family_list(get_logged_in_user_id())
            if group and list_rows:
                return jsonify({"reply": "Cart is empty. You have items in family list. Say 'add family list' first, then 'confirm'."})
            return jsonify({"reply": "Add items first."})
        state["waiting_for_method"] = True
        save_user_state(state)
        return jsonify({"reply": "Select fulfillment method:\n- Pickup (Free)\n- Delivery"})

    categories = list_categories()
    for c in categories:
        if c["name"] in msg:
            menu = products_for_category(c["name"])
            if not menu:
                return jsonify({"reply": f"{c['name'].title()} has no active products yet."})
            lines = "\n".join([f"- {p['name'].title()}: Rs {int(p['price_per_unit'])} / {p['base_unit']}" for p in menu])
            return jsonify({"reply": f"{c['name'].title()} Menu:\n{lines}"})

    item_query, qty, user_unit = parse_item_request(msg)
    product = find_best_product_match(item_query, include_inactive=False)
    if not product and is_context_followup_message(msg) and state.get("last_item"):
        item_query = state["last_item"]
        product = find_best_product_match(item_query, include_inactive=False)
    if product:
        if qty <= 0:
            return jsonify({"reply": "Quantity must be greater than 0."})
        if "add anyway" not in msg and "need more" not in msg and "more stock" not in msg:
            dup_hint = build_family_duplicate_hint(get_logged_in_user_id(), product["name"], lookback_days=7)
            if dup_hint:
                state["pending_add"] = {
                    "item": product["name"],
                    "qty": float(qty),
                    "unit": user_unit,
                }
                save_user_state(state)
                return jsonify({"reply": dup_hint})
        dietary_preference = state.get("dietary_preference")
        blocked = DIETARY_BLOCKLIST.get(dietary_preference, set()) if dietary_preference else set()
        if product["name"] in blocked and "add anyway" not in msg:
            state["pending_add"] = {
                "item": product["name"],
                "qty": float(qty),
                "unit": user_unit,
            }
            save_user_state(state)
            return jsonify(
                {
                    "reply": f"{product['name'].title()} may not fit your {dietary_preference} preference. Try another option or say 'add anyway'."
                }
            )
        qty_in_base, err = convert_quantity_to_base(qty, user_unit, product["base_unit"])
        if err:
            return jsonify({"reply": err})
        line_total = int(round(float(product["price_per_unit"]) * qty_in_base))
        orders.append({"item_id": product["id"], "item": product["name"], "qty": qty_in_base, "unit": product["base_unit"], "price_per_unit": float(product["price_per_unit"]), "line_total": line_total})
        total += line_total
        state["orders"] = orders
        state["total"] = total
        state["last_item"] = product["name"]
        state["pending_add"] = None
        mem = state.get("item_memory", {})
        mem[product["name"]] = int(mem.get(product["name"], 0) + 1)
        state["item_memory"] = mem
        save_user_state(state)
        log_event(
            "cart_item_added",
            {"item": product["name"], "qty_base": qty_in_base, "unit": product["base_unit"], "line_total": line_total, "cart_total": total},
        )
        return jsonify({"reply": f"Added {product['name'].title()} {format_qty(qty_in_base)} {product['base_unit']}. Total: Rs {total}"})

    if looks_like_product_request(msg, qty, user_unit):
        inactive_product = find_best_product_match(item_query, include_inactive=True)
        if inactive_product and int(inactive_product.get("is_active", 1)) == 0:
            alternatives = get_alternative_products(inactive_product, limit=3)
            alt_text = ", ".join([a.title() for a in alternatives]) if alternatives else "No alternatives available"
            return jsonify({"reply": f"{inactive_product['name'].title()} is currently not available. Suggested alternatives: {alt_text}."})
        return jsonify({"reply": f"Sorry, {item_query.title()} is not available right now."})

    if any(word in msg for word in ["location", "where", "address"]):
        return jsonify(
            {
                "reply": reply_text(
                    state,
                    f"Store Address:\n{STORE_LOCATION}",
                    f"Store ka pata:\n{STORE_LOCATION}",
                    f"Store address:\n{STORE_LOCATION}",
                )
            }
        )

    if get_language(state) == "english":
        return jsonify({"reply": chatbot_response(msg)})
    return jsonify(
        {
            "reply": reply_text(
                state,
                "I can help with categories, adding items, bill, checkout, order cancel, and order history. Type 'help'.",
                "Main categories, item add karna, bill, checkout, order cancel aur order history me madad kar sakta hoon. 'help' type kijiye.",
                "Main categories, item add, bill, checkout, cancel order aur order history me help kar sakta hoon. 'help' type karo.",
            )
        }
    )


@app.route("/chat", methods=["POST"])
def chat_alias():
    return chat()


init_db()
if os.environ.get("WERKZEUG_RUN_MAIN") == "true" or not app.debug:
    start_background_reminder_worker()

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port)
