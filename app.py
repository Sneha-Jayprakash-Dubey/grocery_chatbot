from flask import Flask, render_template, request, jsonify
from model import chatbot_response
from difflib import get_close_matches
import datetime
import random
import os
ADMIN_PASSWORD = os.getenv("ADMIN_PASSWORD", "default_fallback")
app = Flask(__name__)

# ==========================================
# STORE OWNER CONFIGURATION
# ==========================================
store_data = {
    "fruits": {"apple": 120, "banana": 60, "orange": 80, "mango": 150},
    "vegetables": {"potato": 40, "tomato": 50, "carrot": 60, "onion": 30},
    "snacks": {"biscuits": 30, "chips": 20, "chocolate": 50},
    "dairy": {"milk": 60, "cheese": 120, "butter": 100}
}

STORE_LOCATION = "123 Green Valley Road, Fresh Market Square"
DELIVERY_FEE, MIN_FREE_DELIVERY, MIN_ORDER_FOR_DELIVERY = 30, 500, 200
products = {item: price for cat in store_data.values() for item, price in cat.items()}

# Global State (Note: Resets if server restarts)
orders, total = [], 0
all_orders = [] # <--- Database for the Store Owner
order_state = {"waiting_for_method": False, "waiting_for_address": False}

@app.route("/")
def home(): return render_template("index.html")

# --- ADD TO CONFIGURATION ---
ADMIN_PASSWORD = "adminbot"

# --- ADMIN ROUTE ---
@app.route("/admin")
def admin_view():
    # Check for password in the URL: /admin?pw=store_admin_secret
    pwd = request.args.get('pw')
    if pwd != ADMIN_PASSWORD:
        return "<h1>Unauthorized Access</h1>", 401

    html = """
    <style>
        body { font-family: sans-serif; padding: 20px; background: #f4f4f4; }
        table { width: 100%; border-collapse: collapse; background: white; }
        th, td { padding: 12px; border: 1px solid #ddd; text-align: left; }
        th { background: #333; color: white; }
    </style>
    <h1>🏪 Store Admin - Recent Orders</h1>
    <table>
        <tr><th>ID</th><th>Time</th><th>Method</th><th>Total</th><th>Details</th></tr>
    """
    for o in reversed(all_orders):
        html += f"<tr><td>{o['id']}</td><td>{o['time']}</td><td>{o['method']}</td><td>₹{o['total']}</td><td>{o['details']}</td></tr>"
    return html + "</table><br><a href='/'>Go back to Chat</a>"

@app.route("/get", methods=["POST"])
def chat():
    global total, orders
    msg = request.json["message"].lower().strip()
    order_id = f"GRC-{random.randint(1000, 9999)}"
    timestamp = datetime.datetime.now().strftime("%I:%M %p")

    # --- 1. STATE: PICKUP vs DELIVERY CHOICE ---
    if order_state["waiting_for_method"]:
        if "pickup" in msg:
            order_state["waiting_for_method"] = False
            # Save to Admin Database
            all_orders.append({"id": order_id, "time": timestamp, "method": "Pickup", "total": total, "details": str(orders)})
            res = (f"🥡 **Pickup Confirmed!**\nOrder ID: **{order_id}**\nTotal: ₹{total}\n\n📍 **Location:** {STORE_LOCATION}")
            orders, total = [], 0
            return jsonify({"reply": res})
        
        elif "delivery" in msg:
            if total < MIN_ORDER_FOR_DELIVERY:
                return jsonify({"reply": f"⚠️ Delivery requires min ₹{MIN_ORDER_FOR_DELIVERY}. Total: ₹{total}. Add more or choose **'Pickup'**."})
            order_state["waiting_for_method"], order_state["waiting_for_address"] = False, True
            return jsonify({"reply": "🚚 **Delivery Selected.**\nPlease type your **Full Delivery Address**:"})

    # --- 2. STATE: ADDRESS COLLECTION ---
    if order_state["waiting_for_address"]:
        order_state["waiting_for_address"] = False
        address = request.json["message"]
        fee = 0 if total >= MIN_FREE_DELIVERY else DELIVERY_FEE
        final_amt = total + fee
        # Save to Admin Database
        all_orders.append({"id": order_id, "time": timestamp, "method": f"Delivery to: {address}", "total": final_amt, "details": str(orders)})
        res = (f"✅ **Order Placed!**\nOrder ID: **{order_id}**\nTotal: ₹{final_amt}\n📍 **Deliver to:** {address}")
        orders, total = [], 0
        return jsonify({"reply": res})

    # --- 3. COMMANDS: HELP, BILL, REMOVE ---
    if "help" in msg:
        return jsonify({"reply": "📖 **Guide:** 'Fruits' (Browse), 'Apple 2' (Add), 'Bill' (Cart), 'Confirm' (Pay)"})

    if "remove" in msg or "undo" in msg:
        if orders:
            item, qty, price = orders.pop(); total -= price
            return jsonify({"reply": f"🔄 Removed **{item.title()}**. New Total: ₹{total}"})

    if any(word in msg for word in ["bill", "total", "cart"]):
        if not orders: return jsonify({"reply": "Cart empty!"})
        items = "\n".join([f"• {i.title()} x{q} = ₹{p}" for i,q,p in orders])
        return jsonify({"reply": f"🧾 **Cart:**\n{items}\n\n**Subtotal: ₹{total}**\nType 'Confirm' to pay."})

    if "confirm" in msg or "checkout" in msg:
        if not orders: return jsonify({"reply": "Add items first!"})
        order_state["waiting_for_method"] = True
        return jsonify({"reply": f"🛒 **Select Method:**\n• **Pickup** (Free)\n• **Delivery** (Min ₹{MIN_ORDER_FOR_DELIVERY})"})

    # --- 4. CATEGORIES & ORDERING ---
    for category in store_data.keys():
        if category in msg:
            items = "\n".join([f"• {k.title()}: ₹{v}" for k,v in store_data[category].items()])
            return jsonify({"reply": f"🛒 **{category.title()} Menu:**\n{items}"})

    words = msg.split()
    if len(words) >= 2 and words[-1].isdigit():
        item_query = " ".join(words[:-1]).replace("add ", "").strip()
        match = get_close_matches(item_query, products.keys(), n=1, cutoff=0.6)
        if match:
            item = match[0]; qty = int(words[-1]); price = products[item] * qty
            total += price; orders.append((item, qty, price))
            return jsonify({"reply": f"✅ Added **{item.title()} x{qty}**. Total: ₹{total}"})

    return jsonify({"reply": chatbot_response(msg)})

if __name__ == "__main__":
    # Get port from environment variable or default to 5000
    port = int(os.environ.get("PORT", 5000))
    app.run(host='0.0.0.0', port=port)