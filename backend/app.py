from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from pathlib import Path
import json
import requests
import os

app = FastAPI(title="Spark Ecommerce Analytics API")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

BASE_DIR = Path(__file__).resolve().parent.parent
OUTPUT_DIR = BASE_DIR / "output"

OLLAMA_URL = os.getenv("OLLAMA_URL", "http://localhost:11434/api/generate")
OLLAMA_MODEL = os.getenv("OLLAMA_MODEL", "qwen3:latest")


def read_json_file(filename: str):
    file_path = OUTPUT_DIR / filename
    if not file_path.exists():
        return {"error": f"{filename} not found"}
    with open(file_path, "r", encoding="utf-8") as f:
        return json.load(f)


class ChatRequest(BaseModel):
    message: str


@app.get("/")
def home():
    return {"message": "Spark Ecommerce Backend Running"}


@app.get("/api/summary")
def get_summary():
    return read_json_file("summary.json")


@app.get("/api/funnel")
def get_funnel():
    return read_json_file("funnel.json")


@app.get("/api/anomalies")
def get_anomalies():
    return read_json_file("anomalies.json")


@app.post("/api/chat")
def chat_with_ollama(payload: ChatRequest):
    message = payload.message.strip().lower()
    summary_data = read_json_file("summary.json")

    total_views = summary_data.get("total_views", "N/A")
    total_carts = summary_data.get("total_carts", "N/A")
    total_purchases = summary_data.get("total_purchases", "N/A")
    view_to_cart_rate = summary_data.get("view_to_cart_rate", "N/A")
    cart_to_purchase_rate = summary_data.get("cart_to_purchase_rate", "N/A")
    overall_conversion_rate = summary_data.get("overall_conversion_rate", "N/A")

    # Greetings
    if message in ["hi", "hello", "hey"]:
        return {
            "reply": "Hello. I’m your AI analytics guide. Ask me about views, carts, purchases, conversion, or anomalies."
        }

    # Views
    if "total views" in message:
        return {
            "reply": f"Total views are {total_views}. This shows how many times products or pages were viewed in the dashboard."
        }

    if "view means" in message or "what does view mean" in message or "what are views" in message:
        return {
            "reply": "Views represent how many times users looked at products or pages. They help measure user interest and traffic."
        }

    # Carts
    if "total carts" in message:
        return {
            "reply": f"Total carts are {total_carts}. This shows how many times users added items to their cart."
        }

    if "cart means" in message or "what does cart mean" in message or "what are carts" in message:
        return {
            "reply": "Carts show how many times users added products to cart. This helps measure purchase intent."
        }

    # Purchases
    if "total purchases" in message:
        return {
            "reply": f"Total purchases are {total_purchases}. This represents completed purchases in the analyzed data."
        }

    if "purchase means" in message or "what does purchase mean" in message or "what are purchases" in message:
        return {
            "reply": "Purchases represent completed transactions. They show how many users finished buying items."
        }

    # View to cart
    if "view to cart" in message:
        return {
            "reply": f"The view-to-cart rate is {view_to_cart_rate}. It shows how often views turned into cart additions."
        }

    # Cart to purchase
    if "cart to purchase" in message or "purchase rate" in message:
        return {
            "reply": f"The cart-to-purchase rate is {cart_to_purchase_rate}. It shows how often cart activity turned into completed purchases."
        }

    # Overall conversion
    if "overall conversion" in message or "conversion rate" in message:
        return {
            "reply": f"The overall conversion rate is {overall_conversion_rate}. It reflects how efficiently user activity turned into purchases."
        }

    if "conversion means" in message or "what does conversion mean" in message:
        return {
            "reply": "Conversion means turning user activity into a desired outcome, such as a purchase. It helps measure dashboard performance."
        }

    # Anomalies
    if "anomaly" in message or "anomalies" in message or "anomaly detection" in message:
        return {
            "reply": "Anomalies are unusual patterns in the data, such as sudden spikes or drops. They help detect unexpected changes in user behavior or conversions."
        }

    # Funnel
    if "funnel" in message or "funnel analysis" in message:
        return {
            "reply": "Funnel analysis tracks how users move from views to carts to purchases. It helps identify where users drop off in the buying journey."
        }

    # Fallback to Ollama
    prompt = f"""
You are an AI analytics assistant for a Distributed E-commerce Dashboard.

Use ONLY the dashboard metrics below when answering.

Dashboard Metrics:
- Total Views: {total_views}
- Total Carts: {total_carts}
- Total Purchases: {total_purchases}
- View to Cart Rate: {view_to_cart_rate}
- Cart to Purchase Rate: {cart_to_purchase_rate}
- Overall Conversion Rate: {overall_conversion_rate}

Rules:
- Answer in 1 or 2 short sentences
- Be specific to these metrics
- Do not give generic AI assistant replies
- Do not invent numbers
- If the question is unclear, say that the dashboard currently provides views, carts, purchases, and conversion metrics

User Question: {payload.message}

Answer:
"""

    try:
        response = requests.post(
            OLLAMA_URL,
            json={
                "model": OLLAMA_MODEL,
                "prompt": prompt,
                "stream": False,
                "options": {
                    "num_predict": 60,
                    "temperature": 0.2
                },
                "keep_alive": "10m"
            },
            timeout=120
        )

        response.raise_for_status()
        result = response.json()
        reply = (result.get("response") or "").strip()

        if not reply:
            reply = "The dashboard currently provides views, carts, purchases, and conversion metrics."

        return {"reply": reply}

    except Exception as e:
        return {"reply": f"Error talking to Ollama: {str(e)}"}