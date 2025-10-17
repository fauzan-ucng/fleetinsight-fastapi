from fastapi import FastAPI
import requests
import os
from dotenv import load_dotenv

load_dotenv()

app = FastAPI(title="FleetInsight FastAPI Service")

TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")

@app.get("/")
def home():
    return {"status": "running"}

@app.post("/send-insight")
def send_insight():
    try:
        if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
            return {"error": "Missing TELEGRAM_BOT_TOKEN or TELEGRAM_CHAT_ID"}

        message = "🚛 FleetInsight Test Message: Bot sudah aktif dan terkoneksi!"
        url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
        payload = {"chat_id": TELEGRAM_CHAT_ID, "text": message}

        response = requests.post(url, json=payload)
        print("Response:", response.text)

        if response.status_code == 200:
            return {"success": True, "response": response.json()}
        else:
            return {"error": True, "details": response.text}
    except Exception as e:
        print("Error:", str(e))
        return {"error": "Internal Server Error", "details": str(e)}
