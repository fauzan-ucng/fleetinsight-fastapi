from fastapi import FastAPI
import os
import sys
import requests
from dotenv import load_dotenv

# ========================================
# 🧠 Environment Check
# ========================================

# Cek apakah sedang di dalam virtual environment
if sys.prefix == sys.base_prefix:
    print("⚠️  WARNING: Virtual environment (venv) belum aktif!")
    print("💡 Jalankan salah satu perintah berikut sebelum run server:")
    print("   👉 PowerShell (sementara): Set-ExecutionPolicy -Scope Process -ExecutionPolicy Bypass")
    print("   👉 PowerShell (aktifkan venv): .\\venv\\Scripts\\activate")
    print("   👉 CMD (alternatif): venv\\Scripts\\activate.bat")
    print("===============================================")

# Cek apakah pandas sudah tersedia
try:
    import pandas as pd
except ImportError:
    print("⚠️  Pandas belum terinstal! Jalankan perintah berikut di terminal:")
    print("   👉 pip install pandas")
    print("===============================================")
    pd = None  # agar script tetap jalan

# ========================================
# 📦 Import Snowflake dependencies
# ========================================
from snowflake.snowpark import Session
from snowflake.snowpark.functions import col

# ========================================
# 🚀 App setup
# ========================================
load_dotenv()
app = FastAPI(title="FleetInsight FastAPI Service", version="3.0")

# ========================================
# 🔧 Configuration
# ========================================
SNOWFLAKE_ACCOUNT = os.getenv("SNOWFLAKE_ACCOUNT")
SNOWFLAKE_USER = os.getenv("SNOWFLAKE_USER")
SNOWFLAKE_PASSWORD = os.getenv("SNOWFLAKE_PASSWORD")
SNOWFLAKE_WAREHOUSE = os.getenv("SNOWFLAKE_WAREHOUSE")
SNOWFLAKE_DATABASE = os.getenv("SNOWFLAKE_DATABASE")
SNOWFLAKE_SCHEMA = os.getenv("SNOWFLAKE_SCHEMA")
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")

# ========================================
# 🔗 Snowflake Connection
# ========================================
def create_snowflake_session():
    """Buat koneksi ke Snowflake."""
    try:
        session = Session.builder.configs({
            "account": SNOWFLAKE_ACCOUNT,
            "user": SNOWFLAKE_USER,
            "password": SNOWFLAKE_PASSWORD,
            "warehouse": SNOWFLAKE_WAREHOUSE,
            "database": SNOWFLAKE_DATABASE,
            "schema": SNOWFLAKE_SCHEMA
        }).create()

        # Pastikan warehouse aktif
        session.sql(f"USE WAREHOUSE {SNOWFLAKE_WAREHOUSE}").collect()
        session.sql(f"USE DATABASE {SNOWFLAKE_DATABASE}").collect()
        session.sql(f"USE SCHEMA {SNOWFLAKE_SCHEMA}").collect()
        return session

    except Exception as e:
        print(f"❌ Snowflake connection failed: {e}")
        return None

# ========================================
# 🔔 Telegram Sender
# ========================================
def send_telegram_message(message: str):
    """Kirim pesan ke Telegram bot."""
    try:
        url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
        payload = {"chat_id": TELEGRAM_CHAT_ID, "text": message, "parse_mode": "Markdown"}
        response = requests.post(url, json=payload)
        if response.status_code != 200:
            print(f"⚠️ Telegram error: {response.text}")
        return response.json()
    except Exception as e:
        print(f"❌ Telegram send failed: {e}")
        return None

# ========================================
# 🏠 Root Endpoint
# ========================================
@app.get("/")
def home():
    return {"message": "✅ FleetInsight FastAPI is running successfully!"}

# ========================================
# 📊 Pull Insight Endpoint
# ========================================
@app.get("/pull-insight")
def pull_insight():
    """Ambil performa kendaraan dari Snowflake."""
    session = create_snowflake_session()
    if session is None:
        return {"status": "error", "message": "Gagal koneksi ke Snowflake"}

    try:
        query = """
        SELECT VEHICLE_ID, 
               AVG(AVG_SPEED) AS AVG_SPEED,
               SUM(TOTAL_DISTANCE_KM) AS TOTAL_DISTANCE,
               AVG(TOTAL_IDLE_TIME_S) AS AVG_IDLE_TIME
        FROM FLEET_ANALYTICS.ANALYTICS.VEHICLE_METRICS
        GROUP BY VEHICLE_ID
        ORDER BY TOTAL_DISTANCE DESC
        LIMIT 5
        """
        result = session.sql(query)

        if pd:
            df = result.to_pandas()
            data = df.to_dict(orient="records")
        else:
            rows = result.collect()
            data = [r.as_dict() for r in rows]

        session.close()
        return {"status": "success", "data": data}

    except Exception as e:
        return {"status": "error", "message": str(e)}

# ========================================
# 📬 Manual Telegram Test
# ========================================
@app.post("/send-insight")
def send_insight():
    """Tes kirim pesan ke Telegram."""
    message = "🚛 FleetInsight: Bot aktif & terhubung!"
    res = send_telegram_message(message)
    return {"status": "sent", "response": res}

# ========================================
# ⚡ Auto Notify Endpoint
# ========================================
@app.post("/auto-notify")
def auto_notify():
    """Ambil insight dari Snowflake, kirim ke Telegram, dan simpan ke tabel log."""
    session = create_snowflake_session()
    if session is None:
        return {"status": "error", "message": "Gagal koneksi ke Snowflake"}

    try:
        query = """
        SELECT VEHICLE_ID, 
               AVG(AVG_SPEED) AS AVG_SPEED,
               SUM(TOTAL_DISTANCE_KM) AS TOTAL_DISTANCE,
               AVG(TOTAL_IDLE_TIME_S) AS AVG_IDLE_TIME
        FROM FLEET_ANALYTICS.ANALYTICS.VEHICLE_METRICS
        GROUP BY VEHICLE_ID
        ORDER BY TOTAL_DISTANCE DESC
        LIMIT 1
        """
        df = session.sql(query).to_pandas() if pd else session.sql(query).collect()
        if not pd and not df:
            return {"status": "no_data", "message": "Tidak ada data performa"}

        if pd:
            record = df.iloc[0]
            vehicle, avg_speed, total_distance, avg_idle = (
                record["VEHICLE_ID"],
                round(record["AVG_SPEED"], 2),
                round(record["TOTAL_DISTANCE"], 2),
                round(record["AVG_IDLE_TIME"], 2)
            )
        else:
            record = df[0].as_dict()
            vehicle, avg_speed, total_distance, avg_idle = (
                record["VEHICLE_ID"],
                round(record["AVG_SPEED"], 2),
                round(record["TOTAL_DISTANCE"], 2),
                round(record["AVG_IDLE_TIME"], 2)
            )

        message = (
            f"🚚 *FleetInsight Daily Report*\n\n"
            f"📍 Kendaraan: `{vehicle}`\n"
            f"🏁 Jarak Tempuh: {total_distance} km\n"
            f"⚡ Kecepatan Rata-rata: {avg_speed} km/h\n"
            f"⏱️ Waktu Idle: {avg_idle} detik\n\n"
            f"Insight dikirim otomatis oleh Snowflake Cortex Agent."
        )

        send_telegram_message(message)

        session.sql(f"""
        INSERT INTO FLEET_ANALYTICS.ANALYTICS.INSIGHT_QUEUE (MESSAGE, STATUS)
        VALUES ('{message}', 'SENT')
        """).collect()

        session.close()
        return {"status": "success", "message": "Insight terkirim ke Telegram dan disimpan ke tabel."}

    except Exception as e:
        return {"status": "error", "message": str(e)}
