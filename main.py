from fastapi import FastAPI
from datetime import datetime
from pydantic import BaseModel
from snowflake.snowpark import Session
from snowflake.snowpark.exceptions import SnowparkSQLException
import os
import requests
import uuid
import pandas as pd
from dotenv import load_dotenv
from apscheduler.schedulers.background import BackgroundScheduler
import threading
import time
from datetime import datetime

# === Load Environment Variables ===
load_dotenv()

# === App Metadata ===
app = FastAPI(
    title="FleetInsight FastAPI",
    version="2.0",
    description="Fleet performance analyzer powered by Snowflake Snowpark + Telegram integration + Scheduler"
)

# === Environment Config ===
SNOWFLAKE_ACCOUNT = os.getenv("SNOWFLAKE_ACCOUNT")
SNOWFLAKE_USER = os.getenv("SNOWFLAKE_USER")
SNOWFLAKE_PASSWORD = os.getenv("SNOWFLAKE_PASSWORD")
SNOWFLAKE_WAREHOUSE = os.getenv("SNOWFLAKE_WAREHOUSE")
SNOWFLAKE_DATABASE = os.getenv("SNOWFLAKE_DATABASE")
SNOWFLAKE_SCHEMA = os.getenv("SNOWFLAKE_SCHEMA")
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")
# Ambil base URL dari environment (Render / Lokal)
APP_BASE_URL = os.getenv("APP_BASE_URL", "http://127.0.0.1:10000")

# === Snowflake Session Factory ===
def get_session():
    try:
        connection_parameters = {
            "account": SNOWFLAKE_ACCOUNT,
            "user": SNOWFLAKE_USER,
            "password": SNOWFLAKE_PASSWORD,
            "warehouse": SNOWFLAKE_WAREHOUSE,
            "database": SNOWFLAKE_DATABASE,
            "schema": SNOWFLAKE_SCHEMA,
        }
        session = Session.builder.configs(connection_parameters).create()
        return session
    except Exception as e:
        raise Exception(f"❌ Snowflake connection failed: {e}")
    

# --- Utility: Log Insight ke Snowflake ---
def log_insight_to_snowflake(df_logs: pd.DataFrame):
    try:
        session = get_session()
        session.use_database(SNOWFLAKE_DATABASE)
        session.use_schema(SNOWFLAKE_SCHEMA)
        # pastikan tabel log ada
        session.sql("""
            CREATE TABLE IF NOT EXISTS FLEET_ANALYTICS.ANALYTICS.INSIGHT_LOGS (
                RUN_AT TIMESTAMP,
                VEHICLE_ID STRING,
                AVG_SPEED FLOAT,
                TOTAL_DISTANCE FLOAT,
                AVG_IDLE_TIME FLOAT,
                TELEGRAM_STATUS STRING,
                MESSAGE_ID STRING,
                EXECUTION_ID STRING
            );
        """).collect()

        snow_df = session.create_dataframe(df_logs)
        snow_df.write.mode("append").save_as_table("FLEET_ANALYTICS.ANALYTICS.INSIGHT_LOGS")
        session.close()
        return True
    except Exception as e:
        print(f"❌ Failed to log insights: {e}")
        return False

# --- Endpoint Root ---
@app.get("/")
def root():
    return {
        "message": "Welcome to FleetInsight FastAPI 🚀",
        "docs": "/docs",
        "timestamp": datetime.now().isoformat()
    }

# --- Endpoint Health Check ---
@app.get("/health")
def health_check():
    snowflake_status = "❌"
    telegram_status = "❌"

    try:
        session = get_session()
        version = session.sql("SELECT CURRENT_VERSION()").collect()[0][0]
        snowflake_status = f"✅ Connected (v{version})"
        session.close()
    except Exception as e:
        snowflake_status = f"❌ {e}"

    try:
        r = requests.get(f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/getMe", timeout=5)
        telegram_status = "✅ Connected" if r.status_code == 200 else f"❌ {r.status_code}"
    except Exception as e:
        telegram_status = f"❌ {e}"

    return {
        "status": "ok",
        "timestamp": datetime.now().isoformat(),
        "snowflake": snowflake_status,
        "telegram": telegram_status,
    }

# --- Endpoint Metrics Overview ---
@app.get("/metrics")
def get_metrics():
    try:
        session = get_session()
        query = """
            SELECT 
                COUNT(DISTINCT VEHICLE_ID) AS TOTAL_VEHICLES,
                AVG(AVG_SPEED) AS AVG_SPEED,
                SUM(TOTAL_DISTANCE_KM) AS TOTAL_DISTANCE,
                AVG(TOTAL_IDLE_TIME_S) AS AVG_IDLE_TIME
            FROM FLEET_ANALYTICS.ANALYTICS.VEHICLE_METRICS
        """
        result = session.sql(query).collect()[0]
        session.close()

        return {
            "status": "success",
            "timestamp": datetime.now().isoformat(),
            "total_vehicle": int(result["TOTAL_VEHICLES"]),
            "avg_speed": float(result["AVG_SPEED"]),
            "total_distance": float(result["TOTAL_DISTANCE"]),
            "avg_idle_time": float(result["AVG_IDLE_TIME"])
        }
    except Exception as e:
        return {"status": "error", "message": str(e)}

# --- Endpoint Pull Insight ---
@app.get("/pull-insight")
def pull_insight():
    try:
        session = get_session()
        df = session.sql("""
            SELECT 
                VEHICLE_ID,
                AVG(AVG_SPEED) AS AVG_SPEED,
                SUM(TOTAL_DISTANCE_KM) AS TOTAL_DISTANCE,
                AVG(TOTAL_IDLE_TIME_S) AS AVG_IDLE_TIME
            FROM FLEET_ANALYTICS.ANALYTICS.VEHICLE_METRICS
            GROUP BY VEHICLE_ID
            ORDER BY TOTAL_DISTANCE DESC
            LIMIT 5
        """).to_pandas()
        session.close()

        return {
            "status": "success",
            "timestamp": datetime.now().isoformat(),
            "data": df.to_dict(orient="records")
        }
    except Exception as e:
        return {"status": "error", "message": str(e)}

# --- Endpoint Auto Notify (kirim ke Telegram + log) ---
@app.post("/auto-notify")
def auto_notify():
    try:
        session = get_session()
        df = session.sql("""
            SELECT 
                VEHICLE_ID,
                AVG(AVG_SPEED) AS AVG_SPEED,
                SUM(TOTAL_DISTANCE_KM) AS TOTAL_DISTANCE,
                AVG(TOTAL_IDLE_TIME_S) AS AVG_IDLE_TIME
            FROM FLEET_ANALYTICS.ANALYTICS.VEHICLE_METRICS
            GROUP BY VEHICLE_ID
            ORDER BY TOTAL_DISTANCE DESC
            LIMIT 3
        """).to_pandas()
        session.close()

        if df.empty:
            return {"status": "warning", "message": "No data found"}

        # buat pesan Telegram
        message = "📊 *Fleet Performance Insights*\n\n"
        for _, row in df.iterrows():
            message += (
                f"🚚 Vehicle: `{row['VEHICLE_ID']}`\n"
                f"• Avg Speed: {row['AVG_SPEED']:.2f} km/h\n"
                f"• Total Distance: {row['TOTAL_DISTANCE']:.2f} km\n"
                f"• Idle Time: {row['AVG_IDLE_TIME']:.2f} min\n\n"
            )
        message += f"🕒 Updated at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"

        telegram_url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
        payload = {"chat_id": TELEGRAM_CHAT_ID, "text": message, "parse_mode": "Markdown"}
        r = requests.post(telegram_url, json=payload)

        # status Telegram & logging
        telegram_status = "✅ SENT" if r.status_code == 200 else f"❌ {r.status_code}"
        message_id = r.json().get("result", {}).get("message_id", "N/A")
        execution_id = str(uuid.uuid4())

        df_logs = pd.DataFrame([
            {
                "RUN_AT": datetime.now(),
                "VEHICLE_ID": row["VEHICLE_ID"],
                "AVG_SPEED": row["AVG_SPEED"],
                "TOTAL_DISTANCE": row["TOTAL_DISTANCE"],
                "AVG_IDLE_TIME": row["AVG_IDLE_TIME"],
                "TELEGRAM_STATUS": telegram_status,
                "MESSAGE_ID": message_id,
                "EXECUTION_ID": execution_id
            }
            for _, row in df.iterrows()
        ])

        log_insight_to_snowflake(df_logs)

        global last_scheduler_run
        last_scheduler_run = datetime.now().isoformat()

        return {
            "status": "success",
            "message": f"Insight sent ({telegram_status})",
            "execution_id": execution_id
        }

    except Exception as e:
        return {"status": "error", "message": str(e)}

# === Scheduler Setup ===
scheduler = BackgroundScheduler()
last_scheduler_run = None

def scheduled_auto_notify():
    """Job otomatis menjalankan auto-notify setiap 60 menit"""
    try:
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        print(f"[{timestamp}] 🚀 Running scheduled auto-notify...")
        print(f"[Scheduler] Calling {APP_BASE_URL}/auto-notify ...")

        # Panggil endpoint auto-notify dengan POST
        response = requests.post(f"{APP_BASE_URL}/auto-notify")

        # Logging hasil
        if response.status_code == 200:
            print(f"[{timestamp}] ✅ Scheduler success: {response.status_code}")
        else:
            print(f"[{timestamp}] ⚠️ Scheduler returned non-200: {response.status_code}")
            print(f"[Scheduler] Response: {response.text}")
    except Exception as e:
        print(f"[{datetime.now()}] ❌ Scheduler failed: {e}")

# Jalankan job setiap 3 menit
scheduler.add_job(scheduled_auto_notify, "interval", minutes=60)
scheduler.start()
print("✅ FleetInsight Scheduler started (interval: 60 minutes)")


# Pastikan scheduler tetap hidup bersama FastAPI # Loop pasif agar thread scheduler tetap hidup
def run_scheduler():
    while True:
        time.sleep(60)  # tunggu 1 menit di setiap loop, hemat CPU

threading.Thread(target=run_scheduler, daemon=True).start()

# === Endpoint Scheduler Status ===
@app.get("/scheduler-status")
def scheduler_status():
    return {
        "status": "running",
        "last_run": last_scheduler_run,
        "interval_minutes": 60,
        "timestamp": datetime.now().isoformat()
    }
