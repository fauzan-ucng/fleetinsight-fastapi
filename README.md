# 🚚 FleetInsight FastAPI

FleetInsight FastAPI adalah service yang menerima insight dari Snowflake (melalui Stored Procedure)  
dan meneruskannya ke Telegram bot menggunakan API Bot Telegram.

---

## ⚙️ Setup Lokal

### 1️⃣ Buat virtual environment
```bash
python -m venv venv
venv\Scripts\activate
pip install -r requirements.txt
```

### 2️⃣ Siapkan file `.env`
Buat file `.env` berisi:
```
TELEGRAM_BOT_TOKEN=your_real_bot_token
TELEGRAM_CHAT_ID=your_real_chat_id
```

### 3️⃣ Jalankan server
```bash
uvicorn main:app --reload
```

Akses dokumentasi API di:
👉 [http://127.0.0.1:8000/docs](http://127.0.0.1:8000/docs)

---

## 🌍 Deploy ke Render

1. Buat repo GitHub bernama `fleetinsight-fastapi`
2. Upload semua file (`main.py`, `requirements.txt`, `.env.example`, `README.md`, `.gitignore`, `render.yaml`)
3. Buka [https://render.com](https://render.com)
4. **New → Web Service → pilih repo GitHub**
5. Isi:
   - Build Command: `pip install -r requirements.txt`
   - Start Command: `uvicorn main:app --host 0.0.0.0 --port 10000`
6. Tambahkan environment variables di tab **Environment**:
   ```
   TELEGRAM_BOT_TOKEN=your_bot_token
   TELEGRAM_CHAT_ID=your_chat_id
   ```

Setelah deploy selesai, buka:
```
https://<your-app-name>.onrender.com/docs
```

---

## 🔗 Integrasi Snowflake

Gunakan Stored Procedure di Snowflake:
```sql
CREATE OR REPLACE PROCEDURE SEND_INSIGHT_TO_TELEGRAM_PY(MESSAGE STRING)
RETURNS STRING
LANGUAGE PYTHON
RUNTIME_VERSION = '3.10'
PACKAGES = ('snowflake-snowpark-python', 'requests')
HANDLER = 'main'
EXECUTE AS CALLER
AS
$$
import requests

def main(session, message):
    FASTAPI_URL = "https://<your-app-name>.onrender.com/send-insight"
    response = requests.post(FASTAPI_URL, json={"message": message})
    return f"Status {response.status_code}: {response.text}"
$$;
```

---

## ✅ Flow Diagram
```
Snowflake Agent → Stored Procedure (Python) → FleetInsight FastAPI → Telegram Bot
```

---

## 🧩 Maintainer
**Fauzan Agung**  
Hackathon Project: *Spurring Innovation with Snowflake Agentic AI*
