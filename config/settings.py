# ----------------------- settings.py (FULL) -----------------------
from __future__ import annotations
import os, json
from dotenv import load_dotenv

load_dotenv()

def _bool(v: str | None, default: bool = False) -> bool:
    if v is None: return default
    return str(v).strip().lower() in ("1","true","yes","y","on")

# ====== Cấu hình cũ (fallback đơn tài khoản) ======
EXCHANGE_ID = os.getenv("EXCHANGE", "binanceusdm")
API_KEY = os.getenv("API_KEY", "")
API_SECRET = os.getenv("API_SECRET", "")
TESTNET = _bool(os.getenv("TESTNET"), False)

PAIR = os.getenv("PAIR", "BTC/USDT")
MODE = os.getenv("MODE", "manual")  # manual | auto
PRESET_MODE = os.getenv("PRESET_MODE", "auto")

# Risk & runtime
MAX_ORDERS_PER_DAY = int(os.getenv("MAX_ORDERS_PER_DAY", "8"))
MAX_ORDERS_PER_TIDE_WINDOW = int(os.getenv("MAX_ORDERS_PER_TIDE_WINDOW", "2"))

TIDE_WINDOW_HOURS = float(os.getenv("TIDE_WINDOW_HOURS", "2.5"))
SCHEDULER_TICK_SEC = int(os.getenv("SCHEDULER_TICK_SEC", "2"))

# Telegram chính (bot tương tác)
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "")

# Telegram phát kèo (EXECUTE-only) — bot/tài khoản khác
TELEGRAM_BROADCAST_BOT_TOKEN = os.getenv("TELEGRAM_BROADCAST_BOT_TOKEN", "")
TELEGRAM_BROADCAST_CHAT_ID = os.getenv("TELEGRAM_BROADCAST_CHAT_ID", "")

# Moon/Tide/Geo
LAT = float(os.getenv("LAT", "32.7503"))
LON = float(os.getenv("LON", "129.8777"))

# Debug flags
AUTO_DEBUG = _bool(os.getenv("AUTO_DEBUG"), False)
AUTO_DEBUG_VERBOSE = _bool(os.getenv("AUTO_DEBUG_VERBOSE"), False)
AUTO_DEBUG_ONLY_WHEN_SKIP = _bool(os.getenv("AUTO_DEBUG_ONLY_WHEN_SKIP"), False)

# ====== MULTI-ACCOUNT ======
ACCOUNTS_JSON = os.getenv("ACCOUNTS_JSON", "").strip()
ACCOUNTS: list[dict] = []
if ACCOUNTS_JSON:
    try:
        ACCOUNTS = json.loads(ACCOUNTS_JSON)
        if not isinstance(ACCOUNTS, list):
            ACCOUNTS = []
    except Exception:
        ACCOUNTS = []

# ====== Fallback single account (giữ tương thích cũ) ======
SINGLE_ACCOUNT = {
    "name": "default",
    "exchange": EXCHANGE_ID,
    "api_key": API_KEY,
    "api_secret": API_SECRET,
    "testnet": TESTNET,
    "pair": PAIR,
    "risk_percent": float(os.getenv("RISK_PERCENT", "20")),
    "leverage": int(os.getenv("LEVERAGE", "44")),
}
