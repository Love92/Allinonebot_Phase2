# ----------------------- config/settings.py -----------------------
from __future__ import annotations
import os, json
from dotenv import load_dotenv

load_dotenv()

# -------- helpers ----------
def _env_bool(key: str, default: str = "false") -> bool:
    return (os.getenv(key, default) or "").strip().lower() in ("1","true","yes","on","y")

def _as_float(env_key: str, default: str) -> float:
    try:
        return float(os.getenv(env_key, default))
    except Exception:
        return float(default)

def _as_int(env_key: str, default: str) -> int:
    try:
        return int(float(os.getenv(env_key, default)))
    except Exception:
        return int(default)

# ===== Telegram =====
TELEGRAM_BOT_TOKEN            = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
TELEGRAM_CHAT_ID              = os.getenv("TELEGRAM_CHAT_ID", "").strip()

# Kênh phát kèo (EXECUTE-only)
TELEGRAM_BROADCAST_BOT_TOKEN  = os.getenv("TELEGRAM_BROADCAST_BOT_TOKEN", "").strip()
TELEGRAM_BROADCAST_CHAT_ID    = os.getenv("TELEGRAM_BROADCAST_CHAT_ID", "").strip()

# ===== Modes / Defaults =====
# (giữ compatibility với bản cũ)
MODE           = os.getenv("MODE", "manual").strip().lower()   # manual | auto
DEFAULT_MODE   = os.getenv("DEFAULT_MODE", MODE)
PAIR           = os.getenv("PAIR", "BTC/USDT").strip().upper()
PRESET_MODE    = os.getenv("PRESET_MODE", "auto")

# ===== Exchange — Single-account (Binance fallback như bản cũ) =====
EXCHANGE_ID    = os.getenv("EXCHANGE", "binanceusdm").strip().lower()
API_KEY        = os.getenv("API_KEY", "").strip()
API_SECRET     = os.getenv("API_SECRET", "").strip()
TESTNET        = _env_bool("TESTNET", "false")

# Cho phép /settings thay đổi risk/leverage chung
RISK_PERCENT_DEFAULT = _as_float("RISK_PERCENT", "20")
LEVERAGE_DEFAULT     = _as_int("LEVERAGE", "44")

# ===== Weather / Tide (để tránh ImportError ở data/moon_tide.py) =====
WEATHERAPI_KEY = os.getenv("WEATHERAPI_KEY", "").strip()
WORLDTIDES_KEY = os.getenv("WORLDTIDES_KEY", "").strip()
try:
    LAT = float(os.getenv("LAT", "10.8231"))
    LON = float(os.getenv("LON", "106.6297"))
except Exception:
    LAT, LON = 10.8231, 106.6297

# ===== Scheduler / Runtime knobs =====
TIDE_WINDOW_HOURS     = _as_float("TIDE_WINDOW_HOURS", "2.5")
SCHEDULER_TICK_SEC    = _as_int("SCHEDULER_TICK_SEC", "2")
MAX_ORDERS_PER_DAY    = _as_int("MAX_ORDERS_PER_DAY", "8")
MAX_ORDERS_PER_TIDE_WINDOW = _as_int("MAX_ORDERS_PER_TIDE_WINDOW", "2")
M5_MAX_DELAY_SEC      = _as_int("M5_MAX_DELAY_SEC", "60")

# ===== Debug flags =====
AUTO_DEBUG                = _env_bool("AUTO_DEBUG", "true")
AUTO_DEBUG_VERBOSE        = _env_bool("AUTO_DEBUG_VERBOSE", "false")
AUTO_DEBUG_ONLY_WHEN_SKIP = _env_bool("AUTO_DEBUG_ONLY_WHEN_SKIP", "false")

# ===== Multi-account (Phase2) =====
# ACCOUNTS_JSON: 1 dòng JSON dạng list các account bổ sung (BingX/OKX...)
# Ví dụ 1 dòng cho BingX (thay vào .env):
# ACCOUNTS_JSON=[{"name":"bingx_test","exchange":"bingx","api_key":"<BINGX_KEY>","api_secret":"<BINGX_SECRET>","testnet":false,"pair":"BTC/USDT:USDT"}]
ACCOUNTS_JSON = (os.getenv("ACCOUNTS_JSON", "") or "").strip()
ACCOUNTS: list[dict] = []
if ACCOUNTS_JSON:
    try:
        ACCOUNTS = json.loads(ACCOUNTS_JSON)
        if not isinstance(ACCOUNTS, list):
            ACCOUNTS = []
    except Exception:
        ACCOUNTS = []

# Fallback Single Binance account (y như bản cũ)
SINGLE_ACCOUNT = {
    "name": "default",
    "exchange": EXCHANGE_ID,          # "binanceusdm"
    "api_key": API_KEY,
    "api_secret": API_SECRET,
    "testnet": TESTNET,
    "pair": PAIR,                     # giữ "BTC/USDT" cho Binance
    # risk/leverage mặc định; runtime /settings sẽ ghi đè khi execute
    "risk_percent": RISK_PERCENT_DEFAULT,
    "leverage": LEVERAGE_DEFAULT,
}

# (auto_trade_engine sẽ import cả ACCOUNTS và SINGLE_ACCOUNT và tự merge)
# Không cần cờ bật/tắt đa sàn: nếu ACCOUNTS trống ⇒ chỉ chạy SINGLE_ACCOUNT (Binance như cũ);
# nếu ACCOUNTS có (ví dụ BingX) ⇒ chạy cả 2.

# ===== Compatibility exports =====
EXCHANGE = EXCHANGE_ID
DEFAULT_PAIR = PAIR
DEFAULT_PRESET_MODE = PRESET_MODE
# ----------------------- /config/settings.py -----------------------
