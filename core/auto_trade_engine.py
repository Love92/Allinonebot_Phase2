# ----------------------- core/auto_trade_engine.py -----------------------
from __future__ import annotations

import os
import asyncio
from dataclasses import dataclass
from typing import Optional, Dict, Any, List, Tuple
from datetime import datetime, timedelta, timezone
from core.approval_flow import create_pending_v2, get_pending, mark_done
from core.tide_gate import TideGateConfig, tide_gate_check, bump_counters_after_execute


# ========= Imports đồng bộ với /report =========
# Lấy KẾT QUẢ CHUẨN H4/M30/Moon từ strategy.signal_generator.evaluate_signal()
try:
    from strategy.signal_generator import evaluate_signal  # trả về dict: ok, skip, signal, confidence, text, frames
except Exception:
    # fallback nếu cấu trúc dự án khác
    from signal_generator import evaluate_signal  # type: ignore

# Moon/tide helpers (để log thêm late-window, TP-by-time)
try:
    from data.moon_tide import get_tide_events
except Exception:
    get_tide_events = None  # type: ignore

# Tham số cửa sổ thủy triều mặc định (có thể đổi qua /tidewindow)
try:
    from config.settings import TIDE_WINDOW_HOURS
except Exception:
    TIDE_WINDOW_HOURS = float(os.getenv("TIDE_WINDOW_HOURS", "2.5"))

# M5 gate: vẫn giữ để kiểm soát cuối cùng nếu cần
try:
    from strategy.m5_strategy import m5_entry_check
except Exception:
    m5_entry_check = None  # type: ignore
    
# [ADD] Hub + formatter thống nhất
try:
    from core.trade_executor import execute_order_flow
except Exception:
    from trade_executor import execute_order_flow  # type: ignore

try:
    from tg.formatter import render_executed_boardcard, render_signal_preview   # Cần theo dõi để cải thiện
except Exception:
    # fallback nếu dự án chưa có đủ 2 hàm (không crash)
    def render_executed_boardcard(**kw):  # type: ignore
        ids = kw.get("entry_ids") or []
        ids_line = " | ".join([str(x) for x in ids]) if ids else "—"
        return (
            "🤖 **EXECUTED**\n"
            f"(Mode: {kw.get('origin','AUTO')}) | {kw.get('symbol','?')} **{kw.get('side','?')}**\n"
            f"🆔 Entry ID(s): {ids_line}"
        )
    def render_signal_preview(*args, **kwargs):  # type: ignore
        return ""
   
    

# Thực thi lệnh (nếu có kết nối sàn)
ExchangeClient = calc_qty = auto_sl_by_leverage = None
for path in ("core.trade_executor", "trade_executor"):
    try:
        mod = __import__(path, fromlist=["ExchangeClient", "calc_qty", "auto_sl_by_leverage"])
        ExchangeClient = getattr(mod, "ExchangeClient", None)
        calc_qty = getattr(mod, "calc_qty", None)
        auto_sl_by_leverage = getattr(mod, "auto_sl_by_leverage", None)
        if ExchangeClient and calc_qty and auto_sl_by_leverage:
            break
    except Exception:
        continue

# ========= Timezone =========
try:
    import pytz
    VN_TZ = pytz.timezone("Asia/Ho_Chi_Minh")
    JST   = pytz.timezone("Asia/Tokyo")
except Exception:
    VN_TZ = timezone(timedelta(hours=7))  # fallback
    JST   = timezone(timedelta(hours=9))

def now_vn() -> datetime:
    try:
        return datetime.now(VN_TZ)
    except Exception:
        return datetime.utcnow() + timedelta(hours=7)

# ========= Helpers chung =========
def _floor_5m_epoch(ts: int) -> int:
    return ts // 300

def _one_line(tag: str, reason: str, now: datetime, extra: str = "") -> str:
    t = now.strftime("%Y-%m-%d %H:%M:%S")
    return f"[{tag}] {t} | {reason} | {extra}".strip()

async def _debug_send(app, uid: int, text: str) -> None:
    try:
        await app.bot.send_message(chat_id=uid, text=text)
    except Exception:
        pass

# [ADD] Chuẩn hoá side về 'buy'/'sell' cho an toàn (dùng nếu cần ở nơi khác)
def _norm_side_txt(side_long_or_str) -> str:
    if isinstance(side_long_or_str, bool):
        return "buy" if side_long_or_str else "sell"
    s = str(side_long_or_str).strip().upper()
    if s in ("LONG", "BUY"):
        return "buy"
    if s in ("SHORT", "SELL"):
        return "sell"
    raise ValueError(f"Invalid side: {side_long_or_str}")

# ========= [ADD] helper side cho open_market =========
def _side_txt_from_bool(side_long: bool) -> str:
    # True = LONG, False = SHORT
    return "LONG" if bool(side_long) else "SHORT"

# ========= [ADD] Broadcast helpers (đồng bộ format với /order) =========
import html as _html
from typing import cast
try:
    from telegram import Bot as _TGBot  # python-telegram-bot (async)
except Exception:
    _TGBot = None  # type: ignore

def _esc(s: object) -> str:
    try:
        return _html.escape(str(s or ""), quote=False)
    except Exception:
        return str(s)

_TELEGRAM_BROADCAST_BOT_TOKEN = (os.getenv("TELEGRAM_BROADCAST_BOT_TOKEN") or "").strip()
_TELEGRAM_BROADCAST_CHAT_ID   = (os.getenv("TELEGRAM_BROADCAST_CHAT_ID") or "").strip()
__bcast_bot = None
if _TGBot and _TELEGRAM_BROADCAST_BOT_TOKEN:
    try:
        __bcast_bot = _TGBot(token=_TELEGRAM_BROADCAST_BOT_TOKEN)
    except Exception:
        __bcast_bot = None

async def _broadcast_html(text: str) -> None:
    """Gửi HTML vào broadcast group. Im lặng nếu thiếu token/chat id."""
    if not (__bcast_bot and _TELEGRAM_BROADCAST_CHAT_ID):
        return
    try:
        await cast(_TGBot, __bcast_bot).send_message(
            chat_id=int(_TELEGRAM_BROADCAST_CHAT_ID),
            text=text,
            parse_mode="HTML",
            disable_web_page_preview=True,
        )
    except Exception:
        pass

# ================== Broadcast tín hiệu  ==================
def _fmt_exec_broadcast(
    *, pair: str, side: str, acc_name: str, ex_id: str,
    lev=None, risk=None, qty=None, entry_spot=None,
    sl: float | None = None, tp: float | None = None,
    tide_label: str | None = None, mode_label: str = "AUTO",
    entry_ids: list[str] | None = None, tp_time=None,
) -> str:
    """
    HTML cho broadcast group — format giống /order_cmd().
    - Các field risk/lev/qty/entry_spot có thể None -> sẽ tự ẩn.
    - entry_ids & tp_time là tùy chọn (nếu có sẽ in thêm).
    """
    import html as _html
    def _esc(x):
        try: return _html.escape("" if x is None else str(x), quote=False)
        except: return str(x)

    lines: list[str] = []
    lines.append(f"🚀 <b>EXECUTED</b> | <b>{_esc(pair)}</b> <b>{_esc(str(side).upper())}</b>")
    lines.append(f"• Mode: {mode_label}")
    lines.append(f"• Account: {_esc(acc_name)} ({_esc(ex_id)})")

    # Risk | Lev
    risk_part = f"Risk {float(risk):.1f}%" if isinstance(risk, (int, float)) else ""
    lev_part  = f"Lev x{int(lev)}"         if isinstance(lev,  (int, float)) else ""
    if risk_part or lev_part:
        joiner = " | " if (risk_part and lev_part) else ""
        lines.append(f"• {risk_part}{joiner}{lev_part}".strip(" |"))

    # Entry(SPOT) | Qty
    entry_part = f"Entry(SPOT)≈{float(entry_spot):.2f}" if isinstance(entry_spot, (int, float)) else ""
    qty_part   = f"Qty={float(qty):.6f}"                 if isinstance(qty,        (int, float)) else ""
    if entry_part or qty_part:
        joiner2 = " | " if (entry_part and qty_part) else ""
        lines.append(f"• {entry_part}{joiner2}{qty_part}".strip(" |"))
    else:
        lines.append("• Entry: —")

    # SL / TP
    lines.append(f"• SL={float(sl):.2f}" if isinstance(sl,(int,float)) else "• SL=—")
    lines.append(f"• TP={float(tp):.2f}" if isinstance(tp,(int,float)) else "• TP=—")

    # TP-by-time (nếu có)
    try:
        if tp_time is not None:
            dt = tp_time
            try:
                from utils.time_utils import VN_TZ  # nếu có
                if getattr(dt, "tzinfo", None) is None:
                    dt = VN_TZ.localize(dt)
                else:
                    dt = dt.astimezone(VN_TZ)
            except Exception:
                pass
            # dt có thể là datetime hoặc string
            timestr = dt.strftime('%Y-%m-%d %H:%M:%S') if hasattr(dt, "strftime") else str(dt)
            lines.append(f"• TP-by-time: {timestr}")
    except Exception:
        pass

    # Tide
    if tide_label:
        lines.append(f"• Tide: {_esc(tide_label)}")

    # Entry IDs (nếu có)
    if entry_ids:
        try:
            ids_str = ", ".join(str(x) for x in entry_ids if x)
            if ids_str:
                lines.append(f"• Entry ID(s): {_esc(ids_str)}")
        except Exception:
            pass

    return "\n".join(lines)

    

def _binance_spot_entry(pair: str) -> float:
    """Lấy giá hiển thị SPOT (Binance) để boardcard. Không dùng cho khớp lệnh."""
    try:
        from data.market_data import get_klines
        sym = pair.replace("/", "").replace(":USDT", "")
        df = get_klines(symbol=sym, interval="1m", limit=2)
        if df is not None and len(df) > 0:
            return float(df.iloc[-1]["close"])
    except Exception:
        pass
    return 0.0

# ========= ENV & runtime knobs (có thể đổi bằng /setenv hoặc preset) =========
def _env_bool(key: str, default: str = "false") -> bool:
    return (os.getenv(key, default) or "").strip().lower() in ("1", "true", "yes", "on", "y")

M5_MAX_DELAY_SEC        = int(float(os.getenv("M5_MAX_DELAY_SEC", "60")))
SCHEDULER_TICK_SEC      = int(float(os.getenv("SCHEDULER_TICK_SEC", "2")))

# ⚠️ DEPRECATED: quota/window cũ ở Gate A (đã chuyển sang TideGate T)
# MAX_TRADES_PER_WINDOW   = int(float(os.getenv("MAX_TRADES_PER_WINDOW", "2")))

ENTRY_LATE_ONLY         = _env_bool("ENTRY_LATE_ONLY", "false")
ENTRY_LATE_FROM_HRS     = float(os.getenv("ENTRY_LATE_FROM_HRS", "1.0"))
ENTRY_LATE_TO_HRS       = float(os.getenv("ENTRY_LATE_TO_HRS", "2.5"))

AUTO_DEBUG              = _env_bool("AUTO_DEBUG", "true")
AUTO_DEBUG_VERBOSE      = _env_bool("AUTO_DEBUG_VERBOSE", "false")
AUTO_DEBUG_ONLY_WHEN_SKIP = _env_bool("AUTO_DEBUG_ONLY_WHEN_SKIP", "false")
AUTO_DEBUG_CHAT_ID      = os.getenv("AUTO_DEBUG_CHAT_ID", "").strip()

# Rule: M5 buộc trùng hướng với M30 (anh có thể /setenv ENFORCE_M5_MATCH_M30 false để tắt)
ENFORCE_M5_MATCH_M30 = _env_bool("ENFORCE_M5_MATCH_M30", "true")

# --- Defaults cho các guard bổ sung (để _apply_runtime_env có giá trị ban đầu) ---
M30_FLIP_GUARD = True              # yêu cầu M30 không flip hướng quá nhanh
M30_STABLE_MIN_SEC = 1800          # số giây tối thiểu M30 phải ổn định
M30_NEED_CONSEC_N = 1              # số nến liên tiếp cần thoả điều kiện

M5_MIN_GAP_MIN = 15                # phút tối thiểu giữa 2 lần vào lệnh (gap guard)
M5_GAP_SCOPED_TO_WINDOW = True     # gap guard tính trong 1 cửa sổ tide hay toàn cục
ALLOW_SECOND_ENTRY = True          # cho phép vào lệnh thứ 2 trong cùng cửa sổ
M5_SECOND_ENTRY_MIN_RETRACE_PCT = 0.1 # % tối thiểu retrace để cho lệnh thứ 2


def _apply_runtime_env(kv: Dict[str, str]) -> None:
    """
    Cho phép /setenv ghi đè nhanh các ENV trong runtime.
    Đồng bộ lại toàn bộ biến module để auto-loop áp dụng ngay.
    """
    global ENTRY_LATE_ONLY, ENTRY_LATE_FROM_HRS, ENTRY_LATE_TO_HRS
    global AUTO_DEBUG, AUTO_DEBUG_VERBOSE, AUTO_DEBUG_ONLY_WHEN_SKIP, AUTO_DEBUG_CHAT_ID
    global ENFORCE_M5_MATCH_M30
    # Guards / filters mới:
    global M30_FLIP_GUARD, M30_STABLE_MIN_SEC, M30_NEED_CONSEC_N
    global M5_MIN_GAP_MIN, M5_GAP_SCOPED_TO_WINDOW, ALLOW_SECOND_ENTRY, M5_SECOND_ENTRY_MIN_RETRACE_PCT

    for k, v in kv.items():
        os.environ[k] = str(v)

    try:
        # late-window (⚠️ chỉ dùng để hiển thị; chặn thực tế đã gom vào TideGate)
        ENTRY_LATE_ONLY         = _env_bool("ENTRY_LATE_ONLY", "true" if ENTRY_LATE_ONLY else "false")
        ENTRY_LATE_FROM_HRS     = float(os.getenv("ENTRY_LATE_FROM_HRS", str(ENTRY_LATE_FROM_HRS)))
        ENTRY_LATE_TO_HRS       = float(os.getenv("ENTRY_LATE_TO_HRS", str(ENTRY_LATE_TO_HRS)))

        # debug
        AUTO_DEBUG              = _env_bool("AUTO_DEBUG", "true" if AUTO_DEBUG else "false")
        AUTO_DEBUG_VERBOSE      = _env_bool("AUTO_DEBUG_VERBOSE", "true" if AUTO_DEBUG_VERBOSE else "false")
        AUTO_DEBUG_ONLY_WHEN_SKIP = _env_bool("AUTO_DEBUG_ONLY_WHEN_SKIP", "true" if AUTO_DEBUG_ONLY_WHEN_SKIP else "false")
        AUTO_DEBUG_CHAT_ID      = os.getenv("AUTO_DEBUG_CHAT_ID", AUTO_DEBUG_CHAT_ID)

        # rules
        ENFORCE_M5_MATCH_M30    = _env_bool("ENFORCE_M5_MATCH_M30", "true" if ENFORCE_M5_MATCH_M30 else "false")

        # guards M30/M5
        M30_FLIP_GUARD          = _env_bool("M30_FLIP_GUARD", "true" if M30_FLIP_GUARD else "false")
        M30_STABLE_MIN_SEC      = int(float(os.getenv("M30_STABLE_MIN_SEC", str(M30_STABLE_MIN_SEC))))
        M30_NEED_CONSEC_N       = int(float(os.getenv("M30_NEED_CONSEC_N", str(M30_NEED_CONSEC_N))))
        M5_MIN_GAP_MIN          = int(float(os.getenv("M5_MIN_GAP_MIN", str(M5_MIN_GAP_MIN))))
        M5_GAP_SCOPED_TO_WINDOW = _env_bool("M5_GAP_SCOPED_TO_WINDOW", "true" if M5_GAP_SCOPED_TO_WINDOW else "false")
        ALLOW_SECOND_ENTRY      = _env_bool("ALLOW_SECOND_ENTRY", "true" if ALLOW_SECOND_ENTRY else "false")
        M5_SECOND_ENTRY_MIN_RETRACE_PCT = float(os.getenv("M5_SECOND_ENTRY_MIN_RETRACE_PCT", str(M5_SECOND_ENTRY_MIN_RETRACE_PCT)))

    except Exception:
        # Không crash auto loop nếu thiếu biến — chỉ bỏ qua cập nhật
        pass

async def _load_tidegate_config(storage, uid: Optional[int]) -> TideGateConfig:
    return TideGateConfig(
        tide_window_hours=float(os.getenv("TIDE_WINDOW_HOURS", "2.5")),
        entry_late_only=_env_bool("ENTRY_LATE_ONLY", False),
        entry_late_from=float(os.getenv("ENTRY_LATE_FROM_HRS", "1.0")),
        entry_late_to=float(os.getenv("ENTRY_LATE_TO_HRS", "2.5")),
        max_orders_per_day=int(os.getenv("MAX_ORDERS_PER_DAY", "8")),
        max_orders_per_tide_window=int(os.getenv("MAX_ORDERS_PER_TIDE_WINDOW", "2")),
        counter_scope=os.getenv("COUNTER_SCOPE", "per_user") or "per_user",
        lat=float(os.getenv("LAT", "32.7503")),
        lon=float(os.getenv("LON", "129.8777")),
    )


# ========= RISK-SENTINEL (Khoá AUTO nếu 2 SL liên tiếp ở 2 lần thủy triều khác nhau trong cùng ngày) =========
AUTO_LOCK_ON_2_SL = (os.getenv("AUTO_LOCK_ON_2_SL", "true").strip().lower() in ("1","true","yes","on","y"))
AUTO_LOCK_NOTIFY  = (os.getenv("AUTO_LOCK_NOTIFY", "true").strip().lower() in ("1","true","yes","on","y"))
_RS_STATE_KEY   = "risk_sentinel"
_RS_STATE_FILE  = "risk_sentinel_state.json"

def _rs_today_str(dt: Optional[datetime] = None) -> str:
    try:
        return (dt or now_vn()).strftime("%Y-%m-%d")
    except Exception:
        return datetime.utcnow().strftime("%Y-%m-%d")

def _rs_load_all(storage) -> Dict[str, Any]:
    if storage:
        data = getattr(storage, "get", lambda k: None)(_RS_STATE_KEY)
        return data if isinstance(data, dict) else {}
    try:
        with open(_RS_STATE_FILE, "r", encoding="utf-8") as f:
            import json as _json
            return _json.load(f) or {}
    except Exception:
        return {}

def _rs_save_all(storage, data: Dict[str, Any]) -> None:
    if storage and hasattr(storage, "set"):
        storage.set(_RS_STATE_KEY, data)
        return
    try:
        with open(_RS_STATE_FILE, "w", encoding="utf-8") as f:
            import json as _json
            _json.dump(data, f, ensure_ascii=False, indent=2)
    except Exception:
        pass

def _rs_get_day(storage, day: str) -> Dict[str, Any]:
    all_data = _rs_load_all(storage)
    return all_data.get(day, {
        "sl_streak": 0,
        "last_result": None,
        "last_window_key": None,
        "locked": False,
        "last_update": now_vn().isoformat() if callable(now_vn) else datetime.utcnow().isoformat(),
    })

def _rs_set_day(storage, day: str, st: Dict[str, Any]) -> None:
    all_data = _rs_load_all(storage)
    st["last_update"] = (now_vn().isoformat() if callable(now_vn) else datetime.utcnow().isoformat())
    all_data[day] = st
    _rs_save_all(storage, all_data)

def _rs_is_locked_today(storage, now: Optional[datetime] = None) -> bool:
    if not AUTO_LOCK_ON_2_SL:
        return False
    d = _rs_today_str(now)
    st = _rs_get_day(storage, d)
    return bool(st.get("locked", False))

def _rs_status_today(storage) -> Dict[str, Any]:
    d = _rs_today_str()
    st = _rs_get_day(storage, d)
    st["day"] = d
    return st

def _rs_reset_today(storage) -> None:
    d = _rs_today_str()
    _rs_set_day(storage, d, {
        "sl_streak": 0, "last_result": None, "last_window_key": None,
        "locked": False, "last_update": now_vn().isoformat() if callable(now_vn) else datetime.utcnow().isoformat()
    })

def _rs_on_trade_close(storage, *, result: str, window_key: Optional[str], when: Optional[datetime] = None) -> bool:
    """
    Gọi khi lệnh ĐÓNG.
    - Tăng sl_streak chỉ khi result == 'SL' và window_key KHÁC với SL trước đó (tức 2 lần thủy triều liên tiếp).
    - Nếu result != 'SL' thì reset streak.
    - Khi sl_streak >= 2 trong cùng ngày => locked=True.
    Trả về: locked_today (bool)
    """
    if not AUTO_LOCK_ON_2_SL:
        return False
    d = _rs_today_str(when)
    st = _rs_get_day(storage, d)

    if str(result).upper() == "SL":
        if st.get("last_result") == "SL" and window_key and st.get("last_window_key") and (window_key != st["last_window_key"]):
            st["sl_streak"] = int(st.get("sl_streak", 0)) + 1
        else:
            st["sl_streak"] = 1
    else:
        st["sl_streak"] = 0

    st["last_result"] = str(result).upper()
    if window_key:
        st["last_window_key"] = window_key

    if int(st.get("sl_streak", 0)) >= 2:
        st["locked"] = True

    _rs_set_day(storage, d, st)
    return bool(st.get("locked", False))
# ========= /RISK-SENTINEL =========

# ========= Tide helpers =========
def _nearest_tide_center(now: datetime) -> Optional[datetime]:
    """
    Lấy mốc thủy triều gần nhất (High/Low) trong ngày để tính late-window & TP-by-time.
    """
    try:
        if not callable(get_tide_events):
            return None
        lines = get_tide_events(now.date().isoformat()) or []
        cands: List[datetime] = []
        for s in lines:
            parts = str(s).split()
            if len(parts) < 2 or ":" not in parts[1]:
                continue
            hh, mm = parts[1].split(":")
            t = now.replace(hour=int(hh), minute=int(mm), second=0, microsecond=0)
            cands.append(t)
        if not cands:
            return None
        return min(cands, key=lambda t: abs((t - now).total_seconds()))
    except Exception:
        return None

def _current_tp_hours() -> float:
    """
    Thời gian giữ lệnh tối đa trước khi TP-by-time (giờ).
    """
    try:
        h = float(os.getenv("TP_TIME_HOURS", "12"))
        return max(0.5, min(48.0, h))
    except Exception:
        return 12.0

# ========= State =========
# Lưu text cuối cùng để /autolog in ra
_last_decision_text: Dict[int, str] = {}
# Chống spam 1 tick trong cùng slot M5
_last_m5_slot_sent: Dict[int, int] = {}
# [DEPRECATED] quota theo cửa sổ thủy triều — đã chuyển sang TideGate
_user_tide_state: Dict[int, Dict[str, Any]] = {}
# Vị thế đang mở (theo UID) để xử lý TP-by-time
_open_pos: Dict[int, Dict[str, Any]] = {}
# [OLD] Mốc thời gian vào lệnh gần nhất (global gap guard)
_LAST_EXEC_TS: Dict[int, float] = {}  # key=uid, val=epoch seconds
# [NEW] States cho patch A & B
_m30_guard_state: Dict[int, Dict[str, Dict[str, Any]]] = {}
_m30_consec_state: Dict[int, Dict[str, Any]] = {}
_last_entry_meta: Dict[int, Dict[str, Any]] = {}

def get_last_decision_text(uid: int) -> Optional[str]:
    return _last_decision_text.get(uid)

# Cho phép /setenv hoặc /preset ghi đè runtime (nếu có API)
def set_runtime_env(kv: Dict[str, str]) -> None:
    _apply_runtime_env(kv)

# ========= Quyết định & vào lệnh =========
@dataclass
class UserState:
    settings: Any

# ==============Hàm (A) Gate/Decision ==================
async def _auto_gate_decision(uid: int, app, storage) -> Optional[dict]:
    """
    (A) Gate/Decision:
    - Toàn bộ các bước gốc của decide_once_for_uid cho AUTO.
    - Nếu skip → return {"ok": False, "reason": msg, "text_block": ...}
    - Nếu pass → return dict chứa mọi dữ liệu cần cho bước B (Hub) và C (Broadcast).
    """
    now = now_vn()

    # 1) Lấy user settings
    try:
        st = storage.get_user(uid)
        pair_disp = st.settings.pair or "BTC/USDT"
        symbol = pair_disp.replace("/", "")
        risk_percent = float(getattr(st.settings, "risk_percent", 10.0))
        leverage = int(float(getattr(st.settings, "leverage", 10)))
        mode = str(getattr(st.settings, "mode", "manual")).lower()
        auto_on = (mode == "auto") or bool(getattr(st.settings, "auto_trade_enabled", False))
        balance_usdt = float(getattr(st.settings, "balance_usdt", 100.0))
        tide_window_hours = float(getattr(st.settings, "tide_window_hours", TIDE_WINDOW_HOURS))
    except Exception:
        # Fallback an toàn
        pair_disp = "BTC/USDT"
        symbol = "BTCUSDT"
        risk_percent = 10.0
        leverage = 10
        mode = "auto"
        auto_on = True
        balance_usdt = 100.0
        tide_window_hours = TIDE_WINDOW_HOURS

    if not auto_on:
        if AUTO_DEBUG and AUTO_DEBUG_VERBOSE:
            await _debug_send(app, uid, _one_line("SKIP", "auto_off", now))
        return {"ok": False, "reason": "auto_off"}

    # === RISK-SENTINEL: chặn auto nếu hôm nay đã bị LOCK ===
    if _rs_is_locked_today(storage, now):
        if AUTO_LOCK_NOTIFY:
            try:
                chat_id = int(AUTO_DEBUG_CHAT_ID) if AUTO_DEBUG_CHAT_ID.isdigit() else uid
                await app.bot.send_message(chat_id=chat_id, text=f"⚠️ Auto LOCKED hôm nay ({_rs_status_today(storage)['day']}). Yêu cầu kiểm tra thủ công.")
            except Exception:
                pass
        return {"ok": False, "reason": "locked_today"}

    # 2) Chỉ xử lý ngay sau khi đóng nến M5
    ts = int(now.timestamp())
    slot = _floor_5m_epoch(ts)
    boundary = slot * 300
    delay = ts - boundary
    if not (0 <= delay <= M5_MAX_DELAY_SEC):
        if AUTO_DEBUG and AUTO_DEBUG_VERBOSE:
            await _debug_send(app, uid, _one_line("SKIP", "not_m5_close", now, f"delay={delay}s"))
        return {"ok": False, "reason": f"not_m5_close delay={delay}"}
    if _last_m5_slot_sent.get(uid) == slot:
        return {"ok": False, "reason": "dup_slot"}
    _last_m5_slot_sent[uid] = slot

    # 3) Evaluate report CHUẨN
    try:
        res = evaluate_signal(pair_disp, tide_window_hours=tide_window_hours, balance_usdt=balance_usdt)
    except TypeError:
        res = evaluate_signal(symbol)  # type: ignore
    except Exception as e:
        await _debug_send(app, uid, _one_line("ERR", "evaluate_signal_error", now, str(e)))
        return {"ok": False, "reason": f"evaluate_signal_error {e}"}

    if not isinstance(res, dict) or not res.get("ok", False):
        reason = (isinstance(res, dict) and (res.get("text") or res.get("reason"))) or "evaluate_signal() failed"
        await _debug_send(app, uid, _one_line("SKIP", "bad_report", now, reason))
        return {"ok": False, "reason": "bad_report"}

    # 4) Rút trích thông tin CHUẨN theo /report
    skip_report  = bool(res.get("skip", True))
    desired_side = str(res.get("signal", "NONE")).upper()
    confidence   = int(res.get("confidence", 0))
    text_block   = (res.get("text") or "").strip()
    frames       = res.get("frames", {}) or {}
    h4           = frames.get("H4", {}) or {}
    m30          = frames.get("M30", {}) or {}
    m5f          = frames.get("M5", {}) or {}

    # 5) Late-window theo mốc thủy triều gần nhất (⚠️ chỉ để hiển thị; chặn sẽ do TideGate T)
    center = _nearest_tide_center(now)
    tau = None
    if isinstance(center, datetime):
        tau = (now - center).total_seconds() / 3600.0
    in_late = (tau is not None) and (ENTRY_LATE_FROM_HRS <= tau <= ENTRY_LATE_TO_HRS)

    # ⚠️ BỎ CHẶN ENTRY_LATE_ONLY Ở A — đã chuyển sang TideGate.
    # if ENTRY_LATE_ONLY and not in_late:
    #     ...

    # === M30 flip-guard & ổn định + consecutive-N ===
    side_m30 = str(m30.get("side", "NONE")).upper()
    if M30_FLIP_GUARD and side_m30 in ("LONG", "SHORT") and isinstance(center, datetime) and (tau is not None):
        stable_sec = int(float(os.getenv("M30_STABLE_MIN_SEC", str(M30_STABLE_MIN_SEC))))
        need_n = max(1, int(float(os.getenv("M30_NEED_CONSEC_N", str(M30_NEED_CONSEC_N)))))
        g_user = _m30_guard_state.setdefault(uid, {})
        g_day  = g_user.setdefault(now.strftime("%Y-%m-%d"), {})
        g      = g_day.setdefault(center.strftime("%H:%M"), {})
        # Trước tâm: ghi nhận và yêu cầu đợi qua tâm
        if tau < 0:
            if "pre_side" not in g:
                g["pre_side"] = side_m30
            msg = _one_line("SKIP", "m30_wait_post_center", now, f"tau={tau:.2f}h pre_side={g['pre_side']}")
            _last_decision_text[uid] = msg + ("\n\n" + text_block if text_block else "")
            if not AUTO_DEBUG_ONLY_WHEN_SKIP:
                await _debug_send(app, uid, msg)
            return {"ok": False, "reason": msg, "text_block": text_block}
        # Sau tâm: yêu cầu ổn định đủ giây
        if "pre_side" not in g:
            g["pre_side"] = side_m30
        if side_m30 != g.get("pre_side"):
            g["flipped"] = True
        if g.get("post_stable_since") is None:
            g["post_stable_since"] = now
        waited = (now - g["post_stable_since"]).total_seconds()
        if waited < max(0, stable_sec):
            msg = _one_line("SKIP", "m30_need_stable_sec", now, f"{waited:.0f}/{stable_sec}s side={side_m30}")
            _last_decision_text[uid] = msg + ("\n\n" + text_block if text_block else "")
            if not AUTO_DEBUG_ONLY_WHEN_SKIP:
                await _debug_send(app, uid, msg)
            return {"ok": False, "reason": msg, "text_block": text_block}
        # Cần N nến M30 liên tiếp
        if need_n > 1:
            stc = _m30_consec_state.setdefault(uid, {"side": None, "count": 0, "last_bar_key": None})
            bar_key = now.strftime("%Y-%m-%d %H") + f":{(now.minute // 30) * 30:02d}"
            if stc["side"] != side_m30:
                stc["side"] = side_m30
                stc["count"] = 1
                stc["last_bar_key"] = bar_key
            else:
                if stc["last_bar_key"] != bar_key:
                    stc["count"] += 1
                    stc["last_bar_key"] = bar_key
            if stc["count"] < need_n:
                msg = _one_line("SKIP", "m30_need_consec_n", now, f"side={side_m30} {stc['count']}/{need_n}")
                _last_decision_text[uid] = msg + ("\n\n" + text_block if text_block else "")
                if not AUTO_DEBUG_ONLY_WHEN_SKIP:
                    await _debug_send(app, uid, msg)
                return {"ok": False, "reason": msg, "text_block": text_block}

    # ⚠️ BỎ QUOTA MỖI CỬA SỔ Ở A — đã chuyển sang TideGate.
    # key_day = now.strftime("%Y-%m-%d")
    # key_win = f"{center.strftime('%H:%M') if center else 'NA'}"
    # st_user = _user_tide_state.setdefault(uid, {})
    # st_day  = st_user.setdefault(key_day, {})
    # st_key  = st_day.setdefault(key_win, {"trade_count": 0})
    # if int(st_key.get("trade_count", 0)) >= MAX_TRADES_PER_WINDOW:
    #     ...

    key_day = now.strftime("%Y-%m-%d")
    key_win = f"{center.strftime('%H:%M') if center else 'NA'}"
    st_key  = {"trade_count": 0}  # giữ cấu trúc để không phá C; đếm thực sẽ do TideGate (sau B)

    # 6) Skip theo /report
    if skip_report:
        msg = _one_line("SKIP", "report_skip", now, text_block.splitlines()[0] if text_block else "")
        _last_decision_text[uid] = msg + "\n\n" + text_block
        if not AUTO_DEBUG_ONLY_WHEN_SKIP:
            await _debug_send(app, uid, msg)
        return {"ok": False, "reason": msg, "text_block": text_block}

    # 7) Không có tín hiệu
    if desired_side not in ("LONG", "SHORT"):
        msg = _one_line("SKIP", "no_signal", now, f"conf={confidence}")
        _last_decision_text[uid] = msg + "\n\n" + text_block
        if not AUTO_DEBUG_ONLY_WHEN_SKIP:
            await _debug_send(app, uid, msg)
        return {"ok": False, "reason": msg, "text_block": text_block}

    # 8) Bắt buộc M5 cùng hướng với M30 (nếu bật)
    if ENFORCE_M5_MATCH_M30:
        if side_m30 not in ("LONG", "SHORT"):
            msg = _one_line("SKIP", "m30_side_none", now, "M30 không có hướng rõ ràng")
            _last_decision_text[uid] = msg + "\n\n" + text_block
            if not AUTO_DEBUG_ONLY_WHEN_SKIP:
                await _debug_send(app, uid, msg)
            return {"ok": False, "reason": msg, "text_block": text_block}
        if desired_side != side_m30:
            msg = _one_line("SKIP", "desired_vs_m30_mismatch", now, f"desired={desired_side} | m30={side_m30}")
            _last_decision_text[uid] = msg + "\n\n" + text_block
            if not AUTO_DEBUG_ONLY_WHEN_SKIP:
                await _debug_send(app, uid, msg)
            return {"ok": False, "reason": msg, "text_block": text_block}

    # 9) (Tùy) Gate M5 lần cuối
    if callable(m5_entry_check):
        gate_side = side_m30 if (ENFORCE_M5_MATCH_M30 and side_m30 in ("LONG", "SHORT")) else desired_side
        ok, reason, m5_meta = m5_entry_check(symbol, gate_side)
        if not ok:
            msg = _one_line("SKIP", "m5_gate_fail", now, f"reason={reason}")
            _last_decision_text[uid] = msg + "\n\n" + text_block
            if not AUTO_DEBUG_ONLY_WHEN_SKIP:
                await _debug_send(app, uid, msg)
            return {"ok": False, "reason": msg, "text_block": text_block}

    # === M5 cooldown theo window + optional second-entry ===
    try:
        gap_min = int(float(os.getenv("M5_MIN_GAP_MIN", os.getenv("ENTRY_SEQ_WINDOW_MIN", "0"))))
    except Exception:
        gap_min = 0
    scoped_to_window = (os.getenv("M5_GAP_SCOPED_TO_WINDOW", "true").strip().lower() in ("1","true","yes","on","y"))
    allow_second = (os.getenv("ALLOW_SECOND_ENTRY", "true").strip().lower() in ("1","true","yes","on","y"))
    try:
        second_retrace_pct = float(os.getenv("M5_SECOND_ENTRY_MIN_RETRACE_PCT", "0.3"))
    except Exception:
        second_retrace_pct = 0.3

    last = _last_entry_meta.get(uid, {})
    last_at = last.get("at")
    last_win = last.get("window")
    same_win = (last_win == key_win)
    under_scope = (same_win if scoped_to_window else True)

    # Cooldown gap
    if gap_min > 0 and last_at is not None and under_scope:
        gap_now = (now - last_at).total_seconds() / 60.0
        if gap_now < gap_min:
            need_m = int(gap_min - gap_now + 0.999)
            note = _one_line("SKIP", "m5_gap_guard", now, f"need≥{gap_min}m, còn≈{need_m}m")
            _last_decision_text[uid] = note + ("\n\n" + text_block if text_block else "")
            if AUTO_DEBUG and not AUTO_DEBUG_ONLY_WHEN_SKIP:
                await _debug_send(app, uid, note)
            return {"ok": False, "reason": note, "text_block": text_block}

    # Second-entry trong cùng window (nếu đã có >=1 lệnh) — CHỈ phục vụ logic phụ (TideGate sẽ đếm quota chính)
    if under_scope and same_win and int(last.get("order_seq", 0)) >= 1:
        if not allow_second:
            msg = _one_line("SKIP", "second_entry_disabled", now, f"win={key_win}")
            _last_decision_text[uid] = msg + "\n\n" + text_block
            if not AUTO_DEBUG_ONLY_WHEN_SKIP:
                await _debug_send(app, uid, msg)
            return {"ok": False, "reason": msg, "text_block": text_block}
        try:
            px_now = float(m5f.get("close") or 0.0) if isinstance(m5f, dict) else 0.0
            last_px = float(last.get("price") or 0.0)
            last_side = str(last.get("side") or desired_side).upper()
            retrace_ok = False
            if last_px > 0 and px_now > 0:
                if last_side == "LONG":
                    retrace_ok = ((last_px - px_now) / last_px * 100.0) >= second_retrace_pct
                else:
                    retrace_ok = ((px_now - last_px) / last_px * 100.0) >= second_retrace_pct
            if not retrace_ok:
                msg = _one_line("SKIP", "second_entry_need_retrace", now, f"need≥{second_retrace_pct}%, last={last_px}, now={px_now}")
                _last_decision_text[uid] = msg + "\n\n" + text_block
                if not AUTO_DEBUG_ONLY_WHEN_SKIP:
                    await _debug_send(app, uid, msg)
                return {"ok": False, "reason": msg, "text_block": text_block}
        except Exception:
            pass

    # Nếu qua được hết → trả bundle đầy đủ cho B & C
    return {
        "ok": True,
        "now": now,
        "pair_disp": pair_disp,
        "symbol": symbol,
        "risk_percent": risk_percent,
        "leverage": leverage,
        "mode": mode,
        "auto_on": auto_on,
        "balance_usdt": balance_usdt,
        "tide_window_hours": tide_window_hours,

        "res": res,
        "skip_report": skip_report,
        "desired_side": desired_side,
        "confidence": confidence,
        "text_block": text_block,
        "frames": frames,
        "h4": h4,
        "m30": m30,
        "m5f": m5f,

        "center": center,
        "tau": tau,
        "in_late": in_late,
        "side_m30": side_m30,

        "key_day": key_day,
        "key_win": key_win,
        "st_key": st_key,
    }

# ================= (B) Execute Hub ===========================
async def _auto_execute_hub(uid: int, app, storage, gate: dict):
    """
    (B) Execute Hub:
    - Thực hiện khớp lệnh qua execute_order_flow.
    - Trả về tất cả dữ liệu cần cho bước (C).
    """
    import os, json
    from datetime import datetime, timedelta

    now          = gate["now"]
    pair_disp    = gate["pair_disp"]
    desired_side = gate["desired_side"]
    h4, m30, m5f = gate["h4"], gate["m30"], gate["m5f"]
    confidence   = gate["confidence"]
    in_late      = gate["in_late"]
    center       = gate["center"]
    risk_percent = gate["risk_percent"]
    leverage     = gate["leverage"]
    text_block   = gate["text_block"]
    st_key       = gate["st_key"]

    opened_real = False
    per_account_logs = []
    exec_result = {}

    # TP-by-time ETA
    try:
        tp_hours = float(os.getenv("TP_TIME_HOURS", "5.5"))
    except Exception:
        tp_hours = 5.5
    center = center or now
    tp_eta = center + timedelta(hours=tp_hours)

    # Nhãn tide (khớp /report: ±TIDE_WINDOW_HOURS quanh anchor/center)
    try:
        tw_hrs = float(os.getenv("TIDE_WINDOW_HOURS", str(TIDE_WINDOW_HOURS)))
    except Exception:
        tw_hrs = TIDE_WINDOW_HOURS
    try:
        if center:
            start_hhmm = (center - timedelta(hours=tw_hrs)).strftime("%H:%M")
            end_hhmm   = (center + timedelta(hours=tw_hrs)).strftime("%H:%M")
            tide_label = f"{start_hhmm}–{end_hhmm}"
        else:
            tide_label = None
    except Exception:
        tide_label = None

    # SL/TP sơ bộ (không nhét 0.0 vào qty_cfg để executor tự tính theo giá live khi cần)
    try:
        ref_close = float(m30.get("close") or h4.get("close"))
    except Exception:
        ref_close = 0.0

    sl_price = None
    tp_price = None
    try:
        if ref_close and ref_close > 0.0:
            sl_price, tp_price = auto_sl_by_leverage(ref_close, desired_side, leverage)
    except Exception:
        sl_price, tp_price = (None, None)

    qty_cfg = {}
    if sl_price and sl_price > 0:
        qty_cfg["sl"] = sl_price
    if tp_price and tp_price > 0:
        qty_cfg["tp"] = tp_price

    risk_cfg = {"risk_percent": float(risk_percent), "leverage": int(leverage)}

    # === ACCOUNTS: bám sát trade_executor (MULTI trước → fallback SINGLE env) ===
    # - MULTI lấy từ settings.ACCOUNTS hoặc ENV ACCOUNTS_JSON (đúng schema: name/exchange/api_key/api_secret/testnet/pair)
    # - KHÔNG lọc theo "default" nữa; execute_order_flow đã đảm bảo chỉ fallback SINGLE khi MULTI = 0 opened.
    accounts_cfg = {"enabled": True, "prefer": "multi", "fallback_single": True}
    accounts_list = []
    try:
        from config import settings as _S
        if isinstance(getattr(_S, "ACCOUNTS", None), list):
            accounts_list = list(_S.ACCOUNTS or [])
    except Exception:
        accounts_list = []

    # merge thêm từ ENV ACCOUNTS_JSON (nếu có)
    j = os.getenv("ACCOUNTS_JSON")
    if j:
        try:
            arr = json.loads(j)
            if isinstance(arr, list):
                accounts_list.extend([a for a in arr if isinstance(a, dict)])
        except Exception:
            pass

    # nếu có danh sách thì truyền thẳng (trade_executor.open_multi_account_orders sẽ dùng trường 'exchange', 'api_key', 'api_secret', ...)
    if accounts_list:
        accounts_cfg["accounts"] = accounts_list

    # Origin (ORDER/MANUAL/AUTO) lấy từ gate để hiển thị/broadcast đúng
    origin = str(gate.get("mode", "AUTO")).strip().upper() or "AUTO"

    meta = {
        "reason": origin,  # thay vì "AUTO_LOOP"
        "score_meta": {"confidence": confidence, "H4": h4.get("score", 0), "M30": m30.get("score", 0)},
        "tide_meta": {
            "center": center.isoformat() if isinstance(center, datetime) else str(center),
            "tide_label": tide_label
        },
        "frames": gate["frames"],
    }

    # === Gọi hub thực thi (MULTI trước → nếu 0 opened mới fallback SINGLE theo ENV) ===
    try:
        opened_real, exec_result = await execute_order_flow(
            app, storage,
            symbol=pair_disp,
            side=desired_side,
            qty_cfg=qty_cfg,
            risk_cfg=risk_cfg,
            accounts_cfg=accounts_cfg,
            meta=meta,
            origin=origin,
        )
    except Exception as e:
        opened_real = False
        exec_result = {"error": str(e), "entry_ids": [], "per_account": {}}

    # Log chi tiết per-account (ưu tiên multi, rồi single)
    try:
        pa = (exec_result.get("per_account") or {})
        mm = pa.get("multi")
        if isinstance(mm, dict):
            multi_opened_any = False
            for acc_name, info in mm.items():
                if not isinstance(info, dict):
                    continue
                if info.get("opened"):
                    multi_opened_any = True
                    per_account_logs.append(f"• {acc_name} | opened | id={info.get('entry_id')}")
                else:
                    per_account_logs.append(f"• {acc_name} | FAILED: {info.get('error','?')}")
            if multi_opened_any:
                pa["single_ignored_because_multi_opened"] = True

        sg = pa.get("single")
        if isinstance(sg, dict) and not pa.get("single_ignored_because_multi_opened"):
            if sg.get("opened"):
                per_account_logs.append(f"• single | opened | id={sg.get('entry_id')}")
            else:
                per_account_logs.append(f"• single | FAILED: {sg.get('error','?')}")

        if pa.get("multi_error"):
            per_account_logs.append(f"• multi_error: {pa['multi_error']}")
        if pa.get("single_error"):
            per_account_logs.append(f"• single_error: {pa['single_error']}")
    except Exception:
        pass

    return {
        "opened_real": opened_real,
        "exec_result": exec_result,
        "sl_price": sl_price,
        "tp_price": tp_price,
        "tp_eta": tp_eta,
        "tide_label": tide_label,
        "per_account_logs": per_account_logs,
        "st_key": st_key,
        "text_block": text_block,
        "desired_side": desired_side,
        "pair_disp": pair_disp,
        "confidence": confidence,
        "h4": h4, "m30": m30, "m5f": m5f,
        "in_late": in_late,
        "now": now,
        "risk_percent": risk_percent,
        "leverage": leverage,
        "center": center,
        "frames": gate["frames"],
        "key_win": gate["key_win"],
    }


#=================(C) Broadcast & Autolog===========================
async def _auto_broadcast_and_log(uid: int, app, storage, result: dict):
    """
    (C) Broadcast + Autolog
    - Gửi boardcast nếu opened_real.
    - Lưu trạng thái, update storage, autolog.
    """
    opened_real      = result["opened_real"]
    exec_result      = result["exec_result"]
    sl_price         = result["sl_price"]
    tp_price         = result["tp_price"]
    tp_eta           = result["tp_eta"]
    tide_label       = result["tide_label"]
    per_account_logs = result["per_account_logs"]
    st_key           = result["st_key"]
    text_block       = result["text_block"]
    desired_side     = result["desired_side"]
    pair_disp        = result["pair_disp"]
    confidence       = result["confidence"]
    h4, m30, m5f     = result["h4"], result["m30"], result["m5f"]
    in_late          = result["in_late"]
    now              = result["now"]
    risk_percent     = result["risk_percent"]
    leverage         = result["leverage"]

    # (An toàn) lấy thêm center/frames/key_win nếu có
    center  = result.get("center")
    frames  = result.get("frames", {})
    key_win = result.get("key_win")

    # [ADD] lấy mode_label từ exec_result.origin (ORDER/MANUAL/AUTO)
    try:
        mode_label = str((exec_result or {}).get("origin", "AUTO")).upper()
        if not mode_label:
            mode_label = "AUTO"
    except Exception:
        mode_label = "AUTO"

    # [ADD] chọn "picked account" để hiển thị (ưu tiên MULTI opened trước, rồi mới đến SINGLE)
    picked = None
    try:
        pa = (exec_result or {}).get("per_account") or {}
        mm = pa.get("multi")
        if isinstance(mm, dict):
            for acc_name, d in mm.items():
                if isinstance(d, dict) and d.get("opened"):
                    tmp = dict(d)
                    tmp["name"] = acc_name if not tmp.get("name") else tmp["name"]
                    if not tmp.get("exchange"):
                        tmp["exchange"] = tmp.get("exid") or "multi"
                    picked = tmp
                    break
        if not picked:
            sg = pa.get("single")
            if isinstance(sg, dict) and (sg.get("opened") or True):  # giữ nguyên hành vi hiển thị single nếu có
                tmp = dict(sg)
                if not tmp.get("name"):
                    tmp["name"] = "single"
                if not tmp.get("exchange"):
                    tmp["exchange"] = tmp.get("exid") or "single"
                picked = tmp
    except Exception:
        picked = None

    # Broadcast executed
    if opened_real:
        # (giữ nguyên preview cho nội bộ nếu cần) + phòng thủ chữ ký
        try:
            center_str = ""
            if center is not None:
                center_str = center.isoformat() if isinstance(center, datetime) else str(center)

            preview_block = render_signal_preview(
                {"signal": desired_side},
                frames,
                {"late": in_late, "tide_label": tide_label},
                {"confidence": confidence},
                {"preset": None},
                {"center": center_str},
                mode_label,  # [MOD] thay "AUTO" bằng mode_label
            )
        except Exception:
            preview_block = ""

        # === Broadcast: dùng _fmt_exec_broadcast giống /order_cmd ===
        try:
            side_label = "LONG" if desired_side == "LONG" else "SHORT"
            pair_clean = pair_disp.replace(":USDT", "")

            # Lấy thông tin từ exec_result (nếu hub trả về)
            single = (exec_result or {}).get("per_account", {}).get("single", {}) if isinstance(exec_result, dict) else {}
            account_name  = single.get("account_name") or single.get("name") or "auto"
            exchange_name = single.get("exchange_name") or single.get("exchange") or single.get("exid") or "auto"
            qty_print     = single.get("qty")
            sl_print      = single.get("sl") if (single.get("sl") is not None) else sl_price
            tp_print      = single.get("tp") if (single.get("tp") is not None) else tp_price

            # [ADD] nếu đã chọn picked (MULTI/SINGLE ưu tiên), dùng picked để override hiển thị
            if isinstance(picked, dict):
                account_name  = picked.get("account_name") or picked.get("name") or account_name
                exchange_name = picked.get("exchange_name") or picked.get("exchange") or picked.get("exid") or exchange_name
                if picked.get("qty") is not None:
                    qty_print = picked.get("qty")
                if picked.get("sl") is not None:
                    sl_print = picked.get("sl")
                if picked.get("tp") is not None:
                    tp_print = picked.get("tp")

            # Entry(SPOT) để hiển thị đẹp
            try:
                entry_spot = _binance_spot_entry(pair_clean)  # nếu helper có sẵn
            except Exception:
                try:
                    entry_spot = float(m30.get("close") or h4.get("close") or 0.0)
                except Exception:
                    entry_spot = None

            btxt = _fmt_exec_broadcast(
                pair=pair_clean,
                side=side_label,
                acc_name=account_name, ex_id=exchange_name,
                lev=leverage, risk=risk_percent, qty=qty_print, entry_spot=entry_spot,
                sl=sl_print, tp=tp_print,
                tide_label=tide_label, mode_label=mode_label,  # [MOD] thay "AUTO" bằng mode_label
                entry_ids=list((exec_result or {}).get("entry_ids") or []),
                tp_time=tp_eta,  # in TP-by-time nếu đang áp dụng
            )
            await _broadcast_html(btxt)
        except Exception:
            pass

    # 11) Lưu trạng thái vị thế để TP-by-time (dùng tp_eta ở trên)
    try:
        tide_window_key = center.strftime("%Y-%m-%dT%H:%M") if isinstance(center, datetime) else str(center)
    except Exception:
        tide_window_key = str(center)

    _open_pos[uid] = {
        "pair": pair_disp,
        "side": desired_side,
        "qty": None if not opened_real else "live",
        "entry_time": now,
        "tide_center": center,
        "tp_deadline": tp_eta,
        "simulation": (not opened_real),
        "sl_price": sl_price,
        "tide_window_key": tide_window_key,
    }

    # (Đếm hiển thị cũ) — giữ state nhẹ để phục vụ cooldown/second-entry; quota thật do TideGate đếm
    order_seq = 0
    if opened_real:
        order_seq = int(_last_entry_meta.get(uid, {}).get("order_seq", 0)) + 1

    # === Đồng bộ storage để /status (today.count, tide_window_trades) ===
    if opened_real:
        try:
            st_persist = storage.get_user(uid)
            # today.count
            try:
                cur = int(getattr(st_persist.today, "count", 0))
                setattr(st_persist.today, "count", cur + 1)
            except Exception:
                pass
            # per-window map
            try:
                twk = tide_window_key
                if not hasattr(st_persist, "tide_window_trades") or not isinstance(st_persist.tide_window_trades, dict):
                    st_persist.tide_window_trades = {}
                st_persist.tide_window_trades[twk] = int(st_persist.tide_window_trades.get(twk, 0)) + 1
            except Exception:
                pass
            storage.put_user(uid, st_persist)
        except Exception:
            pass

    # Lưu meta lần vào để phục vụ cooldown/second-entry
    try:
        px_close = float(m5f.get("close") or 0.0) if isinstance(m5f, dict) else 0.0
        _last_entry_meta[uid] = {
            "at": now if opened_real else _last_entry_meta.get(uid, {}).get("at", None),
            "window": key_win,  # chuỗi HH:MM cửa sổ thủy triều
            "price": px_close if opened_real else _last_entry_meta.get(uid, {}).get("price", 0.0),
            "side": desired_side if opened_real else _last_entry_meta.get(uid, {}).get("side", desired_side),
            "order_seq": order_seq if opened_real else _last_entry_meta.get(uid, {}).get("order_seq", 0),
        }
    except Exception:
        pass

    # 12) Build log /autolog
    try:
        side_m30 = str(m30.get("side", "NONE")).upper()
    except Exception:
        side_m30 = "NONE"

    # [MOD] header dùng mode_label thay vì cố định AUTO
    header = (
        f"🤖 {mode_label} EXECUTE | {pair_disp} {desired_side}\n"
        f"Score H4/M30: {h4.get('score',0)} / {m30.get('score',0)} | Total≈{confidence}\n"
        f"rule M5==M30: {'ON' if ENFORCE_M5_MATCH_M30 else 'OFF'} | m30={side_m30}\n"
        f"late_window={'YES' if in_late else 'NO'} | "
        f"TP-by-time: {tp_eta.strftime('%Y-%m-%d %H:%M:%S')}\n"
        f"➡️ EXECUTE {'OK' if opened_real else 'FAIL'} | {'counted' if opened_real else 'not-counted'}\n"
        f"{chr(10).join(per_account_logs) if per_account_logs else ''}\n"
        "━━━━━━━━━━━━━━━━━━━━━━━\n"
    )
    final_text = header + (text_block or "(no_report_block)")
    _last_decision_text[uid] = final_text

    # Gửi log ra kênh debug
    try:
        chat_id = int(AUTO_DEBUG_CHAT_ID) if str(AUTO_DEBUG_CHAT_ID).isdigit() else uid
    except Exception:
        chat_id = uid
    try:
        await app.bot.send_message(chat_id=chat_id, text=final_text)
    except Exception:
        pass

    return final_text

#================= Mode Auto or Manual trong bot.mode_cmd ===========================
async def decide_once_for_uid(uid: int, app, storage) -> Optional[str]:
    """
    - AUTO  : A -> T -> B -> (bump counters) -> C
    - MANUAL: A -> tạo/kiểm tra pending bằng approval_flow v2;
               nếu APPROVED thì (T -> B -> bump counters -> C);
               nếu REJECTED thì bỏ; nếu PENDING thì chờ.
    """
    # 0) Lấy mode hiện tại
    try:
        st = storage.get_user(uid)
        mode = str(getattr(st.settings, "mode", "manual")).lower()
    except Exception:
        mode = "auto"

    # 1) A: Gate/Decision (luồng chung)
    gate = await _auto_gate_decision(uid, app, storage)
    if not gate or not gate.get("ok"):
        return None if not gate else gate.get("reason")

    # ====== TIDE GATE (T) — Áp dụng cho AUTO ngay sau A ======
    if mode == "auto":
        cfg = await _load_tidegate_config(storage, uid)
        tgr = await tide_gate_check(
            now=now_vn().astimezone(timezone.utc),
            storage=storage,
            cfg=cfg,
            scope_uid=(uid if cfg.counter_scope == "per_user" else None),
        )
        if not tgr.ok:
            if AUTO_DEBUG:
                await _debug_send(app, uid, f"[TideGate BLOCKED] {tgr.reason} {tgr.counters}")
            return f"TIDE_BLOCKED:{tgr.reason}"

        # B
        result = await _auto_execute_hub(uid, app, storage, gate)
        # bump counters sau khi B khớp OK
        try:
            if result and result.get("opened_real"):
                await bump_counters_after_execute(storage, tgr, uid if cfg.counter_scope == "per_user" else None)
        except Exception:
            pass
        # C
        final_text = await _auto_broadcast_and_log(uid, app, storage, result)
        return final_text

    # ===== MANUAL FLOW (dùng approval_flow v2) =====
    # Helper: build payload chuẩn cho create_pending_v2
    def _build_pending_payload_from_gate(g: dict) -> dict:
        # SL/TP gợi ý (để người duyệt xem), giống cách tính ở (B)
        try:
            ref_close = float(g["m30"].get("close") or g["h4"].get("close"))
        except Exception:
            ref_close = 0.0
        try:
            sl_price, tp_price = auto_sl_by_leverage(ref_close, g["desired_side"], g["leverage"])
        except Exception:
            sl_price, tp_price = (None, None)

        qty_cfg = {"sl": sl_price, "tp": tp_price}
        risk_cfg = {"risk_percent": float(g["risk_percent"]), "leverage": int(g["leverage"])}
        accounts_cfg = {"enabled": True}

        boardcard_ctx = {
            "confidence": g["confidence"],
            "frames": g["frames"],
            "late": g["in_late"],
        }
        gates = {
            "enforce_m5_eq_m30": bool(ENFORCE_M5_MATCH_M30),
            "side_m30": g.get("side_m30"),
        }

        return {
            "symbol": g["pair_disp"],
            "suggested_side": g["desired_side"],
            "signal_frames": g["frames"],
            "boardcard_ctx": boardcard_ctx,
            "qty_cfg": qty_cfg,
            "risk_cfg": risk_cfg,
            "accounts_cfg": accounts_cfg,
            "gates": gates,
        }

    # đọc pid gần nhất (nếu có) cho user
    user_pid_key = f"pending_pid:{uid}"
    current_pid = storage.get(user_pid_key)

    # nếu đã có pid → xem trạng thái
    if current_pid:
        rec = get_pending(storage, current_pid)
        if not rec:
            # bị xoá ngoài ý muốn → clear pid để tạo mới
            storage.set(user_pid_key, None)
        else:
            status = rec.status.upper()
            if status == "PENDING":
                # vẫn đang chờ duyệt
                return "MANUAL awaiting approval"
            if status == "REJECTED":
                # user từ chối → đóng và clear pid
                mark_done(storage, rec.pid, "REJECTED")
                storage.set(user_pid_key, None)
                return f"MANUAL rejected id={rec.pid}"
            if status == "APPROVED":
                # ĐÃ DUYỆT → trước khi chạy B phải re-check TideGate (T)
                cfg = await _load_tidegate_config(storage, uid)
                tgr = await tide_gate_check(
                    now=now_vn().astimezone(timezone.utc),
                    storage=storage,
                    cfg=cfg,
                    scope_uid=(uid if cfg.counter_scope == "per_user" else None),
                )
                if not tgr.ok:
                    # không execute, clear pending
                    mark_done(storage, rec.pid, "EXPIRED_TIDE")
                    storage.set(user_pid_key, None)
                    return f"TIDE_BLOCKED:{tgr.reason}"
                # B
                result = await _auto_execute_hub(uid, app, storage, gate)
                if result and result.get("opened_real"):
                    try:
                        await bump_counters_after_execute(storage, tgr, uid if cfg.counter_scope == "per_user" else None)
                    except Exception:
                        pass
                # C
                final_text = await _auto_broadcast_and_log(uid, app, storage, result)
                mark_done(storage, rec.pid, "APPROVED")
                storage.set(user_pid_key, None)
                return final_text
            # trạng thái lạ → clear cho an toàn
            storage.set(user_pid_key, None)

    # chưa có pending cho user → tạo mới
    payload = _build_pending_payload_from_gate(gate)
    rec = create_pending_v2(storage, payload)   # -> ManualPendingRecord(pid=..., status="PENDING")
    storage.set(user_pid_key, rec.pid)

    # gửi thông báo duyệt (để m5report hoặc PM hiển thị ID)
    try:
        pair = gate["pair_disp"]; side = gate["desired_side"]; conf = gate["confidence"]
        msg = (
            f"🟡 <b>MANUAL PENDING</b> | {pair} {side}\n"
            f"• ID: <code>{rec.pid}</code>\n"
            f"• Confidence: {conf}\n"
            f"• /approve {rec.pid} để vào lệnh  |  /reject {rec.pid} để bỏ qua"
        )
        notify_chat_id = getattr(st.settings, "manual_notify_chat_id", None) or uid
        await app.bot.send_message(chat_id=notify_chat_id, text=msg, parse_mode="HTML", disable_web_page_preview=True)
    except Exception:
        pass

    return f"MANUAL PENDING created id={rec.pid}"



# ========= TP-by-time theo mốc thủy triều =========
async def maybe_tp_by_time(uid: int, app, storage) -> Optional[str]:
    if uid not in _open_pos:
        return None

    pos = _open_pos[uid]
    now = now_vn()
    dl = pos.get("tp_deadline")

    # === RISK-SENTINEL: nếu vị thế đã tự đóng trước hạn, kiểm tra xem đó có phải SL không ===
    # Điều kiện: trước hạn TP-by-time nhưng position đã flat (qty=0) -> suy đoán đóng do SL hoặc manual/TP.
    try:
        if callable(ExchangeClient):
            ex = ExchangeClient()
            side_long, qty = await ex.current_position(pos.get("pair","BTC/USDT"))
            if (qty or 0.0) <= 1e-12:
                # Vị thế đã hết. Lấy giá hiện tại để suy đoán.
                last_price = None
                try:
                    ticker = await ex._io(ex.client.fetch_ticker, pos.get("pair","BTC/USDT"))
                    last_price = float(ticker.get("last") or ticker.get("close") or 0.0)
                except Exception:
                    last_price = None

                result = "MANUAL"
                sl_p = pos.get("sl_price")
                side = str(pos.get("side","")).upper()
                if sl_p and last_price:
                    try:
                        if side == "LONG" and last_price <= float(sl_p) * 1.001:
                            result = "SL"
                        elif side == "SHORT" and last_price >= float(sl_p) * 0.999:
                            result = "SL"
                    except Exception:
                        pass

                locked = _rs_on_trade_close(
                    storage=storage,
                    result=result,
                    window_key=pos.get("tide_window_key"),
                    when=now,
                )
                # dọn trạng thái
                _open_pos.pop(uid, None)

                # thông báo nếu khoá
                if locked and AUTO_LOCK_NOTIFY:
                    try:
                        chat_id = int(AUTO_DEBUG_CHAT_ID) if AUTO_DEBUG_CHAT_ID.isdigit() else uid
                        await app.bot.send_message(chat_id=chat_id, text=f"⛔ ĐÃ KHÓA Auto: 2 SL liên tiếp qua 2 lần thủy triều. Auto tạm dừng đến hết ngày {_rs_status_today(storage)['day']}.")
                    except Exception:
                        pass
                return f"AUTO CLOSE detected ({result})"
    except Exception:
        pass

    # cập nhật deadline runtime nếu ENV thay đổi
    base = pos.get("tide_center") or pos.get("entry_time") or now
    try:
        if base.tzinfo is None:
            base = base.replace(tzinfo=now.tzinfo)
    except Exception:
        pass
    pos["tp_deadline"] = base + timedelta(hours=_current_tp_hours())
    dl = pos["tp_deadline"]

    if dl and now >= dl:
        order_msg = "(simulation)"
        if callable(ExchangeClient) and not pos.get("simulation"):
            try:
                ex = ExchangeClient()
                res = await ex.close_position(pos["pair"])
                order_msg = getattr(res, "message", str(res))
            except Exception as e:
                order_msg = f"close_err:{e}"

        # dọn state vị thế
        _open_pos.pop(uid, None)
        msg = f"[TP-BY-TIME] {now.strftime('%Y-%m-%d %H:%M:%S')} | {pos.get('pair')} | {order_msg}"
        try:
            chat_id = int(AUTO_DEBUG_CHAT_ID) if AUTO_DEBUG_CHAT_ID.isdigit() else uid
        except Exception:
            chat_id = uid
        try:
            await app.bot.send_message(chat_id=chat_id, text=msg)
        except Exception:
            pass

        # Risk-sentinel: đánh dấu TP để reset streak
        try:
            _ = _rs_on_trade_close(storage, result="TP", window_key=pos.get("tide_window_key"), when=now)
        except Exception:
            pass
        return msg

    return None

# ========= Vòng lặp nền =========
async def start_auto_loop(app, storage):
    """
    Worker nền: mỗi SCHEDULER_TICK_SEC, tick qua tất cả user đã từng tương tác
    """
    uid_env = os.getenv("TELEGRAM_CHAT_ID", "").strip()
    forced_uid = int(uid_env) if uid_env.isdigit() else 0

    while True:
        # Lấy danh sách UID từ storage (hoặc ép một UID qua env để test)
        uids: List[int] = []
        try:
            data_dict = getattr(storage, "data", {}) or {}
            uids = sorted([int(k) for k in data_dict.keys() if str(k).isdigit()])
        except Exception:
            uids = []
        if forced_uid and forced_uid not in uids:
            uids.append(forced_uid)

        # Tick từng user
        for uid in uids:
            try:
                await decide_once_for_uid(uid, app, storage)
                await maybe_tp_by_time(uid, app, storage)
            except Exception as e:
                if AUTO_DEBUG:
                    await _debug_send(app, uid, f"[AUTO][ERR] {now_vn().strftime('%Y-%m-%d %H:%M:%S')} | exception | {e}")
                continue

        await asyncio.sleep(SCHEDULER_TICK_SEC)
# ----------------------- /core/auto_trade_engine.py -----------------------
