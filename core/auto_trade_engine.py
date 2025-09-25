# ----------------------- core/auto_trade_engine.py -----------------------
from __future__ import annotations

import os
import asyncio
from dataclasses import dataclass
from typing import Optional, Dict, Any, List, Tuple
from datetime import datetime, timedelta, timezone

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
    from tg.formatter import render_executed_boardcard, render_signal_preview
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
# ========= [/ADD] ====================================

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
# ========= [/ADD] ========================================================

# ========= ENV & runtime knobs (có thể đổi bằng /setenv hoặc preset) =========
def _env_bool(key: str, default: str = "false") -> bool:
    return (os.getenv(key, default) or "").strip().lower() in ("1", "true", "yes", "on", "y")

M5_MAX_DELAY_SEC        = int(float(os.getenv("M5_MAX_DELAY_SEC", "60")))
SCHEDULER_TICK_SEC      = int(float(os.getenv("SCHEDULER_TICK_SEC", "2")))
MAX_TRADES_PER_WINDOW   = int(float(os.getenv("MAX_TRADES_PER_WINDOW", "2")))

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
M30_FLIP_GUARD = False              # yêu cầu M30 không flip hướng quá nhanh
M30_STABLE_MIN_SEC = 0              # số giây tối thiểu M30 phải ổn định
M30_NEED_CONSEC_N = 1               # số nến liên tiếp cần thoả điều kiện

M5_MIN_GAP_MIN = 0                  # phút tối thiểu giữa 2 lần vào lệnh (gap guard)
M5_GAP_SCOPED_TO_WINDOW = True      # gap guard tính trong 1 cửa sổ tide hay toàn cục
ALLOW_SECOND_ENTRY = False          # cho phép vào lệnh thứ 2 trong cùng cửa sổ
M5_SECOND_ENTRY_MIN_RETRACE_PCT = 0 # % tối thiểu retrace để cho lệnh thứ 2


def _apply_runtime_env(kv: Dict[str, str]) -> None:
    """
    Cho phép /setenv ghi đè nhanh các ENV trong runtime.
    Đồng bộ lại toàn bộ biến module để auto-loop áp dụng ngay.
    """
    global ENTRY_LATE_ONLY, ENTRY_LATE_FROM_HRS, ENTRY_LATE_TO_HRS
    global AUTO_DEBUG, AUTO_DEBUG_VERBOSE, AUTO_DEBUG_ONLY_WHEN_SKIP, AUTO_DEBUG_CHAT_ID
    global ENFORCE_M5_MATCH_M30, MAX_TRADES_PER_WINDOW
    # Guards / filters mới:
    global M30_FLIP_GUARD, M30_STABLE_MIN_SEC, M30_NEED_CONSEC_N
    global M5_MIN_GAP_MIN, M5_GAP_SCOPED_TO_WINDOW, ALLOW_SECOND_ENTRY, M5_SECOND_ENTRY_MIN_RETRACE_PCT

    for k, v in kv.items():
        os.environ[k] = str(v)

    try:
        # late-window
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

        # quota theo cửa sổ thủy triều
        MAX_TRADES_PER_WINDOW   = int(float(os.getenv("MAX_TRADES_PER_WINDOW",
                                           os.getenv("MAX_ORDERS_PER_TIDE_WINDOW", str(MAX_TRADES_PER_WINDOW)))))

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
# Đếm số lệnh trong một cửa sổ thủy triều (high/low) để giới hạn
_user_tide_state: Dict[int, Dict[str, Any]] = {}
# Vị thế đang mở (theo UID) để xử lý TP-by-time
_open_pos: Dict[int, Dict[str, Any]] = {}
# [ADD] Mốc thời gian vào lệnh gần nhất (gap guard)
_LAST_EXEC_TS: Dict[int, float] = {}  # key=uid, val=epoch seconds

def get_last_decision_text(uid: int) -> Optional[str]:
    return _last_decision_text.get(uid)

# Cho phép /setenv hoặc /preset ghi đè runtime (nếu có API)
def set_runtime_env(kv: Dict[str, str]) -> None:
    _apply_runtime_env(kv)

# ========= Quyết định & vào lệnh =========
@dataclass
class UserState:
    settings: Any

# ================== AUTO: quyết định & thực thi 1 lần cho uid ==================
async def decide_once_for_uid(uid: int, app, storage) -> Optional[str]:
    """
    - Tick theo M5 close (chặn trùng slot)
    - Đồng bộ H4/M30/Moon với /report bằng evaluate_signal()
    - Tôn trọng res['skip'] và res['signal'] (NONE/LONG/SHORT)
    - Kiểm tra late-window theo ENV (ENTRY_LATE_ONLY)
    - (Tùy) M5 gate lần cuối (m5_entry_check) để an toàn
    - Nếu OK: tính qty, SL/TP, mở lệnh (nếu ExchangeClient có sẵn)
    - Lưu full text vào _last_decision_text để /autolog in lại
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
        return None

    # === RISK-SENTINEL: chặn auto nếu hôm nay đã bị LOCK ===
    if _rs_is_locked_today(storage, now):
        if AUTO_LOCK_NOTIFY:
            try:
                chat_id = int(AUTO_DEBUG_CHAT_ID) if AUTO_DEBUG_CHAT_ID.isdigit() else uid
                await app.bot.send_message(chat_id=chat_id, text=f"⚠️ Auto LOCKED hôm nay ({_rs_status_today(storage)['day']}). Yêu cầu kiểm tra thủ công.")
            except Exception:
                pass
        return None

    # 2) Chỉ xử lý ngay sau khi đóng nến M5
    ts = int(now.timestamp())
    slot = _floor_5m_epoch(ts)
    boundary = slot * 300
    delay = ts - boundary
    if not (0 <= delay <= M5_MAX_DELAY_SEC):
        if AUTO_DEBUG and AUTO_DEBUG_VERBOSE:
            await _debug_send(app, uid, _one_line("SKIP", "not_m5_close", now, f"delay={delay}s"))
        return None
    if _last_m5_slot_sent.get(uid) == slot:
        return None
    _last_m5_slot_sent[uid] = slot

    # 3) Evaluate report CHUẨN
    try:
        res = evaluate_signal(pair_disp, tide_window_hours=tide_window_hours, balance_usdt=balance_usdt)
    except TypeError:
        res = evaluate_signal(symbol)  # type: ignore
    except Exception as e:
        await _debug_send(app, uid, _one_line("ERR", "evaluate_signal_error", now, str(e)))
        return None

    if not isinstance(res, dict) or not res.get("ok", False):
        reason = (isinstance(res, dict) and (res.get("text") or res.get("reason"))) or "evaluate_signal() failed"
        await _debug_send(app, uid, _one_line("SKIP", "bad_report", now, reason))
        return None

    # 4) Rút trích thông tin CHUẨN theo /report
    skip_report  = bool(res.get("skip", True))
    desired_side = str(res.get("signal", "NONE")).upper()
    confidence   = int(res.get("confidence", 0))
    text_block   = (res.get("text") or "").strip()
    frames       = res.get("frames", {}) or {}
    h4           = frames.get("H4", {}) or {}
    m30          = frames.get("M30", {}) or {}
    m5f          = frames.get("M5", {}) or {}

    # 5) Late-window theo mốc thủy triều gần nhất
    center = _nearest_tide_center(now)
    tau = None
    if isinstance(center, datetime):
        tau = (now - center).total_seconds() / 3600.0
    in_late = (tau is not None) and (ENTRY_LATE_FROM_HRS <= tau <= ENTRY_LATE_TO_HRS)

    # Guard ENTRY_LATE_ONLY
    if ENTRY_LATE_ONLY and not in_late:
        msg = _one_line("SKIP", "late_only_block", now, f"tau={tau:.2f}h, need {ENTRY_LATE_FROM_HRS}–{ENTRY_LATE_TO_HRS}h")
        _last_decision_text[uid] = msg + ("\n\n" + text_block if text_block else "")
        if not AUTO_DEBUG_ONLY_WHEN_SKIP:
            await _debug_send(app, uid, msg)
        return msg

    # Quota mỗi cửa sổ thủy triều
    key_day = now.strftime("%Y-%m-%d")
    key_win = f"{center.strftime('%H:%M') if center else 'NA'}"
    st_user = _user_tide_state.setdefault(uid, {})
    st_day  = st_user.setdefault(key_day, {})
    st_key  = st_day.setdefault(key_win, {"trade_count": 0})
    if int(st_key.get("trade_count", 0)) >= MAX_TRADES_PER_WINDOW:
        msg = _one_line("SKIP", "reach_trade_limit_window", now, f"win={key_win}")
        _last_decision_text[uid] = msg + "\n\n" + text_block
        if not AUTO_DEBUG_ONLY_WHEN_SKIP:
            await _debug_send(app, uid, msg)
        return msg

    # 6) Skip theo /report
    if skip_report:
        msg = _one_line("SKIP", "report_skip", now, text_block.splitlines()[0] if text_block else "")
        _last_decision_text[uid] = msg + "\n\n" + text_block
        if not AUTO_DEBUG_ONLY_WHEN_SKIP:
            await _debug_send(app, uid, msg)
        return msg

    # 7) Không có tín hiệu
    if desired_side not in ("LONG", "SHORT"):
        msg = _one_line("SKIP", "no_signal", now, f"conf={confidence}")
        _last_decision_text[uid] = msg + "\n\n" + text_block
        if not AUTO_DEBUG_ONLY_WHEN_SKIP:
            await _debug_send(app, uid, msg)
        return msg

    # 8) Bắt buộc M5 cùng hướng với M30 (nếu bật)
    side_m30 = str(m30.get("side", "NONE")).upper()
    if ENFORCE_M5_MATCH_M30:
        if side_m30 not in ("LONG", "SHORT"):
            msg = _one_line("SKIP", "m30_side_none", now, "M30 không có hướng rõ ràng")
            _last_decision_text[uid] = msg + "\n\n" + text_block
            if not AUTO_DEBUG_ONLY_WHEN_SKIP:
                await _debug_send(app, uid, msg)
            return msg
        if desired_side != side_m30:
            msg = _one_line("SKIP", "desired_vs_m30_mismatch", now, f"desired={desired_side} | m30={side_m30}")
            _last_decision_text[uid] = msg + "\n\n" + text_block
            if not AUTO_DEBUG_ONLY_WHEN_SKIP:
                await _debug_send(app, uid, msg)
            return msg

    # 9) (Tùy) Gate M5 lần cuối
    if callable(m5_entry_check):
        gate_side = side_m30 if (ENFORCE_M5_MATCH_M30 and side_m30 in ("LONG", "SHORT")) else desired_side
        ok, reason, m5_meta = m5_entry_check(symbol, gate_side)
        if not ok:
            msg = _one_line("SKIP", "m5_gate_fail", now, f"reason={reason}")
            _last_decision_text[uid] = msg + "\n\n" + text_block
            if not AUTO_DEBUG_ONLY_WHEN_SKIP:
                await _debug_send(app, uid, msg)
            return msg

    # M5 gap guard (nếu cấu hình)
    try:
        gap_min = int(float(os.getenv("M5_MIN_GAP_MIN", os.getenv("ENTRY_SEQ_WINDOW_MIN", "0"))))
    except Exception:
        gap_min = 0
    if gap_min > 0:
        import time
        now_sec = time.time()
        last = _LAST_EXEC_TS.get(uid)
        if last and (now_sec - last) < gap_min * 60:
            need_m = int(gap_min - (now_sec - last) / 60.0 + 0.999)
            note = _one_line("SKIP", "m5_gap_guard", now, f"need≥{gap_min}m, còn≈{need_m}m")
            _last_decision_text[uid] = note + ("\n\n" + text_block if text_block else "")
            if AUTO_DEBUG and not AUTO_DEBUG_ONLY_WHEN_SKIP:
                await _debug_send(app, uid, note)
            return note
        from time import time as _now_s
        _LAST_EXEC_TS[uid] = _now_s()

    # 10) Khớp lệnh — DÙNG HUB + FORMATTER THỐNG NHẤT (minimal diff)
    opened_real = False
    per_account_logs = []
    exec_board_txt = None  # text boardcard EXECUTED

    # TP-by-time ETA (mặc định 5.5h nếu không set ENV)
    try:
        tp_hours = float(os.getenv("TP_TIME_HOURS", "5.5"))
    except Exception:
        tp_hours = 5.5
    center = center or now
    tp_eta = center + timedelta(hours=tp_hours)

    # Nhãn tide hh:mm–hh:mm để chèn vào boardcard preview (nếu cần)
    try:
        tw_hrs = float(os.getenv("TIDE_WINDOW_HOURS", str(tide_window_hours)))
    except Exception:
        tw_hrs = tide_window_hours
    try:
        start_hhmm = (center - timedelta(hours=tw_hrs/2)).strftime("%H:%M")
        end_hhmm   = (center + timedelta(hours=tw_hrs/2)).strftime("%H:%M")
        tide_label = f"{start_hhmm}–{end_hhmm}"
    except Exception:
        tide_label = None

    # (1) Tính sơ bộ SL/TP để lưu vào pos & cung cấp cho hub (hub vẫn có thể tự tính nếu thiếu)
    try:
        try:
            ref_close = float(m30.get("close") or h4.get("close"))
        except Exception:
            ref_close = 0.0
        if (auto_sl_by_leverage is None) or (ref_close <= 0):
            raise RuntimeError("no auto_sl_by_leverage or bad ref_close")
        sl_price, tp_price = auto_sl_by_leverage(ref_close, desired_side, leverage)
    except Exception:
        sl_price, tp_price = (None, None)

    # (2) Chuẩn bị cấu hình cho hub
    qty_cfg = {"sl": sl_price, "tp": tp_price}
    risk_cfg = {"risk_percent": float(risk_percent), "leverage": int(leverage)}
    accounts_cfg = {"enabled": True}
    meta = {
        "reason": "AUTO_LOOP",
        "score_meta": {"confidence": confidence, "H4": h4.get("score", 0), "M30": m30.get("score", 0)},
        "tide_meta": {"center": center.isoformat() if isinstance(center, datetime) else str(center), "tide_label": tide_label},
        "frames": frames,
    }

    # (3) GỌI HUB
    try:
        opened_real, exec_result = await execute_order_flow(
            app, storage,
            symbol=pair_disp,
            side=desired_side,
            qty_cfg=qty_cfg,
            risk_cfg=risk_cfg,
            accounts_cfg=accounts_cfg,
            meta=meta,
            origin="AUTO",
        )
    except Exception as _e_hub:
        opened_real = False
        exec_result = {"error": str(_e_hub), "entry_ids": [], "per_account": {}}

    # (4) Build log per-account (phục vụ /autolog)
    try:
        pa = exec_result.get("per_account", {}) or {}
        for name, info in pa.items():
            if isinstance(info, dict):
                if info.get("opened"):
                    per_account_logs.append(f"• {name} | opened | id={info.get('entry_id')}")
                else:
                    per_account_logs.append(f"• {name} | FAILED: {info.get('error','?')}")
    except Exception:
        pass

    # (5) BROADCAST EXECUTED (format GIỐNG /order_cmd)
    if opened_real:
        # (giữ nguyên preview cho nội bộ nếu cần)
        try:
            preview_block = render_signal_preview(
                {"signal": desired_side}, frames, {"late": in_late, "tide_label": tide_label},
                {"confidence": confidence},
                {"preset": None},
                {"center": center.isoformat() if isinstance(center, datetime) else str(center)},
                "AUTO",
            )
        except Exception:
            preview_block = ""

        # === Thay broadcast: dùng _fmt_exec_broadcast giống /order_cmd ===
        try:
            side_label = "LONG" if desired_side == "LONG" else "SHORT"
            pair_clean = pair_disp.replace(":USDT","")

            # Lấy thông tin từ exec_result (nếu hub trả về)
            single = (exec_result or {}).get("per_account", {}).get("single", {}) if isinstance(exec_result, dict) else {}
            account_name  = single.get("account_name") or single.get("name") or "auto"
            exchange_name = single.get("exchange_name") or single.get("exchange") or single.get("exid") or "auto"
            qty_print = single.get("qty")
            sl_print  = single.get("sl") if single.get("sl") is not None else sl_price
            tp_print  = single.get("tp") if single.get("tp") is not None else tp_price

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
                tide_label=tide_label, mode_label="AUTO",
                entry_ids=list((exec_result or {}).get("entry_ids") or []),
                tp_time=tp_eta,  # in TP-by-time nếu đang áp dụng
            )
            await _broadcast_html(btxt)
        except Exception:
            pass

    # 11) Lưu trạng thái vị thế để TP-by-time (dùng tp_eta ở trên)
    try:
        tide_window_key = center.strftime("%Y-%m-%dT%H:%M")
    except Exception:
        tide_window_key = str(center)

    _open_pos[uid] = {
        "pair": pair_disp, "side": desired_side, "qty": None if not opened_real else "live",
        "entry_time": now, "tide_center": center, "tp_deadline": tp_eta, "simulation": (not opened_real),
        "sl_price": (sl_price if 'sl_price' in locals() else None),
        "tide_window_key": tide_window_key,
    }

    # Chỉ tăng trade_count khi opened_real
    if opened_real:
        st_key["trade_count"] = int(st_key.get("trade_count", 0)) + 1
    order_seq = int(st_key.get("trade_count", 0))

    # 12) Build log /autolog
    header = (
        f"🤖 AUTO EXECUTE | {pair_disp} {desired_side}\n"
        f"Score H4/M30: {h4.get('score',0)} / {m30.get('score',0)} | Total≈{confidence}\n"
        f"rule M5==M30: {'ON' if ENFORCE_M5_MATCH_M30 else 'OFF'} | m30={side_m30}\n"
        f"late_window={'YES' if in_late else 'NO'} | "
        f"TP-by-time: {tp_eta.strftime('%Y-%m-%d %H:%M:%S')}\n"
        f"➡️ EXECUTE {'OK' if opened_real else 'FAIL'} | {'counted' if opened_real else 'not-counted'}\n"
        f"{'\n'.join(per_account_logs) if per_account_logs else ''}\n"
        f"{'📣 Opened trade #' + str(order_seq) + '/' + str(MAX_TRADES_PER_WINDOW) if opened_real else ''}\n"
        "━━━━━━━━━━━━━━━━━━━━━━━\n"
    )
    final_text = header + (text_block or "(no_report_block)")
    _last_decision_text[uid] = final_text

    # Gửi log ra kênh debug
    try:
        chat_id = int(AUTO_DEBUG_CHAT_ID) if AUTO_DEBUG_CHAT_ID.isdigit() else uid
    except Exception:
        chat_id = uid
    try:
        await app.bot.send_message(chat_id=chat_id, text=final_text)
    except Exception:
        pass

    return final_text


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

# (Gỡ bỏ helper place_order_with_retry dùng market_with_sl_tp ở bản cũ;
# nếu anh vẫn muốn retry với co-qty, có thể viết bản mới gọi ex.open_market(...).)
