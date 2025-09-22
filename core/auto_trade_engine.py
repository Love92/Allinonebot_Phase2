# ----------------------- core/auto_trade_engine.py -----------------------
from __future__ import annotations

import os
import asyncio
from dataclasses import dataclass
from typing import Optional, Dict, Any, List, Tuple
from datetime import datetime, timedelta, timezone
# ==== [ADD] Helpers: bool env, broadcast & formatter ====
import html as _html
from typing import Optional as _Opt
from telegram import Bot as _TgBot

def _env_bool_rt(name: str, default: bool = False) -> bool:
    v = os.getenv(name)
    if v is None:
        return default
    return str(v).strip().lower() in ("1","true","yes","on","y")

_TELEGRAM_BROADCAST_BOT_TOKEN = (os.getenv("TELEGRAM_BROADCAST_BOT_TOKEN") or "").strip()
_TELEGRAM_BROADCAST_CHAT_ID   = (os.getenv("TELEGRAM_BROADCAST_CHAT_ID") or "").strip()
__bcast_bot: _Opt[_TgBot] = None
if _TELEGRAM_BROADCAST_BOT_TOKEN:
    try:
        __bcast_bot = _TgBot(token=_TELEGRAM_BROADCAST_BOT_TOKEN)
    except Exception:
        __bcast_bot = None

def _esc_html(s: object) -> str:
    try:
        return _html.escape(str(s or ""), quote=False)
    except Exception:
        return str(s)

async def _broadcast_html(text: str) -> None:
    if not (__bcast_bot and _TELEGRAM_BROADCAST_CHAT_ID):
        return
    try:
        await __bcast_bot.send_message(
            chat_id=int(_TELEGRAM_BROADCAST_CHAT_ID),
            text=text,
            parse_mode="HTML",
            disable_web_page_preview=True,
        )
    except Exception:
        pass

def _fmt_exec_broadcast(
    *, pair: str, side: str, acc_name: str, ex_id: str,
    lev: int, risk: float, qty: float, entry_spot: float,
    sl: float | None, tp: float | None,
    tide_label: str | None = None, mode_label: str = "AUTO",
) -> str:
    lines = [
        f"🚀 <b>EXECUTED</b> | <b>{_esc_html(pair)}</b> <b>{_esc_html(side.upper())}</b>",
        f"• Mode: {mode_label}",
        f"• Account: {_esc_html(acc_name)} ({_esc_html(ex_id)})",
        f"• Risk {risk:.1f}% | Lev x{lev}",
        f"• Entry(SPOT)≈{entry_spot:.2f} | Qty={qty:.6f}",
        f"• SL={sl:.2f}" if sl else "• SL=—",
        f"• TP={tp:.2f}" if tp else "• TP=—",
        f"• Tide: {tide_label}" if tide_label else "",
    ]
    return "\n".join([l for l in lines if l])
# ==== [END ADD] ====


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
    # Các tham số khác có thể đã khai báo ở trên file:
    # (TP_TIME_HOURS, M5_WICK_PCT, M5_VOL_MULT_RELAX/STRICT, EXTREME_* ...)
    # Dùng os.getenv trực tiếp để tránh thiếu biến global

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

        # (Các biến khác như EXTREME_* hoặc TP_TIME_HOURS, … dùng trực tiếp os.getenv ở nơi tiêu thụ
        # để tránh phải khai báo global hết tại đây.)
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

def get_last_decision_text(uid: int) -> Optional[str]:
    return _last_decision_text.get(uid)

# Cho phép /setenv hoặc /preset ghi đè runtime (nếu có API)
def set_runtime_env(kv: Dict[str, str]) -> None:
    _apply_runtime_env(kv)

# ========= Quyết định & vào lệnh =========
@dataclass
class UserState:
    settings: Any

async def decide_once_for_uid(uid: int, app, storage) -> Optional[str]:
    """
    - Tick theo M5 close (chặn trùng slot)
    - Đồng bộ H4/M30/Moon với /report bằng evaluate_signal()
    - Tôn trọng res['skip'] và res['signal'] (NONE/LONG/SHORT)
    - Kiểm tra late-window theo ENV
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
        if AUTO_DEBUG and AUTO_DEBUG_VERBOSE:  # noqa: E713 (đảm bảo không văng nếu flake8)
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
        # fallback nếu signature khác
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
    # ==== [ADD] Guard: ENTRY_LATE_ONLY ====
    ENTRY_LATE_ONLY_RT = _env_bool_rt("ENTRY_LATE_ONLY", False)
    ENTRY_LATE_FROM_HRS_RT = float(os.getenv("ENTRY_LATE_FROM_HRS", str(ENTRY_LATE_FROM_HRS)))
    ENTRY_LATE_TO_HRS_RT   = float(os.getenv("ENTRY_LATE_TO_HRS",   str(ENTRY_LATE_TO_HRS)))
    if ENTRY_LATE_ONLY_RT and not in_late:
        msg = _one_line(
            "SKIP", "late_only_block", now,
            f"tau={tau:.2f}h, need {ENTRY_LATE_FROM_HRS_RT}–{ENTRY_LATE_TO_HRS_RT}h"
        )
        _last_decision_text[uid] = msg + "\n\n" + (text_block or "")
        if not AUTO_DEBUG_ONLY_WHEN_SKIP:
            await _debug_send(app, uid, msg)
        return msg
    # ==== [END ADD] ====


    # Mỗi cửa sổ thủy triều chỉ cho 1 số lệnh nhất định
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

    # 8-NEW) Bắt buộc M5 cùng hướng với M30 (nếu bật ENFORCE_M5_MATCH_M30) ,Tắt: /setenv ENFORCE_M5_MATCH_M30 false
    side_m30 = str(m30.get("side", "NONE")).upper()
    if ENFORCE_M5_MATCH_M30:
        if side_m30 not in ("LONG", "SHORT"):
            msg = _one_line("SKIP", "m30_side_none", now, "M30 không có hướng rõ ràng")
            _last_decision_text[uid] = msg + "\n\n" + text_block
            if not AUTO_DEBUG_ONLY_WHEN_SKIP:
                await _debug_send(app, uid, msg)
            return msg

        if desired_side != side_m30:
            msg = _one_line(
                "SKIP", "desired_vs_m30_mismatch",
                now, f"desired={desired_side} | m30={side_m30}"
            )
            _last_decision_text[uid] = msg + "\n\n" + text_block
            if not AUTO_DEBUG_ONLY_WHEN_SKIP:
                await _debug_send(app, uid, msg)
            return msg

    # 9) (Tùy) Gate M5 lần cuối (vẫn an toàn nếu strategy đã gate)
    if callable(m5_entry_check):
        # Dùng hướng M30 để gate M5 (đảm bảo M5==M30) khi bật rule;
        # nếu tắt rule, dùng desired_side như cũ.
        gate_side = side_m30 if (ENFORCE_M5_MATCH_M30 and side_m30 in ("LONG", "SHORT")) else desired_side
        ok, reason, m5_meta = m5_entry_check(symbol, gate_side)
        if not ok:
            msg = _one_line("SKIP", "m5_gate_fail", now, f"reason={reason}")
            _last_decision_text[uid] = msg + "\n\n" + text_block
            if not AUTO_DEBUG_ONLY_WHEN_SKIP:
                await _debug_send(app, uid, msg)
            return msg

    # 10) Khớp lệnh (nếu có ExchangeClient). Nếu không, chạy mô phỏng và log.
    opened_real = False
    order_msg = "(simulation)"
    try:
        if callable(ExchangeClient) and callable(calc_qty) and callable(auto_sl_by_leverage):
            ex = ExchangeClient()
            # lấy giá gần nhất qua market_data nếu client không support — nhưng đơn giản tính qty theo close hiện tại của sàn
            bal = await ex.balance_usdt()
            # Tạm dùng giá ước lượng từ frames M30/H4 nếu có close; nếu không, client sẽ lấy
            close_price = None
            try:
                close_price = float(m30.get("close") or h4.get("close"))  # strategy có thể không đưa close vào frames
            except Exception:
                close_price = None

            if close_price is None:
                # fallback lấy giá qua client nếu có
                try:
                    ticker = await ex._io(ex.client.fetch_ticker, pair_disp)
                    close_price = float(ticker.get("last") or ticker.get("close") or 0.0)
                except Exception:
                    close_price = 0.0

            qty = calc_qty(bal, risk_percent, leverage, close_price or 0.0)
            # (anh có thể thêm SIZE_MULT theo preset hoặc theo score nếu muốn)
            SIZE_MULT = float(os.getenv("SIZE_MULT", "1.0"))
            qty *= SIZE_MULT

            sl_price, tp_price = auto_sl_by_leverage(close_price or 0.0, desired_side, leverage)
            side_long = (desired_side == "LONG")
            await ex.set_leverage(pair_disp, leverage)
            res = await ex.market_with_sl_tp(pair_disp, side_long, qty, sl_price, tp_price)
            order_msg = getattr(res, "message", str(res))
            opened_real = True
    except Exception as e:
        order_msg = f"place_order_error:{e}"

    # 11) Lưu trạng thái vị thế để TP-by-time
    center = center or now  # tránh None
    tp_eta = center + timedelta(hours=_current_tp_hours())

    # Risk-sentinel window key: định danh lần thủy triều gần nhất tại thời điểm vào lệnh
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

    st_key["trade_count"] = int(st_key.get("trade_count", 0)) + 1
    order_seq = st_key["trade_count"]

    # 12) Build log /autolog: GHIM y nguyên block từ /report để nhìn giống hệt + rule hiển thị
    header = (
        f"🤖 AUTO EXECUTE | {pair_disp} {desired_side}\n"
        f"Score H4/M30: {h4.get('score',0)} / {m30.get('score',0)} | Total≈{confidence}\n"
        f"rule M5==M30: {'ON' if ENFORCE_M5_MATCH_M30 else 'OFF'} | m30={side_m30}\n"
        f"late_window={'YES' if in_late else 'NO'} | "
        f"TP-by-time: {tp_eta.strftime('%Y-%m-%d %H:%M:%S')}\n"
        f"➡️ EXECUTED: {desired_side} {pair_disp} | {order_msg}\n"
        f"📣 Opened trade #{order_seq}/{MAX_TRADES_PER_WINDOW}\n"
        "━━━━━━━━━━━━━━━━━━━━━━━\n"
    )
    final_text = header + (text_block or "(no_report_block)")
    _last_decision_text[uid] = final_text

    # Gửi log ra kênh debug (hoặc user nếu không set)
    try:
        chat_id = int(AUTO_DEBUG_CHAT_ID) if AUTO_DEBUG_CHAT_ID.isdigit() else uid
    except Exception:
        chat_id = uid
    try:
        await app.bot.send_message(chat_id=chat_id, text=final_text)

    # ==== [ADD] Broadcast EXECUTED for AUTO ====
    try:
        if opened_real:
            tw_hrs = float(os.getenv("TIDE_WINDOW_HOURS", str(tide_window_hours if 'tide_window_hours' in locals() else 2.5)))
            _start = (center - timedelta(hours=tw_hrs/2)).strftime("%H:%M") if 'center' in locals() and isinstance(center, datetime) else ""
            _end   = (center + timedelta(hours=tw_hrs/2)).strftime("%H:%M") if 'center' in locals() and isinstance(center, datetime) else ""
            tide_label = f"{_start}–{_end}" if _start and _end else None
            entry_spot = float((locals().get('close_price') or 0.0))
            acc_name = "default"
            ex_id = "binanceusdm"
            try:
                if 'ex' in locals() and getattr(ex, "exchange_id", None):
                    ex_id = ex.exchange_id  # type: ignore
            except Exception:
                pass
            btxt = _fmt_exec_broadcast(
                pair=pair_disp,
                side=desired_side,
                acc_name=acc_name,
                ex_id=ex_id,
                lev=int(leverage if 'leverage' in locals() else 1),
                risk=float(risk_percent if 'risk_percent' in locals() else 0.0),
                qty=float(qty if 'qty' in locals() else 0.0),
                entry_spot=entry_spot,
                sl=(float(sl_price) if ('sl_price' in locals() and sl_price is not None) else None),
                tp=(float(tp_price) if ('tp_price' in locals() and tp_price is not None) else None),
                tide_label=tide_label,
                mode_label="AUTO",
            )
            await _broadcast_html(btxt)
    except Exception:
        pass
    # ==== [END ADD] ====
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

async def place_order_with_retry(ex, pair: str, is_long: bool, qty: float, sl: float, tp: float):
    """Đặt market + SL/TP và tự co-qty nếu gặp limit sàn. Retry tối đa 2 lần."""
    # Round theo lot step nếu client có
    try:
        if hasattr(ex, "round_qty"):
            qty = ex.round_qty(pair, qty)
    except Exception:
        pass

    cap_phrases = (
        "Exceeded the maximum",           # Binance
        "maximum position value",         # BingX
        "Position size exceeds",
        "POSITION_SIZE_LIMIT",
    )
    tries = 0
    while True:
        res = await ex.market_with_sl_tp(pair, is_long, qty, sl, tp)
        ok  = bool(getattr(res, "ok", False))
        msg = str(getattr(res, "message", ""))
        if ok:
            return res
        if tries >= 2:
            return res
        if any(p.lower() in msg.lower() for p in cap_phrases):
            qty = max(qty * 0.85, 0.0001)
            try:
                if hasattr(ex, "round_qty"):
                    qty = ex.round_qty(pair, qty)
            except Exception:
                pass
            tries += 1
            continue
        return res
