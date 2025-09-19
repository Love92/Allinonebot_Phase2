# ----------------------- core/auto_trade_engine.py -----------------------
from __future__ import annotations

import os
import asyncio
from dataclasses import dataclass
from typing import Optional, Dict, Any, List
from datetime import datetime, timedelta, timezone

# ========= Imports đồng bộ với /report =========
try:
    # evaluate_signal trả về dict: {ok, skip, signal, confidence, text, frames={H4,M30,M5}}
    from strategy.signal_generator import evaluate_signal
except Exception:
    from signal_generator import evaluate_signal  # type: ignore

# Moon/tide helpers (để log late-window, TP-by-time nếu có)
try:
    from data.moon_tide import get_tide_events
except Exception:
    get_tide_events = None  # type: ignore

# Tham số cửa sổ thủy triều mặc định (có thể đổi qua /tidewindow hoặc /setenv)
try:
    from config.settings import TIDE_WINDOW_HOURS
except Exception:
    TIDE_WINDOW_HOURS = float(os.getenv("TIDE_WINDOW_HOURS", "2.5"))

# M5 gate cuối (tùy chọn)
try:
    from strategy.m5_strategy import m5_entry_check
except Exception:
    m5_entry_check = None  # type: ignore

# Exchange/Executor nếu có kết nối sàn
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
except Exception:
    VN_TZ = timezone(timedelta(hours=7))

def now_vn() -> datetime:
    try:
        return datetime.now(VN_TZ)
    except Exception:
        return datetime.utcnow() + timedelta(hours=7)

# ========= Helpers =========
def _floor_5m_epoch(ts: int) -> int:
    return ts // 300

def _one_line(tag: str, reason: str, now: datetime, extra: str = "") -> str:
    t = now.strftime("%Y-%m-%d %H:%M:%S")
    return f"[{tag}] {t} | {reason}{(' | ' + extra) if extra else ''}"

async def _debug_send(app, uid: int, text: str) -> None:
    try:
        await app.bot.send_message(chat_id=uid, text=text)
    except Exception:
        pass

def _env_bool(key: str, default: str = "false") -> bool:
    return (os.getenv(key, default) or "").strip().lower() in ("1", "true", "yes", "on", "y")

# ========= ENV & runtime =========
M5_MAX_DELAY_SEC        = int(float(os.getenv("M5_MAX_DELAY_SEC", "60")))
SCHEDULER_TICK_SEC      = int(float(os.getenv("SCHEDULER_TICK_SEC", "2")))
MAX_TRADES_PER_WINDOW   = int(float(os.getenv("MAX_TRADES_PER_WINDOW",
                                os.getenv("MAX_ORDERS_PER_TIDE_WINDOW", "2"))))
ENTRY_LATE_ONLY         = _env_bool("ENTRY_LATE_ONLY", "true")
ENTRY_LATE_FROM_HRS     = float(os.getenv("ENTRY_LATE_FROM_HRS", "0.5"))
ENTRY_LATE_TO_HRS       = float(os.getenv("ENTRY_LATE_TO_HRS", "2.5"))

AUTO_DEBUG              = _env_bool("AUTO_DEBUG", "true")
AUTO_DEBUG_VERBOSE      = _env_bool("AUTO_DEBUG_VERBOSE", "false")
AUTO_DEBUG_ONLY_WHEN_SKIP = _env_bool("AUTO_DEBUG_ONLY_WHEN_SKIP", "false")
AUTO_DEBUG_CHAT_ID      = os.getenv("AUTO_DEBUG_CHAT_ID", "").strip()

ENFORCE_M5_MATCH_M30    = _env_bool("ENFORCE_M5_MATCH_M30", "true")

# Guard chuyển hướng M30 quanh tâm thủy triều
M30_FLIP_GUARD          = _env_bool("M30_FLIP_GUARD", "true")
M30_STABLE_MIN_SEC      = int(float(os.getenv("M30_STABLE_MIN_SEC", "1800")))

# Yêu cầu M30 có N nến liên tiếp cùng hướng trước khi cho M5 vào
M30_NEED_CONSEC_N       = int(float(os.getenv("M30_NEED_CONSEC_N", "2")))

# Chống “hai lệnh M5 liên tiếp 5 phút”
M5_MIN_GAP_MIN          = int(float(os.getenv("M5_MIN_GAP_MIN", "15")))
M5_GAP_SCOPED_TO_WINDOW = _env_bool("M5_GAP_SCOPED_TO_WINDOW", "true")
ALLOW_SECOND_ENTRY      = _env_bool("ALLOW_SECOND_ENTRY", "true")
M5_SECOND_ENTRY_MIN_RETRACE_PCT = float(os.getenv("M5_SECOND_ENTRY_MIN_RETRACE_PCT", "0.3"))

def _apply_runtime_env(kv: Dict[str, str]) -> None:
    global ENTRY_LATE_ONLY, ENTRY_LATE_FROM_HRS, ENTRY_LATE_TO_HRS
    global AUTO_DEBUG, AUTO_DEBUG_VERBOSE, AUTO_DEBUG_ONLY_WHEN_SKIP, AUTO_DEBUG_CHAT_ID
    global ENFORCE_M5_MATCH_M30, M30_FLIP_GUARD, M30_STABLE_MIN_SEC, MAX_TRADES_PER_WINDOW
    global M30_NEED_CONSEC_N, M5_MIN_GAP_MIN, M5_GAP_SCOPED_TO_WINDOW, ALLOW_SECOND_ENTRY, M5_SECOND_ENTRY_MIN_RETRACE_PCT
    for k, v in kv.items():
        os.environ[k] = str(v)
    try:
        ENTRY_LATE_ONLY         = _env_bool("ENTRY_LATE_ONLY", "true" if ENTRY_LATE_ONLY else "false")
        ENTRY_LATE_FROM_HRS     = float(os.getenv("ENTRY_LATE_FROM_HRS", str(ENTRY_LATE_FROM_HRS)))
        ENTRY_LATE_TO_HRS       = float(os.getenv("ENTRY_LATE_TO_HRS", str(ENTRY_LATE_TO_HRS)))
        AUTO_DEBUG              = _env_bool("AUTO_DEBUG", "true" if AUTO_DEBUG else "false")
        AUTO_DEBUG_VERBOSE      = _env_bool("AUTO_DEBUG_VERBOSE", "true" if AUTO_DEBUG_VERBOSE else "false")
        AUTO_DEBUG_ONLY_WHEN_SKIP = _env_bool("AUTO_DEBUG_ONLY_WHEN_SKIP", "true" if AUTO_DEBUG_ONLY_WHEN_SKIP else "false")
        AUTO_DEBUG_CHAT_ID      = os.getenv("AUTO_DEBUG_CHAT_ID", AUTO_DEBUG_CHAT_ID)
        ENFORCE_M5_MATCH_M30    = _env_bool("ENFORCE_M5_MATCH_M30", "true" if ENFORCE_M5_MATCH_M30 else "false")
        M30_FLIP_GUARD          = _env_bool("M30_FLIP_GUARD", "true" if M30_FLIP_GUARD else "false")
        M30_STABLE_MIN_SEC      = int(float(os.getenv("M30_STABLE_MIN_SEC", str(M30_STABLE_MIN_SEC))))
        MAX_TRADES_PER_WINDOW   = int(float(os.getenv("MAX_TRADES_PER_WINDOW",
                                        os.getenv("MAX_ORDERS_PER_TIDE_WINDOW", str(MAX_TRADES_PER_WINDOW)))))
        M30_NEED_CONSEC_N       = int(float(os.getenv("M30_NEED_CONSEC_N", str(M30_NEED_CONSEC_N))))
        M5_MIN_GAP_MIN          = int(float(os.getenv("M5_MIN_GAP_MIN", str(M5_MIN_GAP_MIN))))
        M5_GAP_SCOPED_TO_WINDOW = _env_bool("M5_GAP_SCOPED_TO_WINDOW", "true" if M5_GAP_SCOPED_TO_WINDOW else "false")
        ALLOW_SECOND_ENTRY      = _env_bool("ALLOW_SECOND_ENTRY", "true" if ALLOW_SECOND_ENTRY else "false")
        M5_SECOND_ENTRY_MIN_RETRACE_PCT = float(os.getenv("M5_SECOND_ENTRY_MIN_RETRACE_PCT", str(M5_SECOND_ENTRY_MIN_RETRACE_PCT)))
    except Exception:
        pass

# ========= RISK-SENTINEL =========
AUTO_LOCK_ON_2_SL = _env_bool("AUTO_LOCK_ON_2_SL", "true")
AUTO_LOCK_NOTIFY  = _env_bool("AUTO_LOCK_NOTIFY", "true")
_RS_STATE_KEY     = "risk_sentinel"
_RS_STATE_FILE    = "risk_sentinel_state.json"

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
        import json as _json
        with open(_RS_STATE_FILE, "r", encoding="utf-8") as f:
            return _json.load(f) or {}
    except Exception:
        return {}

def _rs_save_all(storage, data: Dict[str, Any]) -> None:
    if storage and hasattr(storage, "set"):
        storage.set(_RS_STATE_KEY, data)
        return
    try:
        import json as _json
        with open(_RS_STATE_FILE, "w", encoding="utf-8") as f:
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
        "last_update": now_vn().isoformat(),
    })

def _rs_set_day(storage, day: str, st: Dict[str, Any]) -> None:
    all_data = _rs_load_all(storage)
    st["last_update"] = now_vn().isoformat()
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

def _rs_on_trade_close(storage, *, result: str, window_key: Optional[str], when: Optional[datetime] = None) -> bool:
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

# ========= Tide helpers =========
def _nearest_tide_center(now: datetime) -> Optional[datetime]:
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
    try:
        h = float(os.getenv("TP_TIME_HOURS", "12"))
        return max(0.5, min(48.0, h))
    except Exception:
        return 12.0

# ========= States =========
_last_decision_text: Dict[int, str] = {}
_last_m5_slot_sent: Dict[int, int] = {}
_user_tide_state: Dict[int, Dict[str, Any]] = {}
_open_pos: Dict[int, Dict[str, Any]] = {}
_m30_guard_state: Dict[int, Dict[str, Dict[str, Any]]] = {}
_m30_consec_state: Dict[int, Dict[str, Any]] = {}
_last_entry_meta: Dict[int, Dict[str, Any]] = {}

def get_last_decision_text(uid: int) -> Optional[str]:
    return _last_decision_text.get(uid)

def set_runtime_env(kv: Dict[str, str]) -> None:
    _apply_runtime_env(kv)

# ================== BROADCAST BOT (kênh EXECUTE-only) ==================
from telegram import Bot
from config.settings import (
    ACCOUNTS, SINGLE_ACCOUNT,
    TELEGRAM_BROADCAST_BOT_TOKEN, TELEGRAM_BROADCAST_CHAT_ID,
    MAX_ORDERS_PER_DAY, MAX_ORDERS_PER_TIDE_WINDOW,
)
_bcast_bot: Optional[Bot] = None
if TELEGRAM_BROADCAST_BOT_TOKEN:
    _bcast_bot = Bot(token=TELEGRAM_BROADCAST_BOT_TOKEN)

async def _broadcast_html(msg: str):
    if _bcast_bot and TELEGRAM_BROADCAST_CHAT_ID:
        try:
            await _bcast_bot.send_message(
                chat_id=int(TELEGRAM_BROADCAST_CHAT_ID),
                text=msg,
                parse_mode="HTML",
                disable_web_page_preview=True
            )
        except Exception:
            pass

# ================== EXECUTE MULTI-ACCOUNT ==================
async def _execute_one_account(acc: dict, *, pair_disp: str, side: str, base_risk: float,
                               base_lev: int, close_price: float, tide_label: str,
                               tp_hours: float, moon_label: str) -> Dict[str, Any]:
    """
    Thực thi 1 account. Trả về dict kết quả để tổng hợp log & TP-by-time.
    """
    name = acc.get("name", "default")
    ex_id = acc.get("exchange")
    key = acc.get("api_key")
    sec = acc.get("api_secret")
    testnet = bool(acc.get("testnet", False))
    pair_use = acc.get("pair", pair_disp)
    risk_percent = float(acc.get("risk_percent", base_risk))
    leverage = int(acc.get("leverage", base_lev))

    result = {"name": name, "pair": pair_use, "ok": False, "msg": ""}

    try:
        if not callable(ExchangeClient):
            result["msg"] = "no_exchange_client"
            return result

        ex = ExchangeClient(ex_id, key, sec, testnet)
        bal = await ex.balance_usdt()

        # sizing
        try:
            qty = calc_qty(bal, risk_percent, leverage, close_price)
        except Exception:
            # fallback tuyến tính
            risk_value = bal * (risk_percent / 100.0)
            qty = max((risk_value * leverage) / max(close_price, 1e-9), 0.0001)
        SIZE_MULT = float(os.getenv("SIZE_MULT", "1.0"))
        qty *= SIZE_MULT

        # SL/TP
        sl, tp = auto_sl_by_leverage(close_price, side, leverage)

        # set lev + execute
        await ex.set_leverage(pair_use, leverage)
        res = await ex.market_with_sl_tp(pair_use, side.upper() == "LONG", qty, sl, tp)
        ok = bool(getattr(res, "ok", False))
        msg = getattr(res, "message", str(res))

        # broadcast
        if ok:
            bmsg = (
                f"🚀 <b>EXECUTED</b> | <b>{name}</b>\n"
                f"• Pair: <code>{pair_use}</code> | Side: <b>{side.upper()}</b> x{leverage}\n"
                f"• Entry: <b>{close_price:,.2f}</b> | SL: <code>{sl:,.2f}</code> | TP: <code>{tp:,.2f}</code>\n"
                f"• Risk: {risk_percent:.1f}% | Qty: {qty}\n"
                f"• Window: {tide_label} | TP-by-time: {tp_hours:.1f}h\n"
                f"• Moon: {moon_label}\n"
                f"• Time: {now_vn().strftime('%Y-%m-%d %H:%M:%S')} VN"
            )
            await _broadcast_html(bmsg)

        result.update({"ok": ok, "msg": msg, "qty": qty, "sl": sl, "tp": tp, "lev": leverage, "risk": risk_percent,
                       "exchange": ex_id})
        return result
    except Exception as e:
        result["msg"] = f"exec_err:{e}"
        return result


async def execute_for_all_accounts(*, pair_disp: str, side: str, base_risk: float,
                                   base_lev: int, close_price: float, tide_label: str,
                                   tp_hours: float, moon_label: str) -> List[Dict[str, Any]]:
    accounts = ACCOUNTS if ACCOUNTS else [SINGLE_ACCOUNT]
    tasks = [
        _execute_one_account(acc,
            pair_disp=pair_disp, side=side, base_risk=base_risk, base_lev=base_lev,
            close_price=close_price, tide_label=tide_label, tp_hours=tp_hours, moon_label=moon_label
        )
        for acc in accounts
    ]
    return await asyncio.gather(*tasks)

# ========= Quyết định & vào lệnh =========
@dataclass
class UserStateView:
    settings: Any

async def decide_once_for_uid(uid: int, app, storage) -> Optional[str]:
    now = now_vn()

    # 1) User settings
    try:
        st = storage.get_user(uid)
        pair_disp = st.settings.pair or "BTC/USDT"
        symbol = pair_disp.replace("/", "")
        risk_percent = float(getattr(st.settings, "risk_percent", 20.0))
        leverage = int(float(getattr(st.settings, "leverage", 44)))
        mode = str(getattr(st.settings, "mode", "manual")).lower()
        auto_on = (mode == "auto") or bool(getattr(st.settings, "auto_trade_enabled", False))
        balance_usdt = float(getattr(st.settings, "balance_usdt", 100.0))
        tide_window_hours = float(getattr(st.settings, "tide_window_hours", TIDE_WINDOW_HOURS))
    except Exception:
        pair_disp = "BTC/USDT"
        symbol = "BTCUSDT"
        risk_percent = 20.0
        leverage = 44
        auto_on = True
        balance_usdt = 100.0
        tide_window_hours = TIDE_WINDOW_HOURS

    if not auto_on:
        if AUTO_DEBUG and AUTO_DEBUG_VERBOSE:
            await _debug_send(app, uid, _one_line("SKIP", "auto_off", now))
        return None

    # 2) Risk sentinel
    if _rs_is_locked_today(storage, now):
        if AUTO_LOCK_NOTIFY:
            try:
                chat_id = int(AUTO_DEBUG_CHAT_ID) if AUTO_DEBUG_CHAT_ID.isdigit() else uid
                await app.bot.send_message(chat_id=chat_id, text=f"⚠️ Auto LOCKED hôm nay ({_rs_status_today(storage)['day']}).")
            except Exception:
                pass
        return None

    # 3) Chỉ xử lý ngay sau M5 close
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

    # 4) Evaluate report
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

    skip_report  = bool(res.get("skip", True))
    desired_side = str(res.get("signal", "NONE")).upper()
    confidence   = int(res.get("confidence", 0))
    text_block   = (res.get("text") or "").strip()
    frames       = res.get("frames", {}) or {}
    h4           = frames.get("H4", {}) or {}
    m30          = frames.get("M30", {}) or {}
    m5f          = frames.get("M5", {}) or {}

    side_m30 = str(m30.get("side", "NONE")).upper()
    side_m5  = str(m5f.get("side", "NONE")).upper()

    # 5) Late-window theo mốc thủy triều
    center = _nearest_tide_center(now)
    tau = None
    if isinstance(center, datetime):
        tau = (now - center).total_seconds() / 3600.0
    in_late = (tau is not None) and (ENTRY_LATE_FROM_HRS <= tau <= ENTRY_LATE_TO_HRS)

    # Gate: ENTRY_LATE_ONLY
    if ENTRY_LATE_ONLY and not in_late:
        msg = _one_line("SKIP", "not_in_late_window", now,
                        f"tau={tau if tau is not None else 'NA'} | late={ENTRY_LATE_FROM_HRS}-{ENTRY_LATE_TO_HRS}h")
        _last_decision_text[uid] = msg + "\n\n" + text_block
        if not AUTO_DEBUG_ONLY_WHEN_SKIP:
            await _debug_send(app, uid, msg)
        return msg

    # 6) M30 flip guard quanh tâm thủy triều
    if M30_FLIP_GUARD and center is not None:
        key_day = now.strftime("%Y-%m-%d")
        key_win = f"{center.strftime('%H:%M')}"
        g_user = _m30_guard_state.setdefault(uid, {})
        g_day  = g_user.setdefault(key_day, {})
        g      = g_day.setdefault(key_win, {})
        if tau is not None and tau < 0:
            if "pre_side" not in g and side_m30:
                g["pre_side"] = side_m30
            msg = _one_line("SKIP", "m30_flip_guard_wait_post", now, f"tau={tau:.2f}h | pre_m30={g.get('pre_side', side_m30)}")
            _last_decision_text[uid] = msg + "\n\n" + text_block
            if not AUTO_DEBUG_ONLY_WHEN_SKIP:
                await _debug_send(app, uid, msg)
            return msg
        if tau is not None and tau >= 0:
            pre = g.get("pre_side", side_m30)
            if "pre_side" not in g:
                g["pre_side"] = pre
            if side_m30 != pre:
                g["flipped"] = True
            if g.get("post_stable_since") is None:
                g["post_stable_since"] = now
            since = g.get("post_stable_since")
            if (now - since).total_seconds() < M30_STABLE_MIN_SEC:
                msg = _one_line("SKIP", "m30_guard_wait_stable", now, f"stable={(now - since).total_seconds():.0f}/{M30_STABLE_MIN_SEC}s | m30={side_m30}")
                _last_decision_text[uid] = msg + "\n\n" + text_block
                if not AUTO_DEBUG_ONLY_WHEN_SKIP:
                    await _debug_send(app, uid, msg)
                return msg

    # 7) M30 cần N nến liên tiếp cùng hướng
    if side_m30 in ("LONG", "SHORT") and M30_NEED_CONSEC_N > 1:
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
        if stc["count"] < M30_NEED_CONSEC_N:
            msg = _one_line("SKIP", "m30_need_consec_n", now,
                            f"side={side_m30} consec={stc['count']}/{M30_NEED_CONSEC_N}")
            _last_decision_text[uid] = msg + "\n\n" + text_block
            if not AUTO_DEBUG_ONLY_WHEN_SKIP:
                await _debug_send(app, uid, msg)
            return msg

    # 8) giới hạn số lệnh trong một cửa sổ thủy triều
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

    # 9) skip theo /report
    if skip_report:
        msg = _one_line("SKIP", "report_skip", now, text_block.splitlines()[0] if text_block else "")
        _last_decision_text[uid] = msg + "\n\n" + text_block
        if not AUTO_DEBUG_ONLY_WHEN_SKIP:
            await _debug_send(app, uid, msg)
        return msg

    # 10) không có tín hiệu
    if desired_side not in ("LONG", "SHORT"):
        msg = _one_line("SKIP", "no_signal", now, f"conf={confidence}")
        _last_decision_text[uid] = msg + "\n\n" + text_block
        if not AUTO_DEBUG_ONLY_WHEN_SKIP:
            await _debug_send(app, uid, msg)
        return msg

    # 11) M5 bắt buộc match M30 (nếu bật)
    if ENFORCE_M5_MATCH_M30 and side_m30 in ("LONG", "SHORT"):
        if side_m5 != side_m30:
            msg = _one_line("SKIP", "m5_must_match_m30", now, f"m5={side_m5} | m30={side_m30}")
            _last_decision_text[uid] = msg + "\n\n" + text_block
            if not AUTO_DEBUG_ONLY_WHEN_SKIP:
                await _debug_send(app, uid, msg)
            return msg
        desired_side = side_m30

    # 12) COOLDOWN M5
    last = _last_entry_meta.get(uid, {})
    last_at: Optional[datetime] = last.get("at")
    last_win = last.get("window")
    if last_at is not None:
        gap_min = (now - last_at).total_seconds() / 60.0
        same_window = (last_win == key_win)
        under_scope = (same_window if M5_GAP_SCOPED_TO_WINDOW else True)
        if under_scope and gap_min < M5_MIN_GAP_MIN:
            msg = _one_line("SKIP", "m5_cooldown", now,
                            f"gap={gap_min:.1f}/{M5_MIN_GAP_MIN}min | scope={'window' if M5_GAP_SCOPED_TO_WINDOW else 'global'}")
            _last_decision_text[uid] = msg + "\n\n" + text_block
            if not AUTO_DEBUG_ONLY_WHEN_SKIP:
                await _debug_send(app, uid, msg)
            return msg

        if under_scope and same_window and int(st_key.get("trade_count", 0)) >= 1 and not ALLOW_SECOND_ENTRY:
            msg = _one_line("SKIP", "second_entry_disabled", now, f"win={key_win}")
            _last_decision_text[uid] = msg + "\n\n" + text_block
            if not AUTO_DEBUG_ONLY_WHEN_SKIP:
                await _debug_send(app, uid, msg)
            return msg

        if under_scope and same_window and int(st_key.get("trade_count", 0)) >= 1 and ALLOW_SECOND_ENTRY:
            try:
                px = float(m30.get("close") or 0.0)
                last_px = float(last.get("price") or 0.0)
                side_last = str(last.get("side") or desired_side).upper()
                retrace_ok = False
                if last_px and px:
                    if side_last == "LONG":
                        retrace_ok = ((last_px - px) / last_px * 100.0) >= M5_SECOND_ENTRY_MIN_RETRACE_PCT
                    else:
                        retrace_ok = ((px - last_px) / last_px * 100.0) >= M5_SECOND_ENTRY_MIN_RETRACE_PCT
                if not retrace_ok:
                    msg = _one_line("SKIP", "second_entry_need_retrace", now,
                                    f"need≥{M5_SECOND_ENTRY_MIN_RETRACE_PCT}% | last={last_px} now={px}")
                    _last_decision_text[uid] = msg + "\n\n" + text_block
                    if not AUTO_DEBUG_ONLY_WHEN_SKIP:
                        await _debug_send(app, uid, msg)
                    return msg
            except Exception:
                pass

    # 13) (Tùy) Gate M5 lần cuối
    if callable(m5_entry_check):
        ok, reason, _meta = m5_entry_check(symbol, desired_side)
        if not ok:
            msg = _one_line("SKIP", "m5_gate_fail", now, f"reason={reason}")
            _last_decision_text[uid] = msg + "\n\n" + text_block
            if not AUTO_DEBUG_ONLY_WHEN_SKIP:
                await _debug_send(app, uid, msg)
            return msg

    # 14) Khớp lệnh: MULTI-ACCOUNT + broadcast (fallback single-account nếu ACCOUNTS rỗng)
    opened_real = False
    order_msg = "(simulation)"
    sl_price = None

    # close_price (ưu tiên từ report; nếu trống, có thể bổ sung fetch ticker ở executor)
    try:
        close_price = float(m30.get("close") or h4.get("close") or 0.0)
    except Exception:
        close_price = 0.0

    try:
        tp_hours = _current_tp_hours()
        exec_results = await execute_for_all_accounts(
            pair_disp=pair_disp, side=desired_side, base_risk=risk_percent, base_lev=leverage,
            close_price=close_price, tide_label=key_win, tp_hours=tp_hours, moon_label="—"
        )
        opened_real = any(r.get("ok") for r in exec_results)
        order_msg = " | ".join([f"{r.get('name')}: {('OK' if r.get('ok') else 'ERR')}" for r in exec_results]) or "(no_exec)"
        # lưu SL ước lượng để suy đoán kết quả khi dọn vị thế sớm
        for r in exec_results:
            if r.get("ok"):
                sl_price = r.get("sl")
                break
        # Lưu danh sách account để TP-by-time đóng đồng loạt
        _open_pos[uid] = {
            "pair": pair_disp, "side": desired_side,
            "accounts": exec_results,  # list từng account (pair/name/exchange/sl/tp/…)
            "entry_time": now, "tide_center": center or now,
            "tp_deadline": (center or now) + timedelta(hours=tp_hours),
            "simulation": (not opened_real),
            "sl_price": sl_price,
            "tide_window_key": f"{(center or now).strftime('%Y-%m-%dT%H:%M')}",
        }
    except Exception as e:
        order_msg = f"place_order_error:{e}"

    st_key["trade_count"] = int(st_key.get("trade_count", 0)) + 1
    order_seq = st_key["trade_count"]

    # Lưu meta phục vụ cooldown/second-entry
    try:
        _last_entry_meta[uid] = {
            "at": now, "window": key_win, "price": float(close_price or 0.0), "side": desired_side
        }
    except Exception:
        pass

    # 16) Log /autolog
    header = (
        f"🤖 AUTO EXECUTE | {pair_disp} {desired_side}\n"
        f"Score H4/M30: {h4.get('score',0)} / {m30.get('score',0)} | Total≈{confidence}\n"
        f"rule M5==M30: {'ON' if ENFORCE_M5_MATCH_M30 else 'OFF'} | m30={side_m30}\n"
        f"late_window={'YES' if in_late else 'NO'} | "
        f"TP-by-time: {_open_pos[uid]['tp_deadline'].strftime('%Y-%m-%d %H:%M:%S') if uid in _open_pos else '—'}\n"
        f"➡️ EXECUTED: {desired_side} {pair_disp} | {order_msg}\n"
        f"📣 Opened trade #{order_seq}/{MAX_TRADES_PER_WINDOW}\n"
        "━━━━━━━━━━━━━━━━━━━━━━━\n"
    )
    final_text = header + (text_block or "(no_report_block)")
    _last_decision_text[uid] = final_text

    try:
        chat_id = int(AUTO_DEBUG_CHAT_ID) if AUTO_DEBUG_CHAT_ID.isdigit() else uid
    except Exception:
        chat_id = uid
    try:
        await app.bot.send_message(chat_id=chat_id, text=final_text)
    except Exception:
        pass

    return final_text

# ========= TP-by-time =========
async def maybe_tp_by_time(uid: int, app, storage) -> Optional[str]:
    if uid not in _open_pos:
        return None

    pos = _open_pos[uid]
    now = now_vn()

    # Hết hạn TP-by-time → đóng MỌI account đã mở
    dl = pos.get("tp_deadline")
    if dl and now >= dl:
        order_msg = "(simulation)"
        acc_results = []
        if callable(ExchangeClient) and not pos.get("simulation"):
            try:
                for acc in pos.get("accounts", []):
                    if not acc.get("ok"):
                        continue
                    ex = ExchangeClient(acc.get("exchange"), None, None, False)
                    res = await ex.close_position(acc.get("pair", pos.get("pair","BTC/USDT")))
                    acc_results.append(f"{acc.get('name')}: {getattr(res,'message',res)}")
                order_msg = " | ".join(acc_results) if acc_results else "(no_live_positions)"
            except Exception as e:
                order_msg = f"close_err:{e}"

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

        try:
            _ = _rs_on_trade_close(storage, result="TP_TIME", window_key=pos.get("tide_window_key"), when=now)
        except Exception:
            pass
        return msg

    return None

async def start_auto_loop(app, storage):
    uid_env = os.getenv("TELEGRAM_CHAT_ID", "").strip()
    forced_uid = int(uid_env) if uid_env.isdigit() else 0

    while True:
        uids: List[int] = []
        try:
            data_dict = getattr(storage, "data", {}) or {}
            uids = sorted([int(k) for k in data_dict.keys() if str(k).isdigit()])
        except Exception:
            uids = []
        if forced_uid and forced_uid not in uids:
            uids.append(forced_uid)

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
