# ----------------------- m5_reporter.py -----------------------
from __future__ import annotations
import os, asyncio
from typing import Dict, Any, Optional, Callable
from datetime import datetime, timezone, timedelta

# ===== TZ VN =====
try:
    import pytz
    VN_TZ = pytz.timezone("Asia/Ho_Chi_Minh")
except Exception:
    VN_TZ = timezone(timedelta(hours=7))

def _now_vn() -> datetime:
    try:
        return datetime.now(VN_TZ)
    except Exception:
        return datetime.utcnow().replace(tzinfo=timezone.utc) + timedelta(hours=7)

# == Env ==
SCHEDULER_TICK_SEC = int(os.getenv("SCHEDULER_TICK_SEC", "2"))
M5_MAX_DELAY_SEC   = int(os.getenv("M5_MAX_DELAY_SEC", "60"))
M5_BACKFILL_SLOTS  = int(os.getenv("M5_BACKFILL_SLOTS", "2"))
M5_FORCE_ON        = os.getenv("M5_FORCE_ON", "false").lower() in ("1","true","yes","on")
M5_STRICT_CLOSE    = os.getenv("M5_STRICT_CLOSE", "false").lower() in ("1","true","yes","on")

# ===== Beautify (chống lỗi Telegram parse + đồng nhất hiển thị) =====
def _beautify_report(s: str) -> str:
    if not isinstance(s, str):
        return s
    s = s.replace("&lt;=", "≤").replace("&gt;=", "≥")
    s = s.replace("&lt;", "＜").replace("&gt;", "＞")
    s = s.replace("<=", "≤").replace(">=", "≥")
    s = (s.replace(" EMA34<EMA89", " EMA34＜EMA89")
           .replace(" EMA34>EMA89", " EMA34＞EMA89")
           .replace(" Close<EMA34", " Close＜EMA34")
           .replace(" Close>EMA34", " Close＞EMA34")
           .replace(" close<EMA34", " close＜EMA34")
           .replace(" close>EMA34", " close＞EMA34"))
    s = (s.replace("zone Z1(<30)", "zone Z1 [<30]")
           .replace("zone Z2(30-45)", "zone Z2 [30–45]")
           .replace("zone Z3(45-55)", "zone Z3 [45–55]")
           .replace("zone Z4(55-70)", "zone Z4 [55–70]")
           .replace("zone Z5(>70)", "zone Z5 [>70]"))
    s = (s.replace("vol>=MA20", "vol ≥ MA20")
           .replace("vol<=MA20", "vol ≤ MA20")
           .replace("wick>=50%", "wick ≥ 50%")
           .replace("wick<=50%", "wick ≤ 50%"))
    s = s.replace("_", "＿")
    return s

# ===== Lazy imports =====
def _try(fn: Callable[[], Any]) -> Optional[Any]:
    try:
        return fn()
    except Exception:
        return None

_market_data = _try(lambda: __import__("data.market_data", fromlist=["get_klines"]))
_m5_strategy = _try(lambda: __import__("strategy.m5_strategy", fromlist=["m5_entry_check", "m5_snapshot", "m5_entry_summary"]))
_sig_eval    = _try(lambda: __import__("strategy.signal_generator", fromlist=["evaluate_signal"]))

def _floor_5m_slot(ts: int) -> int:
    return ts // 300

def _rsi_zone(v: Optional[float]) -> str:
    try:
        x = float(v)
    except Exception:
        return "UNK"
    if x <= 30: return "Z1"
    if x <= 45: return "Z2"
    if x <  55: return "Z3"
    if x <  70: return "Z4"
    return "Z5"

# ========= unify entry line with /report =========
def _desired_from_htf(symbol: str) -> Optional[str]:
    if not _sig_eval:
        return None
    try:
        res = _sig_eval.evaluate_signal(symbol)
        if isinstance(res, dict):
            d = (res.get("frames", {}) or {}).get("combine", {}).get("desired")
            if d in ("LONG", "SHORT"):
                return d
            s = res.get("signal")
            if s in ("LONG", "SHORT"):
                return s
    except Exception:
        pass
    return None

def _entry_line_from_checker(symbol: str) -> str:
    if not _m5_strategy:
        return "➡️ Entry: ⏸️ Wait (m5_strategy not available)"
    m5_entry_summary = getattr(_m5_strategy, "m5_entry_summary", None)
    if callable(m5_entry_summary):
        desired = _desired_from_htf(symbol)
        try:
            line, meta = m5_entry_summary(symbol, desired)
            return _beautify_report(f"{line}\n{meta}")
        except Exception as e:
            return f"➡️ Entry: ⏸️ Wait (error: {e})"

    m5_entry_check = getattr(_m5_strategy, "m5_entry_check", None)
    if not callable(m5_entry_check):
        return "➡️ Entry: ⏸️ Wait (m5_entry_check() missing)"
    desired = _desired_from_htf(symbol)
    try:
        ok, reason, meta = m5_entry_check(symbol, desired)
    except Exception as e:
        return f"➡️ Entry: ⏸️ Wait (error: {e})"
    meta = meta or {}
    close = meta.get("close", 0.0)
    vol   = meta.get("vol", 0)
    vma20 = meta.get("volMA20", 0)
    uw    = meta.get("uw", 0.0)
    lw    = meta.get("lw", 0.0)
    side_txt = "OK" if ok else "NO"
    line = (
        f"🔎 M5 Entry: {side_txt} — {reason}\n"
        f"• M5 meta: vol{'≥' if float(vol) > float(vma20) else '≤'}MA20; "
        f"{'wick≥50%' if (desired=='LONG' and lw>=0.5) or (desired=='SHORT' and uw>=0.5) else 'wick<50%'}; "
        f"RSI×EMA:—; Stoch:—\n"
        f"(close={close:.2f}, vol={int(vol)}, volMA20={int(vma20)}, uw={uw:.2f}, lw={lw:.2f})"
    )
    return _beautify_report(line)

def _ema(series, span: int):
    return series.ewm(span=span, adjust=False).mean()

async def _build_m5_snapshot(symbol: str) -> Dict[str, Any]:
    if _m5_strategy and hasattr(_m5_strategy, "m5_snapshot"):
        try:
            snap = _m5_strategy.m5_snapshot(symbol)
            if isinstance(snap, str):
                return {"raw_text": snap}
            if isinstance(snap, dict):
                if snap.get("text"):
                    return {"raw_text": str(snap["text"])}
                return {"close": snap.get("close"), "rsi": snap.get("rsi"), "note": snap.get("note")}
        except Exception:
            pass
    close_val = None; rsi_val = None; note = None
    try:
        if not _market_data:
            raise RuntimeError("market_data not available")
        get_klines = getattr(_market_data, "get_klines", None)
        if not callable(get_klines):
            raise RuntimeError("get_klines not callable")
        df = get_klines(symbol, "5m", 120)
        if df is None or len(df) < 20:
            raise RuntimeError("not enough klines")
        close_series = df["close"]
        close_val = float(close_series.iloc[-1])
        delta = close_series.diff()
        up = delta.clip(lower=0.0); down = -1 * delta.clip(upper=0.0)
        roll_up = up.ewm(alpha=1/14, adjust=False).mean()
        roll_down = down.ewm(alpha=1/14, adjust=False).mean()
        rs = roll_up / (roll_down.replace(0, 1e-12))
        rsi_series = 100.0 - (100.0 / (1.0 + rs))
        rsi_val = float(rsi_series.iloc[-1])
    except Exception as e:
        note = f"⚠️ Fallback snapshot error: {e}"
    return {"close": close_val, "rsi": rsi_val, "note": note}

def _fmt_snapshot(d: Dict[str, Any]) -> str:
    raw = d.get("raw_text")
    if isinstance(raw, str) and raw.strip():
        return _beautify_report(raw.strip())
    parts = []
    parts.append("⏱ M5 Snapshot:")
    if d.get("close") is not None:
        parts.append(f"• Close: {d['close']}")
    if d.get("rsi") is not None:
        try:
            parts.append(f"• RSI: {float(d['rsi']):.2f} (zone {_rsi_zone(float(d['rsi']))})")
        except Exception:
            parts.append(f"• RSI: {d['rsi']}")
    if d.get("note"):
        parts.append(f"• Note: {d['note']}")
    parts.append(f"• Time: {_now_vn().strftime('%Y-%m-%d %H:%M:%S')}")
    return _beautify_report("\n".join(parts))

_last_sent_slot_by_uid: Dict[int, int] = {}

async def m5_report_tick(app, storage):
    bot = app.bot
    uid_env = os.getenv("TELEGRAM_CHAT_ID", "").strip()
    forced_uid = int(uid_env) if uid_env.isdigit() else 0

    def _symbol_of(uid: int) -> str:
        try:
            st = storage.get_user(uid)
            return (st.settings.pair or "BTC/USDT").replace("/", "")
        except Exception:
            return "BTCUSDT"

    now = _now_vn()
    ts = int(now.timestamp())
    curr_slot = _floor_5m_slot(ts)
    boundary = curr_slot * 300
    delay = ts - boundary
    just_closed = (0 <= delay <= M5_MAX_DELAY_SEC)

    uids = []
    try:
        data_dict = getattr(storage, "data", {}) or {}
        for k in list(data_dict.keys()):
            try:
                uids.append(int(k))
            except Exception:
                continue
    except Exception:
        pass
    if forced_uid and forced_uid not in uids:
        uids.append(forced_uid)

    if not uids:
        return

    for uid in uids:
        try:
            st = storage.get_user(uid)
        except Exception:
            try:
                storage.get_user(uid)
                st = storage.get_user(uid)
            except Exception:
                st = None

        enabled = False
        if st is not None and getattr(st.settings, "m5_report_enabled", False):
            enabled = True
        elif M5_FORCE_ON and forced_uid and uid == forced_uid:
            enabled = True
        if not enabled:
            continue

        last_slot = _last_sent_slot_by_uid.get(uid, -1)
        if last_slot == curr_slot:
            continue

        send_slots = []
        if last_slot < 0:
            send_slots = [curr_slot]
        else:
            gap = curr_slot - last_slot
            if gap <= 0:
                continue
            max_fill = max(1, M5_BACKFILL_SLOTS)
            start = curr_slot - (max_fill - 1) if gap >= max_fill else last_slot + 1
            send_slots = list(range(start, curr_slot + 1))

        if M5_STRICT_CLOSE and (send_slots and send_slots[-1] == curr_slot) and (not just_closed):
            if len(send_slots) == 1:
                continue
            else:
                send_slots = send_slots[:-1]

        for s in send_slots:
            _last_sent_slot_by_uid[uid] = s
            symbol = _symbol_of(uid)
            try:
                data = await _build_m5_snapshot(symbol)
                text = _fmt_snapshot(data)

                # ➕ dòng Entry — ĐỒNG BỘ VỚI /report (relaxed + strict)
                try:
                    text += "\n" + _entry_line_from_checker(symbol)
                except Exception as _e:
                    text += f"\n➡️ Entry: ⏸️ Wait (error: {_e})"

                text = _beautify_report(text)

                if s != curr_slot:
                    text += f"\n(↩️ backfill for slot {s})"
                await bot.send_message(chat_id=uid, text=text)
            except Exception:
                continue

async def m5_report_loop(app, storage):
    while True:
        try:
            await m5_report_tick(app, storage)
        except Exception:
            pass
        await asyncio.sleep(SCHEDULER_TICK_SEC)
# ----------------------- /m5_reporter.py -----------------------
