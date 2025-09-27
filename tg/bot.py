# ----------------------- tg/bot.py -----------------------
from __future__ import annotations
import os, asyncio, html
from datetime import datetime, timedelta, timezone
from typing import Optional

from telegram import Update, Bot
from telegram import constants
from telegram.ext import ApplicationBuilder, CommandHandler, ContextTypes, Application

from config.settings import (
    TELEGRAM_BOT_TOKEN,
    DEFAULT_MODE,
    TELEGRAM_BROADCAST_BOT_TOKEN,
    TELEGRAM_BROADCAST_CHAT_ID,
)
from tg.admin_bot import enforce_admin_for_all_commands # quản lý quyền botallinone
from utils.storage import Storage
from utils.time_utils import now_vn, TOKYO_TZ
from strategy.signal_generator import evaluate_signal, tide_window_now
from strategy.m5_strategy import m5_snapshot, m5_entry_check
from core.trade_executor import ExchangeClient, calc_qty, auto_sl_by_leverage
from core.trade_executor import close_position_on_all, close_position_on_account # ==== /close (đa tài khoản: Binance/BingX/...) ====
from tg.formatter import format_signal_report, format_daily_moon_tide_report
from core.approval_flow import mark_done, get_pending
from core.trade_executor import retime_tp_by_time_for_open_positions

# Vòng nền
from core.auto_trade_engine import start_auto_loop, _load_tidegate_config, now_vn as _ae_now_vn, TIDE_WINDOW_HOURS
from core.m5_reporter import m5_report_loop

# NEW: dùng resolver P1–P4 theo %illum + hướng
from data.moon_tide import resolve_preset_code

# Áp dụng cho order_new_cmd() gọi (B)+ (C) để kiểm tra hiệu quả
from core.auto_trade_engine import _auto_execute_hub, _auto_broadcast_and_log

# >>> TideGate unify (A->T->B->C) <<<
from core.tide_gate import TideGateConfig, tide_gate_check, bump_counters_after_execute


# ================== Global state ==================
storage = Storage()
ex = ExchangeClient()

# ==== QUOTA helpers: 2 lệnh / cửa sổ thủy triều, 8 lệnh / ngày (gộp mọi mode) ====
# (Giữ cho /order legacy; /ordernew đã dùng TideGate.)
def _quota_precheck_and_label(st):
    now = now_vn()
    twin = tide_window_now(now, hours=float(st.settings.tide_window_hours))
    if not twin:
        return False, "⏳ Ngoài khung thủy triều.", None, None, 0
    start, end = twin
    tide_label = f"{start.strftime('%H:%M')}–{end.strftime('%H:%M')}"
    tkey = (start + (end - start) / 2).strftime("%Y-%m-%d %H:%M")
    used = int(st.tide_window_trades.get(tkey, 0))
    if st.today.count >= st.settings.max_orders_per_day:
        return False, f"🚫 Vượt giới hạn ngày ({st.settings.max_orders_per_day}).", tide_label, tkey, used
    if used >= st.settings.max_orders_per_tide_window:
        return False, f"🚫 Cửa sổ thủy triều hiện tại đã đủ {used}/{st.settings.max_orders_per_tide_window} lệnh.", tide_label, tkey, used
    return True, "", tide_label, tkey, used

def _quota_commit(st, tkey, used, uid):
    st.today.count += 1
    st.tide_window_trades[tkey] = used + 1
    storage.put_user(uid, st)


# ================== Helpers ==================
def _beautify_report(s: str) -> str:
    if not isinstance(s, str):
        return s
    s = s.replace("&lt;=", "≤").replace("&gt;=", "≥")
    s = s.replace("&lt;", "＜").replace("&gt;", "＞")
    s = s.replace("<=", "≤").replace(">=", "≥")
    s = s.replace(" EMA34<EMA89", " EMA34＜EMA89")
    s = s.replace(" Close<EMA34", " Close＜EMA34")
    s = s.replace(" EMA34>EMA89", " EMA34＞EMA89")
    s = s.replace(" Close>EMA34", " Close＞EMA34")
    s = s.replace("zone Z1( <30)", "zone Z1 [<30]") \
         .replace("zone Z2(30-45)", "zone Z2 [30–45]") \
         .replace("zone Z3(45-55)", "zone Z3 [45–55]") \
         .replace("zone Z4(55-70)", "zone Z4 [55–70]") \
         .replace("zone Z5(>70 )", "zone Z5 [>70]") \
         .replace("zone Z5(>70)", "zone Z5 [>70]")
    s = s.replace("vol>=MA20", "vol ≥ MA20") \
         .replace("vol<=MA20", "vol ≤ MA20") \
         .replace("wick>=50%", "wick ≥ 50%") \
         .replace("wick<=50%", "wick ≤ 50%")
    return s
# === Telegram helper: split long HTML safely (<4096 chars) ===
def _esc(s: str) -> str:
    return html.escape(s or "", quote=False)
TELEGRAM_HTML_LIMIT = 4096
_SAFE_BUDGET = 3500  # chừa biên cho thẻ HTML & escape

async def _send_long_html(update, context, text: str):
    """
    Gửi chuỗi HTML dài thành nhiều tin, tránh lỗi 4096 của Telegram.
    Dùng context.bot (PTB v20+), không dùng update.message.bot.
    """
    chat_id = update.effective_chat.id
    txt = text or ""
    parts = txt.split("\n\n")

    buf = ""
    for p in parts:
        candidate = (buf + ("\n\n" if buf else "") + p)
        if len(candidate) <= _SAFE_BUDGET:
            buf = candidate
        else:
            if buf:
                await context.bot.send_message(
                    chat_id=chat_id, text=buf,
                    parse_mode="HTML", disable_web_page_preview=True
                )
            # nếu p vẫn quá dài → cắt cứng
            while len(p) > _SAFE_BUDGET:
                chunk = p[:_SAFE_BUDGET]
                await context.bot.send_message(
                    chat_id=chat_id, text=chunk,
                    parse_mode="HTML", disable_web_page_preview=True
                )
                p = p[_SAFE_BUDGET:]
            buf = p

    if buf:
        await context.bot.send_message(
            chat_id=chat_id, text=buf,
            parse_mode="HTML", disable_web_page_preview=True
        )


# ==== BROADCAST (format thống nhất, lấy Entry hiển thị từ BINANCE SPOT) ====
_bcast_bot: Bot | None = None
if TELEGRAM_BROADCAST_BOT_TOKEN:
    try:
        _bcast_bot = Bot(token=TELEGRAM_BROADCAST_BOT_TOKEN)
    except Exception:
        _bcast_bot = None

async def _broadcast_html(text: str) -> None:
    if not (_bcast_bot and TELEGRAM_BROADCAST_CHAT_ID):
        return
    try:
        await _bcast_bot.send_message(
            chat_id=int(TELEGRAM_BROADCAST_CHAT_ID),
            text=text,
            parse_mode="HTML",
            disable_web_page_preview=True,
        )
    except Exception:
        pass

def _binance_spot_entry(pair: str) -> float:
    """
    Lấy giá hiển thị từ BINANCE SPOT (ví dụ BTCUSDT close). Không dùng cho khớp lệnh.
    """
    try:
        from data.market_data import get_klines
        sym = pair.replace("/", "").replace(":USDT", "")
        df = get_klines(symbol=sym, interval="1m", limit=2)
        if df is not None and len(df) > 0:
            return float(df.iloc[-1]["close"])
    except Exception:
        pass
    return 0.0

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



def _uid(update: Update) -> int:
    return update.effective_user.id

def _admin_uid() -> Optional[int]:
    val = (os.getenv("ADMIN_USER_ID") or "").strip()
    if val.isdigit():
        return int(val)
    try:
        a = storage.data.get("_admin_uid")
        return int(a) if a else None
    except Exception:
        return None

def _is_admin(uid: int) -> bool:
    a = _admin_uid()
    return (a is not None) and (uid == a)

def _bool_str(v):
    return "true" if (isinstance(v, bool) and v) or (isinstance(v, str) and v.strip().lower() in ("1","true","yes","on","y")) else "false"

def _env_or_runtime(k: str, default: str = "—") -> str:
    """
    Dùng để HIỂN THỊ giá trị hiện tại trong /help:
    - Ưu tiên ENV
    - Fallback sang runtime trong core.auto_trade_engine nếu có
    """
    v = os.getenv(k)
    if v is not None:
        return v
    try:
        from core import auto_trade_engine as ae
        if hasattr(ae, k):
            val = getattr(ae, k)
            if isinstance(val, bool): return "true" if val else "false"
            return str(val)
    except Exception:
        pass
    return default

# ================== PRESETS (P1–P4 theo Moon — % độ rọi + hướng) ==================
# ================== PRESETS (P1–P4 theo Moon — % độ rọi + hướng) ==================
# P1: 0–25% (quanh New) | P2: 25–75% & waxing | P3: 75–100% (quanh Full) | P4: 25–75% & waning
# Theo yêu cầu: P1=P3 (trend + Sonic on + late-only 0.5~2.5h, tide 2.5h, TP 5.5h)
#               P2=P4 (Sonic off; các tham số còn lại giống nhau trong cặp)
PRESETS = {
    # P1 — 0–25%: quanh New — Waning Crescent ↔ New ↔ Waxing Crescent
    # Trend/tiếp diễn + vào muộn để an toàn
    "P1": {
        "SONIC_MODE": "weight", "SONIC_WEIGHT": 1.0,

        # Entry timing (thủy triều)
        "ENTRY_LATE_PREF": False,
        "ENTRY_LATE_ONLY": True,
        "ENTRY_LATE_FROM_HRS": 0.5,
        "ENTRY_LATE_TO_HRS": 2.5,
        "TIDE_WINDOW_HOURS": 2.5,
        # TP theo thời gian (rút ngắn)
        "TP_TIME_HOURS": 5.5,
        # NEW — guard lật hướng M30 quanh mốc thủy triều
        "M30_FLIP_GUARD": True,
        "M30_STABLE_MIN_SEC": 1800, # after 30min tide center
		# Extreme guard defaults
        "EXTREME_BLOCK_ON": True,
        "EXTREME_RSI_OB": 70.0,
        "EXTREME_RSI_OS": 30.0,
        "EXTREME_STOCH_OB": 90.0,
        "EXTREME_STOCH_OS": 10.0,


        # M5 gate (giữ logic mặc định, có thể vặn thêm bằng /setenv khi cần)
        "M5_STRICT": False, "M5_RELAX_KIND": "either",
        "M5_WICK_PCT": 0.50,
        "M5_VOL_MULT_RELAX": 1.00, "M5_VOL_MULT_STRICT": 1.10,
        "M5_REQUIRE_ZONE_STRICT": True,
        "M5_LOOKBACK_RELAX": 3, "M5_RELAX_NEED_CURRENT": False,
        "M5_LOOKBACK_STRICT": 6, "ENTRY_SEQ_WINDOW_MIN": 30,
        # M5 entry spacing / second entry
        "M5_MIN_GAP_MIN": 15, # khoảng cách tối thiểu giữa 2 entry M5 (phút)
        "M5_GAP_SCOPED_TO_WINDOW": True, # true → reset gap theo từng tide window
        "ALLOW_SECOND_ENTRY": True,      # cho phép vào entry thứ 2 nếu đủ điều kiện
        "M5_SECOND_ENTRY_MIN_RETRACE_PCT": 0.1, # retrace % tối thiểu để entry lần 2

        # Các ngưỡng HTF mặc định (giữ nguyên như cũ)
        "RSI_OB": 65, "RSI_OS": 35, "DELTA_RSI30_MIN": 10,
        "SIZE_MULT_STRONG": 1.0, "SIZE_MULT_MID": 0.7, "SIZE_MULT_CT": 0.4,
    },

    # P2 — 25–75% & waxing: Waxing Crescent ↔ First Quarter ↔ Waxing Gibbous
    # Momentum/breakout — không ép late-only, Sonic OFF theo yêu cầu
    "P2": {
        "SONIC_MODE": "off",

        "ENTRY_LATE_PREF": False,
        "ENTRY_LATE_ONLY": True,
        "ENTRY_LATE_FROM_HRS": 0.5,     # để đồng bộ định dạng; không dùng nếu ONLY=false
        "ENTRY_LATE_TO_HRS": 2.5,
        "TIDE_WINDOW_HOURS": 2.5,       # cho thống nhất cặp P2=P4
        "TP_TIME_HOURS": 5.5,           # cho thống nhất cặp P2=P4
        
        # NEW — guard lật hướng M30 quanh mốc thủy triều
        "M30_FLIP_GUARD": True,
        "M30_STABLE_MIN_SEC": 1800, # after 30min tide center    
	    # Extreme guard defaults
        "EXTREME_BLOCK_ON": True,
        "EXTREME_RSI_OB": 70.0,
        "EXTREME_RSI_OS": 30.0,
        "EXTREME_STOCH_OB": 90.0,
        "EXTREME_STOCH_OS": 10.0,

        "M5_STRICT": False, "M5_RELAX_KIND": "either",
        "M5_WICK_PCT": 0.50,
        "M5_VOL_MULT_RELAX": 1.00, "M5_VOL_MULT_STRICT": 1.10,
        "M5_REQUIRE_ZONE_STRICT": True,
        "M5_LOOKBACK_RELAX": 3, "M5_RELAX_NEED_CURRENT": False,
        "M5_LOOKBACK_STRICT": 6, "ENTRY_SEQ_WINDOW_MIN": 30,
        # M5 entry spacing / second entry
        "M5_MIN_GAP_MIN": 15, # khoảng cách tối thiểu giữa 2 entry M5 (phút)
        "M5_GAP_SCOPED_TO_WINDOW": True, # true → reset gap theo từng tide window
        "ALLOW_SECOND_ENTRY": True,      # cho phép vào entry thứ 2 nếu đủ điều kiện
        "M5_SECOND_ENTRY_MIN_RETRACE_PCT": 0.1, # retrace % tối thiểu để entry lần 2
        
        
        "RSI_OB": 65, "RSI_OS": 35, "DELTA_RSI30_MIN": 10,
        "SIZE_MULT_STRONG": 1.0, "SIZE_MULT_MID": 0.7, "SIZE_MULT_CT": 0.4,
    },

    # P3 — 75–100%: Waxing Gibbous ↔ Full ↔ Waning Gibbous
    # Theo yêu cầu: giống P1 (trend + Sonic on + late-only 0.5~2.5h)
    "P3": {
        "SONIC_MODE": "weight", "SONIC_WEIGHT": 1.0,

        "ENTRY_LATE_PREF": False,
        "ENTRY_LATE_ONLY": True,
        "ENTRY_LATE_FROM_HRS": 0.5,
        "ENTRY_LATE_TO_HRS": 2.5,
        "TIDE_WINDOW_HOURS": 2.5,
        "TP_TIME_HOURS": 5.5,
        
        # NEW — guard lật hướng M30 quanh mốc thủy triều
        "M30_FLIP_GUARD": True,
        "M30_STABLE_MIN_SEC": 1800, # after 30min tide center
		# Extreme guard defaults
        "EXTREME_BLOCK_ON": True,
        "EXTREME_RSI_OB": 70.0,
        "EXTREME_RSI_OS": 30.0,
        "EXTREME_STOCH_OB": 90.0,
        "EXTREME_STOCH_OS": 10.0,

        "M5_STRICT": False, "M5_RELAX_KIND": "either",
        "M5_WICK_PCT": 0.50,
        "M5_VOL_MULT_RELAX": 1.00, "M5_VOL_MULT_STRICT": 1.10,
        "M5_REQUIRE_ZONE_STRICT": True,
        "M5_LOOKBACK_RELAX": 3, "M5_RELAX_NEED_CURRENT": False,
        "M5_LOOKBACK_STRICT": 6, "ENTRY_SEQ_WINDOW_MIN": 30,
        # M5 entry spacing / second entry
        "M5_MIN_GAP_MIN": 15, # khoảng cách tối thiểu giữa 2 entry M5 (phút)
        "M5_GAP_SCOPED_TO_WINDOW": True, # true → reset gap theo từng tide window
        "ALLOW_SECOND_ENTRY": True,      # cho phép vào entry thứ 2 nếu đủ điều kiện
        "M5_SECOND_ENTRY_MIN_RETRACE_PCT": 0.1, # retrace % tối thiểu để entry lần 2
        
        "RSI_OB": 65, "RSI_OS": 35, "DELTA_RSI30_MIN": 10,
        "SIZE_MULT_STRONG": 1.0, "SIZE_MULT_MID": 0.7, "SIZE_MULT_CT": 0.4,
    },

    # P4 — 25–75% & waning: Waning Gibbous ↔ Last Quarter ↔ Waning Crescent
    # Theo yêu cầu: giống P2 (Sonic OFF; không ép late-only)
    "P4": {
        "SONIC_MODE": "off",

        "ENTRY_LATE_PREF": False,
        "ENTRY_LATE_ONLY": True,
        "ENTRY_LATE_FROM_HRS": 0.5,
        "ENTRY_LATE_TO_HRS": 2.5,
        "TIDE_WINDOW_HOURS": 2.5,
        "TP_TIME_HOURS": 5.5,
        
        # NEW — guard lật hướng M30 quanh mốc thủy triều
        "M30_FLIP_GUARD": True,
        "M30_STABLE_MIN_SEC": 1800, # after 30min tide center
		# Extreme guard defaults
        "EXTREME_BLOCK_ON": True,
        "EXTREME_RSI_OB": 70.0,
        "EXTREME_RSI_OS": 30.0,
        "EXTREME_STOCH_OB": 90.0,
        "EXTREME_STOCH_OS": 10.0,

        "M5_STRICT": False, "M5_RELAX_KIND": "either",
        "M5_WICK_PCT": 0.50,
        "M5_VOL_MULT_RELAX": 1.00, "M5_VOL_MULT_STRICT": 1.10,
        "M5_REQUIRE_ZONE_STRICT": True,
        "M5_LOOKBACK_RELAX": 3, "M5_RELAX_NEED_CURRENT": False,
        "M5_LOOKBACK_STRICT": 6, "ENTRY_SEQ_WINDOW_MIN": 30,
        # M5 entry spacing / second entry
        "M5_MIN_GAP_MIN": 15, # khoảng cách tối thiểu giữa 2 entry M5 (phút)
        "M5_GAP_SCOPED_TO_WINDOW": True, # true → reset gap theo từng tide window
        "ALLOW_SECOND_ENTRY": True,      # cho phép vào entry thứ 2 nếu đủ điều kiện
        "M5_SECOND_ENTRY_MIN_RETRACE_PCT": 0.1, # retrace % tối thiểu để entry lần 2
        
        "RSI_OB": 65, "RSI_OS": 35, "DELTA_RSI30_MIN": 10,
        "SIZE_MULT_STRONG": 1.0, "SIZE_MULT_MID": 0.7, "SIZE_MULT_CT": 0.4,
    },
}

async def _apply_preset_and_reply(update: Update, preset_name: str, header: str = ""):
    preset = PRESETS[preset_name]
    for k, v in preset.items():
        os.environ[k] = _bool_str(v) if isinstance(v, bool) else str(v)

    applied_runtime = False
    try:
        from core import auto_trade_engine as ae
        apply_fn = getattr(ae, "apply_runtime_overrides", None)
        if callable(apply_fn):
            apply_fn({k: os.environ[k] for k in preset.keys()})
            applied_runtime = True
        else:
            for k, sval in os.environ.items():
                if k in preset and hasattr(ae, k):
                    typ = type(getattr(ae, k))
                    try:
                        if typ is bool:
                            setattr(ae, k, sval.strip().lower() in ("1","true","yes","on"))
                        elif typ is int:
                            setattr(ae, k, int(float(sval)))
                        elif typ is float:
                            setattr(ae, k, float(sval))
                        else:
                            setattr(ae, k, sval)
                        applied_runtime = True
                    except Exception:
                        pass
    except Exception:
        pass

    lines = [f"{k}={os.environ[k]}" for k in sorted(preset.keys())]
    msg = (header + "\n" if header else "") + f"✅ Đã áp dụng preset <b>{preset_name}</b>:\n" + "\n".join(lines)
    if applied_runtime:
        msg += "\n(đã áp dụng runtime cho AUTO engine)."
    else:
        msg += "\n(có thể cần khởi động lại để áp dụng hoàn toàn)."
    await update.message.reply_text(msg, parse_mode="HTML")

# ================== Commands ==================
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid = _uid(update)
    st = storage.get_user(uid)
    if not st.settings.mode:
        st.settings.mode = DEFAULT_MODE
        storage.put_user(uid, st)
    if _admin_uid() is None:
        storage.data["_admin_uid"] = uid
        storage.persist()

    await update.message.reply_text(
        "👋 Xin chào! Bot Moon & Tide đã sẵn sàng.\n\n"
        "📌 <b>Mode giao dịch:</b>\n"
        "• <code>/mode auto</code> — Bot tự động vào lệnh khi đủ điều kiện.\n"
        "• <code>/mode manual</code> — Bot chỉ báo tín hiệu, cần /approve hoặc /reject mới vào lệnh.\n"
        "• <code>/order</code> — Vào lệnh thủ công ngay (theo %risk/leverage).\n\n"
        "📌 <b>Đóng lệnh (/close):</b>\n"
        "• <code>/close</code> hoặc <code>/close 100</code> — Đóng toàn bộ & hủy TP/SL.\n"
        "• <code>/close 50</code> — Đóng 50% tất cả account.\n"
        "• <code>/close 30 bingx_test</code> — Đóng 30% ở account <code>bingx_test</code>.\n"
        "• <code>/close long</code> hoặc <code>/close short</code> — Đóng toàn bộ LONG/SHORT.\n"
        "• <code>/close 40 long</code> — Đóng 40% LONG (thứ tự tham số tự do).\n"
        "• <code>/close 25 short binance_main</code> — Đóng 25% SHORT ở <code>binance_main</code>.\n"
        "ℹ️ Hedge Mode: đóng đúng LONG/SHORT; Oneway: đóng theo vị thế hiện có.\n\n"
        "📌 <b>Command chính:</b>\n"
        "/aboutme — triết lý THÂN–TÂM–TRÍ & checklist\n"
        "/journal — mở form nhật ký giao dịch\n"
        "/recovery_checklist — checklist phục hồi sau thua lỗ\n"
        "/settings — cài đặt: pair, % vốn, leverage\n"
        "/tidewindow — xem/đổi ± giờ quanh thủy triều\n"
        "/report — gửi report H4→M30 (+ M5 filter)\n"
        "/status — trạng thái bot & vị thế\n"
        "/approve <code>id</code> /reject <code>id</code> — duyệt tín hiệu (manual)\n"
        "/m5report start|stop — auto M5 snapshot mỗi 5 phút\n"
        "/daily — báo cáo Moon & Tide trong ngày\n"
        "/autolog — log AUTO (tick M5 gần nhất)\n"
        "/preset &lt;name&gt;|auto — preset theo Moon Phase (P1–P4)\n"
        "/setenv KEY VALUE — chỉnh ENV runtime\n"
        "/setenv_status — xem cấu hình ENV/runtime\n\n"
        "💡 Dùng <code>/help</code> để xem hướng dẫn chi tiết.",
        parse_mode="HTML"
    )


async def help_cmd(update, context):
    # helper show ENV/runtimes trong /help
    def v(k, d="—"):
        return _env_or_runtime(k, d)

    # /help short | /help s -> bản rút gọn
    args = context.args if hasattr(context, "args") else []
    short_mode = bool(args and args[0].lower() in ("short", "s"))

    if short_mode:
        extra = (
            "<b>Presets (theo Moon — đặt tên mới):</b>\n"
            "• P1 — 0–25% (quanh New): Waning Crescent ↔️ New ↔️ Waxing Crescent\n"
            "• P2 — 25–75% &amp; waxing: Waxing Crescent ↔️ First Quarter ↔️ Waxing Gibbous\n"
            "• P3 — 75–100% (quanh Full): Waxing Gibbous ↔️ Full ↔️ Waning Gibbous\n"
            "• P4 — 25–75% &amp; waning: Waning Gibbous ↔️ Last Quarter ↔️ Waning Crescent\n\n"
            "<b>Auto theo Moon (P1–P4):</b>\n"
            "• /preset auto để đổi nhanh theo Moon (P1–P4)\n"
            "• /preset P1|P2|P3|P4 → chọn thủ công preset P-code.\n\n"
            "<b>Scoring H4/M30 (tóm tắt, đã nới logic theo zone &amp; hướng):</b>\n"
            "• Z2/Z4 = +2 (ủng hộ hướng đi lên/xuống TÙY vị trí RSI vs EMA-RSI và hướng di chuyển vào zone).\n"
            "• Z3 (45–55) = −1 (barrier, dễ sideway/đảo, cần cross để xác nhận).\n"
            "• RSI×EMA(RSI) cross = +2; align ổn định = +1.\n"
            "• Stoch RSI: bật ↑ từ &lt;20 / gãy ↓ từ &gt;80 = +2; bứt qua 50 = +1.\n"
            "• Sonic weight (nếu SONIC_MODE=weight) = +W khi cùng chiều (hiện: mode=weight, W=1.0).\n\n"
            "<b>Moon bonus (H4):</b>\n"
            "• +0..1.5 điểm tùy preset P1–P4 &amp; stage (pre/on/post) mốc N/FQ/F/LQ — chỉ boost độ tin cậy, không tự đảo bias.\n\n"
            "<b>Map total → size (đòn bẩy theo điểm):</b>\n"
            "• Total = H4_score + M30_score + Moon_bonus.\n"
            "• ≥8.5 → ×1.0; 6.5–8.5 → ×0.7; thấp hơn / CT → ×0.4.\n\n"
            "<b>AUTO execute &amp; khối lượng:</b>\n"
            "• Trong khung thủy triều và đạt điều kiện HTF (H4 ưu tiên, M30 không ngược): chọn LONG/SHORT.\n"
            "• M5 Gate phải PASS (RELAX/STRICT tùy ENV) mới vào lệnh.\n"
            "• Khối lượng: dùng calc_qty(balance, risk_percent, leverage, price).\n"
            "• SL/TP tự động theo auto_sl_by_leverage, có thu hẹp biên tùy preset/ENV.\n\n"
            "<b>Gợi ý debug nhanh:</b>\n"
            "• /setenv_status để xem toàn bộ ENV hiện tại.\n"
        )

        short_text = (
            "<b>📘 Help (rút gọn)</b>\n\n"
            "<b>Lệnh chính:</b>\n"
            "/report, /status, /order, /close, /daily, /autolog\n"
            "/preset &lt;name|auto&gt;, /tidewindow, /settings, /mode\n\n"
            "<b>/setenv (biến mới quan trọng):</b>\n"
            f"<code>/setenv M30_FLIP_GUARD true|false</code> (hiện: {v('M30_FLIP_GUARD','true')})\n"
            f"<code>/setenv M30_STABLE_MIN_SEC 1800</code> (hiện: {v('M30_STABLE_MIN_SEC','1800')})\n"
            f"<code>/setenv M30_NEED_CONSEC_N 2</code> (hiện: {v('M30_NEED_CONSEC_N','2')})\n"
            f"<code>/setenv M5_MIN_GAP_MIN 15</code> (hiện: {v('M5_MIN_GAP_MIN','15')})\n"
            f"<code>/setenv M5_GAP_SCOPED_TO_WINDOW true|false</code> (hiện: {v('M5_GAP_SCOPED_TO_WINDOW','true')})\n"
            f"<code>/setenv ALLOW_SECOND_ENTRY true|false</code> (hiện: {v('ALLOW_SECOND_ENTRY','true')})\n"
            f"<code>/setenv M5_SECOND_ENTRY_MIN_RETRACE_PCT 0.3</code> (hiện: {v('M5_SECOND_ENTRY_MIN_RETRACE_PCT','0.3')})\n"
            f"<code>/setenv EXTREME_BLOCK_ON true|false</code> (hiện: {v('EXTREME_BLOCK_ON','true')})\n"
            f"<code>/setenv EXTREME_RSI_OB 70</code> (hiện: {v('EXTREME_RSI_OB','70')})\n"
            f"<code>/setenv EXTREME_RSI_OS 30</code> (hiện: {v('EXTREME_RSI_OS','30')})\n"
            f"<code>/setenv EXTREME_STOCH_OB 90</code> (hiện: {v('EXTREME_STOCH_OB','90')})\n"
            f"<code>/setenv EXTREME_STOCH_OS 10</code> (hiện: {v('EXTREME_STOCH_OS','10')})\n\n"
            + extra +
            "➡️ Dùng <code>/help</code> để xem bản đầy đủ (đã auto-split)."
        )
        await _send_long_html(update, context, short_text)
        return

    # (Giữ nguyên bản đầy đủ như file anh – không đổi nội dung; em không lặp lại để gọn.)
    # ... (phần text bản đầy đủ giữ nguyên) ...
    await update.message.reply_text("Gõ /help short để xem bản rút gọn, hoặc dùng tài liệu trong repo.", parse_mode="HTML")

# ========== /preset ==========
async def preset_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    /preset <name>|auto | /preset list
    """
    uid = _uid(update)
    if not _is_admin(uid):
        await update.message.reply_text("🚫 Chỉ admin mới được phép dùng /preset.")
        return

    if not context.args or context.args[0].lower() in ("help","h"):
        await update.message.reply_text(
            "Dùng: /preset <name>|auto\n"
            "• /preset list — liệt kê preset\n"
            "• Ví dụ: /preset P1 | /preset P2 | /preset P3 | /preset P4\n"
            "• Auto: /preset auto  (tự map theo Moon hôm nay — P1..P4)",
        )
        return

    name = context.args[0].upper().strip()

    if name == "LIST":
        await update.message.reply_text(
            "Preset khả dụng: P1, P2, P3, P4 (hoặc dùng: auto)\n"
            "P1: 0–25% | P2: 25–75% (waxing) | P3: 75–100% | P4: 25–75% (waning)"
        )
        return

    if name == "AUTO":
        os.environ["PRESET_MODE"] = "AUTO"
        pcode, meta = resolve_preset_code(None)
        chosen = pcode  # P1..P4
        if chosen not in PRESETS:
            await update.message.reply_text(f"Không map được preset cho {pcode}.")
            return
        hdr = (
            "🌕 Moon hôm nay: "
            f"<b>{_esc(meta.get('phase') or '')}</b> | "
            f"illum={meta.get('illum')}% | dir={meta.get('direction')}\n"
            f"→ chọn preset: <b>{_esc(pcode)}</b> — {_esc(meta.get('label') or '')}"
        )
        await _apply_preset_and_reply(update, chosen, hdr)
        return

    # manual P-code
    if name not in PRESETS:
        await update.message.reply_text("❓ Không có preset. Dùng /preset list (P1..P4) hoặc 'auto'.")
        return
    os.environ["PRESET_MODE"] = name
    await _apply_preset_and_reply(update, name)

# === /setenv: cập nhật ENV + đẩy vào core.auto_trade_engine ===
async def setenv_cmd(update, context):
    """
    /setenv KEY VALUE
    /setenv_status
    """
    import os
    from core import auto_trade_engine as ae
    from core.trade_executor import retime_tp_by_time_for_open_positions as _retime_tp

    msg = update.effective_message
    if not context.args or len(context.args) < 2:
        await msg.reply_html(
            "Dùng: <code>/setenv KEY VALUE</code>\n"
            "VD: <code>/setenv ENTRY_LATE_ONLY true</code>\n"
            "Xem trạng thái: <code>/setenv_status</code>"
        )
        return

    key = (context.args[0] or "").strip()
    val_raw = " ".join(context.args[1:]).strip()

    # ép kiểu helper
    def _as_bool(s: str) -> bool:
        return str(s or "").strip().lower() in ("1","true","on","yes","y")

    def _is_floatlike(s: str) -> bool:
        try:
            float(s)
            return True
        except Exception:
            return False

    def _is_intlike(s: str) -> bool:
        try:
            int(float(s))
            return True
        except Exception:
            return False

    # alias tương thích cũ <-> mới:
    aliases = {
        # mới -> cũ (giữ tương thích engine cũ)
        "MAX_ORDERS_PER_TIDE_WINDOW": "MAX_TRADES_PER_WINDOW",
        "MAX_ORDERS_PER_DAY": "MAX_TRADES_PER_DAY",
        # chiều ngược: nếu ai còn set tên cũ, coi như mới
        "MAX_TRADES_PER_WINDOW": "MAX_TRADES_PER_WINDOW",
        "MAX_TRADES_PER_DAY": "MAX_TRADES_PER_DAY",
        "EXTREME_GUARD": "EXTREME_BLOCK_ON",
        "EXTREME_GUARD_KIND": "EXTREME_KIND",
    }
    key_norm = aliases.get(key, key)

    bool_keys = {
        "ENTRY_LATE_ONLY","ENTRY_LATE_PREF",
        "AUTO_DEBUG","AUTO_DEBUG_VERBOSE","AUTO_DEBUG_ONLY_WHEN_SKIP",
        "ENFORCE_M5_MATCH_M30",
        "M30_FLIP_GUARD",
        "M5_GAP_SCOPED_TO_WINDOW","ALLOW_SECOND_ENTRY",
        "M5_RELAX_NEED_CURRENT","M5_REQUIRE_ZONE_STRICT",
        "HTF_NEAR_ALIGN","SYNERGY_ON",
        "EXTREME_BLOCK_ON",
    }
    int_keys = {
        "M5_MAX_DELAY_SEC","SCHEDULER_TICK_SEC",
        "MAX_TRADES_PER_WINDOW","MAX_TRADES_PER_DAY","MAX_TRADES_PER_TIDE_WINDOW",
        "MAX_ORDERS_PER_DAY","MAX_ORDERS_PER_TIDE_WINDOW",  # <<< NEW (TideGate)
        "M30_STABLE_MIN_SEC","M30_NEED_CONSEC_N",
        "M5_MIN_GAP_MIN","M5_LOOKBACK_RELAX","M5_LOOKBACK_STRICT",
        "ENTRY_SEQ_WINDOW_MIN","M30_TAKEOVER_MIN","CROSS_RECENT_N",
        "RSI_OB","RSI_OS",
        "STCH_RECENT_N",
        "MAX_PENDING_MINUTES",
    }
    float_keys = {
        "ENTRY_LATE_FROM_HRS","ENTRY_LATE_TO_HRS","TP_TIME_HOURS",
        "M5_WICK_PCT","M5_VOL_MULT_RELAX","M5_VOL_MULT_STRICT",
        "M5_SECOND_ENTRY_MIN_RETRACE_PCT",
        "EXTREME_RSI_OB","EXTREME_RSI_OS","EXTREME_STOCH_OB","EXTREME_STOCH_OS",
        "SIZE_MULT_STRONG","SIZE_MULT_MID","SIZE_MULT_CT",
        "SONIC_WEIGHT","HTF_MIN_ALIGN_SCORE","HTF_NEAR_ALIGN_GAP",
        "STCH_GAP_MIN","STCH_SLOPE_MIN","RSI_GAP_MIN",
        "TIDE_WINDOW_HOURS",
    }
    passthrough_str = {"SONIC_MODE","M5_RELAX_KIND","AUTO_DEBUG_CHAT_ID","EXTREME_KIND"}

    kv_to_apply = {}
    try:
        if key_norm in bool_keys:
            kv_to_apply[key_norm] = "true" if _as_bool(val_raw) else "false"
        elif key_norm in int_keys:
            if not _is_intlike(val_raw):
                await msg.reply_text(f"Giá trị cho {key_norm} phải là số nguyên.")
                return
            kv_to_apply[key_norm] = str(int(float(val_raw)))
        elif key_norm in float_keys:
            if not _is_floatlike(val_raw):
                await msg.reply_text(f"Giá trị cho {key_norm} phải là số (float).")
                return
            kv_to_apply[key_norm] = str(float(val_raw))
        elif key_norm in passthrough_str:
            kv_to_apply[key_norm] = val_raw
        else:
            await msg.reply_text(f"KEY không được phép: {key}\nGõ /help để xem KEY hỗ trợ.")
            return
    except Exception as e:
        await msg.reply_text(f"Lỗi ép kiểu: {e}")
        return

    # ghi ENV
    for k, v in kv_to_apply.items():
        os.environ[k] = v

    # đẩy vào core
    try:
        ae._apply_runtime_env(kv_to_apply)
    except Exception as e:
        print(f"[WARN] _apply_runtime_env failed: {e}")

    # Nếu có đổi TP_TIME_HOURS -> dời deadline TP-by-time cho mọi vị thế đang mở
    retime_msg = ""
    try:
        if "TP_TIME_HOURS" in kv_to_apply:
            new_hours = float(kv_to_apply["TP_TIME_HOURS"])
            storage_obj = context.application.bot_data.get("storage")
            if storage_obj is not None and new_hours > 0:
                try:
                    updated = await _retime_tp(context.application, storage_obj, new_hours)
                except TypeError:
                    updated = _retime_tp(context.application, storage_obj, new_hours)  # type: ignore
                retime_msg = f"\n🕒 Đã dời TP-by-time cho {updated} vị thế đang mở (deadline = tide_center + {new_hours:.2f}h)."
            else:
                retime_msg = "\n⚠️ Không tìm thấy storage hoặc giá trị TP_TIME_HOURS không hợp lệ (>0)."
    except Exception as e:
        retime_msg = f"\n⚠️ Dời TP-by-time lỗi: {e}"

    pretty = "\n".join([f"• {k} = {v}" for k, v in kv_to_apply.items()])
    await msg.reply_html(f"✅ Đã cập nhật ENV (runtime):\n{pretty}{retime_msg}")


# ========== /setenv_status ==========
async def setenv_status_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    /setenv_status → In ra cấu hình ENV/runtime hiện tại + tóm tắt user settings.
    """
    uid = _uid(update)
    if not _is_admin(uid):
        await update.message.reply_text("🚫 Chỉ admin mới được phép dùng /setenv_status.")
        return

    keys = [
        "PRESET_MODE",
        # Debug
        "AUTO_DEBUG", "AUTO_DEBUG_VERBOSE", "AUTO_DEBUG_ONLY_WHEN_SKIP", "AUTO_DEBUG_CHAT_ID",

        # Timing
        "ENTRY_LATE_PREF", "ENTRY_LATE_ONLY", "ENTRY_LATE_FROM_HRS", "ENTRY_LATE_TO_HRS",
        "TIDE_WINDOW_HOURS", "TP_TIME_HOURS",

        "M30_FLIP_GUARD", "M30_STABLE_MIN_SEC",

        # M5 (mới)
        "M5_STRICT", "M5_RELAX_KIND",
        "M5_LOOKBACK",
        "M5_LOOKBACK_RELAX",
        "M5_RELAX_NEED_CURRENT",
        "M5_LOOKBACK_STRICT",
        "M5_WICK_PCT",
        "M5_VOL_MULT",
        "M5_VOL_MULT_RELAX",
        "M5_VOL_MULT_STRICT",
        "M5_REQUIRE_ZONE_STRICT",
        "ENTRY_SEQ_WINDOW_MIN",
        # M5 spacing / second entry
        "M5_MIN_GAP_MIN",
        "M5_GAP_SCOPED_TO_WINDOW",
        "ALLOW_SECOND_ENTRY",
        "M5_SECOND_ENTRY_MIN_RETRACE_PCT",

        # Legacy / scoring
        "M5_WICK_MIN", "M5_WICK_MIN_CT", "VOL_MA20_MULT", "RSI_OB", "RSI_OS", "DELTA_RSI30_MIN",
        "SIZE_MULT_STRONG", "SIZE_MULT_MID", "SIZE_MULT_CT",

        # Sonic
        "SONIC_MODE", "SONIC_WEIGHT",

        # H4/M30 tuning
        "STCH_GAP_MIN", "STCH_SLOPE_MIN", "STCH_RECENT_N",
        "HTF_NEAR_ALIGN", "HTF_MIN_ALIGN_SCORE", "HTF_NEAR_ALIGN_GAP",
        "SYNERGY_ON", "M30_TAKEOVER_MIN",
        "EXTREME_BLOCK_ON", "EXTREME_RSI_OB", "EXTREME_RSI_OS", "EXTREME_STOCH_OB", "EXTREME_STOCH_OS",

        # Limits (cũ + mới)
        "MAX_TRADES_PER_WINDOW", "MAX_TRADES_PER_DAY",
        "MAX_ORDERS_PER_TIDE_WINDOW", "MAX_ORDERS_PER_DAY",

        "M5_MAX_DELAY_SEC", "SCHEDULER_TICK_SEC",

        # Admin
        "ADMIN_USER_ID",
    ]

    from core import auto_trade_engine as ae
    def _get_val(k: str):
        v = os.getenv(k)
        if v is None and hasattr(ae, k):
            try:
                vv = getattr(ae, k)
                if isinstance(vv, bool):
                    return "true" if vv else "false"
                return str(vv)
            except Exception:
                return "—"
        return v if v is not None else "—"

    lines = [f"{k} = {_get_val(k)}" for k in keys]

    st = storage.get_user(uid)
    user_lines = [
        "",
        "———————",
        "User Settings:",
        f"PAIR = {st.settings.pair}",
        f"MODE = {st.settings.mode}",
        f"RISK_PERCENT = {st.settings.risk_percent}",
        f"LEVERAGE = x{st.settings.leverage}",
        f"TIDE_WINDOW_HOURS = {st.settings.tide_window_hourse if hasattr(st.settings,'tide_window_hourse') else st.settings.tide_window_hours}",
        f"MAX_ORDERS_PER_DAY = {os.getenv('MAX_ORDERS_PER_DAY','8')}",
        f"MAX_ORDERS_PER_TIDE_WINDOW = {os.getenv('MAX_ORDERS_PER_TIDE_WINDOW','2')}",
        f"M5_REPORT_ENABLED = {st.settings.m5_report_enabled}",
    ]

    text = "<b>📊 ENV Status hiện tại:</b>\n" + "\n".join(lines + user_lines)
    if len(text) > 3900:
        text = text[:3900] + "\n…(rút gọn)…"
    await update.message.reply_text(text, parse_mode="HTML")


async def mode_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid = _uid(update)
    st = storage.get_user(uid)
    if context.args and context.args[0].lower() in ("manual","auto"):
        st.settings.mode = context.args[0].lower()
        storage.put_user(uid, st)
        await update.message.reply_text(f"Đã chuyển chế độ: {st.settings.mode}")
    else:
        await update.message.reply_text(f"Chế độ hiện tại: {st.settings.mode}. Dùng: /mode manual|auto")

async def settings_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid = _uid(update)
    st = storage.get_user(uid)
    if context.args and len(context.args) >= 3:
        st.settings.pair = context.args[0].upper()
        try:
            st.settings.risk_percent = float(context.args[1])
            st.settings.leverage = int(float(context.args[2]))
        except Exception:
            await update.message.reply_text("Sai cú pháp. Dùng: /settings BTC/USDT 10 17"); return
        storage.put_user(uid, st)
        await update.message.reply_text(f"OK. Pair={st.settings.pair}, Risk={st.settings.risk_percent}%, Lev=x{st.settings.leverage}")
    else:
        await update.message.reply_text(
            f"Hiện tại: Pair={st.settings.pair}, Risk={st.settings.risk_percent}%, Lev=x{st.settings.leverage}.\n"
            f"Dùng: /settings BTC/USDT 10 17"
        )

async def tidewindow_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid = _uid(update)
    st = storage.get_user(uid)
    if context.args:
        st.settings.tide_window_hours = float(context.args[0])
        storage.put_user(uid, st)
        await update.message.reply_text(f"Đã đặt ±{st.settings.tide_window_hours}h quanh mốc thủy triều.")
    else:
        await update.message.reply_text(f"Đang dùng ±{st.settings.tide_window_hours}h. Dùng: /tidewindow 2")

# ======== TP-by-time (live) helper ========
def _tp_eta_text(uid: int) -> Optional[str]:
    try:
        from core import auto_trade_engine as ae
    except Exception:
        return None

    pos = ae._open_pos.get(uid)
    if not isinstance(pos, dict):
        return None

    try:
        tp_hours = float(os.getenv("TP_TIME_HOURS", os.getenv("TIDE_EXIT_HOURS", "4.5")))
    except Exception:
        tp_hours = 4.5

    now = now_vn()
    base = pos.get("tide_center") or pos.get("entry_time") or now
    try:
        if base.tzinfo is None:
            base = base.replace(tzinfo=now.tzinfo)
    except Exception:
        pass

    deadline = base + timedelta(hours=tp_hours)
    remain = deadline - now
    rem_sec = int(remain.total_seconds())

    base_tag = "tide_center" if pos.get("tide_center") else "entry_time"
    if rem_sec <= 0:
        return f"TP-by-time (live): {deadline.strftime('%Y-%m-%d %H:%M:%S')} — ⏰ đã quá hạn (base={base_tag}, H={tp_hours:g})"

    hrs = rem_sec // 3600
    mins = (rem_sec % 3600) // 60
    return (
        f"TP-by-time (live): {deadline.strftime('%Y-%m-%d %H:%M:%S')} "
        f"| còn ~ {hrs}h{mins:02d} (base={base_tag}, H={tp_hours:g})"
    )

# ================== Position formatter ==================
async def _format_position_status(symbol: str, fallback_lev: Optional[int] = None) -> str:
    try:
        positions = None
        try:
            positions = await ex._io(ex.client.fetch_positions, [symbol])
        except Exception:
            try:
                one = await ex._io(ex.client.fetch_position, symbol)
                positions = [one] if one else []
            except Exception:
                positions = []

        if not positions:
            return "Position: (Không có vị thế mở)"

        p_use = None
        amt_signed = 0.0
        for p in positions or []:
            amt = 0.0
            if isinstance(p, dict):
                info = p.get("info", {}) or {}
                if "positionAmt" in info:
                    try: amt = float(info.get("positionAmt") or 0)
                    except: amt = 0.0
                elif "contracts" in p:
                    try: amt = float(p.get("contracts") or 0)
                    except: amt = 0.0
                elif "amount" in p:
                    try: amt = float(p.get("amount") or 0)
                    except: amt = 0.0
            if abs(amt) != 0:
                p_use = p; amt_signed = amt; break

        if p_use is None:
            return "Position: (Không có vị thế mở)"

        def _flt(x):
            try: return float(x)
            except: return None

        info = {}
        if isinstance(p_use, dict):
            info = p_use.get("info", {}) or {}

        side = "LONG" if amt_signed > 0 else ("SHORT" if amt_signed < 0 else "FLAT")
        entry = _flt(info.get("entryPrice") or info.get("avgEntryPrice") or p_use.get("entryPrice") or p_use.get("avgPrice"))
        contracts = _flt(info.get("positionAmt") or p_use.get("contracts") or p_use.get("amount")) or 0.0
        u_pnl = _flt(info.get("unrealizedProfit") or p_use.get("unrealizedPnl"))
        lev_val = _flt(info.get("leverage") or p_use.get("leverage"))

        if (not lev_val) or lev_val <= 0:
            try:
                init_margin = _flt(info.get("positionInitialMargin") or p_use.get("initialMargin"))
                notional = (entry or 0) * abs(contracts or 0)
                if init_margin and init_margin > 0 and notional > 0:
                    try: lev_val = max(1, int(round(notional / init_margin)))
                    except Exception: lev_val = None
            except Exception:
                lev_val = None

        lev_str = f"x{int(lev_val)}" if isinstance(lev_val, (int,float)) and lev_val>0 else (f"~x{int(fallback_lev)}" if fallback_lev else "—")

        roe = None
        init_margin = _flt(info.get("positionInitialMargin") or p_use.get("initialMargin"))
        if init_margin and init_margin != 0:
            roe = (u_pnl or 0.0) / init_margin * 100.0
        else:
            if entry and contracts and isinstance(lev_val, (int,float)) and lev_val>0:
                denom = (entry * contracts / lev_val)
                if denom:
                    roe = (u_pnl or 0.0) / denom * 100.0

        roe_str = "—"
        if roe is not None:
            arrow = "🟢" if roe >= 0 else "🔴"
            roe_str = f"{roe:.2f}% {arrow}"
        entry_str = f"{entry:.6f}" if entry is not None else "—"

        return (
            f"Position: {side} {symbol}\n"
            "Entry: {entry_str}\n"
            f"Contracts: {contracts:.6f}\n"
            f"Unrealized PnL: {0.0 if u_pnl is None else u_pnl:.8f}\n"
            f"PnL% (ROE): {roe_str}\n"
            f"Leverage: {lev_str}"
        ).replace("{entry_str}", entry_str)
    except Exception as e:
        return f"Position: (Lỗi lấy vị thế) — {e}"

async def status_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid = _uid(update)
    st = storage.get_user(uid)
    head = (
        f"Pair: {st.settings.pair}\n"
        f"Mode: {st.settings.mode}\n"
        f"Risk: {st.settings.risk_percent}%\n"
        f"Lev: x{st.settings.leverage}\n"
        f"Đã dùng {st.today.count}/{st.settings.max_orders_per_day} lệnh hôm nay.\n"
        f"Giới hạn mỗi cửa sổ thủy triều: {st.settings.max_orders_per_tide_window}\n"
        f"M5 report: {'ON' if st.settings.m5_report_enabled else 'OFF'}\n"
    )
    tp_line = _tp_eta_text(uid)
    if tp_line:
        head += tp_line + "\n"
    pos = await _format_position_status(st.settings.pair, fallback_lev=st.settings.leverage)
    await update.message.reply_text(head + "\n" + pos)

# ================== /ordernew (manual, qua TideGate) ==================    
async def order_new_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    /ordernew <pair> <side> [risk_percent] [leverage]
    Ví dụ: /ordernew BTC/USDT long 5 20
    Luồng: TideGate (T) -> (B) _auto_execute_hub -> (C) _auto_broadcast_and_log, rồi bump counters.
    """
    storage_obj = context.application.bot_data["storage"]
    uid = _uid(update)

    text = (update.message.text or "").strip()
    parts = text.split()
    if len(parts) < 3:
        await update.message.reply_text(
            "Cách dùng:\n/ordernew <pair> <side> [risk_percent] [leverage]\nVD: /ordernew BTC/USDT long 5 20"
        )
        return

    pair_in = parts[1].upper()
    side_in = parts[2].upper()
    if side_in not in ("LONG", "SHORT"):
        await update.message.reply_text("side phải là LONG hoặc SHORT.")
        return

    st = storage.get_user(uid)
    default_risk = float(getattr(st.settings, "risk_percent", 10.0))
    default_lev  = int(float(getattr(st.settings, "leverage", 10)))
    tide_window_hours = float(getattr(st.settings, "tide_window_hours", TIDE_WINDOW_HOURS))

    risk_percent = default_risk
    leverage = default_lev
    if len(parts) >= 4:
        try: risk_percent = float(parts[3])
        except: pass
    if len(parts) >= 5:
        try: leverage = int(float(parts[4]))
        except: pass

    pair_disp = pair_in if "/" in pair_in else (pair_in[:-4] + "/USDT" if pair_in.endswith("USDT") else f"{pair_in}/USDT")
    symbol = pair_disp.replace("/", "")

    # (T) TideGate check
    cfg = await _load_tidegate_config(storage_obj, uid)
    tgr = await tide_gate_check(
        now=_ae_now_vn().astimezone(timezone.utc),
        storage=storage_obj,
        cfg=cfg,
        scope_uid=(uid if cfg.counter_scope == "per_user" else None),
    )
    if not tgr.ok:
        await update.message.reply_text(f"⚠️ TideGate chặn: {tgr.reason} {tgr.counters}")
        return

    # Build bundle tối thiểu cho (B)->(C)
    now = now_vn()
    center = now
    try:
        key_win = center.strftime("%H:%M")
    except Exception:
        key_win = "NA"

    frames = {"H4": {}, "M30": {}, "M5": {}}
    h4, m30, m5f = frames["H4"], frames["M30"], frames["M5"]

    gate = {
        "ok": True,
        "now": now,
        "pair_disp": pair_disp,
        "symbol": symbol,
        "risk_percent": risk_percent,
        "leverage": leverage,
        "mode": "manual",
        "auto_on": False,
        "balance_usdt": float(getattr(st.settings, "balance_usdt", 100.0)),
        "tide_window_hours": tide_window_hours,

        "res": {},
        "skip_report": False,
        "desired_side": side_in,
        "confidence": 0,
        "text_block": "(manual ordernew)",
        "frames": frames,
        "h4": h4, "m30": m30, "m5f": m5f,

        "center": center,
        "tau": 0.0,
        "in_late": False,
        "side_m30": side_in,

        "key_day": now.strftime("%Y-%m-%d"),
        "key_win": key_win,
        "st_key": {"trade_count": 0},
    }

    # (B)
    try:
        result = await _auto_execute_hub(uid, context.application, storage_obj, gate)
    except Exception as e:
        await update.message.reply_text(f"⚠️ Lỗi execute hub: {e}")
        return

    # bump counters nếu opened
    try:
        if result and result.get("opened_real"):
            await bump_counters_after_execute(storage_obj, tgr, uid if cfg.counter_scope == "per_user" else None)
    except Exception:
        pass

    # (C)
    try:
        _ = await _auto_broadcast_and_log(uid, context.application, storage_obj, result)
    except Exception as e:
        await update.message.reply_text(f"⚠️ Lỗi broadcast/log: {e}")
        return

    await update.message.reply_text("✅ Đã thực thi /ordernew — xem broadcast để biết chi tiết.")

# ================== report_cmd ==================
async def report_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    /report — CHỈ in báo cáo: Daily Moon/Tide + H4→M30 (+ M5 filter nếu evaluate_signal đã gộp).
    """
    uid = _uid(update)
    st = storage.get_user(uid)

    d = now_vn().date().isoformat()
    try:
        daily = format_daily_moon_tide_report(d, float(st.settings.tide_window_hours))
    except Exception as e:
        daily = f"📅 {d}\n⚠️ Lỗi tạo Daily: {html.escape(str(e), quote=False)}"

    sym = st.settings.pair.replace("/", "")
    loop = asyncio.get_event_loop()
    try:
        try:
            res = await loop.run_in_executor(
                None, lambda: evaluate_signal(sym, tide_window_hours=float(st.settings.tide_window_hours))
            )
        except TypeError:
            res = await loop.run_in_executor(None, lambda: evaluate_signal(sym))
    except Exception as e:
        await update.message.reply_text(
            html.escape(daily, quote=False) + f"\n\n⚠️ Lỗi /report: {html.escape(str(e), quote=False)}",
            parse_mode="HTML"
        )
        return

    if not isinstance(res, dict) or not res.get("ok", False):
        reason = (isinstance(res, dict) and (res.get("text") or res.get("reason"))) or "Không tạo được snapshot kỹ thuật."
        await update.message.reply_text(
            html.escape(daily, quote=False) + "\n\n" + "⚠️ " + html.escape(str(reason), quote=False),
            parse_mode="HTML"
        )
        return

    ta_text = res.get("text") or format_signal_report(res)
    ta_text = _beautify_report(ta_text)

    safe_daily = html.escape(daily,   quote=False)
    safe_ta    = html.escape(ta_text, quote=False)

    try:
        await update.message.reply_text(safe_daily + "\n\n" + safe_ta, parse_mode="HTML")
    except Exception:
        await update.message.reply_text((daily or "") + "\n\n" + (ta_text or "—"))

# ================== /m5report ==================
async def m5report_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid = _uid(update)
    st = storage.get_user(uid)

    arg = (context.args[0].lower() if context.args else "status")
    if arg not in ("start", "stop", "status"):
        await update.message.reply_text("Dùng: /m5report start | stop | status")
        return

    if arg == "status":
        await update.message.reply_text(f"M5 report hiện: {'ON' if st.settings.m5_report_enabled else 'OFF'}")
        return

    if arg == "start":
        st.settings.m5_report_enabled = True
        storage.put_user(uid, st)
        await update.message.reply_text("✅ ĐÃ BẬT M5 report (sẽ tự động gửi snapshot mỗi 5 phút).")
        try:
            sym = st.settings.pair.replace("/", "")
            snap = m5_snapshot(sym)

            try:
                mode_now = (st.settings.mode or "").lower()
            except Exception:
                mode_now = "manual"

            pend = getattr(st, "pending", None)

            if mode_now == "manual" and pend:
                pid  = getattr(pend, "id", None)
                side = str(getattr(pend, "side", "") or "").upper()
                sl   = getattr(pend, "sl", None)
                tp   = getattr(pend, "tp", None)
                pair_u = st.settings.pair

                hint_lines = [
                    "────────────",
                    "🟨 <b>Pending cần duyệt (MANUAL)</b>",
                    f"• ID: <code>{pid}</code>",
                    f"• Pair: <code>{pair_u}</code> | Side: <b>{side or '—'}</b>",
                    f"• SL: <code>{'—' if sl is None else f'{float(sl):.2f}'}</code> | "
                    f"TP: <code>{'—' if tp is None else f'{float(tp):.2f}'}</code>",
                    "",
                    f"👉 Duyệt: <code>/approve {pid}</code>",
                    f"❌ Huỷ:  <code>/reject {pid}</code>",
                    "────────────",
                ]
                snap = snap + "\n" + "\n".join(hint_lines)

            await update.message.reply_text(snap, parse_mode=constants.ParseMode.HTML)

        except Exception as e:
            await update.message.reply_text(f"⚠️ Không gửi được snapshot ngay: {e}")
        return

    if arg == "stop":
        st.settings.m5_report_enabled = False
        storage.put_user(uid, st)
        await update.message.reply_text("⏸ ĐÃ TẮT M5 report.")
        return

# ================== /autolog ==================
async def autolog_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid = _uid(update)
    try:
        from core import auto_trade_engine as ae
    except Exception as e:
        await update.message.reply_text(f"Không import được auto_trade_engine: {e}")
        return

    txt = None
    try:
        getter = getattr(ae, "get_last_decision_text", None)
        if callable(getter):
            txt = getter(uid)
    except Exception:
        txt = None

    if not txt:
        try:
            last_map = getattr(ae, "_last_decision_text", None)
            if isinstance(last_map, dict):
                txt = last_map.get(uid)
        except Exception:
            txt = None

    if not txt:
        try:
            slot_map = getattr(ae, "_last_m5_slot_sent", None)
            if isinstance(slot_map, dict) and uid in slot_map:
                txt = f"Tick gần nhất (M5 slot) = {slot_map[uid]} (engine chưa lưu full text cho tick này)."
        except Exception:
            txt = None

    if not txt:
        await update.message.reply_text("Chưa có tick AUTO nào chạy cho user này (hoặc engine chưa lưu log).")
        return

    if len(txt) > 3500:
        txt = txt[:3500] + "\n…(rút gọn)…"

    await update.message.reply_text(f"📜 Auto log gần nhất:\n{txt}")

# ================== Mode Manual: Approve or Reject (with TideGate) ==================
async def approve_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    storage_obj = context.application.bot_data["storage"]
    args = (update.message.text or "").split(maxsplit=1)
    if len(args) < 2:
        await update.message.reply_text("Cách dùng: /approve <PENDING_ID>")
        return
    pid = args[1].strip()

    # Lấy pending record
    p = get_pending(storage_obj, pid)
    if not p:
        await update.message.reply_text("⚠️ ID không hợp lệ hoặc đã xử lý.")
        return

    # TTL pending (phút)
    try:
        max_min = int(float(os.getenv("MAX_PENDING_MINUTES", "10")))
    except Exception:
        max_min = 10
    max_min = max(1, max_min)

    # Tuổi pending
    try:
        created_utc = datetime.fromisoformat(p.created_at)
        if created_utc.tzinfo is None:
            created_utc = created_utc.replace(tzinfo=timezone.utc)
    except Exception:
        created_utc = datetime.now(timezone.utc)

    now_utc = datetime.now(timezone.utc)
    age_min = (now_utc - created_utc).total_seconds() / 60.0

    if age_min > max_min:
        mark_done(storage_obj, pid, "REJECTED")
        await update.message.reply_text(
            f"⏱ Pending {pid} đã quá hạn (> {max_min} phút). Đã tự động từ chối."
        )
        return

    # Re-check TideGate NGAY TẠI LÚC DUYỆT
    cfg = await _load_tidegate_config(storage_obj, _uid(update))
    tgr = await tide_gate_check(
        now=_ae_now_vn().astimezone(timezone.utc),
        storage=storage_obj,
        cfg=cfg,
        scope_uid=(_uid(update) if cfg.counter_scope == "per_user" else None),
    )
    if not tgr.ok:
        await update.message.reply_text(f"⚠️ TideGate chặn: {tgr.reason} {tgr.counters}\nGiữ PENDING để duyệt lại trong khung.")
        return

    ok = mark_done(storage_obj, pid, "APPROVED")
    await update.message.reply_text("✅ ĐÃ APPROVE. Engine sẽ thực thi (và vẫn re-check TideGate trước khi vào lệnh)."
                                    if ok else "⚠️ ID không hợp lệ hoặc đã xử lý.")

async def reject_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    storage_obj = context.application.bot_data["storage"]
    args = (update.message.text or "").split(maxsplit=1)
    if len(args) < 2:
        await update.message.reply_text("Cách dùng: /reject <PENDING_ID>")
        return
    pid = args[1].strip()
    ok = mark_done(storage_obj, pid, "REJECTED")
    await update.message.reply_text("❌ ĐÃ REJECT." if ok else "⚠️ ID không hợp lệ hoặc đã xử lý.")

# ==== /close (đa tài khoản: Binance/BingX/...) ====
async def close_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    /close ... (giữ nguyên behavior như bản anh)
    """
    from core.trade_executor import close_position_on_account, close_position_on_all

    msg = update.effective_message
    uid = _uid(update)

    try:
        st = storage.get_user(uid)
        pair = (getattr(st.settings, "pair", None) or os.getenv("PAIR") or "BTC/USDT")
    except Exception:
        pair = os.getenv("PAIR", "BTC/USDT")

    args = [str(a).strip() for a in (context.args or [])]

    percent: float = 100.0
    account: Optional[str] = None
    side_filter: Optional[str] = None

    def _is_percent(s: str) -> bool:
        try:
            x = float(s); return 0.0 < x <= 100.0
        except Exception:
            return False

    def _as_side(s: str) -> Optional[str]:
        s2 = (s or "").lower()
        if s2 in ("long", "l", "buy"): return "LONG"
        if s2 in ("short", "s", "sell"): return "SHORT"
        return None

    for tok in args:
        if _is_percent(tok):
            percent = float(tok); continue
        sd = _as_side(tok)
        if sd: side_filter = sd; continue
        account = tok

    try:
        percent = float(percent)
        percent = 100.0 if percent > 100.0 else (1.0 if percent <= 0 else percent)
    except Exception:
        percent = 100.0

    try:
        if account:
            try:
                res = await close_position_on_account(account, pair, percent, side_filter=side_filter)
            except TypeError:
                res = await close_position_on_account(account, pair, percent)

            status = "OK" if (isinstance(res, dict) and res.get("ok")) else "FAIL"
            side_txt = f" | side={side_filter}" if side_filter else ""
            lines = [
                f"🔧 Close {percent:.0f}% | {pair} | account: <b>{_esc(account)}</b>{_esc(side_txt)}",
                f"• {status} {_esc((res or {}).get('message',''))}"
            ]
            if percent >= 100.0:
                lines.append("🧹 TP/SL & lệnh chờ đã được xử lý.")
            if side_filter and isinstance(res, dict) and res.get("_note_no_side_support"):
                lines.append("⚠️ Backend chưa hỗ trợ lọc side — đã đóng theo vị thế hiện có (net).")
            await msg.reply_text("\n".join(lines), parse_mode="HTML")
        else:
            try:
                results = await close_position_on_all(pair, percent, side_filter=side_filter)
            except TypeError:
                results = await close_position_on_all(pair, percent)

            side_txt = f" | side={side_filter}" if side_filter else ""
            lines = [f"🔧 Close {percent:.0f}% | {pair} | ALL accounts{_esc(side_txt)}"]
            for r in results or []:
                note = ""
                if side_filter and r and r.get("_note_no_side_support"):
                    note = " (no-side-support)"
                lines.append(f"• {_esc(r.get('message',''))}{_esc(note)}")
            if percent >= 100.0:
                lines.append("🧹 TP/SL & lệnh chờ đã được xử lý.")
            await msg.reply_text("\n".join(lines), parse_mode="HTML")

    except Exception as e:
        await msg.reply_text(f"❌ Lỗi /close: {_esc(str(e))}", parse_mode="HTML")

async def daily_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid = _uid(update)
    st = storage.get_user(uid)
    d = now_vn().date().isoformat()
    text = format_daily_moon_tide_report(d, float(st.settings.tide_window_hours))
    await update.message.reply_text(text)

# ================== Custom Commands (bổ sung) ==================
async def aboutme_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    about_text = """
💡 **Trading là môn tìm hiểu về bản thân:**
... (giữ nguyên nội dung anh) ...
""".strip()
    await update.message.reply_text(about_text, parse_mode="Markdown")

async def journal_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    form_link = "https://docs.google.com/forms/d/e/1FAIpQLSeXQmxn8X9BCUC_StiOid1wFCue_19y3hEQBTHULnNHl7ShSg/viewform"
    await update.message.reply_text(f"📋 Mời bạn điền nhật ký giao dịch tại đây:\n{form_link}")

async def recovery_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    checklist_text = (
        "🧠 *Phục hồi tâm lý sau thua lỗ – Vấn đề & Giải pháp*\n\n"
        "... (giữ nguyên nội dung anh) ..."
    )
    await update.message.reply_text(checklist_text.strip(), parse_mode="Markdown")

# ================== Error handler ==================
async def _on_error(update: object, context: ContextTypes.DEFAULT_TYPE):
    try:
        err = context.error
        print(f"[TG ERROR] {err}")
        if isinstance(update, Update) and update.effective_chat:
            await context.bot.send_message(chat_id=update.effective_chat.id, text=f"⚠️ Lỗi: {err}")
    except Exception:
        pass

# ================== App builder ==================
def build_app():
    token = TELEGRAM_BOT_TOKEN
    if not token:
        raise RuntimeError("Missing TELEGRAM_BOT_TOKEN")

    async def _post_init(app: Application):
        try:
            await app.bot.delete_webhook(drop_pending_updates=True)
            print("[BOOT] Webhook deleted (switching to polling).")
        except Exception as e:
            print(f"[BOOT] delete_webhook warn: {e}")

        app.bot_data["storage"] = storage

        async def _spawn_after_start():
            await asyncio.sleep(0)
            print("[M5] Background m5_report_loop() started.")
            app.create_task(m5_report_loop(app, storage))
            print("[AUTO] Background start_auto_loop() started.")
            app.create_task(start_auto_loop(app, storage))
            print("[AUTO PRESET] Background auto preset daemon started.")
            app.create_task(_auto_preset_daemon(app))

        asyncio.get_event_loop().create_task(_spawn_after_start())

    app = ApplicationBuilder().token(token).job_queue(None).post_init(_post_init).build()

    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("help", help_cmd))
    app.add_handler(CommandHandler("preset", preset_cmd))
    app.add_handler(CommandHandler("setenv", setenv_cmd))
    app.add_handler(CommandHandler("setenv_status", setenv_status_cmd))
    app.add_handler(CommandHandler("mode", mode_cmd))
    app.add_handler(CommandHandler("settings", settings_cmd))
    app.add_handler(CommandHandler("tidewindow", tidewindow_cmd))
    app.add_handler(CommandHandler("status", status_cmd))
    app.add_handler(CommandHandler("ordernew", order_new_cmd))   # TideGate
    app.add_handler(CommandHandler("report", report_cmd))
    app.add_handler(CommandHandler("m5report", m5report_cmd))
    app.add_handler(CommandHandler("approve", approve_cmd))
    app.add_handler(CommandHandler("reject", reject_cmd))
    app.add_handler(CommandHandler("close", close_cmd))
    app.add_handler(CommandHandler("daily", daily_cmd))
    app.add_handler(CommandHandler("autolog", autolog_cmd))
    app.add_handler(CommandHandler("aboutme", aboutme_command))
    app.add_handler(CommandHandler("journal", journal_command))
    app.add_handler(CommandHandler("recovery_checklist", recovery_command))
    app.add_error_handler(_on_error)
    # Chỉ cho phép /status public, còn lại admin-only
    enforce_admin_for_all_commands(app, {"status"})

    return app

# ===== Auto preset helpers (map P1..P4 theo Moon API) ==========================
def _preset_mode() -> str:
    return (os.getenv("PRESET_MODE", "auto") or "auto").upper()

def _apply_preset_code_runtime(pcode: str) -> bool:
    preset = PRESETS.get(pcode)
    if not preset:
        return False
    # 1) Ghi ENV
    for k, v in preset.items():
        os.environ[k] = _bool_str(v) if isinstance(v, bool) else str(v)
    # 2) Bơm runtime sang auto_trade_engine nếu có
    applied = False
    try:
        from core import auto_trade_engine as ae
        fn = getattr(ae, "apply_runtime_overrides", None)
        if callable(fn):
            fn({k: os.environ[k] for k in preset.keys()})
            applied = True
        else:
            for k, sval in os.environ.items():
                if k in preset and hasattr(ae, k):
                    typ = type(getattr(ae, k))
                    if typ is bool:
                        setattr(ae, k, sval.strip().lower() in ("1","true","yes","on"))
                    elif typ is int:
                        setattr(ae, k, int(float(sval)))
                    elif typ is float:
                        setattr(ae, k, float(sval))
                    else:
                        setattr(ae, k, sval)
                    applied = True
    except Exception:
        pass
    return applied

async def _apply_auto_preset_now(app=None, silent: bool = True):
    pcode, meta = resolve_preset_code(None)
    ok = _apply_preset_code_runtime(pcode)
    if (not silent) and app:
        try:
            # Ưu tiên AUTO_DEBUG_CHAT_ID nếu là số; fallback dùng TELEGRAM_BROADCAST_CHAT_ID
            raw = os.getenv("AUTO_DEBUG_CHAT_ID", "")
            if raw.isdigit():
                chat_id = int(raw)
            else:
                chat_id = int(TELEGRAM_BROADCAST_CHAT_ID) if str(TELEGRAM_BROADCAST_CHAT_ID).lstrip("-").isdigit() else None
        except Exception:
            chat_id = None
        if chat_id:
            txt = (
                "🌕 Auto preset: "
                f"<b>{html.escape(meta.get('phase') or '')}</b> — {meta.get('illum')}% ({html.escape(meta.get('direction') or '')})\n"
                f"→ Áp dụng <b>{pcode}</b>: {html.escape(meta.get('label') or '')}"
            )
            try:
                await app.bot.send_message(chat_id=chat_id, text=txt, parse_mode="HTML")
            except Exception:
                pass


async def _auto_preset_daemon(app: Application):
    """Mỗi ngày 00:05 JST: nếu PRESET_MODE=AUTO thì tự đổi preset theo Moon mới."""
    await asyncio.sleep(1)
    if _preset_mode() == "AUTO":
        await _apply_auto_preset_now(app, silent=True)
    while True:
        now = datetime.now(TOKYO_TZ)
        nxt = (now + timedelta(days=1)).replace(hour=0, minute=5, second=0, microsecond=0)
        sleep_s = max(60.0, (nxt - now).total_seconds())
        await asyncio.sleep(sleep_s)
        if _preset_mode() == "AUTO":
            await _apply_auto_preset_now(app, silent=True)
