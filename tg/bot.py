# ----------------------- tg/bot.py -----------------------
from __future__ import annotations
import os, asyncio, html
from datetime import datetime, timedelta
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

from utils.storage import Storage
from utils.time_utils import now_vn, TOKYO_TZ
from strategy.signal_generator import evaluate_signal, tide_window_now
from strategy.m5_strategy import m5_snapshot, m5_entry_check
from core.trade_executor import ExchangeClient, calc_qty, auto_sl_by_leverage
from core.trade_executor import close_position_on_all, close_position_on_account # ==== /close (đa tài khoản: Binance/BingX/...) ====
from tg.formatter import format_signal_report, format_daily_moon_tide_report
from core.approval_flow import create_pending

# Vòng nền
from core.auto_trade_engine import start_auto_loop
from core.m5_reporter import m5_report_loop

# NEW: dùng resolver P1–P4 theo %illum + hướng
from data.moon_tide import resolve_preset_code

# ================== Global state ==================
storage = Storage()
ex = ExchangeClient()

# ==== QUOTA helpers: 2 lệnh / cửa sổ thủy triều, 8 lệnh / ngày (gộp mọi mode) ====
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

def _fmt_exec_broadcast(
    *, pair: str, side: str, acc_name: str, ex_id: str,
    lev: int, risk: float, qty: float, entry_spot: float,
    sl: float | None, tp: float | None,
    tide_label: str | None = None, mode_label: str = "AUTO",
) -> str:
    lines = [
        f"🚀 <b>EXECUTED</b> | <b>{_esc(pair)}</b> <b>{_esc(side.upper())}</b>",
        f"• Mode: {mode_label}",
        f"• Account: {_esc(acc_name)} ({_esc(ex_id)})",
        f"• Risk {risk:.1f}% | Lev x{lev}",
        f"• Entry(SPOT)≈{entry_spot:.2f} | Qty={qty:.6f}",
        f"• SL={sl:.2f}" if sl else "• SL=—",
        f"• TP={tp:.2f}" if tp else "• TP=—",
        f"• Tide: {tide_label}" if tide_label else "",
    ]
    return "\n".join([l for l in lines if l])


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
        "M5_SECOND_ENTRY_MIN_RETRACE_PCT": 0.3, # retrace % tối thiểu để entry lần 2

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
        "M5_SECOND_ENTRY_MIN_RETRACE_PCT": 0.3, # retrace % tối thiểu để entry lần 2
        
        
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
        "M5_SECOND_ENTRY_MIN_RETRACE_PCT": 0.3, # retrace % tối thiểu để entry lần 2
        
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
        "M5_SECOND_ENTRY_MIN_RETRACE_PCT": 0.3, # retrace % tối thiểu để entry lần 2
        
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
        "• <code>/close</code> hoặc <code>/close 100</code> — Đóng toàn bộ & hủy TP/SL đang treo.\n"
        "• <code>/close 50</code> — Đóng 50% vị thế, vẫn giữ TP/SL phần còn lại.\n"
        "• <code>/close 30 bingx_test</code> — Đóng 30% trên account bingx_test.\n\n"
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
        "/preset <name>|auto — preset theo Moon Phase (P1–P4)\n"
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
        # Khối bổ sung (đã escape HTML & gọn để không vượt giới hạn Telegram)
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

    # BẢN ĐẦY ĐỦ — gom nhóm theo format anh yêu cầu, auto-split
    text = (
        "<b>📘 Hướng dẫn vận hành & DEBUG</b>\n\n"
        "<b>Command chính:</b>\n"
        "/aboutme, /journal, /recovery_checklist\n"
        "/mode, /settings, /tidewindow\n"
        "/report, /status, /order, /approve, /reject, /close\n"
        "/m5report start|stop|status, /daily, /autolog, /preset name|auto\n"
        "/setenv KEY VALUE, /setenv_status\n"
        "\n"

        "<b>ENTRY timing (Tide/Late):</b>\n"
        f"<code>/setenv ENTRY_LATE_ONLY true|false</code> (hiện: {v('ENTRY_LATE_ONLY','true')})\n"
        f"<code>/setenv ENTRY_LATE_FROM_HRS 0.5</code> (hiện: {v('ENTRY_LATE_FROM_HRS','0.5')})\n"
        f"<code>/setenv ENTRY_LATE_TO_HRS 2.5</code> (hiện: {v('ENTRY_LATE_TO_HRS','2.5')})\n"
        f"<code>/setenv TIDE_WINDOW_HOURS 2.5</code> (hiện: {v('TIDE_WINDOW_HOURS','2.5')})\n"
        f"<code>/setenv TP_TIME_HOURS 5.5</code> (hiện: {v('TP_TIME_HOURS','5.5')})\n\n"

        "<b>M30 Flip-guard & ổn định:</b>\n"
        f"<code>/setenv M30_FLIP_GUARD true|false</code> (hiện: {v('M30_FLIP_GUARD','true')})\n"
        f"<code>/setenv M30_STABLE_MIN_SEC 1800</code> (hiện: {v('M30_STABLE_MIN_SEC','1800')})\n"
        f"<code>/setenv M30_NEED_CONSEC_N 2</code> (hiện: {v('M30_NEED_CONSEC_N','2')})\n\n"

        "<b>M5 spacing & second entry:</b>\n"
        f"<code>/setenv M5_MIN_GAP_MIN 15</code> (hiện: {v('M5_MIN_GAP_MIN','15')})\n"
        f"<code>/setenv M5_GAP_SCOPED_TO_WINDOW true|false</code> (hiện: {v('M5_GAP_SCOPED_TO_WINDOW','true')})\n"
        f"<code>/setenv ALLOW_SECOND_ENTRY true|false</code> (hiện: {v('ALLOW_SECOND_ENTRY','true')})\n"
        f"<code>/setenv M5_SECOND_ENTRY_MIN_RETRACE_PCT 0.3</code> (hiện: {v('M5_SECOND_ENTRY_MIN_RETRACE_PCT','0.3')})\n\n"

        "<b>Extreme-guard (RSI/Stoch):</b>\n"
        f"<code>/setenv EXTREME_BLOCK_ON true|false</code> (hiện: {v('EXTREME_BLOCK_ON','true')})\n"
        f"<code>/setenv EXTREME_RSI_OB 70</code> (hiện: {v('EXTREME_RSI_OB','70')})\n"
        f"<code>/setenv EXTREME_RSI_OS 30</code> (hiện: {v('EXTREME_RSI_OS','30')})\n"
        f"<code>/setenv EXTREME_STOCH_OB 90</code> (hiện: {v('EXTREME_STOCH_OB','90')})\n"
        f"<code>/setenv EXTREME_STOCH_OS 10</code> (hiện: {v('EXTREME_STOCH_OS','10')})\n\n"

        "<b>HTF tunings & synergy:</b>\n"
        f"<code>/setenv STCH_GAP_MIN 3</code> (hiện: {v('STCH_GAP_MIN','3')})\n"
        f"<code>/setenv STCH_SLOPE_MIN 2</code> (hiện: {v('STCH_SLOPE_MIN','2')})\n"
        f"<code>/setenv STCH_RECENT_N 3</code> (hiện: {v('STCH_RECENT_N','3')})\n"
        f"<code>/setenv HTF_NEAR_ALIGN true|false</code> (hiện: {v('HTF_NEAR_ALIGN','true')})\n"
        f"<code>/setenv HTF_MIN_ALIGN_SCORE 6.5</code> (hiện: {v('HTF_MIN_ALIGN_SCORE','6.5')})\n"
        f"<code>/setenv HTF_NEAR_ALIGN_GAP 2.0</code> (hiện: {v('HTF_NEAR_ALIGN_GAP','2.0')})\n"
        f"<code>/setenv SYNERGY_ON true|false</code> (hiện: {v('SYNERGY_ON','true')})\n"
        f"<code>/setenv M30_TAKEOVER_MIN 0</code> (hiện: {v('M30_TAKEOVER_MIN','0')})\n\n"
        f"<code>/setenv CROSS_RECENT_N 2</code> (hiện: {v('CROSS_RECENT_N','2')})\n"
        f"<code>/setenv RSI_GAP_MIN 2.0</code> (hiện: {v('RSI_GAP_MIN','2.0')})\n\n"

        "<b>Sonic & M5 filters:</b>\n"
        f"<code>/setenv SONIC_MODE weight|off</code> (hiện: {v('SONIC_MODE','weight')})\n"
        f"<code>/setenv SONIC_WEIGHT 1.0</code> (hiện: {v('SONIC_WEIGHT','1.0')})\n"
        f"<code>/setenv M5_STRICT true|false</code> (hiện: {v('M5_STRICT','false')})\n"
        f"<code>/setenv M5_RELAX_KIND either|rsi_only|candle_only</code> (hiện: {v('M5_RELAX_KIND','either')})\n"
        f"<code>/setenv M5_LOOKBACK_RELAX 3</code> (hiện: {v('M5_LOOKBACK_RELAX','3')})\n"
        f"<code>/setenv M5_RELAX_NEED_CURRENT true|false</code> (hiện: {v('M5_RELAX_NEED_CURRENT','false')})\n"
        f"<code>/setenv M5_LOOKBACK_STRICT 6</code> (hiện: {v('M5_LOOKBACK_STRICT','6')})\n"
        f"<code>/setenv M5_WICK_PCT 0.50</code> (hiện: {v('M5_WICK_PCT','0.50')})\n"
        f"<code>/setenv M5_VOL_MULT_RELAX 1.0</code> (hiện: {v('M5_VOL_MULT_RELAX','1.0')})\n"
        f"<code>/setenv M5_VOL_MULT_STRICT 1.1</code> (hiện: {v('M5_VOL_MULT_STRICT','1.1')})\n"
        f"<code>/setenv M5_REQUIRE_ZONE_STRICT true|false</code> (hiện: {v('M5_REQUIRE_ZONE_STRICT','true')})\n"
        f"<code>/setenv ENTRY_SEQ_WINDOW_MIN 30</code> (hiện: {v('ENTRY_SEQ_WINDOW_MIN','30')})\n\n"

        "<b>Legacy/khác:</b>\n"
        f"<code>/setenv RSI_OB 65</code> (hiện: {v('RSI_OB','65')})\n"
        f"<code>/setenv RSI_OS 35</code> (hiện: {v('RSI_OS','35')})\n"
        f"<code>/setenv DELTA_RSI30_MIN 10</code> (hiện: {v('DELTA_RSI30_MIN','10')})\n"
        f"<code>/setenv SIZE_MULT_STRONG 1.0</code> (hiện: {v('SIZE_MULT_STRONG','1.0')})\n"
        f"<code>/setenv SIZE_MULT_MID 0.7</code> (hiện: {v('SIZE_MULT_MID','0.7')})\n"
        f"<code>/setenv SIZE_MULT_CT 0.4</code> (hiện: {v('SIZE_MULT_CT','0.4')})\n"
        f"<code>/setenv MAX_TRADES_PER_WINDOW 2</code> (hiện: {v('MAX_TRADES_PER_WINDOW','2')})\n"
        f"<code>/setenv MAX_TRADES_PER_DAY 8</code> (hiện: {v('MAX_TRADES_PER_DAY','8')})\n"
        f"<code>/setenv M5_MAX_DELAY_SEC 60</code> (hiện: {v('M5_MAX_DELAY_SEC','60')})\n"
        f"<code>/setenv SCHEDULER_TICK_SEC 2</code> (hiện: {v('SCHEDULER_TICK_SEC','2')})\n"
        "\n💡 Mẹo: dùng <code>/help short</code> để xem nhanh."
    )

    await _send_long_html(update, context, text)

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
    from core import auto_trade_engine as ae

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
        try: float(s); return True
        except: return False

    def _is_intlike(s: str) -> bool:
        try: int(float(s)); return True
        except: return False

    # alias tương thích cũ:
    aliases = {
        "MAX_ORDERS_PER_TIDE_WINDOW": "MAX_TRADES_PER_WINDOW",
        "EXTREME_GUARD": "EXTREME_BLOCK_ON",
        "EXTREME_GUARD_KIND": "EXTREME_KIND",  # tạm passthrough string
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
        "M30_STABLE_MIN_SEC","M30_NEED_CONSEC_N",
        "M5_MIN_GAP_MIN","M5_LOOKBACK_RELAX","M5_LOOKBACK_STRICT",
        "ENTRY_SEQ_WINDOW_MIN","M30_TAKEOVER_MIN","CROSS_RECENT_N",
        "RSI_OB","RSI_OS",
        "STCH_RECENT_N",
    }
    float_keys = {
        "ENTRY_LATE_FROM_HRS","ENTRY_LATE_TO_HRS","TP_TIME_HOURS",
        "M5_WICK_PCT","M5_VOL_MULT_RELAX","M5_VOL_MULT_STRICT",
        "M5_SECOND_ENTRY_MIN_RETRACE_PCT",
        "EXTREME_RSI_OB","EXTREME_RSI_OS","EXTREME_STOCH_OB","EXTREME_STOCH_OS",
        "SIZE_MULT_STRONG","SIZE_MULT_MID","SIZE_MULT_CT",
        "SONIC_WEIGHT","HTF_MIN_ALIGN_SCORE","HTF_NEAR_ALIGN_GAP",
        "STCH_GAP_MIN","STCH_SLOPE_MIN", "RSI_GAP_MIN",
    }
    passthrough_str = {"SONIC_MODE","M5_RELAX_KIND","AUTO_DEBUG_CHAT_ID","EXTREME_KIND"}

    kv_to_apply = {}
    try:
        if key_norm in bool_keys:
            kv_to_apply[key_norm] = "true" if _as_bool(val_raw) else "false"
        elif key_norm in int_keys:
            if not _is_intlike(val_raw):
                await msg.reply_text(f"Giá trị cho {key_norm} phải là số nguyên."); return
            kv_to_apply[key_norm] = str(int(float(val_raw)))
        elif key_norm in float_keys:
            if not _is_floatlike(val_raw):
                await msg.reply_text(f"Giá trị cho {key_norm} phải là số (float)."); return
            kv_to_apply[key_norm] = str(float(val_raw))
        elif key_norm in passthrough_str:
            kv_to_apply[key_norm] = val_raw
        else:
            await msg.reply_text(f"KEY không được phép: {key}\nGõ /help để xem KEY hỗ trợ."); return
    except Exception as e:
        await msg.reply_text(f"Lỗi ép kiểu: {e}"); return

    # ghi ENV
    for k, v in kv_to_apply.items():
        os.environ[k] = v

    # đẩy vào core
    try:
        ae._apply_runtime_env(kv_to_apply)
    except Exception as e:
        print(f"[WARN] _apply_runtime_env failed: {e}")

    pretty = "\n".join([f"• {k} = {v}" for k, v in kv_to_apply.items()])
    await msg.reply_html(f"✅ Đã cập nhật ENV (runtime):\n{pretty}")

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

        "M30_FLIP_GUARD", "M30_STABLE_MIN_SEC", # m30 check chuyển xu hướng M30, sau bao lâu mới cho m5 vào

        # M5 (mới)
        "M5_STRICT", "M5_RELAX_KIND",
        "M5_LOOKBACK",                 # legacy
        "M5_LOOKBACK_RELAX",           # NEW
        "M5_RELAX_NEED_CURRENT",       # NEW
        "M5_LOOKBACK_STRICT",          # NEW
        "M5_WICK_PCT",
        "M5_VOL_MULT",                 # legacy
        "M5_VOL_MULT_RELAX",           # NEW
        "M5_VOL_MULT_STRICT",          # NEW
        "M5_REQUIRE_ZONE_STRICT",
        "ENTRY_SEQ_WINDOW_MIN",
        # M5 entry spacing / second entry
        "M5_MIN_GAP_MIN",
        "M5_GAP_SCOPED_TO_WINDOW",
        "ALLOW_SECOND_ENTRY",
        "M5_SECOND_ENTRY_MIN_RETRACE_PCT",        
        

        # Legacy / scoring
        "M5_WICK_MIN", "M5_WICK_MIN_CT", "VOL_MA20_MULT", "RSI_OB", "RSI_OS", "DELTA_RSI30_MIN",
        "SIZE_MULT_STRONG", "SIZE_MULT_MID", "SIZE_MULT_CT",

        # Sonic
        "SONIC_MODE", "SONIC_WEIGHT",

        # ===== NEW knobs cho H4/M30 nới lỏng & đồng bộ =====
        "STCH_GAP_MIN", "STCH_SLOPE_MIN", "STCH_RECENT_N",
        "HTF_NEAR_ALIGN", "HTF_MIN_ALIGN_SCORE", "HTF_NEAR_ALIGN_GAP",
        "SYNERGY_ON", "M30_TAKEOVER_MIN",
		
		# ===== Extreme guard (block LONG/SHORT ở vùng quá mua/bán H4/M30) =====
		"EXTREME_BLOCK_ON", "EXTREME_RSI_OB", "EXTREME_RSI_OS", "EXTREME_STOCH_OB", "EXTREME_STOCH_OS",


        # Limits
        "MAX_TRADES_PER_WINDOW", "MAX_CONCURRENT_POS", "M5_MAX_DELAY_SEC", "SCHEDULER_TICK_SEC",

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
        f"TIDE_WINDOW_HOURS = {st.settings.tide_window_hours}",
        f"MAX_TRADES_PER_DAY = {st.settings.max_orders_per_day}",
        f"MAX_TRADES_PER_TIDE_WINDOW = {st.settings.max_orders_per_tide_window}",
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
            f"Entry: {entry_str}\n"
            f"Contracts: {contracts:.6f}\n"
            f"Unrealized PnL: {0.0 if u_pnl is None else u_pnl:.8f}\n"
            f"PnL% (ROE): {roe_str}\n"
            f"Leverage: {lev_str}"
        )
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

async def order_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    /order <long|short> [qty|auto] [sl] [tp]
    - Đặt lệnh đồng thời: account mặc định (Binance/SINGLE_ACCOUNT) + tất cả account trong ACCOUNTS_JSON.
    - Khớp lệnh theo futures của từng sàn; broadcast hiển thị Entry theo BINANCE SPOT.
    - Mode label: "Thủ công ORDER".
    """
    import os, json
    from config import settings as _S
    from core.trade_executor import ExchangeClient, calc_qty, auto_sl_by_leverage

    msg = update.effective_message
    uid = update.effective_user.id if update.effective_user else 0

    if not context.args:
        await msg.reply_text("Dùng: /order <long|short> [qty|auto] [sl] [tp]\nVD: /order long auto")
        return

    side_raw = (context.args[0] or "").strip().lower()
    if side_raw not in ("long","short"):
        await msg.reply_text("Side phải là long|short. VD: /order short auto"); return
    side_long = (side_raw == "long")

    qty_arg = None; sl_arg = None; tp_arg = None
    if len(context.args) >= 2 and context.args[1].strip().lower() != "auto":
        try: qty_arg = float(context.args[1])
        except: qty_arg = None
    if len(context.args) >= 3:
        try: sl_arg = float(context.args[2])
        except: sl_arg = None
    if len(context.args) >= 4:
        try: tp_arg = float(context.args[3])
        except: tp_arg = None

    # user settings
    DEFAULT_PAIR = getattr(_S, "PAIR", "BTC/USDT")
    try:
        st = storage.get_user(uid)
        risk_percent = float(st.settings.risk_percent)
        leverage     = int(st.settings.leverage)
        logic_pair   = st.settings.pair or DEFAULT_PAIR
    except Exception:
        risk_percent = float(getattr(_S, "RISK_PERCENT_DEFAULT", 20))
        leverage     = int(getattr(_S, "LEVERAGE_DEFAULT", 44))
        logic_pair   = DEFAULT_PAIR

    # QUOTA precheck (tide + daily)
    ok_quota, why, tide_label, tkey, used = _quota_precheck_and_label(st)
    if not ok_quota:
        await msg.reply_text(why); return

    # tập account
    try:
        ACCOUNTS = getattr(_S, "ACCOUNTS", [])
        if not isinstance(ACCOUNTS, list): ACCOUNTS = []
    except Exception:
        try:
            ACCOUNTS = json.loads(os.getenv("ACCOUNTS_JSON","[]"))
            if not isinstance(ACCOUNTS, list): ACCOUNTS = []
        except Exception:
            ACCOUNTS = []
    SINGLE_ACCOUNT = getattr(_S, "SINGLE_ACCOUNT", None)
    base = ([SINGLE_ACCOUNT] if SINGLE_ACCOUNT else []) + ACCOUNTS

    # lọc trùng
    uniq, seen = [], set()
    for acc in base:
        try:
            exid = str(acc.get("exchange","")).lower()
            key  = (exid, acc.get("api_key",""))
            if key in seen: continue
            seen.add(key)
            if not acc.get("pair"):
                acc = {**acc, "pair": logic_pair}
            uniq.append(acc)
        except Exception:
            continue

    if not uniq:
        await msg.reply_text("Không có account nào để đặt lệnh. Kiểm tra API_KEY/API_SECRET hoặc ACCOUNTS_JSON."); return

    # Entry hiển thị từ BINANCE SPOT
    entry_spot = _binance_spot_entry(logic_pair)

    # chạy từng sàn
    results = []
    for acc in uniq:
        try:
            exid   = str(acc.get("exchange") or "").lower()
            name   = acc.get("name","default")
            api    = acc.get("api_key") or ""
            secret = acc.get("api_secret") or ""
            testnet= bool(acc.get("testnet", False))
            pair   = acc.get("pair") or logic_pair

            cli = ExchangeClient(exid, api, secret, testnet)
            px  = await cli.ticker_price(pair)
            if not px or px <= 0:
                results.append(f"• {name} | {exid} | {pair} → ERR: Không lấy được giá futures."); 
                continue

            bal = await cli.balance_usdt()
            if qty_arg and qty_arg > 0:
                qty = float(qty_arg)
            else:
                qty = calc_qty(bal, risk_percent, leverage, px, float(os.getenv("LOT_STEP_FALLBACK","0.001")))

            # SL/TP
            if sl_arg is None or tp_arg is None:
                sl_auto, tp_auto = auto_sl_by_leverage(px, "LONG" if side_long else "SHORT", leverage)
                sl_use = sl_arg if sl_arg is not None else sl_auto
                tp_use = tp_arg if tp_arg is not None else tp_auto
            else:
                sl_use, tp_use = sl_arg, tp_arg

            try: await cli.set_leverage(pair, leverage)
            except Exception: pass

            r = await cli.market_with_sl_tp(pair, side_long, qty, sl_use, tp_use)
            results.append(f"• {name} | {exid} | {pair} → {r.message}")

            # broadcast khi OK
            if getattr(r, "ok", False):
                side_label = "LONG" if side_long else "SHORT"
                btxt = _fmt_exec_broadcast(
                    pair=pair.replace(":USDT",""),
                    side=side_label,
                    acc_name=name, ex_id=exid,
                    lev=leverage, risk=risk_percent, qty=qty,
                    entry_spot=(entry_spot or px),
                    sl=sl_use, tp=tp_use,
                    tide_label=tide_label, mode_label="Thủ công ORDER",
                )
                await _broadcast_html(btxt)

        except Exception as e:
            results.append(f"• {acc.get('name','?')} | ERR: {e}")

    # QUOTA commit (1 lần)
    _quota_commit(st, tkey, used, uid)

    await msg.reply_text(
        f"✅ /order {side_raw.upper()} | risk={risk_percent:.1f}%, lev=x{leverage}\n"
        f"⏱ Tide window: {tide_label}\n" + "\n".join(results)
    )

async def report_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    /report
    - In báo cáo H4→M30 (và daily Moon/Tide).
    - Nếu MODE=manual: tạo pending (duyệt /approve).
    - Nếu MODE=auto và có tín hiệu hợp lệ:
        + KIỂM TRA QUOTA trước khi vào lệnh (2 lệnh/tide, 8 lệnh/ngày): _quota_precheck_and_label(st)
        + Vào lệnh (single-account hiện tại). Sau khi THỬ vào lệnh xong → _quota_commit(st, tkey, used, uid) 1 lần.
        + Broadcast “Mode: AUTO” (entry hiển thị dùng Binance SPOT).
    """
    uid = _uid(update)
    st = storage.get_user(uid)

    # ----------- 1) Báo cáo kỹ thuật + daily ----------
    d = now_vn().date().isoformat()
    daily = format_daily_moon_tide_report(d, float(st.settings.tide_window_hours))

    sym = st.settings.pair.replace("/", "")
    loop = asyncio.get_event_loop()
    try:
        try:
            res = await loop.run_in_executor(
                None, lambda: evaluate_signal(sym, tide_window_hours=float(st.settings.tide_window_hours))
            )
        except TypeError:
            # fallback version hàm cũ
            res = await loop.run_in_executor(None, lambda: evaluate_signal(sym))
    except Exception as e:
        await update.message.reply_text(_esc(daily) + f"\n\n⚠️ Lỗi /report: {_esc(str(e))}")
        return

    if not isinstance(res, dict) or not res.get("ok", False):
        reason = (isinstance(res, dict) and (res.get("text") or res.get("reason"))) or "Không tạo được snapshot kỹ thuật."
        await update.message.reply_text(_esc(daily) + f"\n\n⚠️ {_esc(reason)}")
        return

    ta_text = res.get("text") or format_signal_report(res)
    ta_text = _beautify_report(ta_text)
    safe_daily = _esc(daily)
    safe_ta    = _esc(ta_text)

    # In báo cáo trước
    try:
        await update.message.reply_text(safe_daily + "\n\n" + safe_ta, parse_mode="HTML")
    except Exception:
        await update.message.reply_text(safe_daily + "\n\n" + (res.get("text") or "—"))

    # Không trade nếu bị skip
    if res.get("skip", True):
        return

    side = (res.get("signal") or "NONE").upper()
    if side not in ("LONG", "SHORT"):
        return

    # ----------- 2) Nhánh MANUAL: tạo pending ----------
    if (st.settings.mode or "manual").lower() == "manual":
        score = int(res.get("confidence", 0))
        ps = create_pending(storage, uid, st.settings.pair, side, score, entry_hint=None, sl=None, tp=None)
        block = (
            safe_ta
            + f"\nID: <code>{ps.id}</code>\nDùng /approve {ps.id} hoặc /reject {ps.id}"
        )
        await update.message.reply_text(safe_daily + "\n\n" + block, parse_mode="HTML")
        return

    # ----------- 3) Nhánh AUTO: quota + timing + vào lệnh ----------
    # 3.1 QUOTA PRECHECK (mục 2.7)
    ok_quota, why, tide_label, tkey, used = _quota_precheck_and_label(st)
    if not ok_quota:
        try:
            await update.message.reply_text("(AUTO) " + why)
        except Exception:
            pass
        return

    # 3.2 Ràng buộc "late window" (nếu bật)
    try:
        late_only = (os.getenv("ENTRY_LATE_ONLY", "false").lower() in ("1","true","yes","on","y"))
        late_pref = (os.getenv("ENTRY_LATE_PREF", "false").lower() in ("1","true","yes","on","y"))
        late_from = float(os.getenv("ENTRY_LATE_FROM_HRS", "1.5"))
        late_to   = float(os.getenv("ENTRY_LATE_TO_HRS", "2.0"))

        now = now_vn()
        twin = tide_window_now(now, hours=float(st.settings.tide_window_hours))
        if twin:
            start, end = twin
            center = start + (end - start) / 2
            delta_hr = (now - center).total_seconds() / 3600.0
            in_late = (delta_hr >= late_from and delta_hr <= late_to)

            if late_only and not in_late:
                await update.message.reply_text(
                    "(AUTO) "
                    + _esc(
                        f"ENTRY_LATE_ONLY=true → chỉ cho phép vào trong late window "
                        f"[{(center + timedelta(hours=late_from)).strftime('%H:%M')}–{(center + timedelta(hours=late_to)).strftime('%H:%M')}] "
                        f"(center={center.strftime('%H:%M')}, now={now.strftime('%H:%M')}).\n"
                        "⏸ Bỏ qua vào lệnh lần này."
                    )
                )
                return

            if (not late_only) and late_pref and (not in_late):
                conf = int(res.get("confidence", 0))
                if conf < 6:
                    await update.message.reply_text(
                        "(AUTO) " + _esc("ENTRY_LATE_PREF=true và ngoài late window → bỏ qua vì điểm chưa đủ mạnh.")
                    )
                    return
    except Exception as _e_enforce:
        print(f"[AUTO][WARN] Late-window check error: {_e_enforce}")

    # 3.3 Lấy giá SPOT (để hiển thị broadcast) + giá futures (để khớp lệnh)
    try:
        from data.market_data import get_klines
        dfp = get_klines(symbol=st.settings.pair.replace("/", ""), interval="5m", limit=2)
        if dfp is None or len(dfp) == 0:
            await update.message.reply_text("(AUTO) Không lấy được giá hiện tại.")
            return
        close = float(dfp.iloc[-1]["close"])
    except Exception as e:
        await update.message.reply_text(f"(AUTO) Lỗi lấy giá: {_esc(str(e))}")
        return

    # 3.4 Tính size + leverage
    try:
        bal = await ex.balance_usdt()
        qty = calc_qty(bal, st.settings.risk_percent, st.settings.leverage, close)
        await ex.set_leverage(st.settings.pair, st.settings.leverage)
    except Exception as e:
        await update.message.reply_text(f"(AUTO) Lỗi khối lượng/leverage: {_esc(str(e))}")
        return

    side_long = (side == "LONG")
    try:
        sl_price, tp_price = auto_sl_by_leverage(close, "LONG" if side_long else "SHORT", st.settings.leverage)
    except Exception:
        if side_long:
            sl_price, tp_price = close * 0.99, close * 1.02
        else:
            sl_price, tp_price = close * 1.01, close * 0.98

    # 3.5 Thử khớp lệnh (single-account hiện tại)
    try:
        res_exe = await ex.market_with_sl_tp(st.settings.pair, side_long, qty, sl_price, tp_price)
    except Exception as e:
        res_exe = OrderResult(False, f"Order failed: {e}")  # fallback để vẫn commit quota

    # 3.6 Lưu lịch sử (không cộng quota ở đây)
    st.history.append({
        "id": f"AUTO-{datetime.now().strftime('%H%M%S')}",
        "side": side, "qty": qty, "entry": close,
        "sl": sl_price, "tp": tp_price,
        "ok": getattr(res_exe, "ok", True), "msg": getattr(res_exe, "message", "")
    })
    storage.put_user(uid, st)

    # 3.7 Broadcast “Mode: AUTO” (hiển thị entry từ Binance SPOT), nếu lệnh OK
    try:
        if getattr(res_exe, "ok", False):
            entry_spot = _binance_spot_entry(st.settings.pair)
            btxt = _fmt_exec_broadcast(
                pair=st.settings.pair.replace(":USDT",""),
                side=("LONG" if side_long else "SHORT"),
                acc_name="default",
                ex_id=getattr(ex, "exchange_id", "binanceusdm"),
                lev=st.settings.leverage,
                risk=st.settings.risk_percent,
                qty=qty,
                entry_spot=(entry_spot or close),
                sl=sl_price, tp=tp_price,
                tide_label=tide_label,  # lấy từ quota-precheck
                mode_label="AUTO",
            )
            await _broadcast_html(btxt)
    except Exception:
        pass

    # 3.8 QUOTA COMMIT (mục 2.7) — chỉ +1 lần cho cả phiên AUTO này
    _quota_commit(st, tkey, used, uid)

    # 3.9 Phản hồi kết quả
    enter_line = (
        f"🔧 Executed: {st.settings.pair} {'LONG' if side_long else 'SHORT'} "
        f"qty={qty:.6f} @~{close:.2f} | SL={sl_price:.2f} | TP={tp_price:.2f}\n"
        f"↳ {getattr(res_exe, 'message', '')}"
    )
    await update.message.reply_text("(AUTO)\n" + enter_line)

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
            # 1) Snapshot M5 như cũ
            sym = st.settings.pair.replace("/", "")
            snap = m5_snapshot(sym)

            # 2) Nếu đang ở MANUAL và có pending -> ghép thêm block gợi ý duyệt tay
            try:
                mode_now = (st.settings.mode or "").lower()
            except Exception:
                mode_now = "manual"

            # st.pending do engine set khi phát hiện đủ điều kiện nhưng ở manual
            pend = getattr(st, "pending", None)

            if mode_now == "manual" and pend:
                # an toàn hoá dữ liệu hiển thị
                pid  = getattr(pend, "id", None)
                side = str(getattr(pend, "side", "") or "").upper()
                sl   = getattr(pend, "sl", None)
                tp   = getattr(pend, "tp", None)

                # Tên cặp định dạng cho futures multi-exchange (ví dụ "BTC/USDT:USDT")
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

            # Gửi ra 1 lần ngay khi start
            await update.message.reply_text(snap, parse_mode=constants.ParseMode.HTML)

        except Exception as e:
            # Giữ nguyên thông báo lỗi cũ
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

    # 1) Ưu tiên: getter chính thức từ engine (nếu có)
    try:
        getter = getattr(ae, "get_last_decision_text", None)
        if callable(getter):
            txt = getter(uid)
    except Exception:
        txt = None

    # 2) Fallback: map nội bộ _last_decision_text
    if not txt:
        try:
            last_map = getattr(ae, "_last_decision_text", None)
            if isinstance(last_map, dict):
                txt = last_map.get(uid)
        except Exception:
            txt = None

    # 3) Fallback nhẹ: hiển thị slot M5 gần nhất (nếu engine chưa lưu log)
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

    # Bảo vệ giới hạn Telegram (khoảng < 4000 ký tự)
    if len(txt) > 3500:
        txt = txt[:3500] + "\n…(rút gọn)…"

    await update.message.reply_text(f"📜 Auto log gần nhất:\n{txt}")


async def approve_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    /approve <pending_id>
    - MANUAL duyệt: thực thi đa sàn (SINGLE_ACCOUNT + ACCOUNTS_JSON).
    - Broadcast theo format thống nhất, Mode: "Manual".
    - Entry hiển thị lấy từ Binance Spot; khớp lệnh theo futures từng sàn.
    """
    import os, json
    from config import settings as _S
    from core.trade_executor import ExchangeClient, calc_qty, auto_sl_by_leverage

    msg = update.effective_message
    uid = update.effective_user.id if update.effective_user else 0

    if not context.args:
        await msg.reply_text("Dùng: /approve <pending_id>"); return
    pend_id = context.args[0].strip()

    st = storage.get_user(uid)
    if not getattr(st, "pending", None) or str(st.pending.id) != pend_id:
        await msg.reply_text("Không có pending hoặc sai ID."); return

    side = str(st.pending.side or "").upper()
    if side not in ("LONG","SHORT"):
        await msg.reply_text("Pending không có side hợp lệ."); return
    side_long = (side == "LONG")
    pair = st.settings.pair or getattr(_S, "PAIR", "BTC/USDT")

    # QUOTA precheck
    ok_quota, why, tide_label, tkey, used = _quota_precheck_and_label(st)
    if not ok_quota:
        await msg.reply_text(why); return

    # user settings
    risk_percent = float(st.settings.risk_percent)
    leverage     = int(st.settings.leverage)

    # SPOT entry for display
    entry_spot = _binance_spot_entry(pair)

    # accounts
    try:
        ACCOUNTS = getattr(_S, "ACCOUNTS", [])
        if not isinstance(ACCOUNTS, list): ACCOUNTS = []
    except Exception:
        try:
            ACCOUNTS = json.loads(os.getenv("ACCOUNTS_JSON","[]"))
            if not isinstance(ACCOUNTS, list): ACCOUNTS = []
        except Exception:
            ACCOUNTS = []
    SINGLE_ACCOUNT = getattr(_S, "SINGLE_ACCOUNT", None)
    base = ([SINGLE_ACCOUNT] if SINGLE_ACCOUNT else []) + ACCOUNTS

    # exec
    results = []
    for acc in base:
        try:
            exid   = str(acc.get("exchange") or "").lower()
            name   = acc.get("name","default")
            api    = acc.get("api_key") or ""
            secret = acc.get("api_secret") or ""
            testnet= bool(acc.get("testnet", False))
            pair_u = acc.get("pair") or pair

            cli = ExchangeClient(exid, api, secret, testnet)
            px  = await cli.ticker_price(pair_u)
            if not px or px <= 0:
                results.append(f"• {name} | {exid} | {pair_u} → ERR: Không lấy được giá futures.")
                continue

            bal = await cli.balance_usdt()
            qty = calc_qty(bal, risk_percent, leverage, px, float(os.getenv("LOT_STEP_FALLBACK","0.001")))

            # nếu pending có SL/TP thì dùng; không thì auto theo leverage
            if (st.pending.sl is None) or (st.pending.tp is None):
                sl_use, tp_use = auto_sl_by_leverage(px, side, leverage)
            else:
                sl_use, tp_use = float(st.pending.sl), float(st.pending.tp)

            try: await cli.set_leverage(pair_u, leverage)
            except Exception: pass

            r = await cli.market_with_sl_tp(pair_u, side_long, qty, sl_use, tp_use)
            results.append(f"• {name} | {exid} | {pair_u} → {r.message}")

            if getattr(r, "ok", False):
                btxt = _fmt_exec_broadcast(
                    pair=pair_u.replace(":USDT",""),
                    side=side,
                    acc_name=name, ex_id=exid,
                    lev=leverage, risk=risk_percent, qty=qty,
                    entry_spot=(entry_spot or px),
                    sl=sl_use, tp=tp_use,
                    tide_label=tide_label, mode_label="Manual",
                )
                await _broadcast_html(btxt)

        except Exception as e:
            results.append(f"• {acc.get('name','?')} | ERR: {e}")

    # clear pending & QUOTA commit (1 lần)
    st.pending = None
    _quota_commit(st, tkey, used, uid)

    await msg.reply_text(f"✅ Đã APPROVE #{pend_id} — {pair} {side}\n" + "\n".join(results))


async def reject_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid = _uid(update)
    st = storage.get_user(uid)
    if st.pending and context.args and context.args[0] == st.pending.id:
        st.pending = None
        storage.put_user(uid, st)
        await update.message.reply_text("Đã từ chối tín hiệu.")
    else:
        await update.message.reply_text("Không có pending hoặc sai ID.")

# ==== /close (đa tài khoản: Binance/BingX/...) ====
async def close_cmd(update, context):
    """
    /close                -> đóng 100% trên tất cả account trong ACCOUNTS_JSON
    /close 50             -> đóng 50% tất cả account
    /close bingx_test     -> đóng 100% riêng account 'bingx_test'
    /close 25 bingx_test  -> đóng 25% riêng 'bingx_test' (hoặc: /close bingx_test 25)
    """
    msg = update.effective_message
    pair = os.getenv("PAIR", "BTC/USDT")

    # parse args
    args = context.args if hasattr(context, "args") else []
    percent = 100.0
    account = None

    def _is_percent(s: str) -> bool:
        try:
            x = float(s)
            return 0.0 <= x <= 100.0
        except:
            return False

    if args:
        a0 = args[0]
        if _is_percent(a0):
            percent = float(a0)
            if len(args) >= 2:
                account = args[1]
        else:
            account = a0
            if len(args) >= 2 and _is_percent(args[1]):
                percent = float(args[1])

    try:
        if account:
            res = await close_position_on_account(account, pair, percent)
            ok = res.get("ok", False)
            text = f"Đã cố gắng đóng {percent:.0f}% vị thế trên <b>{account}</b> ({pair}): {res.get('message','')}"
            await msg.reply_text(text, parse_mode="HTML")
        else:
            results = await close_position_on_all(pair, percent)
            lines = [f"Đóng {percent:.0f}% vị thế ({pair}) trên TẤT CẢ tài khoản:"]
            for i, r in enumerate(results, 1):
                ok = r.get("ok", False)
                lines.append(f"• #{i} → {r.get('message','ok' if ok else 'fail')}")
            await msg.reply_text("\n".join(lines))
    except Exception as e:
        await msg.reply_text(f"❌ Lỗi /close: {e}")


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
- **Thân - Tâm - Trí**
- Phải luôn vận hành 3 hệ thống: **QLV - TÂM LÝ - HTGD**
 🧍‍♂️ *THÂN – Quản lý vốn (Số 8 – Trưởng thành)*
 🧘‍♂️ *TÂM – Tâm lý (Số 2 – Cân bằng cảm xúc)*
 🧠 *TRÍ – Hệ thống giao dịch (Số 3 – Chủ đạo)*

👤 **HỆ THỐNG: User #Me : 4 Mùa & 4 Phase Moon & 4 Tides**
 
🧍‍♂️ *THÂN – Quản lý vốn (Số 8 – Trưởng thành)*
1. Sức khỏe bạn có đang ổn không ? Hôm nay đã vận động min 15 phút chưa ?
2. Quản lý Vốn là: Giữ đều vol ( Vốn x Đòn Bẩy , SL chịu được là giống nhau)
3. Nguyên tắc QLV: 8:8:8/ (2:3:5) theo pytago trong một mạng/ tổng 8) 
4. Số lệnh: Tối đa 8 lệnh/ngày - Mỗi giờ thủy triều max 2 lệnh
5. Tỷ lệ R:R > 1.3 (Tổng 4) : Ghi sẵn target/stop max 50%, không thay đổi sau khi vào lệnh, nếu nó đi ngược Sl 50% vẫn OK - phải đo % để tính đòn bẩy
6. Đòn bẩy: X17,X26,X35,X44 (Nhắc về con số trưởng thành 8) : Thân dò X17 - 20% vốn, Trí  50% Tín hiệu tốt thì X26 / Xác suất cao, SL ngắn X35~X44

🧘‍♂️ *TÂM – Tâm lý (Số 2 – Cân bằng cảm xúc)*
1. Bạn KO bị stress hay căng thẳng chứ ? Hôm nay đã thiền min 15 phút chưa ?
2. Tâm lý trước lệnh : Checklist trước lệnh + Thở 8 lần để tỉnh thức trước click
3. Tâm lý trong lệnh : Nếu đang hoảng loạn "Vô tác", Ko được đi thêm lệnh hoặc DCA
4. Tâm lý sau lệnh: Tổng kết 1 điều tốt + 1 bài học mỗi cuối ngày
5. Tâm lý hồi phục sau thắng/Thua: Dừng giao dịch 48h = 2 ngày để bình tĩnh tâm khi thua

🧠 *TRÍ – Hệ thống giao dịch (Số 3 – Chủ đạo)*
1. Bạn có đang tỉnh táo và sáng suốt không ? Hôm nay đã thiền min 15 phút chưa
2. Chỉ BTC/USDT
3. Theo trend chính (D, H4) - Sonic R " TREND IS YOUR FRIEND "
4. CTTT Đa Khung M30,M5/Fibo 50%/ Wyckoff -Phase-Spring/ Mô Hình /KC-HT lý tưởng/Volumn DGT/ SQ9 -9 cây nến/ Nâng cao Sonic R+Elliot (ăn sóng 3)+Wyckoff
5. Phase trăng - Biến động theo tứ xung theo độ rọi (0 - 25 - 50 - 75 - 100)
6. Vùng giờ thủy triều ±1h "Tín hiệu thường xác định sau khi hết vùng thủy triều"
7. Đồng pha RSI & EMA RSI (đa khung)
8. Chiến lược 1 or phá nền giá Down M5 or Luôn hỏi đã "CẠN CUNG" chưa ?
9. Stoch RSI công tắc xác nhận bật

📝 *Lời nhắc thêm từ Nhân số học:*
10. Số chủ đạo 3: Hạn chế mạng xã hội, tập trung yêu bản thân, hạn chế phân tán năng lượng
11. 4 số 11: Viết nhật ký, Kiểm tra checklist trước vào lệnh, Kiên trì 1 hệ thống giao dịch, Hạn chế cộng đồng
12. Kiếm củi 3 năm đốt trong 1h rất nhiều 
13. Cảnh giác: "Đã bị cháy nhiều lần vì vi phạm HTGD, trả thù, DCA khi hoảng loạn"
14. Không được vào lệnh bằng điện thoại — Phải chậm lại, không hấp tấp
15. "YOU ARE WHAT YOU REPEAT"
16. Biên độ dao động Khung Bé / H4 / D1 / W — đang ntn — Điều chỉnh cái thì sao?

📈 **MỤC TIÊU:**
- Phiên bản ổn định, vững tâm – tự do thật sự từ kỷ luật

🚀 **TRẠNG THÁI HIỆN TẠI:**
- Phiên bản 3.1.4 – *Đang cập nhật mỗi ngày*
""".strip()
    # Dùng Markdown để giữ format in đậm/nghiêng như bạn soạn
    await update.message.reply_text(about_text, parse_mode="Markdown")

async def journal_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    form_link = "https://docs.google.com/forms/d/e/1FAIpQLSeXQmxn8X9BCUC_StiOid1wFCue_19y3hEQBTHULnNHl7ShSg/viewform"
    await update.message.reply_text(f"📋 Mời bạn điền nhật ký giao dịch tại đây:\n{form_link}")

async def recovery_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    checklist_text = (
        "🧠 *Phục hồi tâm lý sau thua lỗ – Vấn đề & Giải pháp*\n\n"
        "❗ *Vấn đề 1:* Hay trả thù, ăn thua với thị trường\n"
        "🔧 *Giải pháp:* Tập không vào thêm lệnh để ăn thua or DCA và tuân thủ SL đã đặt\n\n"
        "❗ *Vấn đề 2:* Cố chấp vẫn bật máy tính để tìm thêm kèo vào lại ngay tức thì or phá vỡ HTGD\n"
        "🔧 *Giải pháp:* Rèn tính rời bỏ máy tính, nhìn chart - quay về quan sát cảm xúc or kiểm điểm lại HTGD\n\n"
        "❗ *Vấn đề 3:* Không có cơ chế phục hồi cảm xúc\n"
        "🔧 *Giải pháp:* Dừng giao dịch 48h = 2 ngày, viết ra cảm xúc, hít thở sâu mỗi ngày\n\n"
        "❗ *Vấn đề 4:* Tập trung quá nhiều vào kết quả\n"
        "🔧 *Giải pháp:* Đặt mục tiêu là tính nhất quán, không phải lợi nhuận\n\n"
        "❗ *Vấn đề 5:* Tự trừng phạt khi sai\n"
        "🔧 *Giải pháp:* Xem sai lầm như dữ liệu cải thiện hệ thống, không phán xét bản thân , hành động khác ngu ngốc ảnh hưởng đến cảm xúc\n\n"
        "❗ *Vấn đề 6:* Thiếu hệ thống rèn tâm\n"
        "🔧 *Giải pháp:* Mỗi sáng viết 3 điều biết ơn, mỗi tối ghi lại cảm xúc – luyện tâm như luyện kỹ thuật\n\n"
        "❗ *Vấn đề 7:* Rà Soát và Tuân Thủ 3 Hệ Thống : THÂN (QLV-8) - TÂM (CẢM XÚC-2) - TRÍ(HTGD-3)\n"
        "🔧 *Giải pháp:* Rà Soát và Tuân Thủ 3 Hệ Thống : THÂN (QLV-8) - TÂM (CẢM XÚC-2) - TRÍ(HTGD-3)\n\n"
        "✅ *Hãy chỉ quay lại thị trường khi 3 hệ thống THÂN - TÂM - TRÍ đã bình ổn.*"
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
    app.add_handler(CommandHandler("order", order_cmd))
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
