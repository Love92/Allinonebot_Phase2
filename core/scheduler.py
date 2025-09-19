# ----------------------- core/scheduler.py -----------------------
from __future__ import annotations
import asyncio
import os
from datetime import datetime
import html

# Robust imports
try:
    from telegram import Application
except Exception:
    Application = None  # type: ignore

try:
    from utils.time_utils import now_vn
except Exception:
    from time_utils import now_vn  # type: ignore

try:
    from tg.formatter import format_daily_moon_tide_report
except Exception:
    from formatter import format_daily_moon_tide_report  # type: ignore

try:
    from strategy.signal_generator import evaluate_signal, tide_window_now
except Exception:
    from signal_generator import evaluate_signal, tide_window_now  # type: ignore


# ========= Helpers to avoid Telegram "Can't parse entities" =========
def _safe_html(text: str) -> str:
    """
    1) html.escape để an toàn
    2) format_daily_moon_tide_report đã tự dùng _beautify_report bên trong.
    3) Kết hợp thêm vài thay thế tối thiểu nếu text được build ở đây.
    """
    if text is None:
        return ""
    # Ở đây ta chỉ escape thô (Telegram parse_mode="HTML" luôn an toàn)
    # Nếu cần ký hiệu ≤ ≥ … hãy build text gốc dạng "<=", ">=" rồi để formatter xử lý.
    return text if text.startswith("📅 ") else html.escape(text, quote=False)


async def _send_report_once(app, chat_id: int, tide_window_hours: float):
    """
    Gửi 1 report tổng hợp (Moon & Tide + H4→M30 evaluate) — an toàn HTML.
    """
    d = now_vn().date().isoformat()
    daily = format_daily_moon_tide_report(d, tide_window_hours)

    # Kỹ thuật H4→M30 (đồng bộ với /report)
    try:
        # evaluate_signal đã format sẵn 'text' chuẩn Markdown/plain; ta escape trước khi gửi HTML
        res = await asyncio.get_event_loop().run_in_executor(
            None, lambda: evaluate_signal("BTCUSDT", tide_window_hours)
        )
    except TypeError:
        res = await asyncio.get_event_loop().run_in_executor(
            None, lambda: evaluate_signal("BTCUSDT")
        )

    if not isinstance(res, dict) or not res.get("ok", False):
        reason = (isinstance(res, dict) and (res.get("text") or res.get("reason"))) or "Không tạo được snapshot kỹ thuật."
        msg = _safe_html(daily) + "\n\n" + _safe_html("⚠️ " + str(reason))
        await app.bot.send_message(chat_id=chat_id, text=msg, parse_mode="HTML")
        return

    ta_text = res.get("text") or ""
    # Chuỗi kỹ thuật có thể chứa 'EMA34<EMA89', '<=', ... — escape trước rồi beautify đã được làm trong formatter.
    msg = _safe_html(daily) + "\n\n" + _safe_html(ta_text)

    await app.bot.send_message(chat_id=chat_id, text=msg, parse_mode="HTML")


# ========= Public API =========
async def scheduler_loop(app, storage):
    """
    Vòng lặp nhẹ gửi báo cáo tự động trong cửa sổ thủy triều, mỗi ~30 phút.
    - Không đổi cấu trúc dữ liệu user; chỉ đảm bảo text an toàn HTML.
    """
    if Application is None:
        print("[SCHED] Telegram Application not available.")
        return

    try:
        tick_sec = int(float(os.getenv("SCHEDULER_TICK_SEC", "60")))
    except Exception:
        tick_sec = 60

    print(f"[SCHED] scheduler_loop started (tick={tick_sec}s).")
    while True:
        try:
            # Duyệt qua toàn bộ user có trong storage
            for uid, st in (storage.data.get("users") or {}).items():
                try:
                    uid_int = int(uid)
                except Exception:
                    continue

                # Lấy tham số
                tide_hours = float(getattr(st.settings, "tide_window_hours", os.getenv("TIDE_WINDOW_HOURS", "2.5")))
                pair = getattr(st.settings, "pair", "BTC/USDT")
                sym = pair.replace("/", "")

                # Kiểm tra đang trong tide-window?
                now = now_vn()
                twin = tide_window_now(now, hours=tide_hours)
                if not twin:
                    continue  # ngoài vùng → không gửi

                # Chỉ gửi mỗi 30 phút một lần (đánh dấu theo “slot” HH:MM // 30)
                slot = f"{now.year}-{now.month:02d}-{now.day:02d} {now.hour:02d}:{(now.minute//30)*30:02d}"
                sent_map = storage.data.setdefault("_sched_sent_slots", {})
                last_slot = sent_map.get(str(uid_int))
                if last_slot == slot:
                    continue  # đã gửi slot này

                # Gửi
                try:
                    await _send_report_once(app, uid_int, tide_hours)
                    sent_map[str(uid_int)] = slot
                    storage.persist()
                except Exception as e_send:
                    # Báo lỗi ngắn gọn, không làm vỡ vòng lặp
                    try:
                        txt = f"🚨 Lỗi scheduler(M30/H4): {e_send}"
                        await app.bot.send_message(chat_id=uid_int, text=html.escape(txt, quote=False))
                    except Exception:
                        pass

        except Exception as e:
            print(f"[SCHED][LOOP_ERR] {e}")

        await asyncio.sleep(tick_sec)

# ----------------------- /core/scheduler.py -----------------------
