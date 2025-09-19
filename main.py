# ----------------------- main.py -----------------------
import os, asyncio, html
from datetime import timedelta
from dotenv import load_dotenv

from tg.bot import build_app
from utils.storage import Storage
from utils.time_utils import now_vn
from data.moon_tide import get_tide_events
from strategy.signal_generator import evaluate_signal
from tg.formatter import format_signal_report, format_daily_moon_tide_report
from tg.formatter import _beautify_report  # dùng lại beautify để tránh Markdown/HTML lỗi
from core.approval_flow import create_pending

# NEW: vòng lặp M5 riêng
from core.m5_reporter import m5_report_loop
# NEW: thuật toán auto vào và tp
from core.auto_trade_engine import start_auto_loop

from telegram.error import BadRequest  # để bắt lỗi parse_mode

load_dotenv()

SCHEDULER_TICK_SEC    = int(os.getenv("SCHEDULER_TICK_SEC", "1"))
M30_SLOT_GRACE_SEC    = int(os.getenv("M30_SLOT_GRACE_SEC", "6"))
CHECKLIST_ENABLED     = os.getenv("CHECKLIST_ENABLED", "true").lower() in ("1","true","yes","on")
CHECKLIST_TIDE_ONLY   = os.getenv("CHECKLIST_TIDE_ONLY", "true").lower() in ("1","true","yes","on")
CHECKLIST_INTERVAL_MIN= int(os.getenv("CHECKLIST_INTERVAL_MIN", "30"))

# 🔔 Checklist rút gọn (Markdown)
SHORT_CHECKLIST = """
📋 *Checkist : 4 Mùa & 4 Phase Moon & 4 Tides của HTGD : THÂN – TÂM – TRÍ :*

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
5. Tâm lý hồi phục sau thắng/Thua: Dừng giao dịch 48h ( 2 ngày = con số thử thách) để bình tĩnh tâm khi thua

🧠 *TRÍ – Hệ thống giao dịch (Số 3 – Chủ đạo)*
1. Bạn có đang tỉnh táo và sáng suốt không ? Hôm nay đã thiền min 15 phút chưa
2. Chỉ BTC/USDT
3. Theo trend chính (D, H4) - Sonic R " TREND IS YOUR FRIEND "
4. CTTT Đa Khung M30,M5/Fibo 50%/ Wyckoff -Phase-Spring/ Mô Hình /KC-HT lý tưởng/Volumn DGT/ SQ9 -9 cây nến/ Nâng cao Sonic R+Elliot (ăn sóng 3)+Wyckoff
5. Phase trăng - Biến động theo tứ xung theo độ rọi (0 - 25 - 50 - 75 - 100)
6. Vùng giờ thủy triều ±1h " Tín hiệu thường xác định sau khi hết giờ vùng giờ thủy triều "
7. Đồng pha RSI & EMA RSI (đa khung)
8. Chiến lược 1 or phá nền giá Down M5 or Luôn hỏi đã "CẠN CUNG" chưa ?
9. Stoch RSI công tắc xác nhận bật đa khung 

📝 *Lời nhắc nhở thêm từ Nhân số học:*
10. Số chủ đạo 3: Hạn chế mạng xã hội, tập trung yêu bản thân, hạn chế phân tán năng lượng
11. 4 số 11: Viết nhật ký, Kiểm tra checklist trước vào lệnh, Kiên trì 1 hệ thống giao dịch, Hạn chế cộng đồng
12. Kiếm củi 3 năm đốt trong 1h rất nhiều 
13. Cảnh giác :" Mày đã bị cháy nhiều lần vì vi phạm không tuân thủ giao dịch và trả thù , DCA khi hoảng loạn " 
14. Không được vào lệnh bằng điện thoại - Phải chậm lại - không hấp tấp "
15. " YOU ARE WHAT YOU REPEAT"
16. Biên độ dao động Khung Bé / H4/ D1 / W - đang ntn - Điều chỉnh cái thì sao ?
""".strip()

storage = Storage()

# ===== Helpers: gửi text “an toàn” cho Telegram =====
def _clean_htmlish(s: str) -> str:
    """Loại bỏ/đổi các tag HTML không được Telegram hỗ trợ, chuẩn hoá xuống dòng."""
    if not isinstance(s, str):
        return s
    s = s.replace("<br/>", "\n").replace("<br>", "\n").replace("<hr>", "\n")
    # đôi khi data nguồn có &nbsp;
    s = s.replace("&nbsp;", " ")
    return s

async def _send_safe(bot, chat_id: int, text: str):
    """
    Gửi dạng HTML an toàn; nếu lỗi Can't parse entities → fallback gửi thường.
    """
    # 1) Beautify để loại dấu so sánh, etc.
    pretty = _beautify_report(text or "")
    # 2) Loại tag HTML không hỗ trợ
    pretty = _clean_htmlish(pretty)
    # 3) Escape toàn bộ để Telegram không hiểu nhầm entity
    safe_html = html.escape(pretty, quote=False)
    try:
        await bot.send_message(
            chat_id=chat_id,
            text=safe_html,
            parse_mode="HTML",
            disable_web_page_preview=True
        )
    except BadRequest as e:
        # Fallback nếu vẫn còn lỗi entity
        if "Can't parse entities" in str(e):
            await bot.send_message(
                chat_id=chat_id,
                text=pretty,            # đã clean, không parse_mode
                disable_web_page_preview=True
            )
        else:
            raise

async def scheduler_loop_m30_h4(app):
    """
    CHỈ xử lý H4/M30:
    - Auto report tại 9 mốc: center ±0/±30/±60/±90/±120 phút
    - Evaluate H4→M30 sau khi gửi report
    - ✅ Gửi checklist 30' một lần TRONG Tide Window, TRƯỚC auto-report
    """
    bot = app.bot
    uid_env = os.getenv("TELEGRAM_CHAT_ID")
    uid = int(uid_env) if (uid_env and uid_env.isdigit()) else 0

    while True:
        try:
            st = storage.get_user(uid) if uid else None
            if not st:
                await asyncio.sleep(30); continue

            # Giới hạn theo ngày
            if st.today.count >= st.settings.max_orders_per_day:
                await asyncio.sleep(120); continue

            # Tìm xem đang ở cửa sổ thủy triều nào
            today_str = now_vn().strftime("%Y-%m-%d")
            tide_lines = get_tide_events(today_str) or []
            now_dt = now_vn()

            in_window, current_key, center_tide = False, None, None
            for line in tide_lines:
                parts = line.split()
                if len(parts) < 2 or ":" not in parts[1]:
                    continue
                label = parts[0].capitalize()
                hh, mm = parts[1].split(":")
                tide_dt = now_dt.replace(hour=int(hh), minute=int(mm), second=0, microsecond=0)
                start = tide_dt - timedelta(hours=float(st.settings.tide_window_hours))
                end   = tide_dt + timedelta(hours=float(st.settings.tide_window_hours))
                if start <= now_dt <= end:
                    in_window = True
                    current_key = f"{today_str} {parts[1]} {label}"
                    center_tide = tide_dt
                    break

            if not in_window:
                await asyncio.sleep(SCHEDULER_TICK_SEC); continue

            # 9 mốc M30 quanh center
            slots = [center_tide + timedelta(minutes=30*k) for k in range(-4, 5)]
            for slot_dt in slots:
                if abs((now_dt - slot_dt).total_seconds()) <= M30_SLOT_GRACE_SEC:
                    slot_key = f"m30:{current_key}:{slot_dt.strftime('%H:%M')}"
                    st2 = storage.get_user(uid)

                    # ================= CHECKLIST (trước /report) =================
                    if CHECKLIST_ENABLED:
                        # tag theo cửa sổ tide + mốc ngày
                        cl_tag = f"checklist:{current_key}"
                        last_ts = st2.tide_window_trades.get(cl_tag, 0)
                        elapsed_min = 9e9
                        if last_ts:
                            elapsed_min = (now_dt.timestamp() - float(last_ts)) / 60.0

                        allow_tide = True if not CHECKLIST_TIDE_ONLY else in_window
                        if allow_tide and (not last_ts or elapsed_min >= CHECKLIST_INTERVAL_MIN):
                            try:
                                await bot.send_message(
                                    chat_id=uid,
                                    text=SHORT_CHECKLIST,
                                    parse_mode="Markdown",  # checklist là Markdown
                                    disable_web_page_preview=True,
                                )
                            finally:
                                st2 = storage.get_user(uid)
                                st2.tide_window_trades[cl_tag] = now_dt.timestamp()
                                storage.put_user(uid, st2)
                    # ============================================================

                    if st2.tide_window_trades.get(slot_key, 0) == 0:
                        # Daily block
                        daily_raw = format_daily_moon_tide_report(today_str, float(st.settings.tide_window_hours))
                        # TA block
                        sym = st.settings.pair.replace("/", "")
                        loop = asyncio.get_event_loop()
                        try:
                            res = await loop.run_in_executor(
                                None, lambda: evaluate_signal(sym, tide_window_hours=float(st.settings.tide_window_hours))
                            )
                        except TypeError:
                            res = await loop.run_in_executor(None, lambda: evaluate_signal(sym))
                        ta_text = (res.get("text") if isinstance(res, dict) else None) or "⚠️ Không tạo được snapshot kỹ thuật."

                        # Ghép & gửi an toàn (HTML-safe → fallback)
                        merged = (daily_raw or "").strip() + "\n\n" + (ta_text or "").strip()
                        await _send_safe(bot, uid, merged)

                        st2 = storage.get_user(uid)
                        st2.tide_window_trades[slot_key] = 1
                        storage.put_user(uid, st2)
                    break

        except Exception as e:
            try:
                # lỗi này gửi thẳng text thường để chắc chắn không dính parse
                await bot.send_message(chat_id=uid, text=f"🚨 Lỗi scheduler(M30/H4): {e}")
            except:
                pass

        await asyncio.sleep(SCHEDULER_TICK_SEC)

if __name__ == "__main__":
    app = build_app()

    # (Tuỳ chọn) tạo event loop mới để hết DeprecationWarning
    import asyncio as _asyncio
    loop = _asyncio.new_event_loop()
    _asyncio.set_event_loop(loop)

    # ✅ Giữ 2 task H4/M30 và AUTO như bạn đang chạy
    loop.create_task(scheduler_loop_m30_h4(app))        # H4/M30
    # ❌ KHÔNG tạo task M5 ở đây (đã tạo trong build_app via app.create_task)
    # loop.create_task(m5_report_loop(app, storage))
    loop.create_task(start_auto_loop(app, storage))     # AUTO Trade_engine.py hoạt động 

    use_webhook = os.getenv("USE_WEBHOOK", "false").lower() == "true"
    base_url    = os.getenv("WEBHOOK_URL") or os.getenv("RENDER_EXTERNAL_URL")
    port        = int(os.getenv("PORT", "8443"))
    token       = os.getenv("TELEGRAM_BOT_TOKEN")
    webhook_path= os.getenv("WEBHOOK_PATH", token or "")

    if use_webhook and base_url:
        base_url = base_url.rstrip("/")
        full_url = f"{base_url}/{webhook_path.lstrip('/')}"
        print(f"Running WEBHOOK on 0.0.0.0:{port} → {full_url}")
        app.run_webhook(listen="0.0.0.0", port=port, url_path=webhook_path, webhook_url=full_url)
    else:
        print("Running POLLING mode")
        app.run_polling()
# ----------------------- /main.py -----------------------
