# tg/join_gate.py
# -*- coding: utf-8 -*-
"""
JOIN GATE (Manual Only)
- KHÔNG dùng join-request tự động. Chỉ duyệt thủ công:
  • Khách bấm deeplink vào bot -> xem hướng dẫn + liên hệ admin.
  • Admin tạo link mời 1 người (có hạn giờ) cho GROUP/CHANNEL: /approvelink_group|channel <user_id> [phút]
- Anti-spam link trong GROUP (xóa/bẻ, whitelist domain).
- Không yêu cầu JobQueue.

ENV cần (đặt trong .env):
  TELEGRAM_BROADCAST_BOT_TOKEN=<token của bot quản trị (ví dụ @Doghli_bot)>

  JOIN_GATE_VIP_GROUP_ID=-100xxxxxxxxxx         # chat_id của GROUP private
  JOIN_GATE_VIP_CHANNEL_ID=-100yyyyyyyyyy       # chat_id của CHANNEL private

  JOIN_GATE_ADMIN_IDS=547597578,7404...         # ID admin được phép chạy lệnh
  JOIN_GATE_ADMIN_CONTACT_USERNAME=Tinh_Nguyen  # username (không @) để hiển thị cho khách
  JOIN_GATE_ADMIN_CONTACT_ID=547597578          # ID admin (để bot cố gắng DM)
  JOIN_GATE_ADMIN_ALERT_CHAT_ID=547597578       # nơi nhận thông báo (tuỳ chọn)

  # Link cố định (chỉ để hiển thị tham khảo, KHÔNG auto dùng):
  JOIN_GATE_GROUP_STATIC_LINK=https://t.me/+uhxJdHPQRdQ1OTQ1
  JOIN_GATE_CHANNEL_STATIC_LINK=https://t.me/+JmzYHeskihY1NmM1

  # Deeplink dẫn khách vào bot (nên dùng cái này để khách đọc hướng dẫn):
  JOIN_GATE_ENTRY_DEEPLINK=https://t.me/AllinoneBot?start=VIP

  # Tin nhắn DM thêm cho khách sau khi /start VIP (tuỳ chọn):
  JOIN_GATE_DM_TEXT=Chao ban! Vui long lien he admin @Tinh_Nguyen va gui UID/screenshot follow...

  # Anti-spam cho GROUP (mặc định bật):
  JOIN_GATE_ANTISPAM_ON=true
  JOIN_GATE_ALLOW_LINKS=false
  JOIN_GATE_ALLOWED_LINK_DOMAINS=t.me/yourvip,binance.com
  JOIN_GATE_LINK_SPAM_MAX=2
  JOIN_GATE_LINK_SPAM_WINDOW_S=300
  JOIN_GATE_AUTOBAN_ON=true
  JOIN_GATE_AUTODELETE_LINKS=true
"""

from __future__ import annotations

import os, re, time, asyncio
from datetime import datetime
from collections import defaultdict, deque
from typing import Optional, Tuple

from telegram import Bot, Update, constants
from telegram.error import TelegramError
from telegram.ext import Application, CommandHandler, ContextTypes, MessageHandler, filters


# ---------- Load ENV & defaults ----------
def _env_bool(name: str, default: bool) -> bool:
    v = (os.getenv(name, "").strip() or str(default)).lower()
    return v in ("1","true","yes","on")

def _env_int(name: str, default: int) -> int:
    try:
        return int(os.getenv(name, str(default)))
    except Exception:
        return default

def _parse_admin_ids() -> set[int]:
    s = os.getenv("JOIN_GATE_ADMIN_IDS", "547597578")
    out = set()
    for tok in re.split(r"[,\s]+", s.strip()):
        if not tok: continue
        try: out.add(int(tok))
        except: pass
    return out

GATE_TOKEN = os.getenv("TELEGRAM_BROADCAST_BOT_TOKEN", "")
gate_bot: Optional[Bot] = Bot(GATE_TOKEN) if GATE_TOKEN else None

VIP_GROUP_ID   = _env_int("JOIN_GATE_VIP_GROUP_ID",   0)
VIP_CHANNEL_ID = _env_int("JOIN_GATE_VIP_CHANNEL_ID", 0)

ADMIN_IDS   = _parse_admin_ids()
ADMIN_ALERT = os.getenv("JOIN_GATE_ADMIN_ALERT_CHAT_ID", "").strip()
ADMIN_ALERT_ID = int(ADMIN_ALERT) if ADMIN_ALERT.lstrip("-").isdigit() else None

ADMIN_CONTACT_USERNAME = os.getenv("JOIN_GATE_ADMIN_CONTACT_USERNAME", "Tinh_Nguyen").lstrip("@")
ADMIN_CONTACT_ID       = _env_int("JOIN_GATE_ADMIN_CONTACT_ID", 547597578)

ENTRY_DEEPLINK = os.getenv("JOIN_GATE_ENTRY_DEEPLINK", "https://t.me/AllinoneBot?start=VIP").strip()
GROUP_STATIC_LINK   = os.getenv("JOIN_GATE_GROUP_STATIC_LINK", "https://t.me/+uhxJdHPQRdQ1OTQ1").strip()
CHANNEL_STATIC_LINK = os.getenv("JOIN_GATE_CHANNEL_STATIC_LINK", "https://t.me/+JmzYHeskihY1NmM1").strip()
JOIN_DM_TEXT        = os.getenv("JOIN_GATE_DM_TEXT", "").strip()

# Anti-spam
ANTISPAM_ON  = _env_bool("JOIN_GATE_ANTISPAM_ON", True)
ALLOW_LINKS  = _env_bool("JOIN_GATE_ALLOW_LINKS", False)
WL_DOMAINS   = [d.strip().lower() for d in (os.getenv("JOIN_GATE_ALLOWED_LINK_DOMAINS","") or "").split(",") if d.strip()]
SPAM_MAX     = _env_int("JOIN_GATE_LINK_SPAM_MAX", 2)
SPAM_WINDOW  = _env_int("JOIN_GATE_LINK_SPAM_WINDOW_S", 300)
AUTOBAN_ON   = _env_bool("JOIN_GATE_AUTOBAN_ON", True)
AUTODELETE   = _env_bool("JOIN_GATE_AUTODELETE_LINKS", True)

_URL_RE = re.compile(r"(https?://\S+|t\.me/\S+)", re.IGNORECASE)
_violate: dict[Tuple[int,int], deque] = defaultdict(deque)


# ---------- Utils ----------
def _is_admin(uid: int) -> bool:
    return uid in ADMIN_IDS

async def _alert(context: ContextTypes.DEFAULT_TYPE, text: str):
    if not ADMIN_ALERT_ID: return
    try:
        await context.bot.send_message(ADMIN_ALERT_ID, text, parse_mode=constants.ParseMode.HTML, disable_web_page_preview=True)
    except Exception:
        pass

async def _dm_user(uid: int, text: str):
    try:
        if gate_bot:
            await gate_bot.send_message(uid, text, parse_mode=constants.ParseMode.MARKDOWN, disable_web_page_preview=True)
    except Exception:
        pass


# ---------- Commands: entry & help ----------
async def start_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    msg = update.effective_message
    uid = update.effective_user.id if update.effective_user else 0

    payload = ""
    try:
        if context.args: payload = (context.args[0] or "").strip().lower()
    except Exception:
        pass

    if payload == "vip":
        guide = [
            "🙋‍♂️ Chào bạn! Đây là cổng vào VIP (duyệt tay).",
            f"• Vui lòng liên hệ admin @{ADMIN_CONTACT_USERNAME} (ID: {ADMIN_CONTACT_ID})",
            "• Gửi UID/screenshot đã follow (Binance/BingX) hoặc thông tin copy trade.",
            "• Sau khi xác minh, admin sẽ cấp cho bạn 1 link mời RIÊNG (chỉ dùng 1 lần, có hạn giờ).",
            "",
            "ℹ️ Tham khảo:",
            f"• Group (KHÔNG dùng trực tiếp): {GROUP_STATIC_LINK}",
            f"• Channel: {CHANNEL_STATIC_LINK}",
        ]
        await msg.reply_text("\n".join(guide), disable_web_page_preview=True)
        if JOIN_DM_TEXT:
            await _dm_user(uid, JOIN_DM_TEXT)
        if ADMIN_ALERT_ID:
            try:
                uname = update.effective_user.username or f"id={uid}"
                await context.bot.send_message(
                    ADMIN_ALERT_ID,
                    f"📥 Yêu cầu VIP mới từ <code>{uname}</code> (uid={uid}).",
                    parse_mode=constants.ParseMode.HTML
                )
            except Exception:
                pass
        return

    await msg.reply_text("Xin chào! Dùng /vip_entry để xem cách tham gia VIP (duyệt tay).")

async def vip_entry(update: Update, context: ContextTypes.DEFAULT_TYPE):
    msg = update.effective_message
    lines = [
        "🔗 Cách tham gia VIP (duyệt tay):",
        f"• Bấm vào đây để bắt đầu: {ENTRY_DEEPLINK}",
        f"• Liên hệ admin @{ADMIN_CONTACT_USERNAME} (ID: {ADMIN_CONTACT_ID}) để được duyệt.",
        f"• (Tham khảo) Group: {GROUP_STATIC_LINK}",
        f"• (Tham khảo) Channel: {CHANNEL_STATIC_LINK}",
    ]
    await msg.reply_text("\n".join(lines), disable_web_page_preview=True)


# ---------- Commands: tạo link mời 1 người (manual approval) ----------
async def approvelink_group(update: Update, context: ContextTypes.DEFAULT_TYPE):
    msg = update.effective_message
    if not _is_admin(update.effective_user.id):
        await msg.reply_text("🚫 Bạn không có quyền."); return
    if gate_bot is None:
        await msg.reply_text("❌ Chưa cấu hình TELEGRAM_BROADCAST_BOT_TOKEN."); return
    if not VIP_GROUP_ID:
        await msg.reply_text("❌ Thiếu JOIN_GATE_VIP_GROUP_ID."); return
    if not context.args:
        await msg.reply_text("Dùng: /approvelink_group <user_id> [minutes_valid=120]"); return

    try:
        target  = int(context.args[0])
        minutes = int(context.args[1]) if len(context.args) >= 2 else 120
        minutes = max(1, minutes)
        expire_ts = int(time.time()) + minutes * 60

        link = await gate_bot.create_chat_invite_link(
            chat_id=VIP_GROUP_ID,
            name=f"VIP GROUP for {target}",
            member_limit=1,
            expire_date=expire_ts,
            creates_join_request=False  # vào thẳng vì đã duyệt tay
        )
        txt = (f"✅ Link mời GROUP (1 người, hết hạn {minutes}'): {link.invite_link}\n"
               f"→ GỬI trực tiếp cho <code>{target}</code>.")
        await msg.reply_text(txt, parse_mode=constants.ParseMode.HTML, disable_web_page_preview=True)

        # cố gắng DM luôn cho user
        try:
            await context.bot.send_message(
                chat_id=target,
                text=f"🎟️ Link vào GROUP VIP (hết hạn {minutes}'): {link.invite_link}",
                disable_web_page_preview=True
            )
        except Exception:
            pass
    except TelegramError as e:
        await msg.reply_text(f"❌ Lỗi tạo link: {e.message}")
    except Exception as e:
        await msg.reply_text(f"❌ Lỗi tạo link: {e}")

async def approvelink_channel(update: Update, context: ContextTypes.DEFAULT_TYPE):
    msg = update.effective_message
    if not _is_admin(update.effective_user.id):
        await msg.reply_text("🚫 Bạn không có quyền."); return
    if gate_bot is None:
        await msg.reply_text("❌ Chưa cấu hình TELEGRAM_BROADCAST_BOT_TOKEN."); return
    if not VIP_CHANNEL_ID:
        await msg.reply_text("❌ Thiếu JOIN_GATE_VIP_CHANNEL_ID."); return
    if not context.args:
        await msg.reply_text("Dùng: /approvelink_channel <user_id> [minutes_valid=120]"); return

    try:
        target  = int(context.args[0])
        minutes = int(context.args[1]) if len(context.args) >= 2 else 120
        minutes = max(1, minutes)
        expire_ts = int(time.time()) + minutes * 60

        link = await gate_bot.create_chat_invite_link(
            chat_id=VIP_CHANNEL_ID,
            name=f"VIP CHANNEL for {target}",
            member_limit=1,
            expire_date=expire_ts,
            creates_join_request=False
        )
        txt = (f"✅ Link mời CHANNEL (1 người, hết hạn {minutes}'): {link.invite_link}\n"
               f"→ GỬI trực tiếp cho <code>{target}</code>.")
        await msg.reply_text(txt, parse_mode=constants.ParseMode.HTML, disable_web_page_preview=True)

        try:
            await context.bot.send_message(
                chat_id=target,
                text=f"📣 Link vào CHANNEL VIP (hết hạn {minutes}'): {link.invite_link}",
                disable_web_page_preview=True
            )
        except Exception:
            pass
    except TelegramError as e:
        await msg.reply_text(f"❌ Lỗi tạo link: {e.message}")
    except Exception as e:
        await msg.reply_text(f"❌ Lỗi tạo link: {e}")


# ---------- Anti-spam cho GROUP ----------
def _is_allowed_link(text: str) -> bool:
    if ALLOW_LINKS:
        # cho phép link, nhưng nếu có whitelist thì vẫn ưu tiên whitelist
        if not WL_DOMAINS:
            return True
    low = text.lower()
    return any(dom in low for dom in WL_DOMAINS)

async def _moderate(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not ANTISPAM_ON: return
    chat = update.effective_chat
    msg  = update.effective_message
    if not chat or chat.id != VIP_GROUP_ID: return
    if not msg: return

    text = (msg.text or msg.caption or "")
    if not _URL_RE.search(text): return

    if _is_allowed_link(text): return

    # Vi phạm
    if AUTODELETE:
        try: await msg.delete()
        except Exception: pass

    key = (chat.id, msg.from_user.id if msg.from_user else 0)
    dq  = _violate[key]
    now = time.time()
    dq.append(now)
    while dq and now - dq[0] > SPAM_WINDOW:
        dq.popleft()

    if AUTOBAN_ON and len(dq) >= SPAM_MAX:
        try:
            if gate_bot:
                await gate_bot.ban_chat_member(chat_id=chat.id, user_id=key[1])
                try:
                    await gate_bot.unban_chat_member(chat_id=chat.id, user_id=key[1], only_if_banned=True)
                except Exception:
                    pass
        except Exception:
            pass
        dq.clear()
        await _alert(context, f"🚫 Auto-ban vì spam link: <code>{key[1]}</code> (group)")


# ---------- Public API ----------
def register_join_gate(app: Application) -> None:
    """
    Gọi trong main.py sau khi build app:
        from tg.join_gate import register_join_gate
        register_join_gate(app)
    """
    # Hướng dẫn
    app.add_handler(CommandHandler("start", start_cmd))
    app.add_handler(CommandHandler("vip_entry", vip_entry))

    # Tạo link mời 1 người (duyệt tay)
    app.add_handler(CommandHandler("approvelink_group",   approvelink_group))
    app.add_handler(CommandHandler("approvelink_channel", approvelink_channel))

    # Anti-spam GROUP
    app.add_handler(MessageHandler(filters.ALL, _moderate), group=21)
