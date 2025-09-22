# tg/join_gate.py
# -*- coding: utf-8 -*-
from __future__ import annotations

import os, re, time
from collections import defaultdict, deque
from datetime import datetime
from typing import Optional, Tuple

from telegram import Bot, Update, constants
from telegram.error import TelegramError
from telegram.ext import Application, CommandHandler, ContextTypes, MessageHandler, filters

# ========= ENV helpers =========
def _env_bool(k: str, default: bool) -> bool:
    v = (os.getenv(k, "").strip() or str(default)).lower()
    return v in ("1", "true", "yes", "on")

def _env_int(k: str, default: int) -> int:
    try: return int(os.getenv(k, str(default)))
    except: return default

def _parse_ids(s: str) -> set[int]:
    out = set()
    for tok in re.split(r"[,\s]+", (s or "").strip()):
        if not tok: continue
        try: out.add(int(tok))
        except: pass
    return out

# ========= Config từ .env =========
GATE_TOKEN = os.getenv("TELEGRAM_BROADCAST_BOT_TOKEN", "")
gate_bot: Optional[Bot] = Bot(GATE_TOKEN) if GATE_TOKEN else None

VIP_GROUP_ID   = _env_int("JOIN_GATE_VIP_GROUP_ID",   0)
VIP_CHANNEL_ID = _env_int("JOIN_GATE_VIP_CHANNEL_ID", 0)

ADMIN_IDS      = _parse_ids(os.getenv("JOIN_GATE_ADMIN_IDS", ""))
ADMIN_ALERT_ID = _env_int("JOIN_GATE_ADMIN_ALERT_CHAT_ID", 0) or None

ADMIN_CONTACT_USERNAME = os.getenv("JOIN_GATE_ADMIN_CONTACT_USERNAME", "admin").lstrip("@")
ADMIN_CONTACT_ID       = _env_int("JOIN_GATE_ADMIN_CONTACT_ID", 0)

ENTRY_DEEPLINK   = os.getenv("JOIN_GATE_ENTRY_DEEPLINK", "").strip() or "https://t.me/YourBot?start=VIP"
GROUP_STATIC_LINK = os.getenv("JOIN_GATE_GROUP_STATIC_LINK", "").strip()
CHAN_STATIC_LINK  = os.getenv("JOIN_GATE_CHANNEL_STATIC_LINK", "").strip()
JOIN_DM_TEXT      = os.getenv("JOIN_GATE_DM_TEXT", "").strip()

# Anti-spam
ANTISPAM_ON  = _env_bool("JOIN_GATE_ANTISPAM_ON", True)
ALLOW_LINKS  = _env_bool("JOIN_GATE_ALLOW_LINKS", False)
WL_DOMAINS   = [d.strip().lower() for d in (os.getenv("JOIN_GATE_ALLOWED_LINK_DOMAINS","") or "").split(",") if d.strip()]
SPAM_MAX     = _env_int("JOIN_GATE_LINK_SPAM_MAX", 2)
SPAM_WINDOW  = _env_int("JOIN_GATE_LINK_SPAM_WINDOW_S", 300)
AUTOBAN_ON   = _env_bool("JOIN_GATE_AUTOBAN_ON", True)
AUTODELETE   = _env_bool("JOIN_GATE_AUTODELETE_LINKS", True)

_URL_RE = re.compile(r"(https?://\S+|t\.me/\S+)", re.IGNORECASE)
_violate: dict[Tuple[int, int], deque] = defaultdict(deque)

# ========= Utils =========
def _is_admin(uid: int) -> bool:
    return uid in ADMIN_IDS

async def _alert(context: ContextTypes.DEFAULT_TYPE, text: str):
    if not ADMIN_ALERT_ID: return
    try:
        await context.bot.send_message(ADMIN_ALERT_ID, text, parse_mode=constants.ParseMode.HTML, disable_web_page_preview=True)
    except Exception:
        pass

async def _dm(uid: int, text: str):
    if not (gate_bot and uid): return
    try:
        await gate_bot.send_message(uid, text, parse_mode=constants.ParseMode.MARKDOWN, disable_web_page_preview=True)
    except Exception:
        pass

# ========= Entry /start =========
async def start_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    msg = update.effective_message
    uid = update.effective_user.id if update.effective_user else 0
    payload = ""
    try:
        if context.args: payload = (context.args[0] or "").strip().lower()
    except Exception:
        pass

    # Khách bấm deeplink ...?start=VIP
    if payload == "vip":
        lines = [
            "🙋‍♂️ Chào bạn! Đây là cổng vào VIP (duyệt thủ công).",
            f"• Vui lòng PM admin @{ADMIN_CONTACT_USERNAME} (ID: {ADMIN_CONTACT_ID}) để được duyệt.",
            "• Gửi UID + screenshot follow (Binance/BingX) hoặc thông tin copy trade.",
            "• Sau khi xác minh, admin sẽ cấp cho bạn link mời RIÊNG (1 người, có hạn giờ).",
        ]
        if GROUP_STATIC_LINK: lines.append(f"• (Tham khảo) Group: {GROUP_STATIC_LINK}")
        if CHAN_STATIC_LINK:  lines.append(f"• (Tham khảo) Channel: {CHAN_STATIC_LINK}")
        await msg.reply_text("\n".join(lines), disable_web_page_preview=True)
        if JOIN_DM_TEXT: await _dm(uid, JOIN_DM_TEXT)

        # Báo admin có yêu cầu mới
        if ADMIN_ALERT_ID:
            u = update.effective_user
            uname = (u.username and f"@{u.username}") or f"id={uid}"
            await _alert(context, f"📥 Yêu cầu VIP mới từ <code>{uname}</code> (uid={uid}).")
        return

    # Trường hợp /start thường
    await msg.reply_text(
        "Xin chào! Để tham gia VIP:\n"
        f"• Bấm deeplink: {ENTRY_DEEPLINK}\n"
        f"• Liên hệ admin @{ADMIN_CONTACT_USERNAME} (ID: {ADMIN_CONTACT_ID}) để được duyệt.",
        disable_web_page_preview=True
    )

# ========= Lệnh admin tạo link mời (duyệt tay) =========
async def approvelink_group(update: Update, context: ContextTypes.DEFAULT_TYPE):
    msg = update.effective_message
    me  = update.effective_user
    if not (me and _is_admin(me.id)):
        await msg.reply_text("🚫 Bạn không có quyền."); return
    if not (gate_bot and VIP_GROUP_ID):
        await msg.reply_text("❌ Thiếu TOKEN hoặc JOIN_GATE_VIP_GROUP_ID."); return
    if not context.args:
        await msg.reply_text("Dùng: /approvelink_group <user_id> [phút=120]"); return

    try:
        target  = int(context.args[0])
        minutes = int(context.args[1]) if len(context.args) >= 2 else 120
        minutes = max(1, minutes)
        expire  = int(time.time()) + minutes * 60

        link = await gate_bot.create_chat_invite_link(
            chat_id=VIP_GROUP_ID,
            name=f"VIP GROUP for {target}",
            member_limit=1,
            expire_date=expire,
            creates_join_request=False
        )
        txt = (f"✅ Link mời GROUP (1 người, hết hạn {minutes}'): {link.invite_link}\n"
               f"→ GỬI trực tiếp cho <code>{target}</code>.")
        await msg.reply_text(txt, parse_mode=constants.ParseMode.HTML, disable_web_page_preview=True)

        # Thử DM thẳng cho user
        try:
            await context.bot.send_message(target, f"🎟️ Link vào GROUP VIP (hết hạn {minutes}'): {link.invite_link}",
                                           disable_web_page_preview=True)
        except Exception:
            pass
    except TelegramError as e:
        await msg.reply_text(f"❌ Lỗi tạo link: {e.message}")
    except Exception as e:
        await msg.reply_text(f"❌ Lỗi tạo link: {e}")

async def approvelink_channel(update: Update, context: ContextTypes.DEFAULT_TYPE):
    msg = update.effective_message
    me  = update.effective_user
    if not (me and _is_admin(me.id)):
        await msg.reply_text("🚫 Bạn không có quyền."); return
    if not (gate_bot and VIP_CHANNEL_ID):
        await msg.reply_text("❌ Thiếu TOKEN hoặc JOIN_GATE_VIP_CHANNEL_ID."); return
    if not context.args:
        await msg.reply_text("Dùng: /approvelink_channel <user_id> [phút=120]"); return

    try:
        target  = int(context.args[0])
        minutes = int(context.args[1]) if len(context.args) >= 2 else 120
        minutes = max(1, minutes)
        expire  = int(time.time()) + minutes * 60

        link = await gate_bot.create_chat_invite_link(
            chat_id=VIP_CHANNEL_ID,
            name=f"VIP CHANNEL for {target}",
            member_limit=1,
            expire_date=expire,
            creates_join_request=False
        )
        txt = (f"✅ Link mời CHANNEL (1 người, hết hạn {minutes}'): {link.invite_link}\n"
               f"→ GỬI trực tiếp cho <code>{target}</code>.")
        await msg.reply_text(txt, parse_mode=constants.ParseMode.HTML, disable_web_page_preview=True)

        try:
            await context.bot.send_message(target, f"📣 Link vào CHANNEL VIP (hết hạn {minutes}'): {link.invite_link}",
                                           disable_web_page_preview=True)
        except Exception:
            pass
    except TelegramError as e:
        await msg.reply_text(f"❌ Lỗi tạo link: {e.message}")
    except Exception as e:
        await msg.reply_text(f"❌ Lỗi tạo link: {e}")

# ========= Anti-spam trong GROUP =========
def _allowed_link(text: str) -> bool:
    if ALLOW_LINKS:
        if not WL_DOMAINS:  # cho phép toàn bộ
            return True
    low = text.lower()
    return any(dom in low for dom in WL_DOMAINS)

async def _moderate(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not ANTISPAM_ON: return
    chat = update.effective_chat
    msg  = update.effective_message
    if not (chat and msg): return
    if chat.id != VIP_GROUP_ID: return

    text = (msg.text or msg.caption or "")
    if not text: return
    if not _URL_RE.search(text): return
    if _allowed_link(text): return

    # Vi phạm: xoá & ghi nhận
    if AUTODELETE:
        try: await msg.delete()
        except Exception: pass

    uid = msg.from_user.id if msg.from_user else 0
    key = (chat.id, uid)
    dq  = _violate[key]
    now = time.time()
    dq.append(now)
    while dq and now - dq[0] > SPAM_WINDOW:
        dq.popleft()

    if AUTOBAN_ON and len(dq) >= SPAM_MAX:
        try:
            if gate_bot:
                await gate_bot.ban_chat_member(chat_id=chat.id, user_id=uid)
                try:
                    await gate_bot.unban_chat_member(chat_id=chat.id, user_id=uid, only_if_banned=True)
                except Exception:
                    pass
        except Exception:
            pass
        dq.clear()
        await _alert(context, f"🚫 Auto-ban vì spam link: <code>{uid}</code>")

# ========= Đăng ký vào Application =========
def register_join_gate(app: Application) -> None:
    """
    main.py:
        from tg.join_gate import register_join_gate
        register_join_gate(app)
    """
    app.add_handler(CommandHandler("start", start_cmd))
    app.add_handler(CommandHandler("vip_entry", lambda u,c: u.effective_message.reply_text(
        "🔗 Cách tham gia VIP: \n"
        f"• Bấm deeplink: {ENTRY_DEEPLINK}\n"
        f"• PM admin @{ADMIN_CONTACT_USERNAME} (ID: {ADMIN_CONTACT_ID}) để được duyệt.\n"
        + (f"• Group: {GROUP_STATIC_LINK}\n" if GROUP_STATIC_LINK else "")
        + (f"• Channel: {CHAN_STATIC_LINK}\n" if CHAN_STATIC_LINK else ""),
        disable_web_page_preview=True
    )))

    app.add_handler(CommandHandler("approvelink_group",   approvelink_group))
    app.add_handler(CommandHandler("approvelink_channel", approvelink_channel))

    app.add_handler(MessageHandler(filters.ALL, _moderate), group=21)
