# tg/join_gate.py
# -*- coding: utf-8 -*-
"""
Join Gate cho GROUP/CHANNEL VIP:
- Bridge thao tác quản trị (create invite link, approve/decline) qua token broadcast (@Doghli_bot),
  dù lệnh được gõ từ "All in one Bot".
- Quản lý thuê bao (grant/revoke/info, DB JSON).
- Anti-spam link cho GROUP.
- Tự động REVOKE primary invite link sau khi tạo link join-request (để bắt buộc duyệt).
- Tương thích cả hai trường hợp: có JobQueue và không có JobQueue.

ENV cần:
  TELEGRAM_BROADCAST_BOT_TOKEN=<token của @Doghli_bot>
  JOIN_GATE_VIP_GROUP_ID=-100xxxxxxxxxx         # nếu dùng group
  JOIN_GATE_VIP_CHANNEL_ID=-100yyyyyyyyyy       # nếu dùng channel
  JOIN_GATE_ADMIN_ALERT_CHAT_ID=<user_id|group_id âm>   # nơi nhận cảnh báo (tùy chọn)
  JOIN_GATE_ADMIN_IDS=547597578,7404...         # ai được chạy lệnh admin

  JOIN_GATE_ANTISPAM_ON=true|false
  JOIN_GATE_ALLOW_LINKS=false
  JOIN_GATE_ALLOWED_LINK_DOMAINS=t.me/yourvip,binance.com
  JOIN_GATE_LINK_SPAM_MAX=2
  JOIN_GATE_LINK_SPAM_WINDOW_S=300
  JOIN_GATE_AUTOBAN_ON=true
  JOIN_GATE_AUTODELETE_LINKS=true

  JOIN_GATE_DM_TEXT="Chao ban!... (dùng \\n để xuống dòng)"
  JOIN_GATE_SUBS_DB=/data/vip_members.json
  JOIN_GATE_DEFAULT_SUBS_DAYS=30
"""

from __future__ import annotations

import asyncio
import json
import os
import re
import time
from collections import defaultdict, deque
from datetime import datetime
from typing import Any, Dict, Optional, Tuple

from telegram import (
    Bot,
    Update,
    ChatInviteLink,
    constants,
)
from telegram.ext import (
    Application,
    CommandHandler,
    ContextTypes,
    MessageHandler,
    filters,
)
from telegram.error import TelegramError

# ----------------- Bridge bot (dùng @Doghli_bot) -----------------
GATE_TOKEN = os.getenv("TELEGRAM_BROADCAST_BOT_TOKEN") or ""
gate_bot: Optional[Bot] = Bot(GATE_TOKEN) if GATE_TOKEN else None

VIP_GROUP_ID = int(os.getenv("JOIN_GATE_VIP_GROUP_ID", "0") or 0)
VIP_CHANNEL_ID = int(os.getenv("JOIN_GATE_VIP_CHANNEL_ID", "0") or 0)

ADMIN_ALERT_CHAT_ID_RAW = os.getenv("JOIN_GATE_ADMIN_ALERT_CHAT_ID", "").strip()
ADMIN_ALERT_CHAT_ID = int(ADMIN_ALERT_CHAT_ID_RAW) if (ADMIN_ALERT_CHAT_ID_RAW and ADMIN_ALERT_CHAT_ID_RAW.lstrip("-").isdigit()) else None

def _parse_admin_ids() -> set[int]:
    s = os.getenv("JOIN_GATE_ADMIN_IDS", "") or ""
    out: set[int] = set()
    for tok in re.split(r"[,\s]+", s.strip()):
        if not tok:
            continue
        try:
            out.add(int(tok))
        except Exception:
            pass
    return out

ADMIN_IDS = _parse_admin_ids()

# ----------------- Subs DB -----------------
SUBS_DB_PATH = os.getenv("JOIN_GATE_SUBS_DB", "vip_members.json")
DEFAULT_SUB_DAYS = int(os.getenv("JOIN_GATE_DEFAULT_SUBS_DAYS", "30") or 30)

# DB format: { "<user_id>": { "exp": <unix_ts>, "note": "..." } }
_subs_cache: Dict[str, Dict[str, Any]] = {}

async def _load_db() -> None:
    """Load DB async (safe cho môi trường có event loop)."""
    global _subs_cache
    try:
        def _read(path: str):
            if not os.path.exists(path):
                return {}
            with open(path, "r", encoding="utf-8") as f:
                return json.load(f)
        data = await asyncio.to_thread(_read, SUBS_DB_PATH)
        _subs_cache = data if isinstance(data, dict) else {}
    except Exception:
        _subs_cache = {}

def _load_db_sync() -> None:
    """Load DB sync (dùng khi không có JobQueue và chưa có loop)."""
    global _subs_cache
    try:
        if not os.path.exists(SUBS_DB_PATH):
            _subs_cache = {}
            return
        with open(SUBS_DB_PATH, "r", encoding="utf-8") as f:
            data = json.load(f)
            _subs_cache = data if isinstance(data, dict) else {}
    except Exception:
        _subs_cache = {}

async def _save_db() -> None:
    data = _subs_cache
    def _write(path: str, obj: dict):
        tmp = path + ".tmp"
        os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(obj, f, ensure_ascii=False, indent=2)
        os.replace(tmp, path)
    try:
        await asyncio.to_thread(_write, SUBS_DB_PATH, data)
    except Exception:
        pass

def _now_ts() -> int:
    return int(time.time())

def _fmt_time(ts: int) -> str:
    try:
        return datetime.fromtimestamp(ts).strftime("%Y-%m-%d %H:%M")
    except Exception:
        return str(ts)

# ----------------- Utilities -----------------
def _is_admin(user_id: int) -> bool:
    return user_id in ADMIN_IDS

async def _alert(context: ContextTypes.DEFAULT_TYPE, text: str) -> None:
    if not ADMIN_ALERT_CHAT_ID:
        return
    try:
        await context.bot.send_message(
            chat_id=ADMIN_ALERT_CHAT_ID,
            text=text,
            parse_mode=constants.ParseMode.HTML,
            disable_web_page_preview=True,
        )
    except Exception:
        pass

async def _dm_user(user_id: int, text: str, *, via_gate: bool = True) -> None:
    try:
        if via_gate and gate_bot:
            await gate_bot.send_message(
                chat_id=user_id,
                text=text,
                parse_mode=constants.ParseMode.MARKDOWN,
                disable_web_page_preview=True,
            )
    except TelegramError:
        # thường là user chưa /start bot
        pass
    except Exception:
        pass

# ---------- Helper: revoke primary invite link (ngăn join thẳng) ----------
async def _revoke_primary_invite(chat_id: int) -> bool:
    """
    Thu hồi (revoke) primary invite link để bắt buộc mọi người phải dùng link dạng join-request.
    Yêu cầu: @Doghli_bot là admin của chat_id.
    """
    if not gate_bot:
        return False
    try:
        primary = await gate_bot.export_chat_invite_link(chat_id=chat_id)  # str (primary link)
        if not primary:
            return False
        await gate_bot.revoke_chat_invite_link(chat_id=chat_id, invite_link=primary)
        return True
    except TelegramError:
        return False
    except Exception:
        return False

# ----------------- Commands: create links -----------------
async def _ensure_gate(msg) -> bool:
    if gate_bot is None:
        await msg.reply_text("❌ Chưa cấu hình TELEGRAM_BROADCAST_BOT_TOKEN (bot @Doghli_bot).")
        return False
    return True

async def vip_link_group(update: Update, context: ContextTypes.DEFAULT_TYPE):
    msg = update.effective_message
    uid = update.effective_user.id if update.effective_user else 0
    if not _is_admin(uid):
        await msg.reply_text("🚫 Bạn không có quyền.")
        return
    if not await _ensure_gate(msg):
        return
    if not VIP_GROUP_ID:
        await msg.reply_text("❌ Thiếu JOIN_GATE_VIP_GROUP_ID.")
        return
    try:
        # Tạo link join-request
        link: ChatInviteLink = await gate_bot.create_chat_invite_link(
            chat_id=VIP_GROUP_ID,
            creates_join_request=True,
            name="VIP Group Link",
        )
        # Revoke primary link mở
        revoked = await _revoke_primary_invite(VIP_GROUP_ID)
        extra = "\n🔒 Đã revoke primary invite link (link mở)." if revoked else ""
        await msg.reply_text(f"🔗 Link join-request GROUP mới:\n{link.invite_link}{extra}")
    except TelegramError as e:
        await msg.reply_text(f"❌ Tạo link lỗi: {e.message}")
    except Exception as e:
        await msg.reply_text(f"❌ Tạo link lỗi: {e}")

async def vip_link_channel(update: Update, context: ContextTypes.DEFAULT_TYPE):
    msg = update.effective_message
    uid = update.effective_user.id if update.effective_user else 0
    if not _is_admin(uid):
        await msg.reply_text("🚫 Bạn không có quyền.")
        return
    if not await _ensure_gate(msg):
        return
    if not VIP_CHANNEL_ID:
        await msg.reply_text("❌ Thiếu JOIN_GATE_VIP_CHANNEL_ID.")
        return
    try:
        # Tạo link join-request
        link: ChatInviteLink = await gate_bot.create_chat_invite_link(
            chat_id=VIP_CHANNEL_ID,
            creates_join_request=True,
            name="VIP Channel Link",
        )
        # Revoke primary link mở
        revoked = await _revoke_primary_invite(VIP_CHANNEL_ID)
        extra = "\n🔒 Đã revoke primary invite link (link mở)." if revoked else ""
        await msg.reply_text(f"🔗 Link join-request CHANNEL mới:\n{link.invite_link}{extra}")
    except TelegramError as e:
        await msg.reply_text(f"❌ Tạo link lỗi: {e.message}")
    except Exception as e:
        await msg.reply_text(f"❌ Tạo link lỗi: {e}")

# ----------------- Commands: approve/decline -----------------
async def approve_group(update: Update, context: ContextTypes.DEFAULT_TYPE):
    msg = update.effective_message
    uid = update.effective_user.id if update.effective_user else 0
    if not _is_admin(uid):
        await msg.reply_text("🚫 Bạn không có quyền.")
        return
    if not await _ensure_gate(msg):
        return
    if not VIP_GROUP_ID:
        await msg.reply_text("❌ Thiếu JOIN_GATE_VIP_GROUP_ID.")
        return
    if not context.args:
        await msg.reply_text("Dùng: /approve_group <user_id>")
        return
    try:
        target = int(context.args[0])
        await gate_bot.approve_chat_join_request(chat_id=VIP_GROUP_ID, user_id=target)
        await msg.reply_text(f"✅ Approved vào GROUP: <code>{target}</code>", parse_mode=constants.ParseMode.HTML)
    except TelegramError as e:
        await msg.reply_text(f"❌ Approve lỗi: {e.message}")
    except Exception as e:
        await msg.reply_text(f"❌ Approve lỗi: {e}")

async def decline_group(update: Update, context: ContextTypes.DEFAULT_TYPE):
    msg = update.effective_message
    uid = update.effective_user.id if update.effective_user else 0
    if not _is_admin(uid):
        await msg.reply_text("🚫 Bạn không có quyền.")
        return
    if not await _ensure_gate(msg):
        return
    if not VIP_GROUP_ID:
        await msg.reply_text("❌ Thiếu JOIN_GATE_VIP_GROUP_ID.")
        return
    if not context.args:
        await msg.reply_text("Dùng: /decline_group <user_id>")
        return
    try:
        target = int(context.args[0])
        await gate_bot.decline_chat_join_request(chat_id=VIP_GROUP_ID, user_id=target)
        await msg.reply_text(f"🛑 Declined vào GROUP: <code>{target}</code>", parse_mode=constants.ParseMode.HTML)
    except TelegramError as e:
        await msg.reply_text(f"❌ Decline lỗi: {e.message}")
    except Exception as e:
        await msg.reply_text(f"❌ Decline lỗi: {e}")

async def approve_channel(update: Update, context: ContextTypes.DEFAULT_TYPE):
    msg = update.effective_message
    uid = update.effective_user.id if update.effective_user else 0
    if not _is_admin(uid):
        await msg.reply_text("🚫 Bạn không có quyền.")
        return
    if not await _ensure_gate(msg):
        return
    if not VIP_CHANNEL_ID:
        await msg.reply_text("❌ Thiếu JOIN_GATE_VIP_CHANNEL_ID.")
        return
    if not context.args:
        await msg.reply_text("Dùng: /approve_channel <user_id>")
        return
    try:
        target = int(context.args[0])
        await gate_bot.approve_chat_join_request(chat_id=VIP_CHANNEL_ID, user_id=target)
        await msg.reply_text(f"✅ Approved vào CHANNEL: <code>{target}</code>", parse_mode=constants.ParseMode.HTML)
    except TelegramError as e:
        await msg.reply_text(f"❌ Approve lỗi: {e.message}")
    except Exception as e:
        await msg.reply_text(f"❌ Approve lỗi: {e}")

async def decline_channel(update: Update, context: ContextTypes.DEFAULT_TYPE):
    msg = update.effective_message
    uid = update.effective_user.id if update.effective_user else 0
    if not _is_admin(uid):
        await msg.reply_text("🚫 Bạn không có quyền.")
        return
    if not await _ensure_gate(msg):
        return
    if not VIP_CHANNEL_ID:
        await msg.reply_text("❌ Thiếu JOIN_GATE_VIP_CHANNEL_ID.")
        return
    if not context.args:
        await msg.reply_text("Dùng: /decline_channel <user_id>")
        return
    try:
        target = int(context.args[0])
        await gate_bot.decline_chat_join_request(chat_id=VIP_CHANNEL_ID, user_id=target)
        await msg.reply_text(f"🛑 Declined vào CHANNEL: <code>{target}</code>", parse_mode=constants.ParseMode.HTML)
    except TelegramError as e:
        await msg.reply_text(f"❌ Decline lỗi: {e.message}")
    except Exception as e:
        await msg.reply_text(f"❌ Decline lỗi: {e}")

# ----------------- Subs: grant/revoke/info/dump/reload -----------------
async def subs_reload(update: Update, context: ContextTypes.DEFAULT_TYPE):
    msg = update.effective_message
    uid = update.effective_user.id if update.effective_user else 0
    if not _is_admin(uid):
        await msg.reply_text("🚫 Bạn không có quyền.")
        return
    await _load_db()
    await msg.reply_text("🔄 Đã reload DB subs.")

async def subs_dump(update: Update, context: ContextTypes.DEFAULT_TYPE):
    msg = update.effective_message
    uid = update.effective_user.id if update.effective_user else 0
    if not _is_admin(uid):
        await msg.reply_text("🚫 Bạn không có quyền.")
        return
    await _save_db()
    await msg.reply_text(f"💾 Subs DB path: {SUBS_DB_PATH}\nSố entries: {len(_subs_cache)}")

async def grant(update: Update, context: ContextTypes.DEFAULT_TYPE):
    msg = update.effective_message
    uid = update.effective_user.id if update.effective_user else 0
    if not _is_admin(uid):
        await msg.reply_text("🚫 Bạn không có quyền.")
        return
    if not context.args:
        await msg.reply_text("Dùng: /grant <user_id> [days]")
        return
    try:
        target = int(context.args[0])
    except Exception:
        await msg.reply_text("user_id không hợp lệ.")
        return
    days = DEFAULT_SUB_DAYS
    if len(context.args) >= 2:
        try:
            days = max(1, int(context.args[1]))
        except Exception:
            pass
    exp = _now_ts() + days * 86400
    _subs_cache[str(target)] = {"exp": exp}
    await _save_db()
    await msg.reply_text(
        f"✅ Grant {days} ngày cho <code>{target}</code> (exp: { _fmt_time(exp) })",
        parse_mode=constants.ParseMode.HTML,
    )

async def revoke(update: Update, Context: ContextTypes.DEFAULT_TYPE):
    # giữ tương thích tên tham số context (không dùng)
    msg = update.effective_message
    uid = update.effective_user.id if update.effective_user else 0
    if not _is_admin(uid):
        await msg.reply_text("🚫 Bạn không có quyền.")
        return
    if not update.message or not update.message.text:
        await msg.reply_text("Dùng: /revoke <user_id>")
        return
    parts = update.message.text.strip().split()
    if len(parts) < 2:
        await msg.reply_text("Dùng: /revoke <user_id>")
        return
    try:
        target = int(parts[1])
    except Exception:
        await msg.reply_text("user_id không hợp lệ.")
        return
    _subs_cache.pop(str(target), None)
    await _save_db()
    await msg.reply_text(f"🗑️ Đã revoke <code>{target}</code>", parse_mode=constants.ParseMode.HTML)
    await _kick_everywhere(target)

async def subinfo(update: Update, context: ContextTypes.DEFAULT_TYPE):
    msg = update.effective_message
    uid = update.effective_user.id if update.effective_user else 0
    if not _is_admin(uid):
        await msg.reply_text("🚫 Bạn không có quyền.")
        return
    if not context.args:
        await msg.reply_text("Dùng: /subinfo <user_id>")
        return
    try:
        target = int(context.args[0])
    except Exception:
        await msg.reply_text("user_id không hợp lệ.")
        return
    rec = _subs_cache.get(str(target))
    if not rec:
        await msg.reply_text("❓ Không có trong DB.")
        return
    exp = int(rec.get("exp", 0) or 0)
    await msg.reply_text(
        f"ℹ️ <code>{target}</code> exp: { _fmt_time(exp) } ({max(0, exp - _now_ts())//86400} ngày còn lại)",
        parse_mode=constants.ParseMode.HTML,
    )

# ----------------- Expiry scan -----------------
async def _kick(chat_id: int, user_id: int) -> None:
    if not gate_bot:
        return
    try:
        # Ban rồi unban để “kick mềm” — cho phép join lại sau này
        await gate_bot.ban_chat_member(chat_id=chat_id, user_id=user_id)
        try:
            await gate_bot.unban_chat_member(chat_id=chat_id, user_id=user_id, only_if_banned=True)
        except Exception:
            pass
    except Exception:
        pass

async def _kick_everywhere(user_id: int) -> None:
    if VIP_GROUP_ID:
        await _kick(VIP_GROUP_ID, user_id)
    if VIP_CHANNEL_ID:
        await _kick(VIP_CHANNEL_ID, user_id)

async def _scan_and_kick(context: ContextTypes.DEFAULT_TYPE):
    now = _now_ts()
    expired = [int(uid) for uid, rec in (_subs_cache or {}).items()
               if int(rec.get("exp", 0) or 0) > 0 and int(rec["exp"]) <= now]
    for u in expired:
        await _kick_everywhere(u)
        _subs_cache.pop(str(u), None)
        if ADMIN_ALERT_CHAT_ID:
            try:
                await context.bot.send_message(ADMIN_ALERT_CHAT_ID, f"⚠️ Đã kick hết hạn: {u}")
            except Exception:
                pass
    if expired:
        await _save_db()

# ----------------- Anti-spam (GROUP) -----------------
ANTISPAM_ON = (os.getenv("JOIN_GATE_ANTISPAM_ON", "true").lower() == "true")
ALLOW_LINKS = (os.getenv("JOIN_GATE_ALLOW_LINKS", "false").lower() == "true")
WL_DOMAINS = [d.strip().lower() for d in (os.getenv("JOIN_GATE_ALLOWED_LINK_DOMAINS", "") or "").split(",") if d.strip()]
SPAM_MAX = int(os.getenv("JOIN_GATE_LINK_SPAM_MAX", "2") or 2)
SPAM_WINDOW = int(os.getenv("JOIN_GATE_LINK_SPAM_WINDOW_S", "300") or 300)
AUTOBAN_ON = (os.getenv("JOIN_GATE_AUTOBAN_ON", "true").lower() == "true")
AUTODELETE = (os.getenv("JOIN_GATE_AUTODELETE_LINKS", "true").lower() == "true")

_violate: dict[Tuple[int,int], deque] = defaultdict(deque)  # key=(chat_id,user_id) -> deque[timestamps]
_URL_RE = re.compile(r"(https?://\S+|t\.me/\S+)", re.IGNORECASE)

def _is_allowed_link(text: str) -> bool:
    if ALLOW_LINKS:
        if not WL_DOMAINS:
            return True
        low = text.lower()
        return any(dom in low for dom in WL_DOMAINS)
    # không cho link: chỉ cho whitelist
    low = text.lower()
    return any(dom in low for dom in WL_DOMAINS)

async def _moderate(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not ANTISPAM_ON:
        return
    msg = update.effective_message
    chat = update.effective_chat
    if not chat or chat.id != VIP_GROUP_ID:
        return
    if not msg or not (msg.text or msg.caption):
        return
    text = (msg.text or msg.caption or "")
    if not _URL_RE.search(text):
        return

    # link phát hiện
    allowed = _is_allowed_link(text)
    if allowed:
        return

    # vi phạm
    if AUTODELETE:
        try:
            await msg.delete()
        except Exception:
            pass

    # ghi nhận
    key = (chat.id, msg.from_user.id if msg.from_user else 0)
    dq = _violate[key]
    now = time.time()
    dq.append(now)
    # cắt cửa sổ
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

# ----------------- Register -----------------
def register_join_gate(app: Application) -> None:
    """
    Gọi trong main.py:
        from tg.join_gate import register_join_gate
        app = build_app()
        register_join_gate(app)
    """
    # Handlers lệnh quản trị/link
    app.add_handler(CommandHandler("vip_link_group", vip_link_group))
    app.add_handler(CommandHandler("vip_link_channel", vip_link_channel))
    app.add_handler(CommandHandler("approve_group", approve_group))
    app.add_handler(CommandHandler("decline_group", decline_group))
    app.add_handler(CommandHandler("approve_channel", approve_channel))
    app.add_handler(CommandHandler("decline_channel", decline_channel))

    # Subs
    app.add_handler(CommandHandler("grant", grant))
    app.add_handler(CommandHandler("revoke", revoke))
    app.add_handler(CommandHandler("subinfo", subinfo))
    app.add_handler(CommandHandler("subs_dump", subs_dump))
    app.add_handler(CommandHandler("subs_reload", subs_reload))

    # Anti-spam (chỉ hoạt động nếu app hiện tại ở trong GROUP và có quyền xóa)
    app.add_handler(MessageHandler(filters.ALL, _moderate), group=21)

    # --- DB + scheduler ---
    jq = getattr(app, "job_queue", None)
    if jq:
        jq.run_once(lambda c: asyncio.create_task(_load_db()), when=1)
        jq.run_repeating(_scan_and_kick, interval=3600, first=30)
    else:
        _load_db_sync()

# --------------- Optional: DM template khi có join-request ---------------
# LƯU Ý: để nhận được ChatJoinRequest update, bot phải chạy BẰNG CHÍNH token @Doghli_bot.
# Với mô hình bridge 1-app hiện tại, ta không nhận được event này; admin duyệt thủ công bằng lệnh.
JOIN_DM_TEXT = os.getenv("JOIN_GATE_DM_TEXT", "").strip()

async def _send_join_dm(user_id: int):
    if not JOIN_DM_TEXT:
        return
    await _dm_user(user_id, JOIN_DM_TEXT, via_gate=True)
