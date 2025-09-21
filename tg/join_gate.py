# tg/join_gate.py
from __future__ import annotations

import os
import re
import json
import time
import logging
from datetime import datetime, timedelta, timezone
from typing import Dict, Optional, Set, Tuple, List

from telegram import Update, ChatPermissions
from telegram.ext import (
    Application,
    ContextTypes,
    ChatJoinRequestHandler,
    CommandHandler,
    MessageHandler,
    filters,
)

log = logging.getLogger(__name__)


# ============== ENV HELPERS ==============

def _env_int(name: str, default: Optional[int] = None) -> Optional[int]:
    v = os.getenv(name, "").strip()
    try:
        if v and (v.lstrip("-").isdigit()):
            return int(v)
    except Exception:
        pass
    return default

def _env_bool(name: str, default: bool = False) -> bool:
    v = (os.getenv(name, "") or "").strip().lower()
    if v in ("1", "true", "yes", "on", "y"): return True
    if v in ("0", "false", "no", "off", "n"): return False
    return bool(default)

def _env_csv(name: str) -> List[str]:
    v = os.getenv(name, "") or ""
    if not v.strip(): return []
    return [p.strip().lower() for p in v.split(",") if p.strip()]

def _env_str(name: str, default: str = "") -> str:
    v = os.getenv(name, None)
    return default if v is None else str(v)


# ============== CONFIG (ENV) ==============
# Chat IDs âm cho group & channel VIP
VIP_GROUP_ID: Optional[int]   = _env_int("JOIN_GATE_VIP_GROUP_ID", None)
VIP_CHANNEL_ID: Optional[int] = _env_int("JOIN_GATE_VIP_CHANNEL_ID", None)

# Nơi nhận cảnh báo (group quản trị)
ADMIN_ALERT_CHAT_ID: Optional[int] = _env_int("JOIN_GATE_ADMIN_ALERT_CHAT_ID", None)

# Admin IDs (được dùng lệnh /approve_*, /decline_*, /vip_link_*, /grant, /revoke, /subinfo, /subs_dump, /subs_reload)
ADMIN_IDS: Set[int] = set()
for s in _env_csv("JOIN_GATE_ADMIN_IDS"):
    if s.lstrip("-").isdigit():
        ADMIN_IDS.add(int(s))

# Anti-spam cho GROUP
ANTISPAM_ON: bool = _env_bool("JOIN_GATE_ANTISPAM_ON", True)
ALLOW_LINKS: bool = _env_bool("JOIN_GATE_ALLOW_LINKS", False)  # nếu False: xoá mọi link không nằm whitelist
ALLOWED_LINK_DOMAINS: Set[str] = set(_env_csv("JOIN_GATE_ALLOWED_LINK_DOMAINS"))  # ví dụ: "t.me/yourvip,binance.com"
LINK_SPAM_MAX: int = int(_env_int("JOIN_GATE_LINK_SPAM_MAX", 2) or 2)  # quá số lần trong window thì block
LINK_SPAM_WINDOW_S: int = int(_env_int("JOIN_GATE_LINK_SPAM_WINDOW_S", 300) or 300)  # 5 phút
AUTOBAN_ON: bool = _env_bool("JOIN_GATE_AUTOBAN_ON", True)
AUTO_DELETE_LINKS: bool = _env_bool("JOIN_GATE_AUTODELETE_LINKS", True)

# DM hướng dẫn khi join-request
DM_TEXT: str = _env_str(
    "JOIN_GATE_DM_TEXT",
    "Chào bạn!\n\n"
    "Để vào VIP, vui lòng **trả lời tin nhắn này** kèm:\n"
    "• Sàn & UID bạn đã follow (Binance/BingX)\n"
    "• Ảnh/screenshot xác nhận đã follow\n\n"
    "Sau khi bạn gửi, admin sẽ duyệt yêu cầu của bạn."
)

# Subscription (thu phí/giữ hạn)
SUBS_DB_PATH: str = _env_str("JOIN_GATE_SUBS_DB", "vip_members.json")
DEFAULT_SUB_DAYS: int = int(_env_int("JOIN_GATE_DEFAULT_SUBS_DAYS", 30) or 30)


# ============== RUNTIME STATE ==============
# Spam buffer: user_id -> list[timestamps] (số lần dính link trong window)
_LINK_HITS: Dict[int, List[float]] = {}

# Membership DB: user_id -> {"until": "2025-10-01T00:00:00Z", "notes": "...", "granted_by": 123456}
_MEMBERS: Dict[str, Dict[str, str]] = {}


# ============== UTILITIES ==============

LINK_RE = re.compile(r'(https?://[^\s]+|www\.[^\s]+|t\.me/[^\s]+)', re.IGNORECASE)

def _now_utc() -> datetime:
    return datetime.now(timezone.utc)

def _iso(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).replace(microsecond=0).isoformat()

def _load_members():
    global _MEMBERS
    try:
        with open(SUBS_DB_PATH, "r", encoding="utf-8") as f:
            _MEMBERS = json.load(f)
            if not isinstance(_MEMBERS, dict):
                _MEMBERS = {}
    except Exception:
        _MEMBERS = {}

def _save_members():
    try:
        with open(SUBS_DB_PATH, "w", encoding="utf-8") as f:
            json.dump(_MEMBERS, f, ensure_ascii=False, indent=2)
    except Exception as e:
        log.warning("save members failed: %s", e)

def _is_admin(uid: int) -> bool:
    return True if not ADMIN_IDS else (uid in ADMIN_IDS)

def _in_vip_scope(chat_id: int) -> bool:
    return (VIP_GROUP_ID is not None and chat_id == VIP_GROUP_ID) or (VIP_CHANNEL_ID is not None and chat_id == VIP_CHANNEL_ID)

def _domain_in_whitelist(url: str) -> bool:
    if not ALLOWED_LINK_DOMAINS:
        return False
    u = url.lower()
    # Cho phép nếu bất kỳ domain trong whitelist là substring
    return any(dom in u for dom in ALLOWED_LINK_DOMAINS)

async def _alert_admin(context: ContextTypes.DEFAULT_TYPE, text: str):
    if ADMIN_ALERT_CHAT_ID:
        try:
            await context.bot.send_message(ADMIN_ALERT_CHAT_ID, text)
        except Exception:
            pass


# ============== JOIN REQUEST HANDLERS ==============

async def on_join_request(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Xử lý join-request cho cả GROUP & CHANNEL đã cấu hình."""
    req = update.chat_join_request  # type: ignore[attr-defined]
    if not req:
        return

    chat_id = req.chat.id
    if not _in_vip_scope(chat_id):
        return

    user = req.from_user
    uid = user.id
    uname = f"@{user.username}" if user and user.username else (user.full_name if user else str(uid))

    # DM hướng dẫn (nếu user chưa chat trước với bot có thể fail)
    try:
        await context.bot.send_message(chat_id=uid, text=DM_TEXT, parse_mode="Markdown")
    except Exception:
        pass

    # Báo admin
    chat_kind = "GROUP" if (VIP_GROUP_ID is not None and chat_id == VIP_GROUP_ID) else "CHANNEL"
    await _alert_admin(context,
        f"🔔 Join request vào VIP {chat_kind}\n"
        f"• User: {uname} (id={uid})\n"
        f"• Chat: {req.chat.title} (id={chat_id})\n\n"
        f"Admin dùng:\n"
        f"/approve_{chat_kind.lower()} {uid}\n"
        f"/decline_{chat_kind.lower()} {uid}"
    )


# ============== ADMIN COMMANDS — APPROVE / DECLINE ==============

async def _approve_common(update: Update, context: ContextTypes.DEFAULT_TYPE, target_chat_id: Optional[int], label: str):
    if not _check_admin(update): return
    if target_chat_id is None:
        await _safe_reply(update, f"{label}: chưa cấu hình chat id.")
        return
    if not context.args:
        await _safe_reply(update, f"Dùng: /approve_{label.lower()} <user_id>")
        return
    try:
        target = int(context.args[0])
        await context.bot.approve_chat_join_request(chat_id=target_chat_id, user_id=target)
        await _safe_reply(update, f"✅ Đã approve user {target} vào {label}.")
        try:
            await context.bot.send_message(target, f"✅ Yêu cầu join vào {label} đã được duyệt. Chào mừng!")
        except Exception:
            pass
    except Exception as e:
        await _safe_reply(update, f"❌ Approve lỗi: {e}")

async def _decline_common(update: Update, context: ContextTypes.DEFAULT_TYPE, target_chat_id: Optional[int], label: str):
    if not _check_admin(update): return
    if target_chat_id is None:
        await _safe_reply(update, f"{label}: chưa cấu hình chat id.")
        return
    if not context.args:
        await _safe_reply(update, f"Dùng: /decline_{label.lower()} <user_id>")
        return
    try:
        target = int(context.args[0])
        await context.bot.decline_chat_join_request(chat_id=target_chat_id, user_id=target)
        await _safe_reply(update, f"🚫 Đã từ chối user {target} vào {label}.")
        try:
            await context.bot.send_message(target, f"❌ Yêu cầu join vào {label} đã bị từ chối.")
        except Exception:
            pass
    except Exception as e:
        await _safe_reply(update, f"❌ Decline lỗi: {e}")

async def approve_group_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await _approve_common(update, context, VIP_GROUP_ID, "GROUP")

async def decline_group_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await _decline_common(update, context, VIP_GROUP_ID, "GROUP")

async def approve_channel_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await _approve_common(update, context, VIP_CHANNEL_ID, "CHANNEL")

async def decline_channel_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await _decline_common(update, context, VIP_CHANNEL_ID, "CHANNEL")


# ============== ADMIN COMMANDS — INVITE LINKS ==============

async def _new_link_common(update: Update, context: ContextTypes.DEFAULT_TYPE, target_chat_id: Optional[int], label: str):
    if not _check_admin(update): return
    if target_chat_id is None:
        await _safe_reply(update, f"{label}: chưa cấu hình chat id.")
        return
    try:
        link = await context.bot.create_chat_invite_link(
            chat_id=target_chat_id,
            creates_join_request=True,
            name=f"VIP {label} Join Request Link",
        )
        await _safe_reply(update, f"🔗 Link join-request {label} mới:\n{link.invite_link}")
    except Exception as e:
        await _safe_reply(update, f"❌ Tạo link lỗi: {e}")

async def vip_link_group_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await _new_link_common(update, context, VIP_GROUP_ID, "GROUP")

async def vip_link_channel_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await _new_link_common(update, context, VIP_CHANNEL_ID, "CHANNEL")


# ============== ADMIN COMMANDS — SUBS (THU PHÍ/HẠN DÙNG) ==============

def _parse_days(arg: Optional[str]) -> int:
    if not arg: return DEFAULT_SUB_DAYS
    try:
        d = int(arg)
        return d if d > 0 else DEFAULT_SUB_DAYS
    except Exception:
        return DEFAULT_SUB_DAYS

async def grant_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    /grant <user_id> [days]
    - Gán hạn dùng (days) vào DB; bot sẽ tự kick khi hết hạn.
    - Không tự approve join-request (vì join-request phải theo quy trình).
    """
    if not _check_admin(update): return
    if not context.args:
        await _safe_reply(update, "Dùng: /grant <user_id> [days]")
        return
    try:
        target = int(context.args[0])
        days = _parse_days(context.args[1] if len(context.args) >= 2 else None)
        until = _now_utc() + timedelta(days=days)
        _MEMBERS[str(target)] = {
            "until": _iso(until),
            "notes": f"granted {days}d",
            "granted_by": str(update.effective_user.id if update.effective_user else 0)
        }
        _save_members()
        await _safe_reply(update, f"✅ Đã grant user {target} {days} ngày (tới {until.date().isoformat()}).")
        try:
            await context.bot.send_message(target, f"🎟️ Bạn đã được cấp quyền VIP {days} ngày (tới {until.date().isoformat()}).")
        except Exception:
            pass
    except Exception as e:
        await _safe_reply(update, f"❌ Grant lỗi: {e}")

async def revoke_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    /revoke <user_id>
    - Xoá khỏi DB và kick khỏi group/channel (nếu đang trong đó).
    """
    if not _check_admin(update): return
    if not context.args:
        await _safe_reply(update, "Dùng: /revoke <user_id>")
        return
    try:
        target = int(context.args[0])
        _MEMBERS.pop(str(target), None)
        _save_members()
        # Kick nếu có trong group/channel
        await _kick_everywhere(context, target)
        await _safe_reply(update, f"🚫 Đã revoke user {target}.")
    except Exception as e:
        await _safe_reply(update, f"❌ Revoke lỗi: {e}")

async def subinfo_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    /subinfo <user_id>
    """
    if not _check_admin(update): return
    if not context.args:
        await _safe_reply(update, "Dùng: /subinfo <user_id>")
        return
    target = str(context.args[0])
    info = _MEMBERS.get(target)
    if not info:
        await _safe_reply(update, f"ℹ️ User {target} chưa có trong DB.")
        return
    await _safe_reply(update, f"User {target}: {json.dumps(info, ensure_ascii=False)}")

async def subs_dump_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Xuất DB ra text (cho admin)."""
    if not _check_admin(update): return
    await _safe_reply(update, json.dumps(_MEMBERS, ensure_ascii=False, indent=2))

async def subs_reload_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Reload DB từ file (khi sửa file thủ công)."""
    if not _check_admin(update): return
    _load_members()
    await _safe_reply(update, "🔄 Đã reload DB từ file.")

async def _kick_everywhere(context: ContextTypes.DEFAULT_TYPE, user_id: int):
    """Kick user khỏi group/channel nếu đang ở trong (ban rồi unban để out)."""
    for cid in [VIP_GROUP_ID, VIP_CHANNEL_ID]:
        if cid is None: continue
        try:
            await context.bot.ban_chat_member(cid, user_id)
            await context.bot.unban_chat_member(cid, user_id)  # unban để vẫn có thể join lại sau
        except Exception:
            pass

async def _subs_cleanup_job(context: ContextTypes.DEFAULT_TYPE):
    """Job chạy định kỳ: kick những user hết hạn."""
    now = _now_utc()
    expired: List[int] = []
    for k, v in list(_MEMBERS.items()):
        try:
            until = datetime.fromisoformat(v.get("until")).astimezone(timezone.utc)
            if now >= until:
                expired.append(int(k))
        except Exception:
            continue

    for uid in expired:
        await _kick_everywhere(context, uid)
        _MEMBERS.pop(str(uid), None)
        await _alert_admin(context, f"⏰ Hết hạn và đã kick user {uid}.")
    if expired:
        _save_members()


# ============== GROUP ANTI-SPAM (DELETE LINKS, AUTOBAN) ==============

def _extract_links(text: str) -> List[str]:
    if not text: return []
    return LINK_RE.findall(text)

def _record_link_hit(user_id: int) -> int:
    """Trả về số hit trong window sau khi ghi nhận một hit mới."""
    now = time.time()
    arr = _LINK_HITS.get(user_id, [])
    # giữ lại hit trong cửa sổ
    arr = [t for t in arr if now - t <= LINK_SPAM_WINDOW_S]
    arr.append(now)
    _LINK_HITS[user_id] = arr
    return len(arr)

async def _group_text_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Giữ group sạch: xoá link spam, block nếu vượt ngưỡng."""
    if not ANTISPAM_ON: return
    msg = update.effective_message
    chat = update.effective_chat
    user = update.effective_user
    if not msg or not chat or not user: return
    if VIP_GROUP_ID is None or chat.id != VIP_GROUP_ID: return
    if user.id in ADMIN_IDS: return  # bỏ qua admin

    text = msg.text or msg.caption or ""
    links = _extract_links(text)
    if not links:
        return

    # Nếu cho phép link, nhưng chỉ whitelist domain, thì loại những link hợp lệ
    bad_links = []
    for u in links:
        if ALLOW_LINKS and _domain_in_whitelist(u):
            continue
        bad_links.append(u)

    if not bad_links:
        return

    # Xoá tin nhắn chứa link không hợp lệ
    if AUTO_DELETE_LINKS:
        try:
            await msg.delete()
        except Exception:
            pass

    hits = _record_link_hit(user.id)
    warn_txt = (
        f"⚠️ @{user.username or user.id}: Link không được phép trong nhóm này."
        f" ({hits}/{LINK_SPAM_MAX} trong {LINK_SPAM_WINDOW_S//60} phút)"
    )
    await _alert_admin(context, f"[ANTI-SPAM] {warn_txt}\nNội dung: {text[:200]}")

    # Autoban khi vượt ngưỡng
    if AUTOBAN_ON and hits >= LINK_SPAM_MAX:
        try:
            await context.bot.ban_chat_member(chat.id, user.id)
            await _alert_admin(context, f"🚫 Đã block user {user.id} vì spam link.")
        except Exception as e:
            await _alert_admin(context, f"❌ Block user {user.id} lỗi: {e}")


# ============== COMMON HELPERS ==============

def _check_admin(update: Update) -> bool:
    uid = update.effective_user.id if update.effective_user else 0
    if not _is_admin(uid):
        # im lặng nếu không phải admin
        return False
    return True

async def _safe_reply(update: Update, text: str):
    try:
        await update.message.reply_text(text)
    except Exception:
        try:
            await update.effective_message.reply_text(text)
        except Exception:
            pass


# ============== REGISTRATION ==============

def register_join_gate(app: Application) -> None:
    """
    Gọi sau khi build Application ở bot chính.
    - Thêm handler join-request cho cả group & channel VIP.
    - Thêm lệnh admin: approve/decline, tạo link, quản lý hạn dùng.
    - Bật anti-spam cho group (delete link + autoban).
    - Đăng ký job cleanup hết hạn mỗi ngày 03:00 UTC (có thể chỉnh).
    """
    # Load subs DB
    _load_members()

    # Join request
    app.add_handler(ChatJoinRequestHandler(on_join_request))

    # Approve/Decline
    app.add_handler(CommandHandler("approve_group", approve_group_cmd))
    app.add_handler(CommandHandler("decline_group", decline_group_cmd))
    app.add_handler(CommandHandler("approve_channel", approve_channel_cmd))
    app.add_handler(CommandHandler("decline_channel", decline_channel_cmd))

    # Invite links
    app.add_handler(CommandHandler("vip_link_group", vip_link_group_cmd))
    app.add_handler(CommandHandler("vip_link_channel", vip_link_channel_cmd))

    # Subs (thu phí/hạn dùng)
    app.add_handler(CommandHandler("grant", grant_cmd))
    app.add_handler(CommandHandler("revoke", revoke_cmd))
    app.add_handler(CommandHandler("subinfo", subinfo_cmd))
    app.add_handler(CommandHandler("subs_dump", subs_dump_cmd))
    app.add_handler(CommandHandler("subs_reload", subs_reload_cmd))

    # Anti-spam cho group
    app.add_handler(MessageHandler(filters.TEXT | filters.Caption(True), _group_text_handler))

    # Job cleanup hết hạn — 1 lần/ngày
    if hasattr(app, "job_queue") and app.job_queue:
        # chạy lúc 03:00 UTC mỗi ngày
        target_time = datetime.time(datetime.strptime("03:00", "%H:%M"))
        try:
            app.job_queue.run_daily(_subs_cleanup_job, time=target_time, name="join_gate_cleanup")
        except Exception:
            # fallback: mỗi 6 giờ
            app.job_queue.run_repeating(_subs_cleanup_job, interval=6*3600, name="join_gate_cleanup_6h")
