# admin_bot.py
# -*- coding: utf-8 -*-
"""
Admin guard cho tg/bot.py

- Lấy danh sách admin từ ENV: ADMIN_Bot_Allinone
- Hàm enforce_admin_for_all_commands(app, allowlist={...})
  -> Bọc tất cả CommandHandler thành admin-only
"""

import os
from functools import wraps
from typing import Callable, Awaitable, Any, Optional

try:
    from telegram.ext import CommandHandler
except Exception:
    CommandHandler = None  # fallback nếu phân tích tĩnh

def _parse_ids(raw: str) -> list[int]:
    ids: list[int] = []
    for p in raw.split(","):
        p = p.strip()
        if not p:
            continue
        try:
            ids.append(int(p))
        except ValueError:
            pass
    return ids

# Lấy admin IDs từ ENV
ADMIN_IDS: list[int] = _parse_ids(os.getenv("ADMIN_Bot_Allinone", ""))

def is_admin(uid: Optional[int]) -> bool:
    return bool(uid and uid in ADMIN_IDS)

def _deny_message() -> str:
    return "⛔ Bạn không có quyền dùng lệnh này."

def admin_only(func: Callable[..., Awaitable[Any]]):
    """Decorator dùng thủ công cho từng command."""
    @wraps(func)
    async def wrapper(update, context, *args, **kwargs):
        uid = getattr(update.effective_user, "id", None)
        if not is_admin(uid):
            try:
                msg = _deny_message()
                if getattr(update, "message", None):
                    await update.message.reply_text(msg)
                elif getattr(update, "edited_message", None):
                    await update.edited_message.reply_text(msg)
                elif getattr(update, "callback_query", None) and update.callback_query.message:
                    await update.callback_query.message.reply_text(msg)
            finally:
                return
        return await func(update, context, *args, **kwargs)
    return wrapper

def _wrap_callback(cb: Callable[..., Awaitable[Any]]) -> Callable[..., Awaitable[Any]]:
    if getattr(cb, "_admin_wrapped", False):
        return cb
    @wraps(cb)
    async def wrapped(update, context, *args, **kwargs):
        uid = getattr(update.effective_user, "id", None)
        if not is_admin(uid):
            try:
                msg = _deny_message()
                if getattr(update, "message", None):
                    await update.message.reply_text(msg)
                elif getattr(update, "edited_message", None):
                    await update.edited_message.reply_text(msg)
                elif getattr(update, "callback_query", None) and update.callback_query.message:
                    await update.callback_query.message.reply_text(msg)
            finally:
                return
        return await cb(update, context, *args, **kwargs)
    setattr(wrapped, "_admin_wrapped", True)
    return wrapped

def enforce_admin_for_all_commands(app, allowlist: set[str] | None = None):
    """
    Sau khi đã add hết CommandHandler trong tg/bot.py:
        enforce_admin_for_all_commands(app, {"start","help"})
    -> Chỉ cho phép admin dùng lệnh, trừ allowlist.
    """
    allowlist = allowlist or set()
    handlers_map = getattr(app, "handlers", None)
    if not handlers_map:
        return
    for _, handlers in handlers_map.items():
        for h in handlers:
            if CommandHandler and isinstance(h, CommandHandler):
                cmd_names = set(getattr(h, "commands", []) or [])
                if any((c not in allowlist) for c in cmd_names):
                    if getattr(h, "callback", None):
                        h.callback = _wrap_callback(h.callback)
