# ----------------------- core/approval_flow.py -----------------------
from __future__ import annotations

import secrets
from dataclasses import dataclass, asdict
from datetime import datetime
from typing import Optional, Literal

from utils.storage import Storage  # giữ nguyên Storage của dự án

# =============================================================================
# V2: GLOBAL MANUAL-APPROVAL RECORDS (keyed by short pid like "ea8860")
# =============================================================================

_KEY_PREFIX = "pending:"

def _ps_key(pid: str) -> str:
    return f"{_KEY_PREFIX}{pid}"

@dataclass
class ManualPendingRecord:
    pid: str
    created_at: str
    symbol: str
    suggested_side: Literal["LONG", "SHORT"]
    signal_frames: dict              # H4/M30/M5 text + raw calc (optional)
    boardcard_ctx: dict              # formatter-ready blocks (optional)
    qty_cfg: dict
    risk_cfg: dict
    accounts_cfg: dict
    gates: dict                      # tide/late/m5 verdicts (optional)
    origin: Literal["MANUAL"] = "MANUAL"
    status: Literal["PENDING", "APPROVED", "REJECTED"] = "PENDING"

def create_pending_v2(storage: Storage, payload: dict) -> ManualPendingRecord:
    """
    Tạo bản ghi pending toàn cục (V2) truy cập bằng pid.
    payload bắt buộc: symbol, suggested_side
    payload tùy chọn: signal_frames, boardcard_ctx, qty_cfg, risk_cfg, accounts_cfg, gates, pid
    """
    pid = payload.get("pid") or secrets.token_hex(3)  # ví dụ: 'ea8860'
    rec = ManualPendingRecord(
        pid=pid,
        created_at=datetime.utcnow().isoformat(),
        symbol=payload["symbol"],
        suggested_side=str(payload["suggested_side"]).upper(),
        signal_frames=payload.get("signal_frames", {}),
        boardcard_ctx=payload.get("boardcard_ctx", {}),
        qty_cfg=payload.get("qty_cfg", {}),
        risk_cfg=payload.get("risk_cfg", {}),
        accounts_cfg=payload.get("accounts_cfg", {}),
        gates=payload.get("gates", {}),
    )
    storage.set(_ps_key(pid), asdict(rec))
    return rec

def get_pending(storage: Storage, pid: str) -> Optional[ManualPendingRecord]:
    raw = storage.get(_ps_key(pid))
    if not raw:
        return None
    return ManualPendingRecord(**raw)

def mark_done(storage: Storage, pid: str, status: Literal["APPROVED", "REJECTED"]) -> bool:
    raw = storage.get(_ps_key(pid))
    if not raw:
        return False
    raw["status"] = status
    storage.set(_ps_key(pid), raw)
    return True
