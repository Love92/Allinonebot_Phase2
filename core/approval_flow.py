# ----------------------- core/approval_flow.py -----------------------
from __future__ import annotations
import uuid
from typing import Optional
from utils.storage import Storage, UserState, PendingSignal
from utils.time_utils import now_vn

def create_pending(storage: Storage, uid: int, symbol: str, side: str, score: int,
                   entry_hint: Optional[float], sl: Optional[float], tp: Optional[float]) -> PendingSignal:
    st = storage.get_user(uid)
    ps = PendingSignal(
        id=str(uuid.uuid4())[:8],
        symbol=symbol,
        side=side,
        score=score,
        entry_hint=entry_hint,
        sl=sl,
        tp=tp,
        created_at=now_vn().isoformat()
    )
    st.pending = ps
    storage.put_user(uid, st)
    return ps

def clear_pending(storage: Storage, uid: int):
    st = storage.get_user(uid)
    st.pending = None
    storage.put_user(uid, st)
