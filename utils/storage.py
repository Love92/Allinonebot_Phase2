# ----------------------- utils/storage.py -----------------------
import os, json
from dataclasses import dataclass, asdict, field
from typing import Dict, Optional, Any, List
from datetime import datetime
import pytz
from config.settings import TIDE_WINDOW_HOURS  # thêm dòng này

VN_TZ = pytz.timezone("Asia/Ho_Chi_Minh")

STATE_FILE = "bot_state.json"

@dataclass
class UserSettings:
    pair: str = "BTC/USDT"
    risk_percent: float = 20.0
    leverage: int = 44
    mode: str = "manual"              # manual | auto
    tide_window_hours: float = TIDE_WINDOW_HOURS  # lấy mặc định từ .env
    max_orders_per_day: int = 8
    max_orders_per_tide_window: int = 2
    # NEW: bật/tắt report M5 theo lệnh Telegram
    m5_report_enabled: bool = False

@dataclass
class UserDay:
    date_str: str
    count: int = 0

@dataclass
class PendingSignal:
    id: str
    symbol: str
    side: str   # LONG/SHORT
    score: int
    entry_hint: Optional[float] = None
    sl: Optional[float] = None
    tp: Optional[float] = None
    created_at: str = ""

@dataclass
class UserState:
    settings: UserSettings
    today: UserDay
    tide_window_trades: Dict[str, int]   # key = tide_time iso, value = count
    pending: Optional[PendingSignal] = None
    history: List[Dict[str, Any]] = field(default_factory=list)

class Storage:
    """
    Lưu ý:
    - self.data là 1 dict "top-level":
        {
          "<uid>": { ...UserState... },
          "<các key tự do>": <giá trị bất kỳ>
        }
    - Các hàm get_user/put_user dùng namespace uid.
    - Các hàm get/set (mới thêm) dùng cho key-value top-level (cờ lock auto, cấu hình tạm, v.v.)
    """
    def __init__(self, path: str = STATE_FILE):
        self.path = path
        self.data: Dict[str, Any] = {}
        self._load()

    def _load(self):
        if os.path.exists(self.path):
            try:
                with open(self.path, 'r', encoding='utf-8') as f:
                    self.data = json.load(f)
            except Exception:
                self.data = {}
        else:
            self.data = {}

    def save(self):
        tmp = self.path + ".tmp"
        with open(tmp, 'w', encoding='utf-8') as f:
            json.dump(self.data, f, ensure_ascii=False, indent=2)
        os.replace(tmp, self.path)

    # Backward-compat: một số code gọi storage.persist()
    def persist(self):
        """Compat: ánh xạ sang save()."""
        try:
            return self.save()
        except Exception:
            # không để raise làm văng bot
            return None

    def _today_str(self):
        return datetime.now(VN_TZ).date().isoformat()

    # ===================== TOP-LEVEL KV API (MỚI) =====================
    def set(self, key: str, value: Any) -> None:
        """
        Đặt giá trị cho một key top-level (ví dụ: "auto_lock_2025-09-24": True).
        Dùng cho các cờ toàn cục, không gắn với uid cụ thể.
        """
        self.data[key] = value
        self.save()

    def get(self, key: str, default: Any = None) -> Any:
        """
        Lấy giá trị top-level theo key. Nếu không có, trả về default.
        """
        return self.data.get(key, default)

    # alias để code cũ/new đều chạy
    def set_value(self, key: str, value: Any) -> None:
        self.set(key, value)

    def get_value(self, key: str, default: Any = None) -> Any:
        return self.get(key, default)

    def delete(self, key: str) -> None:
        """
        Xoá một key top-level nếu tồn tại.
        """
        if key in self.data:
            del self.data[key]
            self.save()

    # ===================== USER NAMESPACE API =====================
    def get_user(self, uid: int) -> UserState:
        key = str(uid)
        if key not in self.data:
            self.data[key] = asdict(UserState(
                settings=UserSettings(),
                today=UserDay(date_str=self._today_str(), count=0),
                tide_window_trades={},
                pending=None,
                history=[]
            ))
            self.save()

        u = self.data[key]
        # reset counter if date changed
        if u["today"]["date_str"] != self._today_str():
            u["today"] = asdict(UserDay(date_str=self._today_str(), count=0))
            u["tide_window_trades"] = {}
            self.save()

        # Bảo toàn backward-compat: nếu bản cũ chưa có khóa m5_report_enabled
        if "m5_report_enabled" not in u["settings"]:
            u["settings"]["m5_report_enabled"] = False
            self.save()

        settings = UserSettings(**u["settings"])
        today = UserDay(**u["today"])
        pending = None
        if u.get("pending"):
            pending = PendingSignal(**u["pending"])
        history = u.get("history", [])
        tide_window_trades = u.get("tide_window_trades", {})
        return UserState(
            settings=settings,
            today=today,
            tide_window_trades=tide_window_trades,
            pending=pending,
            history=history
        )

    def put_user(self, uid: int, state: UserState):
        self.data[str(uid)] = asdict(state)
        self.save()
