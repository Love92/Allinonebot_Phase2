# core/trade_executor.py
# ======================================================================
# Exchange client & execution helpers (async, ccxt)
# - Hỗ trợ: binanceusdm / bingx / okx
# - Market entry + kèm SL/TP reduceOnly
# - Hàm public chính: ExchangeClient.market_with_sl_tp(...)
# - Kèm calc_qty, auto_sl_by_leverage, close helpers (đa tài khoản)
# ======================================================================

from __future__ import annotations

import os
import math
import json
from dataclasses import dataclass
from typing import Any, Dict, Optional, Tuple, Union

# ccxt async
try:
    import ccxt.async_support as ccxt  # type: ignore
except Exception:  # pragma: no cover
    ccxt = None  # sẽ raise khi khởi tạo nếu thiếu


# ======================================================================
# Kết quả chung cho các thao tác đặt lệnh
# ======================================================================
@dataclass
class OrderResult:
    ok: bool
    message: Union[str, Dict[str, Any]]

    @property
    def error(self) -> Optional[str]:
        return None if self.ok else (self.message if isinstance(self.message, str) else json.dumps(self.message))


# ======================================================================
# Helper cơ bản
# ======================================================================
def calc_qty(
    balance_usdt: float,
    risk_percent: float,
    leverage: int,
    entry_price: float,
    *,
    min_qty: float = 0.0,
) -> float:
    """
    Tính khối lượng theo % vốn * leverage, chia cho entry_price.
    Có clamp min_qty nếu sàn yêu cầu.
    """
    balance_usdt = max(float(balance_usdt or 0.0), 0.0)
    risk_percent = max(float(risk_percent or 0.0), 0.0)
    leverage = max(int(leverage or 1), 1)
    entry_price = max(float(entry_price or 0.0), 0.0)

    if entry_price <= 0.0 or balance_usdt <= 0.0 or risk_percent <= 0.0:
        return 0.0

    notional = balance_usdt * (risk_percent / 100.0) * leverage
    qty = notional / entry_price
    if min_qty > 0.0 and qty < min_qty:
        qty = min_qty
    return float(qty)


def auto_sl_by_leverage(
    entry: float,
    side: Union[str, bool],
    lev: int,
    *,
    k: float = 0.75,
) -> Tuple[float, float]:
    """
    Tính SL/TP gợi ý dựa vào leverage.
    Ý tưởng: khoảng chịu lỗ ~ (k / lev) theo tỷ lệ %, TP đối xứng 1:1.
    - LONG: SL = entry * (1 - k/lev/100), TP = entry * (1 + k/lev/100)
    - SHORT: SL = entry * (1 + k/lev/100), TP = entry * (1 - k/lev/100)
    """
    entry = float(entry or 0.0)
    lev = max(int(lev or 1), 1)

    # chuẩn hoá side
    if isinstance(side, bool):
        s = "LONG" if side else "SHORT"
    else:
        s = str(side or "").upper()
        if s in ("BUY",):
            s = "LONG"
        if s in ("SELL",):
            s = "SHORT"

    pct = (k / lev) / 100.0  # về fraction
    if entry <= 0.0 or pct <= 0.0:
        return entry, entry

    if s == "LONG":
        sl = entry * (1.0 - pct)
        tp = entry * (1.0 + pct)
    else:
        sl = entry * (1.0 + pct)
        tp = entry * (1.0 - pct)
    return float(sl), float(tp)


# ======================================================================
# Lớp chính: ExchangeClient
# ======================================================================
class ExchangeClient:
    """
    Client kết nối sàn qua ccxt.async_support
    - exchange_id: binanceusdm / bingx / okx (đọc từ ENV nếu không truyền)
    - api_key/api_secret/testnet: đọc từ ENV nếu không truyền
    """

    def __init__(
        self,
        exchange_id: Optional[str] = None,
        api_key: Optional[str] = None,
        api_secret: Optional[str] = None,
        testnet: Optional[bool] = None,
    ):
        if ccxt is None:  # pragma: no cover
            raise RuntimeError("ccxt.async_support chưa được cài (pip install ccxt)")

        self.exchange_id = (exchange_id or os.getenv("EXCHANGE") or "binanceusdm").strip().lower()
        self.api_key = api_key or os.getenv("API_KEY") or ""
        self.api_secret = api_secret or os.getenv("API_SECRET") or ""
        self.testnet = bool(os.getenv("TESTNET", "false").lower() in ("1", "true", "yes", "on")) if testnet is None else bool(testnet)

        self.client = self._build_ccxt_client()

    # ------------------------- Client builder -------------------------
    def _build_ccxt_client(self):
        eid = self.exchange_id

        if eid in ("binanceusdm", "binance-um", "binanceusdmfutures", "binanceusd"):
            ex = ccxt.binanceusdm()
            if self.testnet:
                ex.set_sandbox_mode(True)
        elif eid in ("bingx",):
            ex = ccxt.bingx()
        elif eid in ("okx", "okex"):
            ex = ccxt.okx()
        else:  # fallback
            ex = getattr(ccxt, eid)()

        if self.api_key and self.api_secret:
            ex.apiKey = self.api_key
            ex.secret = self.api_secret

        # Một số sàn cần options
        if hasattr(ex, "options") and isinstance(ex.options, dict):
            ex.options.setdefault("defaultType", "swap")  # ưu tiên futures/swap
        return ex

    # ------------------------- Utils -------------------------
    async def _io(self, fn, *args, **kwargs):
        """Bọc gọi ccxt kèm try/except ngắn gọn"""
        try:
            return await fn(*args, **kwargs)
        except Exception as e:
            raise e

    def normalize_symbol(self, symbol: str) -> str:
        """
        Chuẩn hoá cặp theo sàn:
        - binanceusdm: 'BTC/USDT'
        - bingx/okx: 'BTC/USDT:USDT' (nếu không có suffix thì thêm)
        """
        s = str(symbol).upper().replace(" ", "")
        if self.exchange_id in ("bingx", "okx", "okex"):
            if ":" not in s:
                s = s + ":USDT"
        return s

    async def ticker_price(self, symbol: str) -> float:
        """Giá last/close hiện tại"""
        sym = self.normalize_symbol(symbol)
        t = await self._io(self.client.fetch_ticker, sym)
        return float(t.get("last") or t.get("close") or 0.0)

    async def get_balance_quote(self) -> float:
        """Số dư USDT (quote)"""
        bal = await self._io(self.client.fetch_balance)
        usdt = bal.get("USDT") or bal.get("USDC") or {}
        free = float(usdt.get("free") or 0.0)
        total = float(usdt.get("total") or 0.0)
        return free if free > 0 else total

    async def _fit_qty(self, symbol: str, qty: float) -> float:
        """
        Fit khối lượng theo bước lot/precision của sàn.
        """
        sym = self.normalize_symbol(symbol)
        mkt = await self._io(self.client.load_markets)
        info = mkt.get(sym)
        if not info:
            return float(qty)

        # ưu tiên precision.amount
        amount_prec = info.get("precision", {}).get("amount")
        if amount_prec is not None:
            q = float(qty)
            q = float(f"{q:.{int(amount_prec)}f}")
            return q

        # thử limits.amount.step
        limits = info.get("limits") or {}
        step = (limits.get("amount") or {}).get("step")
        if step:
            q = math.floor(float(qty) / float(step)) * float(step)
            return float(q)

        return float(qty)

    # ==================================================================
    # 1) MARKET ENTRY + SL/TP (theo yêu cầu)
    # ==================================================================
    async def market_with_sl_tp(
        self,
        symbol: str,
        side_long: Union[str, bool],
        qty: float,
        sl: Optional[float],
        tp: Optional[float],
    ) -> OrderResult:
        """
        Vào lệnh MARKET rồi đặt SL/TP reduceOnly (nếu truyền).
        - side_long: True/'LONG'/'BUY' => long; False/'SHORT'/'SELL' => short
        - Trả OrderResult(ok, message)
        """
        # --- Chuẩn hoá side ---
        if isinstance(side_long, bool):
            is_long = side_long
        else:
            s = str(side_long or "").upper()
            if s in ("LONG", "BUY", "L", "B", "1", "TRUE", "T"):
                is_long = True
            elif s in ("SHORT", "SELL", "S", "0", "FALSE", "F"):
                is_long = False
            else:
                is_long = "LONG" in s or "BUY" in s

        side_txt = "buy" if is_long else "sell"
        sym = self.normalize_symbol(symbol)

        # --- Fit qty & tạo lệnh market ---
        try:
            q_fit = await self._fit_qty(sym, float(qty))
            if q_fit <= 0:
                return OrderResult(False, f"qty_not_valid:{qty}")

            entry = await self._io(self.client.create_order, sym, "market", side_txt, q_fit)
        except Exception as e:
            return OrderResult(False, f"entry_error:{e}")

        # --- Đặt SL/TP reduceOnly (nếu có) ---
        created = {"entry": entry}
        try:
            if self.exchange_id in ("okx", "okex"):
                # OKX: tạo lệnh reduceOnly kèm stopLossPrice/takeProfitPrice qua params
                params_sl = {"reduceOnly": True}
                params_tp = {"reduceOnly": True}
                if sl:
                    try:
                        o_sl = await self._io(
                            self.client.create_order, sym, "market",
                            ("sell" if is_long else "buy"), q_fit, None,
                            {**params_sl, "stopLossPrice": float(sl)}
                        )
                        created["sl"] = o_sl
                    except Exception as e2:
                        created["sl_error"] = str(e2)
                if tp:
                    try:
                        o_tp = await self._io(
                            self.client.create_order, sym, "market",
                            ("sell" if is_long else "buy"), q_fit, None,
                            {**params_tp, "takeProfitPrice": float(tp)}
                        )
                        created["tp"] = o_tp
                    except Exception as e3:
                        created["tp_error"] = str(e3)
            else:
                # BINANCE/BINGX: STOP_MARKET / TAKE_PROFIT_MARKET + reduceOnly
                if sl:
                    try:
                        o_sl = await self._io(
                            self.client.create_order,
                            sym,
                            "STOP_MARKET",
                            ("sell" if is_long else "buy"),
                            q_fit,
                            None,
                            {"reduceOnly": True, "stopPrice": float(sl)},
                        )
                        created["sl"] = o_sl
                    except Exception as e2:
                        created["sl_error"] = str(e2)
                if tp:
                    try:
                        o_tp = await self._io(
                            self.client.create_order,
                            sym,
                            "TAKE_PROFIT_MARKET",
                            ("sell" if is_long else "buy"),
                            q_fit,
                            None,
                            {"reduceOnly": True, "stopPrice": float(tp)},
                        )
                        created["tp"] = o_tp
                    except Exception as e3:
                        created["tp_error"] = str(e3)
        except Exception as e:
            created["post_error"] = str(e)

        return OrderResult(True, created)

    # ==================================================================
    # 2) MARKET ENTRY tối giản (nếu nơi khác cần)
    # ==================================================================
    async def open_market(
        self,
        symbol: str,
        side: Union[str, bool],
        qty: float,
        leverage: Optional[int] = None,
        stop_loss: Optional[float] = None,
    ) -> OrderResult:
        """
        Market entry + optional SL (reduceOnly). TP để logic khác xử lý.
        """
        # chuẩn hoá side (an toàn cho bool/float)
        if isinstance(side, (bool, int, float)):
            s = "LONG" if float(side) > 0 else "SHORT"
        else:
            s = str(side or "").upper()
        if s in ("LONG", "BUY"):
            is_long = True
        elif s in ("SHORT", "SELL"):
            is_long = False
        else:
            return OrderResult(False, f"Invalid side:{side}")

        # leverage (tuỳ sàn, không set cũng không sao)
        try:
            if leverage:
                if self.exchange_id.startswith("binance"):
                    # ccxt: set_leverage(leverage, symbol) (một số version cần params khác)
                    await self._io(self.client.set_leverage, int(leverage), self.normalize_symbol(symbol))
                # bingx/okx: có thể không cần/không hỗ trợ unified — bỏ qua
        except Exception:
            pass

        # gọi hàm chính
        return await self.market_with_sl_tp(symbol, is_long, qty, stop_loss, None)

    # ==================================================================
    # 3) Đóng vị thế / đóng phần trăm
    # ==================================================================
    async def close_position(self, symbol: str) -> OrderResult:
        """
        Đóng toàn bộ vị thế bằng lệnh market reduceOnly (tự suy ra side đóng).
        """
        sym = self.normalize_symbol(symbol)
        try:
            pos = await self._io(self.client.fetch_positions, [sym])
        except Exception as e:
            return OrderResult(False, f"fetch_positions_error:{e}")

        # tìm vị thế hiện tại
        side_to_close = None
        size = 0.0
        for p in pos or []:
            if str(p.get("symbol")).upper() == sym.upper():
                amt = float(p.get("contracts") or p.get("amount") or 0.0)
                if abs(amt) > 0:
                    size = abs(amt)
                    side_to_close = "sell" if amt > 0 else "buy"  # long>0 -> sell; short<0 -> buy
                    break

        if not side_to_close or size <= 0:
            return OrderResult(True, "no_position")

        try:
            o = await self._io(self.client.create_order, sym, "market", side_to_close, size, None, {"reduceOnly": True})
            return OrderResult(True, {"close": o})
        except Exception as e:
            return OrderResult(False, f"close_error:{e}")

    async def close_position_percent(self, symbol: str, percent: float) -> OrderResult:
        """
        Đóng một phần trăm vị thế (market reduceOnly).
        """
        sym = self.normalize_symbol(symbol)
        percent = max(min(float(percent or 0.0), 100.0), 0.0)
        if percent <= 0.0:
            return OrderResult(False, "percent<=0")

        try:
            pos = await self._io(self.client.fetch_positions, [sym])
        except Exception as e:
            return OrderResult(False, f"fetch_positions_error:{e}")

        size = 0.0
        side_to_close = None
        for p in pos or []:
            if str(p.get("symbol")).upper() == sym.upper():
                amt = float(p.get("contracts") or p.get("amount") or 0.0)
                if abs(amt) > 0:
                    size = abs(amt) * (percent / 100.0)
                    side_to_close = "sell" if amt > 0 else "buy"
                    break

        if not side_to_close or size <= 0:
            return OrderResult(True, "no_position")

        size_fit = await self._fit_qty(sym, size)
        if size_fit <= 0:
            return OrderResult(False, "size_fit<=0")

        try:
            o = await self._io(self.client.create_order, sym, "market", side_to_close, size_fit, None, {"reduceOnly": True})
            return OrderResult(True, {"close_part": o})
        except Exception as e:
            return OrderResult(False, f"close_part_error:{e}")

    # ==================================================================
    # 4) Huỷ toàn bộ lệnh chờ theo symbol
    # ==================================================================
    async def cancel_all_orders(self, symbol: str) -> OrderResult:
        sym = self.normalize_symbol(symbol)
        try:
            # ccxt có sẵn cho một số sàn
            if hasattr(self.client, "cancel_all_orders"):
                o = await self._io(self.client.cancel_all_orders, sym)
                return OrderResult(True, {"cancel_all": o})

            # fallback: duyệt open orders rồi cancel từng cái
            opens = await self._io(self.client.fetch_open_orders, sym)
            cancelled = []
            errors = []
            for od in opens or []:
                try:
                    r = await self._io(self.client.cancel_order, od.get("id"), sym)
                    cancelled.append(r)
                except Exception as ce:
                    errors.append(str(ce))
            ok = len(errors) == 0
            msg = {"cancelled": cancelled, "errors": errors}
            return OrderResult(ok, msg)
        except Exception as e:
            return OrderResult(False, f"cancel_all_error:{e}")


# ======================================================================
# Helper: đọc ACCOUNTS_JSON để lấy api/key theo tên account (đa tài khoản)
# ======================================================================
def _resolve_account_cfg(name: str | None):
    if not name:
        return None
    try:
        raw = os.getenv("ACCOUNTS_JSON", "")
        if not raw:
            return None
        arr = json.loads(raw)
        for acc in arr:
            if (acc.get("name") or "").strip().lower() == str(name).strip().lower():
                return {
                    "exchange": acc.get("exchange"),
                    "api_key": acc.get("api_key"),
                    "api_secret": acc.get("api_secret"),
                    "testnet": bool(acc.get("testnet", False)),
                    "pair": acc.get("pair"),
                }
    except Exception:
        return None
    return None


# ======================================================================
# Module-level helpers cho /close (đa tài khoản) — để tg/bot.py import trực tiếp
# ======================================================================
async def close_position_on_account(
    account_name: str,
    symbol: str,
    percent: float | None = None,
    cancel_pending: bool = True,
) -> OrderResult:
    """
    Đóng vị thế trên 1 account:
    - percent=None hoặc >=100  -> đóng full + (tuỳ chọn) huỷ lệnh chờ
    - percent=0..100           -> đóng phần trăm, KHÔNG huỷ lệnh chờ
    """
    cfg = _resolve_account_cfg(account_name)
    # Khởi tạo client theo account (nếu có), không có thì dùng ENV mặc định
    if cfg:
        ex = ExchangeClient(
            exchange_id=cfg.get("exchange"),
            api_key=cfg.get("api_key"),
            api_secret=cfg.get("api_secret"),
            testnet=cfg.get("testnet"),
        )
        # Nếu account có pair riêng, có thể override symbol (tuỳ ý):
        # if cfg.get("pair"): symbol = cfg["pair"]
    else:
        ex = ExchangeClient()

    try:
        if percent is None or float(percent) >= 99.999:
            res = await ex.close_position(symbol)
            if not res.ok:
                return res
            if cancel_pending:
                _ = await ex.cancel_all_orders(symbol)
            return res
        else:
            res = await ex.close_position_percent(symbol, float(percent))
            return res
    except Exception as e:
        return OrderResult(False, f"close_on_account_error:{e}")


async def close_position_on_all(
    symbol: str,
    percent: float | None = None,
    cancel_pending: bool = True,
) -> Dict[str, OrderResult]:
    """
    Đóng trên TẤT CẢ accounts trong ACCOUNTS_JSON.
    Trả về dict {account_name: OrderResult}
    """
    results: Dict[str, OrderResult] = {}
    raw = os.getenv("ACCOUNTS_JSON", "")
    try:
        arr = json.loads(raw) if raw else []
    except Exception:
        arr = []

    # Nếu không có ACCOUNTS_JSON => dùng ENV hiện tại như "default"
    if not arr:
        res = await close_position_on_account("default", symbol, percent, cancel_pending)
        results["default"] = res
        return results

    for acc in arr:
        name = acc.get("name") or "unknown"
        try:
            res = await close_position_on_account(name, symbol, percent, cancel_pending)
            results[name] = res
        except Exception as e:
            results[name] = OrderResult(False, f"close_on_all_error:{e}")

    return results

# ======================================================================
# Hết file
# ======================================================================
