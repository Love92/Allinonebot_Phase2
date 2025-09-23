# core/trade_executor.py
# ======================================================================
# Exchange client & execution helpers (async, ccxt)
# - Hỗ trợ: binanceusdm / bingx / okx
# - Market entry + kèm SL/TP reduceOnly
# - Fit qty theo filters/limits (minQty/stepSize/minNotional/…)
# - Đặt lệnh retry "thu nhỏ dần" nếu đụng giới hạn (max position/notional)
# - Close/cancel luôn trả về dict (hết lỗi .get trên string)
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
        # Luôn dùng trong UI: if not ok -> hiển thị error gọn
        if self.ok:
            return None
        if isinstance(self.message, str):
            return self.message
        return self.message.get("error") or json.dumps(self.message)


# ======================================================================
# Helper cơ bản (giữ tương thích)
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
    Tính khối lượng theo % vốn * leverage / entry_price (thô).
    Dành cho luồng cũ; khuyến nghị dùng market_with_sl_tp + _fit_qty để an toàn.
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
    SL/TP gợi ý theo leverage:
    - LONG: SL = entry*(1 - k/lev/100) ; TP = entry*(1 + k/lev/100)
    - SHORT: SL = entry*(1 + k/lev/100); TP = entry*(1 - k/lev/100)
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

    pct = (k / lev) / 100.0
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
    - exchange_id: binanceusdm / bingx / okx (ENV hoặc truyền vào)
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

        if hasattr(ex, "options") and isinstance(ex.options, dict):
            ex.options.setdefault("defaultType", "swap")
        return ex

    # ------------------------- Utils -------------------------
    async def _io(self, fn, *args, **kwargs):
        """Bọc gọi ccxt (awaitable)."""
        return await fn(*args, **kwargs)

    def normalize_symbol(self, symbol: str) -> str:
        """
        Chuẩn hoá cặp theo sàn:
        - binanceusdm: 'BTC/USDT'
        - bingx/okx  : 'BTC/USDT:USDT' (nếu chưa có suffix thì thêm)
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

    # ------------------------- Qty fitting (nâng cấp theo oldversion) -------------------------
    @staticmethod
    def _floor_step(x: float, step: float) -> float:
        if step and step > 0:
            return math.floor(float(x) / float(step)) * float(step)
        return float(x)

    @staticmethod
    def _should_shrink_on_error(err: Exception) -> bool:
        s = str(err).lower()
        keys = [
            "max quantity",
            "maximum position",
            "max position value",
            "exceeds",
            "notional",
            "reduce your position",
            "beyond the limit",
        ]
        return any(k in s for k in keys)

    async def _fit_qty(self, symbol: str, qty: float, price: Optional[float] = None) -> Tuple[float, Dict[str, Any]]:
        """
        Fit qty theo markets.limits + raw filters (minQty/maxQty/stepSize, minNotional/maxNotional).
        Trả về (qty_fit, meta_limits)
        """
        sym = self.normalize_symbol(symbol)
        markets = await self._io(self.client.load_markets)
        mkt = markets.get(sym) or {}
        info = mkt.get("info", {}) if isinstance(mkt, dict) else {}
        limits = mkt.get("limits", {}) if isinstance(mkt, dict) else {}
        precision = mkt.get("precision", {}) if isinstance(mkt, dict) else {}

        min_qty = None
        max_qty = None
        step = precision.get("amount")

        amt = limits.get("amount") or {}
        if "min" in amt:
            min_qty = amt.get("min")
        if "max" in amt:
            max_qty = amt.get("max")

        min_cost = None
        max_cost = None
        cost = limits.get("cost") or {}
        if "min" in cost:
            min_cost = cost.get("min")
        if "max" in cost:
            max_cost = cost.get("max")

        # raw filters (đặc biệt BinanceUSDM/BingX)
        try:
            for f in info.get("filters", []):
                t = (f.get("filterType") or f.get("type") or "").upper()
                if t in ("LOT_SIZE", "MARKET_LOT_SIZE"):
                    try:
                        if f.get("minQty") is not None:
                            min_qty = float(f.get("minQty"))
                        if f.get("maxQty") is not None:
                            max_qty = float(f.get("maxQty"))
                        if f.get("stepSize") is not None:
                            step = float(f.get("stepSize"))
                    except Exception:
                        pass
                if t in ("MIN_NOTIONAL", "NOTIONAL", "MARKET_MIN_NOTIONAL"):
                    try:
                        if f.get("minNotional") is not None:
                            min_cost = float(f.get("minNotional"))
                        mx = f.get("maxNotional")
                        if mx is not None:
                            max_cost = float(mx)
                    except Exception:
                        pass
        except Exception:
            pass

        q = float(qty)
        px = float(price or 0.0)

        # Fit theo max_cost trước
        if max_cost and px > 0:
            q = min(q, float(max_cost) / px)

        # Fit theo max_qty
        if max_qty is not None:
            q = min(q, float(max_qty))

        # Rounding theo step/precision
        if step:
            q = self._floor_step(q, float(step))
        else:
            # fallback: precision.amount
            amount_prec = precision.get("amount")
            if isinstance(amount_prec, int):
                q = float(f"{q:.{amount_prec}f}")

        # Đảm bảo >= min
        if min_cost and px > 0:
            q = max(q, float(min_cost) / px)
        if min_qty:
            q = max(q, float(min_qty))

        lot_step = float(os.getenv("LOT_STEP_FALLBACK", "0.001"))
        if q <= 0:
            q = float(min_qty or lot_step)

        meta = {
            "min_qty": min_qty,
            "max_qty": max_qty,
            "step": step,
            "min_cost": min_cost,
            "max_cost": max_cost,
        }
        return (float(q), meta)

    async def _place_market_with_retries(
        self, sym: str, side: str, qty: float, *, max_retries: int = 3
    ):
        """
        Tạo lệnh market với chiến lược giảm size dần nếu bị lỗi giới hạn.
        """
        attempt = 0
        last_err = None
        q = float(qty)
        lot_step = float(os.getenv("LOT_STEP_FALLBACK", "0.001"))

        while attempt <= max_retries:
            try:
                return await self._io(self.client.create_order, sym, "market", side, q)
            except Exception as e:
                last_err = e
                if not self._should_shrink_on_error(e):
                    break
                # Giảm size và thử lại (floor theo step fallback)
                q = max(self._floor_step(q * 0.7, lot_step), lot_step)
                attempt += 1

        raise last_err if last_err else Exception("create_order failed")

    # ==================================================================
    # 1) MARKET ENTRY + SL/TP
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

        # --- Fit qty & tạo lệnh market (kèm retry shrink nếu đụng giới hạn) ---
        try:
            price = await self.ticker_price(sym)
            q_fit, meta = await self._fit_qty(sym, float(qty), float(price or 0))
            if q_fit <= 0:
                return OrderResult(False, {"error": f"qty_fit=0 (limits={meta})"})

            entry = await self._place_market_with_retries(sym, side_txt, q_fit)
        except Exception as e:
            return OrderResult(False, {"error": f"entry_error:{e}"})

        # --- Đặt SL/TP reduceOnly (nếu có) ---
        created: Dict[str, Any] = {"entry": entry}
        try:
            if self.exchange_id in ("okx", "okex"):
                # OKX: dùng params stopLossPrice/takeProfitPrice
                if sl:
                    try:
                        o_sl = await self._io(
                            self.client.create_order, sym, "market",
                            ("sell" if is_long else "buy"), q_fit, None,
                            {"reduceOnly": True, "stopLossPrice": float(sl)}
                        )
                        created["sl"] = o_sl
                    except Exception as e2:
                        created["sl_error"] = str(e2)
                if tp:
                    try:
                        o_tp = await self._io(
                            self.client.create_order, sym, "market",
                            ("sell" if is_long else "buy"), q_fit, None,
                            {"reduceOnly": True, "takeProfitPrice": float(tp)}
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
    # 2) MARKET ENTRY tối giản
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
            return OrderResult(False, {"error": f"invalid_side:{side}"})

        # leverage (tuỳ sàn, không set cũng không sao)
        try:
            if leverage:
                if self.exchange_id.startswith("binance"):
                    await self._io(self.client.set_leverage, int(leverage), self.normalize_symbol(symbol))
        except Exception:
            pass

        return await self.market_with_sl_tp(symbol, is_long, qty, stop_loss, None)

    # ==================================================================
    # 3) Đóng vị thế / đóng phần trăm  — luôn trả dict (hết lỗi .get)
    # ==================================================================
    async def close_position(self, symbol: str) -> OrderResult:
        """
        Đóng toàn bộ vị thế bằng lệnh market reduceOnly (tự suy ra side đóng).
        """
        sym = self.normalize_symbol(symbol)
        try:
            pos = await self._io(self.client.fetch_positions, [sym])
        except Exception as e:
            return OrderResult(False, {"error": f"fetch_positions_error:{e}"})

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
            return OrderResult(True, {"note": "no_position"})

        try:
            o = await self._io(self.client.create_order, sym, "market", side_to_close, size, None, {"reduceOnly": True})
            # (tuỳ ý) Huỷ TP/SL chờ
            # opens = await self._io(self.client.fetch_open_orders, sym)
            return OrderResult(True, {"close": o})
        except Exception as e:
            return OrderResult(False, {"error": f"close_error:{e}"})

    async def close_position_percent(self, symbol: str, percent: float) -> OrderResult:
        """
        Đóng một phần trăm vị thế (market reduceOnly).
        """
        sym = self.normalize_symbol(symbol)
        percent = max(min(float(percent or 0.0), 100.0), 0.0)
        if percent <= 0.0:
            return OrderResult(False, {"error": "percent<=0"})

        try:
            pos = await self._io(self.client.fetch_positions, [sym])
        except Exception as e:
            return OrderResult(False, {"error": f"fetch_positions_error:{e}"})

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
            return OrderResult(True, {"note": "no_position"})

        # fit một nhịp để tránh step lỗi
        markets = await self._io(self.client.load_markets)
        info = markets.get(sym) or {}
        lot_step = float(os.getenv("LOT_STEP_FALLBACK", "0.001"))
        step = ((info.get("limits") or {}).get("amount") or {}).get("step")
        if step:
            size_fit = self._floor_step(size, float(step))
        else:
            size_fit = self._floor_step(size, lot_step)

        if size_fit <= 0:
            return OrderResult(False, {"error": "size_fit<=0"})

        try:
            o = await self._io(self.client.create_order, sym, "market", side_to_close, size_fit, None, {"reduceOnly": True})
            return OrderResult(True, {"close_part": o})
        except Exception as e:
            return OrderResult(False, {"error": f"close_part_error:{e}"})

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
            return OrderResult(False, {"error": f"cancel_all_error:{e}"})


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
        return OrderResult(False, {"error": f"close_on_account_error:{e}"})


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
            results[name] = OrderResult(False, {"error": f"close_on_all_error:{e}"})

    return results

# ======================================================================
# Hết file
# ======================================================================
