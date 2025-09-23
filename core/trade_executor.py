# ----------------------- core/trade_executor.py -----------------------
from __future__ import annotations
import asyncio, logging, math, os
from dataclasses import dataclass
from typing import Optional, Tuple

from dotenv import load_dotenv
import ccxt  # type: ignore

from config.settings import EXCHANGE_ID, API_KEY, API_SECRET, TESTNET

load_dotenv()
logging.getLogger(__name__).setLevel(logging.INFO)

@dataclass
class OrderResult:
    ok: bool
    message: str


def _to_thread(fn, *args, **kwargs):
    return asyncio.to_thread(fn, *args, **kwargs)


class ExchangeClient:
    """
    Wrapper CCXT đa sàn (Binance/OKX/BingX):
    - Chọn đúng thị trường futures (future/swap linear).
    - Chuẩn hóa symbol (OKX/BingX: BTC/USDT:USDT).
    - Fit khối lượng theo limits (min/max qty, stepSize, min/max notional).
    - Retry tự động khi gặp lỗi 'max quantity / max position value / notional'.
    """

    def __init__(
        self,
        exchange_id: Optional[str] = None,
        api_key: Optional[str] = None,
        api_secret: Optional[str] = None,
        testnet: Optional[bool] = None,
    ):
        self.exchange_id = (exchange_id or EXCHANGE_ID).lower()
        self.api_key = api_key or API_KEY
        self.api_secret = api_secret or API_SECRET
        self.testnet = TESTNET if testnet is None else bool(testnet)

        ex_class = getattr(ccxt, self.exchange_id)
        params = {
            "apiKey": self.api_key,
            "secret": self.api_secret,
            "enableRateLimit": True,
        }

        # Binance USDM futures
        if self.exchange_id in ("binanceusdm", "binance"):
            params.setdefault("options", {})["defaultType"] = "future"
            if self.testnet:
                params["urls"] = {
                    "api": {
                        "fapiPublic": "https://testnet.binancefuture.com/fapi/v1",
                        "fapiPrivate": "https://testnet.binancefuture.com/fapi/v1",
                    }
                }

        # OKX Perp (swap, linear USDT-M)
        if self.exchange_id == "okx":
            params.setdefault("options", {})["defaultType"] = "swap"
            pw = os.getenv("OKX_PASSPHRASE", "")
            if pw:
                params["password"] = pw

        # BingX Perp (swap, linear USDT-M)
        if self.exchange_id == "bingx":
            opts = params.setdefault("options", {})
            opts["defaultType"] = "swap"
            try:
                opts["defaultSubType"] = "linear"
            except Exception:
                pass

        self.client = ex_class(params)
        self._markets_loaded = False

    # ------------- markets / symbol -------------
    async def _ensure_markets(self):
        if not self._markets_loaded:
            await _to_thread(self.client.load_markets)
            self._markets_loaded = True

    def normalize_symbol(self, pair: str) -> str:
        p = (pair or "").strip().upper()
        if self.exchange_id in ("okx", "bingx"):
            if p.endswith("/USDT") and ":USDT" not in p:
                p = p.replace("/USDT", "/USDT:USDT")
        return p

    async def _market(self, symbol: str):
        await self._ensure_markets()
        sym = self.normalize_symbol(symbol)
        try:
            return self.client.market(sym)
        except Exception:
            return {}

    # ------------- helpers -------------
    async def _io(self, func, *args, **kwargs):
        return await _to_thread(func, *args, **kwargs)

    @staticmethod
    def _floor_step(x: float, step: float) -> float:
        if step and step > 0:
            return math.floor(x / step) * step
        return x

    def _should_shrink_on_error(self, err: Exception) -> bool:
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

    async def _fit_qty(self, symbol: str, qty: float, price: float) -> Tuple[float, dict]:
        """
        Trả về (qty_fit, meta_limits)
        """
        mkt = await self._market(symbol)
        info = mkt.get("info", {}) if isinstance(mkt, dict) else {}
        limits = mkt.get("limits", {}) if isinstance(mkt, dict) else {}
        precision = mkt.get("precision", {}) if isinstance(mkt, dict) else {}

        min_qty = None
        max_qty = None
        step = precision.get("amount")

        # ccxt unified
        amt = limits.get("amount") or {}
        min_qty = amt.get("min", min_qty)
        max_qty = amt.get("max", max_qty)

        min_cost = None
        max_cost = None
        cost = limits.get("cost") or {}
        min_cost = cost.get("min", min_cost)
        max_cost = cost.get("max", max_cost)

        # raw filters (Binance USDM)
        try:
            for f in info.get("filters", []):
                t = f.get("filterType")
                if t in ("LOT_SIZE", "MARKET_LOT_SIZE"):
                    min_qty = float(f.get("minQty", min_qty or 0)) or min_qty
                    max_qty = float(f.get("maxQty", max_qty or 0)) or max_qty
                    step = float(f.get("stepSize", step or 0)) or step
                if t in ("MIN_NOTIONAL", "NOTIONAL", "MARKET_MIN_NOTIONAL"):
                    min_cost = float(f.get("minNotional", min_cost or 0)) or min_cost
                    mx = f.get("maxNotional")
                    if mx is not None:
                        try:
                            max_cost = float(mx)
                        except Exception:
                            pass
        except Exception:
            pass

        # Fit theo max_cost trước (nếu có)
        q = float(qty)
        if max_cost and price:
            q = min(q, float(max_cost) / float(price))

        # Fit theo max_qty
        if max_qty:
            q = min(q, float(max_qty))

        # Rounding theo step
        if step:
            q = self._floor_step(q, float(step))

        # Đảm bảo >= min
        if min_cost and price:
            q = max(q, float(min_cost) / float(price))
        if min_qty:
            q = max(q, float(min_qty))

        # Không để 0
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

        while attempt <= max_retries:
            try:
                return await self._io(self.client.create_order, sym, "market", side, q)
            except Exception as e:
                last_err = e
                if not self._should_shrink_on_error(e):
                    break
                # Giảm size và thử lại
                q = max(self._floor_step(q * 0.7, float(os.getenv("LOT_STEP_FALLBACK", "0.001"))), 0.0)
                attempt += 1

        raise last_err if last_err else Exception("create_order failed")

    # ---------------- Account helpers ----------------
    async def set_leverage(self, symbol: str, lev: int):
        try:
            sym = self.normalize_symbol(symbol)
            if hasattr(self.client, "set_leverage"):
                await self._io(self.client.set_leverage, int(lev), sym)
        except Exception as e:
            logging.warning("set_leverage failed: %s", e)

    async def ticker_price(self, symbol: str) -> float:
        try:
            sym = self.normalize_symbol(symbol)
            t = await self._io(self.client.fetch_ticker, sym)
            p = float(t.get("last") or t.get("close") or 0.0)
            if p > 0:
                return p
        except Exception:
            pass
        return 0.0

    async def balance_usdt(self) -> float:
        try:
            bal = await self._io(self.client.fetch_balance)
        except Exception:
            return 0.0
        for key in ("USDT", "usdt", "USDC", "BUSD"):
            total = (bal.get("total") or {})
            free = (bal.get("free") or {})
            if key in total or key in free:
                return float(free.get(key, total.get(key, 0.0)))
        return float((bal.get("info") or {}).get("availableBalance", 0) or 0)

    # ---------------- Position helpers ----------------
    async def current_position(self, symbol: str):
        """
        Returns (side_long: Optional[bool], qty: float)
        """
        try:
            sym = self.normalize_symbol(symbol)
            positions = None
            try:
                positions = await self._io(self.client.fetch_positions, [sym])
            except Exception:
                try:
                    one = await self._io(self.client.fetch_position, sym)
                    positions = [one] if one else []
                except Exception:
                    positions = []

            amt = 0.0
            side_long = None
            for p in positions or []:
                if not isinstance(p, dict):
                    continue
                contracts = p.get("contracts")
                side = (p.get("side") or "").lower()
                info = p.get("info") or {}
                pos_amt_info = info.get("positionAmt") or info.get("positionAmtRaw") or info.get("amount")

                if contracts is not None:
                    try:
                        amt = float(contracts)
                    except Exception:
                        amt = 0.0
                    side_long = (side == "long") if side in ("long", "short") else (amt > 0)
                elif pos_amt_info is not None:
                    try:
                        amt = float(pos_amt_info)
                    except Exception:
                        amt = 0.0
                    side_long = amt > 0
                elif "amount" in p:
                    try:
                        amt = float(p["amount"])
                    except Exception:
                        amt = 0.0
                    side_long = amt > 0

                if abs(amt) > 0:
                    break

            qty = abs(float(amt))
            if qty <= 0:
                return None, 0.0
            return side_long, qty
        except Exception:
            return None, 0.0

    # ---------------- Order helpers ----------------
    async def fetch_open_orders(self, symbol: str):
        try:
            sym = self.normalize_symbol(symbol)
            return await self._io(self.client.fetch_open_orders, sym)
        except Exception:
            return []

    async def cancel_tp_sl_orders(self, symbol: str) -> OrderResult:
        try:
            orders = await self.fetch_open_orders(symbol)
            cancelled = 0
            for o in orders or []:
                typ = (o.get("type") or "").lower()
                info = o.get("info", {}) or {}
                has_stop = any(k in info for k in ("stopPrice","triggerPrice","stopPx","tpTriggerPx","slTriggerPx"))
                is_tp_sl = ("stop" in typ) or ("take" in typ) or has_stop
                if not is_tp_sl:
                    continue
                oid = o.get("id")
                if not oid:
                    continue
                try:
                    sym = self.normalize_symbol(symbol)
                    await self._io(self.client.cancel_order, oid, sym)
                    cancelled += 1
                except Exception:
                    pass
            return OrderResult(True, f"Đã hủy {cancelled} lệnh SL/TP còn chờ.")
        except Exception as e:
            return OrderResult(False, f"Hủy SL/TP lỗi: {e}")

    async def cancel_all_orders_symbol(self, symbol: str) -> OrderResult:
        try:
            sym = self.normalize_symbol(symbol)
            fn = getattr(self.client, "cancel_all_orders", None) or getattr(self.client, "cancelAllOrders", None)
            if callable(fn):
                try:
                    await self._io(fn, sym)
                    return OrderResult(True, "Đã hủy toàn bộ lệnh chờ.")
                except Exception:
                    pass
            orders = await self.fetch_open_orders(sym)
            cancelled = 0
            for o in orders or []:
                oid = o.get("id")
                if not oid:
                    continue
                try:
                    await self._io(self.client.cancel_order, oid, sym)
                    cancelled += 1
                except Exception:
                    pass
            return OrderResult(True, f"Đã hủy {cancelled} lệnh chờ.")
        except Exception as e:
            return OrderResult(False, f"Hủy lệnh chờ lỗi: {e}")

    # ---------------- Market entry/exit ----------------
    async def open_market(
        self,
        symbol: str,
        side: str,
        qty: float,
        leverage: Optional[int] = None,
        stop_loss: Optional[float] = None,
    ) -> OrderResult:
        try:
            sym = self.normalize_symbol(symbol)
            if leverage is not None and hasattr(self.client, "set_leverage"):
                try:
                    await self.set_leverage(sym, int(leverage))
                except Exception:
                    pass

            # dùng ticker nếu giá chưa có để fit notional
            px = await self.ticker_price(sym)
            s = (side or "").upper()
            if s not in ("LONG", "SHORT"):
                return OrderResult(False, f"Invalid side: {side}")
            order_side = "buy" if s == "LONG" else "sell"

            q_fit, meta = await self._fit_qty(sym, float(qty), float(px or 0))
            if q_fit <= 0:
                return OrderResult(False, f"Order failed: qty_fit=0 (limits={meta})")

            entry = await self._place_market_with_retries(sym, order_side, q_fit)

            # SL (reduceOnly)
            if stop_loss is not None:
                opp = "sell" if order_side == "buy" else "buy"
                params = {"reduceOnly": True, "stopPrice": float(stop_loss)}
                if self.exchange_id == "okx":
                    params["slTriggerPx"] = float(stop_loss)
                try:
                    _ = await self._io(self.client.create_order, sym, "stop_market", opp, q_fit, None, params)
                except Exception as e:
                    try:
                        _ = await self._io(self.client.create_order, sym, "STOP", opp, q_fit, None, params)
                    except Exception:
                        return OrderResult(True, f"Entry ok (orderId={entry.get('id')}) | SL create error: {e}")

            return OrderResult(True, f"Entry ok (orderId={entry.get('id')})")
        except Exception as e:
            return OrderResult(False, f"Order failed: {e}")

    async def close_position(self, symbol: str) -> OrderResult:
        try:
            sym = self.normalize_symbol(symbol)
            side_long, qty = await self.current_position(sym)
            if (side_long is None and qty == 0) or qty <= 0:
                return OrderResult(True, "Không có vị thế mở.")
            side = "sell" if side_long else "buy"
            params = {"reduceOnly": True}
            order = await self._io(self.client.create_order, sym, "market", side, qty, None, params)
            oid = (order or {}).get("id", "N/A")

            await asyncio.sleep(0.7)
            _ = await self.cancel_all_orders_symbol(sym)
            return OrderResult(True, f"Đã đóng toàn bộ vị thế (orderId={oid})")
        except Exception as e:
            return OrderResult(False, f"Close thất bại: {e}")

    async def close_position_pct(self, symbol: str, pct: float) -> OrderResult:
        """
        Đóng theo phần trăm vị thế hiện tại (reduceOnly).
        """
        try:
            pct = max(0.0, min(100.0, float(pct)))
            if pct == 0.0:
                return OrderResult(True, "Không có gì để đóng (0%).")

            sym = self.normalize_symbol(symbol)
            side_long, qty = await self.current_position(sym)
            if (side_long is None and qty == 0) or qty <= 0:
                return OrderResult(True, "Không có vị thế mở.")

            close_qty = qty * (pct / 100.0)
            # làm tròn nhẹ theo LOT_STEP_FALLBACK để tránh lỗi step
            lot_step = float(os.getenv("LOT_STEP_FALLBACK", "0.001"))
            close_qty = max(self._floor_step(close_qty, lot_step), lot_step)

            side = "sell" if side_long else "buy"
            params = {"reduceOnly": True}
            order = await self._io(self.client.create_order, sym, "market", side, close_qty, None, params)
            oid = (order or {}).get("id", "N/A")
            return OrderResult(True, f"Đã đóng {pct:.1f}% vị thế (orderId={oid})")
        except Exception as e:
            return OrderResult(False, f"Close% thất bại: {e}")

    async def market_with_sl_tp(self, symbol: str, side_long: bool, qty: float, sl: float, tp: float) -> OrderResult:
        try:
            sym = self.normalize_symbol(symbol)
            px = await self.ticker_price(sym)

            q_fit, meta = await self._fit_qty(sym, float(qty), float(px or 0))
            if q_fit <= 0:
                return OrderResult(False, f"Order failed: qty_fit=0 (limits={meta})")

            side = "buy" if side_long else "sell"
            entry = await self._place_market_with_retries(sym, side, q_fit)

            params = {"reduceOnly": True}
            opp = "sell" if side == "buy" else "buy"

            # SL
            try:
                sl_params = {**params, "stopPrice": float(sl)}
                if self.exchange_id == "okx":
                    sl_params["slTriggerPx"] = float(sl)
                try:
                    _ = await self._io(self.client.create_order, sym, "stop_market", opp, q_fit, None, sl_params)
                except Exception:
                    _ = await self._io(self.client.create_order, sym, "STOP", opp, q_fit, None, sl_params)
            except Exception:
                pass

            # TP
            try:
                tp_params = {**params, "stopPrice": float(tp)}
                if self.exchange_id == "okx":
                    tp_params["tpTriggerPx"] = float(tp)
                try:
                    _ = await self._io(self.client.create_order, sym, "take_profit_market", opp, q_fit, None, tp_params)
                except Exception:
                    _ = await self._io(self.client.create_order, sym, "TAKE_PROFIT", opp, q_fit, None, tp_params)
            except Exception:
                pass

            return OrderResult(True, f"Live order placed: entry={entry.get('id')}")
        except Exception as e:
            return OrderResult(False, f"Order failed: {e}")


# ----------- sizing helpers ---------------
def calc_qty(balance_usdt: float, risk_percent: float, leverage: int, entry_price: float, lot_step: float = 0.001) -> float:
    """
    Sizing cơ bản theo risk% * leverage / price (trước khi fit theo limits sàn).
    """
    notional = balance_usdt * max(risk_percent, 0) / 100.0 * max(leverage, 1)
    qty_raw = notional / max(entry_price, 1e-9)
    qty = int(qty_raw / lot_step) * lot_step
    return max(qty, lot_step)


def auto_sl_by_leverage(entry: float, side: str, lev: int, rr_mult: float = None):
    lev = max(int(lev), 1)
    try:
        if rr_mult is None:
            rr_mult = float(os.getenv("TP_RR_MULT", "2.0"))
    except Exception:
        rr_mult = 2.0
    dist = float(entry) * (0.5 / float(lev))
    if str(side).upper() == "LONG":
        sl = float(entry) - dist
        tp = float(entry) + rr_mult * dist
    else:
        sl = float(entry) + dist
        tp = float(entry) - rr_mult * dist
    return sl, tp
# ----------------------- /core/trade_executor.py -----------------------
