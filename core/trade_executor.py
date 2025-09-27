# ----------------------- core/trade_executor.py -----------------------
from __future__ import annotations
import asyncio
import logging
import math
import os
import json
from dataclasses import dataclass
from typing import Optional, Tuple, List, Dict, Any, Literal
from datetime import timedelta, datetime

from dotenv import load_dotenv
import ccxt  # type: ignore

# ==== Load settings ====
from config.settings import EXCHANGE_ID, API_KEY, API_SECRET, TESTNET

load_dotenv()
logging.getLogger(__name__).setLevel(logging.INFO)


# ===================== Models =====================
@dataclass
class OrderResult:
    ok: bool
    message: str
    data: Optional[dict] = None


# ===================== Utils ======================
def _to_thread(fn, *args, **kwargs):
    return asyncio.to_thread(fn, *args, **kwargs)


def calc_qty(
    balance_usdt: float,
    risk_percent: float,
    leverage: int,
    entry_price: float,
    min_qty: float = 0.0,
) -> float:
    """
    notional = balance * (risk%/100) * leverage
    qty      = notional / entry_price
    """
    try:
        bal = max(0.0, float(balance_usdt))
        rp = max(0.0, float(risk_percent)) / 100.0
        lev = max(1, int(leverage))
        px = float(entry_price)
        if px <= 0 or bal <= 0 or rp <= 0:
            return 0.0
        qty = (bal * rp * lev) / px
        if min_qty and qty < min_qty:
            qty = min_qty
        return float(qty)
    except Exception:
        return 0.0


def auto_sl_by_leverage(entry: float, side: str, lev: int, rr_mult: float | None = None) -> Tuple[float, float]:
    """
    Công thức “bản cũ”: khoảng cách SL = entry * (0.5 / lev)
    TP = entry + rr_mult * distance (LONG) / đối xứng (SHORT)
    Với lev=44 → SL ~1.136%, TP ~2.273% (nếu rr_mult=2.0)
    """
    lev = max(int(lev), 1)
    try:
        rr_mult = float(os.getenv("TP_RR_MULT", "2.0")) if rr_mult is None else float(rr_mult)
    except Exception:
        rr_mult = 2.0

    entry = float(entry)
    dist = entry * (0.5 / float(lev))
    if str(side).upper() == "LONG":
        sl = entry - dist
        tp = entry + rr_mult * dist
    else:
        sl = entry + dist
        tp = entry - rr_mult * dist
    return sl, tp


def _force_is_long(side) -> bool:
    """
    Chuẩn hoá hướng vào lệnh thành bool:
    - 'LONG'/'long'/'buy'/True/1/+1 → True
    - 'SHORT'/'short'/'sell'/False/0/-1 → False
    - Mặc định False nếu không nhận diện được
    """
    try:
        if isinstance(side, bool):
            return side
        if isinstance(side, (int, float)):
            return float(side) > 0
        s = str(side or "").strip().lower()
        if s in ("long", "buy", "true", "1", "+1", "yes", "y"):
            return True
        if s in ("short", "sell", "false", "0", "-1", "no", "n"):
            return False
    except Exception:
        pass
    return False


def _force_float(x, default=None):
    try:
        return float(x)
    except Exception:
        return default


# ===================== Exchange Client =====================
class ExchangeClient:
    """
    CCXT wrapper đa sàn:
    - Binance USDM / OKX / BingX (USDT-margined swap)
    - Chuẩn hoá symbol (OKX/BingX cần BTC/USDT:USDT)
    - Fit qty theo min/max qty, stepSize, min/max notional
    - Tự co size & retry khi vướng hạn mức
    """

    def __init__(
        self,
        exchange_id: Optional[str] = None,
        api_key: Optional[str] = None,
        api_secret: Optional[str] = None,
        testnet: Optional[bool] = None,
        **kwargs: Any,
    ):
        self.exchange_id = (exchange_id or EXCHANGE_ID).lower()
        self.api_key = api_key or API_KEY
        self.api_secret = api_secret or API_SECRET
        self.testnet = TESTNET if testnet is None else bool(testnet)

        ex_class = getattr(ccxt, self.exchange_id)
        params: Dict[str, Any] = {
            "apiKey": self.api_key,
            "secret": self.api_secret,
            "enableRateLimit": True,
        }

        if self.exchange_id in ("binanceusdm", "binance"):
            params.setdefault("options", {})["defaultType"] = "future"
            if self.testnet:
                params["urls"] = {
                    "api": {
                        "fapiPublic": "https://testnet.binancefuture.com/fapi/v1",
                        "fapiPrivate": "https://testnet.binancefuture.com/fapi/v1",
                    }
                }

        if self.exchange_id == "okx":
            params.setdefault("options", {})["defaultType"] = "swap"
            pw = os.getenv("OKX_PASSPHRASE", "")
            if pw:
                params["password"] = pw

        if self.exchange_id == "bingx":
            opts = params.setdefault("options", {})
            opts["defaultType"] = "swap"
            try:
                opts["defaultSubType"] = "linear"
            except Exception:
                pass

        self.client = ex_class(params)
        self._markets_loaded = False

        # trạng thái Position Mode cho Binance (hedge|oneway|None)
        self._binance_position_mode: Optional[str] = None

    # ---------- markets/symbol ----------
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

    # ---------- async I/O helper ----------
    async def _io(self, func, *args, **kwargs):
        return await _to_thread(func, *args, **kwargs)

    # ---------- Detect Binance position mode ----------
    async def _detect_binance_position_mode(self) -> str:
        """
        Trả về 'hedge' hoặc 'oneway' cho Binance USDⓈ-M.
        Có thể override bằng ENV POSITION_MODE_OVERRIDE=hedge|oneway.
        Nếu không phải binanceusdm → mặc định 'oneway'.
        """
        override = (os.getenv("POSITION_MODE_OVERRIDE") or "").strip().lower()
        if override in ("hedge", "oneway"):
            self._binance_position_mode = override
            return override

        if self.exchange_id != "binanceusdm":
            self._binance_position_mode = "oneway"
            return "oneway"

        try_methods = [
            "fapiPrivate_get_positionside_dual",
            "fapiPrivateGetPositionSideDual",
        ]
        for m in try_methods:
            try:
                fn = getattr(self.client, m)
                resp = await self._io(fn)
                dual = (resp or {}).get("dualSidePosition")
                mode = "hedge" if str(dual).lower() in ("true", "1") else "oneway"
                self._binance_position_mode = mode
                return mode
            except Exception:
                continue

        self._binance_position_mode = "oneway"
        return "oneway"

    # ---------- common helpers ----------
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

    @staticmethod
    def _as_side_long(v) -> bool:
        try:
            if isinstance(v, bool):
                return v
            if isinstance(v, (int, float)):
                return float(v) > 0
            s = str(v or "").strip().lower()
            if s in ("long", "buy", "true", "1", "+1", "yes", "y"):
                return True
            if s in ("short", "sell", "false", "0", "-1", "no", "n"):
                return False
        except Exception:
            pass
        return False

    @staticmethod
    def _is_pos_side_mismatch(err: Exception) -> bool:
        s = str(err).lower()
        return ("-4061" in s) or ("position side does not match" in s)

    # ---------- limits / qty fit ----------
    async def _fit_qty(self, symbol: str, qty: float, price: float) -> Tuple[float, dict]:
        """
        Trả về (qty_fit, meta_limits), xử lý:
        - min/max qty, stepSize
        - min/max notional (cost)
        - fallback LOT_STEP_FALLBACK nếu q<=0
        """
        mkt = await self._market(symbol)
        info = mkt.get("info", {}) if isinstance(mkt, dict) else {}
        limits = mkt.get("limits", {}) if isinstance(mkt, dict) else {}
        precision = mkt.get("precision", {}) if isinstance(mkt, dict) else {}

        min_qty = max_qty = min_cost = max_cost = None
        step = precision.get("amount")

        amt = limits.get("amount") or {}
        min_qty = amt.get("min", min_qty)
        max_qty = amt.get("max", max_qty)

        cost = limits.get("cost") or {}
        min_cost = cost.get("min", min_cost)
        max_cost = cost.get("max", max_cost)

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

        q = float(qty)
        if max_cost and price:
            q = min(q, float(max_cost) / float(price))
        if max_qty:
            q = min(q, float(max_qty))
        if step:
            q = self._floor_step(q, float(step))
        if min_cost and price:
            q = max(q, float(min_cost) / float(price))
        if min_qty:
            q = max(q, float(min_qty))

        if q <= 0:
            lot_step = float(os.getenv("LOT_STEP_FALLBACK", "0.001"))
            q = float(min_qty or lot_step)

        meta = {"min_qty": min_qty, "max_qty": max_qty, "step": step, "min_cost": min_cost, "max_cost": max_cost}
        return float(q), meta

    # ---------- fit stopPrice theo tickSize ----------
    def _fit_stop_price(self, symbol: str, price: float, *, favor: str | None = None) -> float:
        """
        Chuẩn hoá stopPrice theo tickSize của market (Binance rất nghiêm).
        favor: "down" (floor), "up" (ceil), None (round).
        """
        try:
            mkt = self.client.market(self.normalize_symbol(symbol))
            info = mkt.get("info", {}) if isinstance(mkt, dict) else {}
            precision = mkt.get("precision", {}) if isinstance(mkt, dict) else {}
            tick = None

            for f in (info.get("filters") or []):
                if f.get("filterType") == "PRICE_FILTER":
                    ts = f.get("tickSize")
                    if ts:
                        try:
                            tick = float(ts)
                        except Exception:
                            pass
                    break

            if not tick:
                p = precision.get("price")
                if isinstance(p, int) and p >= 0:
                    tick = 10 ** (-p)

            px = float(price)
            if not tick or tick <= 0:
                return px

            q = px / tick
            if favor == "down":
                q = math.floor(q)
            elif favor == "up":
                q = math.ceil(q)
            else:
                q = round(q)
            return float(q * tick)
        except Exception:
            return float(price)

    # ---------- place market with retries ----------
    async def _place_market_with_retries(self, sym: str, side: str, qty: float, *, params: Optional[dict] = None, max_retries: int = 3):
        """
        Tạo lệnh market; nếu lỗi do limit → giảm size (0.7×) và thử lại.
        [MODIFIED] Bổ sung truyền params (vd: positionSide cho Binance hedge).
        """
        attempt = 0
        last_err: Optional[Exception] = None
        q = float(qty)
        lot_step = float(os.getenv("LOT_STEP_FALLBACK", "0.001"))

        while attempt <= max_retries:
            try:
                return await self._io(self.client.create_order, sym, "market", side, q, None, params or {})
            except Exception as e:
                last_err = e
                if not self._should_shrink_on_error(e):
                    break
                q = max(self._floor_step(q * 0.7, lot_step), 0.0)
                attempt += 1
        raise last_err if last_err else Exception("create_order failed")

    # ---------- account / market data ----------
    async def set_leverage(self, symbol: str, lev: int):
        try:
            sym = self.normalize_symbol(symbol)
            if hasattr(self.client, "set_leverage"):
                await self._io(self.client.set_leverage, int(lev), sym)
        except Exception as e:
            logging.warning("set_leverage failed: %s", e)

    async def ticker_price(self, symbol: str) -> float:
        """
        Lấy giá gần nhất cho futures/swap (đặc biệt robust cho Binance USDM):

        Thứ tự:
        1) fetch_ticker (last/close)
        2) [Binance USDM] /fapi/v1/ticker/price (price)
        3) [Binance USDM] /fapi/v1/ticker/24hr (lastPrice/weightedAvgPrice/prevClosePrice/close)
        4) fetch_ohlcv 1m (close)
        5) [Binance USDM] /fapi/v1/premiumIndex (markPrice)
        6) fetch_order_book (mid price)

        Trả 0.0 nếu tất cả đều bất khả kháng.
        """
        try:
            await self._ensure_markets()
            sym = self.normalize_symbol(symbol)

            def _mk_sym_id() -> str:
                try:
                    mkt = self.client.market(sym)
                    mid = (mkt.get("id") or "").upper() if isinstance(mkt, dict) else ""
                except Exception:
                    mid = ""
                if not mid:
                    mid = symbol.upper().replace("/", "")
                    if mid.endswith(":USDT"):
                        mid = mid.replace(":USDT", "")
                return mid

            # 1) CCXT ticker
            try:
                t = await self._io(self.client.fetch_ticker, sym)
                p = t.get("last") or t.get("close")
                if p is not None:
                    p = float(p)
                    if p > 0:
                        return p
            except Exception:
                pass

            # 2 & 3) Endpoint futures chuyên biệt cho Binance USDM
            if self.exchange_id == "binanceusdm":
                sym_id = _mk_sym_id()

                # 2) /fapi/v1/ticker/price
                for _ in range(2):  # retry ngắn
                    try:
                        fn = getattr(self.client, "fapiPublic_get_ticker_price", None) or getattr(self.client, "fapiPublicGetTickerPrice", None)
                        if callable(fn):
                            data = await self._io(fn, {"symbol": sym_id})
                            obj = data[0] if isinstance(data, list) and data else data
                            p = float((obj or {}).get("price") or 0.0)
                            if p > 0:
                                return p
                    except Exception:
                        pass

                # 3) /fapi/v1/ticker/24hr
                try:
                    fn24 = getattr(self.client, "fapiPublic_get_ticker_24hr", None) or getattr(self.client, "fapiPublicGetTicker24hr", None)
                    if callable(fn24):
                        data24 = await self._io(fn24, {"symbol": sym_id})
                        obj24 = data24[0] if isinstance(data24, list) and data24 else data24
                        for k in ("lastPrice", "weightedAvgPrice", "prevClosePrice", "close"):
                            v = _force_float((obj24 or {}).get(k), None)
                            if v and v > 0:
                                return float(v)
                except Exception:
                    pass

            # 4) OHLCV 1m
            try:
                ohlcv = await self._io(self.client.fetch_ohlcv, sym, timeframe="1m", limit=1)
                if ohlcv and len(ohlcv) > 0:
                    close = float(ohlcv[-1][4])
                    if close > 0:
                        return close
            except Exception:
                pass

            # 5) Binance markPrice (premium index)
            if self.exchange_id == "binanceusdm":
                try:
                    sym_id = _mk_sym_id()
                    fnpi = getattr(self.client, "fapiPublic_get_premiumindex", None) or getattr(self.client, "fapiPublicGetPremiumIndex", None)
                    if callable(fnpi):
                        data = await self._io(fnpi, {"symbol": sym_id})
                        obj = data[0] if isinstance(data, list) and data else data
                        mp = float((obj or {}).get("markPrice") or 0.0)
                        if mp > 0:
                            return mp
                except Exception:
                    pass

            # 6) Orderbook mid
            try:
                ob = await self._io(self.client.fetch_order_book, sym, limit=5)
                bid = float(ob["bids"][0][0]) if ob.get("bids") else 0.0
                ask = float(ob["asks"][0][0]) if ob.get("asks") else 0.0
                mid = (bid + ask) / 2.0 if bid > 0 and ask > 0 else max(bid, ask)
                if mid and mid > 0:
                    return mid
            except Exception:
                pass

        except Exception:
            pass
        return 0.0

    async def balance_usdt(self) -> float:
        """
        Lấy free/total USDT (hoặc availableBalance từ info).
        """
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

    async def get_balance_quote(self) -> float:
        return await self.balance_usdt()

    # ---------- positions ----------
    async def current_position(self, symbol: str) -> Tuple[Optional[bool], float]:
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

    # ---------- order maintenance ----------
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
                has_stop = any(k in info for k in ("stopPrice", "triggerPrice", "stopPx", "tpTriggerPx", "slTriggerPx"))
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
        # noqa: E999
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

    # ---------- market entry/exit ----------
    async def open_market(
        self,
        symbol: str,
        side: str,                 # "LONG" | "SHORT"
        qty: float,
        leverage: Optional[int] = None,
        stop_loss: Optional[float] = None,
    ) -> OrderResult:
        """
        Vào lệnh thị trường (side = LONG/SHORT). Tự fit qty + đặt SL reduceOnly nếu truyền.
        [MODIFIED] Tự phát hiện Hedge Mode của Binance & gắn positionSide phù hợp.
        [NEW]     Nếu gặp -4061 → tự động chuyển chế độ cache (hedge↔oneway) và retry.
        [NEW]     Fit stopPrice theo tickSize + set workingType (MARK_PRICE/CONTRACT_PRICE) cho Binance,
                  và retry 1 lần với workingType còn lại nếu bị từ chối.
        """
        try:
            sym = self.normalize_symbol(symbol)

            if leverage is not None and hasattr(self.client, "set_leverage"):
                try:
                    await self.set_leverage(sym, int(leverage))
                except Exception:
                    pass

            px = await self.ticker_price(sym)
            s = (side or "").upper()
            if s not in ("LONG", "SHORT"):
                return OrderResult(False, f"Invalid side: {side}")
            order_side = "buy" if s == "LONG" else "sell"

            q_fit, meta = await self._fit_qty(sym, float(qty), float(px or 0))
            if q_fit <= 0:
                return OrderResult(False, f"Order failed: qty_fit=0 (limits={meta})")

            # Detect mode & build params for entry
            params_entry: Dict[str, Any] = {}
            mode = self._binance_position_mode or await self._detect_binance_position_mode()
            if mode == "hedge" and self.exchange_id == "binanceusdm":
                params_entry["positionSide"] = "LONG" if s == "LONG" else "SHORT"

            # ENTRY (adaptive retry -4061)
            try:
                entry = await self._place_market_with_retries(sym, order_side, q_fit, params=params_entry)
            except Exception as e:
                if self._is_pos_side_mismatch(e) and self.exchange_id == "binanceusdm":
                    if "positionSide" in params_entry:
                        self._binance_position_mode = "oneway"
                        entry = await self._place_market_with_retries(sym, order_side, q_fit, params={})
                    else:
                        self._binance_position_mode = "hedge"
                        params_retry = {"positionSide": "LONG" if s == "LONG" else "SHORT"}
                        entry = await self._place_market_with_retries(sym, order_side, q_fit, params=params_retry)
                else:
                    raise

            # SL (reduceOnly/workingType/positionSide)
            if stop_loss is not None:
                opp = "sell" if order_side == "buy" else "buy"

                # LONG → SL < entry ⇒ floor; SHORT → SL > entry ⇒ ceil
                raw_sp = float(stop_loss)
                favor = "down" if s == "LONG" else "up"
                sp = self._fit_stop_price(sym, raw_sp, favor=favor)

                # Build params: Binance bỏ reduceOnly; các sàn khác giữ reduceOnly
                if self.exchange_id == "binanceusdm":
                    params = {"stopPrice": sp}
                    wt = (os.getenv("BINANCE_WORKING_TYPE") or "MARK_PRICE").strip().upper()
                    if wt not in ("MARK_PRICE", "CONTRACT_PRICE", "LAST_PRICE"):
                        wt = "MARK_PRICE"
                    params["workingType"] = wt
                else:
                    params = {"reduceOnly": True, "stopPrice": sp}

                if self.exchange_id == "okx":
                    params["slTriggerPx"] = sp

                mode_now = self._binance_position_mode or mode
                if mode_now == "hedge" and self.exchange_id == "binanceusdm":
                    params["positionSide"] = "LONG" if s == "LONG" else "SHORT"

                try:
                    await self._io(self.client.create_order, sym, "stop_market", opp, q_fit, None, params)
                except Exception as e:
                    if self._is_pos_side_mismatch(e) and self.exchange_id == "binanceusdm":
                        try:
                            if "positionSide" in params:
                                params2 = {"stopPrice": sp}
                                if self.exchange_id == "okx":
                                    params2["slTriggerPx"] = sp
                                if "workingType" in params:
                                    params2["workingType"] = params["workingType"]
                                await self._io(self.client.create_order, sym, "stop_market", opp, q_fit, None, params2)
                            else:
                                params2 = {
                                    "stopPrice": sp,
                                    "positionSide": "LONG" if s == "LONG" else "SHORT",
                                }
                                if self.exchange_id == "okx":
                                    params2["slTriggerPx"] = sp
                                wt = (os.getenv("BINANCE_WORKING_TYPE") or "MARK_PRICE").strip().upper()
                                if wt not in ("MARK_PRICE", "CONTRACT_PRICE", "LAST_PRICE"):
                                    wt = "MARK_PRICE"
                                params2["workingType"] = wt
                                await self._io(self.client.create_order, sym, "stop_market", opp, q_fit, None, params2)
                        except Exception as _:
                            logging.warning("Create SL order failed after retry: %s", e)
                    elif self.exchange_id == "binanceusdm" and "workingType" in params:
                        try:
                            params_alt = dict(params)
                            params_alt["workingType"] = "CONTRACT_PRICE" if params["workingType"] == "MARK_PRICE" else "MARK_PRICE"
                            await self._io(self.client.create_order, sym, "stop_market", opp, q_fit, None, params_alt)
                        except Exception:
                            logging.warning("Create SL order failed: %s", e)
                    else:
                        logging.warning("Create SL order failed: %s", e)

            return OrderResult(True, f"Live order placed: entry={entry.get('id')}", {"entry": entry})
        except Exception as e:
            return OrderResult(False, f"open_market failed: {e}")

    async def market_with_sl_tp(
        self,
        symbol: str,
        side_long,                 # chấp nhận bool/str/int/float
        qty: float,
        sl_price: Optional[float],
        tp_price: Optional[float],
    ) -> OrderResult:
        """
        Lệnh market + gắn SL/TP reduceOnly (nếu sàn hỗ trợ).
        'side_long' được chuẩn hoá: bool/str/int/float đều OK.
        [MODIFIED] Hỗ trợ Binance Hedge Mode với positionSide cho entry & SL/TP.
        [NEW]     Nếu gặp -4061 → tự động chuyển chế độ cache (hedge↔oneway) và retry.
        [NEW]     Fit stopPrice theo tickSize + set workingType (MARK_PRICE/CONTRACT_PRICE) cho Binance,
                  và retry 1 lần với kiểu còn lại nếu sàn từ chối.
        """
        try:
            sym = self.normalize_symbol(symbol)

            is_long = self._as_side_long(side_long)
            side = "buy" if is_long else "sell"

            px = await self.ticker_price(sym)

            q_fit, meta = await self._fit_qty(sym, float(qty), float(px or 0))
            if q_fit <= 0:
                return OrderResult(False, f"entry_error: qty_fit=0 (limits={meta})")

            params_entry: Dict[str, Any] = {}
            mode = self._binance_position_mode or await self._detect_binance_position_mode()
            if mode == "hedge" and self.exchange_id == "binanceusdm":
                params_entry["positionSide"] = "LONG" if is_long else "SHORT"

            try:
                entry = await self._place_market_with_retries(sym, side, q_fit, params=params_entry)
            except Exception as e:
                if self._is_pos_side_mismatch(e) and self.exchange_id == "binanceusdm":
                    if "positionSide" in params_entry:
                        self._binance_position_mode = "oneway"
                        entry = await self._place_market_with_retries(sym, side, q_fit, params={})
                    else:
                        self._binance_position_mode = "hedge"
                        params_retry = {"positionSide": "LONG" if is_long else "SHORT"}
                        entry = await self._place_market_with_retries(sym, side, q_fit, params=params_retry)
                else:
                    raise

            opp = "sell" if side == "buy" else "buy"

            # ----- Stop Loss -----
            if sl_price is not None:
                try:
                    raw_sp = float(sl_price)
                    favor_sl = "down" if is_long else "up"
                    sp = self._fit_stop_price(sym, raw_sp, favor=favor_sl)

                    # Build params: Binance bỏ reduceOnly; sàn khác giữ reduceOnly
                    if self.exchange_id == "binanceusdm":
                        params = {"stopPrice": sp}
                        wt = (os.getenv("BINANCE_WORKING_TYPE") or "MARK_PRICE").strip().upper()
                        if wt not in ("MARK_PRICE", "CONTRACT_PRICE", "LAST_PRICE"):
                            wt = "MARK_PRICE"
                        params["workingType"] = wt
                    else:
                        params = {"reduceOnly": True, "stopPrice": sp}

                    if self.exchange_id == "okx":
                        params["slTriggerPx"] = sp

                    mode_now = self._binance_position_mode or mode
                    if mode_now == "hedge" and self.exchange_id == "binanceusdm":
                        params["positionSide"] = "LONG" if is_long else "SHORT"

                    await self._io(self.client.create_order, sym, "stop_market", opp, q_fit, None, params)

                except Exception as e:
                    if self._is_pos_side_mismatch(e) and self.exchange_id == "binanceusdm":
                        try:
                            if "positionSide" in params:
                                params2 = {"stopPrice": sp}
                                if self.exchange_id == "okx":
                                    params2["slTriggerPx"] = sp
                                if "workingType" in params:
                                    params2["workingType"] = params["workingType"]
                                await self._io(self.client.create_order, sym, "stop_market", opp, q_fit, None, params2)
                            else:
                                params2 = {
                                    "stopPrice": sp,
                                    "positionSide": "LONG" if is_long else "SHORT",
                                }
                                if self.exchange_id == "okx":
                                    params2["slTriggerPx"] = sp
                                wt = (os.getenv("BINANCE_WORKING_TYPE") or "MARK_PRICE").strip().upper()
                                if wt not in ("MARK_PRICE", "CONTRACT_PRICE", "LAST_PRICE"):
                                    wt = "MARK_PRICE"
                                params2["workingType"] = wt
                                await self._io(self.client.create_order, sym, "stop_market", opp, q_fit, None, params2)
                        except Exception as _:
                            logging.warning("Create SL order failed after retry: %s", e)
                    elif self.exchange_id == "binanceusdm" and "workingType" in params:
                        try:
                            params_alt = dict(params)
                            params_alt["workingType"] = "CONTRACT_PRICE" if params["workingType"] == "MARK_PRICE" else "MARK_PRICE"
                            await self._io(self.client.create_order, sym, "stop_market", opp, q_fit, None, params_alt)
                        except Exception:
                            logging.warning("Create SL order failed: %s", e)
                    else:
                        logging.warning("Create SL order failed: %s", e)

            # ----- Take Profit -----
            if tp_price is not None:
                try:
                    raw_tp = float(tp_price)
                    favor_tp = "up" if is_long else "down"
                    tp = self._fit_stop_price(sym, raw_tp, favor=favor_tp)

                    # Build params: Binance bỏ reduceOnly; sàn khác giữ reduceOnly
                    if self.exchange_id == "binanceusdm":
                        params = {"stopPrice": tp}
                        wt = (os.getenv("BINANCE_WORKING_TYPE") or "MARK_PRICE").strip().upper()
                        if wt not in ("MARK_PRICE", "CONTRACT_PRICE", "LAST_PRICE"):
                            wt = "MARK_PRICE"
                        params["workingType"] = wt
                    else:
                        params = {"reduceOnly": True, "stopPrice": tp}

                    mode_now = self._binance_position_mode or mode
                    if mode_now == "hedge" and self.exchange_id == "binanceusdm":
                        params["positionSide"] = "LONG" if is_long else "SHORT"

                    tptype = "take_profit_market"
                    await self._io(self.client.create_order, sym, tptype, opp, q_fit, None, params)

                except Exception as e:
                    if self._is_pos_side_mismatch(e) and self.exchange_id == "binanceusdm":
                        try:
                            if "positionSide" in params:
                                params2 = {"stopPrice": tp}
                                if "workingType" in params:
                                    params2["workingType"] = params["workingType"]
                                await self._io(self.client.create_order, sym, "take_profit_market", opp, q_fit, None, params2)
                            else:
                                params2 = {
                                    "stopPrice": tp,
                                    "positionSide": "LONG" if is_long else "SHORT",
                                }
                                wt = (os.getenv("BINANCE_WORKING_TYPE") or "MARK_PRICE").strip().upper()
                                if wt not in ("MARK_PRICE", "CONTRACT_PRICE", "LAST_PRICE"):
                                    wt = "MARK_PRICE"
                                params2["workingType"] = wt
                                await self._io(self.client.create_order, sym, "take_profit_market", opp, q_fit, None, params2)
                        except Exception as _:
                            logging.warning("Create TP order failed after retry: %s", e)
                    elif self.exchange_id == "binanceusdm" and "workingType" in params:
                        try:
                            params_alt = dict(params)
                            params_alt["workingType"] = "CONTRACT_PRICE" if params["workingType"] == "MARK_PRICE" else "MARK_PRICE"
                            await self._io(self.client.create_order, sym, "take_profit_market", opp, q_fit, None, params_alt)
                        except Exception:
                            logging.warning("Create TP order failed: %s", e)
                    else:
                        logging.warning("Create TP order failed: %s", e)

            eid = entry.get("id") if isinstance(entry, dict) else None
            return OrderResult(True, f"Live order placed: entry={eid}", {"entry": entry})

        except Exception as e:
            msg = str(e)
            if "min" in msg.lower() and "amount" in msg.lower():
                return OrderResult(False, f"entry_error:{msg}")
            return OrderResult(False, f"entry_error:{msg}")

    async def close_percent(self, symbol: str, percent: float) -> OrderResult:
        """
        Đóng percent% vị thế hiện có (reduceOnly).
        100% → close toàn bộ. Tự co size nếu vướng hạn mức.
        GHI CHÚ: Ở Hedge Mode (Binance), cần gắn positionSide tương ứng LONG/SHORT khi close.
        """
        try:
            pct = max(0.0, min(100.0, float(percent)))
            side_long, qty = await self.current_position(symbol)
            if qty <= 0 or side_long is None:
                return OrderResult(True, "Không có vị thế mở.")

            sym = self.normalize_symbol(symbol)
            close_qty = qty * (pct / 100.0)
            lot_step = float(os.getenv("LOT_STEP_FALLBACK", "0.001"))
            close_qty = self._floor_step(close_qty, lot_step)
            if close_qty <= 0:
                return OrderResult(True, "Không có khối lượng để đóng (sau khi fit step).")

            # Xác định hướng & tham số
            side = "sell" if side_long else "buy"  # đóng LONG -> sell, đóng SHORT -> buy

            # Base params: reduceOnly cho lệnh close
            params = {"reduceOnly": True}

            # Nếu là Binance USDM và đang ở Hedge → gắn positionSide
            mode = self._binance_position_mode or await self._detect_binance_position_mode()
            if self.exchange_id == "binanceusdm" and mode == "hedge":
                params["positionSide"] = "LONG" if side_long else "SHORT"

            # Thử khớp lệnh
            try:
                await self._io(self.client.create_order, sym, "market", side, close_qty, None, params)
            except Exception as e:
                # Nếu mismatch position side (-4061) → thử flip theo cache
                if self._is_pos_side_mismatch(e) and self.exchange_id == "binanceusdm":
                    try:
                        if "positionSide" in params:
                            # Đang nghĩ Hedge nhưng thực tế có thể One-way → thử bỏ positionSide
                            params2 = {"reduceOnly": True}
                            await self._io(self.client.create_order, sym, "market", side, close_qty, None, params2)
                        else:
                            # Đang nghĩ One-way nhưng thực tế Hedge → thêm positionSide và retry
                            params2 = {"reduceOnly": True, "positionSide": "LONG" if side_long else "SHORT"}
                            await self._io(self.client.create_order, sym, "market", side, close_qty, None, params2)
                    except Exception as e2:
                        # Nếu lỗi do hạn mức → co size và thử lại 1-2 lần
                        if self._should_shrink_on_error(e2):
                            q = close_qty
                            for _ in range(2):
                                q = max(self._floor_step(q * 0.7, lot_step), 0.0)
                                if q <= 0:
                                    break
                                try:
                                    await self._io(self.client.create_order, sym, "market", side, q, None, params if "positionSide" in params else {"reduceOnly": True})
                                    return OrderResult(True, f"Closed ~{pct:.0f}% position (partial).")
                                except Exception:
                                    continue
                        return OrderResult(False, f"close_percent failed: {e2}")
                elif self._should_shrink_on_error(e):
                    # Lỗi hạn mức khác → co size và thử lại
                    q = close_qty
                    for _ in range(2):
                        q = max(self._floor_step(q * 0.7, lot_step), 0.0)
                        if q <= 0:
                            break
                        try:
                            await self._io(self.client.create_order, sym, "market", side, q, None, params)
                            return OrderResult(True, f"Closed ~{pct:.0f}% position (partial).")
                        except Exception:
                            continue
                    return OrderResult(False, f"close_percent failed: {e}")
                else:
                    return OrderResult(False, f"close_percent failed: {e}")

            return OrderResult(True, f"Closed {pct:.0f}% position.")
        except Exception as e:
            return OrderResult(False, f"close_percent failed: {e}")


# ===================== Multi-account / close helpers =====================
def _load_all_accounts() -> List[dict]:
    from config import settings as _S
    lst: List[dict] = []

    try:
        single = getattr(_S, "SINGLE_ACCOUNT", None)
        if isinstance(single, dict):
            lst.append(single)
    except Exception:
        pass
    try:
        accs = getattr(_S, "ACCOUNTS", [])
        if isinstance(accs, list):
            lst.extend([a for a in accs if isinstance(a, dict)])
    except Exception:
        pass

    try:
        j = os.getenv("ACCOUNTS_JSON", "")
        if j:
            arr = json.loads(j)
            if isinstance(arr, list):
                lst.extend([a for a in arr if isinstance(a, dict)])
    except Exception:
        pass

    uniq: List[dict] = []
    seen = set()
    for a in lst:
        exid = str(a.get("exchange") or EXCHANGE_ID).lower()
        key = a.get("api_key") or API_KEY
        k = (exid, key)
        if k in seen:
            continue
        seen.add(k)
        uniq.append(a)
    return uniq


def _find_account_by_name_or_exchange(name: str) -> Optional[dict]:
    if not name:
        return None
    name_l = str(name).strip().lower()
    all_accs = _load_all_accounts()

    for a in all_accs:
        nm = str(a.get("name", "")).strip().lower()
        if nm and nm == name_l:
            return a

    for a in all_accs:
        ex = str(a.get("exchange") or EXCHANGE_ID).strip().lower()
        if ex == name_l:
            return a

    return None


async def close_position_on_account(account_name: str, pair: str, percent: float) -> Dict[str, Any]:
    """
    Đóng vị thế trên 1 account (theo tên/hoặc exchange).
    - Tìm account trong SINGLE_ACCOUNT/ACCOUNTS/ACCOUNTS_JSON theo 'name' (ưu tiên) hoặc 'exchange'.
    - Nếu không thấy -> fallback về ENV default như cũ.
    - Khi percent >= 100: hủy TP/SL và toàn bộ lệnh chờ của symbol trước/sau khi close để đảm bảo sạch.
    ENV:
      - CLOSE_CANCEL_ALL_ON_100=true/false (default: true)
      - CLOSE_CANCEL_TP_SL_ON_PARTIAL=true/false (default: false)
    """
    try:
        pct = max(0.0, min(100.0, float(percent)))
        sym_pair = pair or "BTC/USDT"

        exid_d = os.getenv("EXCHANGE_ID", EXCHANGE_ID)
        api_d = os.getenv("API_KEY", API_KEY)
        sec_d = os.getenv("API_SECRET", API_SECRET)
        tnet_d = (os.getenv("TESTNET", str(TESTNET)).strip().lower() in ("1", "true", "yes", "on"))

        acc = _find_account_by_name_or_exchange(account_name)

        exid = str((acc or {}).get("exchange") or exid_d).lower()
        api = (acc or {}).get("api_key") or api_d
        sec = (acc or {}).get("api_secret") or sec_d
        tnet = bool((acc or {}).get("testnet", tnet_d))
        disp_name = (acc or {}).get("name", account_name or "default")

        cli = ExchangeClient(exid, api, sec, tnet)

        cancel_on_100 = (os.getenv("CLOSE_CANCEL_ALL_ON_100", "true").strip().lower() in ("1", "true", "yes", "on"))
        cancel_partial_tp_sl = (os.getenv("CLOSE_CANCEL_TP_SL_ON_PARTIAL", "false").strip().lower() in ("1", "true", "yes", "on"))

        cancelled_msgs: List[str] = []

        if pct >= 99.9 and cancel_on_100:
            r1 = await cli.cancel_tp_sl_orders(sym_pair)
            cancelled_msgs.append(r1.message)
            r2 = await cli.cancel_all_orders_symbol(sym_pair)
            cancelled_msgs.append(r2.message)

        if 0.0 < pct < 99.9 and cancel_partial_tp_sl:
            r0 = await cli.cancel_tp_sl_orders(sym_pair)
            cancelled_msgs.append(r0.message)

        res = await cli.close_percent(sym_pair, pct)

        if pct >= 99.9 and cancel_on_100:
            r3 = await cli.cancel_tp_sl_orders(sym_pair)
            cancelled_msgs.append(r3.message)

        msg = f"{disp_name} | {exid} → {res.message}"
        if cancelled_msgs:
            msg += " | " + " / ".join([m for m in cancelled_msgs if m])

        return {"ok": bool(res.ok), "message": msg}
    except Exception as e:
        return {"ok": False, "message": f"{account_name or 'default'} | {e}"}


async def close_position_on_all(pair: str, percent: float) -> List[Dict[str, Any]]:
    """
    Đóng vị thế trên tất cả account biết tới (SINGLE_ACCOUNT + ACCOUNTS + ACCOUNTS_JSON).
    - Tái sử dụng close_position_on_account để đảm bảo cùng chính sách cancel orders.
    """
    results: List[Dict[str, Any]] = []
    accs = _load_all_accounts()

    if not accs:
        r = await close_position_on_account("default", pair, percent)
        results.append(r)
        return results

    uniq: List[dict] = []
    seen = set()
    for a in accs:
        exid = str(a.get("exchange") or EXCHANGE_ID).lower()
        key = a.get("api_key") or API_KEY
        k = (exid, key)
        if k in seen:
            continue
        seen.add(k)
        uniq.append(a)

    for acc in uniq:
        name = acc.get("name") or acc.get("exchange") or "default"
        r = await close_position_on_account(name, acc.get("pair", pair or "BTC/USDT"), percent)
        results.append(r)

    return results


# ========== Shim adapters if not provided elsewhere ==========
try:
    from core.order_ops import open_single_account_order, open_multi_account_orders  # type: ignore
except Exception:
    async def open_single_account_order(app, storage, *, symbol: str, side: str,
                                       qty_cfg: dict, risk_cfg: dict, meta: dict):
        from config import settings as _S
        exid = getattr(_S, "EXCHANGE_ID", EXCHANGE_ID)
        api = getattr(_S, "API_KEY", API_KEY)
        sec = getattr(_S, "API_SECRET", API_SECRET)
        tnet = getattr(_S, "TESTNET", TESTNET)

        cli = ExchangeClient(exid, api, sec, tnet)

        px = await cli.ticker_price(symbol)
        if px <= 0:
            return False, {"error": "ticker_price<=0"}

        qty = float(qty_cfg.get("qty") or 0)
        if qty <= 0:
            bal = await cli.balance_usdt()
            risk_percent = float(risk_cfg.get("risk_percent", getattr(_S, "RISK_PERCENT", 1.0)))
            lev = int(risk_cfg.get("leverage", getattr(_S, "LEVERAGE", 10)))
            qty = calc_qty(bal, risk_percent, lev, px)

        if qty <= 0:
            return False, {"error": "qty<=0"}

        lev = int(risk_cfg.get("leverage", getattr(_S, "LEVERAGE", 10)))
        sl = qty_cfg.get("sl")
        tp = qty_cfg.get("tp")
        if sl is None or tp is None:
            sl, tp = auto_sl_by_leverage(px, side, lev)

        is_long = _force_is_long(side)
        qty = _force_float(qty, 0.0)
        sl = _force_float(sl, None)
        tp = _force_float(tp, None)

        res = await cli.market_with_sl_tp(symbol, is_long, qty, sl, tp)

        if not res.ok:
            return False, {"error": res.message}

        entry = (res.data or {}).get("entry", {})
        entry_id = entry.get("id")
        return True, {"opened": True, "entry_id": entry_id, "qty": qty, "price": px, "sl": sl, "tp": tp}

    async def open_multi_account_orders(app, storage, *, symbol: str, side: str,
                                       accounts_cfg: dict, qty_cfg: dict, risk_cfg: dict, meta: dict):
        from config import settings as _S

        try:
            ACCOUNTS = getattr(_S, "ACCOUNTS", [])
            if not isinstance(ACCOUNTS, list):
                ACCOUNTS = []
        except Exception:
            try:
                ACCOUNTS = json.loads(os.getenv("ACCOUNTS_JSON", "[]"))
                if not isinstance(ACCOUNTS, list):
                    ACCOUNTS = []
            except Exception:
                ACCOUNTS = []

        results = {}
        any_ok = False

        for acc in ACCOUNTS:
            try:
                name = acc.get("name", "acc")
                exid = str(acc.get("exchange") or EXCHANGE_ID).lower()
                api = acc.get("api_key") or API_KEY
                sec = acc.get("api_secret") or API_SECRET
                tnet = bool(acc.get("testnet", TESTNET))
                pair = acc.get("pair", symbol)

                cli = ExchangeClient(exid, api, sec, tnet)

                px = await cli.ticker_price(pair)
                if px <= 0:
                    results[name] = {"opened": False, "error": "ticker_price<=0"}
                    continue

                qty = float(qty_cfg.get("qty_per_account") or qty_cfg.get("qty") or 0)
                if qty <= 0:
                    bal = await cli.balance_usdt()
                    risk_percent = float(risk_cfg.get("risk_percent", acc.get("risk_percent", getattr(_S, "RISK_PERCENT", 1.0))))
                    lev = int(risk_cfg.get("leverage", acc.get("leverage", getattr(_S, "LEVERAGE", 10))))
                    qty = calc_qty(bal, risk_percent, lev, px)

                if qty <= 0:
                    results[name] = {"opened": False, "error": "qty<=0"}
                    continue

                lev = int(risk_cfg.get("leverage", acc.get("leverage", getattr(_S, "LEVERAGE", 10))))
                sl = qty_cfg.get("sl")
                tp = qty_cfg.get("tp")
                if sl is None or tp is None:
                    sl, tp = auto_sl_by_leverage(px, side, lev)

                is_long = _force_is_long(side)
                qty = _force_float(qty, 0.0)
                sl = _force_float(sl, None)
                tp = _force_float(tp, None)

                res = await cli.market_with_sl_tp(pair, is_long, qty, sl, tp)

                if not res.ok:
                    results[name] = {"opened": False, "error": res.message}
                else:
                    entry = (res.data or {}).get("entry", {})
                    results[name] = {"opened": True, "entry_id": entry.get("id"), "qty": qty, "price": px, "sl": sl, "tp": tp}
                    any_ok = True
            except Exception as e:
                results[name] = {"opened": False, "error": f"{e}"}

        return any_ok, results


# ========== Unified execute hub (MULTI first → fallback SINGLE if needed) ==========
async def execute_order_flow(
    app,
    storage,
    *,
    symbol: str,
    side: Literal["LONG", "SHORT"],
    qty_cfg: dict,
    risk_cfg: dict,
    accounts_cfg: dict,
    meta: dict,
    origin: Literal["AUTO", "MANUAL", "ORDER"],
) -> Tuple[bool, dict]:
    """
    Trình tự:
      1) Nếu bật MULTI (accounts_cfg.enabled): chạy MULTI trước.
      2) Nếu MULTI không mở được account nào → fallback SINGLE.
      3) Nếu MULTI tắt → chạy SINGLE.
    """
    entry_ids: List[str] = []
    per_account: Dict[str, Any] = {}

    multi_enabled = bool(accounts_cfg and accounts_cfg.get("enabled"))
    multi_opened = 0

    if multi_enabled:
        try:
            ok_m, multi = await open_multi_account_orders(
                app,
                storage,
                symbol=symbol,
                side=side,
                accounts_cfg=accounts_cfg,
                qty_cfg=qty_cfg,
                risk_cfg=risk_cfg,
                meta=meta,
            )
            per_account["multi"] = multi
            if isinstance(multi, dict):
                for _, d in multi.items():
                    if isinstance(d, dict) and d.get("opened"):
                        multi_opened += 1
                        eid = d.get("entry_id")
                        if eid:
                            entry_ids.append(eid)
        except Exception as e:
            per_account["multi_error"] = str(e)

    try_single = False
    if multi_enabled:
        try_single = (multi_opened == 0)
    else:
        try_single = True

    if try_single:
        try:
            ok_s, info_s = await open_single_account_order(
                app,
                storage,
                symbol=symbol,
                side=side,
                qty_cfg=qty_cfg,
                risk_cfg=risk_cfg,
                meta=meta,
            )
            if ok_s and isinstance(info_s, dict) and info_s.get("opened"):
                per_account["single"] = info_s
                eid = info_s.get("entry_id")
                if eid:
                    entry_ids.append(eid)
            else:
                per_account["single_error"] = (info_s if isinstance(info_s, str) else str(info_s))
        except Exception as e:
            per_account["single_error"] = str(e)

    opened_real = len(entry_ids) > 0
    result = {
        "entry_ids": entry_ids,
        "per_account": per_account,
        "origin": origin,
        "side": side,
        "symbol": symbol,
    }
    return opened_real, result


# ========= Helper dời TP-by-time cho toàn bộ lệnh đang mở =========
async def retime_tp_by_time_for_open_positions(app, storage, new_hours: float) -> int:
    """
    Đặt lại deadline TP-by-time cho toàn bộ vị thế đang mở theo chuẩn:
        new_deadline = tide_anchor(entry_time) + timedelta(hours=new_hours)
    Nếu không lấy được anchor → fallback entry_time + hours.
    Trả về: số vị thế đã cập nhật.
    """
    from strategy.signal_generator import tide_window_now  # dùng chung tính center (anchor)

    try:
        new_hours = float(new_hours)
    except Exception:
        return 0
    if new_hours <= 0:
        return 0

    def _iter_open(storage_obj):
        for name in ("list_open_positions", "get_open_positions", "list_positions_open", "get_positions_open"):
            if hasattr(storage_obj, name):
                fn = getattr(storage_obj, name)
                try:
                    return fn()
                except TypeError:
                    async def _aw(): return await fn()
                    return _aw()
        for name in ("list_positions", "get_positions", "get_all_positions", "positions"):
            if hasattr(storage_obj, name):
                obj = getattr(storage_obj, name)
                items = obj() if callable(obj) else obj
                return [p for p in (items or []) if not getattr(p, "is_closed", False)]
        for attr in ("state", "__dict__"):
            d = getattr(storage_obj, attr, None)
            if isinstance(d, dict):
                for k in ("positions", "open_positions", "trades", "orders"):
                    if isinstance(d.get(k), (list, tuple)):
                        return [p for p in d[k] if not getattr(p, "is_closed", False)]
        return []

    pos_iter = _iter_open(storage)
    if callable(getattr(pos_iter, "__await__", None)):
        open_positions = await pos_iter
    else:
        open_positions = pos_iter

    updated = 0
    for p in open_positions or []:
        if getattr(p, "is_closed", False):
            continue
        entry_time = getattr(p, "entry_time", None)
        if not entry_time:
            continue

        anchor = getattr(p, "tide_center", None) or getattr(p, "tide_anchor", None)

        if anchor is None:
            try:
                tw = tide_window_now(entry_time, hours=float(os.getenv("TIDE_WINDOW_HOURS", "2.5")))
                if tw:
                    start, end = tw
                    anchor = start + (end - start) / 2
            except Exception:
                anchor = None

        base = anchor or entry_time
        new_deadline = base + timedelta(hours=new_hours)

        if getattr(p, "tp_time_deadline", None) != new_deadline:
            p.tp_time_deadline = new_deadline
            if hasattr(storage, "update_position"):
                try:
                    await storage.update_position(p)
                except TypeError:
                    storage.update_position(p)
            elif hasattr(storage, "save_position"):
                try:
                    await storage.save_position(p)
                except TypeError:
                    storage.save_position(p)
            updated += 1

    app.bot_data.setdefault("tp_time_change_log", []).append(
        {"ts": datetime.now().isoformat(timespec="seconds"), "new_hours": new_hours, "updated": updated}
    )
    return updated
