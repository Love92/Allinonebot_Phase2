# core/trade_executor.py
# -*- coding: utf-8 -*-
from __future__ import annotations

import os, math, asyncio, json, logging
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple

import ccxt  # type: ignore
from dotenv import load_dotenv

load_dotenv()
logging.getLogger(__name__).setLevel(logging.INFO)

# ======================== Public datatypes ========================
@dataclass
class OrderResult:
    ok: bool
    message: str


# ======================== Async helpers ==========================
def _to_thread(fn, *args, **kwargs):
    return asyncio.to_thread(fn, *args, **kwargs)


# ======================== ExchangeClient =========================
class ExchangeClient:
    """
    Wrapper CCXT đa sàn (Binance/OKX/BingX):
    - Chọn đúng thị trường futures (future/swap linear).
    - Chuẩn hóa symbol (OKX/BingX: BTC/USDT:USDT).
    - Fit khối lượng theo limits (min/max qty, stepSize, min/max notional).
    - Retry tự động khi gặp lỗi 'max quantity / position / notional'.
    """

    def __init__(
        self,
        exchange_id: Optional[str] = None,
        api_key: Optional[str] = None,
        api_secret: Optional[str] = None,
        testnet: Optional[bool] = None,
    ):
        # ENV fallback (tương thích code cũ)
        self.exchange_id = (exchange_id or os.getenv("EXCHANGE", "binanceusdm")).lower()
        self.api_key = api_key or os.getenv("API_KEY", "")
        self.api_secret = api_secret or os.getenv("API_SECRET", "")
        self.testnet = bool(testnet if testnet is not None else os.getenv("TESTNET", "false").lower() in ("1","true","yes","on"))

        ex_class = getattr(ccxt, self.exchange_id)
        params = {
            "apiKey": self.api_key,
            "secret": self.api_secret,
            "enableRateLimit": True,
        }

        # Binance USDM
        if self.exchange_id in ("binanceusdm", "binance"):
            params.setdefault("options", {})["defaultType"] = "future"
            if self.testnet:
                params["urls"] = {
                    "api": {
                        "fapiPublic": "https://testnet.binancefuture.com/fapi/v1",
                        "fapiPrivate": "https://testnet.binancefuture.com/fapi/v1",
                    }
                }

        # OKX Perp
        if self.exchange_id == "okx":
            params.setdefault("options", {})["defaultType"] = "swap"
            pw = os.getenv("OKX_PASSPHRASE", "")
            if pw:
                params["password"] = pw

        # BingX Perp (linear)
        if self.exchange_id == "bingx":
            opts = params.setdefault("options", {})
            opts["defaultType"] = "swap"
            try:
                opts["defaultSubType"] = "linear"
            except Exception:
                pass

        self.client = ex_class(params)
        self._markets_loaded = False

    # ---------- Markets / symbol ----------
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
        try:
            return self.client.market(self.normalize_symbol(symbol))
        except Exception:
            return {}

    # ---------- Low-level IO ----------
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
        mkt = await self._market(symbol)
        info = mkt.get("info", {}) if isinstance(mkt, dict) else {}
        limits = mkt.get("limits", {}) if isinstance(mkt, dict) else {}
        precision = mkt.get("precision", {}) if isinstance(mkt, dict) else {}

        min_qty = None
        max_qty = None
        step = precision.get("amount")

        # ccxt unified limits
        amount = limits.get("amount") or {}
        min_qty = amount.get("min", min_qty)
        max_qty = amount.get("max", max_qty)

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
                        try: max_cost = float(mx)
                        except: pass
        except Exception:
            pass

        q = float(qty)
        # Fit theo max_cost trước
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

        lot_step = float(os.getenv("LOT_STEP_FALLBACK", "0.001"))
        if q <= 0:
            q = float(min_qty or lot_step)

        meta = {
            "min_qty": min_qty, "max_qty": max_qty, "step": step,
            "min_cost": min_cost, "max_cost": max_cost,
        }
        return float(q), meta

    async def _place_market_with_retries(self, sym: str, side: str, qty: float, *, max_retries: int = 3):
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
                q = max(self._floor_step(q * 0.7, float(os.getenv("LOT_STEP_FALLBACK", "0.001"))), 0.0)
                attempt += 1
        raise last_err if last_err else Exception("create_order failed")

    # ---------- Account helpers ----------
    async def set_leverage(self, symbol: str, lev: int):
        try:
            sym = self.normalize_symbol(symbol)
            if hasattr(self.client, "set_leverage"):
                await self._io(self.client.set_leverage, int(lev), sym)
        except Exception as e:
            logging.warning("set_leverage failed: %s", e)

    async def ticker_price(self, symbol: str) -> float:
        try:
            t = await self._io(self.client.fetch_ticker, self.normalize_symbol(symbol))
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
            total = bal.get("total") or {}
            free = bal.get("free") or {}
            if key in total or key in free:
                return float(free.get(key, total.get(key, 0.0)))
        return float((bal.get("info") or {}).get("availableBalance", 0) or 0)

    # ---------- Position helpers ----------
    async def current_position(self, symbol: str) -> Tuple[Optional[bool], float]:
        """
        Returns (side_long: Optional[bool], qty: float) — qty = |contracts|.
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
                    try: amt = float(contracts)
                    except: amt = 0.0
                    side_long = (side == "long") if side in ("long", "short") else (amt > 0)
                elif pos_amt_info is not None:
                    try: amt = float(pos_amt_info)
                    except: amt = 0.0
                    side_long = amt > 0
                elif "amount" in p:
                    try: amt = float(p["amount"])
                    except: amt = 0.0
                    side_long = amt > 0

                if abs(amt) > 0:
                    break

            qty = abs(float(amt))
            if qty <= 0:
                return None, 0.0
            return side_long, qty
        except Exception:
            return None, 0.0

    async def fetch_open_orders(self, symbol: str):
        try:
            return await self._io(self.client.fetch_open_orders, self.normalize_symbol(symbol))
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
                    await self._io(self.client.cancel_order, oid, self.normalize_symbol(symbol))
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

    # ---------- Market entry/exit ----------
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
                try: await self.set_leverage(sym, int(leverage))
                except Exception: pass

            px = await self.ticker_price(sym)
            s = (side or "").upper()
            if s not in ("LONG", "SHORT"):
                return OrderResult(False, f"Invalid side: {side}")
            order_side = "buy" if s == "LONG" else "sell"

            q_fit, meta = await self._fit_qty(sym, float(qty), float(px or 0))
            if q_fit <= 0:
                return OrderResult(False, f"Order failed: qty_fit=0 (limits={meta})")

            entry = await self._place_market_with_retries(sym, order_side, q_fit)

            # Optional SL (reduceOnly)
            if stop_loss is not None:
                opp = "sell" if order_side == "buy" else "buy"
                params = {"reduceOnly": True}
                # Tùy sàn: tham số trigger
                if self.exchange_id in ("binanceusdm", "binance"):
                    params["stopPrice"] = float(stop_loss)
                elif self.exchange_id == "okx":
                    params["slTriggerPx"] = float(stop_loss)
                else:  # bingx & others
                    params["stopPrice"] = float(stop_loss)

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
        """
        Đóng FULL vị thế hiện tại (dùng reduceOnly, market).
        """
        try:
            sym = self.normalize_symbol(symbol)
            side_long, qty = await self.current_position(sym)
            if not qty or side_long is None:
                return OrderResult(True, "no_position")
            opp = "sell" if side_long else "buy"
            params = {"reduceOnly": True}
            _ = await self._place_market_with_retries(sym, opp, float(qty))
            return OrderResult(True, "closed_full")
        except Exception as e:
            return OrderResult(False, f"close_err:{e}")

    async def close_position_percent(self, symbol: str, percent: float) -> OrderResult:
        """
        Đóng phần trăm vị thế hiện tại (nếu sàn không hỗ trợ partial reduce,
        sẽ cố gắng tính qty và dùng market reduceOnly).
        """
        try:
            sym = self.normalize_symbol(symbol)
            side_long, qty = await self.current_position(sym)
            if not qty or side_long is None:
                return OrderResult(True, "no_position")

            pct = max(0.0, min(100.0, float(percent)))
            if pct >= 99.0:
                return await self.close_position(sym)

            target = float(qty) * (pct / 100.0)
            return await self.market_close_size(sym, target)
        except Exception as e:
            return OrderResult(False, f"close_percent_err:{e}")

    async def market_close_size(self, symbol: str, size: float) -> OrderResult:
        try:
            sym = self.normalize_symbol(symbol)
            side_long, qty = await self.current_position(sym)
            if not qty or side_long is None:
                return OrderResult(True, "no_position")
            size = max(0.0, min(float(size), float(qty)))
            if size <= 0:
                return OrderResult(True, "size=0")

            opp = "sell" if side_long else "buy"
            params = {"reduceOnly": True}
            _ = await self._place_market_with_retries(sym, opp, float(size))
            return OrderResult(True, f"closed_size={size}")
        except Exception as e:
            return OrderResult(False, f"market_close_size_err:{e}")


# ======================== Risk & SL helpers =======================
def calc_qty(balance_usdt: float, risk_percent: float, leverage: int, price: float) -> float:
    """
    Tính size contracts từ %risk * leverage.
    - risk_percent: % vốn muốn “đưa vào vị thế” (không phải SL risk).
    - leverage: đòn bẩy danh nghĩa.
    - price: giá hiện tại.
    """
    try:
        bal = max(0.0, float(balance_usdt))
        rp = max(0.0, float(risk_percent)) / 100.0
        lev = max(1.0, float(leverage))
        px = max(1e-9, float(price))
        notional = bal * rp * lev
        contracts = notional / px
        return max(0.0, contracts)
    except Exception:
        return 0.0


def auto_sl_by_leverage(side: str, entry_price: float, leverage: int, *, k: float = 0.75) -> float:
    """
    Ước lượng SL dựa trên leverage:
    - Khoảng chịu lỗ danh nghĩa ~ (1/leverage) * k
    - k < 1 để SL “gần” hơn mức thanh lý, mặc định 0.75
    """
    side = (side or "").upper()
    e = float(entry_price)
    lev = max(1.0, float(leverage))
    step = (1.0 / lev) * float(k)
    if side == "LONG":
        return e * (1.0 - step)
    elif side == "SHORT":
        return e * (1.0 + step)
    else:
        return e


# ======================== Multi-Account Close =====================
# Đọc danh sách account từ ENV
def _load_accounts_from_env() -> List[Dict[str, Any]]:
    raw = os.getenv("ACCOUNTS_JSON", "").strip()
    if not raw:
        return []
    try:
        data = json.loads(raw)
        if isinstance(data, list):
            return data
    except Exception:
        pass
    return []

async def _make_client_for_account(acc: Dict[str, Any]) -> ExchangeClient:
    """
    acc = {
      "name": "bingx_test",
      "exchange": "bingx",
      "api_key": "...",
      "api_secret": "...",
      "testnet": false,
      "pair": "BTC/USDT:USDT"
    }
    """
    try:
        cli = ExchangeClient(
            exchange_id=acc.get("exchange") or os.getenv("EXCHANGE", "binanceusdm"),
            api_key=acc.get("api_key") or os.getenv("API_KEY", ""),
            api_secret=acc.get("api_secret") or os.getenv("API_SECRET", ""),
            testnet=bool(acc.get("testnet", os.getenv("TESTNET", "false").lower() in ("1","true","yes","on"))),
        )
        return cli
    except TypeError:
        return ExchangeClient()

async def get_account_clients() -> List[Tuple[Dict[str, Any], ExchangeClient]]:
    accs = _load_accounts_from_env()
    out: List[Tuple[Dict[str, Any], ExchangeClient]] = []
    for acc in accs:
        try:
            cli = await _make_client_for_account(acc)
            out.append((acc, cli))
        except Exception:
            continue
    if not out:
        try:
            out.append(({"name": "default", "exchange": os.getenv("EXCHANGE","binanceusdm")}, ExchangeClient()))
        except Exception:
            pass
    return out

async def _close_percent(client: ExchangeClient, pair: str, pct: float) -> Dict[str, Any]:
    pct = max(0.0, min(100.0, float(pct)))
    try:
        res = await client.close_position_percent(pair, pct)
        return {"ok": bool(getattr(res, "ok", False)), "message": getattr(res, "message", str(res))}
    except Exception as e:
        return {"ok": False, "message": f"close_percent_err:{e}"}

async def close_position_on_account(account_name: str, pair: str, percent: float = 100.0) -> Dict[str, Any]:
    clis = await get_account_clients()
    for acc, cli in clis:
        if str(acc.get("name","")).lower() == str(account_name).lower():
            return await _close_percent(cli, pair, percent)
    return {"ok": False, "message": f"account_not_found:{account_name}"}

async def close_position_on_all(pair: str, percent: float = 100.0) -> List[Dict[str, Any]]:
    clis = await get_account_clients()
    tasks = [_close_percent(cli, pair, percent) for _, cli in clis]
    return await asyncio.gather(*tasks, return_exceptions=False)
