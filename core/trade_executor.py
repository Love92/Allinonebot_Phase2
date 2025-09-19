# ----------------------- core/trade_executor.py -----------------------
from __future__ import annotations
import asyncio, logging, os
from dataclasses import dataclass
from typing import Optional

from dotenv import load_dotenv
import ccxt  # type: ignore

# Giữ import theo project của a
from config.settings import EXCHANGE_ID, API_KEY, API_SECRET, TESTNET

load_dotenv()

@dataclass
class OrderResult:
    ok: bool
    message: str


def _to_thread(fn, *args, **kwargs):
    return asyncio.to_thread(fn, *args, **kwargs)


class ExchangeClient:
    """
    Thin wrapper around ccxt to unify a few ops we need in the auto engine.
    - Hỗ trợ khởi tạo theo từng account (exchange_id/api_key/api_secret/testnet)
      nhưng vẫn tương thích cũ nếu gọi không truyền tham số.
    - Chuẩn hóa symbol theo sàn: OKX/BingX perp USDT-M dùng 'BTC/USDT:USDT'.
    - Ưu tiên reduceOnly cho exit orders.
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
        # Binance futures default & testnet
        if self.exchange_id in ("binanceusdm", "binance"):
            params.setdefault("options", {})["defaultType"] = "future"
            if self.testnet:
                params["urls"] = {
                    "api": {
                        "fapiPublic": "https://testnet.binancefuture.com/fapi/v1",
                        "fapiPrivate": "https://testnet.binancefuture.com/fapi/v1",
                    }
                }
        self.client = ex_class(params)

    # ------------- symbol normalize -------------
    def normalize_symbol(self, pair: str) -> str:
        p = (pair or "").strip().upper()
        if self.exchange_id in ("okx", "bingx"):
            if p.endswith("/USDT") and ":USDT" not in p:
                p = p.replace("/USDT", "/USDT:USDT")
        return p

    # ------------- helpers -------------
    async def _io(self, func, *args, **kwargs):
        return await _to_thread(func, *args, **kwargs)

    # ---------------- Account helpers ----------------
    async def set_leverage(self, symbol: str, lev: int):
        try:
            sym = self.normalize_symbol(symbol)
            if hasattr(self.client, "set_leverage"):
                await self._io(self.client.set_leverage, lev, sym)
        except Exception as e:
            logging.warning("set_leverage failed: %s", e)

    async def balance_usdt(self) -> float:
        try:
            bal = await self._io(self.client.fetch_balance)
        except Exception:
            return 0.0
        for key in ("USDT", "usdt", "USDC", "BUSD"):
            if key in (bal.get("total") or {}):
                return float((bal.get("free") or {}).get(key, (bal["total"] or {})[key]))
        return float((bal.get("info") or {}).get("availableBalance", 0) or 0)

    # ---------------- Position helpers ----------------
    async def current_position(self, symbol: str):
        """
        Returns (side_long: Optional[bool], qty: float) for symbol.
        side_long=None when no position; qty=0.0 means flat.
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
                    if side in ("long", "short"):
                        side_long = side == "long"
                    else:
                        side_long = amt > 0
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
                has_stop = any(k in info for k in ("stopPrice", "triggerPrice", "stopPx", "tpTriggerPx", "slTriggerPx"))
                is_tp_sl = ("stop" in typ) or ("take" in typ) or ("tp" in typ) or has_stop
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

            s = (side or "").upper()
            if s not in ("LONG", "SHORT"):
                return OrderResult(False, f"Invalid side: {side}")

            order_side = "buy" if s == "LONG" else "sell"
            entry = await self._io(self.client.create_order, sym, "market", order_side, qty)

            if stop_loss is not None:
                opp = "sell" if order_side == "buy" else "buy"
                params = {"reduceOnly": True, "stopPrice": float(stop_loss)}
                # OKX một số trường hợp cần triggerPx
                if self.exchange_id == "okx":
                    params["slTriggerPx"] = float(stop_loss)
                try:
                    _ = await self._io(self.client.create_order, sym, "stop_market", opp, qty, None, params)
                except Exception as e:
                    # Thử unified alt
                    try:
                        _ = await self._io(self.client.create_order, sym, "STOP", opp, qty, None, params)
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

    async def market_with_sl_tp(self, symbol: str, side_long: bool, qty: float, sl: float, tp: float) -> OrderResult:
        try:
            sym = self.normalize_symbol(symbol)
            side = "buy" if side_long else "sell"
            entry = await self._io(self.client.create_order, sym, "market", side, qty)

            params = {"reduceOnly": True}
            opp = "sell" if side == "buy" else "buy"

            # SL
            try:
                sl_params = {**params, "stopPrice": float(sl)}
                if self.exchange_id == "okx":
                    sl_params["slTriggerPx"] = float(sl)
                try:
                    _ = await self._io(self.client.create_order, sym, "stop_market", opp, qty, None, sl_params)
                except Exception:
                    _ = await self._io(self.client.create_order, sym, "STOP", opp, qty, None, sl_params)
            except Exception:
                pass

            # TP
            try:
                tp_params = {**params, "stopPrice": float(tp)}
                if self.exchange_id == "okx":
                    tp_params["tpTriggerPx"] = float(tp)
                try:
                    _ = await self._io(self.client.create_order, sym, "take_profit_market", opp, qty, None, tp_params)
                except Exception:
                    _ = await self._io(self.client.create_order, sym, "TAKE_PROFIT", opp, qty, None, tp_params)
            except Exception:
                pass

            return OrderResult(True, f"Live order placed: entry={entry.get('id')}")
        except Exception as e:
            return OrderResult(False, f"Order failed: {e}")

    # ---------------- Convenience ----------------
    async def close_position_pct(self, symbol: str, pct: float) -> OrderResult:
        try:
            sym = self.normalize_symbol(symbol)
            side_long, qty = await self.current_position(sym)
            if (side_long is None and qty == 0) or qty <= 0:
                return OrderResult(True, "Không có vị thế mở.")

            pct = max(0.0, min(100.0, float(pct)))
            close_qty = qty if pct >= 99.5 else qty * (pct / 100.0)
            if close_qty <= 0:
                return OrderResult(False, "Khối lượng đóng = 0.")

            side = "sell" if side_long else "buy"
            params = {"reduceOnly": True}
            order = await self._io(self.client.create_order, sym, "market", side, close_qty, None, params)
            oid = (order or {}).get("id", "N/A")

            await asyncio.sleep(0.7)
            side_long2, qty2 = await self.current_position(sym)
            if pct >= 99.5 or (qty2 is not None and qty2 <= 1e-12):
                _ = await self.cancel_all_orders_symbol(sym)

            return OrderResult(True, f"Đã close {pct:.0f}% vị thế: {sym} qty={close_qty} (orderId={oid})")
        except Exception as e:
            return OrderResult(False, f"Close thất bại: {e}")


# ----------- sizing helpers (giữ nguyên) ---------------
def calc_qty(balance_usdt: float, risk_percent: float, leverage: int, entry_price: float, lot_step: float = 0.001) -> float:
    notional = balance_usdt * (risk_percent / 100.0) * leverage
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
