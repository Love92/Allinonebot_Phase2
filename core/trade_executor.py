# ----------------------- core/trade_executor.py -----------------------
from __future__ import annotations
import asyncio, logging, math, os
from dataclasses import dataclass
from typing import Optional, Tuple, List, Dict, Any

from dotenv import load_dotenv
import ccxt  # type: ignore

# ========== Load basic settings ==========
from config.settings import EXCHANGE_ID, API_KEY, API_SECRET, TESTNET

load_dotenv()
logging.getLogger(__name__).setLevel(logging.INFO)


# ========== Simple result container ==========
@dataclass
class OrderResult:
    ok: bool
    message: str
    data: Optional[dict] = None


# ========== Async helper ==========
def _to_thread(fn, *args, **kwargs):
    return asyncio.to_thread(fn, *args, **kwargs)


# ========== Risk sizing (SINGLE calc_qty) ==========
def calc_qty(
    balance_usdt: float,
    risk_percent: float,
    leverage: int,
    entry_price: float,
    min_qty: float = 0.0,
) -> float:
    """
    Tính số lượng hợp đồng theo:
        notional = balance * (risk%/100) * leverage
        qty = notional / entry_price
    Có ép min_qty (nếu có) và bảo vệ khi thiếu dữ liệu.
    """
    try:
        bal = max(0.0, float(balance_usdt))
        rp = max(0.0, float(risk_percent)) / 100.0
        lev = max(1, int(leverage))
        px = float(entry_price)
        if px <= 0 or bal <= 0 or rp <= 0:
            return 0.0
        notional = bal * rp * lev
        qty = notional / px
        if min_qty and qty < min_qty:
            qty = min_qty
        return float(qty)
    except Exception:
        return 0.0


# ========== Auto SL/TP helper ==========
def auto_sl_by_leverage(entry: float, side: str, lev: int) -> Tuple[float, float]:
    """
    Quy tắc mặc định: khoảng SL/TP theo đòn bẩy (giản lược, đủ dùng).
    Ví dụ:
      - LONG: SL ~ -1/lev * k; TP ~ +2/lev * k
      - SHORT: đối xứng
    """
    try:
        entry = float(entry)
        lev = max(1, int(lev))
        k = 100.0  # hệ số co giãn (tuỳ chỉnh nhanh)
        pct_sl = 1.0 / lev
        pct_tp = 2.0 / lev
        if str(side).upper() == "LONG":
            return (entry * (1 - pct_sl / k), entry * (1 + pct_tp / k))
        else:
            return (entry * (1 + pct_sl / k), entry * (1 - pct_tp / k))
    except Exception:
        # fallback 1% / 2%
        if str(side).upper() == "LONG":
            return (entry * 0.99, entry * 1.02)
        else:
            return (entry * 1.01, entry * 0.98)


# ========== Exchange client (CCXT wrapper) ==========
class ExchangeClient:
    """
    Wrapper CCXT đa sàn (Binance USDM / OKX / BingX):
    - Chọn đúng thị trường futures (future / swap linear).
    - Chuẩn hoá symbol (OKX/BingX cần dạng BTC/USDT:USDT).
    - Fit khối lượng theo limits: min/max qty, stepSize, min/max notional.
    - Tự giảm size & retry khi gặp lỗi “max quantity / max position value / notional”.
    """

    def __init__(
        self,
        exchange_id: Optional[str] = None,
        api_key: Optional[str] = None,
        api_secret: Optional[str] = None,
        testnet: Optional[bool] = None,
        **kwargs: Any,  # giữ tương thích nơi khác gọi bằng account_name=...
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

    # ----- Markets / Symbol -----
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

    # ----- Generic async I/O -----
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

    # --- Helper: chuẩn hoá side_long từ bool/str/float/int ---
    @staticmethod
    def _as_side_long(v) -> bool:
        """
        Map:
        - bool: giữ nguyên
        - str: 'long'/'buy'/'1'/'true' => True; 'short'/'sell'/'0'/'false'/'-1' => False
        - int/float: >0 => True (LONG), <=0 => False (SHORT)
        - mặc định: False
        """
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

    # ----- Fit quantity to exchange limits -----
    async def _fit_qty(self, symbol: str, qty: float, price: float) -> Tuple[float, dict]:
        """
        Trả về (qty_fit, meta_limits).
        Xử lý min/max qty, stepSize, min/max notional (cost).
        """
        mkt = await self._market(symbol)
        info = mkt.get("info", {}) if isinstance(mkt, dict) else {}
        limits = mkt.get("limits", {}) if isinstance(mkt, dict) else {}
        precision = mkt.get("precision", {}) if isinstance(mkt, dict) else {}

        min_qty = None
        max_qty = None
        step = precision.get("amount")

        # unified limits
        amt = limits.get("amount") or {}
        min_qty = amt.get("min", min_qty)
        max_qty = amt.get("max", max_qty)

        min_cost = None
        max_cost = None
        cost = limits.get("cost") or {}
        min_cost = cost.get("min", min_cost)
        max_cost = cost.get("max", max_cost)

        # raw Binance filters
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

        # Max notional → co bớt
        if max_cost and price:
            q = min(q, float(max_cost) / float(price))

        # Max qty
        if max_qty:
            q = min(q, float(max_qty))

        # Snap theo step
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

    # ----- Place market with retries (shrink on limit errors) -----
    async def _place_market_with_retries(
        self, sym: str, side: str, qty: float, *, max_retries: int = 3
    ):
        """
        Tạo lệnh market, nếu lỗi do limit thì tự giảm size và thử lại.
        """
        attempt = 0
        last_err: Optional[Exception] = None
        q = float(qty)
        lot_step = float(os.getenv("LOT_STEP_FALLBACK", "0.001"))

        while attempt <= max_retries:
            try:
                return await self._io(self.client.create_order, sym, "market", side, q)
            except Exception as e:
                last_err = e
                if not self._should_shrink_on_error(e):
                    break
                q = max(self._floor_step(q * 0.7, lot_step), 0.0)
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
        """
        Tổng/free USDT (ưu tiên free). Có fallback theo trường info/availableBalance.
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

    # Alias cho code cũ:
    async def get_balance_quote(self) -> float:
        return await self.balance_usdt()

    # ---------------- Position helpers ----------------
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
        """
        Vào lệnh thị trường. Tự fit qty theo limit; tự đặt SL reduceOnly nếu được truyền.
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
                    logging.warning("Create SL order failed: %s", e)

            return OrderResult(True, f"Live order placed: entry={entry.get('id')}", {"entry": entry})
        except Exception as e:
            return OrderResult(False, f"open_market failed: {e}")

    async def market_with_sl_tp(
        self,
        symbol: str,
        side_long,                      # chấp nhận bool/str/float/int
        qty: float,
        sl_price: Optional[float],
        tp_price: Optional[float],
    ) -> OrderResult:
        """
        Lệnh market + đặt SL/TP reduceOnly (nếu sàn hỗ trợ).
        - 'side_long' có thể là bool/str/float/int, sẽ được chuẩn hoá về bool.
        """
        try:
            sym = self.normalize_symbol(symbol)

            # Chuẩn hoá hướng
            is_long = self._as_side_long(side_long)
            side = "buy" if is_long else "sell"

            # Giá hiện tại (để fit notional/limits)
            px = await self.ticker_price(sym)

            # Fit khối lượng theo limits (min/max qty, step, min/max notional)
            q_fit, meta = await self._fit_qty(sym, float(qty), float(px or 0))
            if q_fit <= 0:
                return OrderResult(False, f"entry_error: qty_fit=0 (limits={meta})")

            # Vào lệnh thị trường (auto shrink nếu vướng giới hạn)
            entry = await self._place_market_with_retries(sym, side, q_fit)

            # Post SL/TP reduceOnly
            opp = "sell" if side == "buy" else "buy"

            # SL
            if sl_price is not None:
                try:
                    sp = float(sl_price)
                    params = {"reduceOnly": True, "stopPrice": sp}
                    if self.exchange_id == "okx":
                        # OKX yêu cầu trigger riêng
                        params["slTriggerPx"] = sp
                    await self._io(self.client.create_order, sym, "stop_market", opp, q_fit, None, params)
                except Exception as e:
                    logging.warning("Create SL order failed: %s", e)

            # TP
            if tp_price is not None:
                try:
                    tp = float(tp_price)
                    params = {"reduceOnly": True, "stopPrice": tp}
                    tptype = "take_profit_market"  # một số sàn map nội bộ sang stop_market
                    await self._io(self.client.create_order, sym, tptype, opp, q_fit, None, params)
                except Exception as e:
                    logging.warning("Create TP order failed: %s", e)

            return OrderResult(True, f"Live order placed: entry={entry.get('id')}",
                               {"entry": entry})

        except Exception as e:
            # Chuẩn hoá thông điệp lỗi quen thuộc
            msg = str(e)
            if "min" in msg.lower() and "amount" in msg.lower():
                return OrderResult(False, f"entry_error:{msg}")
            return OrderResult(False, f"entry_error:{msg}")

    async def close_percent(self, symbol: str, percent: float) -> OrderResult:
        """
        Đóng percent% vị thế hiện có (giữ chiều ngược lại).
        100% sẽ close hết. Giữ reduceOnly.
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

            side = "sell" if side_long else "buy"
            try:
                _ = await self._io(self.client.create_order, sym, "market", side, close_qty, None, {"reduceOnly": True})
            except Exception as e:
                # nếu lỗi do limit -> thử giảm size
                if self._should_shrink_on_error(e):
                    close_qty = max(self._floor_step(close_qty * 0.7, lot_step), 0.0)
                    if close_qty > 0:
                        _ = await self._io(self.client.create_order, sym, "market", side, close_qty, None, {"reduceOnly": True})
                else:
                    raise

            return OrderResult(True, f"Closed {pct:.0f}% position.")
        except Exception as e:
            return OrderResult(False, f"close_percent failed: {e}")


# ========== Multi-account close helpers (for /close_cmd) ==========
async def close_position_on_account(account_name: str, pair: str, percent: float) -> Dict[str, Any]:
    """
    Đóng vị thế trên 1 account (theo env/config hiện tại).
    • Return: {"ok": bool, "message": str}
    """
    try:
        # Với bản hiện tại, account_name chỉ để hiển thị/log.
        # Nếu bạn có nhiều key/secret theo account, hãy map ở đây (ENV hoặc settings).
        exid = os.getenv("EXCHANGE_ID", EXCHANGE_ID)
        api = os.getenv("API_KEY", API_KEY)
        sec = os.getenv("API_SECRET", API_SECRET)
        testnet = (os.getenv("TESTNET", "false").lower() in ("1","true","yes","on"))

        cli = ExchangeClient(exid, api, sec, testnet)
        res = await cli.close_percent(pair, percent)
        return {"ok": bool(res.ok), "message": res.message}
    except Exception as e:
        return {"ok": False, "message": f"{e}"}


async def close_position_on_all(pair: str, percent: float) -> List[Dict[str, Any]]:
    """
    Đóng vị thế trên TẤT CẢ account (SINGLE_ACCOUNT + ACCOUNTS_JSON nếu có).
    • Return: list[{"ok": bool, "message": str}]
    """
    import json
    from config import settings as _S

    results: List[Dict[str, Any]] = []

    # danh sách account từ settings / ENV
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

    SINGLE_ACCOUNT = getattr(_S, "SINGLE_ACCOUNT", None)
    base = ([SINGLE_ACCOUNT] if SINGLE_ACCOUNT else []) + ACCOUNTS

    uniq: List[dict] = []
    seen = set()
    for acc in base:
        if not isinstance(acc, dict):
            continue
        exid = str(acc.get("exchange", "")).lower()
        key = (exid, acc.get("api_key", ""))
        if key in seen:
            continue
        seen.add(key)
        uniq.append(acc)

    if not uniq:
        # Fallback: dùng account mặc định của ENV hiện tại
        r = await close_position_on_account("default", pair, percent)
        results.append(r)
        return results

    for acc in uniq:
        try:
            exid = str(acc.get("exchange") or EXCHANGE_ID).lower()
            api = acc.get("api_key") or API_KEY
            sec = acc.get("api_secret") or API_SECRET
            testnet = bool(acc.get("testnet", TESTNET))
            name = acc.get("name", "default")

            cli = ExchangeClient(exid, api, sec, testnet)
            res = await cli.close_percent(acc.get("pair", pair), percent)
            results.append({"ok": bool(res.ok), "message": f"{name} | {exid} → {res.message}"})
        except Exception as e:
            results.append({"ok": False, "message": f"{acc.get('name','?')} | {e}"})

    return results
