# ----------------------- core/trade_executor.py -----------------------
from __future__ import annotations
import asyncio, logging, math, os, json
from dataclasses import dataclass
from typing import Optional, Tuple, List, Dict, Any
from typing import Literal


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


# ===================== Exchange Client =====================
class ExchangeClient:
    """
    CCXT wrapper đa sàn:
    - Binance USDM / OKX / BingX (USDT-margined swap)
    - Chuẩn hoá symbol (OKX/BingX cần BTC/USDT:USDT)
    - Fit qty theo min/max qty, stepSize, min/max notional
    - Tự co size & retry khi vướng hạn mức
    """

    # ---------- init ----------
    def __init__(
        self,
        exchange_id: Optional[str] = None,
        api_key: Optional[str] = None,
        api_secret: Optional[str] = None,
        testnet: Optional[bool] = None,
        **kwargs: Any,  # giữ tương thích nơi khác có thể truyền account_name=...
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
        """
        Chuẩn hoá hướng:
        - bool: giữ nguyên
        - int/float: >0 → LONG, <=0 → SHORT
        - str: long/buy/true/1/+1/y → LONG; short/sell/false/0/-1/n → SHORT
        - mặc định False (SHORT) nếu không nhận diện được
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

        # raw filters (Binance…)
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
        return (float(q), meta)

    async def _place_market_with_retries(self, sym: str, side: str, qty: float, *, max_retries: int = 3):
        """
        Tạo lệnh market; nếu lỗi do limit → giảm size (0.7×) và thử lại.
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

    # ---------- account / market data ----------
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
                    await self._io(self.client.create_order, sym, "stop_market", opp, q_fit, None, params)
                except Exception as e:
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
        """
        try:
            sym = self.normalize_symbol(symbol)

            # Chuẩn hoá hướng
            is_long = self._as_side_long(side_long)
            side = "buy" if is_long else "sell"

            # Giá hiện tại để fit notional/limits
            px = await self.ticker_price(sym)

            # Fit qty theo limits
            q_fit, meta = await self._fit_qty(sym, float(qty), float(px or 0))
            if q_fit <= 0:
                return OrderResult(False, f"entry_error: qty_fit=0 (limits={meta})")

            # Vào lệnh
            entry = await self._place_market_with_retries(sym, side, q_fit)

            # Đặt SL/TP reduceOnly
            opp = "sell" if side == "buy" else "buy"

            if sl_price is not None:
                try:
                    sp = float(sl_price)
                    params = {"reduceOnly": True, "stopPrice": sp}
                    if self.exchange_id == "okx":
                        params["slTriggerPx"] = sp
                    await self._io(self.client.create_order, sym, "stop_market", opp, q_fit, None, params)
                except Exception as e:
                    logging.warning("Create SL order failed: %s", e)

            if tp_price is not None:
                try:
                    tp = float(tp_price)
                    params = {"reduceOnly": True, "stopPrice": tp}
                    tptype = "take_profit_market"  # vài sàn map nội bộ sang stop_market
                    await self._io(self.client.create_order, sym, tptype, opp, q_fit, None, params)
                except Exception as e:
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
                await self._io(self.client.create_order, sym, "market", side, close_qty, None, {"reduceOnly": True})
            except Exception as e:
                if self._should_shrink_on_error(e):
                    close_qty = max(self._floor_step(close_qty * 0.7, lot_step), 0.0)
                    if close_qty > 0:
                        await self._io(self.client.create_order, sym, "market", side, close_qty, None, {"reduceOnly": True})
                else:
                    raise

            return OrderResult(True, f"Closed {pct:.0f}% position.")
        except Exception as e:
            return OrderResult(False, f"close_percent failed: {e}")


# ===================== Multi-account /close helpers =====================
async def close_position_on_account(account_name: str, pair: str, percent: float) -> Dict[str, Any]:
    """
    Đóng vị thế trên 1 account (map từ ENV/config hiện tại).
    Return: {"ok": bool, "message": str}
    """
    try:
        exid = os.getenv("EXCHANGE_ID", EXCHANGE_ID)
        api = os.getenv("API_KEY", API_KEY)
        sec = os.getenv("API_SECRET", API_SECRET)
        testnet = (os.getenv("TESTNET", "false").strip().lower() in ("1", "true", "yes", "on"))

        cli = ExchangeClient(exid, api, sec, testnet)
        res = await cli.close_percent(pair, percent)
        return {"ok": bool(res.ok), "message": res.message}
    except Exception as e:
        return {"ok": False, "message": f"{e}"}


async def close_position_on_all(pair: str, percent: float) -> List[Dict[str, Any]]:
    """
    Đóng vị thế trên tất cả account (SINGLE_ACCOUNT + ACCOUNTS_JSON).
    Return: list[{"ok": bool, "message": str}]
    """
    from config import settings as _S

    results: List[Dict[str, Any]] = []

    # Lấy danh sách account
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
        exid = str(acc.get("exchange") or EXCHANGE_ID).lower()
        key = (exid, acc.get("api_key") or API_KEY)
        if key in seen:
            continue
        seen.add(key)
        uniq.append(acc)

    # Nếu không có account cấu hình → dùng ENV mặc định
    if not uniq:
        r = await close_position_on_account("default", pair, percent)
        results.append(r)
        return results

    # Đóng trên từng account
    for acc in uniq:
        try:
            exid = str(acc.get("exchange") or EXCHANGE_ID).lower()
            api = acc.get("api_key") or API_KEY
            sec = acc.get("api_secret") or API_SECRET
            testnet = bool(acc.get("testnet", TESTNET))
            name = acc.get("name", "default")
            sym = acc.get("pair", pair)

            cli = ExchangeClient(exid, api, sec, testnet)
            res = await cli.close_percent(sym, percent)
            results.append({"ok": bool(res.ok), "message": f"{name} | {exid} → {res.message}"})
        except Exception as e:
            results.append({"ok": False, "message": f"{acc.get('name','?')} | {e}"})

    return results

# ========== [ADD] Shim adapters if not provided elsewhere ==========
try:
    # Nếu a đã có 2 hàm này ở module khác, import ở đây và BỎ các shim bên dưới.
    from core.order_ops import open_single_account_order, open_multi_account_orders  # type: ignore
except Exception:
    async def open_single_account_order(app, storage, *, symbol: str, side: str,
                                       qty_cfg: dict, risk_cfg: dict, meta: dict):
        """
        Adapter tối thiểu cho SINGLE ACCOUNT.
        Trả về: (ok: bool, info: dict với 'entry_id' nếu mở thành công)
        """
        from config import settings as _S
        exid = getattr(_S, "EXCHANGE_ID", EXCHANGE_ID)
        api  = getattr(_S, "API_KEY", API_KEY)
        sec  = getattr(_S, "API_SECRET", API_SECRET)
        tnet = getattr(_S, "TESTNET", TESTNET)

        cli = ExchangeClient(exid, api, sec, tnet)

        # Lấy giá hiện tại
        px = await cli.ticker_price(symbol)
        if px <= 0:
            return False, {"error": "ticker_price<=0"}

        # Tính qty:
        qty = float(qty_cfg.get("qty") or 0)
        if qty <= 0:
            # fallback theo balance, risk%, lev
            bal = await cli.balance_usdt()
            risk_percent = float(risk_cfg.get("risk_percent", getattr(_S, "RISK_PERCENT", 1.0)))
            lev = int(risk_cfg.get("leverage", getattr(_S, "LEVERAGE", 10)))
            qty = calc_qty(bal, risk_percent, lev, px)

        if qty <= 0:
            return False, {"error": "qty<=0"}

        # SL/TP (nếu chưa có thì auto theo leverage)
        lev = int(risk_cfg.get("leverage", getattr(_S, "LEVERAGE", 10)))
        sl = qty_cfg.get("sl")
        tp = qty_cfg.get("tp")
        if sl is None or tp is None:
            sl, tp = auto_sl_by_leverage(px, side, lev)

        is_long = True if str(side).upper() == "LONG" else False
        res = await cli.market_with_sl_tp(symbol, is_long, qty, sl, tp)

        if not res.ok:
            return False, {"error": res.message}

        entry = (res.data or {}).get("entry", {})
        entry_id = entry.get("id")
        return True, {"opened": True, "entry_id": entry_id, "qty": qty, "price": px, "sl": sl, "tp": tp}

    async def open_multi_account_orders(app, storage, *, symbol: str, side: str,
                                       accounts_cfg: dict, qty_cfg: dict, risk_cfg: dict, meta: dict):
        """
        Adapter tối thiểu cho MULTI ACCOUNT.
        Trả về: (ok: bool, mapping account_name -> info dict)
        """
        from config import settings as _S

        # Lấy danh sách account từ settings.ACCOUNTS hoặc ENV ACCOUNTS_JSON
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
                api  = acc.get("api_key") or API_KEY
                sec  = acc.get("api_secret") or API_SECRET
                tnet = bool(acc.get("testnet", TESTNET))
                pair = acc.get("pair", symbol)

                cli = ExchangeClient(exid, api, sec, tnet)

                # giá hiện tại
                px = await cli.ticker_price(pair)
                if px <= 0:
                    results[name] = {"opened": False, "error": "ticker_price<=0"}
                    continue

                # qty theo acc (ưu tiên qty_cfg['qty_per_account'] nếu có)
                qty = float(qty_cfg.get("qty_per_account") or qty_cfg.get("qty") or 0)
                if qty <= 0:
                    bal = await cli.balance_usdt()
                    risk_percent = float(risk_cfg.get("risk_percent", acc.get("risk_percent", getattr(_S, "RISK_PERCENT", 1.0))))
                    lev = int(risk_cfg.get("leverage", acc.get("leverage", getattr(_S, "LEVERAGE", 10))))
                    qty = calc_qty(bal, risk_percent, lev, px)

                if qty <= 0:
                    results[name] = {"opened": False, "error": "qty<=0"}
                    continue

                # SL/TP
                lev = int(risk_cfg.get("leverage", acc.get("leverage", getattr(_S, "LEVERAGE", 10))))
                sl = qty_cfg.get("sl")
                tp = qty_cfg.get("tp")
                if sl is None or tp is None:
                    sl, tp = auto_sl_by_leverage(px, side, lev)

                is_long = True if str(side).upper() == "LONG" else False
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
        
# ========== [ADD] Unified execute hub (no breaking change) ==========

async def execute_order_flow(app, storage, *,
                            symbol: str, side: Literal["LONG","SHORT"],
                            qty_cfg: dict, risk_cfg: dict, accounts_cfg: dict,
                            meta: dict, origin: Literal["AUTO","MANUAL","ORDER"]) -> Tuple[bool, dict]:
    """
    Return:
      - opened_real: bool
      - result: { 'entry_ids': [...], 'per_account': {...}, 'origin': origin, 'side': side, 'symbol': symbol }
    Ghi chú:
      - Không tự broadcast ở đây; caller sẽ dùng formatter để gửi boardcard EXECUTED (đồng bộ 1 nguồn).
    """
    entry_ids = []
    per_account = {}

    # --- tài khoản đơn (nếu project của anh không dùng, shim sẽ tự xử lý) ---
    try:
        ok, info = await open_single_account_order(
            app, storage, symbol=symbol, side=side,
            qty_cfg=qty_cfg, risk_cfg=risk_cfg, meta=meta
        )
        if ok and isinstance(info, dict) and "entry_id" in info:
            entry_ids.append(info["entry_id"])
            per_account["single"] = info
    except Exception as e:
        per_account["single_error"] = str(e)

    # --- đa tài khoản (nếu bật) ---
    try:
        if accounts_cfg and accounts_cfg.get("enabled"):
            ok2, multi = await open_multi_account_orders(
                app, storage, symbol=symbol, side=side,
                accounts_cfg=accounts_cfg, qty_cfg=qty_cfg, risk_cfg=risk_cfg, meta=meta
            )
            per_account["multi"] = multi
            for acc_name, d in (multi or {}).items():
                if isinstance(d, dict) and d.get("opened") and "entry_id" in d:
                    entry_ids.append(d["entry_id"])
    except Exception as e:
        per_account["multi_error"] = str(e)

    opened_real = len(entry_ids) > 0
    result = {
        "entry_ids": entry_ids,
        "per_account": per_account,
        "origin": origin,
        "side": side,
        "symbol": symbol,
    }
    return opened_real, result

# =====================================================================

