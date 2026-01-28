#!/usr/bin/env python3
# -*- coding: utf-8 -*-

# import shioaji_patch  # Optional: Enable this if you encounter IndexError in fetch_contracts
import time
import uuid
import logging
import os
import threading
import json
import sys
from enum import Enum, auto
from typing import Dict, List, Optional, Any, Tuple, Callable
from dataclasses import dataclass, field
import traceback
from dotenv import load_dotenv

import shioaji as sj
from shioaji import constant as sj_constant
import ssl
import math

# --- SSL Context Hack for macOS ---
try:
    _create_unverified_https_context = ssl._create_unverified_context
except AttributeError:
    pass
else:
    ssl._create_default_https_context = _create_unverified_https_context
# ----------------------------------


# ============================================================
# ROBUST CONTRACT RESOLVERS (Fix: Futures contract lookup/sub)
# ============================================================

def resolve_stock_contract(api: sj.Shioaji, code: str):
    """
    Robust stock contract resolver.
    """
    try:
        c = api.Contracts.Stocks.get(code)
        if c:
            return c
    except Exception:
        pass
    try:
        return api.Contracts.Stocks[code]
    except Exception:
        return None


def _iter_contract_container(container):
    """
    Best-effort iterator over Shioaji contract container nodes.
    Yields contract objects that have attribute 'code'.
    """
    if container is None:
        return
    # If container itself is a contract
    if hasattr(container, "code") and getattr(container, "code", ""):
        yield container
        return

    # dict-like
    try:
        keys = list(container.keys())
    except Exception:
        keys = None

    if keys is not None:
        for k in keys:
            try:
                v = container.get(k)
            except Exception:
                v = None
            if v is None:
                continue
            yield from _iter_contract_container(v)
        return

    # iterable-like
    try:
        for item in container:
            if isinstance(item, tuple) and len(item) >= 2:
                item = item[1]
            if item is None:
                continue
            yield from _iter_contract_container(item)
    except Exception:
        return


def iter_future_contracts(api: sj.Shioaji):
    """
    Iterate all future contracts (futures only) in Shioaji Contracts tree.
    """
    fut_root = api.Contracts.Futures
    for name in dir(fut_root):
        if name.startswith("_"):
            continue
        node = getattr(fut_root, name, None)
        if node is None:
            continue
        if callable(node):
            continue
        yield from _iter_contract_container(node)


def resolve_future_contract(api: sj.Shioaji, code: str):
    """
    Robust future contract resolver by walking the contract tree.
    """
    fut_root = api.Contracts.Futures

    # 1) direct .get if supported
    try:
        c = fut_root.get(code)
        if c:
            return c
    except Exception:
        pass

    # 2) full walk
    for c in iter_future_contracts(api):
        try:
            if getattr(c, "code", "") == code:
                return c
        except Exception:
            pass

    return None


def iter_option_contracts(api: sj.Shioaji):
    """
    Iterate all option contracts in Shioaji Contracts tree (best effort).
    Shioaji may expose options under api.Contracts.Options in some versions.
    """
    # Primary: api.Contracts.Options
    opt_root = getattr(api.Contracts, "Options", None)
    if opt_root is not None:
        for name in dir(opt_root):
            if name.startswith("_"):
                continue
            node = getattr(opt_root, name, None)
            if node is None:
                continue
            if callable(node):
                continue
            yield from _iter_contract_container(node)
        return

    # Fallback: Some versions nest options under Futures (futopt)
    # We'll include contracts that look like options by checking attributes.
    try:
        for c in iter_future_contracts(api):
            # Heuristic: options typically have 'strike_price' or 'option_right'
            if hasattr(c, "strike_price") or hasattr(c, "option_right") or hasattr(c, "cp"):
                yield c
    except Exception:
        return


def resolve_option_contract(api: sj.Shioaji, code: str):
    """
    Robust option contract resolver by walking the options tree.
    """
    # 1) direct .get if supported
    opt_root = getattr(api.Contracts, "Options", None)
    if opt_root is not None:
        try:
            c = opt_root.get(code)
            if c:
                return c
        except Exception:
            pass

    # 2) full walk
    for c in iter_option_contracts(api):
        try:
            if getattr(c, "code", "") == code:
                return c
        except Exception:
            pass

    return None


# ============================================================
# Helpers: snapshot volume extraction & contract selection
# ============================================================

def _snap_volume(snap: Any) -> int:
    """
    Best-effort volume extraction from snapshot objects (stock/future/option).
    """
    for k in ("total_volume", "volume", "totalVolume", "vol"):
        try:
            v = getattr(snap, k, None)
            if v is not None:
                return int(v)
        except Exception:
            pass
    return 0


def _contract_key_near_month(contract: Any) -> Tuple[int, str]:
    """
    Sort key to choose a "near" month contract among many.
    Best-effort: use delivery_date / delivery_month if available, else fallback to code.
    Lower key => more preferred (nearer).
    """
    # delivery_date: might be "YYYY-MM-DD" or similar
    try:
        dd = getattr(contract, "delivery_date", None)
        if dd:
            # Convert to int YYYYMMDD for sorting
            digits = "".join([ch for ch in str(dd) if ch.isdigit()])
            if len(digits) >= 8:
                return (int(digits[:8]), str(getattr(contract, "code", "")))
    except Exception:
        pass

    # delivery_month: might be "YYYYMM"
    try:
        dm = getattr(contract, "delivery_month", None)
        if dm:
            digits = "".join([ch for ch in str(dm) if ch.isdigit()])
            if len(digits) >= 6:
                return (int(digits[:6]) * 100 + 1, str(getattr(contract, "code", "")))
    except Exception:
        pass

    # Fallback: lexicographic code
    return (99999999, str(getattr(contract, "code", "")))


# --- Configuration & Enums ---

class TransactionState(Enum):
    INIT = auto()               # Created
    LEG1_SUBMITTED = auto()     # Stock/Future order sent
    LEG1_PARTIAL = auto()       # Partially filled
    LEG1_FILLED = auto()        # Fully filled
    LEG2_SUBMITTED = auto()     # Second leg order sent
    LEG2_PARTIAL = auto()       # Second leg partially filled
    LEG2_FILLED = auto()        # Second leg fully filled
    COMPLETED = auto()          # Both legs filled
    CANCELLING = auto()         # Cancellation in progress
    CANCELLED = auto()          # Transaction cancelled
    FAILED = auto()             # Fatal error (requires manual intervention)


class SignalType(Enum):
    OPEN = auto()
    CLOSE = auto()
    REPAIR = auto()


@dataclass
class TradeIntent:
    """
    Generated by Strategy, consumed by TxManager.
    """
    type: SignalType
    stock_code: str
    future_code: str
    qty: int
    is_force: bool = False
    details: str = ""


@dataclass
class OrderStub:
    """
    Represents an order within a Transaction.
    Tracks the internal state and the exchange Order ID.
    """
    order_id: str = ""
    seqno: str = ""
    status: str = "INIT"  # Submitted, Filled, Cancelled, Failed
    filled_qty: int = 0
    price: float = 0.0
    action: str = ""
    error_msg: str = ""
    last_deal_qty_reported: int = 0


def _get_deal_qty(status: Dict) -> int:
    """Robustly extract deal quantity."""
    for k in ("deal_quantity", "deal_qty", "filled_qty", "dealQuantity"):
        v = status.get(k, None)
        if v is not None:
            try:
                return int(v)
            except Exception:
                pass
    return 0


def _apply_fill(order: OrderStub, status_data: Dict) -> int:
    q = _get_deal_qty(status_data)
    delta = 0
    # Shioaji 'deal_quantity' is usually cumulative in OrderState.
    if q >= order.last_deal_qty_reported:
        delta = q - order.last_deal_qty_reported
        order.last_deal_qty_reported = q
        order.filled_qty += delta
    else:
        # q < last reported. Might be incremental or reset; ignore to avoid double count.
        pass
    return delta


# --- Market Data & Strategy ---

@dataclass
class MarketSnapshot:
    code: str
    price: float = 0.0
    bid: float = 0.0
    ask: float = 0.0
    bid_size: int = 0
    ask_size: int = 0
    ts: float = 0.0
    volume: int = 0


class StrategyState:
    """
    Tracks EWMA statistics for a specific Pair.
    """
    def __init__(self, half_life_sec: float = 60.0):
        self.mean = 0.0
        self.var = 0.0
        self.std = 0.0
        self.samples = 0
        self.half_life = half_life_sec
        self.last_ts = 0.0

    def update(self, spread: float):
        now = time.time()
        if self.samples == 0:
            self.mean = spread
            self.var = 0.0
            self.last_ts = now
        else:
            dt = now - self.last_ts
            if dt <= 0:
                dt = 0.001

            alpha = 1.0 - math.exp(-0.6931 * dt / self.half_life)

            diff = spread - self.mean
            incr = alpha * diff
            self.mean += incr
            self.var = (1 - alpha) * self.var + alpha * (diff * diff)
            self.last_ts = now

        self.std = self.var ** 0.5
        self.samples += 1


class MarketData:
    def __init__(self, on_tick_callback: Optional[Callable[[str], None]] = None):
        self.stocks: Dict[str, MarketSnapshot] = {}
        self.futures: Dict[str, MarketSnapshot] = {}
        self._lock = threading.Lock()
        self.on_tick_callback = on_tick_callback

        # Quote freshness control
        self.DEFAULT_STALE_SEC = 5.0  # best-effort; bidask/tick should be within this window

    def _extract_best_size(self, tick: Any, side: str) -> int:
        """
        Best-effort extract best bid/ask size at level 1.
        Shioaji bidask may provide bid_volume/ask_volume (list) or bid_qty/ask_qty.
        """
        keys = []
        if side == "bid":
            keys = ["bid_volume", "bid_qty", "bid_size", "bidVolume", "bidQty"]
        else:
            keys = ["ask_volume", "ask_qty", "ask_size", "askVolume", "askQty"]

        for k in keys:
            try:
                v = getattr(tick, k, None)
                if v is None:
                    continue
                if isinstance(v, list):
                    if v:
                        return int(v[0])
                elif isinstance(v, (int, float)):
                    return int(v)
            except Exception:
                pass
        return 0

    def update_stock(self, tick: Any):
        code = ""
        with self._lock:
            code = getattr(tick, "code", "")
            if not code:
                return
            s = self.stocks.get(code, MarketSnapshot(code))

            if hasattr(tick, "close"):
                try:
                    s.price = float(tick.close)
                except Exception:
                    pass
            elif hasattr(tick, "price"):
                try:
                    s.price = float(tick.price)
                except Exception:
                    pass

            if hasattr(tick, "bid_price") and hasattr(tick, "ask_price"):
                try:
                    bp = tick.bid_price
                    ap = tick.ask_price
                    if isinstance(bp, list) and bp:
                        s.bid = float(bp[0])
                    elif isinstance(bp, (float, int)):
                        s.bid = float(bp)

                    if isinstance(ap, list) and ap:
                        s.ask = float(ap[0])
                    elif isinstance(ap, (float, int)):
                        s.ask = float(ap)
                except Exception:
                    pass

            # L1 sizes
            try:
                s.bid_size = self._extract_best_size(tick, "bid")
                s.ask_size = self._extract_best_size(tick, "ask")
            except Exception:
                pass

            try:
                if hasattr(tick, "volume"):
                    s.volume = int(tick.volume)
                elif hasattr(tick, "total_volume"):
                    s.volume = int(tick.total_volume)
            except Exception:
                pass

            s.ts = time.time()
            self.stocks[code] = s

        if self.on_tick_callback and code:
            self.on_tick_callback(code)

    def update_future(self, tick: Any):
        code = ""
        with self._lock:
            code = getattr(tick, "code", "")
            if not code:
                return
            f = self.futures.get(code, MarketSnapshot(code))

            if hasattr(tick, "close"):
                try:
                    f.price = float(tick.close)
                except Exception:
                    pass
            elif hasattr(tick, "price"):
                try:
                    f.price = float(tick.price)
                except Exception:
                    pass

            if hasattr(tick, "bid_price") and hasattr(tick, "ask_price"):
                try:
                    bp = tick.bid_price
                    ap = tick.ask_price
                    if isinstance(bp, list) and bp:
                        f.bid = float(bp[0])
                    elif isinstance(bp, (float, int)):
                        f.bid = float(bp)

                    if isinstance(ap, list) and ap:
                        f.ask = float(ap[0])
                    elif isinstance(ap, (float, int)):
                        f.ask = float(ap)
                except Exception:
                    pass

            # L1 sizes
            try:
                f.bid_size = self._extract_best_size(tick, "bid")
                f.ask_size = self._extract_best_size(tick, "ask")
            except Exception:
                pass

            try:
                if hasattr(tick, "volume"):
                    f.volume = int(tick.volume)
                elif hasattr(tick, "total_volume"):
                    f.volume = int(tick.total_volume)
            except Exception:
                pass

            f.ts = time.time()
            self.futures[code] = f

        if self.on_tick_callback and code:
            self.on_tick_callback(code)

    def get_stock(self, code: str) -> Optional[MarketSnapshot]:
        with self._lock:
            return self.stocks.get(code)

    def get_future(self, code: str) -> Optional[MarketSnapshot]:
        with self._lock:
            return self.futures.get(code)

    def is_fresh_stock(self, code: str, max_age_sec: Optional[float] = None) -> bool:
        max_age_sec = self.DEFAULT_STALE_SEC if max_age_sec is None else max_age_sec
        s = self.get_stock(code)
        if not s:
            return False
        return (time.time() - (s.ts or 0.0)) <= max_age_sec

    def is_fresh_future(self, code: str, max_age_sec: Optional[float] = None) -> bool:
        max_age_sec = self.DEFAULT_STALE_SEC if max_age_sec is None else max_age_sec
        f = self.get_future(code)
        if not f:
            return False
        return (time.time() - (f.ts or 0.0)) <= max_age_sec

    def is_pair_fresh(self, stock_code: str, future_code: str, max_age_sec: Optional[float] = None) -> bool:
        return self.is_fresh_stock(stock_code, max_age_sec) and self.is_fresh_future(future_code, max_age_sec)


class StrategyEngine:
    """
    改為「方向性可成交 spread」
      OPEN : spread_open  = fut_bid - stk_ask  （Buy stock @ask, Sell fut @bid）
      CLOSE: spread_close = fut_ask - stk_bid  （Buy fut @ask, Sell stock @bid）

    並且分開維護 EWMA 統計：
      stats[(s,f,"OPEN")] / stats[(s,f,"CLOSE")]
    """
    def __init__(self, market_data: MarketData, check_holdings_cb: Optional[Callable[[str], bool]] = None):
        self.md = market_data
        self.check_holdings_cb = check_holdings_cb
        self.stats: Dict[Tuple[str, str, str], StrategyState] = {}

        # Parameters
        self.Z_ENTRY = 2.0
        self.Z_EXIT = 0.5
        self.MIN_STD = 0.1

        # Debounce to prevent signal storms
        self.signal_cooldown_sec = 10.0
        self._last_signal_ts: Dict[Tuple[str, str, str], float] = {}

        # Quote freshness requirement
        self.STALE_SEC = 5.0

    def _get_stats(self, stock_code: str, future_code: str, mode: str) -> StrategyState:
        key = (stock_code, future_code, mode)
        if key not in self.stats:
            self.stats[key] = StrategyState(half_life_sec=300)  # 5-min half life
        return self.stats[key]

    def _tradable_spreads(self, stk: Optional[MarketSnapshot], fut: Optional[MarketSnapshot]) -> Tuple[float, float]:
        """
        Return (spread_open, spread_close) using directional executable prices.
        Also rejects stale or invalid quotes.
        """
        if not stk or not fut:
            return 0.0, 0.0

        now = time.time()
        if (now - (stk.ts or 0.0)) > self.STALE_SEC or (now - (fut.ts or 0.0)) > self.STALE_SEC:
            return 0.0, 0.0

        # Need bid/ask valid
        if stk.ask <= 0 or stk.bid <= 0 or fut.ask <= 0 or fut.bid <= 0:
            return 0.0, 0.0

        spread_open = fut.bid - stk.ask   # OPEN: Sell fut @bid, Buy stk @ask
        spread_close = fut.ask - stk.bid  # CLOSE: Buy fut @ask, Sell stk @bid
        return spread_open, spread_close

    def on_tick(self, stock_code: str, future_code: str) -> Optional[TradeIntent]:
        stk = self.md.get_stock(stock_code)
        fut = self.md.get_future(future_code)

        spread_open, spread_close = self._tradable_spreads(stk, fut)
        if spread_open == 0.0 and spread_close == 0.0:
            return None

        has_position = False
        if self.check_holdings_cb:
            has_position = self.check_holdings_cb(stock_code)

        now = time.time()

        # Update correct stats stream
        if not has_position:
            st = self._get_stats(stock_code, future_code, "OPEN")
            st.update(spread_open)
            if st.samples < 10:
                return None

            std = max(st.std, self.MIN_STD)
            z = (spread_open - st.mean) / std

            is_entry = (z > self.Z_ENTRY)
            if is_entry:
                key = (stock_code, future_code, "OPEN")
                if now - self._last_signal_ts.get(key, 0) < self.signal_cooldown_sec:
                    return None
                self._last_signal_ts[key] = now

                print(f"[Strategy][OPEN] {stock_code} Sprd={spread_open:.2f} Z={z:.2f} (µ={st.mean:.2f} σ={std:.2f}) "
                      f"Stk(Ask={stk.ask:.2f} AskSz={stk.ask_size}) Fut(Bid={fut.bid:.2f} BidSz={fut.bid_size})")

                return TradeIntent(
                    type=SignalType.OPEN,
                    stock_code=stock_code,
                    future_code=future_code,
                    qty=1,
                    details=f"OPEN Z={z:.2f} > {self.Z_ENTRY} (sprd={spread_open:.2f})"
                )

            return None

        else:
            st = self._get_stats(stock_code, future_code, "CLOSE")
            st.update(spread_close)
            if st.samples < 10:
                return None

            std = max(st.std, self.MIN_STD)
            z = (spread_close - st.mean) / std

            is_close = (abs(z) < self.Z_EXIT)
            if is_close:
                key = (stock_code, future_code, "CLOSE")
                if now - self._last_signal_ts.get(key, 0) < self.signal_cooldown_sec:
                    return None
                self._last_signal_ts[key] = now

                print(f"[Strategy][CLOSE] {stock_code} Sprd={spread_close:.2f} Z={z:.2f} (µ={st.mean:.2f} σ={std:.2f}) "
                      f"Stk(Bid={stk.bid:.2f} BidSz={stk.bid_size}) Fut(Ask={fut.ask:.2f} AskSz={fut.ask_size})")

                return TradeIntent(
                    type=SignalType.CLOSE,
                    stock_code=stock_code,
                    future_code=future_code,
                    qty=1,
                    details=f"CLOSE |Z|={abs(z):.2f} < {self.Z_EXIT} (sprd={spread_close:.2f})"
                )

            return None


# --- Execution Engine (The Gateway) ---

class ExecutionEngine:
    """
    Handles connection to Shioaji, Authentication, and Raw Order Placement.
    Does NOT manage state. Just executes commands.
    """
    def __init__(self):
        print(f"[Execution] CWD: {os.getcwd()}")
        load_dotenv()

        # Init API
        self.api = sj.Shioaji(simulation=True)

        # Load Credentials
        self.person_id = os.getenv("Sinopack_PERSON_ID")
        self.password = os.getenv("Sinopack_PASSWORD")
        self.ca_api_key = os.getenv("Sinopack_CA_API_KEY")
        self.ca_secret_key = os.getenv("Sinopack_CA_SECRET_KEY")
        self.ca_path = os.getenv("Sinopack_CA_PATH")
        self.ca_password = os.getenv("Sinopack_CA_PASSWORD")

        k_len = len(self.ca_api_key) if self.ca_api_key else 0
        s_len = len(self.ca_secret_key) if self.ca_secret_key else 0
        print(f"[Execution] Keys loaded. API_KEY len={k_len}, SECRET_KEY len={s_len}, PERSON_ID={self.person_id}")

        self.stock_account = None
        self.futopt_account = None

        # Callback for TxManager to hook into
        self.on_order_callback = None

        # Track recent orders for 'o' command
        self.recent_orders = []
        self.seqno_to_trade = {}

    def get_contract(self, code: str, security_type: str = "Stock"):
        if security_type == "Stock":
            return resolve_stock_contract(self.api, code)
        elif security_type == "Future":
            return resolve_future_contract(self.api, code)
        elif security_type == "Option":
            return resolve_option_contract(self.api, code)
        return None

    def login(self):
        print("[Execution] Connecting to Shioaji API...")
        if not self.ca_api_key or not self.ca_secret_key:
            print("[Execution] Fatal: Missing API Key/Secret.")
            return False

        try:
            self.api.login(
                api_key=self.ca_api_key,
                secret_key=self.ca_secret_key,
                contracts_cb=lambda x: print(f"[Execution] Contracts loaded: {x}"),
                subscribe_trade=True
            )
            print("[Execution] Login success (Session Up). Waiting for contracts...")
            time.sleep(5.0)
        except Exception as e:
            print(f"[Execution] Login failed: {e}")
            return False

        print("[Execution] CA Activation SKIPPED (Simulation Mode Stability).")

        self.stock_account = self.api.stock_account
        self.futopt_account = self.api.futopt_account

        if not self.stock_account or not self.futopt_account:
            print("[Execution] Warning: Accounts not retrieved (Sim mode maybe?).")
        else:
            print(f"[Execution] Stock Account: {self.stock_account.account_id}")
            print(f"[Execution] Future Account: {self.futopt_account.account_id}")

        # Hook global callbacks
        self.api.set_order_callback(self._wrap_callback)
        return True

    def _wrap_callback(self, *args, **kwargs):
        """
        Routes Shioaji callbacks (supports both (topic, msg) and (OrderState) signatures).
        Ensures we extract a stable 'seqno' to match trade.order.seqno.
        """
        if not self.on_order_callback:
            return

        data = None

        # Case A: (topic, msg)
        if len(args) == 2 and isinstance(args[0], str):
            topic, msg = args
            if topic != "OrderState":
                return
            data = msg or {}

        # Case B: (state,) object or dict
        elif len(args) >= 1:
            state = args[0]
            if isinstance(state, dict):
                data = state
            elif hasattr(state, "to_dict"):
                data = state.to_dict()
            else:
                data = getattr(state, "__dict__", {}) or {}
        else:
            return

        op = data.get("operation", {}) or {}
        order = data.get("order", {}) or {}
        status = data.get("status", {}) or {}

        op_code = op.get("op_code", "") or ""
        op_msg = op.get("op_msg", "") or ""

        # CRITICAL: seqno must match trade.order.seqno
        seqno = order.get("seqno", "") or ""
        oid = order.get("id", "") or order.get("ordno", "") or ""

        # Normalize deal quantity keys
        if "deal_quantity" not in status:
            for k in ("deal_qty", "filled_qty", "dealQuantity"):
                if k in status:
                    status["deal_quantity"] = status.get(k)
                    break

        print(f"[CB] Seq={seqno} OID={oid} Op={op_code} Msg={op_msg} Stat={status.get('status')} Deal={status.get('deal_quantity')}")

        if not seqno:
            return

        self.on_order_callback(oid, seqno, op_code, op_msg, status)

    def place_stock_order(self, code: str, action: str, price: float, qty: int,
                          price_type: str = "LMT", order_type: str = "ROD") -> Optional[str]:
        """
        Returns Order seqno if successful, None otherwise.
        """
        try:
            contract = resolve_stock_contract(self.api, code)
            if not contract:
                print(f"[Execution] Stock contract not found: {code}")
                return None

            act = sj_constant.Action.Buy if action == "Buy" else sj_constant.Action.Sell
            pt = sj_constant.StockPriceType.LMT if price_type == "LMT" else sj_constant.StockPriceType.MKT
            ot = sj_constant.OrderType.ROD if order_type == "ROD" else sj_constant.OrderType.IOC

            if pt == sj_constant.StockPriceType.MKT:
                price = 0.0

            trade = self.api.place_order(
                contract=contract,
                order=self.api.Order(
                    price=price,
                    quantity=qty,
                    action=act,
                    price_type=pt,
                    order_type=ot,
                    account=self.stock_account
                )
            )

            self.seqno_to_trade[trade.order.seqno] = trade
            self._track_order(trade.order.seqno, code, action, price, qty, "Stock")
            return trade.order.seqno

        except Exception as e:
            print(f"[Execution] Stock Order Failed: {e}")
            return None

    def place_future_order(self, code: str, action: str, price: float, qty: int,
                           price_type: str = "LMT", order_type: str = "ROD") -> Optional[str]:
        try:
            contract = resolve_future_contract(self.api, code)
            if not contract:
                print(f"[Execution] Future contract not found: {code}")
                return None

            act = sj_constant.Action.Buy if action == "Buy" else sj_constant.Action.Sell
            pt = sj_constant.FuturesPriceType.LMT if price_type == "LMT" else sj_constant.FuturesPriceType.MKT
            ot = sj_constant.OrderType.ROD if order_type == "ROD" else sj_constant.OrderType.IOC

            if pt == sj_constant.FuturesPriceType.MKT:
                price = 0.0

            trade = self.api.place_order(
                contract=contract,
                order=self.api.Order(
                    price=price,
                    quantity=qty,
                    action=act,
                    price_type=pt,
                    order_type=ot,
                    account=self.futopt_account
                )
            )

            self.seqno_to_trade[trade.order.seqno] = trade
            self._track_order(trade.order.seqno, code, action, price, qty, "Future")
            return trade.order.seqno

        except Exception as e:
            print(f"[Execution] Future Order Failed: {e}")
            return None

    def _track_order(self, seqno, code, action, price, qty, product):
        ts = time.strftime("%H:%M:%S")
        self.recent_orders.append({
            "time": ts,
            "seqno": seqno,
            "code": code,
            "action": action,
            "price": price,
            "qty": qty,
            "product": product,
            "status": "Submitted"
        })
        if len(self.recent_orders) > 50:
            self.recent_orders.pop(0)

    def dump_orders(self) -> str:
        if not self.recent_orders:
            return "No orders yet."
        lines = ["\n=== RECENT ORDERS ==="]
        for o in self.recent_orders:
            lines.append(f"{o['time']} [{o['product']}] {o['action']} {o['code']} Q={o['qty']} P={o['price']} Seq={o['seqno']} St={o['status']}")
        return "\n".join(lines)

    def cancel_all_orders(self):
        """Cancel all pending orders in both accounts"""
        print("[Execution] Cancelling all open orders...")
        try:
            self.api.update_status()
            for trade in self.api.list_trades():
                if trade.status.status in ("PreSubmitted", "Submitted", "PartFilled"):
                    self.api.cancel_order(trade)
                    print(f"  Cancelled: {trade.contract.code} {trade.order.action}")
        except Exception as e:
            print(f"[Execution] Cancel failed: {e}")

    def cancel_order_by_seqno(self, seqno: str):
        """Cancel order using stored trade object."""
        try:
            trade = self.seqno_to_trade.get(seqno)
            if trade:
                self.api.cancel_order(trade)
                print(f"[Execution] Cancelled order {seqno}")
            else:
                print(f"[Execution] Cannot cancel {seqno}: Trade object not found.")
        except Exception as e:
            print(f"[Execution] Cancel failed for {seqno}: {e}")

    def get_order_status(self, seqno: str) -> Optional[Dict]:
        """Fetch latest status for a specific order by seqno via polling."""
        try:
            if self.stock_account:
                self.api.update_status(self.stock_account)
            if self.futopt_account:
                self.api.update_status(self.futopt_account)

            trades = self.api.list_trades()
            for trade in trades:
                if trade.order.seqno == seqno:
                    status_str = trade.status.status
                    deal_quantity = getattr(trade.status, "deal_quantity", 0)
                    return {
                        "status": status_str,
                        "deal_quantity": deal_quantity,
                        "ordno": trade.order.ordno,
                        "id": trade.order.id,
                        "seqno": trade.order.seqno,
                    }
        except Exception:
            pass
        return None

    def get_all_positions(self) -> Dict[str, int]:
        """Returns map of code -> net signed quantity"""
        pos_map = {}
        try:
            # Stocks
            if self.stock_account:
                for p in self.api.list_positions(self.stock_account):
                    pos_map[p.code] = int(p.quantity)

            # Futures/Options (futopt_account)
            if self.futopt_account:
                for p in self.api.list_positions(self.futopt_account):
                    qty = int(p.quantity)
                    # direction could be Action.Buy/Sell
                    direction = getattr(p, "direction", None)
                    net = qty
                    try:
                        if direction == sj_constant.Action.Sell:
                            net = -qty
                        else:
                            net = qty
                    except Exception:
                        net = qty
                    pos_map[p.code] = net
        except Exception:
            pass
        return pos_map

    def dump_positions(self) -> str:
        lines = ["\n=== POSITIONS ==="]
        try:
            if self.stock_account:
                self.api.update_status(self.stock_account)
            if self.futopt_account:
                self.api.update_status(self.futopt_account)

            if self.stock_account:
                pos = self.api.list_positions(self.stock_account)
                lines.append(f"Stocks ({len(pos)}):")
                for p in pos:
                    lines.append(f"  {p.code} Qty={int(p.quantity)} avgP={p.price}")

            if self.futopt_account:
                pos = self.api.list_positions(self.futopt_account)
                lines.append(f"FutOpt ({len(pos)}):")
                for p in pos:
                    code = getattr(p, "code", "Unknown")
                    qty = getattr(p, "quantity", 0)
                    price = getattr(p, "price", 0)
                    direction = getattr(p, "direction", None)
                    lines.append(f"  {code} Qty={int(qty)} avgP={price} dir={direction}")

        except Exception as e:
            lines.append(f"Error: {e}")
        return "\n".join(lines)


# --- Core Transaction Classes ---

class BaseTransaction:
    def __init__(self, intent: TradeIntent, tx_mgr):
        self.tx_id = str(uuid.uuid4())[:8]
        self.intent = intent
        self.mgr = tx_mgr
        self.state = TransactionState.INIT
        self.created_at = time.time()
        self.updated_at = time.time()
        self.state_enter_ts = time.time()

        self.stock_order = OrderStub()
        self.future_order = OrderStub()

        self.last_poll_ts = 0.0

        self.log(f"Transaction Created: {intent}")

    def log(self, msg: str):
        ts = time.strftime("%H:%M:%S")
        print(f"[{ts}] [Tx-{self.tx_id}] {msg}")

    def set_state(self, new_state: TransactionState, msg: str = ""):
        self.state = new_state
        self.state_enter_ts = time.time()
        if msg:
            self.log(msg)

    def _book_ok_for_market(self, snap: MarketSnapshot, action: str, min_qty: int, product: str) -> bool:
        """
        Gate order placement by L1 book size.
        Buy -> need ask_size >= min_qty
        Sell -> need bid_size >= min_qty
        """
        if not snap:
            return False

        # Also gate stale quote
        stale_sec = self.mgr.QUOTE_STALE_SEC
        if (time.time() - (snap.ts or 0.0)) > stale_sec:
            self.log(f"[BOOK][BLOCK] {product} quote stale (> {stale_sec}s). ts_age={time.time() - (snap.ts or 0.0):.2f}s")
            return False

        if action == "Buy":
            ok = (snap.ask > 0 and snap.ask_size >= min_qty)
            if not ok:
                self.log(f"[BOOK][BLOCK] {product} BUY needs ask_size>={min_qty}, got ask={snap.ask} ask_size={snap.ask_size}")
            return ok
        else:
            ok = (snap.bid > 0 and snap.bid_size >= min_qty)
            if not ok:
                self.log(f"[BOOK][BLOCK] {product} SELL needs bid_size>={min_qty}, got bid={snap.bid} bid_size={snap.bid_size}")
            return ok

    def update(self):
        self.updated_at = time.time()
        if self.state == TransactionState.INIT:
            self._step_init()
        elif self.state == TransactionState.LEG1_SUBMITTED:
            self._check_leg1_polling()
            self._check_leg1_timeout()
        elif self.state == TransactionState.LEG1_FILLED:
            self._step_place_leg2()
        elif self.state == TransactionState.LEG2_SUBMITTED:
            self._check_leg2_polling()
            self._check_leg2_timeout()

    def on_order_update(self, seqno: str, op_code: str, msg: str, status_data: Dict):
        # Leg 1: Stock
        if seqno == self.stock_order.seqno:
            self.log(f"Leg 1 Update: Code={op_code} Msg={msg}")
            if op_code == "00":
                delta = _apply_fill(self.stock_order, status_data)
                if delta > 0:
                    self.log(f"Leg 1 Partial Fill: +{delta} (Total: {self.stock_order.filled_qty})")
                    if self.stock_order.filled_qty >= self.intent.qty:
                        self.set_state(TransactionState.LEG1_FILLED, "Leg 1 Fully Filled.")
                        self.stock_order.status = "Filled"
            else:
                self.stock_order.status = "Failed"
                self.stock_order.error_msg = msg
                self.set_state(TransactionState.FAILED, f"Leg 1 FAILED: {msg}")

        # Leg 2: Future
        elif seqno == self.future_order.seqno:
            self.log(f"Leg 2 Update: Code={op_code} Msg={msg}")
            if op_code == "00":
                delta = _apply_fill(self.future_order, status_data)
                if delta > 0:
                    self.log(f"Leg 2 Partial Fill: +{delta} (Total: {self.future_order.filled_qty})")
                    if self.future_order.filled_qty >= self.intent.qty:
                        self.set_state(TransactionState.COMPLETED, "Transaction COMPLETED.")
                        self.future_order.status = "Filled"
            else:
                self.future_order.status = "Failed"
                self.future_order.error_msg = msg
                self.set_state(TransactionState.FAILED, f"Leg 2 FAILED: {msg}")

    # --- Step Implementations ---

    def _step_init(self):
        snapshot = self.mgr.market_data.get_stock(self.intent.stock_code)
        if not snapshot:
            self.log("No market data for Leg 1. Waiting...")
            return

        if snapshot.price <= 0 and snapshot.bid <= 0 and snapshot.ask <= 0:
            return

        # Freshness gate
        if not self.mgr.market_data.is_pair_fresh(self.intent.stock_code, self.intent.future_code, self.mgr.QUOTE_STALE_SEC):
            self.log(f"Pair quote stale at INIT -> cooldown pair and skip. ({self.intent.stock_code}/{self.intent.future_code})")
            self.mgr.cooldown_pair(self.intent.stock_code, self.intent.future_code, sec=30, reason="INIT stale quote")
            self.set_state(TransactionState.FAILED, "INIT stale quote")
            return

        self.log(f"Placing Leg 1 (Stock {self.intent.stock_code}) Market Order...")

        action = "Buy" if self.intent.type == SignalType.OPEN else "Sell"

        # Gate by book size (stock)
        if not self._book_ok_for_market(snapshot, action, self.mgr.MIN_STK_BOOK_QTY, "Stock"):
            # 不送單 -> 取消交易（避免進場在流動性不足時亂打）
            self.set_state(TransactionState.FAILED, "Leg 1 BLOCKED by Stock book size/stale")
            return

        # --- DUAL CHECK: Before committing to Leg 1 (Stock), check Leg 2 (Future) ---
        fut_snap = self.mgr.market_data.get_future(self.intent.future_code)
        if not fut_snap:
             self.log("No market data for Leg 2 (Future) at INIT. Waiting/Skipping...")
             return

        fut_action = "Sell" if self.intent.type == SignalType.OPEN else "Buy"
        if not self._book_ok_for_market(fut_snap, fut_action, self.mgr.MIN_FUT_BOOK_QTY, "Future (Pre-Leg1)"):
             self.log(f"Leg 1 Blocked because Leg 2 (Future) book size invalid/stale.")
             self.set_state(TransactionState.FAILED, "Leg 1 BLOCKED by Leg 2 (Future) book size")
             return
        # ---------------------------------------------------------------------------

        seqno = self.mgr.execution.place_stock_order(
            self.intent.stock_code, action, 0, self.intent.qty, "MKT", "ROD"
        )

        if seqno:
            self.stock_order.seqno = seqno
            self.stock_order.status = "Submitted"
            self.state = TransactionState.LEG1_SUBMITTED
            self.state_enter_ts = time.time()
            self.log(f"Leg 1 Submitted SeqNo: {seqno}")
        else:
            self.set_state(TransactionState.FAILED, "Leg 1 Submission Failed")

    def _check_leg1_timeout(self):
        if time.time() - self.state_enter_ts > 90:
            self.log("Leg 1 Timeout. Cancelling...")
            self.mgr.execution.cancel_order_by_seqno(self.stock_order.seqno)
            self.set_state(TransactionState.FAILED, "Leg 1 Timeout & Cancelled")

    def _step_place_leg2(self):
        filled_qty = self.stock_order.filled_qty
        if filled_qty <= 0:
            self.log("Leg 1 Filled Qty is 0? Strange.")
            self.set_state(TransactionState.FAILED, "Leg 1 filled qty 0")
            return

        hedge_qty = filled_qty

        self.log(f"Placing Leg 2 (Future {self.intent.future_code}) Qty={hedge_qty}...")
        action = "Sell" if self.intent.type == SignalType.OPEN else "Buy"

        self.log(f"Placing Leg 2 (Future {self.intent.future_code}) Market Order...")

        # Gate by book size (future) - BUT leg2 is risk-critical.
        fut_snap = self.mgr.market_data.get_future(self.intent.future_code)
        if not fut_snap:
            self.log("No market data for Future leg2. Targeted REPAIR to avoid naked.")
            self.mgr.repair_pair(self.intent.stock_code, self.intent.future_code, reason="leg2 missing future quote", flatten_both=True)
            self.set_state(TransactionState.FAILED, "Leg 2 missing future market data -> repaired_pair")
            return

        if not self._book_ok_for_market(fut_snap, action, self.mgr.MIN_FUT_BOOK_QTY, "Future"):
            # 量不足：不送單 -> 立刻 repair（避免 stock 已成交但 future 沒對沖）
            self.log("Leg 2 BLOCKED by Future book size/stale. Triggering Targeted REPAIR...")
            self.mgr.repair_pair(self.intent.stock_code, self.intent.future_code, reason="leg2 blocked by future book/stale", flatten_both=True)
            self.set_state(TransactionState.FAILED, "Leg 2 BLOCKED by Future book size/stale -> repaired_pair")
            return

        seqno = self.mgr.execution.place_future_order(
            self.intent.future_code, action, 0, hedge_qty, "MKT", "IOC"
        )

        if seqno:
            self.future_order.seqno = seqno
            self.future_order.status = "Submitted"
            self.set_state(TransactionState.LEG2_SUBMITTED, f"Leg 2 Submitted SeqNo: {seqno}")
        else:
            self.log("Leg 2 Submission Failed -> Targeted REPAIR")
            self.mgr.repair_pair(self.intent.stock_code, self.intent.future_code, reason="leg2 submit failed", flatten_both=True)
            self.set_state(TransactionState.FAILED, "Leg 2 Submission Failed -> repaired_pair")

    def _check_leg2_timeout(self):
        if time.time() - self.state_enter_ts > 20:
            self.log("Leg 2 Timeout. Cancelling...")
            self.mgr.execution.cancel_order_by_seqno(self.future_order.seqno)
            self.log("Leg 2 Timeout -> Targeted REPAIR")
            self.mgr.repair_pair(self.intent.stock_code, self.intent.future_code, reason="leg2 timeout", flatten_both=True)
            self.set_state(TransactionState.FAILED, "Leg 2 Timeout & Cancelled -> repaired_pair")

    # --- Polling Failsafe ---

    def _check_leg1_polling(self):
        if time.time() - self.last_poll_ts < 2.0:
            return
        self.last_poll_ts = time.time()

        status = self.mgr.execution.get_order_status(self.stock_order.seqno)
        if status:
            s_str = status.get("status")
            qty = int(status.get("deal_quantity", 0) or 0)

            if s_str == "Filled" or qty >= self.intent.qty:
                self.log(f"[Polling] Detected Leg 1 Fill! Qty={qty}")
                self.on_order_update(self.stock_order.seqno, "00", "Polled Fill", status)
            elif s_str == "Cancelled":
                self.log("[Polling] Detected Leg 1 Cancel.")
                self.set_state(TransactionState.FAILED, "Leg 1 Cancelled")

    def _check_leg2_polling(self):
        if time.time() - self.last_poll_ts < 2.0:
            return
        self.last_poll_ts = time.time()

        status = self.mgr.execution.get_order_status(self.future_order.seqno)
        if status:
            s_str = status.get("status")
            qty = int(status.get("deal_quantity", 0) or 0)
            if s_str == "Filled" or qty >= self.intent.qty:
                self.log(f"[Polling] Detected Leg 2 Fill! Qty={qty}")
                self.on_order_update(self.future_order.seqno, "00", "Polled Fill", status)
            elif s_str == "Cancelled":
                self.log("[Polling] Detected Leg 2 Cancel.")
                self.log("Leg 2 Cancelled -> Targeted REPAIR")
                self.mgr.repair_pair(self.intent.stock_code, self.intent.future_code, reason="leg2 cancelled", flatten_both=True)
                self.set_state(TransactionState.FAILED, "Leg 2 Cancelled -> repaired_pair")


class OpenArbitrageTransaction(BaseTransaction):
    """
    Arbitrage Open:
    1. Buy Stock
    2. Sell Future
    """
    pass


class CloseArbitrageTransaction(BaseTransaction):
    """
    Arbitrage Exit:
    1) Buy Future (close short)  <-- leg1
    2) Sell Stock (close long)   <-- leg2
    """
    def update(self):
        self.updated_at = time.time()

        if self.state == TransactionState.INIT:
            self._step_close_init_buy_future()
        elif self.state == TransactionState.LEG1_SUBMITTED:
            self._check_close_leg1_polling()
            self._check_close_leg1_timeout()
        elif self.state == TransactionState.LEG1_FILLED:
            self._step_close_leg2_sell_stock()
        elif self.state == TransactionState.LEG2_SUBMITTED:
            self._check_close_leg2_polling()
            self._check_close_leg2_timeout()

    def _step_close_init_buy_future(self):
        snap = self.mgr.market_data.get_future(self.intent.future_code)
        if not snap or (snap.price <= 0 and snap.bid <= 0 and snap.ask <= 0):
            self.log("No Future market data. Waiting...")
            return

        # Freshness gate
        if not self.mgr.market_data.is_pair_fresh(self.intent.stock_code, self.intent.future_code, self.mgr.QUOTE_STALE_SEC):
            self.log(f"Pair quote stale at CLOSE INIT -> cooldown pair and skip. ({self.intent.stock_code}/{self.intent.future_code})")
            self.mgr.cooldown_pair(self.intent.stock_code, self.intent.future_code, sec=30, reason="CLOSE INIT stale quote")
            self.set_state(TransactionState.FAILED, "CLOSE INIT stale quote")
            return

        # Gate by book size for BUY future
        if not self._book_ok_for_market(snap, "Buy", self.mgr.MIN_FUT_BOOK_QTY, "Future"):
            self.set_state(TransactionState.FAILED, "Close leg1 BLOCKED by Future book size/stale")
            return

        # --- DUAL CHECK: Before committing to Leg 1 (Future), check Leg 2 (Stock) ---
        stk_snap = self.mgr.market_data.get_stock(self.intent.stock_code)
        if not stk_snap:
             self.log("No Stock market data for Close Leg 2. Waiting/Skipping...")
             return
        
        # Close: Leg 1 Buy Future, Leg 2 Sell Stock
        if not self._book_ok_for_market(stk_snap, "Sell", self.mgr.MIN_STK_BOOK_QTY, "Stock (Pre-Leg1)"):
             self.log(f"Close Leg 1 Blocked because Leg 2 (Stock) book size invalid/stale.")
             self.set_state(TransactionState.FAILED, "Close Leg 1 BLOCKED by Leg 2 (Stock) book size")
             return
        # ----------------------------------------------------------------------------

        self.log(f"Closing: BUY Future {self.intent.future_code} MKT IOC Qty={self.intent.qty}")
        seqno = self.mgr.execution.place_future_order(
            self.intent.future_code, "Buy", 0, self.intent.qty, "MKT", "IOC"
        )
        if not seqno:
            self.set_state(TransactionState.FAILED, "Close leg1 (Future Buy) submit failed")
            return

        self.future_order.seqno = seqno
        self.future_order.status = "Submitted"
        self.set_state(TransactionState.LEG1_SUBMITTED, f"Close leg1 submitted seqno={seqno}")

    def _step_close_leg2_sell_stock(self):
        filled_qty = self.future_order.filled_qty
        if filled_qty <= 0:
            self.set_state(TransactionState.FAILED, "Close leg1 filled_qty=0 (unexpected)")
            return

        snap = self.mgr.market_data.get_stock(self.intent.stock_code)
        if not snap or (snap.price <= 0 and snap.bid <= 0 and snap.ask <= 0):
            self.log("No Stock market data for close leg2. Targeted REPAIR.")
            self.mgr.repair_pair(self.intent.stock_code, self.intent.future_code, reason="close leg2 missing stock quote", flatten_both=True)
            self.set_state(TransactionState.FAILED, "Close leg2 missing stock market data -> repaired_pair")
            return

        # Gate by book size for SELL stock
        if not self._book_ok_for_market(snap, "Sell", self.mgr.MIN_STK_BOOK_QTY, "Stock"):
            self.log("Close leg2 BLOCKED by Stock book size/stale -> Targeted REPAIR")
            self.mgr.repair_pair(self.intent.stock_code, self.intent.future_code, reason="close leg2 blocked by stock book/stale", flatten_both=True)
            self.set_state(TransactionState.FAILED, "Close leg2 BLOCKED by Stock book size/stale -> repaired_pair")
            return

        self.log(f"Closing: SELL Stock {self.intent.stock_code} MKT ROD Qty={filled_qty}")
        seqno = self.mgr.execution.place_stock_order(
            self.intent.stock_code, "Sell", 0, filled_qty, "MKT", "ROD"
        )
        if not seqno:
            self.log("Close leg2 submit failed -> Targeted REPAIR")
            self.mgr.repair_pair(self.intent.stock_code, self.intent.future_code, reason="close leg2 submit failed", flatten_both=True)
            self.set_state(TransactionState.FAILED, "Close leg2 (Stock Sell) submit failed -> repaired_pair")
            return

        self.stock_order.seqno = seqno
        self.stock_order.status = "Submitted"
        self.set_state(TransactionState.LEG2_SUBMITTED, f"Close leg2 submitted seqno={seqno}")

    def on_order_update(self, seqno: str, op_code: str, msg: str, status_data: Dict):
        # leg1 = future
        if seqno == self.future_order.seqno:
            self.log(f"[Close] Future Update: {op_code} {msg}")
            if op_code != "00":
                self.future_order.status = "Failed"
                self.set_state(TransactionState.FAILED, f"Close leg1 failed: {msg}")
                return

            delta = _apply_fill(self.future_order, status_data)
            if delta > 0:
                self.log(f"[Close] Future fill +{delta} (total {self.future_order.filled_qty})")

            if self.future_order.filled_qty >= self.intent.qty:
                self.future_order.status = "Filled"
                self.set_state(TransactionState.LEG1_FILLED, "Close leg1 (Future) filled")
            return

        # leg2 = stock
        if seqno == self.stock_order.seqno:
            self.log(f"[Close] Stock Update: {op_code} {msg}")
            if op_code != "00":
                self.stock_order.status = "Failed"
                self.set_state(TransactionState.FAILED, f"Close leg2 failed: {msg}")
                return

            delta = _apply_fill(self.stock_order, status_data)
            if delta > 0:
                self.log(f"[Close] Stock fill +{delta} (total {self.stock_order.filled_qty})")

            tgt = self.future_order.filled_qty
            if tgt > 0 and self.stock_order.filled_qty >= tgt:
                self.stock_order.status = "Filled"
                self.set_state(TransactionState.COMPLETED, "Arbitrage Close COMPLETE")
            return

    # ---- polling failsafe ----
    def _check_close_leg1_polling(self):
        if time.time() - self.last_poll_ts < 2.0:
            return
        self.last_poll_ts = time.time()

        st = self.mgr.execution.get_order_status(self.future_order.seqno)
        if not st:
            return
        s_str = st.get("status")
        qty = int(st.get("deal_quantity", 0) or 0)

        if s_str == "Filled" or qty >= self.intent.qty:
            self.log(f"[Polling] Close leg1 fill detected qty={qty}")
            self.on_order_update(self.future_order.seqno, "00", "Polled Fill", st)
        elif s_str == "Cancelled":
            self.set_state(TransactionState.FAILED, "Close leg1 cancelled")

    def _check_close_leg2_polling(self):
        if time.time() - self.last_poll_ts < 2.0:
            return
        self.last_poll_ts = time.time()

        st = self.mgr.execution.get_order_status(self.stock_order.seqno)
        if not st:
            return
        s_str = st.get("status")
        qty = int(st.get("deal_quantity", 0) or 0)

        tgt = self.future_order.filled_qty
        if tgt > 0 and (s_str == "Filled" or qty >= tgt):
            self.log(f"[Polling] Close leg2 fill detected qty={qty}")
            self.on_order_update(self.stock_order.seqno, "00", "Polled Fill", st)
        elif s_str == "Cancelled":
            self.log("Close leg2 cancelled -> Targeted REPAIR")
            self.mgr.repair_pair(self.intent.stock_code, self.intent.future_code, reason="close leg2 cancelled", flatten_both=True)
            self.set_state(TransactionState.FAILED, "Close leg2 cancelled -> repaired_pair")

    # ---- timeout ----
    def _check_close_leg1_timeout(self):
        if time.time() - self.state_enter_ts > 15:
            self.log("Close leg1 timeout -> cancel")
            self.mgr.execution.cancel_order_by_seqno(self.future_order.seqno)
            self.set_state(TransactionState.FAILED, "Close leg1 timeout & cancelled")

    def _check_close_leg2_timeout(self):
        if time.time() - self.state_enter_ts > 90:
            self.log("Close leg2 timeout -> cancel")
            self.mgr.execution.cancel_order_by_seqno(self.stock_order.seqno)
            self.log("Close leg2 timeout -> Targeted REPAIR")
            self.mgr.repair_pair(self.intent.stock_code, self.intent.future_code, reason="close leg2 timeout", flatten_both=True)
            self.set_state(TransactionState.FAILED, "Close leg2 timeout & cancelled -> repaired_pair")


# --- Pair Discovery ---

class PairDiscovery:
    """
    Pair selection rules (as requested):
      1) 監看 pair 入選門檻：股票成交量 AND 期貨成交量 都要夠大才入選
      2) 入選後排序：用「股票成交量 + 選擇權成交量」做排序（越大越前）
         - 選擇權成交量：以 underlying_code==stock_code 的 option contracts 做聚合（最佳努力）
    """
    def __init__(self, api: sj.Shioaji):
        self.api = api

        # Thresholds (you can tune)
        self.MIN_STOCK_VOL = 3000        # 股票入選最低成交量
        self.MIN_FUTURE_VOL = 3000        # 期貨入選最低成交量

        # Option aggregation control
        self.OPTION_PER_UNDERLYING_CAP = 120     # 每檔 underlying 抽樣最多幾個 option contract 來計算成交量
        self.OPTION_TOTAL_CAP = 2000             # 全部 options snapshot 的總數上限（避免一次太大）

    def _load_local_history(self) -> Tuple[set, set, List[Tuple[str, str]]]:
        """
        Returns (stocks_set, futures_set, pairs_list)
        """
        l_stocks = set()
        l_futures = set()
        l_pairs: List[Tuple[str, str]] = []
        path = "order_history.jsonl"
        if not os.path.exists(path):
            return l_stocks, l_futures, l_pairs

        print(f"[Discovery] Reading local history from {path}...")
        try:
            with open(path, "r", encoding="utf-8") as f:
                lines = f.readlines()
                for line in reversed(lines):
                    try:
                        rec = json.loads(line)
                        f_code = rec.get("fut_code")
                        s_code = rec.get("stock_code")
                        if f_code:
                            l_futures.add(f_code)
                        if s_code:
                            l_stocks.add(s_code)
                        if s_code and f_code:
                            l_pairs.append((s_code, f_code))
                    except Exception:
                        pass
            print(f"[Discovery] Recovered from history - Stocks: {len(l_stocks)}, Futures: {len(l_futures)}")
            if l_futures:
                print(f"[Discovery] History Futures Sample: {list(l_futures)[:5]}")
        except Exception as e:
            print(f"[Discovery] History load failed: {e}")

        return l_stocks, l_futures, l_pairs

    def _sync_positions(self) -> Tuple[set, set]:
        held_stocks = set()
        held_futures = set()

        print("[Discovery] Syncing positions (wait 3s)...")
        for attempt in range(3):
            try:
                if hasattr(self.api, "update_status"):
                    try:
                        self.api.update_status(self.api.stock_account)
                    except Exception:
                        pass
                    try:
                        self.api.update_status(self.api.futopt_account)
                    except Exception:
                        pass
                time.sleep(3.0)
                break
            except Exception as e:
                print(f"[Discovery] Sync attempt {attempt+1} failed: {e}")
                time.sleep(2.0)

        for attempt in range(3):
            try:
                held_stocks.clear()
                held_futures.clear()

                if self.api.stock_account:
                    pos = self.api.list_positions(self.api.stock_account)
                    for p in pos:
                        code = getattr(p, "code", "")
                        if code:
                            held_stocks.add(code)

                if self.api.futopt_account:
                    pos = self.api.list_positions(self.api.futopt_account)
                    for p in pos:
                        code = getattr(p, "code", "")
                        if code:
                            held_futures.add(code)

                break
            except Exception as e:
                print(f"[Discovery] API Position scan attempt {attempt+1} failed: {e}")
                time.sleep(2.0)
                if attempt == 2:
                    print("[Discovery] Giving up on Position Scan. Assuming 0 positions.")

        print(f"[Discovery] Active Held Stocks: {held_stocks}")
        print(f"[Discovery] Active Held Futures: {held_futures}")
        return held_stocks, held_futures

    def _build_underlying_to_future(self) -> Dict[str, Any]:
        """
        Build underlying_code -> best future contract (near month heuristic).
        """
        u2f_list: Dict[str, List[Any]] = {}
        try:
            for contract in iter_future_contracts(self.api):
                if not contract:
                    continue
                f_code = getattr(contract, "code", "")
                u_code = getattr(contract, "underlying_code", "")
                if not f_code or not u_code:
                    continue
                if u_code not in u2f_list:
                    u2f_list[u_code] = []
                u2f_list[u_code].append(contract)
        except Exception as e:
            print(f"[Discovery] Future contract scan failed: {e}")

        u2f: Dict[str, Any] = {}
        for u_code, lst in u2f_list.items():
            try:
                lst_sorted = sorted(lst, key=_contract_key_near_month)
                u2f[u_code] = lst_sorted[0]
            except Exception:
                u2f[u_code] = lst[0]
        return u2f

    def _build_underlying_to_options(self) -> Dict[str, List[Any]]:
        """
        Build underlying_code -> option contracts list (best effort).
        """
        u2opts: Dict[str, List[Any]] = {}
        try:
            for c in iter_option_contracts(self.api):
                if not c:
                    continue
                code = getattr(c, "code", "")
                u_code = getattr(c, "underlying_code", "")
                if not code or not u_code:
                    continue
                if u_code not in u2opts:
                    u2opts[u_code] = []
                u2opts[u_code].append(c)
        except Exception as e:
            print(f"[Discovery] Option contract scan failed: {e}")
        return u2opts

    def _fetch_snapshot_volumes(self, contracts: List[Any], chunk_size: int = 200) -> Dict[str, int]:
        """
        Fetch snapshots for given contracts and return code->volume map.
        """
        vol_map: Dict[str, int] = {}
        if not contracts:
            return vol_map

        for i in range(0, len(contracts), chunk_size):
            chunk = contracts[i:i + chunk_size]
            try:
                snaps = self.api.snapshots(chunk)
                for s in snaps:
                    try:
                        code = getattr(s, "code", "")
                        if not code:
                            continue
                        vol_map[code] = _snap_volume(s)
                    except Exception:
                        pass
            except Exception as e:
                print(f"[Discovery] Snapshot chunk failed: {e}")
        return vol_map

    def scan(self, limit: int = 20) -> List[Tuple[str, str]]:
        found: List[Tuple[str, str]] = []

        # 0) Sync positions / history
        held_stocks, held_futures = self._sync_positions()
        h_stocks, h_futures, h_pairs = self._load_local_history()

        # 1) Build mappings
        print("[Discovery] Building underlying->future map...")
        u2f = self._build_underlying_to_future()
        print(f"[Discovery] Candidate underlyings with futures: {len(u2f)}")

        print("[Discovery] Building underlying->options map...")
        u2opts = self._build_underlying_to_options()
        print(f"[Discovery] Candidate underlyings with options: {len(u2opts)}")

        # 2) Always include held pairs first (risk-first), if possible
        print("[Discovery] Adding held pairs first...")
        for s_code in held_stocks:
            if s_code in u2f:
                f_c = u2f[s_code]
                pair = (s_code, getattr(f_c, "code", ""))
                if pair[1] and pair not in found:
                    found.append(pair)

        # 2.1) Also include history pairs (risk-first), but only if mapping still valid
        print("[Discovery] Adding history pairs (best-effort)...")
        for (s_code, f_code) in h_pairs[:50]:
            if len(found) >= limit:
                break
            # ensure still resolvable
            if resolve_stock_contract(self.api, s_code) and resolve_future_contract(self.api, f_code):
                if (s_code, f_code) not in found:
                    found.append((s_code, f_code))

        # 3) Build candidate stock/future contracts for volume filtering
        #    Note: eligibility requires BOTH stock volume and future volume be high.
        print("[Discovery] Resolving stock & future contracts for volume scans...")
        stk_contracts: List[Any] = []
        fut_contracts: List[Any] = []
        underlying_list: List[str] = []

        # Only underlyings that have BOTH future mapping and a resolvable stock contract
        for u_code, f_c in u2f.items():
            try:
                s_c = resolve_stock_contract(self.api, u_code)
                if s_c is None:
                    continue
                f_code = getattr(f_c, "code", "")
                if not f_code:
                    continue
                stk_contracts.append(s_c)
                fut_contracts.append(f_c)
                underlying_list.append(u_code)
            except Exception:
                pass

        print(f"[Discovery] Volume scan candidates: {len(underlying_list)}")

        # 4) Fetch stock & future volumes
        print("[Discovery] Fetching stock volumes...")
        stk_vol_map = self._fetch_snapshot_volumes(stk_contracts, chunk_size=200)

        print("[Discovery] Fetching future volumes...")
        fut_vol_map = self._fetch_snapshot_volumes(fut_contracts, chunk_size=200)

        # 5) Prepare option contracts list for option volume aggregation
        eligible_underlyings: List[str] = []
        for u_code, f_c in u2f.items():
            f_code = getattr(f_c, "code", "")
            if not f_code:
                continue
            s_vol = stk_vol_map.get(u_code, 0)
            f_vol = fut_vol_map.get(f_code, 0)
            if s_vol >= self.MIN_STOCK_VOL and f_vol >= self.MIN_FUTURE_VOL:
                eligible_underlyings.append(u_code)

        print(f"[Discovery] Eligible (StockVol>={self.MIN_STOCK_VOL} AND FutVol>={self.MIN_FUTURE_VOL}): {len(eligible_underlyings)}")

        # 6) Build an option snapshot list with caps
        option_contracts_to_snap: List[Any] = []
        opt_codes_by_underlying: Dict[str, List[str]] = {}

        def opt_key_near(opt: Any) -> Tuple[int, str]:
            return _contract_key_near_month(opt)

        for u_code in eligible_underlyings:
            opts = u2opts.get(u_code, [])
            if not opts:
                continue
            try:
                opts_sorted = sorted(opts, key=opt_key_near)
            except Exception:
                opts_sorted = opts

            selected = opts_sorted[:self.OPTION_PER_UNDERLYING_CAP]
            opt_codes_by_underlying[u_code] = [getattr(o, "code", "") for o in selected if getattr(o, "code", "")]
            option_contracts_to_snap.extend([o for o in selected if getattr(o, "code", "")])

            if len(option_contracts_to_snap) >= self.OPTION_TOTAL_CAP:
                option_contracts_to_snap = option_contracts_to_snap[:self.OPTION_TOTAL_CAP]
                break

        # 7) Fetch option volumes
        print(f"[Discovery] Fetching option volumes (contracts={len(option_contracts_to_snap)})...")
        opt_vol_map: Dict[str, int] = {}
        if option_contracts_to_snap:
            opt_vol_map = self._fetch_snapshot_volumes(option_contracts_to_snap, chunk_size=200)

        # 8) Aggregate option volume per underlying
        opt_vol_by_underlying: Dict[str, int] = {}
        for u_code, opt_codes in opt_codes_by_underlying.items():
            total = 0
            for oc in opt_codes:
                total += opt_vol_map.get(oc, 0)
            opt_vol_by_underlying[u_code] = total

        # 9) Rank eligible underlyings by (stock_vol + option_vol) DESC
        def score(u_code: str) -> int:
            return int(stk_vol_map.get(u_code, 0) + opt_vol_by_underlying.get(u_code, 0))

        eligible_sorted = sorted(eligible_underlyings, key=score, reverse=True)

        # 10) Add top by ranking until limit
        print("[Discovery] Adding top pairs by (StockVol + OptionVol) after passing eligibility gate...")
        for u_code in eligible_sorted:
            if len(found) >= limit:
                break
            if any(p[0] == u_code for p in found):
                continue
            f_c = u2f.get(u_code)
            if not f_c:
                continue
            f_code = getattr(f_c, "code", "")
            if not f_code:
                continue

            s_vol = stk_vol_map.get(u_code, 0)
            f_vol = fut_vol_map.get(f_code, 0)
            o_vol = opt_vol_by_underlying.get(u_code, 0)
            sc = s_vol + o_vol

            found.append((u_code, f_code))
            print(f"[Discovery] Added: {u_code}<->{f_code} | StockVol={s_vol} FutVol={f_vol} OptVol={o_vol} Score={sc}")

        # 11) fallback
        if not found:
            print("[Discovery] No pairs found under current volume thresholds. Fallback attempting 2330...")
            if resolve_stock_contract(self.api, "2330") and "2330" in u2f:
                f_c = u2f["2330"]
                f_code = getattr(f_c, "code", "")
                if f_code:
                    found.append(("2330", f_code))

        final_list = found[:limit]
        print(f"[Discovery] Final Pairs: {final_list}")
        return final_list


# --- Transaction Manager ---

@dataclass
class PairHealth:
    last_good_ts: float = 0.0
    stale_count: int = 0
    last_resub_ts: float = 0.0
    cooldown_until: float = 0.0
    reason: str = ""


class TransactionManager:
    def __init__(self):
        self._lock = threading.Lock()
        self.market_data = MarketData(on_tick_callback=self._on_market_tick)
        self.execution = ExecutionEngine()

        # Track held codes (best-effort). We keep as two sets for clarity.
        self.held_stock_positions: set[str] = set()
        self.held_future_positions: set[str] = set()

        # ===== Quote & Health Controls =====
        self.QUOTE_STALE_SEC = 5.0     # pair quote must be within this age to trade
        self.PAIR_STALE_KICK_SEC = 10.0  # if a pair is stale for too long, cooldown + resub
        self.PAIR_COOLDOWN_ON_STALE_SEC = 60
        self.PAIR_RESUB_MIN_INTERVAL_SEC = 60
        # ==================================

        # ===== Book size gates (YOU CAN TUNE) =====
        self.MIN_STK_BOOK_QTY = 10   # 股票：最佳一檔 bid/ask 量需 >= 10 才允許市價
        self.MIN_FUT_BOOK_QTY = 10   # 期貨：最佳一檔 bid/ask 量需 >= 10 才允許市價
        # ========================================

        self.active_transactions: Dict[str, BaseTransaction] = {}
        self.completed_transactions: List[BaseTransaction] = []
        self.cooldowns: Dict[str, float] = {}  # per-code cooldown (stock/future)

        self.pair_health: Dict[Tuple[str, str], PairHealth] = {}

        def _check_holdings(stock_code: str) -> bool:
            # holding definition: has stock position OR active tx on that stock
            with self._lock:
                if stock_code in self.active_transactions:
                    return True
            return (stock_code in self.held_stock_positions)

        self.strategy = StrategyEngine(self.market_data, check_holdings_cb=_check_holdings)

        self.monitored_pairs: List[Tuple[str, str]] = []
        self._pair_map: Dict[str, List[Tuple[str, str]]] = {}

        self.execution.on_order_callback = self.on_execution_event

        self.running = False
        self.last_sync_ts = 0.0
        self._last_status_refresh = 0.0

        # history persistence
        self.HISTORY_PATH = "order_history.jsonl"
        self._history_lock = threading.Lock()

    def start(self):
        if not self.execution.login():
            return False

        self.running = True

        @self.execution.api.on_bidask_stk_v1(bind=True)
        def _on_stk_callback(self_api, exchange, bidask):
            self.market_data.update_stock(bidask)

        @self.execution.api.on_bidask_fop_v1(bind=True)
        def _on_fop_callback(self_api, exchange, bidask):
            self.market_data.update_future(bidask)

        self._sync_held_positions()

        @self.execution.api.on_tick_stk_v1(bind=True)
        def _on_tick_stk_callback(self_api, exchange, tick):
            self.market_data.update_stock(tick)

        @self.execution.api.on_tick_fop_v1(bind=True)
        def _on_tick_fop_callback(self_api, exchange, tick):
            self.market_data.update_future(tick)

        return True

    def stop(self):
        self.running = False

    def discover_and_subscribe(self):
        discovery = PairDiscovery(self.execution.api)
        self.monitored_pairs = discovery.scan(limit=20)
        self._build_pair_map()

    def _sync_held_positions_fast(self):
        try:
            pos_map = self.execution.get_all_positions()
            cur_stk = set()
            cur_fut = set()

            # classify by presence in Contracts (best-effort)
            for code, qty in pos_map.items():
                if qty == 0:
                    continue
                # try resolve as stock
                if resolve_stock_contract(self.execution.api, code) is not None:
                    cur_stk.add(code)
                else:
                    cur_fut.add(code)

            # also include currently active tx codes as held (risk-first)
            with self._lock:
                for tx in self.active_transactions.values():
                    cur_stk.add(tx.intent.stock_code)
                    cur_fut.add(tx.intent.future_code)

            self.held_stock_positions = cur_stk
            self.held_future_positions = cur_fut
        except Exception:
            pass

    def _sync_held_positions(self):
        print("[TxMgr] Syncing initial positions (Wait 2s for API)...")
        time.sleep(2.0)
        try:
            if self.execution.stock_account:
                self.execution.api.update_status(self.execution.stock_account)
                print("[TxMgr] Stock Account Status Updated.")
            if self.execution.futopt_account:
                self.execution.api.update_status(self.execution.futopt_account)
                print("[TxMgr] Future Account Status Updated.")

            pos_map = self.execution.get_all_positions()
            print(f"[TxMgr] Raw Positions Attempt 1: {pos_map}")

            if not pos_map:
                print("[TxMgr] Retrying sync in 2s...")
                time.sleep(2.0)
                if self.execution.stock_account:
                    self.execution.api.update_status(self.execution.stock_account)
                pos_map = self.execution.get_all_positions()
                print(f"[TxMgr] Raw Positions Attempt 2: {pos_map}")

            for code, qty in pos_map.items():
                if qty == 0:
                    continue
                if resolve_stock_contract(self.execution.api, code) is not None:
                    self.held_stock_positions.add(code)
                else:
                    self.held_future_positions.add(code)

            print(f"[TxMgr] Initial Sync: Stocks={len(self.held_stock_positions)} Futures={len(self.held_future_positions)}")
            if self.held_stock_positions:
                print(f"[TxMgr] Held Stocks: {self.held_stock_positions}")
            if self.held_future_positions:
                print(f"[TxMgr] Held Futures: {self.held_future_positions}")
        except Exception as e:
            print(f"[TxMgr] Initial Sync Failed: {e}")

    def _build_pair_map(self):
        self._pair_map.clear()
        for s, f in self.monitored_pairs:
            if s not in self._pair_map:
                self._pair_map[s] = []
            self._pair_map[s].append((s, f))

            if f not in self._pair_map:
                self._pair_map[f] = []
            self._pair_map[f].append((s, f))

            if (s, f) not in self.pair_health:
                self.pair_health[(s, f)] = PairHealth(last_good_ts=time.time())

        print(f"[TxMgr] Indexed {len(self.monitored_pairs)} pairs into {len(self._pair_map)} unique codes.")

        print(f"[TxMgr] Subscribing to {len(self.monitored_pairs)} pairs...")
        for s, f in self.monitored_pairs:
            print(f"[TxMgr] Subscribed: {s} <-> {f}")
            c_stk = resolve_stock_contract(self.execution.api, s)
            c_fut = resolve_future_contract(self.execution.api, f)

            if not c_stk:
                print(f"[SUBSCRIBE][ERR] Stock contract NOT FOUND: {s}")
            if not c_fut:
                print(f"[SUBSCRIBE][ERR] Future contract NOT FOUND: {f}")

            if c_stk:
                try:
                    self.execution.api.quote.subscribe(c_stk, quote_type=sj_constant.QuoteType.Tick)
                except Exception:
                    pass
                try:
                    self.execution.api.quote.subscribe(c_stk, quote_type=sj_constant.QuoteType.BidAsk)
                except Exception:
                    pass
            if c_fut:
                try:
                    self.execution.api.quote.subscribe(c_fut, quote_type=sj_constant.QuoteType.Tick)
                except Exception:
                    pass
                try:
                    self.execution.api.quote.subscribe(c_fut, quote_type=sj_constant.QuoteType.BidAsk)
                except Exception:
                    pass

    def _resubscribe_pair_best_effort(self, stock_code: str, future_code: str, reason: str = ""):
        """
        Best-effort unsubscribe + subscribe to refresh quote stream.
        """
        now = time.time()
        ph = self.pair_health.get((stock_code, future_code))
        if not ph:
            ph = PairHealth()
            self.pair_health[(stock_code, future_code)] = ph

        if now - (ph.last_resub_ts or 0.0) < self.PAIR_RESUB_MIN_INTERVAL_SEC:
            return

        ph.last_resub_ts = now
        ph.reason = reason

        c_stk = resolve_stock_contract(self.execution.api, stock_code)
        c_fut = resolve_future_contract(self.execution.api, future_code)

        print(f"\n[TxMgr][RESUB] {stock_code}<->{future_code} reason={reason}")

        # Unsubscribe (best-effort)
        if c_stk:
            for qt in (sj_constant.QuoteType.Tick, sj_constant.QuoteType.BidAsk):
                try:
                    self.execution.api.quote.unsubscribe(c_stk, quote_type=qt)
                except Exception:
                    pass
        if c_fut:
            for qt in (sj_constant.QuoteType.Tick, sj_constant.QuoteType.BidAsk):
                try:
                    self.execution.api.quote.unsubscribe(c_fut, quote_type=qt)
                except Exception:
                    pass

        # Subscribe again
        if c_stk:
            for qt in (sj_constant.QuoteType.Tick, sj_constant.QuoteType.BidAsk):
                try:
                    self.execution.api.quote.subscribe(c_stk, quote_type=qt)
                except Exception:
                    pass
        if c_fut:
            for qt in (sj_constant.QuoteType.Tick, sj_constant.QuoteType.BidAsk):
                try:
                    self.execution.api.quote.subscribe(c_fut, quote_type=qt)
                except Exception:
                    pass

    def cooldown_pair(self, stock_code: str, future_code: str, sec: int, reason: str = ""):
        until = time.time() + sec
        self.cooldowns[stock_code] = max(self.cooldowns.get(stock_code, 0), until)
        self.cooldowns[future_code] = max(self.cooldowns.get(future_code, 0), until)
        ph = self.pair_health.get((stock_code, future_code))
        if not ph:
            ph = PairHealth()
            self.pair_health[(stock_code, future_code)] = ph
        ph.cooldown_until = max(ph.cooldown_until, until)
        ph.reason = reason

    def _pair_in_cooldown(self, stock_code: str, future_code: str) -> bool:
        now = time.time()
        if now < self.cooldowns.get(stock_code, 0):
            return True
        if now < self.cooldowns.get(future_code, 0):
            return True
        ph = self.pair_health.get((stock_code, future_code))
        if ph and now < (ph.cooldown_until or 0.0):
            return True
        return False

    def _pair_preflight(self, intent: TradeIntent) -> bool:
        """
        Before accepting a Tx, ensure the quote is fresh and the required L1 sizes exist.
        This reduces "leg2 blocked -> repair" frequency.
        """
        s = intent.stock_code
        f = intent.future_code

        if not self.market_data.is_pair_fresh(s, f, self.QUOTE_STALE_SEC):
            return False

        stk = self.market_data.get_stock(s)
        fut = self.market_data.get_future(f)

        if not stk or not fut:
            return False

        # Must have bid/ask
        if stk.bid <= 0 or stk.ask <= 0 or fut.bid <= 0 or fut.ask <= 0:
            return False

        # For OPEN: need buy stock @ask (ask_size) and sell future @bid (bid_size)
        if intent.type == SignalType.OPEN:
            if stk.ask_size < self.MIN_STK_BOOK_QTY:
                return False
            if fut.bid_size < self.MIN_FUT_BOOK_QTY:
                return False

        # For CLOSE: close with buy future @ask (ask_size), sell stock @bid (bid_size)
        elif intent.type == SignalType.CLOSE:
            if fut.ask_size < self.MIN_FUT_BOOK_QTY:
                return False
            if stk.bid_size < self.MIN_STK_BOOK_QTY:
                return False

        return True

    def _on_market_tick(self, code: str):
        affected_pairs = self._pair_map.get(code)
        if not affected_pairs:
            return

        now = time.time()

        for s, f in affected_pairs:
            # pair-level health checks
            stk = self.market_data.get_stock(s)
            fut = self.market_data.get_future(f)

            is_fresh = self.market_data.is_pair_fresh(s, f, self.QUOTE_STALE_SEC)
            ph = self.pair_health.get((s, f))
            if not ph:
                ph = PairHealth(last_good_ts=now)
                self.pair_health[(s, f)] = ph

            if is_fresh:
                ph.last_good_ts = now
                ph.stale_count = 0
            else:
                # stale bookkeeping
                ph.stale_count += 1
                age_s = (now - (stk.ts or 0.0)) if stk else 999
                age_f = (now - (fut.ts or 0.0)) if fut else 999
                if (age_s > self.PAIR_STALE_KICK_SEC) or (age_f > self.PAIR_STALE_KICK_SEC):
                    # cooldown + resubscribe
                    if not self._pair_in_cooldown(s, f):
                        print(f"\n[TxMgr][STALE] {s}<->{f} age_s={age_s:.2f}s age_f={age_f:.2f}s -> cooldown+resub")
                    self.cooldown_pair(s, f, sec=self.PAIR_COOLDOWN_ON_STALE_SEC, reason="pair stale kick")
                    self._resubscribe_pair_best_effort(s, f, reason=f"stale kick age_s={age_s:.1f} age_f={age_f:.1f}")
                continue

            if self._pair_in_cooldown(s, f):
                continue

            with self._lock:
                if s in self.active_transactions:
                    continue

            intent = self.strategy.on_tick(s, f)
            if intent:
                self.request_new_transaction(intent)

    def _check_position_safety(self, intent: TradeIntent) -> bool:
        positions = self.execution.get_all_positions()

        s_qty = positions.get(intent.stock_code, 0)
        f_qty = positions.get(intent.future_code, 0)

        if intent.type == SignalType.OPEN:
            if abs(s_qty) > 0 or abs(f_qty) > 0:
                print(f"[TxMgr] REJECT OPEN: Existing Position found {intent.stock_code}={s_qty}, {intent.future_code}={f_qty}")
                return False

        elif intent.type == SignalType.CLOSE:
            # Expect arbitrage shape: long stock (>0) and short future (<0)
            if not (s_qty > 0 and f_qty < 0):
                print(f"[TxMgr] REJECT CLOSE: Position not arbitrage-shape (s={s_qty}, f={f_qty}). Use Repair instead.")
                self.cooldowns[intent.stock_code] = time.time() + 60
                self.cooldowns[intent.future_code] = time.time() + 60
                return False

            if s_qty == 0 and f_qty == 0:
                return False

        return True

    def request_new_transaction(self, intent: TradeIntent) -> bool:
        with self._lock:
            if intent.stock_code in self.active_transactions:
                return False

            for k in (intent.stock_code, intent.future_code):
                if k in self.cooldowns and time.time() < self.cooldowns[k]:
                    return False

            # Pair cooldown
            if self._pair_in_cooldown(intent.stock_code, intent.future_code):
                return False

            # Preflight gate (unless force)
            if not intent.is_force:
                if not self._pair_preflight(intent):
                    print(f"[TxMgr] REJECT (Preflight): {intent.stock_code}<->{intent.future_code} ({intent.details})")
                    self.cooldown_pair(intent.stock_code, intent.future_code, sec=20, reason="preflight fail")
                    return False

            if not self._check_position_safety(intent):
                return False

            print(f"[TxMgr] ACCEPT: Starting new transaction for {intent.stock_code} ({intent.details})")

            if intent.type == SignalType.OPEN:
                tx = OpenArbitrageTransaction(intent, self)
            elif intent.type == SignalType.CLOSE:
                tx = CloseArbitrageTransaction(intent, self)
            else:
                tx = BaseTransaction(intent, self)

            self.active_transactions[intent.stock_code] = tx
            return True

    def _write_history(self, tx: BaseTransaction, extra: Optional[Dict[str, Any]] = None):
        rec = {
            "ts": time.time(),
            "time": time.strftime("%Y-%m-%d %H:%M:%S"),
            "tx_id": tx.tx_id,
            "type": tx.intent.type.name,
            "stock_code": tx.intent.stock_code,
            "fut_code": tx.intent.future_code,
            "qty": tx.intent.qty,
            "state": tx.state.name,
            "stock_seqno": tx.stock_order.seqno,
            "fut_seqno": tx.future_order.seqno,
            "stock_filled": tx.stock_order.filled_qty,
            "fut_filled": tx.future_order.filled_qty,
            "details": tx.intent.details,
        }
        if extra:
            try:
                rec.update(extra)
            except Exception:
                pass

        line = json.dumps(rec, ensure_ascii=False)
        with self._history_lock:
            try:
                with open(self.HISTORY_PATH, "a", encoding="utf-8") as f:
                    f.write(line + "\n")
            except Exception as e:
                print(f"[TxMgr] History write failed: {e}")

    def run_step(self):
        # Avoid hammering update_status every second
        now = time.time()
        try:
            if now - self._last_status_refresh > 2.0:
                self._last_status_refresh = now
                if self.execution.stock_account:
                    self.execution.api.update_status(self.execution.stock_account)
                if self.execution.futopt_account:
                    self.execution.api.update_status(self.execution.futopt_account)
        except Exception:
            pass

        if time.time() - self.last_sync_ts > 30:
            self.last_sync_ts = time.time()
            self._sync_held_positions_fast()

        print(f"[Main] Active: {len(self.active_transactions)} | Monitored: {len(self.monitored_pairs)} | {time.strftime('%H:%M:%S')}", end='\r')

        with self._lock:
            codes = list(self.active_transactions.keys())

        clean_up_list = []

        for code in codes:
            with self._lock:
                tx = self.active_transactions.get(code)
            if not tx:
                continue

            tx.update()

            if tx.state in [TransactionState.COMPLETED, TransactionState.FAILED, TransactionState.CANCELLED]:
                print(f"\n[TxMgr] Archiving Tx {tx.tx_id} state={tx.state.name}")
                self.completed_transactions.append(tx)

                # write history on terminal state
                try:
                    self._write_history(tx)
                except Exception:
                    pass

                if tx.state == TransactionState.FAILED:
                    print(f"[TxMgr] Apply Cooldown (60s) for {code}")
                    with self._lock:
                        self.cooldowns[code] = time.time() + 60
                        self.cooldowns[tx.intent.future_code] = time.time() + 60

                clean_up_list.append(code)

        if clean_up_list:
            with self._lock:
                for code in clean_up_list:
                    if code in self.active_transactions:
                        del self.active_transactions[code]

    # =========================
    # Targeted Repair (Pair Only)
    # =========================

    def repair_pair(self, stock_code: str, future_code: str, reason: str = "", flatten_both: bool = True, dry_run: bool = False):
        """
        Targeted repair that ONLY touches (stock_code, future_code).
        flatten_both=True: force both legs to 0 positions (emergency safe mode).
        """
        print(f"\n[TxMgr] === REPAIR PAIR {stock_code}<->{future_code} flatten_both={flatten_both} dry_run={dry_run} reason={reason} ===")

        try:
            if self.execution.stock_account:
                self.execution.api.update_status(self.execution.stock_account)
            if self.execution.futopt_account:
                self.execution.api.update_status(self.execution.futopt_account)
            time.sleep(0.5)
        except Exception:
            pass

        pos = self.execution.get_all_positions()
        s_qty = int(pos.get(stock_code, 0) or 0)
        f_qty = int(pos.get(future_code, 0) or 0)

        print(f"[RepairPair] Current pos: Stock {stock_code}={s_qty}, Future {future_code}={f_qty}")

        # For safety, cooldown this pair a bit
        self.cooldown_pair(stock_code, future_code, sec=60, reason=f"repair_pair: {reason}")

        # If not flatten_both, we could attempt to hedge only missing leg;
        # but to be safe (your preference in prior discussion), we default to flatten both.
        actions = []

        if flatten_both:
            if s_qty != 0:
                act = "Sell" if s_qty > 0 else "Buy"
                qty = abs(s_qty)
                actions.append(("Stock", stock_code, act, qty))
            if f_qty != 0:
                act = "Sell" if f_qty > 0 else "Buy"
                qty = abs(f_qty)
                actions.append(("Future", future_code, act, qty))
        else:
            # Minimal hedging mode (best-effort):
            # If stock exists but future is 0, flatten stock.
            # If future exists but stock is 0, flatten future.
            if s_qty != 0 and f_qty == 0:
                act = "Sell" if s_qty > 0 else "Buy"
                actions.append(("Stock", stock_code, act, abs(s_qty)))
            if f_qty != 0 and s_qty == 0:
                act = "Sell" if f_qty > 0 else "Buy"
                actions.append(("Future", future_code, act, abs(f_qty)))

        if not actions:
            print("[RepairPair] Nothing to do (already flat or no mismatch).")
            return

        for prod, code, act, qty in actions:
            print(f"[RepairPair] {prod}: {act} {code} qty={qty}")
            if dry_run:
                continue
            if prod == "Stock":
                self.execution.place_stock_order(code, act, 0, qty, "MKT", "ROD")
            else:
                self.execution.place_future_order(code, act, 0, qty, "MKT", "IOC")

        print("[RepairPair] Done.\n")

    # Kept original aggressive API but now delegates to pair repair to avoid collateral damage
    def repair_positions(self):
        print("\n[TxMgr] === REPAIRING POSITIONS (Delegated to Pair Repair) ===")
        for s, f in self.monitored_pairs:
            # Only touch pairs that have mismatch or any position exists on either leg
            pos = self.execution.get_all_positions()
            s_qty = int(pos.get(s, 0) or 0)
            f_qty = int(pos.get(f, 0) or 0)
            if s_qty != 0 or f_qty != 0:
                self.repair_pair(s, f, reason="manual repair_positions()", flatten_both=True, dry_run=False)

        print("[Repair] Scan complete.\n")

    def repair_flatten_unpaired_futures(self, dry_run: bool = True, cooldown_sec: int = 30):
        print(f"\n[TxMgr] Fixing Unpaired Futures... (dry_run={dry_run})")

        try:
            if self.execution.stock_account:
                self.execution.api.update_status(self.execution.stock_account)
            if self.execution.futopt_account:
                self.execution.api.update_status(self.execution.futopt_account)
            time.sleep(1.0)
        except Exception:
            pass

        s_holdings = {}
        if self.execution.stock_account:
            try:
                for p in self.execution.api.list_positions(self.execution.stock_account):
                    s_holdings[p.code] = int(p.quantity)
            except Exception:
                pass

        f_holdings = {}
        if self.execution.futopt_account:
            try:
                for p in self.execution.api.list_positions(self.execution.futopt_account):
                    qty = int(p.quantity)
                    net = qty if p.direction == sj_constant.Action.Buy else -qty
                    f_holdings[p.code] = net
            except Exception:
                pass

        for s, f in self.monitored_pairs:
            if s_holdings.get(s, 0) != 0:
                self.held_stock_positions.add(s)
            elif s in self.held_stock_positions:
                self.held_stock_positions.remove(s)

            s_qty = s_holdings.get(s, 0)
            f_qty = f_holdings.get(f, 0)

            if s_qty != 0 or f_qty == 0:
                continue

            exp = self.cooldowns.get(s, 0)
            if time.time() < exp:
                continue

            action = "Sell" if f_qty > 0 else "Buy"
            closing_qty = abs(f_qty)

            print(f"[Repair] {s}/{f}: Stock=0, Fut={f_qty} -> {action} {closing_qty} (flatten)")

            self.cooldowns[s] = time.time() + cooldown_sec
            self.cooldowns[f] = time.time() + cooldown_sec

            if dry_run:
                continue

            self.execution.place_future_order(f, action, 0, closing_qty, "MKT", "IOC")

        print("[Repair] Done.\n")

    def on_execution_event(self, oid, seqno, op_code, op_msg, status_data):
        found_active = False

        with self._lock:
            active_txs = list(self.active_transactions.values())

        for tx in active_txs:
            if tx.stock_order.seqno and tx.stock_order.seqno == seqno:
                if status_data.get("deal_quantity", 0):
                    self.held_stock_positions.add(tx.intent.stock_code)

                tx.on_order_update(seqno, op_code, msg=op_msg, status_data=status_data)
                found_active = True
                break

            if tx.future_order.seqno and tx.future_order.seqno == seqno:
                if status_data.get("deal_quantity", 0):
                    self.held_future_positions.add(tx.intent.future_code)

                tx.on_order_update(seqno, op_code, msg=op_msg, status_data=status_data)
                found_active = True
                break

        if found_active:
            return

        # Late fill detection for archived txs
        deal_qty = 0
        try:
            deal_qty = int(status_data.get("deal_quantity", 0) or 0)
        except Exception:
            deal_qty = 0

        if deal_qty > 0:
            for tx in self.completed_transactions:
                if (tx.stock_order.seqno and tx.stock_order.seqno == seqno) or \
                   (tx.future_order.seqno and tx.future_order.seqno == seqno):
                    print(f"\n[TxMgr] ⚠️ CRITICAL: Late Fill detected for Archived Tx {tx.tx_id} (Seq={seqno})!")
                    print("[TxMgr] This causes a Naked Position. Triggering Targeted Auto-Repair...")
                    self.repair_pair(tx.intent.stock_code, tx.intent.future_code, reason="late fill archived tx", flatten_both=True)
                    break

    def dump_status(self) -> str:
        lines = ["\n=== STATUS ==="]
        lines.append(f"Monitored Pairs: {len(self.monitored_pairs)}")
        for s, f in self.monitored_pairs:
            stk_data = self.market_data.get_stock(s)
            fut_data = self.market_data.get_future(f)

            s_bid = stk_data.bid if stk_data else 0.0
            s_ask = stk_data.ask if stk_data else 0.0
            f_bid = fut_data.bid if fut_data else 0.0
            f_ask = fut_data.ask if fut_data else 0.0

            spread_open = (f_bid - s_ask) if (f_bid > 0 and s_ask > 0) else 0.0
            spread_close = (f_ask - s_bid) if (f_ask > 0 and s_bid > 0) else 0.0

            st_open = self.strategy.stats.get((s, f, "OPEN"))
            st_close = self.strategy.stats.get((s, f, "CLOSE"))

            extra = []
            if st_open and st_open.samples > 0:
                std = max(st_open.std, self.strategy.MIN_STD)
                z = (spread_open - st_open.mean) / std if std > 0 else 0.0
                extra.append(f"OPEN Sprd={spread_open:.1f} Z={z:.2f} (Avg={st_open.mean:.1f})")
            else:
                extra.append("OPEN (No Stats)")

            if st_close and st_close.samples > 0:
                std = max(st_close.std, self.strategy.MIN_STD)
                z = (spread_close - st_close.mean) / std if std > 0 else 0.0
                extra.append(f"CLOSE Sprd={spread_close:.1f} Z={z:.2f} (Avg={st_close.mean:.1f})")
            else:
                extra.append("CLOSE (No Stats)")

            age_s = (time.time() - (stk_data.ts or 0.0)) if stk_data else 999
            age_f = (time.time() - (fut_data.ts or 0.0)) if fut_data else 999

            ph = self.pair_health.get((s, f))
            cd = 0
            rsn = ""
            if ph:
                cd = max(0, int((ph.cooldown_until or 0) - time.time()))
                rsn = ph.reason or ""

            lines.append(
                f"  {s} Bid={s_bid} Ask={s_ask} (Bsz={stk_data.bid_size if stk_data else 0}/Asz={stk_data.ask_size if stk_data else 0}) age={age_s:.1f}s "
                f"<-> {f} Bid={f_bid} Ask={f_ask} (Bsz={fut_data.bid_size if fut_data else 0}/Asz={fut_data.ask_size if fut_data else 0}) age={age_f:.1f}s "
                f"| CD={cd}s {('RSN='+rsn) if rsn else ''} | {' | '.join(extra)}"
            )

        lines.append(f"\nActive Transactions: {len(self.active_transactions)}")
        for code, tx in self.active_transactions.items():
            lines.append(f"  [{code}] State={tx.state.name} Created={time.strftime('%H:%M:%S', time.localtime(tx.created_at))}")
            lines.append(f"    Leg1(Stock): {tx.stock_order.status} ({tx.stock_order.filled_qty}/{tx.intent.qty}) seq={tx.stock_order.seqno}")
            lines.append(f"    Leg2(Fut):   {tx.future_order.status} ({tx.future_order.filled_qty}) seq={tx.future_order.seqno}")

        lines.append(self.execution.dump_positions())

        return "\n".join(lines)


# --- Interactive Keyboard ---
try:
    import termios, tty, select
    _HAS_TERMIOS = True
except Exception:
    _HAS_TERMIOS = False

try:
    import msvcrt
    _HAS_MS = True
except Exception:
    _HAS_MS = False


class KeyboardMonitor(threading.Thread):
    """
    p -> print status/positions
    o -> print orders
    h -> help
    q -> quit
    x -> repair (close naked positions)
    X -> force repair (same as x, but you can separate later if you want)
    """
    def __init__(self, manager: 'TransactionManager', poll_sec: float = 0.5):
        super().__init__(daemon=True)
        self.mgr = manager
        self.poll_sec = poll_sec
        self._stop_evt = threading.Event()

    def stop(self):
        self._stop_evt.set()

    def run(self):
        self._print_help_once()
        if _HAS_MS:
            self._run_windows()
        elif _HAS_TERMIOS:
            self._run_posix()
        else:
            print("[Keyboard] No interactive support (no termios/msvcrt).")
            while not self._stop_evt.is_set():
                time.sleep(1.0)

    def _print_help_once(self):
        print("\n[Keyboard] p=status, o=orders, h=help, q=quit, x=repair\n", flush=True)

    def _handle_key(self, ch: str):
        ch = (ch or "").strip()
        if not ch:
            return

        if ch.lower() == "p":
            print(self.mgr.dump_status(), flush=True)
        elif ch.lower() == "o":
            print(self.mgr.execution.dump_orders(), flush=True)
        elif ch == "x":
            print("[Keyboard] 'x' pressed: Running repair (pair-targeted)...")
            self.mgr.repair_positions()
        elif ch == "X":
            print("[Keyboard] 'X' pressed: FORCE REPAIR started...")
            self.mgr.repair_positions()
        elif ch == "h":
            self._print_help_once()
        elif ch == "q":
            print("\n[Keyboard] Quit requested...", flush=True)
            self.mgr.stop()
            self.stop()
            print("[Keyboard] Cancelling ALL open orders...", flush=True)
            self.mgr.execution.cancel_all_orders()

    def _run_windows(self):
        while not self._stop_evt.is_set():
            if msvcrt.kbhit():
                ch = msvcrt.getwch()
                self._handle_key(ch)
            time.sleep(self.poll_sec)

    def _run_posix(self):
        fd = sys.stdin.fileno()
        try:
            old = termios.tcgetattr(fd)
        except Exception:
            return

        try:
            tty.setcbreak(fd)
            while not self._stop_evt.is_set():
                r, _, _ = select.select([sys.stdin], [], [], self.poll_sec)
                if r:
                    ch = sys.stdin.read(1)
                    self._handle_key(ch)
        finally:
            termios.tcsetattr(fd, termios.TCSADRAIN, old)


# --- Main ---
def main():
    logging.basicConfig(level=logging.WARNING)

    mgr = TransactionManager()

    if not mgr.start():
        print("[Main] Failed to start system.")
        return

    kb = KeyboardMonitor(mgr)
    kb.start()

    print("[Main] System Started. Press 'h' for help.")

    try:
        mgr.discover_and_subscribe()

        loops = 0
        while mgr.running:
            mgr.run_step()
            time.sleep(1.0)
            loops += 1
            if loops % 10 == 0:
                print(f"[Main] Loop {loops}...", end='\r')

    except KeyboardInterrupt:
        print("\n[Main] Interrupted.")
    except Exception as e:
        traceback.print_exc()
        print(f"[Main] Error: {e}")
    finally:
        kb.stop()
        print("[Main] Shutdown.")


if __name__ == "__main__":
    main()
