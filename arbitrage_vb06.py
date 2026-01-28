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
import dataclasses
import traceback
from dotenv import load_dotenv
import queue  # <--- Added for Event Queue

import shioaji as sj
from shioaji import constant as sj_constant
from shioaji.constant import Unit
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


class SystemConfig:
    ZOMBIE_TIMEOUT = 60.0
    STALENESS_THRESHOLD = 15.0
    REPAIR_COOLDOWN = 5.0
    MAIN_LOOP_SLEEP = 0.1
    LEG1_TIMEOUT = 90.0
    LEG2_TIMEOUT = 20.0
    DEFAULT_LOT_SIZE = 1000
    CONNECTION_CHECK_INTERVAL = 60.0
    STOCK_SHARES_PER_LOT = 1000
    DEFAULT_FUTURE_MULTIPLIER = 2000

class InstrumentRegistry:
    """
    Central registry for instrument specifications.
    Resolves multipliers and ratios dynamically from API contracts.
    """
    # {code: {'type': 'Stock'|'Future', 'multiplier': int, 'name': str}}
    _specs: Dict[str, dict] = {}
    
    @classmethod
    def register(cls, code: str, contract):
        """
        Register a contract.
        For Stock: multiplier = contract.unit (usually 1000)
        For Future: multiplier = contract.multiplier (e.g. 2000)
        """
        # Determine strict type and multiplier
        # Note: shioaji contracts might differ in structure, handle carefully.
        # Stock contract from sj.Contracts.Stocks...
        # Future contract from sj.Contracts.Futures...
        
        # We assume 'contract' is a Shioaji Contract object
        ctype = "Stock"
        mult = 1000 # Default fallback
        name = getattr(contract, "name", code)
        
        # Heuristic detection or explicit passing
        # Usually we know if we are scanning stocks or futures
        if hasattr(contract, 'multiplier'):
             # Future
             ctype = "Future"
             mult = int(contract.multiplier)
        elif hasattr(contract, 'unit'):
             # Stock
             ctype = "Stock"
             mult = int(contract.unit)
             
        cls._specs[code] = {
            "type": ctype,
            "multiplier": mult,
            "name": name
        }
        # print(f"[Registry] Registered {code} ({ctype}) swap={mult}")

    @classmethod
    def get_multiplier(cls, code: str) -> int:
        spec = cls._specs.get(code)
        if spec: return spec["multiplier"]
        # Fallback if not registered (should not happen if flow provides it)
        # Attempt safe guess? No, unsafe. return default.
        return 2000 if "F" in code or len(code)>6 else 1000

    @classmethod
    def get_ratio(cls, stock_code: str, future_code: str) -> int:
        """
        Returns Integer Ratio = Future Multiplier / Stock Multiplier.
        E.g. 2000 / 1000 = 2.
        Assumes strictly validated by PairDiscovery.
        """
        s_mult = cls.get_multiplier(stock_code)
        f_mult = cls.get_multiplier(future_code)
        if s_mult == 0: return 2 # avoid div0
        return int(f_mult // s_mult)


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
    product: str = ""          # "Stock"/"Future"
    code: str = ""             # contract code
    target_qty: int = 0        # intent qty
    submit_ts: float = 0.0     # Submission timestamp
    fut_margin_ratio: float = 0.50


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
    # Shioaji 'deal_quantity' is usually cumulative.
    # Safety: ensure we don't regress if callbacks arrive out of order (rare but possible)
    # or if we get incremental updates (though usually cumulative).
    if q > order.last_deal_qty_reported:
        delta = q - order.last_deal_qty_reported
        order.last_deal_qty_reported = q
        order.filled_qty += delta
    else:
        # q <= last reported. Ignore.
        pass
    return delta


# ============================================================
# Market Data (L1 book + ticks) + Snapshot
# ============================================================

@dataclass
class MarketSnapshot:
    code: str
    product: str  # "Stock" / "Future"
    ts: float = 0.0

    price: float = 0.0
    bid: float = 0.0
    ask: float = 0.0
    bid_size: int = 0
    ask_size: int = 0
    volume: int = 0

    def __bool__(self):
        return bool(self.code)

    def update_from_tick(self, tick: Any):
        self.ts = time.time()
        try:
            self.price = float(getattr(tick, "close", 0.0) or getattr(tick, "price", 0.0) or getattr(tick, "last_price", 0.0) or self.price)
        except Exception:
            pass
        try:
            # Prioritize total_volume (cumulative) over volume (tick qty)
            self.volume = int(getattr(tick, "total_volume", 0) or getattr(tick, "volume", 0) or self.volume)
        except Exception:
            pass
        # some tick has bid/ask too
        # for k in ("bid_price", "bid", "bidPrice"):
        #     try:
        #         v = getattr(tick, k, None)
        #         if v is not None:
        #             self.bid = float(v)
        #             break
        #     except Exception:
        #         pass
        # for k in ("ask_price", "ask", "askPrice"):
        #     try:
        #         v = getattr(tick, k, None)
        #         if v is not None:
        #             self.ask = float(v)
        #             break
        #     except Exception:
        #         pass

    def update_from_bidask(self, bidask: Any):
        self.ts = time.time()
        # bid/ask price
        for k in ("bid_price", "bid", "bidPrice"):
            try:
                v = getattr(bidask, k, None)
                if v is not None:
                    # sometimes list
                    if isinstance(v, (list, tuple)) and v:
                        self.bid = float(v[0] or 0.0)
                    else:
                        self.bid = float(v or 0.0)
                    break
            except Exception:
                pass

        for k in ("ask_price", "ask", "askPrice"):
            try:
                v = getattr(bidask, k, None)
                if v is not None:
                    if isinstance(v, (list, tuple)) and v:
                        self.ask = float(v[0] or 0.0)
                    else:
                        self.ask = float(v or 0.0)
                    break
            except Exception:
                pass

        # bid/ask size
        for k in ("bid_volume", "bid_size", "bidSize", "bidVolume"):
            try:
                v = getattr(bidask, k, None)
                if v is not None:
                    if isinstance(v, (list, tuple)) and v:
                        self.bid_size = int(v[0] or 0)
                    else:
                        self.bid_size = int(v or 0)
                    break
            except Exception:
                pass

        for k in ("ask_volume", "ask_size", "askSize", "askVolume"):
            try:
                v = getattr(bidask, k, None)
                if v is not None:
                    if isinstance(v, (list, tuple)) and v:
                        self.ask_size = int(v[0] or 0)
                    else:
                        self.ask_size = int(v or 0)
                    break
            except Exception:
                pass

        # some bidask includes price-like
        if self.price <= 0 and self.bid > 0 and self.ask > 0:
            self.price = (self.bid + self.ask) / 2.0
        elif self.price <= 0 and self.bid > 0:
            self.price = self.bid
        elif self.price <= 0 and self.ask > 0:
            self.price = self.ask


class MarketData:
    def __init__(self, on_tick_callback: Optional[Callable[[str], None]] = None, min_stk_qty: int = 1, min_fut_qty: int = 1):
        self._lock = threading.Lock()
        self._stk: Dict[str, MarketSnapshot] = {}
        self._fut: Dict[str, MarketSnapshot] = {}
        self._on_tick = on_tick_callback
        
        # CPU Optimization: Only trigger size updates if crossing these thresholds
        self.min_stk_qty = min_stk_qty
        self.min_fut_qty = min_fut_qty


    def get_stock(self, code: str) -> Optional[MarketSnapshot]:
        # Extreme Optimization for GUI: NO LOCK.
        # Direct dict access is thread-safe in CPython for retrieval.
        # We might read a 'torn' object (mid-update), but preventing GUI freeze is priority.
        snap = self._stk.get(code)
        if snap:
            # Return same instance or fast copy? 
            # Returning same instance is fastest, but GUI might see mutations during render.
            # Let's return a fast copy.
            return MarketSnapshot(
                code=snap.code, product=snap.product, ts=snap.ts,
                price=snap.price, bid=snap.bid, ask=snap.ask,
                bid_size=snap.bid_size, ask_size=snap.ask_size, volume=snap.volume
            )
        return None

    def get_future(self, code: str) -> Optional[MarketSnapshot]:
        # NO LOCK
        snap = self._fut.get(code)
        if snap:
            return MarketSnapshot(
                code=snap.code, product=snap.product, ts=snap.ts,
                price=snap.price, bid=snap.bid, ask=snap.ask,
                bid_size=snap.bid_size, ask_size=snap.ask_size, volume=snap.volume
            )
        return None

    def update_stock(self, obj: Any):
        code = getattr(obj, "code", "") or getattr(obj, "symbol", "") or ""
        if not code:
            return
            
        trigger_strategy = False
        
        with self._lock:
            snap = self._stk.get(code)
            if not snap:
                snap = MarketSnapshot(code=code, product="Stock")
                self._stk[code] = snap
            
            # Record state before update
            old_price_tuple = (snap.price, snap.bid, snap.ask)
            old_size_tuple = (snap.bid_size, snap.ask_size)
            
            if hasattr(obj, "bid_price") or hasattr(obj, "ask_price") or hasattr(obj, "bid_volume") or hasattr(obj, "ask_volume"):
                snap.update_from_bidask(obj)
            else:
                snap.update_from_tick(obj)
                
            # Compare state
            new_price_tuple = (snap.price, snap.bid, snap.ask)
            new_size_tuple = (snap.bid_size, snap.ask_size)

            # 1. Price/Quote Changed? -> Always Trigger
            if new_price_tuple != old_price_tuple:
                trigger_strategy = True
            
            # 2. Size Changed? -> Only Trigger if "Crossing the Gate" (Optimization)
            elif new_size_tuple != old_size_tuple:
                # Check Bid Size Crossing
                # "Old < Threshold <= New"
                if (old_size_tuple[0] < self.min_stk_qty <= new_size_tuple[0]):
                    trigger_strategy = True
                # Check Ask Size Crossing
                elif (old_size_tuple[1] < self.min_stk_qty <= new_size_tuple[1]):
                    trigger_strategy = True
                # Else: Size changed but didn't cross the useful threshold (e.g. 2->3 or 50->60), ignore.

        if trigger_strategy and self._on_tick:
            try:
                self._on_tick(code)
            except Exception:
                pass


    def update_future(self, obj: Any):
        code = getattr(obj, "code", "") or getattr(obj, "symbol", "") or ""
        if not code:
            return
        
        trigger_strategy = False
        
        with self._lock:
            snap = self._fut.get(code)
            if not snap:
                snap = MarketSnapshot(code=code, product="Future")
                self._fut[code] = snap

            # Record state before update
            old_price_tuple = (snap.price, snap.bid, snap.ask)
            old_size_tuple = (snap.bid_size, snap.ask_size)

            if hasattr(obj, "bid_price") or hasattr(obj, "ask_price") or hasattr(obj, "bid_volume") or hasattr(obj, "ask_volume"):
                snap.update_from_bidask(obj)
            else:
                snap.update_from_tick(obj)

            # Compare state
            new_price_tuple = (snap.price, snap.bid, snap.ask)
            new_size_tuple = (snap.bid_size, snap.ask_size)

            # 1. Price/Quote Changed? -> Always Trigger
            if new_price_tuple != old_price_tuple:
                trigger_strategy = True
            
            # 2. Size Changed? -> Only Trigger if "Crossing the Gate"
            elif new_size_tuple != old_size_tuple:
                if (old_size_tuple[0] < self.min_fut_qty <= new_size_tuple[0]):
                     trigger_strategy = True
                elif (old_size_tuple[1] < self.min_fut_qty <= new_size_tuple[1]):
                     trigger_strategy = True

        if trigger_strategy and self._on_tick:
            try:
                self._on_tick(code)
            except Exception:
                pass


# ============================================================
# Strategy Engine (spread stats + signals)
# ============================================================

@dataclass
class RunningStat:
    n: int = 0
    mean: float = 0.0
    m2: float = 0.0
    std: float = 0.0
    samples: int = 0

    def update(self, x: float):
        self.n += 1
        self.samples = self.n
        delta = x - self.mean
        self.mean += delta / self.n
        delta2 = x - self.mean
        self.m2 += delta * delta2
        if self.n >= 2:
            var = self.m2 / (self.n - 1)
            self.std = math.sqrt(max(0.0, var))
        else:
            self.std = 0.0


class StrategyEngine:
    """
    你可以把這裡當作可調參數區：
      - 使用 spread_open = fut_bid - stk_ask
      - spread_close = fut_ask - stk_bid
    以 RunningStat 估 mean/std，當 zscore 達到門檻給出 OPEN/CLOSE intent。
    """
    def __init__(self, market_data: MarketData,
                 check_holdings_cb: Optional[Callable[[str, str], bool]] = None,
                 get_book_gate_cb: Optional[Callable[[], Tuple[int, int]]] = None):
        self.md = market_data
        self.check_holdings_cb = check_holdings_cb or (lambda s, f: False)
        self.get_book_gate_cb = get_book_gate_cb or (lambda: (10, 10))
        self._lock = threading.Lock()

        self.stats: Dict[Tuple[str, str, str], RunningStat] = {}

        # ====== Strategy thresholds (tunable) ======
        self.MIN_SAMPLES = 80
        self.MIN_STD = 1e-6
        self.OPEN_Z = 2.0
        self.CLOSE_Z = -0.5  # close when spread mean-reverts (can tune)
        self.MIN_EDGE_PCT = 0.0005  # 0.05%
        self.DEFAULT_QTY = 1
        # =========================================

        self._cooldown: Dict[Tuple[str, str], float] = {}
        self.COOLDOWN_SEC = SystemConfig.REPAIR_COOLDOWN
        self.MAX_STALENESS = SystemConfig.STALENESS_THRESHOLD

    def _get_stat(self, s: str, f: str, mode: str) -> RunningStat:
        k = (s, f, mode)
        if k not in self.stats:
            self.stats[k] = RunningStat()
        return self.stats[k]

    def on_tick(self, stock_code: str, future_code: str) -> Optional[TradeIntent]:
        stk = self.md.get_stock(stock_code)
        fut = self.md.get_future(future_code)
        if not stk or not fut:
            return None

        # require valid L1
        if stk.bid <= 0 and stk.ask <= 0 and stk.price <= 0:
            return None
        if fut.bid <= 0 and fut.ask <= 0 and fut.price <= 0:
            return None

        with self._lock:
            # compute spreads
            s_ask = stk.ask if stk.ask > 0 else stk.price
            s_bid = stk.bid if stk.bid > 0 else stk.price
            f_bid = fut.bid if fut.bid > 0 else fut.price
            f_ask = fut.ask if fut.ask > 0 else fut.price
            if s_ask <= 0 or s_bid <= 0 or f_bid <= 0 or f_ask <= 0:
                return None

            spread_open = f_bid - s_ask
            spread_close = f_ask - s_bid

            st_open = self._get_stat(stock_code, future_code, "OPEN")
            st_close = self._get_stat(stock_code, future_code, "CLOSE")

            # update stats
            st_open.update(spread_open)
            st_close.update(spread_close)

            # cooldown
            now = time.time()

            # Data Staleness Gate
            if (now - stk.ts > self.MAX_STALENESS) or (now - fut.ts > self.MAX_STALENESS):
                return None

            cd_key = (stock_code, future_code)
            exp = self._cooldown.get(cd_key, 0.0)
            if now < exp:
                return None

            # avoid entering if already holding/active
            is_holding = self.check_holdings_cb(stock_code, future_code)

            # book gate thresholds
            min_stk_book, min_fut_book = self.get_book_gate_cb()

            # OPEN signal:
            if not is_holding and st_open.samples >= self.MIN_SAMPLES:
                std = max(st_open.std, self.MIN_STD)
                z = (spread_open - st_open.mean) / std
                edge_pct = (f_bid - s_ask) / max(1e-9, s_ask)
                if z >= self.OPEN_Z and edge_pct >= self.MIN_EDGE_PCT:
                    # extra liquidity gate here (soft; Tx class will hard gate again)
                    if stk.ask_size < min_stk_book or fut.bid_size < min_fut_book:
                        return None
                    self._cooldown[cd_key] = now + self.COOLDOWN_SEC
                    return TradeIntent(
                        type=SignalType.OPEN,
                        stock_code=stock_code,
                        future_code=future_code,
                        qty=self.DEFAULT_QTY,
                        details=f"OPEN z={z:.2f} edge={edge_pct*100:.2f}% sprd={spread_open:.2f} mean={st_open.mean:.2f} std={std:.2f}"
                    )

            # CLOSE signal: if we are holding (or manager will check shape)
            # Strategy only emits close when spread_close is "low" vs mean (mean-revert).
            if is_holding and st_close.samples >= self.MIN_SAMPLES:
                std = max(st_close.std, self.MIN_STD)
                z = (spread_close - st_close.mean) / std
                if z <= self.CLOSE_Z:
                    # we let TxMgr _check_position_safety() decide if shape is correct
                    self._cooldown[cd_key] = now + self.COOLDOWN_SEC
                    return TradeIntent(
                        type=SignalType.CLOSE,
                        stock_code=stock_code,
                        future_code=future_code,
                        qty=self.DEFAULT_QTY,
                        details=f"CLOSE z={z:.2f} sprd={spread_close:.2f} mean={st_close.mean:.2f} std={std:.2f}"
                    )

            return None


# ============================================================
# Portfolio Ledger + Reporter
# ============================================================

@dataclass
class PositionLine:
    code: str
    product: str  # "Stock" / "Future"
    qty: int = 0
    avg_price: float = 0.0
    last_price: float = 0.0
    direction: str = ""  # "Buy"/"Sell" or ""

    def market_value(self, stock_multiplier: int = 1, fut_multiplier: int = 1) -> float:
        px = self.last_price if self.last_price > 0 else self.avg_price
        if self.product == "Stock":
            return px * self.qty * stock_multiplier
        else:
            return px * self.qty * fut_multiplier


class PortfolioLedger:
    """
    只做 best-effort：用 positions 推估 stock cost / margin used。
    cash_total 可由 sync_balance() 寫入（真實或 fallback）。
    """
    def __init__(self, fut_multiplier_default: int = 2000, fut_margin_ratio: float = 0.5):
        self.fut_multiplier_default = fut_multiplier_default 
        # Note: default multiplier used only if Registry lookup fails or specific fallback needed,
        # otherwise we now try to use Registry.
        self.fut_margin_ratio = fut_margin_ratio

        self.cash_total: float = 0.0
        self.reserved_stock: float = 0.0
        self.reserved_margin: float = 0.0

        self.positions: Dict[str, PositionLine] = {}
        self.last_sync_ts: float = 0.0

    def upsert(self, line: PositionLine):
        self.positions[line.code] = line

    def check_funds(self, req_cash: float, req_margin: float) -> bool:
        """
        Returns True if total occupied (held + reserved) + required <= cash_total.
        """
        s_cost, f_margin = self.est_holding_cost()
        occupied = s_cost + f_margin + self.reserved_stock + self.reserved_margin
        return (occupied + req_cash + req_margin) <= self.cash_total

    def reserve_funds(self, req_cash: float, req_margin: float):
        self.reserved_stock += req_cash
        self.reserved_margin += req_margin

    def release_reserved(self, req_cash: float, req_margin: float):
        self.reserved_stock = max(0.0, self.reserved_stock - req_cash)
        self.reserved_margin = max(0.0, self.reserved_margin - req_margin)

    def est_holding_cost(self) -> Tuple[float, float]:
        """
        Returns (stock_cost_est, fut_margin_est)
        Uses InstrumentRegistry for multipliers.
        """
        stock_cost = 0.0
        fut_margin = 0.0
        for p in self.positions.values():
            px = p.avg_price if p.avg_price > 0 else p.last_price
            if px <= 0:
                continue
            
            mult = InstrumentRegistry.get_multiplier(p.code)
            
            if p.product == "Stock":
                if p.qty > 0:
                    stock_cost += px * p.qty * mult
            else:
                # future: margin estimate uses abs(qty)
                fut_margin += px * abs(p.qty) * mult * self.fut_margin_ratio
        return stock_cost, fut_margin

    def snapshot(self, tag: str) -> str:
        s_cost, f_mrg = self.est_holding_cost()
        free_cash = self.cash_total - self.reserved_stock - self.reserved_margin
        lines = []
        lines.append(f"\n=== PORTFOLIO [{tag}] ===")
        lines.append(f"CashTotal : {self.cash_total:,.2f}")
        lines.append(f"Reserved  : Stock={self.reserved_stock:,.2f}  Margin={self.reserved_margin:,.2f}")
        lines.append(f"EstHold   : StockCost={s_cost:,.2f}  FutMargin={f_mrg:,.2f}")
        lines.append(f"FreeCash  : {free_cash:,.2f}")
        lines.append(f"Mult      : (dynamic per code via Registry) fut_margin_ratio={self.fut_margin_ratio}")
        lines.append("Positions :")
        if not self.positions:
            lines.append("  (none)")
        else:
            for code, p in sorted(self.positions.items(), key=lambda x: (x[1].product, x[0])):
                lines.append(f"  {p.product:<6} {code:<10} qty={p.qty:<6} avg={p.avg_price:<10.2f} last={p.last_price:<10.2f} dir={p.direction}")
        lines.append("=========================\n")
        return "\n".join(lines)


class AccountReporter:
    def __init__(self, api: sj.Shioaji, md: MarketData, stock_account, futopt_account):
        self.api = api
        self.md = md
        self.stock_account = stock_account
        self.futopt_account = futopt_account

    def summary_text(self) -> str:
        lines = ["=== ACCOUNT REPORT (best effort) ==="]
        try:
            if self.stock_account:
                lines.append(f"StockAccount: {getattr(self.stock_account, 'account_id', '')}")
            if self.futopt_account:
                lines.append(f"FutOptAccount: {getattr(self.futopt_account, 'account_id', '')}")
        except Exception:
            pass
        return "\n".join(lines)


# ============================================================
# Execution Engine (login / place / cancel / status / positions)
# ============================================================

class ExecutionEngine:
    def __init__(self):
        self.api: Optional[sj.Shioaji] = None
        self.stock_account = None
        self.futopt_account = None

        self.on_order_callback: Optional[Callable[[Any, str, str, str, Dict], None]] = None

        # local caches
        self._order_cache: Dict[str, Dict] = {}  # seqno -> status_data
        self._trade_cache: Dict[str, Any] = {}   # seqno -> trade obj
        self._contract_cache: Dict[str, Any] = {} # code -> contract
        self._lock = threading.Lock()
        self._creds: Tuple[str, str] = ("", "") # api_key, secret_key

    def login(self) -> bool:
        # Environment Variables
        load_dotenv()
        api_key = os.getenv("Sinopack_CA_API_KEY", "")
        secret_key = os.getenv("Sinopack_CA_SECRET_KEY", "")
        person_id = os.getenv("Sinopack_PERSON_ID", "")
        
        # New: CA and Simulation Config
        sim_env = os.getenv("Sinopack_SIMULATION", "True").lower()
        is_simulation = (sim_env == "true")
        
        ca_path = os.getenv("Sinopack_CA_PATH", "")
        ca_passwd = os.getenv("Sinopack_CA_PASSWD", "")

        print(f"[Execution] CWD: {os.getcwd()}")
        k_len = len(api_key) if api_key else 0
        s_len = len(secret_key) if secret_key else 0
        p_Mask = (person_id[:2] + "***" + person_id[-2:]) if person_id and len(person_id) > 4 else "***"
        print(f"[Execution] Keys loading: Sim={is_simulation}, API_KEY len={k_len}, PERSON_ID={p_Mask}")

        # Init API
        self.api = sj.Shioaji(simulation=is_simulation)


        try:
            print(f"[Execution] Connecting to Shioaji API (Sim={is_simulation})...")
            self.api.login(
                api_key=api_key,
                secret_key=secret_key,
                contracts_cb=lambda x: print(f"[Execution] Contracts loaded: {x}"),
                subscribe_trade=True
            )
            
            # CA Activation for Production
            if not is_simulation:
                if ca_path and ca_passwd:
                    print(f"[Execution] Activating CA: {ca_path}")
                    self.api.activate_ca(
                        ca_path=ca_path,
                        ca_passwd=ca_passwd,
                        person_id=person_id
                    )
                    print("[Execution] CA Activated.")
                else:
                    print("[Execution] WARNING: Production mode but CA_PATH or CA_PASSWD missing!")

            self._creds = (api_key, secret_key)
            print("[Execution] Login success (Session Up). Waiting for contracts...")
            time.sleep(5.0)
        except Exception as e:
            print(f"[Execution] Login failed: {e}")
            traceback.print_exc()
            return False

        # pick accounts
        try:
            if hasattr(self.api, "stock_account") and self.api.stock_account:
                self.stock_account = self.api.stock_account
            else:
                # fallback: pick first stock account
                if hasattr(self.api, "list_accounts"):
                    for a in self.api.list_accounts():
                        if "Stock" in str(type(a)) or getattr(a, "account_type", "") == "S":
                            self.stock_account = a
                            break
        except Exception:
            pass

        try:
            if hasattr(self.api, "futopt_account") and self.api.futopt_account:
                self.futopt_account = self.api.futopt_account
            else:
                if hasattr(self.api, "list_accounts"):
                    for a in self.api.list_accounts():
                        if "FutOpt" in str(type(a)) or getattr(a, "account_type", "") in ("F", "O"):
                            self.futopt_account = a
                            break
        except Exception:
            pass

        print(f"[Execution] Stock account: {getattr(self.stock_account, 'account_id', None)}")
        print(f"[Execution] FutOpt account: {getattr(self.futopt_account, 'account_id', None)}")

        # register order callback
        try:
            if hasattr(self.api, "set_order_callback"):
                self.api.set_order_callback(self._order_cb)
                print("[Execution] set_order_callback registered.")
        except Exception as e:
            print(f"[Execution] set_order_callback failed: {e}")

        return True

    # ------------------ callback parsing ------------------

    def _safe_to_dict(self, obj: Any) -> Dict[str, Any]:
        if obj is None:
            return {}
        if isinstance(obj, dict):
            return obj
        d = {}
        for k in dir(obj):
            if k.startswith("_"):
                continue
            try:
                v = getattr(obj, k)
            except Exception:
                continue
            if callable(v):
                continue
            # keep primitives only to avoid huge dumps
            if isinstance(v, (str, int, float, bool, type(None))):
                d[k] = v
        return d

    def _extract_seqno(self, trade: Any) -> str:
        # Try common paths (Strict seqno only)
        for path in [
            ("seqno",),
            ("order", "seqno"),
            ("status", "seqno"),
        ]:
            cur = trade
            ok = True
            for k in path:
                try:
                    cur = getattr(cur, k)
                except Exception:
                    ok = False
                    break
            if ok and cur:
                return str(cur)
        return ""

    def _extract_op(self, trade: Any) -> Tuple[str, str]:
        # op_code, op_msg best effort
        for k in ("op_code", "opCode", "code", "status_code"):
            try:
                v = getattr(trade, k, None)
                if v is not None:
                    return str(v), str(getattr(trade, "msg", "") or getattr(trade, "message", "") or "")
            except Exception:
                pass
        # sometimes in trade.status
        st = getattr(trade, "status", None)
        if st is not None:
            for k in ("op_code", "opCode", "code", "status_code"):
                try:
                    v = getattr(st, k, None)
                    if v is not None:
                        msg = str(getattr(st, "msg", "") or getattr(st, "message", "") or "")
                        return str(v), msg
                except Exception:
                    pass
        return "00", ""  # assume ok if unknown

    def _extract_status_data(self, trade: Any) -> Dict[str, Any]:
        # Try to get a dict that includes deal_quantity + status
        # 1) if trade is dict-like
        if isinstance(trade, dict):
            return trade
        # 2) if trade.status exists
        st = getattr(trade, "status", None)
        if isinstance(st, dict):
            return st
        if st is not None:
            d = self._safe_to_dict(st)
            # sometimes deal qty lives in "deal_quantity" or "deal_qty" etc.
            return d
        # 3) fallback: trade itself
        return self._safe_to_dict(trade)

    def _order_cb(self, trade: Any):
        """
        Shioaji order callback.
        We normalize and forward to TxManager callback signature:
           on_execution_event(oid, seqno, op_code, op_msg, status_data)
        """
        try:
            seqno = self._extract_seqno(trade)
            op_code, op_msg = self._extract_op(trade)
            status_data = self._extract_status_data(trade)

            # normalize fields
            if "seqno" not in status_data and seqno:
                status_data["seqno"] = seqno

            # normalize status string
            if "status" not in status_data:
                # try common fields
                for k in ("order_status", "state", "status_code"):
                    if k in status_data:
                        status_data["status"] = status_data.get(k)
                        break

            # cache
            if seqno:
                with self._lock:
                    self._trade_cache[seqno] = trade
                    self._order_cache[seqno] = status_data

            if self.on_order_callback:
                oid = ""
                try:
                    oid = str(getattr(getattr(trade, "order", None), "id", "") or getattr(getattr(trade, "order", None), "order_id", "") or "")
                except Exception:
                    oid = ""
                self.on_order_callback(oid, seqno, str(op_code), str(op_msg), status_data)

        except Exception as e:
            print(f"[Execution] order_cb error: {e}")
            traceback.print_exc()

    # ------------------ order helpers ------------------

    def _build_stock_order(self, action: str, price: float, qty: int, price_type: str, order_type: str):
        # price_type: "MKT"/"LMT"
        # order_type: "ROD"/"IOC"/...
        act = sj_constant.Action.Buy if action == "Buy" else sj_constant.Action.Sell
        pt = sj_constant.StockPriceType.MKT if price_type == "MKT" else sj_constant.StockPriceType.LMT
        ot = sj_constant.OrderType.ROD if order_type == "ROD" else sj_constant.OrderType.IOC

        o = sj.Order(
            price=price,
            quantity=int(qty),
            action=act,
            price_type=pt,
            order_type=ot,
            account=self.stock_account,
        )
        return o

    def _build_future_order(self, action: str, price: float, qty: int, price_type: str, order_type: str):
        act = sj_constant.Action.Buy if action == "Buy" else sj_constant.Action.Sell

        # Futures price type / order type enums can differ by versions; do best-effort.
        # Prefer FuturesPriceType / FuturesOrderType if exist.
        try:
            FPT = sj_constant.FuturesPriceType
            FOT = sj_constant.FuturesOrderType
            pt = FPT.MKT if price_type == "MKT" else FPT.LMT
            ot = FOT.IOC if order_type == "IOC" else FOT.ROD
        except Exception:
            # fallback to generic
            pt = sj_constant.StockPriceType.MKT if price_type == "MKT" else sj_constant.StockPriceType.LMT
            ot = sj_constant.OrderType.IOC if order_type == "IOC" else sj_constant.OrderType.ROD

        o = sj.Order(
            price=price,
            quantity=int(qty),
            action=act,
            price_type=pt,
            order_type=ot,
            account=self.futopt_account,
        )
        return o

    def place_stock_order(self, code: str, action: str, price: float, qty: int, price_type: str, order_type: str) -> str:
        if not self.api or not self.stock_account:
            return ""
        
        c = self._contract_cache.get(code)
        if not c:
            c = resolve_stock_contract(self.api, code)
            if c:
                self._contract_cache[code] = c

        if not c:
            print(f"[Execution][ERR] Stock contract not found: {code}")
            return ""
        try:
            o = self._build_stock_order(action, price, qty, price_type, order_type)
            trade = self.api.place_order(c, o)
            seqno = self._extract_seqno(trade)
            if not seqno:
                # last resort: try trade.seqno
                seqno = str(getattr(trade, "seqno", "") or "")
            return seqno
        except Exception as e:
            print(f"[Execution][ERR] place_stock_order failed: {e}")
            traceback.print_exc()
            return ""

    def check_connection(self) -> bool:
        if not self.api: return False
        try:
            # heartbeat check
            self.api.list_accounts()
            return True
        except Exception:
            return False

    def relogin(self):
        if not self.api: return
        key, secret = self._creds
        if not key or not secret:
            print("[Execution] No credentials for re-login.")
            return
        
        print(f"[Execution] Attempting Re-Login at {time.strftime('%H:%M:%S')}...")
        try:
            self.api.login(api_key=key, secret_key=secret, subscribe_trade=True)
            print("[Execution] Re-Login Success.")
        except Exception as e:
            print(f"[Execution] Re-Login Failed: {e}")

    def get_future_multiplier(self, code: str) -> float:
        # Check cache
        c = self._contract_cache.get(code)
        if not c:
            c = resolve_future_contract(self.api, code)
            if c: self._contract_cache[code] = c
        
        if c:
            # try different attributes
            return float(getattr(c, "multiplier", 0) or SystemConfig.DEFAULT_FUTURE_MULTIPLIER)
        return float(SystemConfig.DEFAULT_FUTURE_MULTIPLIER)

    def place_future_order(self, code: str, action: str, price: float, qty: int, price_type: str, order_type: str) -> str:
        if not self.api or not self.futopt_account:
            return ""

        c = self._contract_cache.get(code)
        if not c:
            c = resolve_future_contract(self.api, code)
            if c:
                self._contract_cache[code] = c
        
        if not c:
            print(f"[Execution][ERR] Future contract not found: {code}")
            return ""
        try:
            o = self._build_future_order(action, price, qty, price_type, order_type)
            trade = self.api.place_order(c, o)
            seqno = self._extract_seqno(trade)
            if not seqno:
                seqno = str(getattr(trade, "seqno", "") or "")
            return seqno
        except Exception as e:
            print(f"[Execution][ERR] place_future_order failed: {e}")
            traceback.print_exc()
            return ""

    def place_pair_orders_simultaneous(self,
                                       stock_code: str, stock_action: str, stock_qty: int,
                                       fut_code: str, fut_action: str, fut_qty: int,
                                       stock_price_type: str = "MKT", stock_order_type: str = "ROD",
                                       fut_price_type: str = "MKT", fut_order_type: str = "IOC") -> Tuple[str, str]:
        """
        Best-effort "simultaneous": we submit as close as possible.
        Return (stock_seqno, fut_seqno)
        """
        s_seq = ""
        f_seq = ""
        # Risk Mitigation: Submit Future (IOC) first to ensure immediate result, then Stock.
        # This reduces naked stock risk if future cannot be filled.
        f_seq = self.place_future_order(fut_code, fut_action, 0, fut_qty, fut_price_type, fut_order_type)
        s_seq = self.place_stock_order(stock_code, stock_action, 0, stock_qty, stock_price_type, stock_order_type)
        return s_seq, f_seq

    # ------------------ cancel / status ------------------

    def cancel_order_by_seqno(self, seqno: str):
        if not self.api or not seqno:
            return
        try:
            # Shioaji cancel can take trade or order; try find in list_trades
            t = None
            try:
                trades = self.api.list_trades()
                for tr in trades:
                    s = self._extract_seqno(tr)
                    if s == seqno:
                        t = tr
                        break
            except Exception:
                t = None

            if t is not None:
                try:
                    self.api.cancel_order(t)
                    return
                except Exception:
                    pass

            # fallback: cancel by seqno if version supports
            if hasattr(self.api, "cancel_order"):
                # some versions accept seqno directly (rare)
                try:
                    self.api.cancel_order(seqno)
                except Exception:
                    pass

        except Exception as e:
            print(f"[Execution][WARN] cancel_order_by_seqno failed: {e}")

    def cancel_all_orders(self):
        if not self.api:
            return
        try:
            trades = self.api.list_trades()
        except Exception:
            trades = []

        for tr in trades:
            try:
                # cancel if still active
                st = getattr(tr, "status", None)
                s_str = ""
                if isinstance(st, dict):
                    s_str = str(st.get("status", "") or st.get("order_status", "") or "")
                else:
                    s_str = str(getattr(st, "status", "") or getattr(st, "order_status", "") or "")
                if "Filled" in s_str:
                    continue
                self.api.cancel_order(tr)
            except Exception:
                pass

    def get_order_status(self, seqno: str) -> Optional[Dict]:
        """
        Best-effort:
          1) use callback cache
          2) scan api.list_trades()
        """
        if not seqno or not self.api:
            return None

        with self._lock:
            if seqno in self._order_cache:
                return dict(self._order_cache[seqno])

        try:
            trades = self.api.list_trades()
            for tr in trades:
                s = self._extract_seqno(tr)
                if s == seqno:
                    d = self._extract_status_data(tr)
                    # normalize
                    if "seqno" not in d:
                        d["seqno"] = seqno
                    # map filled qty
                    # sometimes in tr.status.deal_quantity
                    return d
        except Exception:
            pass

        return None

    # ------------------ positions ------------------

    def get_all_positions(self) -> Dict[str, int]:
        """
        Return net positions map:
          - stock: code -> +qty
          - future: code -> signed qty (Buy positive / Sell negative)
        """
        pos: Dict[str, int] = {}
        if not self.api:
            return pos

        # stock
        if self.stock_account:
            try:
                lst = self.api.list_positions(self.stock_account, unit=Unit.Share)
                for p in lst:
                    code = getattr(p, "code", "")
                    if not code:
                        continue
                    qty = int(float(getattr(p, "quantity", 0) or 0))
                    pos[code] = qty
            except Exception:
                pass

        # future
        if self.futopt_account:
            try:
                lst = self.api.list_positions(self.futopt_account)
                for p in lst:
                    code = getattr(p, "code", "")
                    if not code:
                        continue
                    qty = int(float(getattr(p, "quantity", 0) or 0))
                    direction = getattr(p, "direction", None)
                    net = qty
                    try:
                        if direction == sj_constant.Action.Sell:
                            net = -qty
                    except Exception:
                        # direction might be string
                        if str(direction).lower().find("sell") >= 0:
                            net = -qty
                    pos[code] = net
            except Exception:
                pass

        return pos

    def update_ledger_positions(self, ledger: PortfolioLedger):
        """
        Update ledger.positions using list_positions and market snapshot if possible.
        """
        if not self.api:
            return

        # stock
        if self.stock_account:
            try:
                lst = self.api.list_positions(self.stock_account)
                for p in lst:
                    code = getattr(p, "code", "")
                    if not code:
                        continue
                    qty = int(float(getattr(p, "quantity", 0) or 0))
                    avgp = float(getattr(p, "price", 0.0) or getattr(p, "avg_price", 0.0) or getattr(p, "cost_price", 0.0) or 0.0)
                    ledger.upsert(PositionLine(code=code, product="Stock", qty=qty, avg_price=avgp, last_price=0.0, direction="Buy" if qty > 0 else ""))
            except Exception:
                pass

        # future
        if self.futopt_account:
            try:
                lst = self.api.list_positions(self.futopt_account)
                for p in lst:
                    code = getattr(p, "code", "")
                    if not code:
                        continue
                    qty = int(float(getattr(p, "quantity", 0) or 0))
                    avgp = float(getattr(p, "price", 0.0) or getattr(p, "avg_price", 0.0) or getattr(p, "cost_price", 0.0) or 0.0)
                    direction = getattr(p, "direction", None)
                    net = qty
                    dstr = ""
                    try:
                        if direction == sj_constant.Action.Sell:
                            net = -qty
                            dstr = "Sell"
                        else:
                            dstr = "Buy"
                    except Exception:
                        dstr = str(direction)
                        if str(direction).lower().find("sell") >= 0:
                            net = -qty
                    ledger.upsert(PositionLine(code=code, product="Future", qty=net, avg_price=avgp, last_price=0.0, direction=dstr))
            except Exception:
                pass

        ledger.last_sync_ts = time.time()

    def dump_orders(self) -> str:
        lines = ["\n=== ORDERS (best effort) ==="]
        with self._lock:
            for seq, st in list(self._order_cache.items())[-50:]:
                lines.append(f"  seq={seq} status={st.get('status','')} deal={st.get('deal_quantity', st.get('deal_qty',''))}")
        return "\n".join(lines)

    def dump_positions(self) -> str:
        pos = self.get_all_positions()
        lines = ["\n=== POSITIONS (net) ==="]
        if not pos:
            lines.append("  (none)")
        else:
            for k, v in sorted(pos.items()):
                lines.append(f"  {k}: {v}")
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

        self.stock_order = OrderStub(product="Stock", code=intent.stock_code, target_qty=intent.qty)
        self.future_order = OrderStub(product="Future", code=intent.future_code, target_qty=intent.qty)
        self.extra_seqnos: set[str] = set()

        self._lock = threading.RLock()  # Thread-safety for API callbacks vs Logic Loop

        self.last_poll_ts = 0.0

        self.log(f"Transaction Created: {intent}")

    def log(self, msg: str):
        ts = time.strftime("%H:%M:%S")
        print(f"[{ts}] [Tx-{self.tx_id}] {msg}")

    def set_state(self, new_state: TransactionState, msg: str = ""):
        with self._lock:
            if self.state in [TransactionState.COMPLETED, TransactionState.FAILED, TransactionState.CANCELLED]:
                return
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
        with self._lock:
            # Leg 1: Stock
            if seqno == self.stock_order.seqno:
                self.log(f"Leg 1 Update: Code={op_code} Msg={msg}")
                if op_code == "00":
                    delta = _apply_fill(self.stock_order, status_data)
                    if delta > 0:
                        self.log(f"Leg 1 Partial Fill: +{delta} (Total: {self.stock_order.filled_qty})")
                        self.mgr.print_portfolio("FILL(stock)")
                        if self.stock_order.filled_qty >= self.intent.qty:
                            self.set_state(TransactionState.LEG1_FILLED, "Leg 1 Fully Filled.")
                            self.stock_order.status = "Filled"
                else:
                    self.stock_order.status = "Failed"
                    self.stock_order.error_msg = msg
                    self.set_state(TransactionState.FAILED, f"Leg 1 FAILED: {msg}")
                    self.mgr.print_portfolio("FAILED(stock)")

            # Leg 2: Future
            elif seqno == self.future_order.seqno:
                self.log(f"Leg 2 Update: Code={op_code} Msg={msg}")
                if op_code == "00":
                    delta = _apply_fill(self.future_order, status_data)
                    if delta > 0:
                        self.log(f"Leg 2 Partial Fill: +{delta} (Total: {self.future_order.filled_qty})")
                        self.mgr.print_portfolio("FILL(future)")
                        if self.future_order.filled_qty >= self.intent.qty:
                            self.set_state(TransactionState.COMPLETED, "Transaction COMPLETED.")
                            self.future_order.status = "Filled"
                            self.mgr.print_portfolio("COMPLETED")
                else:
                    self.future_order.status = "Failed"
                    self.future_order.error_msg = msg
                    self.set_state(TransactionState.FAILED, f"Leg 2 FAILED: {msg}")
                    self.mgr.print_portfolio("FAILED(future)")

    # --- Step Implementations ---

    def _step_init(self):
        snapshot = self.mgr.market_data.get_stock(self.intent.stock_code)
        if not snapshot:
            self.log("No market data for Leg 1. Waiting...")
            return

        if snapshot.price <= 0 and snapshot.bid <= 0 and snapshot.ask <= 0:
            return

        self.log(f"Placing Leg 1 (Stock {self.intent.stock_code}) Market Order...")

        action = "Buy" if self.intent.type == SignalType.OPEN else "Sell"

        # Gate by book size (stock)
        if not self._book_ok_for_market(snapshot, action, self.mgr.MIN_STK_BOOK_QTY, "Stock"):
            # 不送單 -> 取消交易（避免進場在流動性不足時亂打）
            self.set_state(TransactionState.FAILED, "Leg 1 BLOCKED by Stock book size")
            self.mgr.print_portfolio("BLOCKED(stock)")
            return

        seqno = self.mgr.execution.place_stock_order(
            self.intent.stock_code, action, 0, self.intent.qty, "MKT", "ROD"
        )

        if seqno:
            self.stock_order.seqno = seqno
            self.stock_order.status = "Submitted"
            self.stock_order.action = action
            self.stock_order.submit_ts = time.time()
            self.state = TransactionState.LEG1_SUBMITTED
            self.state_enter_ts = time.time()
            self.log(f"Leg 1 Submitted SeqNo: {seqno}")
            self.mgr.print_portfolio("SUBMIT(stock)")
        else:
            self.set_state(TransactionState.FAILED, "Leg 1 Submission Failed")
            self.mgr.print_portfolio("FAILED(stock_submit)")

    def _check_leg1_timeout(self):
        if time.time() - self.state_enter_ts > SystemConfig.LEG1_TIMEOUT:
            self.log("Leg 1 Timeout. Cancelling...")
            self.mgr.execution.cancel_order_by_seqno(self.stock_order.seqno)
            self.set_state(TransactionState.FAILED, "Leg 1 Timeout & Cancelled")
            self.mgr.print_portfolio("CANCEL(stock_timeout)")

    def _step_place_leg2(self):
        filled_qty = self.stock_order.filled_qty
        if filled_qty <= 0:
            self.log("Leg 1 Filled Qty is 0? Strange.")
            self.set_state(TransactionState.FAILED, "Leg 1 filled qty 0")
            self.mgr.print_portfolio("FAILED(leg1_qty0)")
            return

        hedge_qty = filled_qty

        self.log(f"Placing Leg 2 (Future {self.intent.future_code}) Qty={hedge_qty}...")
        action = "Sell" if self.intent.type == SignalType.OPEN else "Buy"

        self.log(f"Placing Leg 2 (Future {self.intent.future_code}) Market Order...")

        # Gate by book size (future) - BUT leg2 is risk-critical.
        fut_snap = self.mgr.market_data.get_future(self.intent.future_code)
        if not fut_snap:
            self.log("No market data for Future leg2. Auto-Repair to avoid naked.")
            self.mgr.trigger_repair = True
            self.mgr.print_portfolio("REPAIR(no_fut_md)")
            return

        if not self._book_ok_for_market(fut_snap, action, self.mgr.MIN_FUT_BOOK_QTY, "Future"):
            # 量不足：不送單 -> 立刻 repair（避免 stock 已成交但 future 沒對沖）
            self.log("Leg 2 BLOCKED by Future book size. Triggering Auto-Repair...")
            self.mgr.trigger_repair = True
            self.set_state(TransactionState.FAILED, "Leg 2 BLOCKED by Future book size -> repaired")
            self.mgr.print_portfolio("REPAIR(fut_blocked)")
            return

        seqno = self.mgr.execution.place_future_order(
            self.intent.future_code, action, 0, hedge_qty, "MKT", "IOC"
        )

        if seqno:
            self.future_order.seqno = seqno
            self.future_order.status = "Submitted"
            self.future_order.action = action
            self.future_order.submit_ts = time.time()
            self.set_state(TransactionState.LEG2_SUBMITTED, f"Leg 2 Submitted SeqNo: {seqno}")
            self.mgr.print_portfolio("SUBMIT(future)")
        else:
            self.log("Leg 2 Submission Failed -> Auto-Repair")
            self.mgr.trigger_repair = True
            self.set_state(TransactionState.FAILED, "Leg 2 Submission Failed -> repaired")
            self.mgr.print_portfolio("REPAIR(fut_submit_fail)")

    def _check_leg2_timeout(self):
        if time.time() - self.state_enter_ts > SystemConfig.LEG2_TIMEOUT:
            self.log("Leg 2 Timeout. Cancelling...")
            self.mgr.execution.cancel_order_by_seqno(self.future_order.seqno)
            self.log("Leg 2 Timeout -> Auto-Repair")
            self.mgr.trigger_repair = True
            self.set_state(TransactionState.FAILED, "Leg 2 Timeout & Cancelled -> repaired")
            self.mgr.print_portfolio("REPAIR(fut_timeout)")

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
                self.mgr.print_portfolio("CANCEL(stock)")
            # Partial fill detection fallback
            elif _get_deal_qty(status) > self.stock_order.last_deal_qty_reported:
                 self.log(f"[Polling] Detected Leg 1 Partial! Qty={_get_deal_qty(status)}")
                 self.on_order_update(self.stock_order.seqno, "00", "Polled Partial", status)

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
                self.log("Leg 2 Cancelled -> Auto-Repair")
                self.mgr.trigger_repair = True
                self.set_state(TransactionState.FAILED, "Leg 2 Cancelled -> repaired")
                self.mgr.print_portfolio("REPAIR(fut_cancelled)")


class SimultaneousOpenTransaction(BaseTransaction):
    """
    OPEN: 同時送兩腳
      - Buy Stock @MKT ROD
      - Sell Future @MKT IOC
    目標：降低缺腳時間窗
    """
    def update(self):
        self.updated_at = time.time()
        self.last_hedge_ts = getattr(self, "last_hedge_ts", 0.0)

        if self.state == TransactionState.INIT:
            self._step_open_simultaneous_submit()
        elif self.state == TransactionState.LEG1_SUBMITTED:
            self._poll_both()
            self._timeout_and_hedge()
        elif self.state == TransactionState.COMPLETED:
            return
        elif self.state == TransactionState.FAILED:
            return

    def _step_open_simultaneous_submit(self):
        # 1. PREPARE (Locked)
        with self._lock:
            if self.state != TransactionState.INIT:
                return

            stk = self.mgr.market_data.get_stock(self.intent.stock_code)
            fut = self.mgr.market_data.get_future(self.intent.future_code)

            if not stk or not fut:
                self.log("Missing market data (stk/fut). Waiting...")
                return

            if stk.ask <= 0 or fut.bid <= 0:
                return

            # Gate: BOTH legs liquidity
            if not self._book_ok_for_market(stk, "Buy", self.mgr.MIN_STK_BOOK_QTY * self.intent.qty, "Stock"):
                self.set_state(TransactionState.FAILED, "OPEN blocked by Stock book size")
                self.mgr.print_portfolio("BLOCKED(open_stock)")
                return
            if not self._book_ok_for_market(fut, "Sell", self.mgr.MIN_FUT_BOOK_QTY * self.intent.qty, "Future"):
                self.set_state(TransactionState.FAILED, "OPEN blocked by Future book size")
                self.mgr.print_portfolio("BLOCKED(open_future)")
                return

            # Unit Conversion (Auto-Detect)
            fut_multiplier = self.mgr.execution.get_future_multiplier(self.intent.future_code)
            total_shares = self.intent.qty * fut_multiplier
            stock_qty_lots = int(total_shares / SystemConfig.STOCK_SHARES_PER_LOT)
            
            # Safety check for odd lots
            if total_shares % SystemConfig.STOCK_SHARES_PER_LOT != 0:
                 self.log(f"[Units][WARN] Non-standard multiplier {fut_multiplier}! Shares={total_shares}. Using {stock_qty_lots} Lots (Under-hedge).")
            
            if stock_qty_lots == 0:
                 self.log(f"[Units][ERR] Calculated Stock Lots = 0 (Total Shares={total_shares} < {SystemConfig.STOCK_SHARES_PER_LOT}). Mini Contracts not supported yet.")
                 self.set_state(TransactionState.FAILED, "Zero Stock Qty (Mini Contract?)")
                 return
                 
            # Extract params for IO
            s_code = self.intent.stock_code
            f_code = self.intent.future_code
            f_qty = self.intent.qty

        # 2. IO (Unlocked) - PREVENT BLOCKING API THREAD
        self.log(f"[OPEN][SIM] Submitting BOTH legs: Buy Stock {s_code} & Sell Fut {f_code} qty={f_qty}")
        
        s_seq, f_seq = self.mgr.execution.place_pair_orders_simultaneous(
            stock_code=s_code, stock_action="Buy", stock_qty=stock_qty_lots,
            fut_code=f_code, fut_action="Sell", fut_qty=f_qty,
            stock_price_type="MKT", stock_order_type="ROD",
            fut_price_type="MKT", fut_order_type="IOC"
        )

        # 3. UPDATE (Locked)
        with self._lock:
            # Re-check state in case we were cancelled/failed during IO
            if self.state != TransactionState.INIT:
                self.log(f"[OPEN][SIM] State changed during submission IO! (Now: {self.state}). Ignoring new seqnos.")
                # Possible enhancement: Cancel these new orders immediately? 
                # For now, we just don't attach them to the transaction to avoid confusing state.
                return

            if not s_seq and not f_seq:
                self.set_state(TransactionState.FAILED, "OPEN submit failed both legs")
                self.mgr.print_portfolio("FAILED(open_submit_both)")
                return

            # even if one leg failed, we continue and hedge/repair
            if s_seq:
                self.stock_order.seqno = s_seq
                self.stock_order.status = "Submitted"
                self.stock_order.action = "Buy"
                self.stock_order.submit_ts = time.time()

            if f_seq:
                self.future_order.seqno = f_seq
                self.future_order.status = "Submitted"
                self.future_order.action = "Sell"
                self.future_order.submit_ts = time.time()

            self.set_state(TransactionState.LEG1_SUBMITTED, f"OPEN both legs submitted (s={s_seq}, f={f_seq})")
            self.mgr.print_portfolio("SUBMIT(open_both)")

    def on_order_update(self, seqno: str, op_code: str, msg: str, status_data: Dict):
        with self._lock:
            # Stock leg
            if seqno == self.stock_order.seqno:
                self.log(f"[OPEN][SIM] Stock Update: {op_code} {msg}")
                if op_code != "00":
                    self.stock_order.status = "Failed"
                    self.set_state(TransactionState.FAILED, f"OPEN stock leg failed: {msg}")
                    self.mgr.print_portfolio("FAILED(open_stock)")
                    # if future already filled -> repair
                    if self.future_order.filled_qty > 0:
                        self.log("[OPEN][SIM] Stock leg failed but future filled => repair")
                        self.mgr.trigger_repair = True
                        self.mgr.print_portfolio("REPAIR(open_stock_fail)")
                    return

                delta = _apply_fill(self.stock_order, status_data)
                if delta > 0:
                    self.log(f"[OPEN][SIM] Stock fill +{delta} (total {self.stock_order.filled_qty})")
                    # self.mgr.held_positions.add(self.intent.stock_code) # Too early! Moved to COMPLETED.
                    self.mgr.print_portfolio("FILL(open_stock)")

            # Future leg
            elif seqno == self.future_order.seqno:
                self.log(f"[OPEN][SIM] Future Update: {op_code} {msg}")
                if op_code != "00":
                    self.future_order.status = "Failed"
                    self.set_state(TransactionState.FAILED, f"OPEN future leg failed: {msg}")
                    self.mgr.print_portfolio("FAILED(open_future)")
                    # if stock already filled -> repair
                    if self.stock_order.filled_qty > 0:
                        self.log("[OPEN][SIM] Future leg failed but stock filled => repair")
                        self.mgr.trigger_repair = True
                        self.mgr.print_portfolio("REPAIR(open_future_fail)")
                    return

                delta = _apply_fill(self.future_order, status_data)
                if delta > 0:
                    self.log(f"[OPEN][SIM] Future fill +{delta} (total {self.future_order.filled_qty})")
                    self.mgr.print_portfolio("FILL(open_future)")

            elif seqno in self.extra_seqnos:
                 self.log(f"[OPEN][SIM][Hedge] Update: seq={seqno} Code={op_code} Msg={msg}")
                 if op_code == "00":
                     q = _get_deal_qty(status_data)
                     self.log(f"[OPEN][SIM][Hedge] Fill Qty: {q}")

            # completion check
            if self.stock_order.filled_qty >= self.intent.qty and self.future_order.filled_qty >= self.intent.qty:
                self.stock_order.status = "Filled"
                self.future_order.status = "Filled"
                self.mgr.held_positions.add(self.intent.stock_code)  # Correctly mark as held only when completed
                self.set_state(TransactionState.COMPLETED, "OPEN SIM completed (both legs filled)")
                self.mgr.print_portfolio("COMPLETED(open)")
                return

    def _poll_both(self):
        if time.time() - self.last_poll_ts < 1.0:
            return
        self.last_poll_ts = time.time()

        if self.stock_order.seqno:
            st = self.mgr.execution.get_order_status(self.stock_order.seqno)
            if st:
                s_str = st.get("status")
                qty = int(st.get("deal_quantity", 0) or 0)
                if s_str == "Filled" or qty >= self.intent.qty:
                    self.on_order_update(self.stock_order.seqno, "00", "Polled Fill", st)
                elif s_str == "Cancelled":
                    self.log("[OPEN][SIM] Stock cancelled (polled)")
                    self.stock_order.status = "Cancelled"
                    self.mgr.print_portfolio("CANCEL(open_stock)")

        if self.future_order.seqno:
            st = self.mgr.execution.get_order_status(self.future_order.seqno)
            if st:
                s_str = st.get("status")
                qty = int(st.get("deal_quantity", 0) or 0)
                if s_str == "Filled" or qty >= self.intent.qty:
                    self.on_order_update(self.future_order.seqno, "00", "Polled Fill", st)
                elif s_str == "Cancelled":
                    self.log("[OPEN][SIM] Future cancelled (polled)")
                    self.future_order.status = "Cancelled"
                    self.mgr.print_portfolio("CANCEL(open_future)")

    def _timeout_and_hedge(self):
        """
        If one leg fills but the other doesn't, repair to maintain delta neutral.
        Uses InstrumentRegistry for dynamic ratio.
        """
        HEDGE_WINDOW_SEC = 3.0
        HARD_TIMEOUT_SEC = 15.0
        
        # Dynamic Ratio
        RATIO = InstrumentRegistry.get_ratio(self.intent.stock_code, self.intent.future_code)
        # Fallback safety (should not happen if filtered correctly)
        if RATIO < 1: RATIO = 1

        with self._lock:
            if self.state in [TransactionState.COMPLETED, TransactionState.FAILED]:
                return

            age = time.time() - self.state_enter_ts

            # Hard timeout -> Cancel & Mark Failed
            if age > HARD_TIMEOUT_SEC:
                self.log("[OPEN][SIM] Hard timeout -> cancel & repair")
                if self.stock_order.seqno:
                    self.mgr.execution.cancel_order_by_seqno(self.stock_order.seqno)
                if self.future_order.seqno:
                    self.mgr.execution.cancel_order_by_seqno(self.future_order.seqno)
                
                self.mgr.trigger_repair = True
                self.set_state(TransactionState.FAILED, "OPEN SIM hard timeout -> repaired")
                self.mgr.print_portfolio("REPAIR(open_timeout)")
                return

            # Soft Hedge Window
            if age > HEDGE_WINDOW_SEC:
                if time.time() - getattr(self, "last_hedge_ts", 0) < 2.0:
                    return

                s_filled = self.stock_order.filled_qty
                f_filled = self.future_order.filled_qty
                
                # Case 1: Stock filled MORE than Futures (Excess Long)
                # We need to Short Futures or Sell Back Stock.
                # Matched = f * 2. Excess = s - Matched.
                matched_s = f_filled * RATIO
                excess_s = s_filled - matched_s
                
                if excess_s > 0:
                    # Can we hedge with whole futures?
                    hedge_f_qty = excess_s // RATIO
                    retreat_s_qty = excess_s % RATIO
                    
                    if hedge_f_qty > 0:
                        self.log(f"[OPEN][SIM] Hedge: Excess Stock {excess_s}. Selling {hedge_f_qty} Future(s).")
                        h_seq = self.mgr.execution.place_future_order(
                            self.intent.future_code, "Sell", 0, hedge_f_qty, "MKT", "IOC"
                        )
                        if h_seq: self.extra_seqnos.add(h_seq)
                        
                    if retreat_s_qty > 0:
                        # Cannot match with a future. Must close this partial stock position.
                        self.log(f"[OPEN][SIM] Hedge: Odd Stock {retreat_s_qty}. Cannot hedge. Selling back (Retreat).")
                        h_seq = self.mgr.execution.place_stock_order(
                            self.intent.stock_code, "Sell", 0, retreat_s_qty, "MKT", "ROD"
                        )
                        if h_seq: self.extra_seqnos.add(h_seq)

                    self.mgr.print_portfolio("HEDGE(open_stock_imbalance)")
                    self.last_hedge_ts = time.time()

                # Case 2: Future filled MORE than Stock (Excess Short)
                # We need to Buy Stock.
                # Target Stock = f * 2.
                target_s = f_filled * RATIO
                missing_s = target_s - s_filled
                
                if missing_s > 0:
                    self.log(f"[OPEN][SIM] Hedge: Future filled {f_filled}, Stock {s_filled}. Need Buy {missing_s} Stocks.")
                    h_seq = self.mgr.execution.place_stock_order(
                        self.intent.stock_code, "Buy", 0, missing_s, "MKT", "ROD"
                    )
                    if h_seq: self.extra_seqnos.add(h_seq)
                    
                    self.mgr.print_portfolio("HEDGE(open_fut_imbalance)")
                    self.last_hedge_ts = time.time()

    def _timeout_and_hedge_close(self):
        # Placeholder for SimultaneousCloseTransaction
        pass


class SimultaneousCloseTransaction(BaseTransaction):
    """
    CLOSE: 同時送兩腳 (反向)
      - Buy Future @MKT IOC   (close short)
      - Sell Stock @MKT ROD   (close long)
    """
    def update(self):
        self.updated_at = time.time()

        if self.state == TransactionState.INIT:
            self._step_close_simultaneous_submit()
        elif self.state == TransactionState.LEG1_SUBMITTED:
            self._poll_both()
            self._timeout_and_hedge()
        elif self.state == TransactionState.COMPLETED:
            return
        elif self.state == TransactionState.FAILED:
            return

    def _step_close_simultaneous_submit(self):
        # 1. PREPARE (Locked)
        with self._lock:
            if self.state != TransactionState.INIT:
                return

            stk = self.mgr.market_data.get_stock(self.intent.stock_code)
            fut = self.mgr.market_data.get_future(self.intent.future_code)

            if not stk or not fut:
                self.log("Missing market data (stk/fut). Waiting...")
                return

            if stk.bid <= 0 or fut.ask <= 0:
                return

            # Gate: BOTH legs liquidity for close directions
            if not self._book_ok_for_market(fut, "Buy", self.mgr.MIN_FUT_BOOK_QTY * self.intent.qty, "Future"):
                self.set_state(TransactionState.FAILED, "CLOSE blocked by Future book size")
                self.mgr.print_portfolio("BLOCKED(close_future)")
                return
            if not self._book_ok_for_market(stk, "Sell", self.mgr.MIN_STK_BOOK_QTY * self.intent.qty, "Stock"):
                self.set_state(TransactionState.FAILED, "CLOSE blocked by Stock book size")
                self.mgr.print_portfolio("BLOCKED(close_stock)")
                return

            # Unit Conversion
            fut_multiplier = self.mgr.execution.get_future_multiplier(self.intent.future_code)
            total_shares = self.intent.qty * fut_multiplier
            stock_qty_lots = int(total_shares / SystemConfig.STOCK_SHARES_PER_LOT)

            if stock_qty_lots == 0:
                 self.log(f"[Units][ERR] Calculated Stock Lots = 0 (Total Shares={total_shares} < {SystemConfig.STOCK_SHARES_PER_LOT}). Mini Contracts not supported yet.")
                 self.set_state(TransactionState.FAILED, "Zero Stock Qty (Mini Contract?)")
                 return
                 
            # Extract params for IO
            s_code = self.intent.stock_code
            f_code = self.intent.future_code
            f_qty = self.intent.qty

        # 2. IO (Unlocked)
        self.log(f"[CLOSE][SIM] Submitting BOTH legs: Buy Fut {f_code} & Sell Stock {s_code} qty={f_qty}")

        # Note: stock is ROD, future is IOC
        s_seq, f_seq = self.mgr.execution.place_pair_orders_simultaneous(
            stock_code=s_code, stock_action="Sell", stock_qty=stock_qty_lots,
            fut_code=f_code, fut_action="Buy", fut_qty=f_qty,
            stock_price_type="MKT", stock_order_type="ROD",
            fut_price_type="MKT", fut_order_type="IOC"
        )

        # 3. UPDATE (Locked)
        with self._lock:
            # Re-check state 
            if self.state != TransactionState.INIT:
                return

            if not s_seq and not f_seq:
                self.set_state(TransactionState.FAILED, "CLOSE submit failed both legs")
                self.mgr.print_portfolio("FAILED(close_submit_both)")
                return

            if s_seq:
                self.stock_order.seqno = s_seq
                self.stock_order.status = "Submitted"
                self.stock_order.action = "Sell"
                self.stock_order.submit_ts = time.time()

            if f_seq:
                self.future_order.seqno = f_seq
                self.future_order.status = "Submitted"
                self.future_order.action = "Buy"
                self.future_order.submit_ts = time.time()

            self.set_state(TransactionState.LEG1_SUBMITTED, f"CLOSE both legs submitted (s={s_seq}, f={f_seq})")
            self.mgr.print_portfolio("SUBMIT(close_both)")

    def on_order_update(self, seqno: str, op_code: str, msg: str, status_data: Dict):
        with self._lock:
            # Stock leg
            if seqno == self.stock_order.seqno:
                self.log(f"[CLOSE][SIM] Stock Update: {op_code} {msg}")
                if op_code != "00":
                    self.stock_order.status = "Failed"
                    self.set_state(TransactionState.FAILED, f"CLOSE stock leg failed: {msg}")
                    self.mgr.print_portfolio("FAILED(close_stock)")
                    if self.future_order.filled_qty > 0:
                        self.log("[CLOSE][SIM] Stock leg failed but future filled => repair")
                        self.mgr.trigger_repair = True
                        self.mgr.print_portfolio("REPAIR(close_stock_fail)")
                    return

                delta = _apply_fill(self.stock_order, status_data)
                if delta > 0:
                    self.log(f"[CLOSE][SIM] Stock fill +{delta} (total {self.stock_order.filled_qty})")
                    self.mgr.print_portfolio("FILL(close_stock)")

            # Future leg
            elif seqno == self.future_order.seqno:
                self.log(f"[CLOSE][SIM] Future Update: {op_code} {msg}")
                if op_code != "00":
                    self.future_order.status = "Failed"
                    self.set_state(TransactionState.FAILED, f"CLOSE future leg failed: {msg}")
                    self.mgr.print_portfolio("FAILED(close_future)")
                    if self.stock_order.filled_qty > 0:
                        self.log("[CLOSE][SIM] Future leg failed but stock filled => repair")
                        self.mgr.trigger_repair = True
                        self.mgr.print_portfolio("REPAIR(close_future_fail)")
                    return

                delta = _apply_fill(self.future_order, status_data)
                if delta > 0:
                    self.log(f"[CLOSE][SIM] Future fill +{delta} (total {self.future_order.filled_qty})")
                    self.mgr.print_portfolio("FILL(close_future)")

            elif seqno in self.extra_seqnos:
                 self.log(f"[CLOSE][SIM][Hedge] Update: seq={seqno} Code={op_code} Msg={msg}")
                 if op_code == "00":
                     q = _get_deal_qty(status_data)
                     self.log(f"[CLOSE][SIM][Hedge] Fill Qty: {q}")

            if self.stock_order.filled_qty >= self.intent.qty and self.future_order.filled_qty >= self.intent.qty:
                self.stock_order.status = "Filled"
                self.future_order.status = "Filled"
                self.set_state(TransactionState.COMPLETED, "CLOSE SIM completed (both legs filled)")
                self.mgr.print_portfolio("COMPLETED(close)")
                return

    def _poll_both(self):
        if time.time() - self.last_poll_ts < 1.0:
            return
        self.last_poll_ts = time.time()

        if self.stock_order.seqno:
            st = self.mgr.execution.get_order_status(self.stock_order.seqno)
            if st:
                s_str = st.get("status")
                qty = int(st.get("deal_quantity", 0) or 0)
                if s_str == "Filled" or qty >= self.intent.qty:
                    self.on_order_update(self.stock_order.seqno, "00", "Polled Fill", st)
                elif s_str == "Cancelled":
                    self.log("[CLOSE][SIM] Stock cancelled (polled)")
                    self.stock_order.status = "Cancelled"
                    self.mgr.print_portfolio("CANCEL(close_stock)")

        if self.future_order.seqno:
            st = self.mgr.execution.get_order_status(self.future_order.seqno)
            if st:
                s_str = st.get("status")
                qty = int(st.get("deal_quantity", 0) or 0)
                if s_str == "Filled" or qty >= self.intent.qty:
                    self.on_order_update(self.future_order.seqno, "00", "Polled Fill", st)
                elif s_str == "Cancelled":
                    self.log("[CLOSE][SIM] Future cancelled (polled)")
                    self.future_order.status = "Cancelled"
                    self.mgr.print_portfolio("CANCEL(close_future)")

    def _timeout_and_hedge(self):
        """
        Close Hedge: Sell Stock / Buy Future.
        Uses InstrumentRegistry for dynamic ratio.
        """
        HEDGE_WINDOW_SEC = 3.0
        HARD_TIMEOUT_SEC = 15.0
        
        # Dynamic Ratio
        RATIO = InstrumentRegistry.get_ratio(self.intent.stock_code, self.intent.future_code)
        if RATIO < 1: RATIO = 1

        with self._lock:
            if self.state in [TransactionState.COMPLETED, TransactionState.FAILED]:
                return

            age = time.time() - self.state_enter_ts

            if age > HARD_TIMEOUT_SEC:
                self.log("[CLOSE][SIM] Hard timeout -> cancel & repair")
                if self.stock_order.seqno:
                    self.mgr.execution.cancel_order_by_seqno(self.stock_order.seqno)
                if self.future_order.seqno:
                    self.mgr.execution.cancel_order_by_seqno(self.future_order.seqno)
                self.mgr.trigger_repair = True
                self.set_state(TransactionState.FAILED, "CLOSE SIM hard timeout -> repaired")
                self.mgr.print_portfolio("REPAIR(close_timeout)")
                return

            if age > HEDGE_WINDOW_SEC:
                if time.time() - getattr(self, "last_hedge_ts", 0) < 2.0:
                    return

                s_filled = self.stock_order.filled_qty
                f_filled = self.future_order.filled_qty

                # Case 1: Future filled MORE than Stock (Excess Short Closed)
                # Future Bought (Closed Short) > Stock Sold (Closed Long).
                # We need to Sell MORE Stock.
                target_s_sold = f_filled * RATIO
                missing_s_sold = target_s_sold - s_filled

                if missing_s_sold > 0:
                    self.log(f"[CLOSE][SIM] Hedge: Future filled {f_filled} (bought), Stock {s_filled} (sold). Need Sell {missing_s_sold} Stocks.")
                    h_seq = self.mgr.execution.place_stock_order(
                        self.intent.stock_code, "Sell", 0, missing_s_sold, "MKT", "ROD"
                    )
                    if h_seq: self.extra_seqnos.add(h_seq)
                    self.mgr.print_portfolio("HEDGE(close_sell_stock_remain)")
                    self.last_hedge_ts = time.time()

                # Case 2: Stock filled MORE than Future (Excess Long Closed)
                # Stock Sold > Future Bought.
                # We need to Buy MORE Future.
                # Matched = f * 2. Excess Sold = s - Matched.
                matched_s_sold = f_filled * RATIO
                excess_s_sold = s_filled - matched_s_sold
                
                if excess_s_sold > 0:
                    buy_f_qty = excess_s_sold // RATIO
                    odd_s_qty = excess_s_sold % RATIO
                    
                    if buy_f_qty > 0:
                        self.log(f"[CLOSE][SIM] Hedge: Excess Stock Sold {excess_s_sold}. Buying {buy_f_qty} Future(s).")
                        h_seq = self.mgr.execution.place_future_order(
                            self.intent.future_code, "Buy", 0, buy_f_qty, "MKT", "IOC"
                        )
                        if h_seq: self.extra_seqnos.add(h_seq)
                    
                    if odd_s_qty > 0:
                         # We sold an odd number of stocks (e.g. 1) that can't be matched by buying a whole future.
                         # We are now 'Under-hedged' (Short 0.5 Future equivalent).
                         # Option A: Re-Buy the Stock (Undo).
                         # Option B: Leave it (We reduced position).
                         # Choosing Option A (Retreat) to be safe/neutral, similar to Open logic.
                         self.log(f"[CLOSE][SIM] Hedge: Odd Stock Sold {odd_s_qty}. Cannot Close Future. Re-Buying Stock (Retreat).")
                         h_seq = self.mgr.execution.place_stock_order(
                            self.intent.stock_code, "Buy", 0, odd_s_qty, "MKT", "ROD"
                         )
                         if h_seq: self.extra_seqnos.add(h_seq)

                    self.mgr.print_portfolio("HEDGE(close_buy_fut_remain)")
                    self.last_hedge_ts = time.time()


# --- Pair Discovery ---  (你貼的 PairDiscovery 原樣使用)
class PairDiscovery:
    """
    Pair selection rules (as requested):
      1) 監看 pair 入選門檻：股票成交量 AND 期貨成交量 都要夠大才入選
      2) 入選後排序：用「股票成交量 + 選擇權成交量」做排序（越大越前）
         - 選擇權成交量：以 underlying_code==stock_code 的 option contracts 做聚合（最佳努力）
    """
    def __init__(self, api: sj.Shioaji, stock_account=None, futopt_account=None):
        self.api = api
        self.stock_account = stock_account
        self.futopt_account = futopt_account

        # Thresholds (you can tune)
        self.MIN_STOCK_VOL = 3000        # 股票入選最低成交量
        self.MIN_FUTURE_VOL = 3000        # 期貨入選最低成交量

        # Option aggregation control
        self.OPTION_PER_UNDERLYING_CAP = 120
        self.OPTION_TOTAL_CAP = 2000

    def _load_local_history(self) -> Tuple[set, set]:
        l_stocks = set()
        l_futures = set()
        path = "order_history.jsonl"
        if not os.path.exists(path):
            return l_stocks, l_futures

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
                    except Exception:
                        pass
            print(f"[Discovery] Recovered from history - Stocks: {len(l_stocks)}, Futures: {len(l_futures)}")
            if l_futures:
                print(f"[Discovery] History Futures Sample: {list(l_futures)[:5]}")
        except Exception as e:
            print(f"[Discovery] History load failed: {e}")

        return l_stocks, l_futures

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

                sa = self.stock_account or getattr(self.api, "stock_account", None)
                fa = self.futopt_account or getattr(self.api, "futopt_account", None)

                if sa:
                    pos = self.api.list_positions(sa)
                    for p in pos:
                        code = getattr(p, "code", "")
                        if code:
                            held_stocks.add(code)

                if fa:
                    pos = self.api.list_positions(fa)
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
                all_futures.append(contract)
        except Exception as e:
            print(f"[Discovery] Future contract scan failed: {e}")
        
        print(f"[Discovery] Fetched {len(all_futures)} future contracts.")
        
        # Register all future contracts
        for c in all_futures:
            InstrumentRegistry.register(c.code, c)
            
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
        all_options = []
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
                all_options.append(c)
        except Exception as e:
            print(f"[Discovery] Option contract scan failed: {e}")

        print(f"[Discovery] Fetched {len(all_options)} option contracts.")

        # Register all option contracts
        for c in all_options:
            InstrumentRegistry.register(c.code, c)

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
        h_stocks, h_futures = self._load_local_history()

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

        # 3) Build candidate stock/future contracts for volume filtering
        print("[Discovery] Resolving stock & future contracts for volume scans...")
        stk_contracts: List[Any] = []
        fut_contracts: List[Any] = []
        underlying_list: List[str] = []
        
        # New: Store all stock contracts for registration
        all_stock_contracts: List[Any] = []

        for u_code, f_c in u2f.items():
            try:
                s_c = resolve_stock_contract(self.api, u_code)
                if s_c is None:
                    continue
                
                # Register stock contract
                InstrumentRegistry.register(s_c.code, s_c)
                all_stock_contracts.append(s_c)

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

        # 5) eligibility requires BOTH stock volume and future volume be high.
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
            
            # --- RATIO FILTER (Moved) ---
            # Strictly enforce Integer Multiples.
            # Reject Mini Futures (e.g. 100) if Stock is 1000.
            # Ratio = FutMult / StockMult
            
            s_mult = InstrumentRegistry.get_multiplier(u_code)
            f_mult = InstrumentRegistry.get_multiplier(f_code)
            
            if s_mult <= 0: s_mult = 1000 # Safety
            
            if f_mult < s_mult:
                # e.g. Future=100, Stock=1000. Ratio=0.1. Skip.
                continue
            
            if f_mult % s_mult != 0:
                # e.g. Future=2500, Stock=1000 (hypothetical). Ratio=2.5. Skip.
                continue
            # --------------------

            s_vol = stk_vol_map.get(u_code, 0)
            f_vol = fut_vol_map.get(f_code, 0)
            o_vol = opt_vol_by_underlying.get(u_code, 0)
            sc = s_vol + o_vol + int(0.5 * f_vol)

            found.append((u_code, f_code))
            print(f"[Discovery] Added: {u_code}<->{f_code} | StockVol={s_vol} FutVol={f_vol} OptVol={o_vol} Score={sc}")

        # 11) fallback
        if not found:
            print("[Discovery] No pairs found under current volume thresholds. Fallback attempting 2330...")
            if resolve_stock_contract(self.api, "2330") and "2330" in u2f:
                s_c = resolve_stock_contract(self.api, "2330") # Re-fetch to object
                if s_c: InstrumentRegistry.register("2330", s_c)
                
                f_c = u2f["2330"]
                f_code = getattr(f_c, "code", "")
                if f_code:
                    InstrumentRegistry.register(f_code, f_c)
                    found.append(("2330", f_code))

        final_list = found[:limit]
        print(f"[Discovery] Final Pairs: {final_list}")
        return final_list


# --- Transaction Manager ---  (你貼的 TxMgr 原樣使用，只保留必要 import/接口)
class TransactionManager:
    def __init__(self, initial_cash: float = 1_000_000.0):
        self._lock = threading.Lock()
        self.execution = ExecutionEngine()
        self.held_positions: set[str] = set()
        # ===== Book size gates (YOU CAN TUNE) =====
        self.MIN_STK_BOOK_QTY = 10   # 股票：最佳一檔 bid/ask 量需 >= 10 才允許市價
        self.MIN_FUT_BOOK_QTY = 10   # 期貨：最佳一檔 bid/ask 量需 >= 10 才允許市價
        # ========================================

        self.market_data = MarketData(on_tick_callback=self._on_market_tick, 
                                      min_stk_qty=self.MIN_STK_BOOK_QTY, 
                                      min_fut_qty=self.MIN_FUT_BOOK_QTY)
        # ========================================

        self.active_transactions: Dict[str, BaseTransaction] = {}
        self.completed_transactions: List[BaseTransaction] = []
        self.cooldowns: Dict[str, float] = {}

        def _check_holdings(stock_code, future_code):
            # 1. Check strict Stock holding (held positions)
            if stock_code in self.held_positions:
                return True
            # 2. Check overlap in ACTIVE transactions (Stock or Future used?)
            with self._lock:
                for tx in self.active_transactions.values():
                    if tx.intent.stock_code == stock_code or tx.intent.future_code == future_code:
                        return True
            # 3. Check Cooldowns (both)
            if time.time() < self.cooldowns.get(stock_code, 0) or time.time() < self.cooldowns.get(future_code, 0):
                return True
            return False

        def _get_book_gates():
            return (self.MIN_STK_BOOK_QTY, self.MIN_FUT_BOOK_QTY)

        self.strategy = StrategyEngine(self.market_data, check_holdings_cb=_check_holdings, get_book_gate_cb=_get_book_gates)

        self.monitored_pairs: List[Tuple[str, str]] = []
        self._pair_map: Dict[str, List[Tuple[str, str]]] = {}

        self.execution.on_order_callback = self.on_execution_event

        self.running = False
        self.last_sync_ts = 0.0
        self._last_status_refresh = 0.0
        
        self.trigger_repair: bool = False
        self.last_repair_ts: float = 0.0
        self.last_conn_check_ts: float = time.time()

        self.reporter: Optional[AccountReporter] = None
        self.ledger = PortfolioLedger(fut_multiplier_default=2000, fut_margin_ratio=0.5) # shares/2000 default
        self.ledger.cash_total = initial_cash # Set initial cash
        self.initial_cash = initial_cash # Store initial cash for fallback

        # --- Event Queue Implementation ---
        self.event_queue = queue.Queue()
        self._worker_thread = threading.Thread(target=self._event_worker_loop, daemon=True, name="TxEventWorker")
        self._worker_thread.start()

        # --- Quote Worker Implementation (Coalescing) ---
        self._pending_quote_codes: set[str] = set()
        self._quote_event = threading.Event()
        self._quote_lock = threading.Lock()
        self._quote_worker_thread = threading.Thread(target=self._quote_worker_loop, daemon=True, name="QuoteWorker")
        self._quote_worker_thread.start()


    def print_portfolio(self, tag: str):
        """
        Print positions/cash/pending (best-effort).
        Called on submit/fill/cancel/repair.
        """
        print(self.ledger.snapshot(tag))

    # _detect_stock_lot_unit removed (obsolete)

    def start(self):
        if not self.execution.login():
            return False

        # reporter init (after accounts ready)
        self.reporter = AccountReporter(self.execution.api, self.market_data, self.execution.stock_account, self.execution.futopt_account)

        self.running = True

        # --- Define and Register Quote Callbacks (Explicitly) ---
        print("[TxMgr] Registering Quote Callbacks...")
        
        def _on_tick_stk(exchange, tick):
            self.market_data.update_stock(tick)
            
        def _on_tick_fop(exchange, tick):
            self.market_data.update_future(tick)

        def _on_quote_stk(exchange, quote):
             self.market_data.update_stock(quote)
             
        def _on_quote_fop(exchange, quote):
             self.market_data.update_future(quote)

        # Register TICK
        self.execution.api.quote.set_on_tick_stk_v1_callback(_on_tick_stk)
        self.execution.api.quote.set_on_tick_fop_v1_callback(_on_tick_fop)
        
        # Register BIDASK
        self.execution.api.quote.set_on_bidask_stk_v1_callback(_on_quote_stk)
        self.execution.api.quote.set_on_bidask_fop_v1_callback(_on_quote_fop)
        
        print("[TxMgr] Callbacks Registered.")

        self._sync_held_positions()
        
        # Force Share Unit (Unit.Share enforced in ExecutionEngine)
        # self.ledger.stock_multiplier assignment removed (Dynamic Registry used)
        
        self.sync_balance()

        self.print_portfolio("START")
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
            current_held = set()
            for code, qty in pos_map.items():
                if qty != 0:
                    current_held.add(code)

            recalc_held = current_held.copy()
            with self._lock:
                for tx in self.active_transactions.values():
                    recalc_held.add(tx.intent.stock_code)

            self.held_positions = recalc_held
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

            count = 0
            for code, qty in pos_map.items():
                if qty != 0:
                    self.held_positions.add(code)
                    count += 1
            print(f"[TxMgr] Initial Sync: {count} active positions cached: {self.held_positions}")
        except Exception as e:
            print(f"[TxMgr] Initial Sync Failed: {e}")

    def _build_pair_map(self):
        # self._pair_map.clear() # Keep existing to support non-monitored holdings
        for s, f in self.monitored_pairs:
            if s not in self._pair_map:
                self._pair_map[s] = []
            self._pair_map[s].append((s, f))

            if f not in self._pair_map:
                self._pair_map[f] = []
            self._pair_map[f].append((s, f))

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
                self.execution.api.quote.subscribe(c_stk, quote_type=sj_constant.QuoteType.Tick)
                self.execution.api.quote.subscribe(c_stk, quote_type=sj_constant.QuoteType.BidAsk)
            if c_fut:
                self.execution.api.quote.subscribe(c_fut, quote_type=sj_constant.QuoteType.Tick)
                self.execution.api.quote.subscribe(c_fut, quote_type=sj_constant.QuoteType.BidAsk)

    def _on_market_tick(self, code: str):
        # Optimization: Don't block API thread. Signal worker.
        with self._quote_lock:
             self._pending_quote_codes.add(code)
        self._quote_event.set()

    def _quote_worker_loop(self):
        print("[TxMgr] Quote Logic Worker Started.")
        while True:
             self._quote_event.wait()
             self._quote_event.clear()

             pending = []
             with self._quote_lock:
                 if not self._pending_quote_codes:
                     continue
                 pending = list(self._pending_quote_codes)
                 # Clear set implicitly by creating new empty set
                 # (so incoming ticks during processing fill the new one)
                 self._pending_quote_codes = set()
            
             if not pending:
                 continue

             # Coalesce: find unique pairs affected
             unique_pairs = set()
             for code in pending:
                 pairs = self._pair_map.get(code)
                 if pairs:
                     for p in pairs:
                         unique_pairs.add(p)
            
             # Process Unique Pairs
             for s, f in unique_pairs:
                 # Check active transactions with main lock
                 with self._lock:
                     if s in self.active_transactions:
                         continue
                 
                 # Strategy logic (thread-safe inside StrategyEngine)
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

            # Strict Safety: Check quantity matching
            # Prevent closing more than we hold in matched pairs
            matched_qty = min(s_qty, abs(f_qty))
            if matched_qty < intent.qty:
                 print(f"[Safety] CLOSE REJECT: Quantity Mismatch. Intent={intent.qty} but matched(hedged)={matched_qty}. (S={s_qty}, F={f_qty})")
                 self.cooldowns[intent.stock_code] = time.time() + 10
                 return False

            return True

    def request_new_transaction(self, intent: TradeIntent) -> bool:
        with self._lock:
            self.sync_balance() # JIT Sync inside lock
            if intent.stock_code in self.active_transactions:
                return False

            for k in (intent.stock_code, intent.future_code):
                if k in self.cooldowns and time.time() < self.cooldowns[k]:
                    return False

            if not self._check_position_safety(intent):
                return False

            print(f"[{time.strftime('%H:%M:%S')}] [Tx-{str(uuid.uuid4())[:8]}] Transaction Created: {intent}")
            # Print status before order as requested
            # self.print_portfolio("PRE-ORDER") # Removed to avoid double print


            print(f"[TxMgr] ACCEPT: Starting new transaction for {intent.stock_code} ({intent.details})")

            # ===== IMPORTANT: use SIMULTANEOUS transactions =====
            if intent.type == SignalType.OPEN:
                tx = SimultaneousOpenTransaction(intent, self)
                # Estimate cost for reservation
                stk_snap = self.market_data.get_stock(intent.stock_code)
                fut_snap = self.market_data.get_future(intent.future_code)
                if stk_snap and fut_snap:
                    # OPEN: Buy Stock, Sell Future
                    est_price = stk_snap.ask if stk_snap.ask > 0 else stk_snap.price
                    
                    # Accurate Cost Calculation
                    fut_mult = self.execution.get_future_multiplier(intent.future_code)
                    req_cash = est_price * intent.qty * fut_mult
                    
                    est_fut = fut_snap.bid if fut_snap.bid > 0 else fut_snap.price
                    req_margin = est_fut * intent.qty * fut_mult * self.ledger.fut_margin_ratio

                    # --- FUND CHECK ---
                    if not self.ledger.check_funds(req_cash, req_margin):
                        print(f"[TxMgr] REJECT OPEN: Insufficient Funds. Required Cash={req_cash:,.0f}, Margin={req_margin:,.0f}")
                        return False

                    self.ledger.reserve_funds(req_cash, req_margin)
                    tx.reserved_cash = req_cash
                    tx.reserved_margin = req_margin

            elif intent.type == SignalType.CLOSE:
                tx = SimultaneousCloseTransaction(intent, self)
            else:
                tx = BaseTransaction(intent, self)

            self.active_transactions[intent.stock_code] = tx
            self.print_portfolio("ACCEPT(tx)")
            return True

    def _release_reservation(self, tx):
        """Releases reserved funds when tx is done/failed."""
        reserved_c = getattr(tx, "reserved_cash", 0.0)
        reserved_m = getattr(tx, "reserved_margin", 0.0)
        self.ledger.release_reserved(reserved_c, reserved_m)
        tx.reserved_cash = 0.0
        tx.reserved_margin = 0.0

    # ---------- Balance Sync ----------
    def sync_balance(self):
        """
        Try to fetch real balance. If fails, fallback to estimation:
        Cash = Initial - HoldingCost - MarginUsed
        (Ignoring realized PnL from past unrelated trades, as best effort)
        """
        # 1. Update Positions: Ledger MUST be updated first for correct holding cost
        self.execution.update_ledger_positions(self.ledger)

        success = False

        # Only try real balance if NOT simulation
        if not getattr(self.execution.api, "simulation", False):
            try:
                # Try stock account balance
                if self.execution.stock_account:
                    bal = self.execution.api.account_balance(device_id="", acc_id=self.execution.stock_account.account_id)
                    current_bal = None
                    if isinstance(bal, list) and len(bal) > 0:
                        b = bal[0]
                        for attr in ["acc_balance", "available_balance", "net_asset"]:
                            if hasattr(b, attr):
                                current_bal = float(getattr(b, attr))
                                break
                    elif hasattr(bal, "acc_balance"):
                        current_bal = float(bal.acc_balance)

                    if current_bal is not None:
                        self.ledger.cash_total = current_bal
                        success = True
                        print(f"[TxMgr] Sync Balance Success: {current_bal:,.2f}")
            except Exception as e:
                print(f"[TxMgr] Balance Fetch Warning: {e}")

        if not success:
            # Fallback
            s_cost, f_margin = self.ledger.est_holding_cost()
            estimated = self.initial_cash - s_cost - f_margin
            self.ledger.cash_total = estimated

    def archive_transaction(self, tx: BaseTransaction):
        # 1. Cleanup Orders (Prevent Zombies)
        # Even if state is FAILED, orders might be ROD/Active. Cancel them.
        if tx.stock_order.seqno:
            self.execution.cancel_order_by_seqno(tx.stock_order.seqno)
        if tx.future_order.seqno:
            self.execution.cancel_order_by_seqno(tx.future_order.seqno)
        if hasattr(tx, "extra_seqnos"):
            for seq in tx.extra_seqnos:
                 self.execution.cancel_order_by_seqno(seq)

        with self._lock:
            if tx.intent.stock_code in self.active_transactions:
                del self.active_transactions[tx.intent.stock_code]

        self._release_reservation(tx)

        self.completed_transactions.append(tx)
        # keep last 50
        if len(self.completed_transactions) > 50:
            self.completed_transactions.pop(0)

        print(f"[TxMgr] Archiving Tx {tx.tx_id}")
        
        # IO Operation (sync_balance -> account_balance) MUST be outside lock to prevent GUI freeze
        self.sync_balance()
            
        self.print_portfolio("ARCHIVE")

        # Apply cooldown if needed (e.g. after close or fail)
        if tx.state == TransactionState.FAILED or tx.intent.type == SignalType.CLOSE:
            # simple cooldown (both sides)
            duration = 60.0
            print(f"[TxMgr] Apply Cooldown ({duration}s) for {tx.intent.stock_code} & {tx.intent.future_code}")
            self.cooldowns[tx.intent.stock_code] = time.time() + duration
            self.cooldowns[tx.intent.future_code] = time.time() + duration

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

        # Connection Watchdog
        if time.time() - self.last_conn_check_ts > SystemConfig.CONNECTION_CHECK_INTERVAL:
            self.last_conn_check_ts = time.time()
            if not self.execution.check_connection():
                print("[TxMgr] Connection Health Check FAILED. Triggering Re-login...")
                self.execution.relogin()
            else:
                pass

        # Disable console print if GUI is active (simplification: just comment out for now or check flag)
        # print(f"[Main] Active: {len(self.active_transactions)} | Monitored: {len(self.monitored_pairs)} | {time.strftime('%H:%M:%S')}", end='\r')

        with self._lock:
            # COPY keys to avoid 'dictionary changed size during iteration'
            codes = list(self.active_transactions.keys())

        clean_up_list = []

        for code in codes:
            # Re-acquire lock briefly just to get the object safely
            # (In case it was deleted by another thread - unlikely but safe)
            with self._lock:
                tx = self.active_transactions.get(code)
            
            if not tx:
                continue

            # Run logic OUTSIDE the lock
            # self.archive_transaction will re-acquire lock if needed
            tx.update()

            if tx.state in [TransactionState.COMPLETED, TransactionState.FAILED, TransactionState.CANCELLED]:
                clean_up_list.append(tx)
            elif time.time() - tx.created_at > SystemConfig.ZOMBIE_TIMEOUT:
                # Zombie check
                print(f"[TxMgr] Force-clearing ZOMBIE tx {tx.tx_id} (Age > {SystemConfig.ZOMBIE_TIMEOUT}s)")
                tx.set_state(TransactionState.FAILED, "Zombie Timeout Force Clear")
                clean_up_list.append(tx)

        for tx in clean_up_list:
             self.archive_transaction(tx)

        # Non-blocking Repair Check
        if self.trigger_repair:
            if time.time() - self.last_repair_ts > SystemConfig.REPAIR_COOLDOWN:
                self.trigger_repair = False
                self.last_repair_ts = time.time()
                self.repair_positions()

    def repair_positions(self):
        print("\n[TxMgr] === REPAIRING POSITIONS (Aggressive Scan) ===")
        self.print_portfolio("REPAIR(start)")
        
        try:
            if self.execution.stock_account:
                self.execution.api.update_status(self.execution.stock_account)
            if self.execution.futopt_account:
                self.execution.api.update_status(self.execution.futopt_account)
        except Exception:
            pass

        # 1. Get ALL Positions (not just monitored)
        pos_map = self.execution.get_all_positions()
        
        # --- Active Transaction Netting ---
        # Provide immunity to active/healthy transactions so we don't double-hedge them.
        active_claims = {}
        with self._lock:
            for tx in self.active_transactions.values():
                # Stock Claim
                if tx.stock_order.filled_qty > 0:
                    # Default to Buy if not set (safe assumption for unsubmitted? No, filled implies submitted)
                    act = tx.stock_order.action or "Buy" 
                    sign = 1 if act == "Buy" else -1
                    active_claims[tx.intent.stock_code] = active_claims.get(tx.intent.stock_code, 0) + (tx.stock_order.filled_qty * sign)
                
                # Future Claim
                if tx.future_order.filled_qty > 0:
                    act = tx.future_order.action or "Sell"
                    sign = 1 if act == "Buy" else -1
                    active_claims[tx.intent.future_code] = active_claims.get(tx.intent.future_code, 0) + (tx.future_order.filled_qty * sign)

        # Calculate Effective (Adjusted) Positions
        adjusted_pos_map = pos_map.copy()
        for code, claim in active_claims.items():
            new_val = adjusted_pos_map.get(code, 0) - claim
            adjusted_pos_map[code] = new_val
        
        print(f"[Repair] Raw: {pos_map}")
        print(f"[Repair] Claims (Active Txs): {active_claims}")
        print(f"[Repair] Effective (for scan): {adjusted_pos_map}")
        
        # Use adjusted map for repair logic
        pos_map = adjusted_pos_map 
        # ----------------------------------

        # Reverse map for finding partners
        s2f_map = {}
        f2s_map = {}
        for s, pairs in self._pair_map.items():
            for _, f in pairs:
                # pick first future as primary hedge target if multiple
                if s not in s2f_map:
                    s2f_map[s] = f
                f2s_map[f] = s
        
        checked = set()

        for code, qty in pos_map.items():
            if qty == 0: continue
            if code in checked: continue
            
            # Case A: Stock
            if code in self._pair_map:
                checked.add(code)
                s_qty = qty
                if s_qty > 0:
                    # Should have corresponding future short
                    f_code = s2f_map.get(code)
                    if not f_code:
                        print(f"[Repair] Unmapped Stock {code} held. Manual check needed.")
                        continue
                     
                    f_qty = pos_map.get(f_code, 0)
                checked.add(f_code)
                
                # Dynamic Ratio
                RATIO = InstrumentRegistry.get_ratio(code, f_code)
                if RATIO < 1: RATIO = 1 # Safety
                
                # Target F = - (Stock / Ratio)
                target_f = - (s_qty // RATIO)
                diff = f_qty - target_f # current - target
                
                if diff > 0: 
                    # f_qty (-1) > target (-3) => diff=2. Under-hedged (Short not enough).
                    # Need to Sell more.
                    sell_q = diff
                    print(f"[Repair] Under-hedged Stock {code} (S={s_qty}, F={f_qty}, Ratio={RATIO}). Selling {sell_q} Future {f_code}.")
                    self.execution.place_future_order(f_code, "Sell", 0, sell_q, "MKT", "IOC")
                elif diff < 0:
                    # f_qty (-5) < target (-3) => diff=-2. Over-hedged (Short too much).
                    # Need to Buy back.
                    buy_q = abs(diff)
                    print(f"[Repair] Over-hedged Stock {code} (S={s_qty}, F={f_qty}, Ratio={RATIO}). Buying {buy_q} Future {f_code}.")
                    self.execution.place_future_order(f_code, "Buy", 0, buy_q, "MKT", "IOC")
                        
            # Case B: Future
            elif code in f2s_map:
                # If we are here, it means we didn't visit it via Stock loop (implying Stock qty=0 or not in pos_map)
                checked.add(code)
                f_qty = qty
                s_code = f2s_map[code]
                
                # Check if we hold stock? 
                s_qty = pos_map.get(s_code, 0)
                # If s_qty > 0, we would have handled it in Case A (unless s_code not in keys?)
                # If s_qty == 0:
                if s_qty == 0 and f_qty != 0:
                    print(f"[Repair] Naked Future {code} (qty={f_qty}). Closing...")
                    action = "Buy" if f_qty < 0 else "Sell"
                    self.execution.place_future_order(code, action, 0, abs(f_qty), "MKT", "IOC")

        print("[TxMgr] Repair Scan Done.")
        self.print_portfolio("REPAIR(done)")

    def on_execution_event(self, oid, seqno, op_code, op_msg, status_data):
        """
        API Callback Thread (Producer).
        Non-blocking: Just puts data into queue.
        """
        event = {
            "type": "ORDER_UPDATE",
            "oid": oid,
            "seqno": seqno,
            "op_code": op_code,
            "op_msg": op_msg,
            "status_data": status_data,
            "ts": time.time()
        }
        self.event_queue.put(event)

    def _event_worker_loop(self):
        """
        Worker Thread (Consumer).
        Takes events, acquires Locks, updates State.
        """
        print("[TxMgr] Event Worker Thread Started.")
        while True:
            try:
                event = self.event_queue.get()
                if event is None: # Sentinel
                    break
                
                # Dispatch
                if event.get("type") == "ORDER_UPDATE":
                    self._handle_order_update_event(event)
                
                self.event_queue.task_done()
            except Exception as e:
                print(f"[TxMgr][Worker] Error: {e}")
                traceback.print_exc()

    def _handle_order_update_event(self, event):
        seqno = event["seqno"]
        op_code = event["op_code"]
        op_msg = event["op_msg"]
        status_data = event["status_data"]
        
        found_active = False

        with self._lock:
            # Copy list to iterate safely
            active_txs = list(self.active_transactions.values())

        for tx in active_txs:
            # We call on_order_update which has its own tx._lock
            # This is safe because _event_worker_loop is not holding any other conflicting locks
            
            if tx.stock_order.seqno and tx.stock_order.seqno == seqno:
                tx.on_order_update(seqno, op_code, msg=op_msg, status_data=status_data)
                found_active = True
                break

            if tx.future_order.seqno and tx.future_order.seqno == seqno:
                tx.on_order_update(seqno, op_code, msg=op_msg, status_data=status_data)
                found_active = True
                break
            
            if hasattr(tx, "extra_seqnos") and seqno in tx.extra_seqnos:
                 tx.on_order_update(seqno, op_code, msg=op_msg, status_data=status_data)
                 found_active = True
                 break

        if found_active:
            return

        # -------------------------------------------------------------
        # Re-queue Logic for "Too Fast" Callbacks (Submit-Callback Gap)
        # -------------------------------------------------------------
        retries = event.get("retries", 0)
        MAX_RETRIES = 5
        
        if retries < MAX_RETRIES:
            # Maybe the Main Thread hasn't assigned the seqno yet (Unlock-IO-Lock gap).
            # We give it a chance by re-queueing with a small delay.
            event["retries"] = retries + 1
            print(f"[TxMgr][Worker] Seq {seqno} not found in active Txs. Re-queueing (Attempt {retries+1}/{MAX_RETRIES})...")
            
            # Non-blocking delay: ensure we don't busy-spin or block other events
            # Use a Timer to put it back after 200ms
            def _requeue():
                self.event_queue.put(event)
            
            t = threading.Timer(0.2, _requeue)
            t.start()
            return
        
        # -------------------------------------------------------------
        # Late fill detection for archived txs (Genuine Orphans)
        # -------------------------------------------------------------
        
        print(f"[TxMgr][Worker] Seq {seqno} dropped from Active search after {MAX_RETRIES} retries. Checking Archives...")

        deal_qty = 0
        try:
            deal_qty = int(status_data.get("deal_quantity", 0) or 0)
        except Exception:
            deal_qty = 0

        if deal_qty > 0:
            # We use self._lock to protect read of completed_transactions if we want to be strict,
            # but usually this list is append-only + pop(0). 
            # Let's acquire lock briefly to be safe.
            msg_tx = None
            with self._lock:
                 for tx in self.completed_transactions:
                    if (tx.stock_order.seqno and tx.stock_order.seqno == seqno) or \
                       (tx.future_order.seqno and tx.future_order.seqno == seqno):
                        msg_tx = tx
                        break
            
            if msg_tx:
                print(f"\n[TxMgr] ⚠️ CRITICAL: Late Fill detected for Archived Tx {msg_tx.tx_id} (Seq={seqno})!")
                print("[TxMgr] This causes a Naked Position. Triggering Auto-Repair...")
                self.trigger_repair = True

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
                edge_pct = 0.0
                if stk_data and fut_data and stk_data.ask > 0:
                    edge_pct = (f_bid - s_ask) / stk_data.ask
                extra.append(f"OPEN Sprd={spread_open:.1f} Z={z:.2f} Edge={edge_pct*100:.2f}% (Avg={st_open.mean:.1f})")
            else:
                extra.append("OPEN (No Stats)")

            if st_close and st_close.samples > 0:
                std = max(st_close.std, self.strategy.MIN_STD)
                z = (spread_close - st_close.mean) / std if std > 0 else 0.0
                extra.append(f"CLOSE Sprd={spread_close:.1f} Z={z:.2f} (Avg={st_close.mean:.1f})")
            else:
                extra.append("CLOSE (No Stats)")

            lines.append(
                f"  {s} Bid={s_bid} Ask={s_ask} (Bsz={stk_data.bid_size if stk_data else 0}/Asz={stk_data.ask_size if stk_data else 0}) "
                f"<-> {f} Bid={f_bid} Ask={f_ask} (Bsz={fut_data.bid_size if fut_data else 0}/Asz={fut_data.ask_size if fut_data else 0}) "
                f"| {' | '.join(extra)}"
            )

        lines.append(f"\nActive Transactions: {len(self.active_transactions)}")
        for code, tx in self.active_transactions.items():
            lines.append(f"  [{code}] State={tx.state.name} Created={time.strftime('%H:%M:%S', time.localtime(tx.created_at))}")
            lines.append(f"    Stock: {tx.stock_order.status} ({tx.stock_order.filled_qty}/{tx.intent.qty}) seq={tx.stock_order.seqno}")
            lines.append(f"    Fut  : {tx.future_order.status} ({tx.future_order.filled_qty}/{tx.intent.qty}) seq={tx.future_order.seqno}")

        if self.reporter:
            lines.append("\n" + self.reporter.summary_text())

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
            print("[Keyboard] 'x' pressed: Running repair...")
            self.mgr.trigger_repair = True
        elif ch == "X":
            print("[Keyboard] 'X' pressed: FORCE REPAIR started...")
            self.mgr.trigger_repair = True
        elif ch == "h":
            self._print_help_once()
        elif ch == "q":
            print("\n[Keyboard] Quit requested...", flush=True)
            self.mgr.stop()
            self.stop()
            print("[Keyboard] Cancelling ALL open orders...", flush=True)
            self.mgr.execution.cancel_all_orders()
            self.mgr.print_portfolio("QUIT(cancel_all)")

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


    # --- GUI Integration ---
    try:
        from gui_dashboard import ArbitrageDashboard
        _HAS_GUI = True
    except ImportError:
        print("[Main] GUI module not found. Falling back to Console mode.")
        _HAS_GUI = False

    if _HAS_GUI:
        print("[Main] Launching GUI Dashboard...")
        dashboard = ArbitrageDashboard(mgr)
        
        # Start background discovery
        try:
             mgr.discover_and_subscribe()
        except Exception as e:
             print(f"[Main] Discover failed: {e}")
        
        # --- THREADING FIX: Run logic in background thread ---
        stop_event = threading.Event()
        
        def background_logic():
            print("[Background] Logic thread started.")
            loops = 0
            while not stop_event.is_set() and mgr.running:
                try:
                    # Run business logic
                    mgr.run_step()
                except Exception as e:
                    print(f"[Background] Error in run_step: {e}")
                
                # FORCE YIELD: Sleep 100ms to allow GUI updates (prevent starvation)
                time.sleep(0.1) 
                loops += 1
            print("[Background] Logic thread stopped.")

        logic_thread = threading.Thread(target=background_logic, daemon=True)
        logic_thread.start()

        # Run GUI loop (Only UI updates)
        try:
            dashboard.run()
        except KeyboardInterrupt:
            pass
        except Exception as e:
            traceback.print_exc()
            print(f"[Main] GUI Error: {e}")
        finally:
            # Signal stop
            stop_event.set()
            mgr.stop()
            # wait briefly
            logic_thread.join(timeout=1.0)
            
    else:
        # Fallback to Console Loop
        kb = KeyboardMonitor(mgr)
        kb.start()
        print("[Main] System Started (Console Mode). Press 'h' for help.")

        try:
            mgr.discover_and_subscribe()
            loops = 0
            while mgr.running:
                mgr.run_step()
                time.sleep(SystemConfig.MAIN_LOOP_SLEEP)
                loops += 1
        except KeyboardInterrupt:
            print("\n[Main] Interrupted.")
        except Exception as e:
            traceback.print_exc()
            print(f"[Main] Error: {e}")
        finally:
            kb.stop()

    if mgr and mgr.execution:
        print("[Main] Safety: Cancelling ALL open orders on exit (Cleanup)...")
        try:
            mgr.execution.cancel_all_orders()
        except Exception as e:
            print(f"[Main] Cleanup Cancel Failed: {e}")
    print("[Main] Shutdown.")


if __name__ == "__main__":
    main()
