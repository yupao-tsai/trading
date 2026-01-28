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
    product: str = ""          # "Stock"/"Future"
    code: str = ""             # contract code
    target_qty: int = 0        # intent qty
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
                s.price = float(tick.close)
            elif hasattr(tick, "price"):
                s.price = float(tick.price)

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
                f.price = float(tick.close)
            elif hasattr(tick, "price"):
                f.price = float(tick.price)

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


class StrategyEngine:
    """
    改為「方向性可成交 spread」
      OPEN : spread_open  = fut_bid - stk_ask  （Buy stock @ask, Sell fut @bid）
      CLOSE: spread_close = fut_ask - stk_bid  （Buy fut @ask, Sell stock @bid）

    另外新增：
      - 開倉門檻：edge_pct >= PROFIT_THRESHOLD_PCT (例如 0.5%)
      - 量要夠大：OPEN 需要 stk.ask_size / fut.bid_size 足夠；CLOSE 需要 stk.bid_size / fut.ask_size 足夠
    """
    def __init__(self, market_data: MarketData, check_holdings_cb: Optional[Callable[[str], bool]] = None,
                 get_book_gate_cb: Optional[Callable[[], Tuple[int, int]]] = None):
        self.md = market_data
        self.check_holdings_cb = check_holdings_cb
        self.get_book_gate_cb = get_book_gate_cb
        self.stats: Dict[Tuple[str, str, str], StrategyState] = {}

        # ===== Parameters you requested =====
        self.PROFIT_THRESHOLD_PCT = 0.005  # 0.5%
        self.MIN_WAIT_STK_QTY = 10         # 你說的「等待量要夠大」(股票)
        self.MIN_WAIT_FUT_QTY = 10         # 期貨
        # ===================================

        # Z-score style (保留原本統計，讓你仍可用 z 的概念做輔助)
        self.Z_ENTRY = 2.0
        self.Z_EXIT = 0.5
        self.MIN_STD = 0.1

        # Debounce to prevent signal storms
        self.signal_cooldown_sec = 10.0
        self._last_signal_ts: Dict[Tuple[str, str, str], float] = {}

    def _get_stats(self, stock_code: str, future_code: str, mode: str) -> StrategyState:
        key = (stock_code, future_code, mode)
        if key not in self.stats:
            self.stats[key] = StrategyState(half_life_sec=300)  # 5-min half life
        return self.stats[key]

    def _tradable_spreads(self, stk: Optional[MarketSnapshot], fut: Optional[MarketSnapshot]) -> Tuple[float, float]:
        """
        Return (spread_open, spread_close) using directional executable prices.
        """
        if not stk or not fut:
            return 0.0, 0.0

        # Need bid/ask valid
        if stk.ask <= 0 or stk.bid <= 0 or fut.ask <= 0 or fut.bid <= 0:
            return 0.0, 0.0

        spread_open = fut.bid - stk.ask   # OPEN: Sell fut @bid, Buy stk @ask
        spread_close = fut.ask - stk.bid  # CLOSE: Buy fut @ask, Sell stock @bid
        return spread_open, spread_close

    def _edge_pct_open(self, stk: MarketSnapshot, fut: MarketSnapshot) -> float:
        # executable prices: buy stock @ask, sell future @bid
        if stk.ask <= 0:
            return 0.0
        return (fut.bid - stk.ask) / stk.ask

    def _edge_pct_close(self, stk: MarketSnapshot, fut: MarketSnapshot) -> float:
        # close executable: buy future @ask, sell stock @bid
        if stk.bid <= 0:
            return 0.0
        return (stk.bid - fut.ask) / stk.bid  # "close edge" as improvement; mostly for debugging
        # 注意：close 的盈利用 edge 不是必要條件；通常你用 z_exit / mean reversion 來退場即可。

    def _wait_qty_gates(self) -> Tuple[int, int]:
        """
        Strategy-level waiting qty gate. Can be overridden by TxMgr's MIN_* book qty if provided.
        """
        stk_gate = self.MIN_WAIT_STK_QTY
        fut_gate = self.MIN_WAIT_FUT_QTY
        if self.get_book_gate_cb:
            try:
                mg_stk, mg_fut = self.get_book_gate_cb()
                # 以更嚴格者為準
                stk_gate = max(stk_gate, int(mg_stk))
                fut_gate = max(fut_gate, int(mg_fut))
            except Exception:
                pass
        return stk_gate, fut_gate

    def _check_liquidity_open(self, stk: MarketSnapshot, fut: MarketSnapshot, qty: int) -> bool:
        stk_gate, fut_gate = self._wait_qty_gates()
        need_stk = stk_gate * max(1, qty)
        need_fut = fut_gate * max(1, qty)
        return (stk.ask_size >= need_stk) and (fut.bid_size >= need_fut)

    def _check_liquidity_close(self, stk: MarketSnapshot, fut: MarketSnapshot, qty: int) -> bool:
        stk_gate, fut_gate = self._wait_qty_gates()
        need_stk = stk_gate * max(1, qty)
        need_fut = fut_gate * max(1, qty)
        return (stk.bid_size >= need_stk) and (fut.ask_size >= need_fut)

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

        if not has_position:
            # update OPEN stats with spread_open
            st = self._get_stats(stock_code, future_code, "OPEN")
            st.update(spread_open)
            if st.samples < 10:
                return None

            std = max(st.std, self.MIN_STD)
            z = (spread_open - st.mean) / std

            # ===== NEW: profit threshold & liquidity gate =====
            edge_pct = self._edge_pct_open(stk, fut) if stk and fut else 0.0
            liq_ok = self._check_liquidity_open(stk, fut, qty=1) if stk and fut else False

            is_entry = (z > self.Z_ENTRY) and (edge_pct >= self.PROFIT_THRESHOLD_PCT) and liq_ok
            if is_entry:
                key = (stock_code, future_code, "OPEN")
                if now - self._last_signal_ts.get(key, 0) < self.signal_cooldown_sec:
                    return None
                self._last_signal_ts[key] = now

                print(f"[Strategy][OPEN] {stock_code} "
                      f"Sprd={spread_open:.2f} Z={z:.2f} (µ={st.mean:.2f} σ={std:.2f}) "
                      f"Edge={edge_pct*100:.2f}% >= {self.PROFIT_THRESHOLD_PCT*100:.2f}% "
                      f"LiqOK={liq_ok} "
                      f"Stk(Ask={stk.ask:.2f} AskSz={stk.ask_size}) Fut(Bid={fut.bid:.2f} BidSz={fut.bid_size})")

                return TradeIntent(
                    type=SignalType.OPEN,
                    stock_code=stock_code,
                    future_code=future_code,
                    qty=1,
                    details=f"OPEN Z={z:.2f}>{self.Z_ENTRY} edge={edge_pct*100:.2f}% liq_ok={liq_ok} sprd={spread_open:.2f}"
                )

            return None

        else:
            # update CLOSE stats with spread_close
            st = self._get_stats(stock_code, future_code, "CLOSE")
            st.update(spread_close)
            if st.samples < 10:
                return None

            std = max(st.std, self.MIN_STD)
            z = (spread_close - st.mean) / std

            liq_ok = self._check_liquidity_close(stk, fut, qty=1) if stk and fut else False

            is_close = (abs(z) < self.Z_EXIT) and liq_ok
            if is_close:
                key = (stock_code, future_code, "CLOSE")
                if now - self._last_signal_ts.get(key, 0) < self.signal_cooldown_sec:
                    return None
                self._last_signal_ts[key] = now

                print(f"[Strategy][CLOSE] {stock_code} "
                      f"Sprd={spread_close:.2f} Z={z:.2f} (µ={st.mean:.2f} σ={std:.2f}) "
                      f"LiqOK={liq_ok} "
                      f"Stk(Bid={stk.bid:.2f} BidSz={stk.bid_size}) Fut(Ask={fut.ask:.2f} AskSz={fut.ask_size})")

                return TradeIntent(
                    type=SignalType.CLOSE,
                    stock_code=stock_code,
                    future_code=future_code,
                    qty=1,
                    details=f"CLOSE |Z|={abs(z):.2f}<{self.Z_EXIT} liq_ok={liq_ok} sprd={spread_close:.2f}"
                )

            return None


# ============================================================
# Account Reporter (Cash / Positions / Pending)
# ============================================================

class AccountReporter:
    """
    Best-effort:
      - Positions (stocks)
      - Available cash / balance
      - Pending notional (from open trades)
    """
    def __init__(self, api: sj.Shioaji, md: MarketData, stock_account=None, futopt_account=None):
        self.api = api
        self.md = md
        self.stock_account = stock_account
        self.futopt_account = futopt_account

    def _safe_to_dict(self, obj: Any) -> Dict[str, Any]:
        if obj is None:
            return {}
        try:
            if isinstance(obj, dict):
                return obj
            if hasattr(obj, "to_dict"):
                return obj.to_dict()
            if hasattr(obj, "__dict__"):
                return dict(obj.__dict__)
        except Exception:
            pass
        return {}

    def get_stock_positions(self) -> Dict[str, int]:
        m: Dict[str, int] = {}
        try:
            if self.stock_account:
                pos = self.api.list_positions(self.stock_account)
                for p in pos:
                    code = getattr(p, "code", "")
                    qty = int(getattr(p, "quantity", 0) or 0)
                    if code:
                        m[code] = qty
        except Exception:
            pass
        return m

    def get_cash_available(self) -> Optional[float]:
        """
        Shioaji versions differ. Try common APIs & keys.
        Return None if unavailable.
        """
        # Try account_balance(account)
        try:
            if hasattr(self.api, "account_balance") and self.stock_account:
                bal = self.api.account_balance(self.stock_account)
                d = self._safe_to_dict(bal)
                # common keys in various env
                for k in ("available_balance", "availableBalance", "withdrawable", "cash_balance", "cashBalance",
                          "acc_balance", "balance", "settled_amount", "settledAmount"):
                    v = d.get(k, None)
                    if v is not None:
                        try:
                            return float(v)
                        except Exception:
                            pass
        except Exception:
            pass

        # Try account_balance() w/o args
        try:
            if hasattr(self.api, "account_balance"):
                bal = self.api.account_balance()
                d = self._safe_to_dict(bal)
                for k in ("available_balance", "availableBalance", "withdrawable", "cash_balance", "cashBalance",
                          "acc_balance", "balance", "settled_amount", "settledAmount"):
                    v = d.get(k, None)
                    if v is not None:
                        try:
                            return float(v)
                        except Exception:
                            pass
        except Exception:
            pass

        return None

    def _estimate_trade_notional(self, trade: Any) -> float:
        """
        For pending notional:
          remaining_qty * estimated_price
        If order.price == 0 (market), estimate from MD by side.
        """
        try:
            order = getattr(trade, "order", None)
            status = getattr(trade, "status", None)
            contract = getattr(trade, "contract", None)

            code = getattr(contract, "code", "") if contract else ""
            qty = int(getattr(order, "quantity", 0) or 0)
            action = getattr(order, "action", None)
            price = float(getattr(order, "price", 0.0) or 0.0)

            # Filled / remaining
            deal_qty = int(getattr(status, "deal_quantity", 0) or 0)
            remaining = max(0, qty - deal_qty)
            if remaining <= 0:
                return 0.0

            is_stock = False
            try:
                # heuristic: stock codes numeric; futures often include letters
                # but not always. We'll fallback to market data availability.
                is_stock = (self.md.get_stock(code) is not None)
            except Exception:
                is_stock = False

            # Market estimate
            est_price = price
            if est_price <= 0:
                if is_stock:
                    snap = self.md.get_stock(code)
                    if snap:
                        # if Buy -> assume ask; Sell -> assume bid
                        if action == sj_constant.Action.Buy:
                            est_price = snap.ask if snap.ask > 0 else snap.price
                        else:
                            est_price = snap.bid if snap.bid > 0 else snap.price
                else:
                    snap = self.md.get_future(code)
                    if snap:
                        if action == sj_constant.Action.Buy:
                            est_price = snap.ask if snap.ask > 0 else snap.price
                        else:
                            est_price = snap.bid if snap.bid > 0 else snap.price

            if est_price <= 0:
                return 0.0

            return float(remaining) * float(est_price)
        except Exception:
            return 0.0

    def get_pending_notional(self) -> float:
        """
        Sum notional of open / part-filled trades.
        """
        total = 0.0
        try:
            trades = self.api.list_trades()
            for t in trades:
                st = getattr(getattr(t, "status", None), "status", "")
                if st in ("PreSubmitted", "Submitted", "PartFilled"):
                    total += self._estimate_trade_notional(t)
        except Exception:
            return 0.0
        return total

    def summary_text(self) -> str:
        pos = self.get_stock_positions()
        cash = self.get_cash_available()
        pending = self.get_pending_notional()

        # Show top holdings
        items = sorted(pos.items(), key=lambda x: abs(x[1]), reverse=True)
        top = items[:10]
        pos_str = ", ".join([f"{c}:{q}" for c, q in top]) if top else "(none)"

        cash_str = f"{cash:.2f}" if cash is not None else "N/A"
        return f"[Portfolio] CashAvail={cash_str} | PendingNotional~={pending:.2f} | StockPosTop={pos_str}"


# --- Portfolio Ledger (for tracking positions and estimating costs) ---

@dataclass
class Position:
    code: str
    qty: int = 0
    avg_price: float = 0.0

class PortfolioLedger:
    def __init__(self, stock_multiplier: int = 1000, fut_multiplier_default: int = 50, fut_margin_ratio: float = 0.5):
        self.stock_pos: Dict[str, Position] = {}
        self.fut_pos: Dict[str, Position] = {}
        self.cash_total: float = 0.0
        self.reserved_stock: float = 0.0
        self.reserved_margin: float = 0.0

        self.stock_multiplier = stock_multiplier
        self.fut_multiplier_default = fut_multiplier_default
        self.fut_margin_ratio = fut_margin_ratio

    def cash_avail(self) -> float:
        return self.cash_total - self.reserved_stock - self.reserved_margin

    def top_stock_positions(self, limit: int = 5) -> str:
        items = sorted(self.stock_pos.values(), key=lambda p: abs(p.qty), reverse=True)
        return ", ".join([f"{p.code}:{p.qty}" for p in items[:limit]]) if items else "(none)"

    def est_holding_cost(self, fut_multipliers: Optional[Dict[str, int]] = None) -> Tuple[float, float]:
        """
        Returns (stock_cost, fut_margin_used)
        stock_cost = sum(qty * avg_price * stock_multiplier)
        fut_margin = sum(abs(qty) * avg_price * fut_multiplier * margin_ratio)
        """
        s_cost = sum(p.qty * p.avg_price * self.stock_multiplier for p in self.stock_pos.values())
        
        f_margin = 0.0
        if fut_multipliers is None:
            fut_multipliers = {}
            
        for code, p in self.fut_pos.items():
            mul = fut_multipliers.get(code, self.fut_multiplier_default)
            # Use avg_price as proxy for current price to estimate margin requirement
            notional = abs(p.qty) * p.avg_price * mul
            f_margin += notional * self.fut_margin_ratio
            
        return s_cost, f_margin

    def snapshot(self, tag: str = "", fut_multipliers: Optional[Dict[str, int]] = None) -> str:
        s_cost, f_margin = self.est_holding_cost(fut_multipliers)
        holding_val = s_cost  # User asked for "Holding Value" implies stock value. 
                              # "Spent" includes margin. 
                              
        return (
            f"[{time.strftime('%H:%M:%S')}] [{tag}] [Portfolio] "
            f"CashTot={self.cash_total:.2f} | CashAvail={self.cash_avail():.2f} | "
            f"HoldingVal={holding_val:.2f} | MarginUsed={f_margin:.2f} | "
            f"ReservedStk={self.reserved_stock:.2f} | ReservedMrg={self.reserved_margin:.2f} | "
            f"StockPosTop={self.top_stock_positions()}"
        )


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

    def place_pair_orders_simultaneous(self,
                                       stock_code: str, stock_action: str, stock_qty: int,
                                       fut_code: str, fut_action: str, fut_qty: int,
                                       stock_price_type: str = "MKT", stock_order_type: str = "ROD",
                                       fut_price_type: str = "MKT", fut_order_type: str = "IOC") -> Tuple[Optional[str], Optional[str]]:
        """
        Place stock & future orders back-to-back (same timestamp window) to reduce leg risk.
        Returns (stock_seqno, future_seqno).
        """
        s_seq = self.place_stock_order(stock_code, stock_action, 0, stock_qty, stock_price_type, stock_order_type)
        f_seq = self.place_future_order(fut_code, fut_action, 0, fut_qty, fut_price_type, fut_order_type)
        return s_seq, f_seq

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

    def update_ledger_positions(self, ledger: 'PortfolioLedger'):
        """Populates ledger.stock_pos and fut_pos from API."""
        try:
            # Stocks
            if self.stock_account:
                for p in self.api.list_positions(self.stock_account):
                    qty = int(p.quantity)
                    if qty == 0: continue
                    # heuristic for price: p.price or p.avg_price or p.cost_price
                    price = float(getattr(p, "price", 0.0) or getattr(p, "avg_price", 0.0) or getattr(p, "cost_price", 0.0))
                    ledger.stock_pos[p.code] = Position(code=p.code, qty=qty, avg_price=price)
            
            # Futures
            if self.futopt_account:
                for p in self.api.list_positions(self.futopt_account):
                    qty = int(p.quantity)
                    if qty == 0: continue
                    direction = getattr(p, "direction", None)
                    net = qty
                    if direction == sj_constant.Action.Sell:
                        net = -qty
                    price = float(getattr(p, "price", 0.0) or getattr(p, "avg_price", 0.0) or getattr(p, "cost_price", 0.0))
                    ledger.fut_pos[p.code] = Position(code=p.code, qty=net, avg_price=price)
                    
        except Exception as e:
            print(f"[Execution] UpdateLedger Failed: {e}")

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

        self.stock_order = OrderStub(product="Stock", code=intent.stock_code, target_qty=intent.qty)
        self.future_order = OrderStub(product="Future", code=intent.future_code, target_qty=intent.qty)

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
        if time.time() - self.state_enter_ts > 90:
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
            self.mgr.repair_positions()
            self.mgr.print_portfolio("REPAIR(no_fut_md)")
            return

        if not self._book_ok_for_market(fut_snap, action, self.mgr.MIN_FUT_BOOK_QTY, "Future"):
            # 量不足：不送單 -> 立刻 repair（避免 stock 已成交但 future 沒對沖）
            self.log("Leg 2 BLOCKED by Future book size. Triggering Auto-Repair...")
            self.mgr.repair_positions()
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
            self.mgr.repair_positions()
            self.set_state(TransactionState.FAILED, "Leg 2 Submission Failed -> repaired")
            self.mgr.print_portfolio("REPAIR(fut_submit_fail)")

    def _check_leg2_timeout(self):
        if time.time() - self.state_enter_ts > 20:
            self.log("Leg 2 Timeout. Cancelling...")
            self.mgr.execution.cancel_order_by_seqno(self.future_order.seqno)
            self.log("Leg 2 Timeout -> Auto-Repair")
            self.mgr.repair_positions()
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
                self.mgr.repair_positions()
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

        self.log(f"[OPEN][SIM] Submitting BOTH legs: Buy Stock {self.intent.stock_code} & Sell Fut {self.intent.future_code} qty={self.intent.qty}")

        s_seq, f_seq = self.mgr.execution.place_pair_orders_simultaneous(
            stock_code=self.intent.stock_code, stock_action="Buy", stock_qty=self.intent.qty,
            fut_code=self.intent.future_code, fut_action="Sell", fut_qty=self.intent.qty,
            stock_price_type="MKT", stock_order_type="ROD",
            fut_price_type="MKT", fut_order_type="IOC"
        )

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
                    self.mgr.repair_positions()
                    self.mgr.print_portfolio("REPAIR(open_stock_fail)")
                return

            delta = _apply_fill(self.stock_order, status_data)
            if delta > 0:
                self.log(f"[OPEN][SIM] Stock fill +{delta} (total {self.stock_order.filled_qty})")
                self.mgr.held_positions.add(self.intent.stock_code)
                self.mgr.print_portfolio("FILL(open_stock)")

        # Future leg
        if seqno == self.future_order.seqno:
            self.log(f"[OPEN][SIM] Future Update: {op_code} {msg}")
            if op_code != "00":
                self.future_order.status = "Failed"
                self.set_state(TransactionState.FAILED, f"OPEN future leg failed: {msg}")
                self.mgr.print_portfolio("FAILED(open_future)")
                # if stock already filled -> repair
                if self.stock_order.filled_qty > 0:
                    self.log("[OPEN][SIM] Future leg failed but stock filled => repair")
                    self.mgr.repair_positions()
                    self.mgr.print_portfolio("REPAIR(open_future_fail)")
                return

            delta = _apply_fill(self.future_order, status_data)
            if delta > 0:
                self.log(f"[OPEN][SIM] Future fill +{delta} (total {self.future_order.filled_qty})")
                self.mgr.print_portfolio("FILL(open_future)")

        # completion check
        if self.stock_order.filled_qty >= self.intent.qty and self.future_order.filled_qty >= self.intent.qty:
            self.stock_order.status = "Filled"
            self.future_order.status = "Filled"
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
        If one leg fills but the other doesn't within a short time,
        cancel remaining and immediately repair.
        """
        # configurable
        HEDGE_WINDOW_SEC = 3.0
        HARD_TIMEOUT_SEC = 15.0

        age = time.time() - self.state_enter_ts

        # Hard timeout -> repair
        if age > HARD_TIMEOUT_SEC:
            self.log("[OPEN][SIM] Hard timeout -> cancel & repair")
            if self.stock_order.seqno:
                self.mgr.execution.cancel_order_by_seqno(self.stock_order.seqno)
            if self.future_order.seqno:
                self.mgr.execution.cancel_order_by_seqno(self.future_order.seqno)
            self.mgr.repair_positions()
            self.set_state(TransactionState.FAILED, "OPEN SIM hard timeout -> repaired")
            self.mgr.print_portfolio("REPAIR(open_timeout)")
            return

            # Hedge window: if one filled and other not, repair quickly
        if age > HEDGE_WINDOW_SEC:
            # Throttle hedging to avoid spamming
            if time.time() - self.last_hedge_ts < 2.0:
                return

            s_filled = self.stock_order.filled_qty
            f_filled = self.future_order.filled_qty

            # stock filled but future not matched
            if s_filled > 0 and f_filled < s_filled:
                remain = s_filled - f_filled
                self.log(f"[OPEN][SIM] Hedge: stock filled {s_filled} but future filled {f_filled}. Need sell fut remain={remain}")
                # try place future order for remaining (IOC)
                self.mgr.execution.place_future_order(self.intent.future_code, "Sell", 0, remain, "MKT", "IOC")
                self.mgr.print_portfolio("HEDGE(open_sell_fut_remain)")
                self.last_hedge_ts = time.time()

            # future filled but stock not matched
            if f_filled > 0 and s_filled < f_filled:
                remain = f_filled - s_filled
                self.log(f"[OPEN][SIM] Hedge: future filled {f_filled} but stock filled {s_filled}. Need buy stock remain={remain}")
                self.mgr.execution.place_stock_order(self.intent.stock_code, "Buy", 0, remain, "MKT", "ROD")
                self.mgr.print_portfolio("HEDGE(open_buy_stock_remain)")
                self.last_hedge_ts = time.time()


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

        self.log(f"[CLOSE][SIM] Submitting BOTH legs: Buy Fut {self.intent.future_code} & Sell Stock {self.intent.stock_code} qty={self.intent.qty}")

        # Note: stock is ROD, future is IOC
        s_seq, f_seq = self.mgr.execution.place_pair_orders_simultaneous(
            stock_code=self.intent.stock_code, stock_action="Sell", stock_qty=self.intent.qty,
            fut_code=self.intent.future_code, fut_action="Buy", fut_qty=self.intent.qty,
            stock_price_type="MKT", stock_order_type="ROD",
            fut_price_type="MKT", fut_order_type="IOC"
        )

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
        # Stock leg
        if seqno == self.stock_order.seqno:
            self.log(f"[CLOSE][SIM] Stock Update: {op_code} {msg}")
            if op_code != "00":
                self.stock_order.status = "Failed"
                self.set_state(TransactionState.FAILED, f"CLOSE stock leg failed: {msg}")
                self.mgr.print_portfolio("FAILED(close_stock)")
                if self.future_order.filled_qty > 0:
                    self.log("[CLOSE][SIM] Stock leg failed but future filled => repair")
                    self.mgr.repair_positions()
                    self.mgr.print_portfolio("REPAIR(close_stock_fail)")
                return

            delta = _apply_fill(self.stock_order, status_data)
            if delta > 0:
                self.log(f"[CLOSE][SIM] Stock fill +{delta} (total {self.stock_order.filled_qty})")
                self.mgr.print_portfolio("FILL(close_stock)")

        # Future leg
        if seqno == self.future_order.seqno:
            self.log(f"[CLOSE][SIM] Future Update: {op_code} {msg}")
            if op_code != "00":
                self.future_order.status = "Failed"
                self.set_state(TransactionState.FAILED, f"CLOSE future leg failed: {msg}")
                self.mgr.print_portfolio("FAILED(close_future)")
                if self.stock_order.filled_qty > 0:
                    self.log("[CLOSE][SIM] Future leg failed but stock filled => repair")
                    self.mgr.repair_positions()
                    self.mgr.print_portfolio("REPAIR(close_future_fail)")
                return

            delta = _apply_fill(self.future_order, status_data)
            if delta > 0:
                self.log(f"[CLOSE][SIM] Future fill +{delta} (total {self.future_order.filled_qty})")
                self.mgr.print_portfolio("FILL(close_future)")

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
        HEDGE_WINDOW_SEC = 3.0
        HARD_TIMEOUT_SEC = 15.0

        age = time.time() - self.state_enter_ts

        if age > HARD_TIMEOUT_SEC:
            self.log("[CLOSE][SIM] Hard timeout -> cancel & repair")
            if self.stock_order.seqno:
                self.mgr.execution.cancel_order_by_seqno(self.stock_order.seqno)
            if self.future_order.seqno:
                self.mgr.execution.cancel_order_by_seqno(self.future_order.seqno)
            self.mgr.repair_positions()
            self.set_state(TransactionState.FAILED, "CLOSE SIM hard timeout -> repaired")
            self.mgr.print_portfolio("REPAIR(close_timeout)")
            return

        if age > HEDGE_WINDOW_SEC:
            s_filled = self.stock_order.filled_qty
            f_filled = self.future_order.filled_qty

            # if future filled but stock not, we still need sell stock for remain
            if f_filled > 0 and s_filled < f_filled:
                remain = f_filled - s_filled
                self.log(f"[CLOSE][SIM] Hedge: future filled {f_filled} but stock filled {s_filled}. Need sell stock remain={remain}")
                self.mgr.execution.place_stock_order(self.intent.stock_code, "Sell", 0, remain, "MKT", "ROD")
                self.mgr.print_portfolio("HEDGE(close_sell_stock_remain)")

            # if stock filled but future not, need buy future remain
            if s_filled > 0 and f_filled < s_filled:
                remain = s_filled - f_filled
                self.log(f"[CLOSE][SIM] Hedge: stock filled {s_filled} but future filled {f_filled}. Need buy fut remain={remain}")
                self.mgr.execution.place_future_order(self.intent.future_code, "Buy", 0, remain, "MKT", "IOC")
                self.mgr.print_portfolio("HEDGE(close_buy_fut_remain)")


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

class TransactionManager:
    def __init__(self, initial_cash: float = 1_000_000.0):
        self._lock = threading.Lock()
        self.market_data = MarketData(on_tick_callback=self._on_market_tick)
        self.execution = ExecutionEngine()
        self.held_positions: set[str] = set()

        # ===== Book size gates (YOU CAN TUNE) =====
        self.MIN_STK_BOOK_QTY = 10   # 股票：最佳一檔 bid/ask 量需 >= 10 才允許市價
        self.MIN_FUT_BOOK_QTY = 10   # 期貨：最佳一檔 bid/ask 量需 >= 10 才允許市價
        # ========================================

        self.active_transactions: Dict[str, BaseTransaction] = {}
        self.completed_transactions: List[BaseTransaction] = []
        self.cooldowns: Dict[str, float] = {}

        def _check_holdings(stock_code):
            return (stock_code in self.active_transactions) or (stock_code in self.held_positions)

        def _get_book_gates():
            return (self.MIN_STK_BOOK_QTY, self.MIN_FUT_BOOK_QTY)

        self.strategy = StrategyEngine(self.market_data, check_holdings_cb=_check_holdings, get_book_gate_cb=_get_book_gates)

        self.monitored_pairs: List[Tuple[str, str]] = []
        self._pair_map: Dict[str, List[Tuple[str, str]]] = {}

        self.execution.on_order_callback = self.on_execution_event

        self.running = False
        self.last_sync_ts = 0.0
        self._last_status_refresh = 0.0

        self.reporter: Optional[AccountReporter] = None
        self.ledger = PortfolioLedger(stock_multiplier=1, fut_margin_ratio=0.5) # Default stock_multiplier=1 (shares), auto-detect later
        self.ledger.cash_total = initial_cash # Set initial cash
        self.initial_cash = initial_cash # Store initial cash for fallback

    def print_portfolio(self, tag: str):
        """
        Print positions/cash/pending (best-effort).
        Called on submit/fill/cancel/repair.
        """
        print(self.ledger.snapshot(tag))

    def _detect_stock_lot_unit(self) -> int:
        """
        Auto-detect whether stock position quantity is in:
          - shares  (lot = 1)
          - lots    (lot = 1000)

        Returns: lot unit to use in money calculation.
        """
        try:
            if not self.execution.stock_account:
                print("[LOT] No stock account, default lot=1000")
                return 1000

            pos = self.execution.api.list_positions(self.execution.stock_account)
            if not pos:
                print("[LOT] No stock positions, default lot=1000")
                return 1000

            # take first position for detection
            p = pos[0]
            code = p.code
            qty = float(p.quantity)
            if qty == 0:
                print("[LOT] Qty 0, default lot=1000")
                return 1000
                
            # heuristic for price
            avgp = float(getattr(p, "price", 0.0) or getattr(p, "avg_price", 0.0) or getattr(p, "cost_price", 0.0))

            snap = self.market_data.get_stock(code)
            if not snap:
                print(f"[LOT] No market snapshot for {code}, default lot=1000")
                return 1000

            mkt_price = snap.bid if snap.bid > 0 else snap.price
            if mkt_price <= 0:
                print(f"[LOT] Invalid market price {mkt_price}, default lot=1000")
                return 1000

            # two hypotheses
            v_share = qty * mkt_price
            v_lot   = qty * mkt_price * 1000

            # compare which one is closer to avg cost * real size
            # using avg price to avoid bid noise
            ref = qty * avgp
            err_share = abs(v_share - ref)
            err_lot   = abs(v_lot   - ref)

            if err_share < err_lot:
                print(f"[LOT] Detected: quantity is in SHARES -> lot=1 (qty={qty}, price={mkt_price}, avg={avgp})")
                return 1
            else:
                print(f"[LOT] Detected: quantity is in LOTS -> lot=1000 (qty={qty}, price={mkt_price}, avg={avgp})")
                return 1000

        except Exception as e:
            print(f"[LOT] Detection failed ({e}), fallback lot=1000")
            return 1000

    def start(self):
        if not self.execution.login():
            return False

        # reporter init (after accounts ready)
        self.reporter = AccountReporter(self.execution.api, self.market_data, self.execution.stock_account, self.execution.futopt_account)

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

        # ===== auto-detect stock lot unit =====
        # We need updated market data, so we wait briefly or trigger update above
        print("[TxMgr] Waiting 2s for market data to stabilize for Lot Detection...")
        time.sleep(2.0)
        
        detected_lot = self._detect_stock_lot_unit()
        self.ledger.stock_multiplier = detected_lot
        print(f"[TxMgr] Applied Stock Multiplier: {self.ledger.stock_multiplier}")

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
        self._pair_map.clear()
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
        affected_pairs = self._pair_map.get(code)
        if not affected_pairs:
            return

        for s, f in affected_pairs:
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

            if not self._check_position_safety(intent):
                return False

            print(f"[{time.strftime('%H:%M:%S')}] [Tx-{str(uuid.uuid4())[:8]}] Transaction Created: {intent}")
            # Print status before order as requested
            self.print_portfolio("PRE-ORDER")

            print(f"[TxMgr] ACCEPT: Starting new transaction for {intent.stock_code} ({intent.details})")

            # ===== IMPORTANT: use SIMULTANEOUS transactions =====
            if intent.type == SignalType.OPEN:
                tx = SimultaneousOpenTransaction(intent, self)
                # Estimate cost for reservation
                # Stock Cost + Future Margin
                # We need prices. Use market snapshot.
                stk_snap = self.market_data.get_stock(intent.stock_code)
                fut_snap = self.market_data.get_future(intent.future_code)
                if stk_snap and fut_snap:
                    # OPEN: Buy Stock, Sell Future
                    est_price = stk_snap.ask if stk_snap.ask > 0 else stk_snap.price
                    req_cash = est_price * intent.qty * self.ledger.stock_multiplier
                    
                    est_fut = fut_snap.bid if fut_snap.bid > 0 else fut_snap.price
                    req_margin = est_fut * intent.qty * self.ledger.fut_multiplier_default * self.ledger.fut_margin_ratio
                    
                    self.ledger.reserved_stock += req_cash
                    self.ledger.reserved_margin += req_margin
                    tx.reserved_cash = req_cash
                    tx.reserved_margin = req_margin
                    
            elif intent.type == SignalType.CLOSE:
                tx = SimultaneousCloseTransaction(intent, self)
                # CLOSE: Sell Stock, Buy Future. Usually releases funds, but we might verify margin safety?
                # Arbitrage Close releases margin and stock cost.
                # However, pending order might need margin check? 
                # For Close, we generally assume we hold the position.
                pass
            else:
                tx = BaseTransaction(intent, self)

            self.active_transactions[intent.stock_code] = tx
            self.print_portfolio("ACCEPT(tx)")
            return True

    def _release_reservation(self, tx):
        """Releases reserved funds when tx is done/failed."""
        reserved_c = getattr(tx, "reserved_cash", 0.0)
        reserved_m = getattr(tx, "reserved_margin", 0.0)
        if reserved_c > 0:
            self.ledger.reserved_stock = max(0.0, self.ledger.reserved_stock - reserved_c)
        if reserved_m > 0:
            self.ledger.reserved_margin = max(0.0, self.ledger.reserved_margin - reserved_m)
        
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
            # Only print if significantly changed or first run? Let's just print to confirm it's working.
            # print(f"[TxMgr] Balance Sync FAILED/SIM. Using Fallback: Init({self.initial_cash:,.0f}) - Hold({s_cost:,.0f}) - Mrg({f_margin:,.0f}) = {estimated:,.0f}")

    def archive_transaction(self, tx: BaseTransaction):
        with self._lock:
            if tx.intent.stock_code in self.active_transactions:
                del self.active_transactions[tx.intent.stock_code]
        
        self._release_reservation(tx)
        
        self.completed_transactions.append(tx)
        # keep last 50
        if len(self.completed_transactions) > 50:
            self.completed_transactions.pop(0)
        
        print(f"[TxMgr] Archiving Tx {tx.tx_id}")
        
        # Apply cooldown if needed (e.g. after close or fail)
        if tx.state == TransactionState.FAILED or tx.intent.type == SignalType.CLOSE:
            # simple cooldown
            print(f"[TxMgr] Apply Cooldown (60s) for {tx.intent.stock_code}")
            self.cooldowns[tx.intent.stock_code] = time.time() + 60.0

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
                clean_up_list.append(tx)

        if clean_up_list:
            for tx in clean_up_list:
                self.archive_transaction(tx)

    def repair_positions(self):
        print("\n[TxMgr] === REPAIRING POSITIONS (Aggressive) ===")
        self.print_portfolio("REPAIR(start)")
        try:
            if self.execution.stock_account:
                self.execution.api.update_status(self.execution.stock_account)
            if self.execution.futopt_account:
                self.execution.api.update_status(self.execution.futopt_account)
        except Exception:
            pass

        stk_map = {}
        if self.execution.stock_account:
            try:
                for p in self.execution.api.list_positions(self.execution.stock_account):
                    stk_map[p.code] = int(p.quantity)
            except Exception:
                pass

        fut_map = {}
        if self.execution.futopt_account:
            try:
                for p in self.execution.api.list_positions(self.execution.futopt_account):
                    qty = int(p.quantity)
                    net = qty if p.direction == sj_constant.Action.Buy else -qty
                    fut_map[p.code] = net
            except Exception:
                pass

        for s, f in self.monitored_pairs:
            s_qty = stk_map.get(s, 0)
            f_net = fut_map.get(f, 0)

            if f_net > 0:
                print(f"[Repair] {f} is Net Long ({f_net}). Closing (Sell)...")
                self.execution.place_future_order(f, "Sell", 0, abs(f_net), "MKT", "IOC")

            elif f_net < 0:
                if s_qty == 0:
                    print(f"[Repair] {f} is Naked Short ({f_net}). Closing (Buy)...")
                    self.execution.place_future_order(f, "Buy", 0, abs(f_net), "MKT", "IOC")

            if s_qty > 0 and f_net == 0:
                print(f"[Repair] {s} is Naked Stock ({s_qty}). Closing (Sell)...")
                self.execution.place_stock_order(s, "Sell", 0, s_qty, "MKT", "ROD")

        print("[Repair] Scan complete.\n")
        self.print_portfolio("REPAIR(done)")

    def repair_flatten_unpaired_futures(self, dry_run: bool = True, cooldown_sec: int = 30):
        print(f"\n[TxMgr] Fixing Unpaired Futures... (dry_run={dry_run})")
        self.print_portfolio("REPAIR(flatten_start)")

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
                self.held_positions.add(s)
            elif s in self.held_positions:
                self.held_positions.remove(s)

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

            if dry_run:
                continue

            self.execution.place_future_order(f, action, 0, closing_qty, "MKT", "IOC")

        print("[Repair] Done.\n")
        self.print_portfolio("REPAIR(flatten_done)")

    def on_execution_event(self, oid, seqno, op_code, op_msg, status_data):
        found_active = False

        with self._lock:
            active_txs = list(self.active_transactions.values())

        for tx in active_txs:
            if tx.stock_order.seqno and tx.stock_order.seqno == seqno:
                if status_data.get("deal_quantity", 0):
                    self.held_positions.add(tx.intent.stock_code)

                tx.on_order_update(seqno, op_code, msg=op_msg, status_data=status_data)
                found_active = True
                break

            if tx.future_order.seqno and tx.future_order.seqno == seqno:
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
                    print("[TxMgr] This causes a Naked Position. Triggering Auto-Repair...")
                    self.repair_positions()
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
                mgr.sync_balance()
                mgr.print_portfolio(f"LOOP({loops})")
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
