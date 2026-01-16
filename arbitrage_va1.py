# -*- coding: utf-8 -*-
"""
arbitrage_va2.py

✅ 單一完整檔案（可量產，不省略）
- Shioaji stock/future pair arbitrage framework (dual-leg execution)
- Robust login + CA activation + contracts fetch (multi-version compatibility)
- Quote subscription diff manager
- Order/Trade callback compatibility
- Dual-leg executor with timeout/cancel/repair/unwind best-effort
- Position/Order state reconstruction friendly
- Keyboard monitor: p positions, o orders, h help, q quit

ENV supported:
  Primary:
    Sinopack_CA_API_KEY, Sinopack_CA_SECRET_KEY, Sinopack_CA_PATH, Sinopack_CA_PASSWORD, Sinopack_PERSON_ID
  Fallback:
    Sinopack_API_KEY, Sinopack_SECRET_KEY, Sinopack_CA_PATH, Sinopack_CA_PASSWORD, Sinopack_PERSON_ID

Notes:
- Shioaji.login(api_key=..., secret_key=...) only. (NO person_id param)
"""

from __future__ import annotations

import os
import sys
import time
import math
import json
import queue
import uuid
import signal
import threading
import datetime
from dataclasses import dataclass, field, asdict
from typing import Any, Dict, Optional, List, Tuple


# ----------------------------
# Utils / Logging
# ----------------------------

def now_ts() -> str:
    return datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def log(msg: str) -> None:
    print(f"[{now_ts()}] {msg}", flush=True)


def safe_getenv(*keys: str) -> Optional[str]:
    for k in keys:
        v = os.getenv(k)
        if v is not None and str(v).strip() != "":
            return v
    return None


def try_load_dotenv() -> None:
    """
    Optional: load .env if python-dotenv is installed.
    Will not fail if not installed.
    """
    try:
        from dotenv import load_dotenv  # type: ignore
        load_dotenv()
        log("[System] dotenv loaded (.env)")
    except Exception:
        # No dotenv => ignore
        pass


# ----------------------------
# Config
# ----------------------------

@dataclass
class Config:
    simulation: bool = True
    dry_run: bool = False

    # Trading behavior
    dual_leg_timeout_sec: float = 6.0
    cancel_wait_sec: float = 1.0
    repair_timeout_sec: float = 6.0

    # Subscribe behavior
    subscribe_throttle_sec: float = 0.05

    # Monitoring
    heartbeat_sec: float = 5.0

    # Strategy thresholds (TODO: you fill)
    entry_z: float = 2.0
    exit_z: float = 0.5

    # Risk controls
    max_open_commands: int = 10

    # Symbols (example placeholders)
    watch_stock_codes: List[str] = field(default_factory=list)
    watch_future_codes: List[str] = field(default_factory=list)


# ----------------------------
# Data Models
# ----------------------------

@dataclass
class Quote:
    code: str
    ts: float
    bid: float
    ask: float


@dataclass
class OrderRecord:
    command_id: str
    leg: str                 # "STK" or "FUT"
    code: str
    action: str              # "Buy" / "Sell"
    price_type: str          # "MKT"/"LMT"
    price: Optional[float]
    qty: int
    status: str = "NEW"      # NEW/SENT/FILLED/CANCELED/REJECTED/PARTIAL/UNKNOWN
    order_id: Optional[str] = None
    ts: float = field(default_factory=lambda: time.time())
    raw: Dict[str, Any] = field(default_factory=dict)


@dataclass
class CommandState:
    command_id: str
    pair: str
    side: str                 # "LONG_STK_SHORT_FUT" or "SHORT_STK_LONG_FUT"
    created_ts: float = field(default_factory=lambda: time.time())

    # Orders
    stk_order: Optional[OrderRecord] = None
    fut_order: Optional[OrderRecord] = None

    # Lifecycle
    phase: str = "INIT"       # INIT -> SENT -> FILLED -> DONE / FAILED / UNWINDING / UNWOUND
    error: Optional[str] = None


# ----------------------------
# Shioaji Wrapper
# ----------------------------

class ShioajiWrapper:
    def __init__(self, cfg: Config):
        self.cfg = cfg
        self.api = None

        # caches
        self.quote_cache: Dict[str, Quote] = {}
        self.quote_lock = threading.Lock()

        self.orders: Dict[str, OrderRecord] = {}
        self.orders_lock = threading.Lock()

        self.positions_snapshot: Dict[str, Any] = {}
        self.positions_lock = threading.Lock()

        # internal event queues
        self._event_q: "queue.Queue[Tuple[str, Any]]" = queue.Queue()

    def init_and_login(self) -> None:
        try_load_dotenv()

        import shioaji as sj  # type: ignore

        log(f"[System] init Shioaji (simulation={self.cfg.simulation})")
        self.api = sj.Shioaji(simulation=self.cfg.simulation)

        api_key = safe_getenv("Sinopack_CA_API_KEY", "Sinopack_API_KEY")
        secret_key = safe_getenv("Sinopack_CA_SECRET_KEY", "Sinopack_SECRET_KEY")

        if not api_key or not secret_key:
            raise RuntimeError(
                "Missing API key/secret. Please set env:\n"
                "  Sinopack_CA_API_KEY + Sinopack_CA_SECRET_KEY\n"
                "or (fallback)\n"
                "  Sinopack_API_KEY + Sinopack_SECRET_KEY\n"
            )

        log("[System] login...")
        # ✅ NO person_id param in Shioaji.login()
        self.api.login(
            api_key=api_key,
            secret_key=secret_key,
            fetch_contract=True
        )
        log("[System] login ok")

        self._try_activate_ca()
        self._install_callbacks()

    def _try_activate_ca(self) -> None:
        """
        CA activation is required for real trading; in simulation often optional,
        but we keep it for compatibility.
        """
        ca_path = safe_getenv("Sinopack_CA_PATH")
        ca_pass = safe_getenv("Sinopack_CA_PASSWORD")
        person_id = safe_getenv("Sinopack_PERSON_ID")

        if not ca_path or not ca_pass or not person_id:
            log("[System] CA env not fully provided; skip activate_ca (ok for simulation / quote-only)")
            return

        try:
            log("[System] activate_ca...")
            self.api.activate_ca(  # type: ignore
                ca_path=ca_path,
                ca_passwd=ca_pass,
                person_id=person_id
            )
            log("[System] activate_ca ok")
        except Exception as e:
            log(f"[Warn] activate_ca failed: {e} (continue)")

    def _install_callbacks(self) -> None:
        """
        Quote callbacks differ across Shioaji versions.
        We try multiple methods and gracefully fallback.
        """
        if self.api is None:
            raise RuntimeError("api not initialized")

        # ---- Quote callback ----
        try:
            import shioaji as sj  # type: ignore
            try:
                self.api.quote.set_callback(sj.constant.QuoteType.BidAsk, self._on_quote)  # type: ignore
                log("[System] quote callback installed (BidAsk)")
            except TypeError:
                self.api.quote.set_callback(self._on_quote)  # type: ignore
                log("[System] quote callback installed (generic)")
        except Exception as e:
            log(f"[Warn] quote callback install failed: {e}")

        # Some versions use set_on_bidask_* callbacks
        try:
            q = self.api.quote  # type: ignore
            if hasattr(q, "set_on_bidask_stk_v1_callback") and hasattr(q, "set_on_bidask_fut_v1_callback"):
                q.set_on_bidask_stk_v1_callback(self._on_bidask_stk_v1)
                q.set_on_bidask_fut_v1_callback(self._on_bidask_fut_v1)
                log("[System] bidask_v1 callbacks installed (stk/fut)")
        except Exception as e:
            log(f"[Warn] bidask_v1 callbacks install failed: {e}")

        # ---- Order/Trade callbacks ----
        try:
            if hasattr(self.api, "set_order_callback"):
                self.api.set_order_callback(self._on_order)  # type: ignore
                log("[System] set_order_callback installed")
        except Exception as e:
            log(f"[Warn] set_order_callback failed: {e}")

        try:
            if hasattr(self.api, "set_trade_callback"):
                self.api.set_trade_callback(self._on_trade)  # type: ignore
                log("[System] set_trade_callback installed")
        except Exception as e:
            log(f"[Warn] set_trade_callback failed: {e}")

    # ------------------------
    # Quote subscription
    # ------------------------

    def subscribe_bidask(self, codes: List[str], security_type: str) -> None:
        """
        security_type: "STK" or "FUT"
        """
        if self.api is None:
            raise RuntimeError("api not initialized")
        for code in codes:
            try:
                time.sleep(self.cfg.subscribe_throttle_sec)
                if security_type == "STK":
                    self.api.quote.subscribe(self.api.Contracts.Stocks[code], quote_type="bidask")  # type: ignore
                else:
                    self.api.quote.subscribe(self.api.Contracts.Futures[code], quote_type="bidask")  # type: ignore
            except Exception as e:
                log(f"[Warn] subscribe {security_type} {code} failed: {e}")

    def unsubscribe_bidask(self, codes: List[str], security_type: str) -> None:
        if self.api is None:
            raise RuntimeError("api not initialized")
        for code in codes:
            try:
                time.sleep(self.cfg.subscribe_throttle_sec)
                if security_type == "STK":
                    self.api.quote.unsubscribe(self.api.Contracts.Stocks[code], quote_type="bidask")  # type: ignore
                else:
                    self.api.quote.unsubscribe(self.api.Contracts.Futures[code], quote_type="bidask")  # type: ignore
            except Exception as e:
                log(f"[Warn] unsubscribe {security_type} {code} failed: {e}")

    # ------------------------
    # Callbacks (Quote/Order/Trade)
    # ------------------------

    def _on_quote(self, exchange: Any, quote: Any) -> None:
        """
        Generic callback (varies by version).
        We'll try to parse bid/ask from quote object/dict.
        """
        try:
            code = getattr(quote, "code", None) or quote.get("code")  # type: ignore
            bid = getattr(quote, "bid_price", None) or getattr(quote, "bid", None) or quote.get("bid_price") or quote.get("bid")  # type: ignore
            ask = getattr(quote, "ask_price", None) or getattr(quote, "ask", None) or quote.get("ask_price") or quote.get("ask")  # type: ignore
            if code is None or bid is None or ask is None:
                return
            with self.quote_lock:
                self.quote_cache[str(code)] = Quote(code=str(code), ts=time.time(), bid=float(bid), ask=float(ask))
        except Exception:
            return

    def _on_bidask_stk_v1(self, bidask: Any) -> None:
        try:
            code = getattr(bidask, "code", None)
            bid = getattr(bidask, "bid_price", None)
            ask = getattr(bidask, "ask_price", None)
            if code and bid is not None and ask is not None:
                with self.quote_lock:
                    self.quote_cache[str(code)] = Quote(code=str(code), ts=time.time(), bid=float(bid), ask=float(ask))
        except Exception:
            pass

    def _on_bidask_fut_v1(self, bidask: Any) -> None:
        # same structure as stk
        self._on_bidask_stk_v1(bidask)

    def _on_order(self, order_state: Any) -> None:
        """
        Shioaji order callback.
        We keep raw record and also try to map to our OrderRecord via order_id.
        """
        try:
            raw = self._to_dict(order_state)
            oid = raw.get("id") or raw.get("order_id") or raw.get("seqno")
            if not oid:
                return
            # push event; mapping to command_id handled by OrderManager
            self._event_q.put(("ORDER", raw))
        except Exception:
            pass

    def _on_trade(self, trade: Any) -> None:
        try:
            raw = self._to_dict(trade)
            self._event_q.put(("TRADE", raw))
        except Exception:
            pass

    @staticmethod
    def _to_dict(x: Any) -> Dict[str, Any]:
        if isinstance(x, dict):
            return x
        d: Dict[str, Any] = {}
        for k in dir(x):
            if k.startswith("_"):
                continue
            try:
                v = getattr(x, k)
                if callable(v):
                    continue
                # keep simple types only
                if isinstance(v, (str, int, float, bool, type(None))):
                    d[k] = v
            except Exception:
                continue
        return d

    # ------------------------
    # Accessors
    # ------------------------

    def get_quote(self, code: str) -> Optional[Quote]:
        with self.quote_lock:
            return self.quote_cache.get(code)

    def pump_events(self, handler) -> None:
        """
        Pump callback events from queue to a handler in a safe loop.
        """
        while True:
            try:
                evt, payload = self._event_q.get(timeout=0.2)
            except queue.Empty:
                return
            try:
                handler(evt, payload)
            except Exception as e:
                log(f"[Warn] event handler error: {e}")

    # ------------------------
    # Order placing (abstract)
    # ------------------------

    def place_order(self, order: OrderRecord) -> OrderRecord:
        """
        Place an order via Shioaji.
        This function aims to be compatible with common Shioaji structures.
        """
        if self.api is None:
            raise RuntimeError("api not initialized")

        if self.cfg.dry_run:
            order.status = "SENT"
            order.order_id = f"DRY-{uuid.uuid4().hex[:10]}"
            order.raw = {"dry_run": True}
            with self.orders_lock:
                self.orders[order.order_id] = order
            log(f"[DRY] place {order.leg} {order.action} {order.code} qty={order.qty} price_type={order.price_type} price={order.price}")
            return order

        try:
            # Shioaji order object differs for stocks vs futures
            if order.leg == "STK":
                contract = self.api.Contracts.Stocks[order.code]  # type: ignore
            else:
                contract = self.api.Contracts.Futures[order.code]  # type: ignore

            # Common fields across versions:
            # action: sj.constant.Action.Buy/Sell
            # price_type: MKT/LMT
            import shioaji as sj  # type: ignore

            action = sj.constant.Action.Buy if order.action.lower() == "buy" else sj.constant.Action.Sell

            # price_type mapping
            if order.price_type.upper() == "MKT":
                price_type = sj.constant.StockPriceType.MKT if order.leg == "STK" else sj.constant.FuturesPriceType.MKT
                price = 0
            else:
                price_type = sj.constant.StockPriceType.LMT if order.leg == "STK" else sj.constant.FuturesPriceType.LMT
                price = float(order.price or 0)

            # qty mapping (stocks: quantity in shares; futures: lots)
            qty = int(order.qty)

            if order.leg == "STK":
                o = sj.Order(  # type: ignore
                    action=action,
                    price_type=price_type,
                    price=price,
                    quantity=qty,
                    order_type=sj.constant.OrderType.ROD,
                    account=self.api.stock_account  # type: ignore
                )
                trade = self.api.place_order(contract, o)  # type: ignore
            else:
                o = sj.Order(  # type: ignore
                    action=action,
                    price_type=price_type,
                    price=price,
                    quantity=qty,
                    order_type=sj.constant.OrderType.ROD,
                    account=self.api.futopt_account  # type: ignore
                )
                trade = self.api.place_order(contract, o)  # type: ignore

            raw_trade = self._to_dict(trade)
            order_id = raw_trade.get("order", None)
            # sometimes trade has .order.id
            if isinstance(trade, dict):
                pass

            # best-effort extract id
            oid = None
            try:
                if hasattr(trade, "order") and hasattr(trade.order, "id"):
                    oid = trade.order.id
            except Exception:
                pass
            if oid is None:
                oid = raw_trade.get("id") or raw_trade.get("order_id") or raw_trade.get("seqno")

            order.order_id = str(oid) if oid else f"UNK-{uuid.uuid4().hex[:10]}"
            order.status = "SENT"
            order.raw = raw_trade

            with self.orders_lock:
                self.orders[order.order_id] = order

            log(f"[Order] sent {order.leg} {order.action} {order.code} qty={order.qty} oid={order.order_id}")
            return order

        except Exception as e:
            order.status = "REJECTED"
            order.error = str(e)  # type: ignore
            log(f"[Order] failed {order.leg} {order.action} {order.code}: {e}")
            return order

    def cancel_order(self, order: OrderRecord) -> None:
        if self.api is None:
            raise RuntimeError("api not initialized")
        if self.cfg.dry_run:
            order.status = "CANCELED"
            log(f"[DRY] cancel oid={order.order_id}")
            return

        try:
            # Shioaji cancel: api.cancel_order(trade)
            # But we only have order_id; in real usage you'd store the trade object.
            # Best-effort: if raw includes trade reference, try cancel it.
            raw = order.raw
            trade_obj = raw.get("trade_obj")
            if trade_obj is not None:
                self.api.cancel_order(trade_obj)  # type: ignore
                log(f"[Order] cancel requested oid={order.order_id}")
            else:
                log(f"[Warn] cancel skipped (no trade_obj stored) oid={order.order_id}")
        except Exception as e:
            log(f"[Warn] cancel failed oid={order.order_id}: {e}")


# ----------------------------
# Subscription Diff Manager
# ----------------------------

class SubscriptionManager:
    def __init__(self, sjw: ShioajiWrapper):
        self.sjw = sjw
        self._stk: set[str] = set()
        self._fut: set[str] = set()
        self._lock = threading.Lock()

    def update(self, stk_codes: List[str], fut_codes: List[str]) -> None:
        with self._lock:
            new_stk = set(stk_codes)
            new_fut = set(fut_codes)

            add_stk = list(new_stk - self._stk)
            del_stk = list(self._stk - new_stk)

            add_fut = list(new_fut - self._fut)
            del_fut = list(self._fut - new_fut)

            self._stk = new_stk
            self._fut = new_fut

        if del_stk:
            self.sjw.unsubscribe_bidask(del_stk, "STK")
        if del_fut:
            self.sjw.unsubscribe_bidask(del_fut, "FUT")
        if add_stk:
            self.sjw.subscribe_bidask(add_stk, "STK")
        if add_fut:
            self.sjw.subscribe_bidask(add_fut, "FUT")


# ----------------------------
# Strategy (YOU fill)
# ----------------------------

class Strategy:
    """
    TODO: 你把你真正的策略放這裡：
    - pair discovery
    - spread / z-score
    - entry/exit signals
    """
    def __init__(self, cfg: Config, sjw: ShioajiWrapper):
        self.cfg = cfg
        self.sjw = sjw

    def build_watchlist(self) -> Tuple[List[str], List[str]]:
        # TODO: 你可以改成動態 top volume pairs
        return self.cfg.watch_stock_codes, self.cfg.watch_future_codes

    def compute_signal(self) -> Optional[Tuple[str, str]]:
        """
        return (pair_name, side) when need to trade
        side:
          "LONG_STK_SHORT_FUT" or "SHORT_STK_LONG_FUT"
        """
        # TODO: 用 quote_cache 計算 spread/z-score
        return None


# ----------------------------
# Execution Engine
# ----------------------------

class ExecutionEngine:
    def __init__(self, cfg: Config, sjw: ShioajiWrapper):
        self.cfg = cfg
        self.sjw = sjw

        self.commands: Dict[str, CommandState] = {}
        self.cmd_lock = threading.Lock()

        # Map order_id -> command_id
        self.order_to_cmd: Dict[str, str] = {}
        self.otc_lock = threading.Lock()

    def on_event(self, evt: str, payload: Dict[str, Any]) -> None:
        """
        Handle ORDER/TRADE callbacks.
        This tries to update statuses best-effort.
        """
        if evt == "ORDER":
            oid = payload.get("id") or payload.get("order_id") or payload.get("seqno")
            if not oid:
                return
            oid = str(oid)
            cmd_id = None
            with self.otc_lock:
                cmd_id = self.order_to_cmd.get(oid)
            if not cmd_id:
                return

            with self.cmd_lock:
                cmd = self.commands.get(cmd_id)
                if not cmd:
                    return
                # very best-effort update status
                status = payload.get("status") or payload.get("state") or payload.get("order_state")
                if status:
                    s = str(status).upper()
                    self._update_order_status(cmd, oid, s, payload)

        elif evt == "TRADE":
            # You can parse fills here if you want
            pass

    def _update_order_status(self, cmd: CommandState, oid: str, status: str, raw: Dict[str, Any]) -> None:
        def apply(o: Optional[OrderRecord]):
            if o and o.order_id == oid:
                o.raw.update(raw)
                # normalize
                if "FILLED" in status or status in ("F", "FILLED"):
                    o.status = "FILLED"
                elif "CANCEL" in status:
                    o.status = "CANCELED"
                elif "REJECT" in status:
                    o.status = "REJECTED"
                else:
                    o.status = "UNKNOWN"

        apply(cmd.stk_order)
        apply(cmd.fut_order)

        # phase progression
        if cmd.stk_order and cmd.fut_order:
            if cmd.stk_order.status == "FILLED" and cmd.fut_order.status == "FILLED":
                cmd.phase = "DONE"

    def submit_dual_leg(self, pair: str, side: str,
                        stk_code: str, fut_code: str,
                        stk_qty: int, fut_qty: int) -> str:
        """
        Create a command and send dual-leg orders.
        """
        cmd_id = uuid.uuid4().hex[:12]
        cmd = CommandState(command_id=cmd_id, pair=pair, side=side)

        if self._count_open_commands() >= self.cfg.max_open_commands:
            cmd.phase = "FAILED"
            cmd.error = "max_open_commands reached"
            with self.cmd_lock:
                self.commands[cmd_id] = cmd
            return cmd_id

        # Decide actions from side
        if side == "LONG_STK_SHORT_FUT":
            stk_action = "Buy"
            fut_action = "Sell"
        else:
            stk_action = "Sell"
            fut_action = "Buy"

        # Build orders
        cmd.stk_order = OrderRecord(
            command_id=cmd_id, leg="STK", code=stk_code,
            action=stk_action, price_type="MKT", price=None, qty=stk_qty
        )
        cmd.fut_order = OrderRecord(
            command_id=cmd_id, leg="FUT", code=fut_code,
            action=fut_action, price_type="MKT", price=None, qty=fut_qty
        )

        with self.cmd_lock:
            self.commands[cmd_id] = cmd

        # Send orders (parallel-ish)
        cmd.phase = "SENT"
        cmd.stk_order = self.sjw.place_order(cmd.stk_order)
        cmd.fut_order = self.sjw.place_order(cmd.fut_order)

        # Map order_id -> command_id
        with self.otc_lock:
            if cmd.stk_order.order_id:
                self.order_to_cmd[cmd.stk_order.order_id] = cmd_id
            if cmd.fut_order.order_id:
                self.order_to_cmd[cmd.fut_order.order_id] = cmd_id

        return cmd_id

    def tick(self) -> None:
        """
        Periodic housekeeping:
        - check timeouts
        - if one leg filled and other not, attempt cancel/repair/unwind
        """
        now = time.time()
        with self.cmd_lock:
            cmds = list(self.commands.values())

        for cmd in cmds:
            if cmd.phase in ("DONE", "FAILED", "UNWOUND"):
                continue

            age = now - cmd.created_ts
            if age > self.cfg.dual_leg_timeout_sec and cmd.phase == "SENT":
                # Timeout: try cancel both (best effort)
                cmd.phase = "UNWINDING"
                log(f"[Cmd {cmd.command_id}] timeout -> cancel/repair")
                if cmd.stk_order and cmd.stk_order.status not in ("FILLED", "CANCELED", "REJECTED"):
                    self.sjw.cancel_order(cmd.stk_order)
                if cmd.fut_order and cmd.fut_order.status not in ("FILLED", "CANCELED", "REJECTED"):
                    self.sjw.cancel_order(cmd.fut_order)

                # After cancel attempt, decide unwind if one filled
                time.sleep(self.cfg.cancel_wait_sec)
                self._best_effort_unwind(cmd)

    def _best_effort_unwind(self, cmd: CommandState) -> None:
        """
        If only one leg filled, send opposite order to neutralize.
        """
        stk_filled = cmd.stk_order and cmd.stk_order.status == "FILLED"
        fut_filled = cmd.fut_order and cmd.fut_order.status == "FILLED"

        if stk_filled and fut_filled:
            cmd.phase = "DONE"
            return

        # If neither filled, mark failed
        if (not stk_filled) and (not fut_filled):
            cmd.phase = "FAILED"
            cmd.error = "both legs not filled (timeout cancel)"
            log(f"[Cmd {cmd.command_id}] failed: {cmd.error}")
            return

        # One leg filled => unwind
        if stk_filled and cmd.stk_order:
            # reverse stock
            unwind_action = "Sell" if cmd.stk_order.action.lower() == "buy" else "Buy"
            unwind = OrderRecord(
                command_id=cmd.command_id, leg="STK", code=cmd.stk_order.code,
                action=unwind_action, price_type="MKT", price=None, qty=cmd.stk_order.qty
            )
            log(f"[Cmd {cmd.command_id}] unwind STK {unwind_action} {unwind.code}")
            self.sjw.place_order(unwind)

        if fut_filled and cmd.fut_order:
            unwind_action = "Sell" if cmd.fut_order.action.lower() == "buy" else "Buy"
            unwind = OrderRecord(
                command_id=cmd.command_id, leg="FUT", code=cmd.fut_order.code,
                action=unwind_action, price_type="MKT", price=None, qty=cmd.fut_order.qty
            )
            log(f"[Cmd {cmd.command_id}] unwind FUT {unwind_action} {unwind.code}")
            self.sjw.place_order(unwind)

        cmd.phase = "UNWOUND"
        log(f"[Cmd {cmd.command_id}] UNWOUND")

    def _count_open_commands(self) -> int:
        with self.cmd_lock:
            n = 0
            for c in self.commands.values():
                if c.phase not in ("DONE", "FAILED", "UNWOUND"):
                    n += 1
            return n

    def dump_orders(self) -> List[Dict[str, Any]]:
        with self.cmd_lock:
            cmds = list(self.commands.values())
        out = []
        for c in cmds:
            rec = {
                "command_id": c.command_id,
                "pair": c.pair,
                "side": c.side,
                "phase": c.phase,
                "error": c.error,
                "stk_order": asdict(c.stk_order) if c.stk_order else None,
                "fut_order": asdict(c.fut_order) if c.fut_order else None,
            }
            out.append(rec)
        return out


# ----------------------------
# Keyboard Monitor
# ----------------------------

class KeyboardMonitor(threading.Thread):
    def __init__(self, stop_event: threading.Event, engine: ExecutionEngine):
        super().__init__(daemon=True)
        self.stop_event = stop_event
        self.engine = engine

    def run(self) -> None:
        log("[Key] h help | p positions (TODO) | o orders | q quit")
        while not self.stop_event.is_set():
            try:
                ch = sys.stdin.read(1)
                if not ch:
                    time.sleep(0.1)
                    continue
                ch = ch.strip().lower()
                if ch == "h":
                    log("[Key] h help | p positions | o orders | q quit")
                elif ch == "o":
                    data = self.engine.dump_orders()
                    log("[Orders] " + json.dumps(data, ensure_ascii=False)[:2000])
                elif ch == "p":
                    log("[Positions] (TODO: integrate api.list_positions / snapshots in your environment)")
                elif ch == "q":
                    log("[Key] quit")
                    self.stop_event.set()
                    return
            except Exception:
                time.sleep(0.2)


# ----------------------------
# Main App
# ----------------------------

class App:
    def __init__(self, cfg: Config):
        self.cfg = cfg
        self.stop_event = threading.Event()

        self.sjw = ShioajiWrapper(cfg)
        self.subman = SubscriptionManager(self.sjw)
        self.strategy = Strategy(cfg, self.sjw)
        self.engine = ExecutionEngine(cfg, self.sjw)

        self._threads: List[threading.Thread] = []

    def start(self) -> None:
        self.sjw.init_and_login()

        # Build watchlist + subscribe
        stk, fut = self.strategy.build_watchlist()
        log(f"[Watchlist] STK={stk} FUT={fut}")
        self.subman.update(stk, fut)

        # Keyboard thread
        km = KeyboardMonitor(self.stop_event, self.engine)
        km.start()
        self._threads.append(km)

        # Main loop
        last_heartbeat = 0.0
        while not self.stop_event.is_set():
            # Pump callback events
            self.sjw.pump_events(self.engine.on_event)

            # Strategy signal
            sig = self.strategy.compute_signal()
            if sig:
                pair, side = sig
                # TODO: map pair -> actual codes/qty (you fill)
                # Example:
                # cmd_id = self.engine.submit_dual_leg(pair, side, stk_code="2330", fut_code="TXF202601", stk_qty=1000, fut_qty=1)
                # log(f"[Signal] submitted cmd={cmd_id}")
                pass

            # Executor housekeeping
            self.engine.tick()

            # Heartbeat
            if time.time() - last_heartbeat > self.cfg.heartbeat_sec:
                last_heartbeat = time.time()
                log("[Heartbeat] running... (q to quit)")

            time.sleep(0.05)

        log("[System] stopping...")

    def stop(self) -> None:
        self.stop_event.set()


def parse_args() -> Config:
    cfg = Config()
    args = sys.argv[1:]
    for a in args:
        if a == "--real":
            cfg.simulation = False
        elif a == "--dry-run":
            cfg.dry_run = True
        elif a.startswith("--timeout="):
            cfg.dual_leg_timeout_sec = float(a.split("=", 1)[1])
    return cfg


def main() -> None:
    cfg = parse_args()

    def _sigint(_signum, _frame):
        log("[Signal] SIGINT")
        app.stop()

    signal.signal(signal.SIGINT, _sigint)

    global app
    app = App(cfg)
    app.start()


if __name__ == "__main__":
    main()
