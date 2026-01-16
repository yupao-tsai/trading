# -*- coding: utf-8 -*-
"""
arbitrage_full.py

✅ 單一完整檔案（不省略）：
- Shioaji stock/future pair arbitrage framework
- Dynamic pair discovery by volume (top N)
- Diff-based subscriptions + linger + locked codes
- Robust PositionManager (state machine + inflight lock + order registry + raw order events)
- Execution with dual-leg + timeout + cancel + repair/unwind best effort
- AccountMonitor sync to detect UNHEDGED
- Keyboard monitor: p positions, o orders, h help, q quit

注意：
1) 這個檔案預設 SIMULATION=True
2) 你要放 .env 並包含：
   Sinopack_CA_API_KEY
   Sinopack_CA_SECRET_KEY
   Sinopack_PERSON_ID
   Sinopack_CA_PATH
   Sinopack_CA_PASSWORD
   Sinopack_PASSWORD   (正式環境你才需要)
"""

import math
import os
import sys
import logging
import json
import time
import ssl
# Bypass SSL verification for legacy macOS Python environments
ssl._create_default_https_context = ssl._create_unverified_context
import threading
import ssl

# Hack for macOS SSL certificate issues
try:
    _create_unverified_https_context = ssl._create_unverified_context
except AttributeError:
    pass
else:
    ssl._create_default_https_context = _create_unverified_https_context
import queue
import datetime
import threading
import concurrent.futures
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Tuple, Any, Set
from collections import Counter
from enum import Enum
from decimal import Decimal, ROUND_FLOOR, ROUND_CEILING
import inspect
import pandas as pd
import shioaji as sj
from dotenv import load_dotenv

try:
    load_dotenv()
except Exception:
    pass


# ---- SUPPRESS: Filter out unwanted TAIFEX BidAsk prints (C-level) ----

# =========================================================
# 🩹 Monkeypatch pysolace to fix IndexError in fetch_contracts
#    Cause: shioaji/pysolace C-extension buffer overflow with page_size=5000
#    Fix: Intercept request and reduce page_size to 500
# =========================================================
# =========================================================
# 🩹 Monkeypatch pysolace to fix IndexError in fetch_contracts
#    Cause: shioaji/pysolace C-extension buffer overflow with page_size=5000
#    Fix: 1. Intercept LOW-LEVEL request at _core level and reduce page_size to 100
#         2. Re-implement onreply_callback_wrap to SUPPRESS IndexError (loguru catches it otherwise)
# =========================================================
try:
    import pysolace
    import pysolace._core as _core_module
    
    # Check if we already patched
    if not getattr(_core_module, "_patched_by_bot", False):
        _original_core_request = _core_module.request

        def patched_core_request(*args):
            # Signature: request(sol_ptr, topic, corrid, payload, qos, cos, format)
            # We expect at least 4 args, with payload at index 3
            if len(args) >= 4:
                payload = args[3]
                if isinstance(payload, dict) and "page_size" in payload:
                    # Aggressively reduce to 100
                    if payload["page_size"] > 100:
                        # print(f"[Patch] 🩹 Core: Reducing page_size from {payload['page_size']} to 100.")
                        payload["page_size"] = 100
            return _original_core_request(*args)

        _core_module.request = patched_core_request
        _core_module._patched_by_bot = True
        pysolace.solclient.request = patched_core_request
        
        # ---------------------------------------------------------
        # Re-implement onreply_callback_wrap to bypass loguru catch
        # ---------------------------------------------------------
        def patched_onreply_callback_wrap(self, topic: str, corrid: str, reply: dict):
            try:
                # 1. Update response dict in map
                if corrid in self.req_rep_map:
                    # Update the dict in place so 'request' function sees changes
                    for k, v in reply.items():
                        self.req_rep_map[corrid][k] = v
                
                # 2. Signal completion event
                if corrid in self.rep_event_map:
                    recv = self.rep_event_map.pop(corrid)
                    recv.set()
                
                # 3. Cleanup map
                if corrid in self.req_rep_map:
                    self.req_rep_map.pop(corrid)
                
                # 4. Execute Callback (The source of IndexError)
                if corrid in self.rep_callback_map:
                    rep_cb = self.rep_callback_map.pop(corrid)
                    if rep_cb:
                        try:
                            # This calls api.SolaceAPI._fetch_contracts_cb
                            rep_cb(topic, corrid, reply)
                        except IndexError:
                            # Supress the specific crash
                            # print(f"[Patch] ⚠️ Suppressed IndexError for {corrid}. Batch skipped.")
                            pass
                        except Exception as e:
                            print(f"[Patch] ⚠️ Suppressed Exception for {corrid}: {e}")
                    else:
                        # Fallback to default if needed (mostly unused for fetch_contracts)
                        self.onreply_callback(topic, corrid, reply)
            except Exception as e:
                 print(f"[Patch] Critical error in patched_onreply: {e}")
            return 0

        pysolace.SolClient.onreply_callback_wrap = patched_onreply_callback_wrap
        
        print(f"[Patch] pysolace fully monkeypatched (Core Request + OnReply).")
    else:
        print(f"[Patch] pysolace already patched.")

except ImportError:
    print("[Patch] pysolace not found, skipping patch.")
except Exception as e:
    print(f"[Patch] Failed to patch pysolace: {e}")



class CLevelFilter:
    def __init__(self):
        self._stop = False
        self._thread = None
        self.original_stdout_fd = None
        self.original_stderr_fd = None
        self.pipe_r = None
        self.pipe_w = None

    def start(self):
        try:
            # Save original fds
            self.original_stdout_fd = os.dup(sys.stdout.fileno())
            self.original_stderr_fd = os.dup(sys.stderr.fileno())

            # Create pipe
            self.pipe_r, self.pipe_w = os.pipe()

            # Redirect stdout/stderr to pipe write end
            os.dup2(self.pipe_w, sys.stdout.fileno())
            os.dup2(self.pipe_w, sys.stderr.fileno())

            # Start reader thread
            self._thread = threading.Thread(target=self._reader, daemon=True)
            self._thread.start()
        except Exception as e:
            print(f"Failed to install C-level filter: {e}")

    def _reader(self):
        with os.fdopen(self.pipe_r, 'r', errors='replace') as reader:
            while not self._stop:
                try:
                    line = reader.readline()
                    if not line:
                        break
                    
                    # Filter logic
                    if "Exchange.TAIFEX" in line and "BidAsk(" in line:
                        continue
                    
                    # Write to original stdout
                    os.write(self.original_stdout_fd, line.encode('utf-8'))
                except Exception:
                    break

    def stop(self):
        self._stop = True
        # Restore fds
        if self.original_stdout_fd is not None:
            os.dup2(self.original_stdout_fd, sys.stdout.fileno())
            os.close(self.original_stdout_fd)
        if self.original_stderr_fd is not None:
            os.dup2(self.original_stderr_fd, sys.stderr.fileno())
            os.close(self.original_stderr_fd)
        
        # Close pipe write end to unblock reader
        if self.pipe_w is not None:
            try:
                os.close(self.pipe_w)
            except OSError:
                pass

# Install filter immediately
# _c_filter = CLevelFilter()
# _c_filter.start()

# ---------------------------------------------
# =========================
# --- 全域設定 ---
# =========================
SIMULATION = True
ORDER_HISTORY_FILE = "order_history.jsonl"  # 本地訂單記錄檔

# Risk & Stability (P0)
KILL_SWITCH = False          # 一鍵停止開倉 (只允許平倉/修復)
DEGRADE_MODE = False         # 降級模式 (只減倉不開倉，風控變嚴)
MAX_DATA_DELAY_SEC = 2.0     # 資料時間 vs 事件時間 最大容忍延遲
SIMULATE_CHAOS = False       # 混沌測試 (隨機掉單/部分成交 - 限 Simulation)

# Position sizing
STOCK_QTY = 2          # 現股張數（Common lot）
FUTURE_QTY = 1         # 期貨口數
MIN_ORDER_QTY_THRESHOLD = 3 # 最佳一檔掛單量門檻 (張/口)
MAX_CAPITAL = 10000000 # 最大總投資金額 (1000萬)
ENTRY_THRESHOLD = 100  # 進場淨利門檻（以金額）
EXIT_THRESHOLD = 0     # 出場價差門檻
MIN_TOP_VOLUME = 3     # 最佳一檔量門檻 (Stock Ask >= 3, Future Bid >= 3)

# 平衡換算（台灣：現股 1 張=1000 股；個股期貨多數 1 口=2000 股）
STOCK_SHARES_PER_LOT = 1000
FUTURE_SHARES_EQUIV = 2000  # ✅ 一口 2000 股（主流個股期）

# 交易成本估算（你可自行調整）
STOCK_FEE_RATE = 0.001425 * 0.3
STOCK_TAX_RATE = 0.003
FUT_FEE_PER_CONTRACT = 20
FUT_TAX_RATE = 0.00002

# Hedge/Repair timeout
HEDGE_TIMEOUT_SEC = 30.0
REPAIR_TIMEOUT_SEC = 5.0
MIN_PROFIT_TO_REPAIR = 0

# Tick protection
STOCK_BUY_TICKS = 5
STOCK_SELL_TICKS = 5
FUT_BUY_TICKS = 5
FUT_SELL_TICKS = 5
REPAIR_TICKS = 20       # Aggressive tick limit for repair
FORCE_CLOSE_TICKS = 20  # Reduced from 50 to avoid hitting limits

# Dynamic pair discovery
MIN_VOLUME_THRESHOLD = 500
VOLUME_HISTORY_FILE = "volume_history.json"
TOP_N_PAIRS = 30
REFRESH_PAIRS_EVERY_SEC = 600  # 10分鐘
MAX_SUBS = 80
UNSUB_LINGER_SEC = 60 # P1: 退訂延遲

# Debug watch
DBG_WATCH_STOCK = "6770" # Powerchip (High Volume)
DBG_WATCH_FUT: Optional[str] = None
DBG_LAST_LINE = None
DBG_LINE_LOCK = threading.Lock()

# =========================
# --- Strategy Parameters (Z-Score & Risk) ---
# =========================
EWMA_HALF_LIFE = 60.0    # 秒
Z_ENTRY = 1.0            # 進場 Z-score (Mean Reversion) - LOW for Testing
Z_EXIT = 0.5             # 出場 Z-score
MIN_PROFIT_Z = 2.0       # 最小期望獲利 (以 Sigma 倍數計)

MAX_HOLDING_SEC = 1800   # 最大持倉時間 (30分鐘)
UNHEDGED_REPAIR_DEADLINE = 5.0 # 5秒內嘗試補單 (Repair)
UNHEDGED_FORCE_CLOSE_SEC = 15.0  # 超過 15秒 強制清倉 (Unwind/Panic)

STD_FLOOR = 0.5          # Z-score 分母下限 (避免爆表)
MIN_SAMPLES = 5          # EWMA 暖機樣本數 - LOW for Testing
INFLIGHT_COOLDOWN_SEC = 2.0 # 交易完成後冷卻時間
WARMUP_SEC = 5.0         # 啟動後暖機時間 (P1)

SLIPPAGE_SCALE = 0.5     # 滑價估算係數 (Ticks per Ratio)

# Risk Control
MAX_OPEN_PAIRS = 5       # 最大同時持倉對數
MAX_DAILY_LOSS = -50000  # 當日最大虧損 (金額)
MAX_ORDERS_PER_MIN = 20  # Global throttle
MAX_DAILY_ORDERS = 300   # Global daily limit
USE_MARKET_ORDER = True  # ✅ Use Market Order for Entry (Reduce Legging Risk)

# Debug mode
DEBUG_MODE = True  # Debug mode: only allow one stock trading at a time
SIMPLE_LOG = True  # Simplified log output
MANAGE_EXISTING_POSITIONS = False # If False, only positions opened by this session will be managed (Closed/Repaired)

# Credentials (.env)
PERSON_ID = os.getenv("Sinopack_PERSON_ID")
PASSWORD = os.getenv("Sinopack_PASSWORD")
CA_API_KEY = os.getenv("Sinopack_CA_API_KEY")
CA_SECRET_KEY = os.getenv("Sinopack_CA_SECRET_KEY")
CA_PATH = os.getenv("Sinopack_CA_PATH")
CA_PASSWORD = os.getenv("Sinopack_CA_PASSWORD")


def _check_hedge_ratio():
    stock_shares = STOCK_QTY * STOCK_SHARES_PER_LOT
    future_shares = FUTURE_QTY * FUTURE_SHARES_EQUIV
    if stock_shares != future_shares:
        print(f"[⚠️ Hedge Ratio Warning] 股票={stock_shares}股 vs 期貨={future_shares}股，不是 1:1 對沖！")
    else:
        print(f"[Hedge Ratio OK] 股票={stock_shares}股 == 期貨={future_shares}股")
_check_hedge_ratio()

def _safe_activate_ca(api, ca_path: str, ca_passwd: str, person_id: str):
    fn = api.activate_ca
    sig = inspect.signature(fn)
    params = sig.parameters

    kwargs = {}

    # ca_path
    if "ca_path" in params:
        kwargs["ca_path"] = ca_path
    elif "path" in params:
        kwargs["path"] = ca_path

    # ca_passwd
    if "ca_passwd" in params:
        kwargs["ca_passwd"] = ca_passwd
    elif "ca_password" in params:
        kwargs["ca_password"] = ca_passwd

    # person_id / id
    if "person_id" in params:
        kwargs["person_id"] = person_id
    elif "id" in params:
        kwargs["id"] = person_id

    try:
        if kwargs:
            return fn(**kwargs)
        return fn(ca_path, ca_passwd, person_id)
    except TypeError:
        return fn(ca_path, ca_passwd, person_id)

# =========================
# --- Helper Classes ---
# =========================
class ApiThrottler:
    """
    Global throttler for frequent API calls (e.g., update_status).
    """
    def __init__(self, interval: float = 1.0):
        self.interval = interval
        self.last_ts = 0.0
        self.lock = threading.Lock()

    def call(self, func, *args, **kwargs):
        with self.lock:
            now = time.time()
            if now - self.last_ts < self.interval:
                return None
            self.last_ts = now
        try:
            return func(*args, **kwargs)
        except Exception:
            return None


# =========================
# --- Quote safe access ---
# =========================
def _first(x, default=0.0):
    try:
        if x is None:
            return default
        if isinstance(x, (list, tuple)):
            return float(x[0]) if len(x) > 0 else default
        return float(x)
    except Exception:
        return default

def _get_code(q) -> str:
    return getattr(q, "code", "") or ""

def normalize_status(x) -> str:
    """
    將 Shioaji 各種可能的 status 表示法（Enum/object/str/dict）
    統一成 canonical uppercase token.
    """
    if x is None:
        return ""

    if isinstance(x, dict):
        x = x.get("status") or x.get("Status") or ""

    if hasattr(x, "name"):
        s = str(getattr(x, "name"))
    elif hasattr(x, "value"):
        s = str(getattr(x, "value"))
    else:
        s = str(x)

    s = s.strip()
    if not s:
        return ""

    s = s.replace("Status.", "").replace("status.", "")
    u = s.upper()

    if u in ("FILLING", "PARTFILLED", "PART_FILLED", "PARTFILL", "PARTIALFILLED", "PARTIAL_FILLED"):
        return "PARTFILLED"
    if u in ("FILLED",):
        return "FILLED"
    if u in ("CANCELLED", "CANCELED", "USERCANCELED", "USER_CANCELLED", "SYSTEMCANCELED", "SYSTEM_CANCELLED"):
        return "CANCELLED"
    if u in ("FAILED", "REJECTED", "ERROR"):
        return "FAILED"
    if u in ("SUBMITTED", "PENDINGSUBMIT", "PENDING_SUBMIT", "NEW"):
        return "SUBMITTED"

    return u

def _get_bid(q) -> float:
    return _first(getattr(q, "bid_price", None), 0.0)

def _get_ask(q) -> float:
    return _first(getattr(q, "ask_price", None), 0.0)

def _get_bid_vol(q) -> int:
    return int(_first(getattr(q, "bid_volume", None), 0))

def _get_ask_vol(q) -> int:
    return int(_first(getattr(q, "ask_volume", None), 0))

def _get_ts(q) -> float:
    t = getattr(q, "datetime", None)
    if isinstance(t, datetime.datetime):
        return t.timestamp()
    t2 = getattr(q, "ts", None)
    if isinstance(t2, (int, float)):
        return float(t2)
    return time.time()


# =========================================================
# 1) PositionState  (✅ 修正：移除同值 alias，避免誤判)
# =========================================================
class PositionState(Enum):
    EMPTY = 0            # 沒有部位（stock=0 & fut=0）
    PENDING_ENTRY = 1    # 已發出進場單（任一腿未完成）
    PENDING_EXIT = 2     # 已發出出場單（任一腿未完成）
    HOLDING = 3          # 完整對沖持倉中
    REPAIRING = 4        # 不對沖/缺腿，需要修復或強制清倉


# =========================================================
# 2) dataclasses
# =========================================================
@dataclass
class LegFill:
    order_id: Optional[str] = None
    status: str = "INIT"           # INIT / Submitted / Filled / Cancelled / Failed / PartFilled
    filled_qty: int = 0
    avg_price: float = 0.0
    last_event: Optional[dict] = None


@dataclass
class PhaseSync:
    done: threading.Event = field(default_factory=threading.Event)
    failed: threading.Event = field(default_factory=threading.Event)

    def reset(self):
        self.done.clear()
        self.failed.clear()


@dataclass
class PairState:
    stock_code: str
    fut_code: str

    # ✅ per-pair future shares equivalent (通常 2000；不同商品可變)
    future_shares_equiv: int = 2000

    state: PositionState = PositionState.EMPTY

    # targets (lots / contracts)
    target_stock_lots: int = 0
    target_fut_qty: int = 0
    last_signal_net: float = 0.0

    # real positions
    pos_stock_shares: int = 0
    pos_fut_qty: int = 0

    # inflight guard
    inflight: bool = False
    last_action_ts: float = 0.0
    last_close_ts: float = 0.0
    last_entry_ts: float = 0.0
    last_repair_ts: float = 0.0

    # legs
    open_stock: LegFill = field(default_factory=lambda: LegFill())
    open_future: LegFill = field(default_factory=lambda: LegFill())
    close_stock: LegFill = field(default_factory=lambda: LegFill())
    close_future: LegFill = field(default_factory=lambda: LegFill())

    open_sync: PhaseSync = field(default_factory=lambda: PhaseSync())
    close_sync: PhaseSync = field(default_factory=lambda: PhaseSync())
    
    repair_failures: int = 0  # Track consecutive failures (e.g. Error 88)

    open_ts: float = 0.0
    close_ts: float = 0.0
    unhedged_ts: float = 0.0

    # EWMA
    last_tick_ts: float = 0.0
    basis_mean: float = 0.0
    basis_std: float = 0.0
    sample_count: int = 0
    initialized: bool = False
    
    # Session Management
    opened_by_this_session: bool = False  # True if this position was opened by the current running instance

class MarketData:
    def __init__(self):
        self._lock = threading.Lock()
        self._stk: Dict[str, Any] = {}
        self._fut: Dict[str, Any] = {}

    def update_stock(self, quote: Any):
        code = _get_code(quote)
        if not code:
            return
        with self._lock:
            self._stk[code] = quote

    def update_future(self, quote: Any):
        code = _get_code(quote)
        if not code:
            return
        with self._lock:
            self._fut[code] = quote

    def get_quotes(self, stock_code: str, future_code: str):
        with self._lock:
            return self._stk.get(stock_code), self._fut.get(future_code)

    def get_stock(self, code: str) -> Optional[Any]:
        with self._lock:
            return self._stk.get(code)

    def get_future(self, code: str) -> Optional[Any]:
        with self._lock:
            return self._fut.get(code)


# =========================
# --- Snapshot Wrapper ---
# =========================
class SnapshotWrapper:
    """
    把 snapshot 包裝成類似 BidAsk v1 介面，避免策略剛起來 quote 還沒來時全 None。
    """
    def __init__(self, snapshot):
        self.code = snapshot.code
        self.ask_price = [float(getattr(snapshot, "sell_price", 0.0) or 0.0)]
        self.bid_price = [float(getattr(snapshot, "buy_price", 0.0) or 0.0)]
        self.ask_volume = [int(getattr(snapshot, "sell_volume", 0) or 0)]
        self.bid_volume = [int(getattr(snapshot, "buy_volume", 0) or 0)]
        self.datetime = datetime.datetime.now()
        ts = getattr(snapshot, "ts", None)
        if ts:
            try:
                self.datetime = datetime.datetime.fromtimestamp(ts / 1e9)
            except Exception:
                pass


# =========================
# --- OrderTracker (Persistence) ---
# =========================
class OrderTracker:
    """
    JSONL append-only local order log.
    Records: 1. Orders sent, 2. Orders filled, 3. Orders cancelled/failed, 4. P&L
    """
    def __init__(self, filename=ORDER_HISTORY_FILE):
        self.filename = filename
        self._queue = queue.Queue()
        self._stop_event = threading.Event()
        self._writer_thread = threading.Thread(target=self._writer_loop, daemon=True)
        self._writer_thread.start()
        self._lock = threading.Lock()
        # Track order status for P&L calculation
        self._order_status: Dict[str, Dict] = {}  # order_id -> {phase, stock_code, fut_code, open_prices, close_prices, ...}

    def record(self, order_id: str, stock_code: str, fut_code: str, phase: str, leg: str, action: str, price: float, qty: int):
        """記錄送出的訂單"""
        if not order_id:
            return
        record = {
            "event_type": "ORDER_SENT",
            "ts": time.time(),
            "date": datetime.datetime.now().strftime('%Y-%m-%d'),
            "order_id": str(order_id),
            "stock_code": stock_code,
            "fut_code": fut_code,
            "phase": phase,  # 'open' or 'close' or 'repair'
            "leg": leg,      # 'stock' or 'future'
            "action": str(action),
            "price": float(price or 0.0),
            "qty": int(qty or 0),
            "status": "Submitted"
        }
        self._queue.put(record)
        
        # Track order for P&L calculation
        with self._lock:
            if order_id not in self._order_status:
                self._order_status[order_id] = {
                    "ts": time.time(), # Added timestamp for stale check
                    "phase": phase,
                    "stock_code": stock_code,
                    "fut_code": fut_code,
                    "leg": leg,
                    "action": action,
                    "order_price": price,
                    "order_qty": qty,
                    "filled_qty": 0,
                    "filled_price": 0.0,
                    "status": "Submitted"
                }

    def record_fill(self, order_id: str, deal_qty: int, deal_price: float, status: str = "Filled"):
        """記錄成交的訂單"""
        if not order_id:
            return
        
        with self._lock:
            order_info = self._order_status.get(order_id, {})
            if not order_info:
                return
            
            # Update filled info
            order_info["filled_qty"] = deal_qty
            order_info["filled_price"] = deal_price
            order_info["status"] = status
            
            record = {
                "event_type": "ORDER_FILLED",
                "ts": time.time(),
                "date": datetime.datetime.now().strftime('%Y-%m-%d'),
                "order_id": str(order_id),
                "stock_code": order_info.get("stock_code", ""),
                "fut_code": order_info.get("fut_code", ""),
                "phase": order_info.get("phase", ""),
                "leg": order_info.get("leg", ""),
                "action": str(order_info.get("action", "")),
                "deal_qty": int(deal_qty or 0),
                "deal_price": float(deal_price or 0.0),
                "status": status,
                "order_price": float(order_info.get("order_price", 0.0)),
                "order_qty": int(order_info.get("order_qty", 0))
            }
            self._queue.put(record)
            
            # Check if pair is complete for P&L calculation
            self._check_and_record_pnl(order_info.get("stock_code"), order_info.get("phase"))

    def record_cancel(self, order_id: str, reason: str = ""):
        """記錄取消/失敗的訂單"""
        if not order_id:
            return
        
        with self._lock:
            order_info = self._order_status.get(order_id, {})
            if not order_info:
                return
            
            record = {
                "event_type": "ORDER_CANCELLED",
                "ts": time.time(),
                "date": datetime.datetime.now().strftime('%Y-%m-%d'),
                "order_id": str(order_id),
                "stock_code": order_info.get("stock_code", ""),
                "fut_code": order_info.get("fut_code", ""),
                "phase": order_info.get("phase", ""),
                "leg": order_info.get("leg", ""),
                "action": str(order_info.get("action", "")),
                "reason": reason,
                "status": "Cancelled"
            }
            self._queue.put(record)
            
            # Mark as cancelled
            order_info["status"] = "Cancelled"
            
            # If this is a repair order that failed, record it
            if order_info.get("phase") == "repair":
                self._record_repair_failure(order_info.get("stock_code"), order_info.get("leg"), reason)

    def _check_and_record_pnl(self, stock_code: str, phase: str):
        """檢查配對是否完成，計算損益"""
        if not stock_code or phase != "close":
            return
        
        with self._lock:
            # Find all orders for this stock_code and phase
            open_orders = {}
            close_orders = {}
            
            for oid, info in self._order_status.items():
                if info.get("stock_code") == stock_code:
                    if info.get("phase") == "open" and info.get("status") == "Filled":
                        leg = info.get("leg", "")
                        open_orders[leg] = info
                    elif info.get("phase") == "close" and info.get("status") == "Filled":
                        leg = info.get("leg", "")
                        close_orders[leg] = info
            
            # Calculate P&L if both legs are filled
            if "stock" in open_orders and "future" in open_orders and "stock" in close_orders and "future" in close_orders:
                self._calculate_and_record_pnl(stock_code, open_orders, close_orders)

    def get_pending_qty(self, stock_code: str, leg: str, action: str) -> int:
        """
        Get total pending quantity for a specific target and action.
        Used to prevent over-sending repair orders.
        """
        total_pending = 0
        with self._lock:
            for oid, info in self._order_status.items():
                # Filter by target
                if info.get("stock_code") != stock_code:
                    continue
                if info.get("leg") != leg:
                    continue
                
                # Check action (Buy/Sell)
                order_action = str(info.get("action", ""))
                target_action = str(action)
                if order_action != target_action:
                    continue
                
                # Check status: Submitted or PartFilled
                status = info.get("status", "")
                if status in ["Submitted", "PartFilled", "PendingSubmit", "PreSubmitted"]:
                    order_qty = int(info.get("order_qty", 0))
                    filled_qty = int(info.get("filled_qty", 0))
                    remaining = max(0, order_qty - filled_qty)
                    total_pending += remaining
        
        return total_pending

    def get_active_order_qty(self, stock_code: str, leg: str, action: str) -> int:
        """
        Get TOTAL INITIAL ORDER QTY of all active orders.
        Used for repair logic to avoid double-filling due to latency.
        """
        total = 0
        with self._lock:
            for oid, info in self._order_status.items():
                if info.get("stock_code") != stock_code: continue
                if info.get("leg") != leg: continue
                if str(info.get("action", "")) != str(action): continue
                
                status = info.get("status", "")
                if status in ["Submitted", "PartFilled", "PendingSubmit", "PreSubmitted"]:
                    total += int(info.get("order_qty", 0))
        return total

    def get_stale_pending_orders(self, stock_code: str, leg: str, action: str, timeout_sec: float) -> List[str]:
        """
        Identify active orders that have been pending for longer than timeout_sec.
        """
        stale_oids = []
        now = time.time()
        with self._lock:
            for oid, info in self._order_status.items():
                if info.get("stock_code") != stock_code: continue
                if info.get("leg") != leg: continue
                if str(info.get("action", "")) != str(action): continue
                
                status = info.get("status", "")
                if status in ["Submitted", "PartFilled", "PendingSubmit", "PreSubmitted"]:
                    order_ts = info.get("ts", now)
                    if (now - order_ts) > timeout_sec:
                        stale_oids.append(oid)
        return stale_oids

    def get_pending_orders(self, stock_code: str, leg: str, action: str) -> List[str]:
        """
        Get list of pending order IDs for a specific target and action.
        """
        pending_oids = []
        with self._lock:
            for oid, info in self._order_status.items():
                if info.get("stock_code") != stock_code:
                    continue
                if info.get("leg") != leg:
                    continue
                
                order_action = str(info.get("action", ""))
                target_action = str(action)
                if order_action != target_action:
                    continue
                
                status = info.get("status", "")
                if status in ["Submitted", "PartFilled", "PendingSubmit", "PreSubmitted"]:
                    pending_oids.append(oid)
        return pending_oids

    def _calculate_and_record_pnl(self, stock_code: str, open_orders: Dict, close_orders: Dict):
        """計算並記錄損益"""
        try:
            # Open prices
            open_stock = open_orders.get("stock", {})
            open_future = open_orders.get("future", {})
            
            # Close prices
            close_stock = close_orders.get("stock", {})
            close_future = close_orders.get("future", {})
            
            # Calculate stock P&L
            stock_open_price = float(open_stock.get("filled_price", 0.0))
            stock_close_price = float(close_stock.get("filled_price", 0.0))
            stock_qty = int(open_stock.get("filled_qty", 0))
            stock_action = str(open_stock.get("action", ""))
            
            # Calculate future P&L
            future_open_price = float(open_future.get("filled_price", 0.0))
            future_close_price = float(close_future.get("filled_price", 0.0))
            future_qty = int(open_future.get("filled_qty", 0))
            future_action = str(open_future.get("action", ""))
            
            stock_direction = 1 if "Buy" in stock_action else -1
            future_direction = 1 if "Buy" in future_action else -1

            # P&L calculation (Corrected for Unit Multipliers)
            # Stock: (Close - Open) * Qty(Lots) * 1000 * Direction
            # Future: (Close - Open) * Qty(Contracts) * 2000 * Direction
            
            stock_pnl = (stock_close_price - stock_open_price) * stock_qty * stock_direction * STOCK_SHARES_PER_LOT
            future_pnl = (future_close_price - future_open_price) * future_qty * future_direction * FUTURE_SHARES_EQUIV
            
            # Transaction costs (simplified: 0.1425% for stock, 0.002% tax for future + fee)
            # Note: Future tax is 0.00002 (10萬分之2) based on contract value
            stock_cost = (stock_open_price + stock_close_price) * stock_qty * STOCK_SHARES_PER_LOT * 0.001425
            future_cost = (future_open_price + future_close_price) * future_qty * FUTURE_SHARES_EQUIV * 0.00002
            
            total_pnl = stock_pnl + future_pnl - stock_cost - future_cost
            
            record = {
                "event_type": "P&L",
                "ts": time.time(),
                "date": datetime.datetime.now().strftime('%Y-%m-%d'),
                "stock_code": stock_code,
                "open_stock_price": stock_open_price,
                "open_future_price": future_open_price,
                "close_stock_price": stock_close_price,
                "close_future_price": future_close_price,
                "stock_qty": stock_qty,
                "future_qty": future_qty,
                "stock_pnl": stock_pnl,
                "future_pnl": future_pnl,
                "transaction_cost": stock_cost + future_cost,
                "total_pnl": total_pnl
            }
            self._queue.put(record)
            
            print(f"[P&L] {stock_code} Total P&L: {total_pnl:.2f} (Stock: {stock_pnl:.2f}, Future: {future_pnl:.2f}, Cost: {stock_cost + future_cost:.2f})")
            
        except Exception as e:
            print(f"[OrderTracker] P&L calculation failed: {e}")

    def _record_repair_failure(self, stock_code: str, leg: str, reason: str):
        """記錄修復失敗"""
        record = {
            "event_type": "REPAIR_FAILED",
            "ts": time.time(),
            "date": datetime.datetime.now().strftime('%Y-%m-%d'),
            "stock_code": stock_code,
            "leg": leg,
            "reason": reason
        }
        self._queue.put(record)

    def _writer_loop(self):
        while not self._stop_event.is_set():
            try:
                record = self._queue.get(timeout=1.0)
                try:
                    with open(self.filename, 'a', encoding='utf-8') as f:
                        f.write(json.dumps(record, ensure_ascii=False) + "\n")
                except Exception as e:
                    print(f"[OrderTracker] write failed: {e}")
            except queue.Empty:
                continue
            except Exception:
                pass

    def stop(self):
        self._stop_event.set()
        try:
            while True:
                record = self._queue.get_nowait()
                with open(self.filename, 'a', encoding='utf-8') as f:
                    f.write(json.dumps(record, ensure_ascii=False) + "\n")
        except queue.Empty:
            pass
        if self._writer_thread.is_alive():
            self._writer_thread.join(timeout=2.0)

    def load_history(self) -> List[Dict]:
        records = []
        if not os.path.exists(self.filename):
            return []
        today = datetime.datetime.now().strftime('%Y-%m-%d')
        try:
            with self._lock:
                with open(self.filename, 'r', encoding='utf-8') as f:
                    for line in f:
                        line = line.strip()
                        if not line:
                            continue
                        try:
                            rec = json.loads(line)
                            if rec.get('date') == today:
                                records.append(rec)
                        except Exception:
                            pass
        except Exception as e:
            print(f"[OrderTracker] load failed: {e}")
        return records

# =========================================================
# 3) AccountSnapshot / AccountMonitor
# =========================================================
@dataclass
class AccountSnapshot:
    ts: float = field(default_factory=lambda: time.time())
    stock_positions: Dict[str, int] = field(default_factory=dict)   # shares (signed)
    future_positions: Dict[str, int] = field(default_factory=dict)  # contracts (signed)

    def get_stock_qty(self, stock_code: str) -> int:
        return int(self.stock_positions.get(stock_code, 0))

    def get_future_qty(self, future_code: str) -> int:
        return int(self.future_positions.get(future_code, 0))

    @staticmethod
    def normalize_stock_quantity_to_shares(raw_qty: int, shares_per_lot: int) -> int:
        """
        Best-effort heuristic:
        - abs(q) >= shares_per_lot and divisible -> treat as shares
        - abs(q) <= 50 -> treat as lots -> shares
        - else (51~999) -> treat as shares (零股/小部位)
        """
        q = int(raw_qty or 0)
        aq = abs(q)
        if aq == 0:
            return 0
        if aq >= shares_per_lot and (aq % shares_per_lot == 0):
            return q
        if aq <= 50:
            return q * int(shares_per_lot)
        return q

    @staticmethod
    def direction_to_sign(direction: Any) -> int:
        d_str = str(direction).upper() if direction is not None else ""
        return 1 if "BUY" in d_str else -1


class AccountMonitor:
    def __init__(self, fetcher, interval_sec: float = 2.0) -> None:
        self._fetcher = fetcher
        self._interval = float(interval_sec)
        self._lock = threading.RLock()
        self._latest: Optional[AccountSnapshot] = None
        self._stop = threading.Event()
        self._thr: Optional[threading.Thread] = None

    def start(self) -> None:
        if self._thr and self._thr.is_alive():
            return
        self._stop.clear()
        self._thr = threading.Thread(target=self._run, name="AccountMonitor", daemon=True)
        self._thr.start()

    def stop(self) -> None:
        self._stop.set()
        if self._thr:
            self._thr.join(timeout=3.0)

    def _run(self) -> None:
        while not self._stop.is_set():
            try:
                snap = self._fetcher()
                with self._lock:
                    self._latest = snap
            except Exception:
                # FIX: Do NOT overwrite with empty snapshot on error. Just skip.
                # This prevents position flickering when API fails.
                pass
            time.sleep(self._interval)

    def latest(self) -> Optional[AccountSnapshot]:
        with self._lock:
            return self._latest


# =========================================================
# 4) PositionManager  (✅ 修正：狀態與對沖判定更一致)
# =========================================================
class PositionManager:
    """
    - PairState registry
    - inflight lock
    - order_id -> (stock_code, phase, leg)
    - order event ring buffer
    """
    def __init__(self, api=None):
        self._lock = threading.RLock()
        self._pairs: Dict[str, PairState] = {}
        self.api = api  # Store API reference for future lookups

        self._order_map: Dict[str, Dict[str, Any]] = {}
        self._orders_ring: List[Dict[str, Any]] = []
        self._orders_ring_max = 2000
        self.tracker: Optional['OrderTracker'] = None  # Set by TradingSystem
        self.account_monitor: Optional['AccountMonitor'] = None  # Set by TradingSystem
    
    def set_tracker(self, tracker: 'OrderTracker'):
        """設置 OrderTracker 引用以便記錄訂單狀態"""
        self.tracker = tracker

    def get_pending_repair_qty(self, stock_code: str, leg: str, action: str) -> int:
        """Get pending repair quantity (using initial order qty to cover latency) from tracker"""
        if self.tracker:
            return self.tracker.get_active_order_qty(stock_code, leg, action)
        return 0

    def get_pending_repair_orders(self, stock_code: str, leg: str, action: str) -> List[str]:
        if self.tracker:
            return self.tracker.get_pending_orders(stock_code, leg, action)
        return []

    def get_stale_repair_orders(self, stock_code: str, leg: str, action: str, timeout: float) -> List[str]:
        if self.tracker:
            return self.tracker.get_stale_pending_orders(stock_code, leg, action, timeout)
        return []
        
    def force_fail_order(self, order_id: str, reason: str):
        if self.tracker:
            self.tracker.record_cancel(order_id, reason)

    # ---------- Pair CRUD ----------
    def _get_underlying_stock_from_future(self, fut_code: str) -> Optional[str]:
        """Get underlying stock code from future contract code"""
        try:
            # Try to get future contract from API
            if self.api:
                try:
                    fut_contract = self.api.Contracts.Futures.get(fut_code)
                    if fut_contract and hasattr(fut_contract, 'underlying_code'):
                        underlying = str(fut_contract.underlying_code).strip()
                        if underlying and len(underlying) == 4:
                            return underlying
                except (KeyError, AttributeError, Exception):
                    pass
            
            # Fallback: Try to extract from future code pattern
            # CCFA6 -> underlying might be inferred from code
            # For now, return None if we can't determine
            return None
        except Exception:
            return None

    def ensure_pair(self, stock_code: str, fut_code: str, future_shares_equiv: Optional[int] = None) -> PairState:
        with self._lock:
            ps = self._pairs.get(stock_code)
            if ps is None:
                ps = PairState(stock_code=stock_code, fut_code=fut_code or "")
                ps.state = PositionState.EMPTY
                if future_shares_equiv is not None and int(future_shares_equiv) > 0:
                    ps.future_shares_equiv = int(future_shares_equiv)
                self._pairs[stock_code] = ps
            else:
                if fut_code and (ps.fut_code != fut_code):
                    ps.fut_code = fut_code
                if future_shares_equiv is not None and int(future_shares_equiv) > 0:
                    ps.future_shares_equiv = int(future_shares_equiv)
            return ps

    def get_pair(self, stock_code: str) -> Optional[PairState]:
        with self._lock:
            return self._pairs.get(stock_code)

    def all_pairs(self) -> List[PairState]:
        with self._lock:
            return list(self._pairs.values())

    def set_future_pair(self, stock_code: str, fut_code: str, future_shares_equiv: Optional[int] = None) -> None:
        with self._lock:
            ps = self._pairs.get(stock_code)
            if ps is None:
                ps = PairState(stock_code=stock_code, fut_code=fut_code or "")
                ps.state = PositionState.EMPTY
                if future_shares_equiv is not None and int(future_shares_equiv) > 0:
                    ps.future_shares_equiv = int(future_shares_equiv)
                self._pairs[stock_code] = ps
            else:
                ps.fut_code = fut_code or ps.fut_code
                if future_shares_equiv is not None and int(future_shares_equiv) > 0:
                    ps.future_shares_equiv = int(future_shares_equiv)

    # ---------- inflight ----------
    def set_state(self, stock_code: str, st: PositionState) -> None:
        with self._lock:
            ps = self._pairs.get(stock_code)
            if ps:
                ps.state = st
                ps.last_action_ts = time.time()

    def set_inflight(self, stock_code: str, inflight: bool) -> None:
        with self._lock:
            ps = self._pairs.get(stock_code)
            if ps:
                ps.inflight = bool(inflight)
                ps.last_action_ts = time.time()

    def set_opened_by_session(self, stock_code: str, val: bool) -> None:
        with self._lock:
            ps = self._pairs.get(stock_code)
            if ps:
                ps.opened_by_this_session = bool(val)

    def try_acquire_lock(self, stock_code: str) -> bool:
        with self._lock:
            ps = self._pairs.get(stock_code)
            if ps is None:
                return False
            now = time.time()
            if ps.inflight:
                # safety unlock if stuck too long
                if now - ps.last_action_ts > 30.0:
                    ps.inflight = False
                else:
                    return False
            ps.inflight = True
            ps.last_action_ts = now
            return True

    def release_lock(self, stock_code: str) -> None:
        with self._lock:
            ps = self._pairs.get(stock_code)
            if ps:
                ps.inflight = False
                ps.last_action_ts = time.time()

    # ---------- Phase helpers ----------
    def prepare_phase(self, stock_code: str, phase: str, target_stock_lots: int, target_fut_qty: int, last_signal_net: float = 0.0):
        with self._lock:
            ps = self._pairs.get(stock_code)
            if not ps:
                return
            ps.target_stock_lots = int(target_stock_lots)
            ps.target_fut_qty = int(target_fut_qty)
            ps.last_signal_net = float(last_signal_net or 0.0)

            if phase == "open":
                ps.open_stock = LegFill()
                ps.open_future = LegFill()
                ps.open_sync.reset()
                ps.open_ts = time.time()
                ps.last_entry_ts = time.time()
            elif phase in ("close", "repair"):
                # close/repair 都使用 close 容器（你也可再細分，但這裡保持一致）
                ps.close_stock = LegFill()
                ps.close_future = LegFill()
                ps.close_sync.reset()
                ps.close_ts = time.time()
                ps.last_close_ts = time.time()

    def get_phase_sync(self, stock_code: str, phase: str) -> Optional[PhaseSync]:
        with self._lock:
            ps = self._pairs.get(stock_code)
            if not ps:
                return None
            if phase == "open":
                return ps.open_sync
            return ps.close_sync

    # ---------- Order registry ----------
    def register_order(self, order_id: str, stock_code: str, phase: str, leg: str) -> None:
        if not order_id:
            return
        with self._lock:
            self._order_map[str(order_id)] = {
                "stock_code": str(stock_code),
                "phase": str(phase),
                "leg": str(leg),
                "ts": time.time(),
            }

    def get_inflight_orders(self, stock_code: str) -> List[str]:
        """
        Return list of pending Order IDs associated with this stock_code.
        Useful for Execution/Repair to verify if there are any active orders before sending new ones.
        """
        pending_ids = []
        with self._lock:
            # Check all registered orders in _order_map
            # Logic: If order in map but NOT finalized (Filled/Cancelled/Failed), it's potentially inflight.
            # However, _order_map doesn't track status directly except implicitly.
            # Ideally we check the `ps.open_stock`/`ps.close_stock` status or better yet check internal tracked status.
            # But currently `_order_map` is just a registry.
            
            # Alternative: Iterate `self._pairs[stock_code]` Last Events?
            ps = self._pairs.get(stock_code)
            if not ps:
                return []
            
            # Helper to check leg status
            def check_leg(leg_fill):
                if leg_fill and leg_fill.order_id and leg_fill.status in ["INIT", "Submitted", "PartFilled", "Ranking", "PendingSubmit"]:
                    return leg_fill.order_id
                return None
            
            oid1 = check_leg(ps.open_stock)
            if oid1: pending_ids.append(str(oid1))
            
            oid2 = check_leg(ps.open_future)
            if oid2: pending_ids.append(str(oid2))
            
            oid3 = check_leg(ps.close_stock)
            if oid3: pending_ids.append(str(oid3))
            
            oid4 = check_leg(ps.close_future)
            if oid4: pending_ids.append(str(oid4))
            
        return list(set(pending_ids))

    def _append_order_event(self, ev: Dict[str, Any]) -> None:
        self._orders_ring.append(ev)
        if len(self._orders_ring) > self._orders_ring_max:
            self._orders_ring = self._orders_ring[-self._orders_ring_max:]

    def dump_orders(self, last_n: int = 120) -> List[Dict[str, Any]]:
        with self._lock:
            rows = self._orders_ring[-int(last_n):]
        out = []
        for r in rows:
            rr = dict(r)
            ts = rr.get("ts", 0)
            rr["time"] = datetime.datetime.fromtimestamp(ts).strftime("%H:%M:%S") if ts else "--:--:--"
            out.append(rr)
        return out

    def dump_positions_pretty(self) -> str:
        with self._lock:
            ps_list = list(self._pairs.values())

        lines = []
        lines.append("\n=== POSITIONS ===")
        lines.append(f"{'Stock':<6} {'Future':<10} {'State':<14} {'S(sh)':>8} {'F(q)':>6} {'F_eq':>6} {'Inflight':>8} {'Unhedged(s)':>10}")
        lines.append("-" * 90)
        now = time.time()
        for ps in sorted(ps_list, key=lambda x: x.stock_code):
            unhedged_age = (now - ps.unhedged_ts) if (ps.state == PositionState.REPAIRING and ps.unhedged_ts > 0) else 0.0
            lines.append(
                f"{ps.stock_code:<6} {ps.fut_code:<10} {ps.state.name:<14} "
                f"{ps.pos_stock_shares:>8} {ps.pos_fut_qty:>6} {int(ps.future_shares_equiv):>6} {str(ps.inflight):>8} {unhedged_age:>10.1f}"
            )
        lines.append("")
        return "\n".join(lines)

    # ---------- Account Sync -> detect hedged / unhedged ----------
    def sync_from_snapshot(self, snap: AccountSnapshot) -> List[str]:
        """
        return: list of stock_codes that became UNHEDGED (enter REPAIRING)
        """
        changed = []
        with self._lock:
            # 1. Discover unknown keys from snapshot (Stock Side)
            # If we hold a stock but don't have a PairState, create one to track it.
            for code, shares in snap.stock_positions.items():
                if shares != 0 and code not in self._pairs:
                    # Found untracked position. Register likely unhedged pair (no fut_code yet)
                    ps = self.ensure_pair(code, fut_code="")
                    # Mark REPAIRING so it gets inspected
                    ps.state = PositionState.REPAIRING 
                    ps.pos_stock_shares = int(shares)
                    ps.unhedged_ts = time.time()
                    changed.append(code)

            # 1.5. FIX: Discover unknown futures from snapshot and map to correct stock
            # If we hold a future but don't have a matching PairState, find the underlying stock
            # and create/update the correct PairState
            # ALSO: Check if futures are mapped to wrong stocks and correct them
            for fut_code, fut_qty in snap.future_positions.items():
                if fut_qty == 0:
                    continue
                
                # Find which PairState has this future
                matching_ps = None
                for ps in self._pairs.values():
                    if ps.fut_code == fut_code:
                        matching_ps = ps
                        break
                
                if matching_ps:
                    # Check if the stock actually exists in account
                    actual_stock_qty = snap.get_stock_qty(matching_ps.stock_code)
                    if actual_stock_qty == 0 and matching_ps.stock_code not in snap.stock_positions:
                        # Future exists but mapped stock doesn't exist in account!
                        # Try to find correct underlying stock from future contract
                        correct_stock = self._get_underlying_stock_from_future(fut_code)
                        if correct_stock and correct_stock != matching_ps.stock_code:
                            if not SIMPLE_LOG:
                                print(f"[PosMgr] Future {fut_code} was mapped to wrong stock {matching_ps.stock_code} (not in account). Correcting to {correct_stock}")
                            # Update future code mapping for correct stock
                            correct_ps = self.ensure_pair(correct_stock, fut_code=fut_code)
                            correct_ps.pos_fut_qty = int(fut_qty)
                            # Clear wrong mapping and mark as ignored
                            matching_ps.fut_code = ""
                            matching_ps.pos_fut_qty = 0
                            matching_ps.is_ignored = True
                            matching_ps.repair_failures = 999
                else:
                    # Future exists in account but not tracked. Find underlying stock
                    underlying_stock = self._get_underlying_stock_from_future(fut_code)
                    if underlying_stock:
                        # Create or update PairState with correct stock code
                        ps = self.ensure_pair(underlying_stock, fut_code=fut_code)
                        ps.pos_fut_qty = int(fut_qty)
                        if ps.pos_stock_shares == 0:
                            # Future exists but stock doesn't - mark as REPAIRING (unless stock doesn't exist)
                            if underlying_stock in snap.stock_positions:
                                ps.state = PositionState.REPAIRING
                                ps.unhedged_ts = time.time()
                                changed.append(underlying_stock)
                            else:
                                # Stock doesn't exist - mark as ignored
                                ps.is_ignored = True
                                ps.repair_failures = 999
                        if not SIMPLE_LOG:
                            print(f"[PosMgr] Found untracked future {fut_code} (qty={fut_qty}), mapped to stock {underlying_stock}")

            # 2. Update existing pairs
            for ps in self._pairs.values():
                s = ps.stock_code
                f = ps.fut_code

                new_s = snap.get_stock_qty(s)
                new_f = snap.get_future_qty(f) if f else 0

                # FIX: Always update positions from snapshot (even if unchanged)
                # This ensures positions reflect actual account state
                # FIX: Log when future quantity changes to track why it's increasing
                old_f_qty = ps.pos_fut_qty
                ps.pos_stock_shares = int(new_s)
                ps.pos_fut_qty = int(new_f)
                
                # FIX: Debug log to track why future quantity is increasing
                if f and old_f_qty != new_f and abs(new_f) > abs(old_f_qty):
                    # Future quantity increased (in absolute value)
                    print(f"[PosMgr] {s} future {f} quantity INCREASED: {old_f_qty} → {new_f} (from snapshot)")

                # FIX: If position is zero, mark as EMPTY and skip further processing
                # This prevents trying to repair non-existent positions
                if ps.pos_stock_shares == 0 and ps.pos_fut_qty == 0:
                    if ps.state != PositionState.EMPTY:
                        # Only log if state changed
                        if not SIMPLE_LOG:
                            print(f"[PosMgr] {s} position is zero, marking as EMPTY (was {ps.state})")
                    ps.state = PositionState.EMPTY
                    ps.unhedged_ts = 0.0
                    ps.repair_failures = 0  # Reset failures when position is cleared
                    continue

                feq = int(ps.future_shares_equiv or 0) or 2000
                hedged = (
                    (ps.pos_stock_shares * ps.pos_fut_qty) < 0
                    and abs(ps.pos_stock_shares) == abs(ps.pos_fut_qty) * feq
                )

                if hedged:
                    # 如果正在 pending exit/entry，也仍可視為 holding（由策略/執行再推進）
                    if ps.state != PositionState.PENDING_EXIT and ps.state != PositionState.PENDING_ENTRY:
                        ps.state = PositionState.HOLDING
                    ps.unhedged_ts = 0.0
                else:
                    # 進入 repairing
                    # ✅ FIX: 如果還在 pending 狀態（Execution Engine 還在跑），不要被 Snapshot 強制拉回 Repairing
                    # 只有當原本已經是 HOLDING/REPAIRING/EMPTY 卻發現不平衡時，才觸發 Repair
                    if ps.state in (PositionState.PENDING_ENTRY, PositionState.PENDING_EXIT):
                        # do nothing, let execution engine handle it (or timeout)
                        pass
                    else:
                        if ps.state != PositionState.REPAIRING:
                            ps.unhedged_ts = time.time()
                            changed.append(ps.stock_code)
                        ps.state = PositionState.REPAIRING

        return changed

    # ---------- Order/Trade callbacks ----------
    def on_order_event(self, order_event: Any) -> None:
        if order_event and not SIMPLE_LOG:
            print(f"[DEBUG RAW ORDER] {order_event}")
        try:
            raw = self._to_dict(order_event)
        except Exception:
            raw = {"raw": str(order_event)}

        # order_id
        oid = str(raw.get("order_id") or raw.get("id") or raw.get("order", {}).get("id") or "")
        status = normalize_status(raw.get("status") or raw.get("state") or raw.get("order_status"))
        deal_qty = int(raw.get("deal_qty") or raw.get("deal_quantity") or raw.get("filled_qty") or 0)
        deal_px = float(raw.get("deal_price") or raw.get("filled_price") or raw.get("avg_price") or 0.0)
        
        # Extract Operation (Immediate Rejection/Confirm)
        op = raw.get("operation") or {}
        op_msg = str(op.get("op_msg") or "")
        op_code = str(op.get("op_code") or "")
        
        # Extract Message
        top_msg = raw.get("msg") or raw.get("message") or raw.get("error_message") or ""
        final_msg = op_msg if op_msg else top_msg

        # Infer Status from Op Code if Status is empty/pending but Op failed
        if op_code and op_code != "00":
             status = "Failed"

        # Force Failed status if error message detected
        # This handles cases where status remains "Submitted" but msg says "No such commodity"
        if "無此" in final_msg or "Error" in final_msg or "失敗" in final_msg:
             status = "Failed"
             
        code = str(raw.get("code") or raw.get("contract", {}).get("code") or raw.get("symbol") or "")

        info = self._order_map.get(oid, {})
        stock_code = info.get("stock_code") or ""
        phase = info.get("phase") or ""
        leg = info.get("leg") or ""

        with self._lock:
            evt_data = {
                "ts": time.time(),
                "order_id": oid,
                "code": code,
                "stock_code": stock_code,
                "phase": phase,
                "leg": leg,
                "status": status or "",
                "deal_qty": deal_qty,
                "deal_price": deal_px,
                "msg": final_msg,
            }
            self._append_order_event(evt_data)
            
            # DEBUG PRINT (only in verbose mode)
            if not SIMPLE_LOG:
                if status in ("Cancelled", "Failed", "Error") or evt_data["msg"]:
                    print(f"[Order] {code} Status: {status} Msg: {evt_data['msg']}")
            elif status in ("Failed", "Error") and evt_data["msg"]:
                # In simple mode, only show failures
                print(f"[錯誤] {code}: {evt_data['msg']}")
            elif status in ("Filled", "PartFilled") and deal_qty > 0:
                print(f">>> [成交回報] {code} {status} 數量:{deal_qty} 價格:{deal_px} (實際成交價)")

            # Handle "無此商品代碼" error for ANY order (not just repair phase)
            # This is critical because if stock doesn't exist, we should mark it as ignored immediately
            if stock_code and ("無此" in str(evt_data["msg"]) or "無此商品代碼" in str(evt_data["msg"])):
                ps = self._pairs.get(stock_code)
                if ps:
                    ps.is_ignored = True
                    ps.repair_failures = 999  # Trip circuit breaker
                    print(f"[PosMgr] Stock {stock_code} not found in simulation (msg='{evt_data['msg']}'). Marking as IGNORED to prevent blocking.")

            # Circuit Breaker: Track Repair Failures
            if stock_code and phase == "repair":
                ps = self._pairs.get(stock_code)
                if ps:
                    # Reset on any success
                    if deal_qty > 0 or status == "Filled" or status == "PartFilled":
                        ps.repair_failures = 0
                    
                    # Increment on Failure
                    # "無此商品代碼" = No such commodity code (Error 88)
                    # "集保賣出餘股數不足" = Insufficient holdings for sell (Error about inventory)
                    elif status in ("Failed", "Error", "Cancelled") or "無此" in str(evt_data["msg"]) or "餘股數" in str(evt_data["msg"]):
                        ps.repair_failures += 1
                        print(f"[PosMgr] Repair FAILED for {stock_code} (msg='{evt_data['msg']}'), fail_count={ps.repair_failures}")
                        
                        # FIX: Extract actual inventory from error message "集保賣出餘股數不足，餘股數 2000 股"
                        msg = str(evt_data["msg"] or "")
                        if "餘股數" in msg:
                            # Try to extract the actual inventory number
                            import re
                            match = re.search(r'餘股數\s*(\d+)\s*股', msg)
                            if match:
                                actual_inventory = int(match.group(1))
                                print(f"[PosMgr] 更新實際庫存: {stock_code} 實際庫存={actual_inventory}股 (原記錄={ps.pos_stock_shares}股)")
                                ps.pos_stock_shares = actual_inventory  # Update with actual inventory from broker
                                # If inventory is 0 or less, we can't sell
                                if actual_inventory <= 0:
                                    ps.is_ignored = True
                                    ps.repair_failures = 999
                                    print(f"[PosMgr] {stock_code} 庫存為 0，標記為忽略")
                        
                        # If "無此商品代碼" (commodity not found), mark as ignored immediately
                        # This prevents blocking new trades when stock doesn't exist in simulation
                        if "無此" in msg or "無此商品代碼" in msg:
                            ps.is_ignored = True
                            ps.repair_failures = 999  # Trip circuit breaker
                            print(f"[PosMgr] Stock {stock_code} not found in simulation. Marking as IGNORED to prevent blocking.")
                        elif ps.repair_failures >= 3:
                            # Circuit breaker: too many failures
                            ps.is_ignored = True
                            print(f"[PosMgr] {stock_code} 連續失敗 {ps.repair_failures} 次，標記為忽略")

        if stock_code:
            self._update_legfill_from_event(stock_code, phase, leg, status, deal_qty, deal_px, raw)
            
            # Record fill/cancel in OrderTracker
            if oid and self.tracker:
                try:
                    if status in ("Filled", "PartFilled") and deal_qty > 0:
                        self.tracker.record_fill(oid, deal_qty, deal_px, status)
                    elif status in ("Cancelled", "Failed", "Error"):
                        reason = final_msg if final_msg else status
                        self.tracker.record_cancel(oid, reason)
                except Exception as e:
                    # Silently fail to avoid disrupting order processing
                    pass

    def on_trade_event(self, trade_event: Any) -> None:
        self.on_order_event(trade_event)

    def _update_legfill_from_event(self, stock_code: str, phase: str, leg: str, status: str, deal_qty: int, deal_px: float, raw: Dict[str, Any]) -> None:
        with self._lock:
            ps = self._pairs.get(stock_code)
            if not ps:
                return

            # choose legfill container
            if phase == "open":
                lf = ps.open_stock if leg == "stock" else ps.open_future
            else:
                # close/repair 共用 close 容器
                lf = ps.close_stock if leg == "stock" else ps.close_future

            if status in ("SUBMITTED", "NEW", "ACK"):
                lf.status = "Submitted"
            elif status == "PARTFILLED":
                lf.status = "PartFilled"
            elif status == "FILLED":
                lf.status = "Filled"
            elif status == "CANCELLED":
                lf.status = "Cancelled"
            elif status == "FAILED":
                lf.status = "Failed"

            if deal_qty > 0:
                lf.filled_qty = max(lf.filled_qty, int(deal_qty))
                if deal_px > 0:
                    lf.avg_price = float(deal_px)

            lf.last_event = raw

            # mark phase done
            if phase == "open":
                if ps.open_stock.status == "Filled" and ps.open_future.status == "Filled":
                    ps.open_sync.done.set()
            else:
                if ps.close_stock.status == "Filled" and ps.close_future.status == "Filled":
                    ps.close_sync.done.set()

            # failed triggers
            if lf.status == "Failed":
                if phase == "open":
                    ps.open_sync.failed.set()
                else:
                    ps.close_sync.failed.set()

    def _to_dict(self, obj: Any) -> Dict[str, Any]:
        if obj is None:
            return {}
        if isinstance(obj, dict):
            return obj

        out: Dict[str, Any] = {}
        try:
            if hasattr(obj, "__dict__"):
                out.update({k: v for k, v in obj.__dict__.items() if not k.startswith("_")})
        except Exception:
            pass

        try:
            order = getattr(obj, "order", None)
            if order is not None:
                od = {}
                try:
                    if hasattr(order, "__dict__"):
                        od.update({k: v for k, v in order.__dict__.items() if not k.startswith("_")})
                except Exception:
                    pass
                out["order"] = od
                out["order_id"] = getattr(order, "id", None) or od.get("id")
                out["deal_qty"] = getattr(order, "deal_quantity", None) or od.get("deal_quantity")
                out["deal_price"] = getattr(order, "deal_price", None) or od.get("deal_price")

            contract = getattr(obj, "contract", None)
            if contract is not None:
                cd = {}
                try:
                    if hasattr(contract, "__dict__"):
                        cd.update({k: v for k, v in contract.__dict__.items() if not k.startswith("_")})
                except Exception:
                    pass
                out["contract"] = cd
                out["code"] = getattr(contract, "code", None) or cd.get("code")

            st = getattr(obj, "status", None)
            if st is not None:
                out["status"] = st
        except Exception:
            pass

        if "order_id" not in out:
            out["order_id"] = getattr(obj, "id", None)

        return out


# =========================================================
# 5) Reconciler
# =========================================================
class Reconciler:
    """
    啟動時：讀本地 JSONL + update_status + list_positions，盡量把 pos_mgr 接回正確狀態。
    """
    def __init__(self, api: sj.Shioaji, pos_mgr: PositionManager, order_tracker: 'OrderTracker'):
        self.api = api
        self.pos_mgr = pos_mgr
        self.tracker = order_tracker

    def reconcile(self):
        if not SIMPLE_LOG:
            print(">>> [Reconciler] Starting State Reconstruction...")

        local_orders = self.tracker.load_history()
        if local_orders and not SIMPLE_LOG:
            print(f"  [Reconciler] Found {len(local_orders)} local orders today.")
            for rec in local_orders:
                oid = str(rec.get("order_id") or "")
                s = str(rec.get("stock_code") or "")
                f = str(rec.get("fut_code") or "")
                phase = str(rec.get("phase") or "")
                leg = str(rec.get("leg") or "")
                if s and f:
                    self.pos_mgr.ensure_pair(s, f)
                if oid and s and phase and leg:
                    self.pos_mgr.register_order(oid, s, phase, leg)

        try:
            if not SIMPLE_LOG:
                print("  [Reconciler] update_status...")
            if hasattr(self.api, "update_status"):
                try:
                    self.api.update_status(self.api.stock_account)
                except Exception:
                    pass
                try:
                    self.api.update_status(self.api.futopt_account)
                except Exception:
                    pass
            time.sleep(1.2)
        except Exception as e:
            print(f"  [Reconciler] update_status failed: {e}")

        try:
            snap = self._fetch_snapshot_once()
            self.pos_mgr.sync_from_snapshot(snap)
        except Exception as e:
            print(f"  [Reconciler] position sync failed: {e}")

        if not SIMPLE_LOG:
            print(">>> [Reconciler] Done.")

    def _fetch_snapshot_once(self) -> AccountSnapshot:
        stock_positions: Dict[str, int] = {}
        future_positions: Dict[str, int] = {}

        # stocks
        try:
            stk_pos = self.api.list_positions(self.api.stock_account)
            for p in stk_pos:
                code = str(getattr(p, "code", "") or "")
                raw_qty = int(getattr(p, "quantity", 0) or 0)
                direction = getattr(p, "direction", None)
                sign = AccountSnapshot.direction_to_sign(direction)
                shares = AccountSnapshot.normalize_stock_quantity_to_shares(raw_qty, STOCK_SHARES_PER_LOT)
                stock_positions[code] = stock_positions.get(code, 0) + shares * sign
        except Exception:
            pass

        # futures
        try:
            fut_pos = self.api.list_positions(self.api.futopt_account)
            for p in fut_pos:
                code = str(getattr(p, "code", "") or "")
                qty = int(getattr(p, "quantity", 0) or 0)
                direction = getattr(p, "direction", None)
                sign = AccountSnapshot.direction_to_sign(direction)
                # FIX: Accumulate positions correctly (each position entry may have different direction/quantity)
                # But if same code appears multiple times, they should be summed
                future_positions[code] = future_positions.get(code, 0) + qty * sign
        except Exception:
            pass

        return AccountSnapshot(stock_positions=stock_positions, future_positions=future_positions)


# =========================================================
# 6) PairDiscoverer  (✅ 新增：future_contract_map，讓別的模組不用亂掃)
# =========================================================
class VolumeManager:
    def __init__(self, filename="volume_history.json"):
        self.filename = filename
        self.today_str = datetime.datetime.now().strftime('%Y-%m-%d')
        self.data = self._load()

    def _load(self):
        if os.path.exists(self.filename):
            try:
                with open(self.filename, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    if data and isinstance(data, dict):
                        first_key = next(iter(data))
                        if first_key and isinstance(data.get(first_key), dict) and 'volumes' in data[first_key]:
                            return data
                    return {}
            except Exception:
                return {}
        return {}

    def _save(self):
        try:
            with open(self.filename, 'w', encoding='utf-8') as f:
                json.dump(self.data, f, indent=2, ensure_ascii=False)
        except Exception:
            pass

    def get_5ma(self, code: str, is_market_closed: bool) -> int:
        record = self.data.get(code)
        if not record or 'volumes' not in record:
            return 0
        saved_date = record.get('date')
        volumes = record.get('volumes') or []
        valid_volumes = [int(v) for v in volumes if int(v) > 0]
        if not valid_volumes:
            return 0
        avg_vol = int(sum(valid_volumes) / len(valid_volumes))
        if not is_market_closed or saved_date == self.today_str:
            return avg_vol
        return 0

    def peek_avg(self, code: str) -> int:
        record = self.data.get(code)
        if not record or 'volumes' not in record:
            return 0
        valid = [int(v) for v in record['volumes'] if int(v) > 0]
        if not valid:
            return 0
        return int(sum(valid) / len(valid))

    def update_window_init(self, code: str, vol_list: list):
        final_list = [int(v) for v in vol_list][-5:]
        self.data[code] = {"date": self.today_str, "volumes": final_list}
        self._save()

    def slide_window(self, code: str, new_vol: int):
        record = self.data.get(code)
        if record and 'volumes' in record:
            volumes = record['volumes']
            volumes.append(int(new_vol))
            if len(volumes) > 5:
                volumes.pop(0)
            self.data[code] = {"date": self.today_str, "volumes": volumes}
            self._save()


class PairDiscoverer:
    """
    掃描個股期貨（當月）-> 用 stock/future volume 做 score -> top N
    ✅ 額外輸出：
      - future_shares_equiv_map[fut_code] = unit shares
      - future_contract_map[fut_code] = contract object
    """
    def __init__(self, api: sj.Shioaji, trading_system=None):
        self.api = api
        self.trading_system = trading_system  # Reference to TradingSystem for re-login
        self.vol_mgr = VolumeManager()
        self.daily_quotes_map: Dict[str, int] = {}

        self.future_shares_equiv_map: Dict[str, int] = {}
        self.future_contract_map: Dict[str, Any] = {}

        self._fetch_daily_quotes_cache()

    def _fetch_daily_quotes_cache(self):
        print("[PairDiscoverer] building Daily Quotes cache...")
        try:
            dq = self.api.daily_quotes(date=datetime.date.today())
            if not dq or not getattr(dq, "Code", None):
                yesterday = datetime.date.today() - datetime.timedelta(days=1)
                dq = self.api.daily_quotes(date=yesterday)

            if dq and getattr(dq, "Code", None):
                codes = list(dq.Code)
                vols = list(dq.Volume)
                for i, code in enumerate(codes):
                    self.daily_quotes_map[str(code)] = int(vols[i] or 0)
                print(f"[PairDiscoverer] daily quotes cached: {len(self.daily_quotes_map)}")
            else:
                if not SIMPLE_LOG:
                    print("[PairDiscoverer] daily quotes empty")
        except Exception as e:
            error_str = str(e)
            # Check for token expiration specifically
            if "401" in error_str or "Token is expired" in error_str or "expired" in error_str.lower() or "Topic: api/" in error_str:
                print(f"[PairDiscoverer] Daily Quotes Cache Error (Auth/Net): {e}")
                print("[PairDiscoverer] Attempting to re-login...")
                # Try to re-login if trading_system is available
                if self.trading_system:
                    try:
                        self.trading_system._re_login()
                        # Retry after re-login
                        try:
                            dq = self.api.daily_quotes(date=datetime.date.today())
                            if not dq or not getattr(dq, "Code", None):
                                yesterday = datetime.date.today() - datetime.timedelta(days=1)
                                dq = self.api.daily_quotes(date=yesterday)
                            if dq and getattr(dq, "Code", None):
                                codes = list(dq.Code)
                                vols = list(dq.Volume)
                                for i, code in enumerate(codes):
                                    self.daily_quotes_map[str(code)] = int(vols[i] or 0)
                                print(f"[PairDiscoverer] Daily quotes cached after re-login: {len(self.daily_quotes_map)}")
                        except Exception as retry_e:
                            print(f"[PairDiscoverer] Retry after re-login failed: {retry_e}")
                    except Exception as relogin_e:
                        print(f"[PairDiscoverer] Re-login failed: {relogin_e}")
                else:
                    print("[PairDiscoverer] Note: This is non-fatal. The system will continue using alternative volume sources.")
            else:
                print(f"[PairDiscoverer] Daily Quotes Cache Error: {e}")

    def _is_day_market_closed(self) -> bool:
        now = datetime.datetime.now().time()
        start = datetime.time(8, 45)
        end = datetime.time(13, 45)
        return not (start <= now <= end)

    def _fetch_last_n_days_volumes(self, contract_or_code, n_days=5) -> List[int]:
        today = datetime.datetime.now()
        is_closed = self._is_day_market_closed()

        try:
            if isinstance(contract_or_code, str):
                target = self.api.Contracts.Stocks[contract_or_code]
            else:
                target = contract_or_code

            end_date = today.strftime('%Y-%m-%d')
            start_date = (today - datetime.timedelta(days=25)).strftime('%Y-%m-%d')
            kbars = self.api.kbars(target, start=start_date, end=end_date)
            df = pd.DataFrame({**kbars})
            if df.empty:
                return []

            df['ts'] = pd.to_datetime(df['ts'])
            if is_closed:
                valid_df = df[df['ts'].dt.date <= today.date()]
            else:
                valid_df = df[df['ts'].dt.date < today.date()]

            if valid_df.empty:
                return []

            recent_df = valid_df.tail(n_days)
            return recent_df['Volume'].astype(int).tolist()
        except Exception:
            return []

    def _get_avg_volume_smart(self, code: str, contract_obj=None) -> int:
        cache_key = code
        if contract_obj is not None:
            try:
                stype = str(contract_obj.security_type)
                if stype in ['FUT', 'Future', str(sj.constant.SecurityType.Future)]:
                    # ✅ FIX: Use unique code for futures (e.g. FZFR1 vs FZFR2) instead of underlying
                    cache_key = f"F_{contract_obj.code}"
            except Exception:
                pass

        is_closed = self._is_day_market_closed()
        cached_avg = self.vol_mgr.get_5ma(cache_key, is_market_closed=is_closed)
        if cached_avg > 0:
            return cached_avg

        # close market: try daily quotes first
        if is_closed:
            today_vol = 0
            if code in self.daily_quotes_map:
                today_vol = int(self.daily_quotes_map[code])
            else:
                try:
                    target = contract_obj if contract_obj else self.api.Contracts.Stocks[code]
                    snaps = self.api.snapshots([target])
                    if snaps and len(snaps) > 0:
                        today_vol = int(getattr(snaps[0], "total_volume", 0) or 0)
                except Exception:
                    today_vol = 0

            if today_vol > 0 and cache_key in self.vol_mgr.data:
                self.vol_mgr.slide_window(cache_key, today_vol)
                refreshed = self.vol_mgr.get_5ma(cache_key, is_market_closed=True)
                if refreshed > 0:
                    return refreshed

        # try snapshots
        snapshot_vol = 0
        try:
            target = contract_obj if contract_obj else self.api.Contracts.Stocks[code]
            snaps = self.api.snapshots([target])
            if snaps and len(snaps) > 0:
                snapshot_vol = int(getattr(snaps[0], "total_volume", 0) or 0)
                if snapshot_vol > int(MIN_VOLUME_THRESHOLD * 0.5):
                    self.vol_mgr.update_window_init(cache_key, [snapshot_vol])
                    return snapshot_vol
        except Exception:
            pass

        # daily quotes fallback
        if code in self.daily_quotes_map:
            dq_vol = int(self.daily_quotes_map[code] or 0)
            if dq_vol > MIN_VOLUME_THRESHOLD:
                self.vol_mgr.update_window_init(cache_key, [dq_vol])
                return dq_vol

        # kbars fallback
        vol_list = self._fetch_last_n_days_volumes(contract_obj if contract_obj else code, n_days=5)
        if len(vol_list) > 0:
            self.vol_mgr.update_window_init(cache_key, vol_list)
            valid_vols = [v for v in vol_list if v > 0]
            if valid_vols:
                return int(sum(valid_vols) / len(valid_vols))
            return 0

        last_avg = self.vol_mgr.peek_avg(cache_key)
        if last_avg > 0:
            return last_avg

        if snapshot_vol > 0:
            return snapshot_vol

        return 0

    def get_future_shares_equiv_map(self) -> Dict[str, int]:
        return dict(self.future_shares_equiv_map)

    def get_future_contract_map(self) -> Dict[str, Any]:
        return dict(self.future_contract_map)

    def find_active_pairs(self, top_n: int = 30) -> List[Tuple[str, str]]:
        if not SIMPLE_LOG:
            print("[PairDiscoverer] scanning active stock-future pairs (R1/R2 priority)...")
        now = datetime.datetime.now()
        
        # Calculate Current Month (YM) and Next Month (YM)
        # e.g. 202510, 202511
        def get_ym(dt):
            return dt.strftime('%Y%m')
        
        current_ym = get_ym(now)
        
        # Next month calculation
        if now.month == 12:
            next_dt = datetime.datetime(now.year + 1, 1, 1)
        else:
            next_dt = datetime.datetime(now.year, now.month + 1, 1)
        next_ym = get_ym(next_dt)
        
        if not SIMPLE_LOG:
            print(f"  [PairDiscoverer] Target Months: Current={current_ym}, Next={next_ym}")

        # clear & rebuild each scan
        self.future_shares_equiv_map = {}
        self.future_contract_map = {}

        # 1) Collect all valid candidates first
        # candidates_map[underlying_code] = { "current": [c1, c2], "next": [c1, c2] }
        candidates_map = {}
        
        seen_contracts = set()

        for category in self.api.Contracts.Futures:
            try:
                iter(category)
            except TypeError:
                continue

            for contract in category:
                if isinstance(contract, tuple):
                    contract = contract[1]
                if not hasattr(contract, 'security_type'):
                    continue
                if contract.code in seen_contracts:
                    continue
                seen_contracts.add(contract.code)

                try:
                    is_futures = str(contract.security_type) in ['FUT', 'Future', str(sj.constant.SecurityType.Future)]
                    if not is_futures:
                        continue

                    if not (hasattr(contract, 'underlying_code') and contract.underlying_code and len(contract.underlying_code) == 4):
                        continue

                    d_month = getattr(contract, "delivery_month", "")
                    
                    # Filter for Current or Next month
                    is_current = (d_month == current_ym)
                    is_next = (d_month == next_ym)
                    
                    if not (is_current or is_next):
                        continue

                    # filter out small/mini by name (best-effort)
                    c_name = str(getattr(contract, 'name', '') or "")
                    if ("小型" in c_name) or ("微型" in c_name):
                        continue

                    # shares unit
                    f_unit = int(getattr(contract, 'unit', 0) or 0)
                    shares_equiv = 0
                    if f_unit == FUTURE_SHARES_EQUIV:
                        shares_equiv = FUTURE_SHARES_EQUIV
                    elif f_unit == 1:
                        shares_equiv = FUTURE_SHARES_EQUIV
                    else:
                        continue

                    # Store candidate
                    scode = str(contract.underlying_code)
                    if scode not in candidates_map:
                        candidates_map[scode] = {"current": [], "next": []}
                    
                    if is_current:
                        candidates_map[scode]["current"].append(contract)
                    else:
                        candidates_map[scode]["next"].append(contract)

                    # Update unit maps temporarily (will filter later, but map needs to exist for potential winners)
                    self.future_shares_equiv_map[str(contract.code)] = int(shares_equiv)
                    # We will update future_contract_map only for winners at the end to keep it clean? 
                    # Actually better to keep all valid ones in map just in case.
                    self.future_contract_map[str(contract.code)] = contract
                    
                except Exception as e:
                    continue

        # 2) Priority Selection (R1 > Specific)
        final_candidates = []
        
        for scode, slots in candidates_map.items():
            # Select Current Month Winner
            winner_current = None
            # Priority: Ends with 'R1' > specific month code
            r1_candidates = [c for c in slots["current"] if c.code.endswith("R1")]
            if r1_candidates:
                winner_current = r1_candidates[0] # Pick first R1 if multiple (shouldn't be)
            elif slots["current"]:
                # If no R1, pick the one with shortest code length (usually FZFA6 vs others) or just first
                # Typically specific codes are 5 chars + month? e.g. FZFA6
                winner_current = slots["current"][0]
                
            if winner_current:
                final_candidates.append(winner_current)
            
            # Select Next Month Winner
            winner_next = None
            r2_candidates = [c for c in slots["next"] if c.code.endswith("R2")]
            if r2_candidates:
                winner_next = r2_candidates[0]
            elif slots["next"]:
                winner_next = slots["next"][0]

            if winner_next:
                final_candidates.append(winner_next)

        # 3) Volume Filters & Ranking
        # batch snapshot for stocks
        stock_contracts_map = {}
        for contract in final_candidates:
            scode = str(contract.underlying_code)
            try:
                if scode not in stock_contracts_map:
                    stock_contracts_map[scode] = self.api.Contracts.Stocks[scode]
            except Exception:
                pass

        if not SIMPLE_LOG:
            print(f"  [PairDiscoverer] Batch querying {len(stock_contracts_map)} stocks & {len(final_candidates)} futures...")
        
        # 1. Stocks Snapshot
        stock_info_cache = {}
        all_stk_contracts = list(stock_contracts_map.values())
        chunk_size = 200
        for i in range(0, len(all_stk_contracts), chunk_size):
            chunk = all_stk_contracts[i:i+chunk_size]
            try:
                snaps = self.api.snapshots(chunk)
                for s in snaps:
                    price = float(getattr(s, "close", 0) or getattr(s, "reference_price", 0) or 0)
                    vol = int(getattr(s, "total_volume", 0) or 0)
                    ref_price = float(getattr(s, "reference_price", 0) or 0)
                    stock_info_cache[s.code] = (price, vol, ref_price)
            except Exception as e:
                print(f"  [PairDiscoverer] Stock snapshot chunk failed: {e}")
                time.sleep(0.5)

        # 2. Futures Snapshot (For Spread Calculation)
        future_info_cache = {} # code -> price
        
        # Build specific code map: (underlying, month) -> specific_code
        # This is needed because R1 contracts (e.g. QZFR1) return R1 in snapshots, 
        # but callbacks return specific codes (e.g. QZFA6).
        specific_code_map = {} # Key: (underlying_code, delivery_month) -> specific_code(str)
        
        # Scan TFE/STF explicitly (or all futures) to find specific contracts
        # We only need this for the current/next months we care about.
        try:
             # Iterate all categories in Futures (TFE, etc.)
             for category in self.api.Contracts.Futures:
                 try:
                     iter(category)
                 except TypeError:
                     continue
                 for contract in category:
                     if hasattr(contract, 'code') and hasattr(contract, 'underlying_code') and hasattr(contract, 'delivery_month'):
                         uc = str(contract.underlying_code)
                         dm = str(contract.delivery_month)
                         cc = str(contract.code)
                         
                         if uc == '6770':
                             if not SIMPLE_LOG:
                                 print(f"[Debug] Build Map Candidate: {uc} {dm} -> {cc}")

                         # Only store if it looks like a standard contract (Length 5: QZFA6)
                         # Filtering strictly to avoid noise, focusing on our target months
                         # CRITICAL: Exclude R1/R2 themselves, otherwise they overwrite the specific code in the map!
                         if len(cc) == 5 and not cc.endswith('R1') and not cc.endswith('R2'): 
                             specific_code_map[(uc, dm)] = cc
        except Exception as e:
            print(f"  [PairDiscoverer] specific_code_map build failed: {e}")

        for i in range(0, len(final_candidates), chunk_size):
            chunk = final_candidates[i:i+chunk_size]
            try:
                snaps = self.api.snapshots(chunk)
                for s in snaps:
                    price = float(getattr(s, "close", 0) or getattr(s, "settlement_price", 0) or getattr(s, "reference_price", 0) or 0)
                    future_info_cache[s.code] = price
            except Exception as e:
                print(f"  [PairDiscoverer] Future snapshot chunk failed: {e}")
                time.sleep(0.5)

        best_by_pair = {} # Key: (stock_code, fut_code)

        for contract in final_candidates:
            stock_code = str(contract.underlying_code)
            # Try to resolve valid specific code from map
            generic_code = str(contract.code) 
            
            # Determine target month for this candidate (Is it R1 or R2?)
            # R1 -> current_ym, R2 -> next_ym
            # Shioaji R1/R2 objects don't strictly say "I am R1".
            # But the loop that built `final_candidates` (lines 1391+) was iterating specific contracts?
            # Wait, `final_candidates` comes from `active_pairs` logic?
            # Actually, `final_candidates` (line 1496) is just `list(set(...))`.
            # And `_get_potential_pairs` returns `(candidates, final_cond)`.
            # In `_get_potential_pairs`, we returned specific contracts (from the loop iteration).
            # If so, why does `contract.code` return `QZFR1`?
            # Because `api.Contracts.Futures.R1` contains special proxy contracts.
            
            # Resolution Logic:
            # If code is R1/R2, use specific map.
            # R1 usually maps to `current_ym`.
            
            # HACK: Identify if it is R1/R2 by length?
            # QZFR1 (length 5). QZFA6 (length 5).
            # But R1 ends with R1.
            
            if generic_code.endswith("R1"):
                resolved_code = specific_code_map.get((stock_code, current_ym), generic_code)
                if stock_code == '6770':
                    if not SIMPLE_LOG:
                        print(f"[Debug] Resolve Attempt: {stock_code} {current_ym} -> {resolved_code}")
                future_code = resolved_code
            elif generic_code.endswith("R2"):
                resolved_code = specific_code_map.get((stock_code, next_ym), generic_code)
                future_code = resolved_code
            else:
                future_code = generic_code
            
            if future_code != generic_code:
                # print(f"[Debug] Resolved {generic_code} -> {future_code}")
                # Ensure we use the resolved code for price lookup if possible?
                # Snapshot cache keys are likely Generic (R1).
                # So we must check cache using generic, but Store/Return using Specific.
                pass 
                
            stk_c = stock_contracts_map.get(stock_code)
            if stk_c:
                # 1. Check unit = 1000
                s_unit = int(getattr(stk_c, 'unit', 0) or 0)
                if s_unit not in (STOCK_SHARES_PER_LOT, 1):
                    continue
                
                # 2. Check Day Trade Status (Filters out Restricted/Disposition stocks)
                #    If day_trade != Yes (e.g. No or OnlyBuy), we skip it.
                if getattr(stk_c, 'day_trade', None) != sj.constant.DayTrade.Yes:
                    continue

            info = stock_info_cache.get(stock_code)
            if info:
                s_price, s_vol, s_ref_price = info

                # drop limit-up/down like extremes if strictly needed
                # (Removing the hard 9% skip might be better to see opportunities, but keeping safety for now)
                
                if s_price > 500:
                    continue

                if s_vol < MIN_VOLUME_THRESHOLD:
                    if s_vol == 0 and (not self._is_day_market_closed()):
                        continue
                    else:
                        s_vol = self._get_avg_volume_smart(stock_code, None)

                stock_avg = s_vol if s_vol > 0 else self._get_avg_volume_smart(stock_code, None)
            else:
                stock_avg = self._get_avg_volume_smart(stock_code, None)
                s_price = 0.0

            if stock_avg < MIN_VOLUME_THRESHOLD:
                continue

            future_avg = self._get_avg_volume_smart(future_code, contract_obj=contract)
            if future_avg < MIN_VOLUME_THRESHOLD:
                continue
                
            f_price = future_info_cache.get(future_code, 0.0)
            
            # --- Spread Calculation ---
            spread_pct = 0.0
            if s_price > 0:
                spread_pct = (f_price - s_price) / s_price * 100.0

            score = min(stock_avg, future_avg)

            s_name = getattr(stk_c, "name", "N/A") if stk_c else "N/A"
            f_name = getattr(contract, "name", "N/A")
            
            rec = (score, stock_avg, future_avg, stock_code, future_code, str(s_name).strip(), str(f_name).strip(), spread_pct)
            key = (stock_code, future_code)
            
            if key not in best_by_pair or rec[0] > best_by_pair[key][0]:
                best_by_pair[key] = rec

        ranked = list(best_by_pair.values())
        ranked.sort(key=lambda x: x[0], reverse=True)
        top = ranked[:top_n]

        if not SIMPLE_LOG:
            print(f"[PairDiscoverer] done. matched={len(ranked)} return={len(top)}")
            print("\n=== Active Pairs (Top) ===")
            print(f"{'Stock':<10} {'Name':<12} {'Future':<10} {'Name':<12} {'Score':<8} {'Spread%':<8} {'S_Vol':<8} {'F_Vol':<8}")
        if not SIMPLE_LOG:
            print("-" * 94)
            for row in top:
                scr, sa, fa, sc, fc, sn, fn, spr = row
                print(f"{sc:<10} {sn:<12} {fc:<10} {fn:<12} {scr:<8} {spr:>7.2f}% {sa:<8} {fa:<8}")
            print("==========================\n")

        return [(s_code, f_code) for (_, _, _, s_code, f_code, _, _, _) in top]


# =========================================================
# 7) SubscriptionManager  (✅ 修正：locked + linger + max_subs 更穩)
# =========================================================
class SubscriptionManager:
    def __init__(
        self,
        api: sj.Shioaji,
        max_subs: int = 80,
        quote_type=None,
        quote_version=None,
        verbose: bool = True,
        pos_mgr: Optional[PositionManager] = None,
        unsub_linger_sec: float = 60.0,
    ):
        self.api = api
        self.max_subs = int(max_subs)
        self.verbose = verbose

        self.quote_type = quote_type or sj.constant.QuoteType.BidAsk
        self.quote_version = quote_version or sj.constant.QuoteVersion.v1

        self.pos_mgr = pos_mgr
        self.unsub_linger_sec = float(unsub_linger_sec)

        self._refcnt: Counter[str] = Counter()
        self._kind: Dict[str, str] = {}
        self._subscribed: Set[str] = set()

        # ✅ 統一來源：由 PairDiscoverer 提供 fut_code -> contract object
        self._future_map: Dict[str, Any] = {}

        self._linger_deadline: Dict[str, float] = {}
        self._lock = threading.RLock()

    def set_future_map(self, m: Dict[str, Any]):
        with self._lock:
            self._future_map = dict(m or {})

    def apply_pairs(self, pairs: List[Tuple[str, Optional[str]]], log_prefix: str = "") -> None:
        now = time.time()
        with self._lock:
            # ---- locked codes: 持倉/repair/inflight 的 code 永遠不退訂 ----
            locked_codes: Set[str] = set()
            if self.pos_mgr is not None:
                try:
                    for ps in self.pos_mgr.all_pairs():
                        has_pos = (int(ps.pos_stock_shares) != 0) or (int(ps.pos_fut_qty) != 0)
                        is_active_state = ps.state in (
                            PositionState.PENDING_ENTRY,
                            PositionState.PENDING_EXIT,
                            PositionState.HOLDING,
                            PositionState.REPAIRING,
                        )
                        if has_pos or is_active_state or bool(ps.inflight):
                            if ps.stock_code:
                                locked_codes.add(str(ps.stock_code))
                            if ps.fut_code:
                                locked_codes.add(str(ps.fut_code))
                except Exception:
                    pass

            desired = self._build_desired_refcnt(pairs)
            for c in locked_codes:
                desired[str(c)] += 1

            desired_kind = self._infer_kind_map(pairs)
            for c in locked_codes:
                if c not in desired_kind:
                    desired_kind[c] = self._guess_kind_by_contracts(c)

            # ---- manage linger ----
            for code in list(self._linger_deadline.keys()):
                if desired.get(code, 0) > 0:
                    self._linger_deadline.pop(code, None)

            for code, old_cnt in list(self._refcnt.items()):
                if old_cnt > 0 and desired.get(code, 0) == 0:
                    if code not in self._linger_deadline:
                        self._linger_deadline[code] = now + self.unsub_linger_sec

            # still in linger -> keep desired
            for code, deadline in list(self._linger_deadline.items()):
                if now < deadline:
                    desired[code] += 1

            to_sub: List[str] = []
            to_unsub: List[str] = []

            for code, new_cnt in desired.items():
                old_cnt = self._refcnt.get(code, 0)
                if old_cnt == 0 and new_cnt > 0:
                    to_sub.append(code)

            for code, old_cnt in list(self._refcnt.items()):
                new_cnt = desired.get(code, 0)
                if old_cnt > 0 and new_cnt == 0:
                    deadline = self._linger_deadline.get(code, 0.0)
                    if deadline <= 0.0 or now >= deadline:
                        # 最後保護：locked 絕不退訂
                        if code not in locked_codes:
                            to_unsub.append(code)

            projected = (self._subscribed - set(to_unsub)) | set(to_sub)
            if len(projected) > self.max_subs:
                keep_existing = list(self._subscribed - set(to_unsub))
                for c in locked_codes:
                    if c not in keep_existing:
                        keep_existing.append(c)
                keep_existing_set = set(keep_existing)

                allowed_new = self.max_subs - len(keep_existing_set)
                if allowed_new < 0:
                    allowed_new = 0

                locked_to_sub = [c for c in to_sub if c in locked_codes]
                normal_to_sub = [c for c in to_sub if c not in locked_codes]
                to_sub = (locked_to_sub + normal_to_sub)[:allowed_new]

                if self.verbose:
                    print(
                        f"{log_prefix}[Subscription] WARNING: exceed max_subs={self.max_subs}. "
                        f"keep_existing={len(keep_existing_set)} allowed_new={allowed_new} final_to_sub={len(to_sub)}"
                    )

            self._batch_unsubscribe(to_unsub, log_prefix=log_prefix)
            self._batch_subscribe(to_sub, desired_kind, log_prefix=log_prefix)

            # rebuild subscribed set
            actual_subscribed = set(self._subscribed)
            actual_subscribed -= set(to_unsub)
            actual_subscribed |= set(to_sub)

            self._subscribed = actual_subscribed

            new_refcnt = Counter()
            for c in self._subscribed:
                new_refcnt[str(c)] = max(1, int(desired.get(c, 1)))
            self._refcnt = new_refcnt

            if self.verbose and not SIMPLE_LOG:
                print(f"{log_prefix} now subscribed={len(self._subscribed)} (limit={self.max_subs})")

    def force_unsubscribe_all(self, log_prefix: str = ""):
        with self._lock:
            codes = list(self._subscribed)
            self._batch_unsubscribe(codes, log_prefix=log_prefix)
            self._refcnt.clear()
            self._kind.clear()
            self._subscribed.clear()
            self._linger_deadline.clear()

    def _build_desired_refcnt(self, pairs: List[Tuple[str, Optional[str]]]) -> Counter:
        c = Counter()
        for s, f in pairs:
            if s:
                c[str(s)] += 1
            if f:
                c[str(f)] += 1
        return c

    def _infer_kind_map(self, pairs: List[Tuple[str, Optional[str]]]) -> Dict[str, str]:
        kind: Dict[str, str] = {}
        for s, _ in pairs:
            if s:
                kind[str(s)] = "stk"
        for _, f in pairs:
            if f:
                kind[str(f)] = "fut"
        return kind

    def _guess_kind_by_contracts(self, code: str) -> str:
        try:
            _ = self.api.Contracts.Stocks[code]
            return "stk"
        except Exception:
            pass
        try:
            _ = self.api.Contracts.Futures[code]
            return "fut"
        except Exception:
            pass
        return "stk"

    def _get_contract(self, code: str, kind: str):
        if kind == "stk":
            return self.api.Contracts.Stocks[code]
        if kind == "fut":
            if code in self._future_map:
                return self._future_map[code]
            # last resort
            return self.api.Contracts.Futures[code]
        raise ValueError(f"Unknown kind={kind} code={code}")

    def _batch_subscribe(self, codes: List[str], desired_kind: Dict[str, str], log_prefix: str = ""):
        ok = 0
        for code in codes:
            kind = desired_kind.get(code) or self._guess_kind_by_contracts(code)
            try:
                c = self._get_contract(code, kind)
                self.api.quote.subscribe(c, quote_type=self.quote_type, version=self.quote_version)
                self._kind[code] = kind
                ok += 1
            except Exception as e:
                print(f"{log_prefix} +subscribe {code} FAILED: {e}")
        if self.verbose and codes and not SIMPLE_LOG:
            print(f"{log_prefix} +subscribe {len(codes)} (ok={ok})")

    def _batch_unsubscribe(self, codes: List[str], log_prefix: str = ""):
        ok = 0
        for code in codes:
            kind = self._kind.get(code) or self._guess_kind_by_contracts(code)
            try:
                c = self._get_contract(code, kind)
                self.api.quote.unsubscribe(c, quote_type=self.quote_type, version=self.quote_version)
                self._kind.pop(code, None)
                ok += 1
            except Exception as e:
                print(f"{log_prefix} -unsubscribe {code} FAILED: {e}")
        if self.verbose and codes:
            if not SIMPLE_LOG:
                print(f"{log_prefix} -unsubscribe {len(codes)} (ok={ok})")


# =========================================================
# 8) CapitalManager
# =========================================================
class CapitalManager:
    def __init__(self, max_capital: float, pos_mgr: PositionManager, market_data: 'MarketData'):
        self.max_capital = float(max_capital)
        self.pos_mgr = pos_mgr
        self.market_data = market_data

    def get_usage(self) -> Tuple[float, float, float]:
        holding_val = 0.0
        pending_val = 0.0

        pairs = self.pos_mgr.all_pairs()
        for ps in pairs:
            # Skip Ignored/Blacklisted positions (Circuit Breaker)
            if getattr(ps, "repair_failures", 0) > 3:
                continue

            if ps.pos_stock_shares != 0:
                quote = self.market_data.get_stock(ps.stock_code)
                price = _get_bid(quote)
                if price <= 0:
                    price = ps.open_stock.avg_price or 0.0
                holding_val += abs(ps.pos_stock_shares) * price

            if ps.state == PositionState.PENDING_ENTRY:
                quote = self.market_data.get_stock(ps.stock_code)
                price = _get_ask(quote)
                if price > 0:
                    pending_val += (ps.target_stock_lots * STOCK_SHARES_PER_LOT * price)

        total_used = holding_val + pending_val
        return total_used, holding_val, pending_val

    def check_available(self, required_amount: float) -> bool:
        used, _, _ = self.get_usage()
        remaining = self.max_capital - used
        return remaining >= required_amount

    def log_status(self):
        used, h, p = self.get_usage()
        print(f"[Capital] Max: {self.max_capital/10000:.0f}萬 | Used: {used/10000:.0f}萬 (Hold:{h/10000:.0f} Pending:{p/10000:.0f}) | Rem: {(self.max_capital-used)/10000:.0f}萬")


# =========================================================
# 9) StrategyEngine  (✅ 修正：lock release 用 finally，避免漏釋放)
# =========================================================
class TradeSignal(dict):
    pass


class StrategyEngine:
    def __init__(self, market_data: 'MarketData', pos_mgr: PositionManager, capital_mgr: CapitalManager):
        self.last_risk_warning_ts = 0  # Throttle risk warnings
        self.market_data = market_data
        self.pos_mgr = pos_mgr
        self.capital_mgr = capital_mgr
        self._pairs_lock = threading.RLock()
        self.pairs: List[Tuple[str, str]] = []
        self.pair_map: Dict[str, List[str]] = {}
        self.future_to_stock: Dict[str, str] = {}

        self.order_count_min = 0
        self.last_min_ts = time.time()
        self.order_count_day = 0
        self.today_str = datetime.datetime.now().strftime('%Y-%m-%d')
        self.start_ts = time.time()
        
        # Heartbeat tracking
        self.latest_z_scores = {}
        self.last_heartbeat_ts = time.time()

    def update_pairs(self, pairs: List[Tuple[str, str]]):
        with self._pairs_lock:
            self.pairs = list(pairs)
            pair_map: Dict[str, List[str]] = {}
            future_to_stock: Dict[str, str] = {}
            for s, f in self.pairs:
                pair_map.setdefault(s, []).append(f)
                if f:
                    future_to_stock[f] = s
                self.pos_mgr.ensure_pair(s, f)
            self.pair_map = pair_map
            self.future_to_stock = future_to_stock
        if not SIMPLE_LOG:
            print(f"[Strategy] pairs updated: {len(self.pairs)}")

    def check_risk(self, is_open: bool = True, current_stock: str = None) -> bool:
        now = time.time()
        if now - self.last_min_ts > 60:
            self.last_min_ts = now
            self.order_count_min = 0

        day_str = datetime.datetime.now().strftime('%Y-%m-%d')
        if day_str != self.today_str:
            self.today_str = day_str
            self.order_count_day = 0

        if self.order_count_min >= MAX_ORDERS_PER_MIN:
            print(f"[Strategy] Risk: Max orders/min reached ({self.order_count_min})")
            return False

        if self.order_count_day >= MAX_DAILY_ORDERS:
            print(f"[Strategy] Risk: Max daily orders reached ({self.order_count_day})")
            return False

        if KILL_SWITCH and is_open:
            return False
        if DEGRADE_MODE and is_open:
            return False

        if is_open:
            # TIME CHECK: Stop new entries after 13:25
            now_dt = datetime.datetime.now()
            if now_dt.time() >= datetime.time(13, 25):
                # Throttle warning
                if time.time() - self.last_risk_warning_ts > 60:
                     if SIMPLE_LOG:
                         print(f"[系統] 13:25 後停止新開倉。")
                     else:
                         print(f"[Strategy] Risk: No new entries after 13:25.")
                     self.last_risk_warning_ts = time.time()
                return False

            # DEBUG_MODE: STRICT SINGLE THREADED TRADING
            # If any pair is in a working state (PENDING_ENTRY, PENDING_EXIT, REPAIRING), block new OPENs.
            if DEBUG_MODE:
                for ps in self.pos_mgr.all_pairs():
                    # Skip Self
                    if current_stock and ps.stock_code == current_stock:
                        continue

                    if getattr(ps, "is_ignored", False): continue
                    if getattr(ps, "repair_failures", 0) > 3: continue
                    # Skip effectively empty pairs
                    if ps.pos_stock_shares == 0 and ps.pos_fut_qty == 0 and ps.state == PositionState.EMPTY and not ps.inflight: continue
                    
                    # If this pair is doing something, BLOCK everything else
                    if ps.state in (PositionState.PENDING_ENTRY, PositionState.PENDING_EXIT, PositionState.REPAIRING) or ps.inflight:
                        # Throttle warning
                        if time.time() - self.last_risk_warning_ts > 10:
                            if SIMPLE_LOG:
                                print(f"[系統] 暫停新交易，因有進行中部位: {ps.stock_code} ({ps.state.name})")
                            else:
                                print(f"[Debug] Blocked by working pair: {ps.stock_code} ({ps.state.name})")
                            self.last_risk_warning_ts = time.time()
                        return False

            active = 0
            unhedged = 0
            blocker = None
            for ps in self.pos_mgr.all_pairs():
                # Skip Ignored/Blacklisted positions (Circuit Breaker)
                # CRITICAL FIX: Respect explicit Ignore flag from startup choice
                if getattr(ps, "is_ignored", False):
                    continue

                if getattr(ps, "repair_failures", 0) > 3:
                    continue

                # Also skip positions that are effectively empty (no actual position)
                # This handles cases where position exists in pos_mgr but actual shares are 0
                if abs(getattr(ps, "pos_stock_shares", 0)) == 0 and abs(getattr(ps, "pos_fut_qty", 0)) == 0:
                    # Position is effectively empty, skip it
                    continue

                if ps.state in (PositionState.PENDING_ENTRY, PositionState.PENDING_EXIT, PositionState.HOLDING, PositionState.REPAIRING):
                    active += 1
                if ps.state == PositionState.REPAIRING:
                    unhedged += 1
                    # Debug which one is blocking
                    if unhedged == 1:
                         blocker = ps.stock_code
            if unhedged > 0:
                # Throttle warning messages: only print once per 60 seconds
                now = time.time()
                if now - self.last_risk_warning_ts > 60:
                    if SIMPLE_LOG:
                        print(f"[系統] 暫停新交易，因有缺腳部位: {blocker if blocker else '?'}")
                    else:
                        print(f"[Strategy] Risk: Blocked by unhedged positions ({unhedged}). Blocker example: {blocker if blocker else '?'}")
                    self.last_risk_warning_ts = now
                return False
            if active >= MAX_OPEN_PAIRS:
                print(f"[Strategy] Risk: Max open pairs reached ({active}/{MAX_OPEN_PAIRS})")
                return False

        return True

    def _increment_risk_counter(self):
        self.order_count_min += 1
        self.order_count_day += 1

    def calculate_cost(self, stock_price: float, future_price: float, future_shares_equiv: int) -> float:
        stock_notional = stock_price * STOCK_SHARES_PER_LOT * STOCK_QTY
        future_notional = future_price * int(future_shares_equiv) * FUTURE_QTY
        stock_fee = stock_notional * STOCK_FEE_RATE
        future_fee = FUT_FEE_PER_CONTRACT * FUTURE_QTY
        tax_stock = stock_notional * STOCK_TAX_RATE
        tax_future = future_notional * FUT_TAX_RATE
        return stock_fee + future_fee + tax_stock + tax_future

    def _update_ewma(self, ps: PairState, basis: float, now: float):
        if not ps.initialized:
            ps.basis_mean = basis
            ps.basis_std = STD_FLOOR
            ps.last_tick_ts = now
            ps.sample_count = 1
            ps.initialized = True
            return

        dt = now - ps.last_tick_ts
        if dt <= 0:
            return

        alpha = 1.0 - math.exp(-0.693 * dt / EWMA_HALF_LIFE)
        if alpha > 1.0:
            alpha = 1.0

        diff = basis - ps.basis_mean
        ps.basis_mean += alpha * diff

        mad_diff = abs(diff) - ps.basis_std
        ps.basis_std += alpha * mad_diff

        if ps.basis_std < STD_FLOOR:
            ps.basis_std = STD_FLOOR

        ps.last_tick_ts = now
        ps.sample_count += 1

    def on_quote(self, quote: Any) -> Optional[TradeSignal]:
        code = _get_code(quote)
        # Debug entry point
        if code == DBG_WATCH_STOCK:
             print(f"[Strategy Entry] {code}")

        if time.time() - self.start_ts < WARMUP_SEC:
            if code == DBG_WATCH_STOCK: print(f"[Trace] Limit: Warmup")
            return None
        if not code:
            return None

        with self._pairs_lock:
            pair_map = self.pair_map
            future_to_stock = self.future_to_stock

        if code in pair_map:
            target_stock = code
            futures = pair_map[code]
        elif code in future_to_stock:
            target_stock = future_to_stock[code]
            futures = [code]
        else:
            if code == DBG_WATCH_STOCK: print(f"[Trace] Limit: Not in Map")
            return None

        should_release_lock = True
        if not self.pos_mgr.try_acquire_lock(target_stock):
            if code == DBG_WATCH_STOCK: print(f"[Trace] Limit: Lock Failed")
            return None

        try:
            ps = self.pos_mgr.get_pair(target_stock)
            if not ps:
                if code == DBG_WATCH_STOCK: print(f"[Trace] Limit: No Pair State")
                return None
            
            if code == DBG_WATCH_STOCK: print(f"[Trace] Post-Lock OK. State={ps.state}")

            now = time.time()

            # FIX: Check for STUCK PENDING states (Callback lost)
            # If state is PENDING but we actually have the position, force update.
            if ps.state in (PositionState.PENDING_ENTRY, PositionState.PENDING_EXIT):
                 if now - ps.last_action_ts > 5.0:
                     if self.pos_mgr.account_monitor:
                         snap = self.pos_mgr.account_monitor.latest()
                         if snap:
                             self.pos_mgr.sync_from_snapshot(snap)
                             
                             s = ps.pos_stock_shares
                             f = ps.pos_fut_qty
                             eq = ps.future_shares_equiv or 2000
                             
                             # Case 1: PENDING_ENTRY but actually filled (Balanced)
                             if ps.state == PositionState.PENDING_ENTRY:
                                 # e.g. Stock 2000, Future -1 => Balanced
                                 if s > 0 and f < 0 and (s + f * eq == 0):
                                      if SIMPLE_LOG:
                                          print(f"[系統] {target_stock} 偵測到已成交 (S:{s}, F:{f})，強制更新狀態為 HOLDING")
                                      else:
                                          print(f"[Strategy] {target_stock} Correction: PENDING_ENTRY -> HOLDING (Synced)")
                                      self.pos_mgr.set_state(target_stock, PositionState.HOLDING)
                                      self.pos_mgr.set_inflight(target_stock, False) # Clear inflight to allow exit logic

                             # Case 2: PENDING_EXIT but actually empty
                             if ps.state == PositionState.PENDING_EXIT:
                                 if s == 0 and f == 0:
                                      if SIMPLE_LOG:
                                          print(f"[系統] {target_stock} 偵測到已平倉 (S:0, F:0)，強制更新狀態為 EMPTY")
                                      else:
                                          print(f"[Strategy] {target_stock} Correction: PENDING_EXIT -> EMPTY (Synced)")
                                      self.pos_mgr.set_state(target_stock, PositionState.EMPTY)
                                      self.pos_mgr.set_inflight(target_stock, False)
                                 
                                 # Case 3: PENDING_EXIT but stuck with position (Zombie Order)
                                 # If we still have position after timeout, revert to HOLDING to trigger retry
                                 elif now - ps.last_action_ts > 45.0:
                                      if SIMPLE_LOG:
                                          print(f"[系統] {target_stock} 平倉卡住超過 45s (仍有庫存)，重置狀態為 HOLDING 以便重試")
                                      else:
                                          print(f"[Strategy] {target_stock} Stuck PENDING_EXIT > 45s. Revert to HOLDING.")
                                      self.pos_mgr.set_state(target_stock, PositionState.HOLDING)
                                      self.pos_mgr.set_inflight(target_stock, False)

            # ---- AUTO REPAIR ----
            if ps.state == PositionState.REPAIRING:
                # NEW: Session Management Check
                if not ps.opened_by_this_session and not MANAGE_EXISTING_POSITIONS:
                     if not getattr(ps, "is_ignored", False):
                         if SIMPLE_LOG:
                             print(f"[系統] 發現外部既有部位 {target_stock} (需修復)，標記為忽略。")
                         ps.is_ignored = True
                         ps.repair_failures = 999
                     return None

                # BREAK: Too many failures OR already ignored
                if getattr(ps, "is_ignored", False):
                    # Position is ignored - skip all repair attempts
                    return None
                
                if getattr(ps, "repair_failures", 0) > 3:
                     # Silenced (User req): only log extremely rarely (1 hour)
                     if now - ps.last_repair_ts > 3600.0:
                         # print(f"[Strategy] Auto-Repair SKIP {target_stock}: Too many failures ({ps.repair_failures}). Manual intervention required.")
                         ps.last_repair_ts = now
                     return None

                # Increase cooldown to avoid spamming orders if they don't fill immediately
                if now - ps.last_repair_ts < 5.0:
                    return None

                duration = now - (ps.unhedged_ts or now)
                
                # FIX: Force sync from AccountMonitor BEFORE any checks
                # This ensures we use the latest actual positions from broker
                if self.pos_mgr.account_monitor:
                    snap = self.pos_mgr.account_monitor.get_latest()
                    if snap:
                        # FIX: Log before sync to debug position changes
                        old_s_shares = ps.pos_stock_shares
                        old_f_qty = ps.pos_fut_qty
                        
                        self.pos_mgr.sync_from_snapshot(snap)
                        # Re-read ps after sync (it may have changed)
                        ps = self.pos_mgr.get_pair(target_stock)
                        if not ps:
                            return None
                        
                        # Re-read positions after sync
                        s_shares = ps.pos_stock_shares
                        f_qty = ps.pos_fut_qty
                        
                        # FIX: Log position changes to debug why positions are increasing
                        if old_f_qty != f_qty or old_s_shares != s_shares:
                            if not SIMPLE_LOG:
                                print(f"[Debug] {target_stock} position changed: s_shares {old_s_shares}→{s_shares}, f_qty {old_f_qty}→{f_qty}")
                
                # FIX: Verify position actually exists in account before attempting repair
                # If both positions are zero, mark as EMPTY and skip repair
                s_shares = ps.pos_stock_shares
                f_qty = ps.pos_fut_qty
                
                # FIX: Critical check - if stock doesn't exist in account snapshot, mark as ignored IMMEDIATELY
                # ** 這才是問題的核心：系統嘗試修復不存在的股票（2303），導致錯誤的下單 **
                # ** 當有期貨 CCFA6 但沒有股票時，應該直接平倉期貨，而不是去買賣不存在的股票 2303 **
                # MUST check BEFORE any repair logic to avoid wasting time/resources and prevent wrong orders
                if self.pos_mgr.account_monitor:
                    snap = self.pos_mgr.account_monitor.get_latest()
                    if snap:
                        # Check if stock actually exists in account snapshot
                        stock_exists_in_account = target_stock in snap.stock_positions
                        actual_stock_qty = snap.get_stock_qty(target_stock)
                        
                        # FIX: 如果股票不存在，但期貨存在，應該直接平倉期貨
                        # 用戶邏輯：CCFA6 就是期貨，直接用 contracts 沖銷就好，不需要映射到股票
                        if s_shares == 0 and actual_stock_qty == 0 and not stock_exists_in_account and f_qty != 0:
                            # 有期貨但沒有股票，直接平倉期貨
                            if ps.fut_code:
                                # 直接生成平倉期貨的信號，使用期貨代碼
                                is_force_close = (duration > UNHEDGED_REPAIR_DEADLINE)
                                if f_qty < 0:
                                    # 做空期貨，買回平倉
                                    action = "FORCE_CLOSE_FUTURE_BUY"
                                else:
                                    # 做多期貨，賣出平倉
                                    action = "FORCE_CLOSE_FUTURE_SELL"
                                qty = abs(f_qty)
                                
                                if SIMPLE_LOG:
                                    print(f"[缺腳] {ps.fut_code} 直接平倉: {action} qty={qty} (股票 {target_stock} 不存在)")
                                else:
                                    print(f"[Strategy] {target_stock} stock doesn't exist, closing future {ps.fut_code} directly: {action} qty={qty}")
                                
                                ps.last_repair_ts = now
                                return TradeSignal({
                                    "type": "REPAIR",
                                    "sub_type": action,
                                    "stock_code": target_stock,  # Keep for tracking, but won't be used for order
                                    "future_code": ps.fut_code,  # Use future code directly
                                    "qty": qty,
                                    "is_force": is_force_close
                                })
                            else:
                                # No future code - mark as ignored
                                if not getattr(ps, "is_ignored", False):
                                    ps.is_ignored = True
                                    ps.repair_failures = 999
                                    if SIMPLE_LOG:
                                        print(f"[錯誤] {target_stock}: 股票不存在於帳戶中（期貨={f_qty}），標記為忽略")
                                    else:
                                        print(f"[Strategy] {target_stock} stock doesn't exist in account (f_qty={f_qty}), marking as IGNORED")
                                return None
                
                # FIX: Additional check - if stock position is 0 AND we're trying to buy stock,
                # but the stock doesn't exist in account, skip repair
                # This prevents trying to repair non-existent stocks (e.g., 2303)
                if s_shares == 0 and f_qty == 0:
                    # Position is actually empty, mark as EMPTY and skip repair
                    ps.state = PositionState.EMPTY
                    ps.unhedged_ts = 0.0
                    ps.repair_failures = 0  # Reset failures
                    if not SIMPLE_LOG:
                        print(f"[Strategy] {target_stock} position is empty after sync, marking as EMPTY")
                    return None
                
                # FIX: Critical check - if stock doesn't exist in account snapshot, mark as ignored
                # This prevents trying to repair non-existent stocks (e.g., 2303)
                # Even if we have future position, if stock doesn't exist in account, we can't trade it
                if self.pos_mgr.account_monitor:
                    snap = self.pos_mgr.account_monitor.get_latest()
                    if snap:
                        # Check if stock actually exists in account snapshot
                        stock_exists_in_account = target_stock in snap.stock_positions
                        
                        # If stock doesn't exist in account (s_shares=0 and not in snapshot),
                        # mark as ignored to prevent blocking and repeated repair attempts
                        if s_shares == 0 and not stock_exists_in_account:
                            if not getattr(ps, "is_ignored", False):
                                ps.is_ignored = True
                                ps.repair_failures = 999
                                if SIMPLE_LOG:
                                    print(f"[錯誤] {target_stock}: 股票不存在於帳戶中（期貨={f_qty}），標記為忽略")
                                else:
                                    print(f"[Strategy] {target_stock} stock doesn't exist in account (f_qty={f_qty}), marking as IGNORED")
                            return None
                
                # REPAIR LOGIC:
                # 1. Check if we have PENDING orders in the missing leg. If so, CANCEL them first.
                # 2. Only if no pending orders, then FORCE CLOSE the existing leg.
                
                # Check pending orders for this stock
                # FIX: PENDING ORDER CHECK (SMART WAIT)
                # Instead of cancelling, check if pending quantity is sufficient
                # If we have enough active orders, just wait for them to fill.
                
                # Determine intended action first to check pending qty against it
                # Logic reused from below... but we need it here.
                
                is_force_close = (duration > UNHEDGED_REPAIR_DEADLINE)
                action = None
                qty = 0
                leg = "stock" # default
                target_action_enum = None # To query tracker
                
                # FIX: Log actual positions for debugging (always log for quantity issues)
                if not SIMPLE_LOG:
                    print(f"[Strategy] Repair {target_stock}: s_shares={s_shares}, f_qty={f_qty}, duration={duration:.1f}s")

                feq = int(ps.future_shares_equiv or 0) or FUTURE_SHARES_EQUIV
                
                # FIX: Debug log to check actual values
                if not SIMPLE_LOG or qty > 100:
                    print(f"[Debug] {target_stock} hedge calc: s_shares={s_shares}, f_qty={f_qty}, feq={feq}")
                
                if s_shares != 0 and f_qty == 0:
                     # Only Stock exists -> Close Stock
                     leg = "stock"
                     if s_shares > 0:
                         action = "FORCE_CLOSE_STOCK_SELL" # Sell long stock
                         target_action_enum = sj.constant.Action.Sell
                     else:
                         action = "FORCE_CLOSE_STOCK_BUY"  # Buy back short stock
                         target_action_enum = sj.constant.Action.Buy
                     qty = abs(s_shares) // STOCK_SHARES_PER_LOT

                elif s_shares == 0 and f_qty != 0:
                     # Only Future exists -> Close Future
                     leg = "future"
                     if f_qty > 0:
                         action = "FORCE_CLOSE_FUTURE_SELL" # Sell long future
                         target_action_enum = sj.constant.Action.Sell
                     else:
                         action = "FORCE_CLOSE_FUTURE_BUY"  # Buy back short future
                         target_action_enum = sj.constant.Action.Buy
                     qty = abs(f_qty)

                elif s_shares != 0 and f_qty != 0:
                     # Both sides have position, check if they're opposite (hedged) or same (wrong)
                     if (s_shares > 0 and f_qty > 0) or (s_shares < 0 and f_qty < 0):
                         # Both long or both short - wrong direction, close stock first
                         if s_shares > 0:
                             action = "FORCE_CLOSE_STOCK_SELL"  # Sell long stock
                             target_action_enum = sj.constant.Action.Sell
                         else:
                             action = "FORCE_CLOSE_STOCK_BUY"   # Buy back short stock
                             target_action_enum = sj.constant.Action.Buy
                         qty = abs(s_shares) // STOCK_SHARES_PER_LOT
                         leg = "stock"
                     else:
                         # Opposite signs - might be hedged, check balance
                         stock_equiv = abs(s_shares)
                         fut_equiv = abs(f_qty) * feq
                         imbalance = abs(stock_equiv - fut_equiv)
                         
                         if stock_equiv > fut_equiv:
                             # Need to close excess stock
                             if s_shares > 0:
                                 action = "FORCE_CLOSE_STOCK_SELL"  # Sell excess long stock
                                 target_action_enum = sj.constant.Action.Sell
                             else:
                                 action = "FORCE_CLOSE_STOCK_BUY"   # Buy back excess short stock
                                 target_action_enum = sj.constant.Action.Buy
                             qty = (imbalance + STOCK_SHARES_PER_LOT - 1) // STOCK_SHARES_PER_LOT  # Round up
                             leg = "stock"
                         else:
                             # Need to close excess future
                             if f_qty > 0:
                                 action = "FORCE_CLOSE_FUTURE_SELL"  # Sell excess long future
                                 target_action_enum = sj.constant.Action.Sell
                             else:
                                 action = "FORCE_CLOSE_FUTURE_BUY"   # Buy back excess short future
                                 target_action_enum = sj.constant.Action.Buy
                             qty = (imbalance + feq - 1) // feq  # Round up
                             leg = "future"
                         
                         if imbalance == 0:
                             if not SIMPLE_LOG:
                                 print(f"[Strategy] {target_stock} positions are balanced (stock={s_shares}, fut={f_qty}), skipping repair")
                             ps.last_repair_ts = now
                             return None
                         
                         if qty <= 0:
                             # Fallback to closing entire position if calculation fails
                             if stock_equiv > fut_equiv:
                                 qty = stock_equiv // STOCK_SHARES_PER_LOT
                             else:
                                 qty = abs(f_qty)
                             if not SIMPLE_LOG:
                                 print(f"[Strategy] {target_stock} qty calculation failed, using full position qty={qty}")

                # FIX: PENDING ORDER CHECK (SMART WAIT)
                # Before sending new order, check if we already have pending orders covering this qty
                if action and qty > 0 and target_action_enum:
                    pending_qty = self.pos_mgr.get_pending_repair_qty(target_stock, leg, str(target_action_enum))
                    
                    if pending_qty > 0:
                        if pending_qty >= qty:
                            # FIX: Check for STALE orders (Zombie check)
                            stale_oids = self.pos_mgr.get_stale_repair_orders(target_stock, leg, str(target_action_enum), timeout=45.0)
                            if stale_oids:
                                if SIMPLE_LOG:
                                    print(f"[Strategy] {target_stock} 發現 {len(stale_oids)} 筆逾時僵屍單，強制清除狀態並重試。")
                                else:
                                    print(f"[Strategy] Found {len(stale_oids)} stale orders for {target_stock}. Forcing fail.")
                                
                                for oid in stale_oids:
                                    self.pos_mgr.force_fail_order(oid, "Timeout/Stale")
                                return None # Yield to let state update, retry next tick

                            if SIMPLE_LOG:
                                print(f"[Strategy] {target_stock} {leg} 已有足夠掛單 (pending={pending_qty} >= needed={qty})，等待成交...")
                            else:
                                print(f"[Strategy] {target_stock} Pending {leg} order {pending_qty} covers needed {qty}. Waiting.")
                            ps.last_repair_ts = now # Update timestamp to avoid timeout logic triggering too soon
                            return None
                        else:
                            # Partial cover, reduce qty
                            original_qty = qty
                            qty = qty - pending_qty
                            if SIMPLE_LOG:
                                print(f"[Strategy] {target_stock} {leg} 部分掛單 (pending={pending_qty})，補足差額 {qty}")
                            else:
                                print(f"[Strategy] {target_stock} Pending {leg} order {pending_qty} insufficient. Adding {qty} (Total needed {original_qty}).")

                if action and qty > 0:
                    # FIX: Log detailed calculation for debugging quantity issues
                    if SIMPLE_LOG:
                        print(f"[缺腳] {target_stock} 自動沖銷: {action} qty={qty} (s_shares={s_shares}, f_qty={f_qty}, feq={feq})")
                    else:
                        print(f"[Strategy] {target_stock} UNHEDGED ({duration:.1f}s) -> {action} qty={qty} (s_shares={s_shares}, f_qty={f_qty})")
                    ps.last_repair_ts = now
                    should_release_lock = False  # Keep lock until processed
                    return TradeSignal({
                        "type": "REPAIR",
                        "sub_type": action,
                        "stock_code": target_stock,
                        "future_code": ps.fut_code or futures[0],
                        "qty": qty,
                        "is_force": is_force_close
                    })
                return None

            # pending states block new decisions
            if ps.state in (PositionState.PENDING_ENTRY, PositionState.PENDING_EXIT):
                return None

            f_code = ps.fut_code or futures[0]
            s_q, f_q = self.market_data.get_quotes(target_stock, f_code)
            if not s_q or not f_q:
                if code == DBG_WATCH_STOCK: print(f"[Trace] Limit: Missing Quotes (stk={bool(s_q)} fut={bool(f_q)})")
                return None

            s_bid = _get_bid(s_q); s_ask = _get_ask(s_q)
            f_bid = _get_bid(f_q); f_ask = _get_ask(f_q)

            latency = max(now - _get_ts(s_q), now - _get_ts(f_q))
            
            # Super Debug Watch (Early)
            if target_stock == DBG_WATCH_STOCK:
                 print(f"[Watch EARLY] {target_stock} Latency={latency:.1f}s Bid={s_bid} Ask={s_ask} FBid={f_bid} FAsk={f_ask}")

            if latency > MAX_DATA_DELAY_SEC:
                # if target_stock == DBG_WATCH_STOCK: print(f"[Watch REJECT] Latency {latency:.1f} > {MAX_DATA_DELAY_SEC}")
                # return None # DISABLE LATENCY CHECK FOR TEST
                pass 

            if s_bid <= 0 or s_ask <= 0 or f_bid <= 0 or f_ask <= 0:
                 if target_stock == DBG_WATCH_STOCK: print(f"[Trace] Limit: Price <= 0")
                 return None

            s_mid = (s_bid + s_ask) / 2.0
            f_mid = (f_bid + f_ask) / 2.0

            feq = int(ps.future_shares_equiv or 0) or FUTURE_SHARES_EQUIV
            basis = (f_mid * feq * FUTURE_QTY) - (s_mid * STOCK_SHARES_PER_LOT * STOCK_QTY)

            self._update_ewma(ps, basis, now)
            if (not ps.initialized) or ps.basis_std <= 0 or ps.sample_count < MIN_SAMPLES:
                if code == DBG_WATCH_STOCK: 
                    print(f"[Trace] Limit: Stats Not Ready (Init={ps.initialized} Std={ps.basis_std:.2f} Samples={ps.sample_count}/{MIN_SAMPLES})")
                return None

            z_score = (basis - ps.basis_mean) / ps.basis_std
            
            # --- Heartbeat Logic ---
            self.latest_z_scores[target_stock] = z_score
            if now - self.last_heartbeat_ts > 30.0:
                sorted_z = sorted(self.latest_z_scores.items(), key=lambda x: abs(x[1]), reverse=True)[:3]
                top_str = ", ".join([f"{c}:Z={z:.2f}" for c, z in sorted_z])
                if SIMPLE_LOG:
                    print(f"[系統] 監控中... 最佳機會: {top_str}")
                else:
                    print(f"[Heartbeat] Scanning. Top Z: {top_str}")
                self.last_heartbeat_ts = now

            # Debug Watch
            if target_stock == DBG_WATCH_STOCK:
                print(f"[Watch] {target_stock} Z={z_score:.2f} Mean={ps.basis_mean:.0f} Std={ps.basis_std:.0f} Samples={ps.sample_count} State={ps.state.name} Basis={basis:.0f}")

            # ---- ENTRY ----
            # Temporary Debug for high Z-score silence
            if z_score > 20.0 and SIMPLE_LOG:
                 # Check if state is blocking entry
                 if ps.state != PositionState.EMPTY:
                      if now - self.last_risk_warning_ts > 10:
                          print(f"[觀察] {target_stock} Z={z_score:.2f} 但狀態非空 ({ps.state.name})")
                          self.last_risk_warning_ts = now

            if ps.state == PositionState.EMPTY:
                if z_score >= Z_ENTRY:
                    if not self.check_risk(is_open=True, current_stock=target_stock):
                        # check_risk already prints reason
                        return None

                    s_ask_vol = _get_ask_vol(s_q)
                    f_bid_vol = _get_bid_vol(f_q)
                    if s_ask_vol < STOCK_QTY * 3 or f_bid_vol < FUTURE_QTY * 3:
                        if SIMPLE_LOG:
                            print(f"[觀察] {target_stock} 價差達標 (Z={z_score:.2f}) 但流動性不足 (S:{s_ask_vol}, F:{f_bid_vol})")
                        return None

                    stock_notional = s_ask * STOCK_SHARES_PER_LOT * STOCK_QTY
                    if not self.capital_mgr.check_available(stock_notional):
                        if SIMPLE_LOG:
                            print(f"[觀察] {target_stock} 價差達標 (Z={z_score:.2f}) 但資金不足")
                        return None

                    future_notional = f_bid * feq * FUTURE_QTY
                    spread_value = future_notional - stock_notional
                    cost = self.calculate_cost(s_ask, f_bid, feq)

                    s_slip = SLIPPAGE_SCALE * (STOCK_QTY / max(1, s_ask_vol)) * 5.0
                    f_slip = SLIPPAGE_SCALE * (FUTURE_QTY / max(1, f_bid_vol)) * 5.0
                    est_risk_cost = (s_slip * s_ask * 0.0005) + (f_slip * f_bid * 0.0002)

                    net = spread_value - cost - est_risk_cost
                    if net > ENTRY_THRESHOLD:
                        self._increment_risk_counter()
                        self.pos_mgr.set_future_pair(target_stock, f_code, future_shares_equiv=feq)
                        self.pos_mgr.set_state(target_stock, PositionState.PENDING_ENTRY)
                        self.pos_mgr.set_opened_by_session(target_stock, True)
                        if SIMPLE_LOG:
                            print(f"[送單] {target_stock}/{f_code} 買{STOCK_QTY}張 賣{FUTURE_QTY}口 | 股價:{s_ask:.2f} 期貨:{f_bid:.2f} (市場報價) | 預計利潤: {net:.0f}元")
                        else:
                            print(f"[Strategy] OPEN {target_stock} Z={z_score:.2f} Mean={ps.basis_mean:.2f} Std={ps.basis_std:.2f} Net={net:.0f} Basis={basis:.2f}")
                        
                        should_release_lock = False  # Keep lock until processed
                        return TradeSignal({
                            "type": "OPEN",
                            "stock_code": target_stock,
                            "future_code": f_code,
                            "stock_px": s_ask,
                            "fut_px": f_bid,
                            "net": net,
                        })
                    else:
                        # Only log rejection in non-simple mode
                        if not SIMPLE_LOG:
                            print(f"[Strategy] REJECT {target_stock}: Net Profit {net:.0f} < Threshold {ENTRY_THRESHOLD} (Z={z_score:.2f})")

            # ---- EXIT ----
            elif ps.state == PositionState.HOLDING:
                # NEW: Time Check (13:25 Stop)
                # Stock market closed/auction, cannot execute pair exit safely.
                if datetime.datetime.now().time() >= datetime.time(13, 25):
                    return None

                # NEW: Session Management Check
                if not ps.opened_by_this_session and not MANAGE_EXISTING_POSITIONS:
                    return None

                # FIX: Check for Stale CLOSE orders (Zombie Check)
                exit_action = "Action.Sell" if ps.pos_stock_shares > 0 else "Action.Buy"
                stale_oids = self.pos_mgr.get_stale_repair_orders(target_stock, "stock", exit_action, timeout=45.0)
                if stale_oids:
                     if SIMPLE_LOG:
                         print(f"[Strategy] {target_stock} 發現逾時平倉單，強制清除狀態重試。")
                     else:
                         print(f"[Strategy] Found stale EXIT orders for {target_stock}. Forcing fail.")
                     for oid in stale_oids:
                         self.pos_mgr.force_fail_order(oid, "Timeout/Stale - Exit")

                is_timeout = (ps.open_ts > 0 and (now - ps.open_ts) > MAX_HOLDING_SEC)
                if z_score <= Z_EXIT or is_timeout:
                    s_bid_vol = _get_bid_vol(s_q)
                    f_ask_vol = _get_ask_vol(f_q)
                    if s_bid_vol < 1 or f_ask_vol < 1:
                        return None

                    stock_notional = s_bid * STOCK_SHARES_PER_LOT * STOCK_QTY
                    future_notional = f_ask * feq * FUTURE_QTY
                    current_spread = future_notional - stock_notional

                    reason = "TIMEOUT" if is_timeout else "Z-SCORE"
                    self._increment_risk_counter()
                    self.pos_mgr.set_state(target_stock, PositionState.PENDING_EXIT)
                    if SIMPLE_LOG:
                        print(f"[送單] {target_stock}/{f_code} 平倉 | 股價:{s_bid:.2f} 期貨:{f_ask:.2f} (市場報價) | 預計利潤: {current_spread:.0f}元 ({reason})")
                    else:
                        print(f"[Strategy] CLOSE {target_stock} ({reason}) Z={z_score:.2f} Net={current_spread:.0f}")

                    should_release_lock = False  # Keep lock until processed
                    return TradeSignal({
                        "type": "CLOSE",
                        "stock_code": target_stock,
                        "future_code": f_code,
                        "stock_px": s_bid,
                        "fut_px": f_ask,
                        "net": current_spread,
                    })

            return None
        finally:
            if should_release_lock:
                self.pos_mgr.release_lock(target_stock)


# =========================================================
# 10) ExecutionEngine  (✅ 修正：close 成功 -> EMPTY)
# =========================================================
class ExecutionEngine:
    def __init__(self, market_data: 'MarketData', pos_mgr: PositionManager, order_tracker: 'OrderTracker', throttler: Optional['ApiThrottler'] = None):
        self.market_data = market_data
        self.pos_mgr = pos_mgr
        self.tracker = order_tracker
        self.throttler = throttler
        self._future_map: Dict[str, Any] = {}

    def set_future_map(self, future_map: Dict[str, Any]):
        self._future_map = dict(future_map or {})

    def _resolve_future_contract(self, api: sj.Shioaji, future_code: str):
        code = str(future_code or "")
        if not code:
            raise KeyError("future_code empty")

        if code in self._future_map:
            return self._future_map[code]

        # fallback scan
        for category in api.Contracts.Futures:
            try:
                iter(category)
            except TypeError:
                continue
            for c in category:
                if isinstance(c, tuple):
                    c = c[1]
                if getattr(c, "code", None) == code:
                    return c

        # last resort
        return api.Contracts.Futures[code]

    def _tick_size_by_price(self, px: float) -> float:
        if px < 10: return 0.01
        if px < 50: return 0.05
        if px < 100: return 0.1
        if px < 500: return 0.5
        if px < 1000: return 1.0
        return 5.0

    def _round_to_tick_by_its_own_tier(self, px: float, side: str) -> float:
        if px <= 0:
            return px
        tick = self._tick_size_by_price(px)
        dpx = Decimal(str(px))
        dt = Decimal(str(tick))
        q = dpx / dt
        q2 = q.to_integral_value(rounding=ROUND_CEILING if side == "buy" else ROUND_FLOOR)
        out = float(q2 * dt)
        return max(out, tick)

    def _step_one_tick_strict(self, px: float, side: str) -> float:
        if px <= 0:
            return px
        tick_now = self._tick_size_by_price(px)
        if side == "buy":
            px2 = px + tick_now
            return self._round_to_tick_by_its_own_tier(px2, "buy")
        else:
            px2 = px - tick_now
            if px2 <= 0:
                px2 = tick_now
            return self._round_to_tick_by_its_own_tier(px2, "sell")

    def _mk_mktable_price_strict(self, side: str, ref_px: float, protect_ticks: int, limit_up: float = 0.0, limit_down: float = 0.0) -> float:
        if ref_px <= 0:
            return ref_px
        px = self._round_to_tick_by_its_own_tier(ref_px, "buy" if side == "buy" else "sell")
        for _ in range(int(max(0, protect_ticks))):
            px = self._step_one_tick_strict(px, side)
        
        px = self._round_to_tick_by_its_own_tier(px, side)

        # Clamp to limits if provided
        if limit_up > 0 and px > limit_up:
            px = limit_up
        if limit_down > 0 and px < limit_down:
            px = limit_down

        return px

    def _place_stock_order(self, api: sj.Shioaji, stock_code: str, action, price: float, qty_lot: int, price_type=None, order_type=None):
        # Try to get contract with fallback logic (try TSE first, then OTC)
        contract_s = None
        try:
            # First try direct access (Shioaji may auto-select exchange)
            contract_s = api.Contracts.Stocks[stock_code]
        except (KeyError, AttributeError):
            pass
        
        # If direct access failed, try TSE and OTC explicitly
        if contract_s is None:
            for ex_name in ("TSE", "OTC"):
                try:
                    bucket = getattr(api.Contracts.Stocks, ex_name, None)
                    if bucket is not None:
                        # Try code as key
                        if stock_code in bucket:
                            contract_s = bucket[stock_code]
                            break
                        # Try scanning contracts
                        for c in bucket:
                            if hasattr(c, "code") and str(getattr(c, "code", "")) == stock_code:
                                contract_s = c
                                break
                        if contract_s:
                            break
                except Exception:
                    continue
        
        if contract_s is None:
            print(f"[Execution] Stock {stock_code} not found in Contracts (tried TSE and OTC)")
            raise RuntimeError(f"Stock {stock_code} contract not available (may not exist in simulation)")
        
        if price_type is None:
            price_type = sj.constant.StockPriceType.LMT
        
        if order_type is None:
            order_type = sj.constant.OrderType.ROD if hasattr(sj.constant, "OrderType") else "ROD"

        # Set order_cond and daytrade_short for proper order handling
        # For closing short positions (negative shares), use Cash and daytrade_short=False
        # This matches the pattern used in clear_all_positions.py
        order_cond = sj.constant.StockOrderCond.Cash if hasattr(sj.constant, "StockOrderCond") else None
        daytrade_short = False

        order_s = api.Order(
            price=float(price),
            quantity=int(qty_lot),
            action=action,
            price_type=price_type,
            order_type=order_type,
            order_lot=sj.constant.StockOrderLot.Common if hasattr(sj.constant, "StockOrderLot") else "Common",
            account=api.stock_account,
        )
        
        # Add order_cond and daytrade_short if available
        if order_cond is not None:
            order_s.order_cond = order_cond
        if hasattr(order_s, 'daytrade_short'):
            order_s.daytrade_short = daytrade_short
        
        return api.place_order(contract_s, order_s)

    def _place_future_order(self, api: sj.Shioaji, future_code: str, action, price: float, qty: int, price_type=None, order_type=None):
        contract_f = self._resolve_future_contract(api, future_code)

        octype = sj.constant.FuturesOCType.Auto if hasattr(sj.constant, "FuturesOCType") else "Auto"
        if price_type is None:
            price_type = sj.constant.FuturesPriceType.LMT if hasattr(sj.constant, "FuturesPriceType") else sj.constant.StockPriceType.LMT
        
        if order_type is None:
            order_type = sj.constant.FuturesOrderType.ROD if hasattr(sj.constant, "FuturesOrderType") else (
                sj.constant.OrderType.ROD if hasattr(sj.constant, "OrderType") else "ROD"
            )

        order_f = api.Order(
            action=action,
            price=float(price),
            quantity=int(qty),
            price_type=price_type,
            order_type=order_type,
            octype=octype,
            account=api.futopt_account,
        )
        return api.place_order(contract_f, order_f)

    def on_signal(self, sig: Dict[str, Any], api: sj.Shioaji):
        typ = str(sig.get("type") or "")
        stock_code = str(sig.get("stock_code") or "")
        future_code = str(sig.get("future_code") or "")

        if typ == "CANCEL":
            stock_code = sig.get("stock_code")
            leg = sig.get("leg")
            action = sig.get("action") # Enum
            
            if not SIMPLE_LOG:
                print(f"[Execution] Requesting CANCEL for {stock_code} {leg} {action}")
            
            # Get pending IDs
            pending_oids = self.pos_mgr.get_pending_repair_orders(stock_code, leg, str(action))
            
            if not pending_oids:
                if not SIMPLE_LOG:
                    print(f"[Execution] No pending orders found in tracker for CANCEL request.")
                return

            # Force update status to ensure api.list_trades is up to date
            try:
                # api.update_status(api.stock_account)
                # api.update_status(api.futures_account)
                # Note: update_status might be rate limited or slow. Rely on cache if possible.
                pass
            except: pass

            # Find orders in API list
            targets = set(pending_oids)
            cancelled_count = 0
            
            # Helper to scan
            def scan_and_cancel(trades_list):
                c = 0
                for t in trades_list:
                    if t.order.id in targets:
                        # Check status if possible, but cancel_order handles checks too
                        if t.status.status in [sj.constant.Status.Submitted, sj.constant.Status.PartFilled, sj.constant.Status.PendingSubmit]:
                            try:
                                api.cancel_order(t)
                                c += 1
                                if not SIMPLE_LOG:
                                    print(f"  -> Cancelled {t.order.id}")
                            except Exception as e:
                                if not SIMPLE_LOG:
                                    print(f"  -> Cancel failed {t.order.id}: {e}")
                return c

            cancelled_count += scan_and_cancel(api.list_trades())
            
            if SIMPLE_LOG:
                print(f"[Execution] 取消了 {cancelled_count} 筆過剩掛單。")
            else:
                print(f"[Execution] Cancelled {cancelled_count} pending orders for {stock_code}.")
            return

        # ----------------------------
        # REPAIR
        # ----------------------------
        if typ == "REPAIR":
            sub_type = str(sig.get("sub_type") or "")
            qty = int(sig.get("qty") or 0)
            is_force = bool(sig.get("is_force", False)) or ("FORCE" in sub_type)

            if qty <= 0:
                self.pos_mgr.set_state(stock_code, PositionState.REPAIRING)
                self.pos_mgr.set_inflight(stock_code, False)
                return

            if not SIMPLE_LOG:
                print(f"[Execution] Executing Auto-Repair: {sub_type} {stock_code} qty={qty} (is_force={is_force})")
            self.pos_mgr.set_state(stock_code, PositionState.REPAIRING)

            try:
                ps = self.pos_mgr.get_pair(stock_code)

                if "STOCK" in sub_type:
                    quote = self.market_data.get_stock(stock_code)
                    
                    # Get contract for limits and fallback price
                    c_stk = None
                    limit_up, limit_down = 0.0, 0.0
                    fallback_px = 0.0
                    try:
                        c_stk = api.Contracts.Stocks[stock_code]
                        limit_up = float(getattr(c_stk, "limit_up", 0.0) or 0.0)
                        limit_down = float(getattr(c_stk, "limit_down", 0.0) or 0.0)
                        fallback_px = float(getattr(c_stk, "reference_price", 0.0) or getattr(c_stk, "close", 0.0) or 0.0)
                    except Exception:
                        pass

                    # FIX: Check inventory before selling stock
                    pos_shares = getattr(ps, "pos_stock_shares", 0) if ps else 0
                    
                    # FIX: Determine buy/sell direction ONLY from sub_type (must contain BUY or SELL)
                    # sub_type should be like "FORCE_CLOSE_STOCK_SELL" or "FORCE_CLOSE_STOCK_BUY"
                    if "BUY" in sub_type:
                        # Explicit BUY in sub_type (buy back short stock)
                        action = sj.constant.Action.Buy
                        ref_px = _get_ask(quote) if quote else 0.0
                        if ref_px <= 0: ref_px = fallback_px
                        side = "buy"
                    elif "SELL" in sub_type:
                        # FIX: Only sell if we have inventory, and check if qty doesn't exceed inventory
                        if pos_shares <= 0:
                            print(f"[錯誤] {stock_code}: 無法賣出，庫存={pos_shares} <= 0")
                            # Mark as ignored if inventory is zero and we're trying to sell
                            if ps:
                                ps.repair_failures += 1
                                if ps.repair_failures >= 3:
                                    ps.is_ignored = True
                                    print(f"[錯誤] {stock_code}: 連續失敗 {ps.repair_failures} 次，標記為忽略")
                            self.pos_mgr.set_inflight(stock_code, False)
                            return
                        # FIX: qty is in lots (張), convert to shares and check against inventory
                        # FIX: For FORCE_CLOSE, use actual inventory, not calculated qty
                        if "FORCE" in sub_type:
                            # Force close should use all available inventory
                            qty = pos_shares // STOCK_SHARES_PER_LOT
                            if qty <= 0:
                                print(f"[錯誤] {stock_code}: 庫存不足，餘股數 {pos_shares} 股（不足1張）")
                                if ps:
                                    ps.repair_failures += 1
                                    if ps.repair_failures >= 3:
                                        ps.is_ignored = True
                                        print(f"[錯誤] {stock_code}: 連續失敗 {ps.repair_failures} 次，標記為忽略")
                                self.pos_mgr.set_inflight(stock_code, False)
                                return
                        else:
                            # Repair: check if calculated qty exceeds inventory
                            qty_shares = qty * STOCK_SHARES_PER_LOT
                            if qty_shares > pos_shares:
                                # Adjust qty to available inventory
                                qty = pos_shares // STOCK_SHARES_PER_LOT
                                if qty <= 0:
                                    print(f"[錯誤] {stock_code}: 庫存不足，餘股數 {pos_shares} 股，需要 {qty_shares} 股")
                                    if ps:
                                        ps.repair_failures += 1
                                        if ps.repair_failures >= 3:
                                            ps.is_ignored = True
                                            print(f"[錯誤] {stock_code}: 連續失敗 {ps.repair_failures} 次，標記為忽略")
                                    self.pos_mgr.set_inflight(stock_code, False)
                                    return
                                print(f"[警告] {stock_code}: 調整賣出數量，庫存={pos_shares}股，原需求={qty_shares}股，調整為={qty}張")
                        action = sj.constant.Action.Sell
                        ref_px = _get_bid(quote) if quote else 0.0
                        if ref_px <= 0: ref_px = fallback_px
                        side = "sell"
                    else:
                        # ERROR: sub_type must contain BUY or SELL
                        print(f"[錯誤] {stock_code}: sub_type '{sub_type}' 不包含 BUY 或 SELL，無法判斷方向")
                        self.pos_mgr.set_inflight(stock_code, False)
                        if ps:
                            ps.repair_failures += 1
                            if ps.repair_failures >= 3:
                                ps.is_ignored = True
                                print(f"[錯誤] {stock_code}: 連續失敗 {ps.repair_failures} 次，標記為忽略")
                        return

                    px = self._mk_mktable_price_strict(
                        side, ref_px, 
                        protect_ticks=(FORCE_CLOSE_TICKS if is_force else REPAIR_TICKS),
                        limit_up=limit_up, limit_down=limit_down
                    ) if ref_px > 0 else 0.0

                    # Auto-Repair: FORCE MKT (simulated via MKT Price Type + IOC)
                    # This aligns with user's clear_all_positions logic: Send aggressive order, if not filled, next loop retries.
                    if px > 0 or is_force:
                        if is_force:
                            # Force: MKT ROD
                            run_px = 0
                            run_type = sj.constant.StockPriceType.MKT
                            if not SIMPLE_LOG:
                                print(f"[Execution] Placing STOCK order (FORCE ROD): {stock_code} {action} {qty} @ MKT")
                            order_type_use = sj.constant.OrderType.ROD
                        else:
                            run_px = px
                            run_type = sj.constant.StockPriceType.LMT
                            if not SIMPLE_LOG:
                                print(f"[Execution] Placing STOCK order (IOC): {stock_code} {action} {qty} @ {run_px}")
                            order_type_use = sj.constant.OrderType.IOC

                        try:
                            trade_fix = self._place_stock_order(api, stock_code, action, run_px, qty, 
                                                                price_type=run_type,
                                                                order_type=order_type_use)
                            oid = str(trade_fix.order.id)
                            try: self.tracker.record(oid, stock_code, future_code, "repair", "stock", action, run_px, qty)
                            except Exception: pass
                            try: self.pos_mgr.register_order(oid, stock_code, "repair", "stock")
                            except Exception: pass
                            # Reset repair failures on successful order submission
                            if ps:
                                ps.repair_failures = 0
                        except (KeyError, RuntimeError) as e:
                            error_str = str(e)
                            print(f"[Execution] Auto-Repair STOCK {stock_code} failed: {e}")
                            print(f"[Execution] Stock {stock_code} may not exist in simulation environment. Skipping.")
                            # Mark as ignored to prevent retry loops
                            if ps:
                                ps.repair_failures = 999
                                ps.is_ignored = True
                                print(f"[Execution] Stock {stock_code} marked as IGNORED due to contract error.")
                        except Exception as e:
                            # Catch any other exception during order placement
                            error_str = str(e)
                            print(f"[Execution] Auto-Repair STOCK {stock_code} failed with exception: {e}")
                            if ps:
                                # If error contains "無此" or similar, mark as ignored
                                if "無此" in error_str or "not found" in error_str.lower() or "not available" in error_str.lower():
                                    ps.repair_failures = 999
                                    ps.is_ignored = True
                                    print(f"[Execution] Stock {stock_code} marked as IGNORED due to contract error.")
                    else:
                        print(f"[Execution] Auto-Repair STOCK px invalid (ref={ref_px})")

                elif "FUTURE" in sub_type:
                    quote = self.market_data.get_future(future_code)
                    
                    # Get contract for limits and fallback price
                    c_fut = None
                    limit_up, limit_down = 0.0, 0.0
                    fallback_px = 0.0
                    try:
                        c_fut = self._resolve_future_contract(api, future_code)
                        limit_up = float(getattr(c_fut, "limit_up", 0.0) or 0.0)
                        limit_down = float(getattr(c_fut, "limit_down", 0.0) or 0.0)
                        fallback_px = float(getattr(c_fut, "reference_price", 0.0) or getattr(c_fut, "close", 0.0) or 0.0)
                    except Exception:
                        pass

                    # FIX: Determine buy/sell direction from sub_type (should contain BUY or SELL)
                    # sub_type format: "FORCE_CLOSE_FUTURE_SELL" or "FORCE_CLOSE_FUTURE_BUY"
                    if "BUY" in sub_type:
                        # Explicit BUY in sub_type (buy back short future)
                        action = sj.constant.Action.Buy
                        ref_px = _get_ask(quote) if quote else 0.0
                        if ref_px <= 0: ref_px = fallback_px
                        side = "buy"
                    elif "SELL" in sub_type:
                        # Explicit SELL in sub_type (sell long future)
                        action = sj.constant.Action.Sell
                        ref_px = _get_bid(quote) if quote else 0.0
                        if ref_px <= 0: ref_px = fallback_px
                        side = "sell"
                    else:
                        # Fallback: determine from position (should not happen with correct sub_type)
                        pos_fut_qty = getattr(ps, "pos_fut_qty", 0) if ps else 0
                        if pos_fut_qty > 0:
                            action = sj.constant.Action.Sell
                            ref_px = _get_bid(quote) if quote else 0.0
                            if ref_px <= 0: ref_px = fallback_px
                            side = "sell"
                        else:
                            action = sj.constant.Action.Buy
                            ref_px = _get_ask(quote) if quote else 0.0
                            if ref_px <= 0: ref_px = fallback_px
                            side = "buy"

                    px = self._mk_mktable_price_strict(
                        side, ref_px, 
                        protect_ticks=(FORCE_CLOSE_TICKS if is_force else REPAIR_TICKS),
                        limit_up=limit_up, limit_down=limit_down
                    ) if ref_px > 0 else 0.0

                    # Auto-Repair: FORCE MKT (simulated via MKT Price Type + IOC)
                    if px > 0 or is_force:
                        if is_force:
                            # Future MKT + ROD (Stay in book until filled or cancelled by next repair loop)
                            run_px = 0
                            run_type = sj.constant.FuturesPriceType.MKT if hasattr(sj.constant, "FuturesPriceType") else sj.constant.StockPriceType.MKT
                            if not SIMPLE_LOG:
                                print(f"[Execution] Placing FUTURE order (FORCE ROD): {future_code} {action} {qty} @ MKT")
                            
                            # Determine ROD constant
                            rod_const = "ROD"
                            if hasattr(sj.constant, "FuturesOrderType") and hasattr(sj.constant.FuturesOrderType, "ROD"):
                                 rod_const = sj.constant.FuturesOrderType.ROD
                            elif hasattr(sj.constant, "OrderType") and hasattr(sj.constant.OrderType, "ROD"):
                                 rod_const = sj.constant.OrderType.ROD
                            
                            trade_fix = self._place_future_order(api, future_code, action, run_px, qty, 
                                                                 price_type=run_type,
                                                                 order_type=rod_const)
                        else:
                            run_px = px
                            run_type = sj.constant.FuturesPriceType.LMT if hasattr(sj.constant, "FuturesPriceType") else sj.constant.StockPriceType.LMT
                            print(f"[Execution] Placing FUTURE order (IOC): {future_code} {action} {qty} @ {run_px}")
 
                            # Determine IOC constant safely
                            ioc_const = "IOC"
                            if hasattr(sj.constant, "FuturesOrderType") and hasattr(sj.constant.FuturesOrderType, "IOC"):
                                 ioc_const = sj.constant.FuturesOrderType.IOC
                            elif hasattr(sj.constant, "OrderType") and hasattr(sj.constant.OrderType, "IOC"):
                                 ioc_const = sj.constant.OrderType.IOC
                            
                            trade_fix = self._place_future_order(api, future_code, action, run_px, qty, 
                                                                 price_type=run_type,
                                                                 order_type=ioc_const)
                        
                        oid = str(trade_fix.order.id)
                        try: self.tracker.record(oid, stock_code, future_code, "repair", "future", action, run_px, qty)
                        except Exception: pass
                        try: self.pos_mgr.register_order(oid, stock_code, "repair", "future")
                        except Exception: pass
                    else:
                        print(f"[Execution] Auto-Repair FUTURE px invalid (ref={ref_px})")

            except Exception as e:
                print(f"[Execution] Auto-Repair failed: {e}")

            self.pos_mgr.set_inflight(stock_code, False)
            return

        # ----------------------------
        # OPEN / CLOSE
        # ----------------------------
        self.pos_mgr.set_inflight(stock_code, True)
        is_open = (typ == "OPEN")
        phase = "open" if is_open else "close"

        try:
            init_stock_px = float(sig.get("stock_px") or 0.0)
            init_fut_px = float(sig.get("fut_px") or 0.0)
            net = float(sig.get("net") or 0.0)

            # --- Validation: Contract Size ---
            # Try to get dynamic unit from contract
            s_unit = 1000
            try:
                c_stk_obj = api.Contracts.Stocks[stock_code]
                if c_stk_obj:
                    # Shioaji 'unit' is shares per lot
                    s_unit = int(getattr(c_stk_obj, "unit", 1000))
            except: pass
            
            calc_stock_shares = s_unit * STOCK_QTY
            calc_future_shares = FUTURE_SHARES_EQUIV * FUTURE_QTY
            
            if calc_stock_shares != calc_future_shares:
                print(f"[REJECT] Hedge Mismatch! Stock {stock_code} ({calc_stock_shares} shares) != Future ({calc_future_shares} shares). Check 'unit' or quantities.")
                # Mark as failed to reset state
                self.pos_mgr.set_state(stock_code, PositionState.REPAIRING) # Or keep EMPTY? repair ensures cleanup check
                self.pos_mgr.set_inflight(stock_code, False)
                return
            # ---------------------------------

            self.pos_mgr.prepare_phase(stock_code, phase, target_stock_lots=STOCK_QTY, target_fut_qty=FUTURE_QTY, last_signal_net=net)

            stock_action = sj.constant.Action.Buy if is_open else sj.constant.Action.Sell
            future_action = sj.constant.Action.Sell if is_open else sj.constant.Action.Buy

            stock_px_type = sj.constant.StockPriceType.LMT
            stock_order_type = sj.constant.OrderType.ROD # Default to ROD for LMT
            fut_px_type = sj.constant.FuturesPriceType.LMT if hasattr(sj.constant, "FuturesPriceType") else sj.constant.StockPriceType.LMT
            fut_order_type = sj.constant.OrderType.ROD # Default to ROD for LMT

            # Determine IOC constant safely for Futures
            fut_ioc_const = "IOC"
            if hasattr(sj.constant, "FuturesOrderType") and hasattr(sj.constant.FuturesOrderType, "IOC"):
                 fut_ioc_const = sj.constant.FuturesOrderType.IOC
            elif hasattr(sj.constant, "OrderType") and hasattr(sj.constant.OrderType, "IOC"):
                 fut_ioc_const = sj.constant.OrderType.IOC
            
            # Switch to Market Order if Enabled
            # IMPORTANT: Use ROD (Rest of Day) instead of IOC for Market Orders
            # IOC orders get cancelled immediately if not filled, causing unhedged positions
            # ROD allows orders to stay in the market until filled, ensuring both legs can complete
            if USE_MARKET_ORDER and is_open:
                if not SIMPLE_LOG:
                    print(f"[Execution] Using MARKET ORDER (MKT+ROD) for ENTRY (Reduce Legging Risk)")
                stock_px = 0
                stock_px_type = sj.constant.StockPriceType.MKT
                stock_order_type = sj.constant.OrderType.ROD  # Changed from IOC to ROD
                
                fut_px = 0
                fut_px_type = sj.constant.FuturesPriceType.MKT if hasattr(sj.constant, "FuturesPriceType") else sj.constant.StockPriceType.MKT
                # Use ROD for futures as well
                if hasattr(sj.constant, "FuturesOrderType") and hasattr(sj.constant.FuturesOrderType, "ROD"):
                    fut_order_type = sj.constant.FuturesOrderType.ROD
                elif hasattr(sj.constant, "OrderType") and hasattr(sj.constant.OrderType, "ROD"):
                    fut_order_type = sj.constant.OrderType.ROD
                else:
                    fut_order_type = "ROD"
            else:
                # Use LIMIT Order (Aggressive)
                # Note: Currently using basic ROD/LMT but priced aggressively. 
                # Ideally, for Arb, we want IOC even for Limit to avoid hanging.
                # Changing to IOC for LMT as well to match previous behavior logic.
                stock_order_type = sj.constant.OrderType.IOC
                fut_order_type = fut_ioc_const

                if is_open:
                    stock_px = self._mk_mktable_price_strict("buy", init_stock_px, STOCK_BUY_TICKS + 1)
                    fut_px = self._mk_mktable_price_strict("sell", init_fut_px, FUT_SELL_TICKS + 1)
                else:
                    stock_px = self._mk_mktable_price_strict("sell", init_stock_px, STOCK_SELL_TICKS + 1)
                    fut_px = self._mk_mktable_price_strict("buy", init_fut_px, FUT_BUY_TICKS + 1)

            # send dual-leg concurrently
            with concurrent.futures.ThreadPoolExecutor(max_workers=2) as ex:
                fs = ex.submit(self._place_stock_order, api, stock_code, stock_action, stock_px, 
                               STOCK_QTY, price_type=stock_px_type, order_type=stock_order_type)
                
                ff = ex.submit(self._place_future_order, api, future_code, future_action, fut_px, 
                               FUTURE_QTY, price_type=fut_px_type, order_type=fut_order_type)
                
                trade_s = fs.result()
                trade_f = ff.result()

            # record
            try:
                self.tracker.record(str(trade_s.order.id), stock_code, future_code, phase, "stock", stock_action, stock_px, STOCK_QTY)
                self.tracker.record(str(trade_f.order.id), stock_code, future_code, phase, "future", future_action, fut_px, FUTURE_QTY)
            except Exception:
                pass

            try:
                self.pos_mgr.register_order(str(trade_s.order.id), stock_code, phase, "stock")
                self.pos_mgr.register_order(str(trade_f.order.id), stock_code, phase, "future")
            except Exception:
                pass

            if SIMPLE_LOG:
                pass  # Already logged in StrategyEngine
            else:
                print(f"[{phase.upper()}] orders sent: stock={trade_s.order.id}, future={trade_f.order.id}")

        except Exception as e:
            print(f"[{phase.upper()}] send/record failed: {e}")
            self.pos_mgr.set_state(stock_code, PositionState.REPAIRING)
            ps = self.pos_mgr.get_pair(stock_code)
            if ps and ps.unhedged_ts <= 0:
                ps.unhedged_ts = time.time()
            self.pos_mgr.set_inflight(stock_code, False)
            return

        sync = self.pos_mgr.get_phase_sync(stock_code, phase)
        if not sync:
            self.pos_mgr.set_state(stock_code, PositionState.REPAIRING)
            ps = self.pos_mgr.get_pair(stock_code)
            if ps and ps.unhedged_ts <= 0:
                ps.unhedged_ts = time.time()
            self.pos_mgr.set_inflight(stock_code, False)
            return

        ok = sync.done.wait(timeout=HEDGE_TIMEOUT_SEC)
        failed = sync.failed.is_set()

        if failed or (not ok):
            # FIX: Timeout or Failed -> Cancel pending orders first!
            # This prevents late fills from creating new unhedged positions while we are repairing.
            if not SIMPLE_LOG:
                print(f"[{phase.upper()}] Timed out (>{HEDGE_TIMEOUT_SEC}s). Cancelling pending orders...")
            
            try:
                if trade_s:
                    api.cancel_order(trade_s)
                    if not SIMPLE_LOG: print(f"[Execution] Cancel stock order {trade_s.order.id}")
            except Exception: pass
            
            try:
                if trade_f:
                    api.cancel_order(trade_f)
                    if not SIMPLE_LOG: print(f"[Execution] Cancel future order {trade_f.order.id}")
            except Exception: pass
            
            # Wait for cancellation to propagate
            time.sleep(1.0)

            # Check if one leg filled but the other didn't (unhedged situation)
            ps = self.pos_mgr.get_pair(stock_code)
            if ps:
                if phase == "open":
                    stock_filled = ps.open_stock.status == "Filled"
                    future_filled = ps.open_future.status == "Filled"
                else:
                    stock_filled = ps.close_stock.status == "Filled"
                    future_filled = ps.close_future.status == "Filled"
                
                # If only one leg filled, we have an unhedged position
                if (stock_filled and not future_filled) or (future_filled and not stock_filled):
                    if SIMPLE_LOG:
                        missing_leg = "期貨" if not future_filled else "股票"
                        filled_leg = "股票" if stock_filled else "期貨"
                        print(f"[缺腳] {stock_code} {filled_leg}已成交，{missing_leg}未成交，開始沖銷")
                    else:
                        print(f"[{phase.upper()}] UNHEDGED: Stock={'Filled' if stock_filled else 'Pending'}, Future={'Filled' if future_filled else 'Pending'} -> set REPAIRING")
                    # Cancel the unfilled order if it's still pending
                    # The repair logic will handle completing the missing leg
                    # FIX: Update position state immediately to reflect the filled leg
                    # This ensures sync_from_snapshot can detect the imbalance correctly
                else:
                    if not SIMPLE_LOG:
                        print(f"[{phase.upper()}] timeout/failed (both legs pending) -> set REPAIRING")
            
            self.pos_mgr.set_state(stock_code, PositionState.REPAIRING)
            ps = self.pos_mgr.get_pair(stock_code)
            if ps and ps.unhedged_ts <= 0:
                ps.unhedged_ts = time.time()
        else:
            # Both legs filled successfully
            if phase == "open":
                self.pos_mgr.set_state(stock_code, PositionState.HOLDING)
                if SIMPLE_LOG:
                    ps = self.pos_mgr.get_pair(stock_code)
                    if ps:
                        stock_price = getattr(ps.open_stock, 'avg_price', 0) or 0
                        future_price = getattr(ps.open_future, 'avg_price', 0) or 0
                        print(f"[成交] {stock_code} 開倉完成 | 股票: {stock_price:.2f} 期貨: {future_price:.2f}")
            else:
                # ✅ FIX: close success -> EMPTY
                self.pos_mgr.set_state(stock_code, PositionState.EMPTY)
                ps = self.pos_mgr.get_pair(stock_code)
                if ps:
                    ps.unhedged_ts = 0.0
                    if SIMPLE_LOG:
                        stock_price = getattr(ps.close_stock, 'avg_price', 0) or 0
                        future_price = getattr(ps.close_future, 'avg_price', 0) or 0
                        # Calculate actual P&L if possible
                        print(f"[成交] {stock_code} 平倉完成 | 股票: {stock_price:.2f} 期貨: {future_price:.2f}")

        self.pos_mgr.set_inflight(stock_code, False)


    def force_close_position(self, api: sj.Shioaji, stock_code: str):
        """
        Manually trigger a FORCE CLOSE (REPAIR) logic for the given pair interactively.
        Sends aggressive orders (IOC MKT/Limit) to zero out positions.
        """
        ps = self.pos_mgr.get_pair(stock_code)
        if not ps:
            print(f"[Execution] force_close: pair {stock_code} not found.")
            return

        future_code = ps.fut_code
        s_qty = ps.pos_stock_shares
        f_qty = ps.pos_fut_qty

        print(f"[Execution] FORCE CLOSE START: {stock_code}({s_qty}) / {future_code}({f_qty})")

        # 1. Stock
        if abs(s_qty) > 0:
            qty_lot = abs(s_qty) // STOCK_SHARES_PER_LOT # Simplified lot logic
            if qty_lot > 0:
                is_buy = (s_qty < 0)
                sub_type = "FORCE_REPAIR_STOCK_" + ("BUY" if is_buy else "SELL")
                
                # Construct a fake signal to reuse on_signal logic or call directly.
                # Calling on_signal is cleaner if we package it right.
                sig = {
                    "type": "REPAIR",
                    "sub_type": sub_type,
                    "stock_code": stock_code,
                    "future_code": future_code,
                    "qty": qty_lot,
                    "is_force": True
                }
                print(f"[Execution] Sending signal for stock repair: {sub_type} qty={qty_lot}")
                self.on_signal(sig, api)
                print(f"[Execution] Stock repair signal sent.")
            elif abs(s_qty) > 0:
                 print(f"[Execution] Odd lot stock {s_qty} ignored in force close (only full lots supported).")

        # 2. Future
        if abs(f_qty) > 0:
            is_buy = (f_qty < 0)
            sub_type = "FORCE_REPAIR_FUTURE_" + ("BUY" if is_buy else "SELL")
            sig = {
                "type": "REPAIR",
                "sub_type": sub_type,
                "stock_code": stock_code,
                "future_code": future_code,
                "qty": abs(f_qty),
                "is_force": True
            }
            print(f"[Execution] Sending signal for future repair: {sub_type} qty={abs(f_qty)}")
            self.on_signal(sig, api)
            print(f"[Execution] Future repair signal sent.")
        
        print(f"[Execution] FORCE CLOSE END: {stock_code}")


# =========================================================
# 11) TradingSystem  (✅ 修正：refresh_pairs 一次同步所有 map)
# =========================================================
class TradingSystem:
    def __init__(self, args=None):
        self.args = args
        self.api = None
        self.market_data = MarketData()
        self.pos_mgr = PositionManager(api=self.api)
        self.tracker = OrderTracker()
        self.throttler = ApiThrottler(interval=1.0)

        self.capital_mgr = CapitalManager(MAX_CAPITAL, self.pos_mgr, self.market_data)
        self.strategy = StrategyEngine(self.market_data, self.pos_mgr, self.capital_mgr)
        self.exec_engine = ExecutionEngine(self.market_data, self.pos_mgr, self.tracker, throttler=self.throttler)

        self.sub_mgr: Optional[SubscriptionManager] = None
        self.discoverer: Optional[PairDiscoverer] = None

        self.signal_q = queue.Queue()
        self._stop_evt = threading.Event()

        self.keyboard = KeyboardMonitor(self)
        self.account_monitor: Optional[AccountMonitor] = None
        self.reconciler: Optional[Reconciler] = None

        self._threads: List[threading.Thread] = []

    def stop(self):
        self._stop_evt.set()

    def is_stopped(self) -> bool:
        return self._stop_evt.is_set()

    def _re_login(self):
        """重新登入以刷新 token (Auto-retry on 503)"""
        print("[System] Re-logging in to refresh token...")
        
        max_retries = 3
        for attempt in range(max_retries):
            try:
                # Close existing connection if needed
                if self.api:
                    try:
                        self.api.logout()
                    except:
                        pass
                
                # Re-initialize API
                self.api = sj.Shioaji(simulation=SIMULATION)
                
                # Re-login (same parameters as _init_api)
                self.api.login(
                    api_key=CA_API_KEY,
                    secret_key=CA_SECRET_KEY,
                    contracts_cb=lambda x: print(f"  Load {x} done.") if x else None,
                    subscribe_trade=True
                )
                
                if not SIMULATION:
                    if CA_PATH and CA_PASSWORD and PERSON_ID:
                        try:
                            _safe_activate_ca(self.api, CA_PATH, CA_PASSWORD, PERSON_ID)
                        except Exception as e:
                            print(f"[System] Re-activate_ca failed: {e}")
                
                # Re-install callbacks
                self._install_callbacks()
                
                # Update references in other components
                if self.discoverer:
                    self.discoverer.api = self.api
                if self.sub_mgr:
                    self.sub_mgr.api = self.api
                if self.reconciler:
                    self.reconciler.api = self.api
                # Note: AccountMonitor uses a fetcher function, doesn't store api directly
                # Note: ExecutionEngine doesn't directly store api
                
                print("[System] Re-login successful!")
                return True
                
            except Exception as e:
                error_str = str(e)
                print(f"[System] Re-login attempt {attempt+1} failed: {e}")
                
                # Check for 503 Rate Limit (SystemMaintenance)
                # "操作異常，請1分鐘後再重新登入"
                if "503" in error_str or "SystemMaintenance" in error_str or "操作異常" in error_str:
                    if attempt < max_retries - 1:
                        print(f"[System] Server requested wait (503). Sleeping 65s before retry...")
                        time.sleep(65)
                        continue
                
                # If not 503 or max retries reached, print trace and fail
                import traceback
                traceback.print_exc()
                return False
        
        return False

    def _init_api(self):
        """Initial API login"""
        if not SIMPLE_LOG:
            print(f"[System] init Shioaji (simulation={SIMULATION})")
        
        if not CA_API_KEY or not CA_SECRET_KEY:
            raise RuntimeError("Missing Sinopack_CA_API_KEY / Sinopack_CA_SECRET_KEY in .env")

        max_retries = 3
        for attempt in range(max_retries):
            try:
                self.api = sj.Shioaji(simulation=SIMULATION)
                
                if not SIMPLE_LOG:
                    print(f"[System] login (attempt {attempt+1})...")
                
                self.api.login(
                    api_key=CA_API_KEY,
                    secret_key=CA_SECRET_KEY,
                    contracts_cb=lambda x: print(f"  Load {x} done.") if (x and not SIMPLE_LOG) else None,
                    subscribe_trade=True
                )
                if not SIMPLE_LOG:
                    print("[System] Login Success.")
                
                if not SIMULATION:
                    if not (CA_PATH and CA_PASSWORD and PERSON_ID):
                        raise RuntimeError("Missing CA_PATH / CA_PASSWORD / PERSON_ID for real trading")
                    if not SIMPLE_LOG:
                        print("[System] activate_ca...")
                    _safe_activate_ca(self.api, CA_PATH, CA_PASSWORD, PERSON_ID)

                if not SIMPLE_LOG:
                    print("[System] fetch_contracts... (timeout=30s)")
                # Increase timeout to account for smaller page_size (monkeypatch)
                self.api.fetch_contracts(contracts_timeout=30000)
                
                # If we get here, initialization was successful
                break
                
            except Exception as e:
                error_str = str(e)
                print(f"[System] Init login attempt {attempt+1} failed: {e}")
                
                # Check for 503 Rate Limit (SystemMaintenance)
                # "操作異常，請1分鐘後再重新登入"
                if "503" in error_str or "SystemMaintenance" in error_str or "操作異常" in error_str:
                    if attempt < max_retries - 1:
                        print(f"[System] Server requested wait (503). Sleeping 65s before retry...")
                        time.sleep(65)
                        continue
                
                # If not 503 or retries exhausted, re-raise
                if attempt == max_retries - 1:
                    raise
                else:
                    # For other errors, sleep briefly and retry
                    time.sleep(5)

        self._install_callbacks()

        # Set tracker reference in PositionManager for order event logging
        self.pos_mgr.set_tracker(self.tracker)

        self.sub_mgr = SubscriptionManager(self.api, max_subs=MAX_SUBS, pos_mgr=self.pos_mgr, unsub_linger_sec=UNSUB_LINGER_SEC)
        self.discoverer = PairDiscoverer(self.api, trading_system=self)
        self.reconciler = Reconciler(self.api, self.pos_mgr, self.tracker)

    def _install_callbacks(self):
        # decorators
        try:
            @self.api.on_bidask_stk_v1(bind=True)
            def _on_stk_bidask_v1(self_api, exchange, bidask):
                try:
                    if bidask is None:
                        return
                    
                    # DEBUG CALLBACK (Broaden scope)
                    c = getattr(bidask, "code", "")
                    # Print first 50 ticks of ANY stock to prove liveliness (only in verbose mode)
                    if not SIMPLE_LOG:
                        if not hasattr(self, "_debug_tick_count"):
                            self._debug_tick_count = 0
                        
                        if self._debug_tick_count < 50:
                            print(f"[RawCallback] {c} recv (Total={self._debug_tick_count})")
                            self._debug_tick_count += 1
                        
                    self.market_data.update_stock(bidask)
                    sig = self.strategy.on_quote(bidask)
                    if sig is not None:
                        self.signal_q.put(sig)
                except Exception as e:
                    print(f"[Callback STK Error] {e}")
                    import traceback
                    traceback.print_exc()
                    return

            @self.api.on_bidask_fop_v1(bind=True)
            def _on_fut_bidask_v1(self_api, exchange, bidask):
                try:
                    if bidask is None:
                        return
                    
                    # DEBUG CALLBACK (FUTURE)
                    c = getattr(bidask, "code", "")
                    # Reuse tick count (only in verbose mode)
                    if not SIMPLE_LOG:
                        if not hasattr(self, "_debug_tick_count_fut"):
                            self._debug_tick_count_fut = 0
                        
                        if self._debug_tick_count_fut < 50:
                            print(f"[RawCallback FUT] {c} recv (Total={self._debug_tick_count_fut})")
                            self._debug_tick_count_fut += 1

                    self.market_data.update_future(bidask)
                    sig = self.strategy.on_quote(bidask)
                    if sig is not None:
                        self.signal_q.put(sig)
                except Exception as e:
                    print(f"[Callback FUT Error] {e}")
                    return

            if not SIMPLE_LOG:
                print("[System] quote callbacks installed (decorators: STK/FUT BidAsk v1)")
        except Exception as e:
            print(f"[System] quote callback install failed: {e}")

        try:
            if hasattr(self.api, "set_order_callback"):
                self.api.set_order_callback(self._on_order)
                if not SIMPLE_LOG:
                    print("[System] set_order_callback installed")
        except Exception as e:
            print(f"[System] set_order_callback failed: {e}")

        try:
            if hasattr(self.api, "set_trade_callback"):
                self.api.set_trade_callback(self._on_trade)
                if not SIMPLE_LOG:
                    print("[System] set_trade_callback installed")
        except Exception as e:
            print(f"[System] set_trade_callback failed: {e}")

    def _on_order(self, p1, p2=None):
        try:
            order_event = p2 if p2 is not None else p1
            self.pos_mgr.on_order_event(order_event)
        except Exception:
            pass

    def _on_trade(self, p1, p2=None):
        try:
            trade_event = p2 if p2 is not None else p1
            self.pos_mgr.on_trade_event(trade_event)
        except Exception:
            pass

    def _fetch_account_snapshot(self) -> AccountSnapshot:
        stock_positions: Dict[str, int] = {}
        future_positions: Dict[str, int] = {}

        try:
            stk_pos = self.api.list_positions(self.api.stock_account)
            for p in stk_pos:
                code = str(getattr(p, "code", "") or "")
                raw_qty = int(getattr(p, "quantity", 0) or 0)
                direction = getattr(p, "direction", None)
                sign = AccountSnapshot.direction_to_sign(direction)
                shares = AccountSnapshot.normalize_stock_quantity_to_shares(raw_qty, STOCK_SHARES_PER_LOT)
                stock_positions[code] = stock_positions.get(code, 0) + shares * sign
        except Exception as e:
            # FIX: Propagate error to avoid returning partial/empty snapshot
            raise e

        try:
            fut_pos = self.api.list_positions(self.api.futopt_account)
            for p in fut_pos:
                code = str(getattr(p, "code", "") or "")
                qty = int(getattr(p, "quantity", 0) or 0)
                direction = getattr(p, "direction", None)
                sign = AccountSnapshot.direction_to_sign(direction)
                future_positions[code] = future_positions.get(code, 0) + qty * sign
                
                # FIX: Debug log to see raw future positions
                if not SIMPLE_LOG:
                    print(f"[Debug] Raw future position: {code} qty={qty} direction={direction}")
        except Exception as e:
            # FIX: Propagate error
            raise e

        return AccountSnapshot(stock_positions=stock_positions, future_positions=future_positions)

    def _thread_execution_loop(self):
        while not self.is_stopped():
            try:
                sig = self.signal_q.get(timeout=0.5)
            except queue.Empty:
                continue
            try:
                self.exec_engine.on_signal(sig, self.api)
            except Exception as e:
                print(f"[ExecLoop] on_signal error: {e}")

    def _thread_account_sync_loop(self):
        while not self.is_stopped():
            try:
                snap = self.account_monitor.latest() if self.account_monitor else None
                if snap:
                    self.pos_mgr.sync_from_snapshot(snap)
            except Exception:
                pass
            time.sleep(1.0)

    def _thread_refresh_pairs_loop(self):
        last_ts = 0.0
        while not self.is_stopped():
            now = time.time()
            if now - last_ts < REFRESH_PAIRS_EVERY_SEC:
                time.sleep(1.0)
                continue
            last_ts = now

            try:
                pairs = self.discoverer.find_active_pairs(top_n=TOP_N_PAIRS) if self.discoverer else []
                
                # FIX: Add held/repairing positions to pairs list to ensure they are subscribed and monitored
                # This ensures we see "Waiting for fill..." logs for 8358 etc.
                if self.pos_mgr:
                    held_pairs = []
                    for ps in self.pos_mgr.all_pairs():
                        if ps.pos_stock_shares != 0 or ps.pos_fut_qty != 0 or ps.state == PositionState.REPAIRING:
                            # EVEN IF fut_code is missing (e.g. 8358 single leg), track it.
                            if ps.stock_code:
                                f_code = ps.fut_code if ps.fut_code else ""
                                held_pairs.append((ps.stock_code, f_code))
                    
                    # Merge lists
                    current_set = set(pairs)
                    for p in held_pairs:
                        if p not in current_set:
                            pairs.append(p)
                            if not SIMPLE_LOG:
                                print(f"[Pairs] Adding held pair {p[0]} to active list for monitoring")

                if not pairs:
                    print("[Pairs] no pairs found")
                    continue

                # ✅ 統一取 map（避免 Contracts.Futures[code] 各種取不到）
                fut_contract_map = self.discoverer.get_future_contract_map() if self.discoverer else {}
                feq_map = self.discoverer.get_future_shares_equiv_map() if self.discoverer else {}

                # 1) 先把 PairState future_shares_equiv 填好（風控/hedge 判定一致）
                for s, f in pairs:
                    feq = int(feq_map.get(f, FUTURE_SHARES_EQUIV) or FUTURE_SHARES_EQUIV)
                    self.pos_mgr.ensure_pair(s, f, future_shares_equiv=feq)

                # 2) 更新 strategy pairs
                self.strategy.update_pairs(pairs)

                # 3) 更新 subscription manager 的 future_map + apply subscriptions
                if self.sub_mgr:
                    self.sub_mgr.set_future_map(fut_contract_map)
                    self.sub_mgr.apply_pairs(pairs, log_prefix="[Pairs] ")

                # 4) 更新 execution engine 的 future_map（下單解析合約更穩）
                self.exec_engine.set_future_map(fut_contract_map)

            except Exception as e:
                if not SIMPLE_LOG:
                    print(f"[Pairs] refresh failed: {e}")

    def run(self):
        self._init_api()

        # FIX: Cancel all old orders first!
        self._cancel_all_active_orders_startup()

        if self.reconciler:
            self.reconciler.reconcile()

        # STARTUP CHECK: Broken Legs
        # Perform check AFTER reconcile (which fills pos_mgr) but BEFORE fast loops start
        self._check_broken_legs_startup()

        self.account_monitor = AccountMonitor(self._fetch_account_snapshot, interval_sec=2.0)
        self.account_monitor.start()

        self.keyboard.start()

        t_exec = threading.Thread(target=self._thread_execution_loop, daemon=True, name="ExecLoop")
        t_acct = threading.Thread(target=self._thread_account_sync_loop, daemon=True, name="AccountSync")
        t_pairs = threading.Thread(target=self._thread_refresh_pairs_loop, daemon=True, name="RefreshPairs")

        self._threads = [t_exec, t_acct, t_pairs]
        for t in self._threads:
            t.start()
            
        if not SIMPLE_LOG:
            print("[System] running. press 'h' for help.")
        try:
            while not self.is_stopped():
                time.sleep(0.5)
        except KeyboardInterrupt:
            pass

        print("[System] stopping...")

        try:
            self.keyboard.stop()
        except Exception:
            pass

        try:
            if self.account_monitor:
                self.account_monitor.stop()
        except Exception:
            pass

        try:
            if self.sub_mgr:
                self.sub_mgr.force_unsubscribe_all(log_prefix="[System] ")
        except Exception:
            pass

        try:
            self.tracker.stop()
        except Exception:
            pass

        try:
            if self.api:
                self.api.logout()
        except Exception:
            pass

        print("[System] stopped.")



    def _cancel_all_active_orders_startup(self):
        """
        Startup: Cancel ALL active orders to ensure a clean state.
        """
        if not SIMPLE_LOG:
            print("[System] Cancelling all active orders at startup...")
        else:
            print("[啟動] 清除所有未成交掛單...")
            
        try:
            # Force update status
            try:
                self.api.update_status(self.api.stock_account)
                self.api.update_status(self.api.futopt_account)
            except: pass
            
            trades = self.api.list_trades()
            count = 0
            for t in trades:
                if t.status.status in [sj.constant.Status.Submitted, sj.constant.Status.PartFilled, sj.constant.Status.PendingSubmit]:
                    try:
                        self.api.cancel_order(t)
                        count += 1
                        if not SIMPLE_LOG:
                            print(f"  -> Cancelled {t.order.id} ({t.contract.code})")
                    except Exception as e:
                        if not SIMPLE_LOG:
                            print(f"  -> Failed to cancel {t.order.id}: {e}")
            
            if count > 0:
                print(f"[啟動] 已送出取消指令: {count} 筆。等待 2 秒...")
                time.sleep(2.0) # Wait for propagation
            else:
                if not SIMPLE_LOG: print("[System] No active orders found.")

        except Exception as e:
            print(f"[System] Error cancelling orders: {e}")

    def _check_broken_legs_startup(self):
        """
        Scans for REPAIRING/Unhedged positions at startup.
        Allows ignoring or fixing based on args or user input.
        """
        if not SIMPLE_LOG:
            print("\n[System] Checking for broken/unhedged positions...")
        
        all_ps = self.pos_mgr.all_pairs()
        broken_list = []
        
        for ps in all_ps:
            is_broken = False
            
            # Check Balance First
            eq = ps.future_shares_equiv or FUTURE_SHARES_EQUIV
            s = ps.pos_stock_shares
            f = ps.pos_fut_qty
            
            # If perfectly balanced, it's NOT broken, even if state is REPAIRING
            # (Because sync_from_snapshot sets REPAIRING by default for new pairs)
            # e.g. Stock 2000, Future -1, EQ 2000 => 2000 - 2000 = 0 (Balanced)
            is_balanced = (s + f * eq == 0) and (s != 0 or f != 0)
            
            if is_balanced:
                if ps.state == PositionState.REPAIRING:
                    if not SIMPLE_LOG:
                        print(f"[System] Auto-correcting state for {ps.stock_code}: REPAIRING -> HOLDING (Balanced)")
                    ps.state = PositionState.HOLDING
                    ps.unhedged_ts = 0.0
                continue # Healthy pair, skip adding to broken list

            # If not balanced, check if it needs repair
            if ps.state == PositionState.REPAIRING:
                is_broken = True
            elif ps.state == PositionState.HOLDING:
                if not is_balanced:
                    is_broken = True
            elif ps.state == PositionState.EMPTY:
                if s != 0 or f != 0:
                    is_broken = True
            
            if is_broken:
                broken_list.append(ps)

        if not broken_list:
            if not SIMPLE_LOG:
                print("[System] No broken positions found. Good.\n")
            return

        if not SIMPLE_LOG:
            print(f"!!! FOUND {len(broken_list)} BROKEN/UNHEDGED POSITIONS !!!")
        else:
            codes = [ps.stock_code for ps in broken_list]
            print(f"[啟動] 發現 {len(broken_list)} 個既有缺腳部位: {', '.join(codes)}")

        # Check args first
        should_ignore = False
        should_fix = False
        
        if self.args:
            if self.args.ignore_broken:
                should_ignore = True
                print("[System] --ignore-broken: Marking all broken positions as IGNORED.")
            elif self.args.fix_broken:
                should_fix = True
                print("[System] --fix-broken: Attempting FORCE CLOSE.")

        if not should_ignore and not should_fix:
            # Interactive or Default behavior
            # Always try to ask if interactive
            try:
                if sys.stdin.isatty():
                    print("\n[啟動] 發現缺腳部位。請問是否執行強制平倉？ (y=是 / N=忽略)")
                    print(">> ", end="", flush=True)
                    choice = input().strip().lower()
                    if choice == 'y':
                        should_fix = True
                    else:
                        should_ignore = True
                        print("[啟動] 已選擇忽略。")
                else:
                    if SIMPLE_LOG:
                        print("[啟動] 無參數且非互動環境，預設忽略既有部位以免卡住。")
                    else:
                        print("[System] Non-interactive mode. Defaulting to IGNORE.")
                    should_ignore = True
            except (EOFError, KeyboardInterrupt):
                print("\n[System] Input interrupted. Defaulting to IGNORE.")
                should_ignore = True

        if should_ignore:
            for ps in broken_list:
                ps.is_ignored = True
                ps.repair_failures = 999
                if not SIMPLE_LOG:
                    print(f"  -> {ps.stock_code}: Marked IGNORED.")
            return

        if should_fix:
            print("[啟動] 執行強制平倉...")
            for ps in broken_list:
                ps.repair_failures = 0
                ps.is_ignored = False
                self.exec_engine.force_close_position(self.api, ps.stock_code)
                time.sleep(0.2)
            print("[System] Force close commands sent.\n")

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
    p -> print positions
    o -> print orders
    h -> help
    q -> quit
    """
    def __init__(self, system, poll_sec: float = 0.05):
        super().__init__(daemon=True)
        self.system = system
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
            self._run_fallback_input()

    def _print_help_once(self):
        print("\n[Keyboard] p=positions, o=orders, h=help, q=quit\n", flush=True)

    def _handle_key(self, ch: str):
        ch = (ch or "").strip().lower()
        if not ch:
            return
        if ch == "p":
            print(self.system.pos_mgr.dump_positions_pretty(), flush=True)
        elif ch == "o":
            rows = self.system.pos_mgr.dump_orders(last_n=120)
            print("\n=== ORDERS (latest) ===")
            for r in rows:
                print(
                    f"{r['time']} oid={r.get('order_id','')} "
                    f"code={r.get('code','-') or '-'} {r.get('phase','-') or '-'} {r.get('leg','-') or '-'} "
                    f"st={r.get('status','')} qty={r.get('deal_qty',0)} px={r.get('deal_price',0)} msg={r.get('msg','') or ''}"
                )
            print("", flush=True)
        elif ch == "h":
            self._print_help_once()
        elif ch == "q":
            print("\n[Keyboard] quit requested\n", flush=True)
            self.system.stop()

    def _run_windows(self):
        while not self._stop_evt.is_set():
            if msvcrt.kbhit():
                ch = msvcrt.getwch()
                self._handle_key(ch)
            time.sleep(self.poll_sec)

    def _run_posix(self):
        fd = sys.stdin.fileno()
        old = termios.tcgetattr(fd)
        try:
            tty.setcbreak(fd)
            while not self._stop_evt.is_set():
                r, _, _ = select.select([sys.stdin], [], [], self.poll_sec)
                if r:
                    ch = sys.stdin.read(1)
                    self._handle_key(ch)
        finally:
            termios.tcsetattr(fd, termios.TCSADRAIN, old)

    def _run_fallback_input(self):
        while not self._stop_evt.is_set():
            try:
                s = input().strip().lower()
                if s:
                    self._handle_key(s[0])
            except EOFError:
                break

import argparse

# =========================
# --- main ---
# =========================
def main():
    parser = argparse.ArgumentParser(description="Arbitrage Trading System")
    parser.add_argument("--ignore-broken", action="store_true", help="Ignore broken/unhedged positions at startup")
    parser.add_argument("--fix-broken", action="store_true", help="Attempt to force close broken/unhedged positions at startup")
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.WARNING,
        format='%(asctime)s [%(levelname)s] %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    # logging.basicConfig(level=logging.WARNING)  # Suppress INFO logs globally
    logging.getLogger('shioaji').setLevel(logging.ERROR)  # Suppress API logs
    sys.setrecursionlimit(10000)
    system = TradingSystem(args=args)
    system.run()

if __name__ == "__main__":
    main()
