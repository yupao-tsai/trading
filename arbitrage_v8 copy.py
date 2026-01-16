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
import os
import sys
import threading

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
ENTRY_THRESHOLD = 500  # 進場淨利門檻（以金額）
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
HEDGE_TIMEOUT_SEC = 10.0
REPAIR_TIMEOUT_SEC = 5.0
MIN_PROFIT_TO_REPAIR = 0

# Tick protection
STOCK_BUY_TICKS = 5
STOCK_SELL_TICKS = 5
FUT_BUY_TICKS = 5
FUT_SELL_TICKS = 5
REPAIR_TICKS = 20       # Aggressive tick limit for repair
FORCE_CLOSE_TICKS = 50  # Very aggressive for force close

# Dynamic pair discovery
MIN_VOLUME_THRESHOLD = 500
VOLUME_HISTORY_FILE = "volume_history.json"
TOP_N_PAIRS = 30
REFRESH_PAIRS_EVERY_SEC = 600  # 10分鐘
MAX_SUBS = 80
UNSUB_LINGER_SEC = 60 # P1: 退訂延遲

# Debug watch
DBG_WATCH_STOCK = "2313"
DBG_WATCH_FUT: Optional[str] = None
DBG_LAST_LINE = None
DBG_LINE_LOCK = threading.Lock()

# =========================
# --- Strategy Parameters (Z-Score & Risk) ---
# =========================
EWMA_HALF_LIFE = 60.0    # 秒
Z_ENTRY = 2.0            # 進場 Z-score (Mean Reversion)
Z_EXIT = 0.5             # 出場 Z-score
MIN_PROFIT_Z = 2.0       # 最小期望獲利 (以 Sigma 倍數計)

MAX_HOLDING_SEC = 1800   # 最大持倉時間 (30分鐘)
UNHEDGED_REPAIR_DEADLINE = 5.0 # 5秒內嘗試補單 (Repair)
UNHEDGED_FORCE_CLOSE_SEC = 15.0  # 超過 15秒 強制清倉 (Unwind/Panic)

STD_FLOOR = 0.5          # Z-score 分母下限 (避免爆表)
MIN_SAMPLES = 50         # EWMA 暖機樣本數
INFLIGHT_COOLDOWN_SEC = 2.0 # 交易完成後冷卻時間
WARMUP_SEC = 5.0         # 啟動後暖機時間 (P1)

SLIPPAGE_SCALE = 0.5     # 滑價估算係數 (Ticks per Ratio)

# Risk Control
MAX_OPEN_PAIRS = 5       # 最大同時持倉對數
MAX_DAILY_LOSS = -50000  # 當日最大虧損 (金額)
MAX_ORDERS_PER_MIN = 20  # Global throttle
MAX_DAILY_ORDERS = 300   # Global daily limit

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

    open_ts: float = 0.0
    close_ts: float = 0.0
    unhedged_ts: float = 0.0

    # EWMA
    last_tick_ts: float = 0.0
    basis_mean: float = 0.0
    basis_std: float = 0.0
    sample_count: int = 0
    initialized: bool = False

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
    """
    def __init__(self, filename=ORDER_HISTORY_FILE):
        self.filename = filename
        self._queue = queue.Queue()
        self._stop_event = threading.Event()
        self._writer_thread = threading.Thread(target=self._writer_loop, daemon=True)
        self._writer_thread.start()
        self._lock = threading.Lock()

    def record(self, order_id: str, stock_code: str, fut_code: str, phase: str, leg: str, action: str, price: float, qty: int):
        if not order_id:
            return
        record = {
            "ts": time.time(),
            "date": datetime.datetime.now().strftime('%Y-%m-%d'),
            "order_id": str(order_id),
            "stock_code": stock_code,
            "fut_code": fut_code,
            "phase": phase,  # 'open' or 'close'
            "leg": leg,      # 'stock' or 'future'
            "action": str(action),
            "price": float(price or 0.0),
            "qty": int(qty or 0)
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
                with self._lock:
                    self._latest = AccountSnapshot(ts=time.time())
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
    def __init__(self):
        self._lock = threading.RLock()
        self._pairs: Dict[str, PairState] = {}

        self._order_map: Dict[str, Dict[str, Any]] = {}
        self._orders_ring: List[Dict[str, Any]] = []
        self._orders_ring_max = 2000

    # ---------- Pair CRUD ----------
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
            for ps in self._pairs.values():
                s = ps.stock_code
                f = ps.fut_code

                new_s = snap.get_stock_qty(s)
                new_f = snap.get_future_qty(f) if f else 0

                if new_s != ps.pos_stock_shares or new_f != ps.pos_fut_qty:
                    ps.pos_stock_shares = int(new_s)
                    ps.pos_fut_qty = int(new_f)

                # zero -> EMPTY
                if ps.pos_stock_shares == 0 and ps.pos_fut_qty == 0:
                    ps.state = PositionState.EMPTY
                    ps.unhedged_ts = 0.0
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
                    if ps.state != PositionState.REPAIRING:
                        ps.unhedged_ts = time.time()
                        changed.append(ps.stock_code)
                    ps.state = PositionState.REPAIRING

        return changed

    # ---------- Order/Trade callbacks ----------
    def on_order_event(self, order_event: Any) -> None:
        try:
            raw = self._to_dict(order_event)
        except Exception:
            raw = {"raw": str(order_event)}

        # order_id
        oid = str(raw.get("order_id") or raw.get("id") or raw.get("order", {}).get("id") or "")
        status = normalize_status(raw.get("status") or raw.get("state") or raw.get("order_status"))
        deal_qty = int(raw.get("deal_qty") or raw.get("deal_quantity") or raw.get("filled_qty") or 0)
        deal_px = float(raw.get("deal_price") or raw.get("filled_price") or raw.get("avg_price") or 0.0)
        code = str(raw.get("code") or raw.get("contract", {}).get("code") or raw.get("symbol") or "")

        info = self._order_map.get(oid, {})
        stock_code = info.get("stock_code") or ""
        phase = info.get("phase") or ""
        leg = info.get("leg") or ""

        with self._lock:
            self._append_order_event({
                "ts": time.time(),
                "order_id": oid,
                "code": code,
                "stock_code": stock_code,
                "phase": phase,
                "leg": leg,
                "status": status or "",
                "deal_qty": deal_qty,
                "deal_price": deal_px,
                "msg": raw.get("msg") or raw.get("message") or raw.get("error_message") or "",
            })

        if stock_code:
            self._update_legfill_from_event(stock_code, phase, leg, status, deal_qty, deal_px, raw)

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
        print(">>> [Reconciler] Starting State Reconstruction...")

        local_orders = self.tracker.load_history()
        if local_orders:
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
    def __init__(self, api: sj.Shioaji):
        self.api = api
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
                print("[PairDiscoverer] daily quotes empty")
        except Exception as e:
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
                if stype in ['FUT', 'Future', str(sj.constant.SecurityType.Future)] and hasattr(contract_obj, 'underlying_code'):
                    cache_key = f"F_{contract_obj.underlying_code}"
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
        print("[PairDiscoverer] scanning active stock-future pairs...")
        today = datetime.datetime.now()
        target_month_str = today.strftime('%Y%m')

        # clear & rebuild each scan
        self.future_shares_equiv_map = {}
        self.future_contract_map = {}

        candidates = []
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

                    if getattr(contract, "delivery_month", "") != target_month_str:
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
                        # sim env sometimes shows unit=1 -> fallback to default
                        shares_equiv = FUTURE_SHARES_EQUIV
                    else:
                        # unknown unit -> skip (avoid wrong hedging)
                        continue

                    candidates.append(contract)
                    self.future_shares_equiv_map[str(contract.code)] = int(shares_equiv)
                    self.future_contract_map[str(contract.code)] = contract

                except Exception:
                    continue

        # batch snapshot for stocks
        stock_contracts_map = {}
        for contract in candidates:
            scode = str(contract.underlying_code)
            try:
                if scode not in stock_contracts_map:
                    stock_contracts_map[scode] = self.api.Contracts.Stocks[scode]
            except Exception:
                pass

        print(f"  [PairDiscoverer] Batch querying {len(stock_contracts_map)} stocks...")
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
                print(f"  [PairDiscoverer] Snapshot chunk failed: {e}")
                time.sleep(0.5)

        best_by_pair = {}

        for contract in candidates:
            stock_code = str(contract.underlying_code)
            future_code = str(contract.code)

            stk_c = stock_contracts_map.get(stock_code)
            if stk_c:
                s_unit = int(getattr(stk_c, 'unit', 0) or 0)
                if s_unit not in (STOCK_SHARES_PER_LOT, 1):
                    continue

            info = stock_info_cache.get(stock_code)
            if info:
                s_price, s_vol, s_ref_price = info

                # drop limit-up/down like extremes
                if s_ref_price > 0 and s_price > 0:
                    change_pct = (s_price - s_ref_price) / s_ref_price
                    if abs(change_pct) > 0.09:
                        continue

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

            if stock_avg < MIN_VOLUME_THRESHOLD:
                continue

            future_avg = self._get_avg_volume_smart(future_code, contract_obj=contract)
            if future_avg < MIN_VOLUME_THRESHOLD:
                continue

            score = min(stock_avg, future_avg)

            s_name = getattr(stk_c, "name", "N/A") if stk_c else "N/A"
            f_name = getattr(contract, "name", "N/A")

            rec = (score, stock_avg, future_avg, stock_code, future_code, str(s_name).strip(), str(f_name).strip())
            key = (stock_code, future_code)
            if key not in best_by_pair or rec[0] > best_by_pair[key][0]:
                best_by_pair[key] = rec

        ranked = list(best_by_pair.values())
        ranked.sort(key=lambda x: x[0], reverse=True)
        top = ranked[:top_n]

        print(f"[PairDiscoverer] done. matched={len(ranked)} return={len(top)}")
        print("\n=== Active Pairs (Top) ===")
        print(f"{'Stock':<10} {'Name':<12} {'Future':<10} {'Name':<12} {'Score':<8} {'S_Vol':<8} {'F_Vol':<8}")
        print("-" * 84)
        for row in top:
            scr, sa, fa, sc, fc, sn, fn = row
            print(f"{sc:<10} {sn:<12} {fc:<10} {fn:<12} {scr:<8} {sa:<8} {fa:<8}")
        print("==========================\n")

        return [(s_code, f_code) for (_, _, _, s_code, f_code, _, _) in top]


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

            if self.verbose:
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
        if self.verbose and codes:
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

    def update_pairs(self, pairs: List[Tuple[str, str]]):
        with self._pairs_lock:
            self.pairs = list(pairs)
            pair_map: Dict[str, List[str]] = {}
            future_to_stock: Dict[str, str] = {}
            for s, f in self.pairs:
                pair_map.setdefault(s, []).append(f)
                future_to_stock[f] = s
                self.pos_mgr.ensure_pair(s, f)
            self.pair_map = pair_map
            self.future_to_stock = future_to_stock
        print(f"[Strategy] pairs updated: {len(self.pairs)}")

    def check_risk(self, is_open: bool = True) -> bool:
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
            active = 0
            unhedged = 0
            for ps in self.pos_mgr.all_pairs():
                if ps.state in (PositionState.PENDING_ENTRY, PositionState.PENDING_EXIT, PositionState.HOLDING, PositionState.REPAIRING):
                    active += 1
                if ps.state == PositionState.REPAIRING:
                    unhedged += 1
            if unhedged > 0:
                return False
            if active >= MAX_OPEN_PAIRS:
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
        if time.time() - self.start_ts < WARMUP_SEC:
            return None

        code = _get_code(quote)
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
            return None

        if not self.pos_mgr.try_acquire_lock(target_stock):
            return None

        try:
            ps = self.pos_mgr.get_pair(target_stock)
            if not ps:
                return None

            now = time.time()

            # ---- AUTO REPAIR ----
            if ps.state == PositionState.REPAIRING:
                if now - ps.last_repair_ts < 1.5:
                    return None

                duration = now - (ps.unhedged_ts or now)
                is_force_close = (duration > UNHEDGED_REPAIR_DEADLINE)

                action = None
                qty = 0
                s_shares = ps.pos_stock_shares
                f_qty = ps.pos_fut_qty

                if s_shares > 0 and f_qty == 0:
                    action = "FORCE_CLOSE_STOCK" if is_force_close else "SELL_FUTURE"
                    qty = abs(s_shares) // STOCK_SHARES_PER_LOT if "STOCK" in action else FUTURE_QTY
                elif s_shares == 0 and f_qty < 0:
                    action = "FORCE_CLOSE_FUTURE" if is_force_close else "BUY_STOCK"
                    qty = abs(f_qty) if "FUTURE" in action else STOCK_QTY
                elif s_shares < 0 and f_qty == 0:
                    action = "FORCE_CLOSE_STOCK" if is_force_close else "BUY_FUTURE"
                    qty = abs(s_shares) // STOCK_SHARES_PER_LOT if "STOCK" in action else FUTURE_QTY
                elif s_shares == 0 and f_qty > 0:
                    action = "FORCE_CLOSE_FUTURE" if is_force_close else "SELL_STOCK"
                    qty = abs(f_qty) if "FUTURE" in action else STOCK_QTY
                elif s_shares != 0 and f_qty != 0:
                    action = "FORCE_CLOSE_STOCK"
                    qty = abs(s_shares) // STOCK_SHARES_PER_LOT

                if action and qty > 0:
                    print(f"[Strategy] {target_stock} UNHEDGED ({duration:.1f}s) -> {action} qty={qty}")
                    ps.last_repair_ts = now
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
                return None

            s_bid = _get_bid(s_q); s_ask = _get_ask(s_q)
            f_bid = _get_bid(f_q); f_ask = _get_ask(f_q)

            latency = max(now - _get_ts(s_q), now - _get_ts(f_q))
            if latency > MAX_DATA_DELAY_SEC:
                return None

            if s_bid <= 0 or s_ask <= 0 or f_bid <= 0 or f_ask <= 0:
                return None

            s_mid = (s_bid + s_ask) / 2.0
            f_mid = (f_bid + f_ask) / 2.0

            feq = int(ps.future_shares_equiv or 0) or FUTURE_SHARES_EQUIV
            basis = (f_mid * feq * FUTURE_QTY) - (s_mid * STOCK_SHARES_PER_LOT * STOCK_QTY)

            self._update_ewma(ps, basis, now)
            if (not ps.initialized) or ps.basis_std <= 0 or ps.sample_count < MIN_SAMPLES:
                return None

            z_score = (basis - ps.basis_mean) / ps.basis_std

            # ---- ENTRY ----
            if ps.state == PositionState.EMPTY:
                if z_score >= Z_ENTRY:
                    if not self.check_risk(is_open=True):
                        return None

                    s_ask_vol = _get_ask_vol(s_q)
                    f_bid_vol = _get_bid_vol(f_q)
                    if s_ask_vol < STOCK_QTY * 3 or f_bid_vol < FUTURE_QTY * 3:
                        return None

                    stock_notional = s_ask * STOCK_SHARES_PER_LOT * STOCK_QTY
                    if not self.capital_mgr.check_available(stock_notional):
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
                        print(f"[Strategy] OPEN {target_stock} Z={z_score:.2f} Mean={ps.basis_mean:.2f} Std={ps.basis_std:.2f} Net={net:.0f} Basis={basis:.2f}")
                        return TradeSignal({
                            "type": "OPEN",
                            "stock_code": target_stock,
                            "future_code": f_code,
                            "stock_px": s_ask,
                            "fut_px": f_bid,
                            "net": net,
                        })

            # ---- EXIT ----
            elif ps.state == PositionState.HOLDING:
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
                    print(f"[Strategy] CLOSE {target_stock} ({reason}) Z={z_score:.2f} Net={current_spread:.0f}")

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
            # ✅ 保證釋放
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

    def _mk_mktable_price_strict(self, side: str, ref_px: float, protect_ticks: int) -> float:
        if ref_px <= 0:
            return ref_px
        px = self._round_to_tick_by_its_own_tier(ref_px, "buy" if side == "buy" else "sell")
        for _ in range(int(max(0, protect_ticks))):
            px = self._step_one_tick_strict(px, side)
        return self._round_to_tick_by_its_own_tier(px, side)

    def _place_stock_order(self, api: sj.Shioaji, stock_code: str, action, price: float, qty_lot: int, price_type=None):
        contract_s = api.Contracts.Stocks[stock_code]
        if price_type is None:
            price_type = sj.constant.StockPriceType.LMT

        order_s = api.Order(
            price=float(price),
            quantity=int(qty_lot),
            action=action,
            price_type=price_type,
            order_type=sj.constant.OrderType.ROD if hasattr(sj.constant, "OrderType") else "ROD",
            order_lot=sj.constant.StockOrderLot.Common if hasattr(sj.constant, "StockOrderLot") else "Common",
            account=api.stock_account,
        )
        return api.place_order(contract_s, order_s)

    def _place_future_order(self, api: sj.Shioaji, future_code: str, action, price: float, qty: int, price_type=None):
        contract_f = self._resolve_future_contract(api, future_code)

        octype = sj.constant.FuturesOCType.Auto if hasattr(sj.constant, "FuturesOCType") else "Auto"
        if price_type is None:
            price_type = sj.constant.FuturesPriceType.LMT if hasattr(sj.constant, "FuturesPriceType") else sj.constant.StockPriceType.LMT
        order_type_f = sj.constant.FuturesOrderType.ROD if hasattr(sj.constant, "FuturesOrderType") else (
            sj.constant.OrderType.ROD if hasattr(sj.constant, "OrderType") else "ROD"
        )

        order_f = api.Order(
            action=action,
            price=float(price),
            quantity=int(qty),
            price_type=price_type,
            order_type=order_type_f,
            octype=octype,
            account=api.futopt_account,
        )
        return api.place_order(contract_f, order_f)

    def on_signal(self, sig: Dict[str, Any], api: sj.Shioaji):
        typ = str(sig.get("type") or "")
        stock_code = str(sig.get("stock_code") or "")
        future_code = str(sig.get("future_code") or "")

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

            print(f"[Execution] Executing Auto-Repair: {sub_type} {stock_code} qty={qty} (is_force={is_force})")
            self.pos_mgr.set_state(stock_code, PositionState.REPAIRING)

            try:
                ps = self.pos_mgr.get_pair(stock_code)

                if "STOCK" in sub_type:
                    quote = self.market_data.get_stock(stock_code)
                    if "BUY" in sub_type or (("FORCE" in sub_type) and ps is not None and getattr(ps, "pos_stock_shares", 0) < 0):
                        action = sj.constant.Action.Buy
                        ref_px = _get_ask(quote) if quote else 0.0
                        side = "buy"
                    else:
                        action = sj.constant.Action.Sell
                        ref_px = _get_bid(quote) if quote else 0.0
                        side = "sell"

                    px = self._mk_mktable_price_strict(side, ref_px, protect_ticks=(FORCE_CLOSE_TICKS if is_force else REPAIR_TICKS)) if ref_px > 0 else 0.0
                    if px > 0:
                        trade_fix = self._place_stock_order(api, stock_code, action, px, qty, price_type=sj.constant.StockPriceType.LMT)
                        oid = str(trade_fix.order.id)
                        try: self.tracker.record(oid, stock_code, future_code, "repair", "stock", action, px, qty)
                        except Exception: pass
                        try: self.pos_mgr.register_order(oid, stock_code, "repair", "stock")
                        except Exception: pass
                    else:
                        print("[Execution] Auto-Repair STOCK px invalid")

                elif "FUTURE" in sub_type:
                    quote = self.market_data.get_future(future_code)
                    if "BUY" in sub_type or (("FORCE" in sub_type) and ps is not None and getattr(ps, "pos_fut_qty", 0) < 0):
                        action = sj.constant.Action.Buy
                        ref_px = _get_ask(quote) if quote else 0.0
                        side = "buy"
                    else:
                        action = sj.constant.Action.Sell
                        ref_px = _get_bid(quote) if quote else 0.0
                        side = "sell"

                    px = self._mk_mktable_price_strict(side, ref_px, protect_ticks=(FORCE_CLOSE_TICKS if is_force else REPAIR_TICKS)) if ref_px > 0 else 0.0
                    if px > 0:
                        trade_fix = self._place_future_order(api, future_code, action, px, qty)
                        oid = str(trade_fix.order.id)
                        try: self.tracker.record(oid, stock_code, future_code, "repair", "future", action, px, qty)
                        except Exception: pass
                        try: self.pos_mgr.register_order(oid, stock_code, "repair", "future")
                        except Exception: pass
                    else:
                        print("[Execution] Auto-Repair FUTURE px invalid")

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

            self.pos_mgr.prepare_phase(stock_code, phase, target_stock_lots=STOCK_QTY, target_fut_qty=FUTURE_QTY, last_signal_net=net)

            stock_action = sj.constant.Action.Buy if is_open else sj.constant.Action.Sell
            future_action = sj.constant.Action.Sell if is_open else sj.constant.Action.Buy

            if is_open:
                stock_px = self._mk_mktable_price_strict("buy", init_stock_px, STOCK_BUY_TICKS + 1)
                fut_px = self._mk_mktable_price_strict("sell", init_fut_px, FUT_SELL_TICKS + 1)
            else:
                stock_px = self._mk_mktable_price_strict("sell", init_stock_px, STOCK_SELL_TICKS + 1)
                fut_px = self._mk_mktable_price_strict("buy", init_fut_px, FUT_BUY_TICKS + 1)

            # send dual-leg concurrently
            with concurrent.futures.ThreadPoolExecutor(max_workers=2) as ex:
                fs = ex.submit(self._place_stock_order, api, stock_code, stock_action, stock_px, STOCK_QTY)
                ff = ex.submit(self._place_future_order, api, future_code, future_action, fut_px, FUTURE_QTY)
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
            print(f"[{phase.upper()}] timeout/failed -> set REPAIRING")
            self.pos_mgr.set_state(stock_code, PositionState.REPAIRING)
            ps = self.pos_mgr.get_pair(stock_code)
            if ps and ps.unhedged_ts <= 0:
                ps.unhedged_ts = time.time()
        else:
            if phase == "open":
                self.pos_mgr.set_state(stock_code, PositionState.HOLDING)
            else:
                # ✅ FIX: close success -> EMPTY
                self.pos_mgr.set_state(stock_code, PositionState.EMPTY)
                ps = self.pos_mgr.get_pair(stock_code)
                if ps:
                    ps.unhedged_ts = 0.0

        self.pos_mgr.set_inflight(stock_code, False)


# =========================================================
# 11) TradingSystem  (✅ 修正：refresh_pairs 一次同步所有 map)
# =========================================================
class TradingSystem:
    def __init__(self):
        self.api = None
        self.market_data = MarketData()
        self.pos_mgr = PositionManager()
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

    def _init_api(self):
        print(f"[System] init Shioaji (simulation={SIMULATION})")
        self.api = sj.Shioaji(simulation=SIMULATION)

        if not CA_API_KEY or not CA_SECRET_KEY:
            raise RuntimeError("Missing Sinopack_CA_API_KEY / Sinopack_CA_SECRET_KEY in .env")

        print("[System] login...")
        self.api.login(
            api_key=CA_API_KEY,
            secret_key=CA_SECRET_KEY,
            contracts_cb=lambda x: None,
            subscribe_trade=True,
        )

        if not SIMULATION:
            if not (CA_PATH and CA_PASSWORD and PERSON_ID):
                raise RuntimeError("Missing CA_PATH / CA_PASSWORD / PERSON_ID for real trading")
            try:
                print("[System] activate_ca...")
                _safe_activate_ca(self.api, CA_PATH, CA_PASSWORD, PERSON_ID)
            except Exception as e:
                print(f"[System] activate_ca failed: {e}")
                raise

        print("[System] fetch_contracts...")
        try:
            self.api.fetch_contracts()
        except Exception as e:
            print(f"[System] fetch_contracts failed: {e}")

        self._install_callbacks()

        self.sub_mgr = SubscriptionManager(self.api, max_subs=MAX_SUBS, pos_mgr=self.pos_mgr, unsub_linger_sec=UNSUB_LINGER_SEC)
        self.discoverer = PairDiscoverer(self.api)
        self.reconciler = Reconciler(self.api, self.pos_mgr, self.tracker)

    def _install_callbacks(self):
        # decorators
        try:
            @self.api.on_bidask_stk_v1(bind=True)
            def _on_stk_bidask_v1(self_api, exchange, bidask):
                try:
                    if bidask is None:
                        return
                    self.market_data.update_stock(bidask)
                    sig = self.strategy.on_quote(bidask)
                    if sig is not None:
                        self.signal_q.put(sig)
                except Exception:
                    return

            @self.api.on_bidask_fop_v1(bind=True)
            def _on_fut_bidask_v1(self_api, exchange, bidask):
                try:
                    if bidask is None:
                        return
                    self.market_data.update_future(bidask)
                    sig = self.strategy.on_quote(bidask)
                    if sig is not None:
                        self.signal_q.put(sig)
                except Exception:
                    return

            print("[System] quote callbacks installed (decorators: STK/FUT BidAsk v1)")
        except Exception as e:
            print(f"[System] quote callback install failed: {e}")

        try:
            if hasattr(self.api, "set_order_callback"):
                self.api.set_order_callback(self._on_order)
                print("[System] set_order_callback installed")
        except Exception as e:
            print(f"[System] set_order_callback failed: {e}")

        try:
            if hasattr(self.api, "set_trade_callback"):
                self.api.set_trade_callback(self._on_trade)
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
        except Exception:
            pass

        try:
            fut_pos = self.api.list_positions(self.api.futopt_account)
            for p in fut_pos:
                code = str(getattr(p, "code", "") or "")
                qty = int(getattr(p, "quantity", 0) or 0)
                direction = getattr(p, "direction", None)
                sign = AccountSnapshot.direction_to_sign(direction)
                future_positions[code] = future_positions.get(code, 0) + qty * sign
        except Exception:
            pass

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
                print(f"[Pairs] refresh failed: {e}")

    def run(self):
        self._init_api()

        if self.reconciler:
            self.reconciler.reconcile()

        self.account_monitor = AccountMonitor(self._fetch_account_snapshot, interval_sec=2.0)
        self.account_monitor.start()

        self.keyboard.start()

        t_exec = threading.Thread(target=self._thread_execution_loop, daemon=True, name="ExecLoop")
        t_acct = threading.Thread(target=self._thread_account_sync_loop, daemon=True, name="AccountSync")
        t_pairs = threading.Thread(target=self._thread_refresh_pairs_loop, daemon=True, name="RefreshPairs")

        self._threads = [t_exec, t_acct, t_pairs]
        for t in self._threads:
            t.start()

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

# =========================
# --- main ---
# =========================
def main():
    logging.basicConfig(level=logging.WARNING)  # Suppress INFO logs globally
    logging.getLogger('shioaji').setLevel(logging.ERROR)  # Suppress API logs
    sys.setrecursionlimit(10000)
    system = TradingSystem()
    system.run()

if __name__ == "__main__":
    main()
