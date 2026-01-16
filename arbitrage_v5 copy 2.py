# -*- coding: utf-8 -*-
"""
arbitrage_full.py

✅ 單一完整檔案（不省略）：
- Shioaji stock/future pair arbitrage framework
- Dynamic pair discovery by volume (top N)
- Diff-based subscriptions
- Robust PositionManager (state machine + order indexing + raw order events)
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

import os
import sys
import json
import time
import queue
import datetime
import threading
import concurrent.futures
from dataclasses import dataclass, field, asdict
from enum import Enum
from decimal import Decimal, ROUND_FLOOR, ROUND_CEILING
from typing import Dict, List, Tuple, Optional, Any, Set
from collections import Counter
import threading

import pandas as pd
import shioaji as sj
from dotenv import load_dotenv

load_dotenv()

# =========================
# --- 全域設定 ---
# =========================
SIMULATION = True
ORDER_HISTORY_FILE = "order_history.jsonl"  # 本地訂單記錄檔

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
FUTURE_SHARES_EQUIV = 2000  # ✅ 一口 2000 股

# 交易成本估算（你可自行調整）
STOCK_FEE_RATE = 0.001425 * 0.3
STOCK_TAX_RATE = 0.003
FUT_FEE_PER_CONTRACT = 20
FUT_TAX_RATE = 0.00002

# Hedge/Repair timeout
HEDGE_TIMEOUT_SEC = 5.0
REPAIR_TIMEOUT_SEC = 5.0
MIN_PROFIT_TO_REPAIR = 0

# Tick protection
STOCK_BUY_TICKS = 1
STOCK_SELL_TICKS = 1
FUT_BUY_TICKS = 1
FUT_SELL_TICKS = 1

# Dynamic pair discovery
MIN_VOLUME_THRESHOLD = 500
VOLUME_HISTORY_FILE = "volume_history.json"
TOP_N_PAIRS = 30
REFRESH_PAIRS_EVERY_SEC = 600  # 10分鐘
MAX_SUBS = 80

# Debug watch
DBG_WATCH_STOCK = "2313"
DBG_WATCH_FUT: Optional[str] = None
DBG_LAST_LINE = None
DBG_LINE_LOCK = threading.Lock()

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


# =========================
# --- 狀態列舉/資料結構 ---
# =========================
class PositionState(Enum):
    EMPTY = 0
    PENDING_ENTRY = 1
    HOLDING = 2
    PENDING_EXIT = 3
    UNHEDGED = 4

@dataclass
class LegFill:
    order_id: Optional[str] = None
    status: str = "INIT"           # INIT / Submitted / Filled / Cancelled / Failed / PartFilled
    filled_qty: int = 0
    avg_price: float = 0.0
    last_event: Optional[dict] = None  # raw trade_dict for debugging

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

    state: PositionState = PositionState.EMPTY

    # open/close targets
    target_stock_shares: int = 0
    target_fut_qty: int = 0
    last_signal_net: float = 0.0

    # position snapshot (shares / contracts)
    pos_stock_shares: int = 0
    pos_fut_qty: int = 0

    # open/close legs
    open_stock: LegFill = field(default_factory=LegFill)
    open_future: LegFill = field(default_factory=LegFill)
    close_stock: LegFill = field(default_factory=LegFill)
    close_future: LegFill = field(default_factory=LegFill)

    open_sync: PhaseSync = field(default_factory=PhaseSync)
    close_sync: PhaseSync = field(default_factory=PhaseSync)

    open_ts: float = 0.0
    close_ts: float = 0.0


# =========================
# --- MarketData ---
# =========================
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
    負責將下單記錄寫入本地檔案，以便重啟後重建狀態。
    使用 JSONL (Append Only) 格式，寫入速度快且不易損壞。
    """
    def __init__(self, filename=ORDER_HISTORY_FILE):
        self.filename = filename
        self._lock = threading.Lock()
        
    def record(self, order_id: str, stock_code: str, fut_code: str, phase: str, leg: str, action: str, price: float, qty: int):
        if not order_id: return
        
        record = {
            "ts": time.time(),
            "date": datetime.datetime.now().strftime('%Y-%m-%d'),
            "order_id": str(order_id),
            "stock_code": stock_code,
            "fut_code": fut_code,
            "phase": phase,  # 'open' or 'close'
            "leg": leg,      # 'stock' or 'future'
            "action": str(action),
            "price": price,
            "qty": qty
        }
        
        # Async write or fast append
        threading.Thread(target=self._append_file, args=(record,), daemon=True).start()

    def _append_file(self, record: dict):
        try:
            with self._lock:
                with open(self.filename, 'a', encoding='utf-8') as f:
                    f.write(json.dumps(record) + "\n")
        except Exception as e:
            print(f"[OrderTracker] write failed: {e}")

    def load_history(self) -> List[Dict]:
        """讀取歷史訂單 (只讀取今天的，或者全部讀取後由 Caller 過濾)"""
        records = []
        if not os.path.exists(self.filename):
            return []
            
        today = datetime.datetime.now().strftime('%Y-%m-%d')
        try:
            with self._lock:
                with open(self.filename, 'r', encoding='utf-8') as f:
                    for line in f:
                        line = line.strip()
                        if not line: continue
                        try:
                            rec = json.loads(line)
                            # 只回傳今天的單，避免處理太舊的歷史
                            if rec.get('date') == today:
                                records.append(rec)
                        except: pass
        except Exception as e:
            print(f"[OrderTracker] load failed: {e}")
        return records


# =========================
# --- Reconciler (State Reconstruction) ---
# =========================
class Reconciler:
    """
    負責在系統啟動時，比對「本地訂單記錄」、「券商成交回報」與「實際庫存」，
    來重建 PositionManager 的狀態 (HOLDING, UNHEDGED, etc.)
    """
    def __init__(self, api: sj.Shioaji, pos_mgr: 'PositionManager', order_tracker: OrderTracker):
        self.api = api
        self.pos_mgr = pos_mgr
        self.tracker = order_tracker

    def reconcile(self):
        print(">>> [Reconciler] Starting State Reconstruction...")
        
        # 1. 讀取本地訂單記錄 (了解我們"試圖"做了什麼)
        local_orders = self.tracker.load_history()
        if not local_orders:
            print("  [Reconciler] No local order history today.")
            # 即使沒歷史，也要查庫存 (既有部位)
            self._sync_from_positions_only()
            return

        print(f"  [Reconciler] Found {len(local_orders)} local orders today.")
        
        # 建立 Order ID -> Info 映射
        # 我們只關心"最後一次"對該 Pair 的操作意圖
        # 但比較簡單的是：把所有 Order ID 註冊進 PositionManager，讓它能處理後續回報
        for rec in local_orders:
            oid = rec['order_id']
            self.pos_mgr.register_order(oid, rec['stock_code'], rec['phase'], rec['leg'])
            # 順便恢復 pair 的 future code 對應
            if rec.get('fut_code'):
                self.pos_mgr.set_future_pair(rec['stock_code'], rec['fut_code'])

        # 2. 查詢券商成交明細 (List Trades) - 了解"實際"成交了什麼
        # 注意：Shioaji list_trades 可能需要時間或特定參數，這裡假設能取到今日成交
        print("  [Reconciler] Fetching trades from broker...")
        try:
            # 模擬環境 list_trades 有時會空，實盤較準
            # 這裡我們用 list_trades 來更新 PositionManager 裡的 LegStatus
            # 先更新股票
            stk_trades = self.api.list_trades() 
            # 再更新期貨 (API 可能共用或分開，視版本而定，通常 list_trades 回傳全部)
            # 如果分帳號:
            # stk_trades = self.api.list_trades(self.api.stock_account)
            # fut_trades = self.api.list_trades(self.api.futopt_account)
            
            # 為了簡化，我們先假設 update_leg_status 會被後續的 callback 觸發
            # 或者我們主動呼叫 update_status (Shioaji 功能)
            pass 
        except Exception as e:
            print(f"  [Reconciler] List trades failed: {e}")

        # 3. 查詢券商委託狀態 (List Orders/Update Status) - 了解有哪些還在掛
        # 這一步很重要，如果單子還在 Pending，我們要能接管
        print("  [Reconciler] Syncing order status...")
        try:
            # 針對我們記錄過的 Order ID 去查狀態
            # Shioaji 沒有 batch query by ID，通常是用 update_status() 更新全部
            self.api.update_status(self.api.stock_account)
            self.api.update_status(self.api.futopt_account)
            # update_status 會觸發 callback，所以 _on_order_status 會被呼叫
            # PositionManager 的狀態會因此更新
            time.sleep(2) # 等待 callback
        except Exception as e:
            print(f"  [Reconciler] Update status failed: {e}")

        # 4. 最後：強制同步庫存 (Final Truth)
        # 這是最底層的防線，確保 HOLDING 狀態正確
        self._sync_from_positions_only()
        
        print(">>> [Reconciler] Done.")

    def _sync_from_positions_only(self):
        print("  [Reconciler] Syncing from Positions (Final Check)...")
        # 這裡的邏輯跟 AccountMonitor.sync_positions 類似，但只跑一次
        # 直接呼叫 AccountMonitor 的邏輯 (如果有的話)，或者重寫
        # 為了避免重複代碼，我們這裡簡單實作：讀庫存 -> 寫入 PosMgr
        
        try:
            stk_pos = self.api.list_positions(self.api.stock_account)
            fut_pos = self.api.list_positions(self.api.futopt_account)
            
            real_stk = {}
            real_fut = {}
            
            for p in stk_pos:
                code = getattr(p, 'code', '')
                qty = int(getattr(p, 'quantity', 0))
                # 簡單處理方向：Shioaji Position quantity 總是正的，要看 direction
                direction = getattr(p, 'direction', None)
                sign = 1 if direction == sj.constant.Action.Buy else -1
                real_stk[code] = real_stk.get(code, 0) + (qty * 1000 * sign) # 假設 1張=1000股
            
            for p in fut_pos:
                code = getattr(p, 'code', '')
                qty = int(getattr(p, 'quantity', 0))
                direction = getattr(p, 'direction', None)
                sign = 1 if direction == sj.constant.Action.Buy else -1
                real_fut[code] = real_fut.get(code, 0) + (qty * sign)

            # 更新所有已知的 Pair
            for ps in self.pos_mgr.all_pairs():
                s = ps.stock_code
                f = ps.fut_code
                
                s_sh = real_stk.get(s, 0)
                f_qty = real_fut.get(f, 0) if f else 0
                
                # 如果庫存吻合 (多空配對)，強制設為 HOLDING
                # 例如：股票 +2000 股 (2張), 期貨 -1 口
                if s_sh > 0 and f_qty < 0: 
                    # 簡單比例檢查：每 2000 股對應 1 口 (概略)
                    # 只要有配對，就視為 HOLDING
                    print(f"  [Reconciler] {s} matched positions (S:{s_sh} F:{f_qty}) -> Force HOLDING")
                    ps.state = PositionState.HOLDING
                    ps.pos_stock_shares = s_sh
                    ps.pos_fut_qty = f_qty
                
                elif s_sh != 0 or f_qty != 0:
                     print(f"  [Reconciler] {s} unhedged positions (S:{s_sh} F:{f_qty}) -> UNHEDGED")
                     ps.state = PositionState.UNHEDGED
                     ps.pos_stock_shares = s_sh
                     ps.pos_fut_qty = f_qty

        except Exception as e:
            print(f"  [Reconciler] Position sync failed: {e}")


# =========================
# --- PositionManager (核心) ---
# =========================
class PositionManager:
    """
    ✅ 可量產版：
    - pair 狀態機（PositionState）
    - order_id -> (stock_code, phase, leg) 對照（必修：避免 callback 找不到）
    - raw order events（按 o 查詢）
    - phase sync（ExecutionEngine 等 done/failed）
    - monitor sync（偵測 UNHEDGED）
    """

    def __init__(self):
        self._lock = threading.RLock()
        self._pairs: Dict[str, PairState] = {}
        self._order_index: Dict[str, Tuple[str, str, str]] = {}
        self._order_events: List[Dict[str, Any]] = []

    # ---- pair management ----
    def set_active_pairs(self, pairs: List[Tuple[str, str]]):
        with self._lock:
            for s, f in pairs:
                s = str(s); f = str(f)
                if s not in self._pairs:
                    self._pairs[s] = PairState(stock_code=s, fut_code=f)
                else:
                    self._pairs[s].fut_code = f

    def ensure_pair(self, stock_code: str, fut_code: Optional[str] = None) -> PairState:
        with self._lock:
            if stock_code not in self._pairs:
                self._pairs[stock_code] = PairState(stock_code=stock_code, fut_code=fut_code or "")
            if fut_code:
                self._pairs[stock_code].fut_code = fut_code
            return self._pairs[stock_code]

    def get_pair(self, stock_code: str) -> Optional[PairState]:
        with self._lock:
            return self._pairs.get(stock_code)

    def all_pairs(self) -> List[PairState]:
        with self._lock:
            return list(self._pairs.values())

    def set_future_pair(self, stock_code: str, future_code: str):
        ps = self.ensure_pair(stock_code, future_code)
        with self._lock:
            ps.fut_code = future_code

    # ---- order tracking ----
    def register_order(self, order_id: str, stock_code: str, phase: str, leg: str):
        if not order_id:
            return
        with self._lock:
            self._order_index[str(order_id)] = (stock_code, phase, leg)
            ps = self._pairs.get(stock_code)
            if ps:
                if phase == "open":
                    if leg == "stock":
                        ps.open_stock.order_id = str(order_id)
                        ps.open_stock.status = "Submitted"
                    else:
                        ps.open_future.order_id = str(order_id)
                        ps.open_future.status = "Submitted"
                elif phase == "close":
                    if leg == "stock":
                        ps.close_stock.order_id = str(order_id)
                        ps.close_stock.status = "Submitted"
                    else:
                        ps.close_future.order_id = str(order_id)
                        ps.close_future.status = "Submitted"

    def lookup_order(self, order_id: str) -> Optional[Tuple[str, str, str]]:
        if not order_id:
            return None
        with self._lock:
            return self._order_index.get(str(order_id))

    def record_order_event(self, order_state: Any, trade_dict: Dict[str, Any]):
        try:
            oid = (trade_dict.get("order", {}) or {}).get("id") or trade_dict.get("trade_id")
            status = (trade_dict.get("status", {}) or {}).get("status") or trade_dict.get("status")
            ts = time.time()
            with self._lock:
                self._order_events.append({
                    "ts": ts,
                    "order_id": str(oid) if oid else None,
                    "status": str(status),
                    "raw": trade_dict,
                })
                if len(self._order_events) > 8000:
                    self._order_events = self._order_events[-5000:]
        except Exception:
            pass

    def dump_orders(self, last_n: int = 200) -> List[Dict[str, Any]]:
        with self._lock:
            evs = list(self._order_events[-last_n:])
            idx = dict(self._order_index)

        out: List[Dict[str, Any]] = []
        for e in evs:
            oid = e.get("order_id")
            info = idx.get(str(oid)) if oid else None
            stock_code, phase, leg = info if info else (None, None, None)

            td = e.get("raw") or {}
            st = td.get("status") or {}
            if not isinstance(st, dict):
                st = {}

            out.append({
                "time": datetime.datetime.fromtimestamp(e.get("ts", time.time())).strftime("%H:%M:%S"),
                "order_id": oid,
                "code": stock_code,
                "phase": phase,
                "leg": leg,
                "status": e.get("status"),
                "deal_qty": st.get("deal_quantity"),
                "deal_price": st.get("deal_price") or st.get("price"),
                "msg": st.get("msg") or st.get("errmsg"),
            })
        return out

    # ---- state/phase ----
    def set_state(self, stock_code: str, state: PositionState):
        with self._lock:
            ps = self._pairs.get(stock_code)
            if not ps:
                return
            ps.state = state
        print(f"[State Update] {stock_code} -> {state.name}")

    def prepare_phase(
        self,
        stock_code: str,
        phase: str,
        target_stock_shares: int,
        target_fut_qty: int,
        last_signal_net: float = 0.0,
    ):
        ps = self.ensure_pair(stock_code)
        with self._lock:
            ps.target_stock_shares = int(target_stock_shares)
            ps.target_fut_qty = int(target_fut_qty)
            ps.last_signal_net = float(last_signal_net)

            if phase == "open":
                ps.open_ts = time.time()
                ps.open_sync.reset()
                ps.open_stock = LegFill()
                ps.open_future = LegFill()
            else:
                ps.close_ts = time.time()
                ps.close_sync.reset()
                ps.close_stock = LegFill()
                ps.close_future = LegFill()

    def reset_phase_events_only(self, stock_code: str, phase: str):
        with self._lock:
            ps = self._pairs.get(stock_code)
            if not ps:
                return
            if phase == "open":
                ps.open_sync.reset()
            else:
                ps.close_sync.reset()

    def get_phase_sync(self, stock_code: str, phase: str) -> Optional[PhaseSync]:
        with self._lock:
            ps = self._pairs.get(stock_code)
            if not ps:
                return None
            return ps.open_sync if phase == "open" else ps.close_sync

    def update_leg_status(
        self,
        stock_code: str,
        phase: str,
        leg: str,
        status: str,
        filled_qty: int,
        deal_price: float,
        last_event: Optional[dict] = None,
    ):
        """
        ✅ 修復你 p 畫面「status 變成 dict」的主因：
        - 永遠把 status 存成 str
        - raw event 只放 last_event
        """
        ps = self.ensure_pair(stock_code)
        status_s = str(status)

        with self._lock:
            if phase == "open":
                lf = ps.open_stock if leg == "stock" else ps.open_future
            else:
                lf = ps.close_stock if leg == "stock" else ps.close_future

            lf.status = status_s
            lf.last_event = last_event

            # fill accumulation
            if filled_qty and filled_qty > 0 and deal_price and deal_price > 0:
                prev_qty = lf.filled_qty
                prev_px = lf.avg_price
                new_qty = prev_qty + int(filled_qty)
                if new_qty > 0:
                    lf.avg_price = (prev_qty * prev_px + int(filled_qty) * float(deal_price)) / new_qty
                lf.filled_qty = new_qty

            # sync done/fail logic
            sync = ps.open_sync if phase == "open" else ps.close_sync
            leg_a = (ps.open_stock, ps.open_future) if phase == "open" else (ps.close_stock, ps.close_future)

            def _is_filled(x: LegFill) -> bool:
                return (x.status == str(sj.constant.Status.Filled)) or (str(x.status).upper() == "FILLED")

            def _is_failed(x: LegFill) -> bool:
                return (x.status == str(sj.constant.Status.Failed)) or (str(x.status).upper() == "FAILED")

            def _is_cancel(x: LegFill) -> bool:
                return (x.status == str(sj.constant.Status.Cancelled)) or (str(x.status).upper() == "CANCELLED")

            a, b = leg_a
            if _is_filled(a) and _is_filled(b):
                sync.done.set()
            else:
                ended_a = _is_failed(a) or _is_cancel(a)
                ended_b = _is_failed(b) or _is_cancel(b)
                if ended_a and ended_b:
                    sync.failed.set()

        if status_s == str(sj.constant.Status.Filled):
            print(f"[成交] {phase.upper()} {leg.upper()} {stock_code} qty={filled_qty} px={deal_price}")
        elif status_s in [str(sj.constant.Status.Cancelled), str(sj.constant.Status.Failed)]:
            print(f"[取消/失敗] {phase.upper()} {leg.upper()} {stock_code} status={status_s}")

    # ---- monitor sync ----
    def monitor_sync(self, stock_code: str, pos_stock_shares: int, pos_fut_qty: int):
        ps = self.ensure_pair(stock_code)
        with self._lock:
            ps.pos_stock_shares = int(pos_stock_shares)
            ps.pos_fut_qty = int(pos_fut_qty)

            # 1. 偵測 UNHEDGED (單邊)
            if (ps.pos_stock_shares != 0 and ps.pos_fut_qty == 0) or (ps.pos_stock_shares == 0 and ps.pos_fut_qty != 0):
                if ps.state not in (PositionState.PENDING_ENTRY, PositionState.PENDING_EXIT):
                    ps.state = PositionState.UNHEDGED
            
            # 2. 偵測既有部位 (雙邊都有) -> 自動設為 HOLDING
            # 簡單判斷：股票是正的 (Buy)，期貨是負的 (Sell)，且都有量
            elif ps.pos_stock_shares > 0 and ps.pos_fut_qty < 0:
                # 如果當前狀態是 EMPTY，代表剛啟動或狀態遺失，強制同步為 HOLDING
                if ps.state == PositionState.EMPTY:
                    print(f"[Monitor Sync] {stock_code} Detect existing positions -> Force HOLDING")
                    ps.state = PositionState.HOLDING

        tag = "UNHEDGED" if ps.state == PositionState.UNHEDGED else ("HOLDING" if ps.state == PositionState.HOLDING else "OK")
        print(f"[Monitor Sync] {stock_code} -> {tag} (stock={ps.pos_stock_shares}, fut={ps.pos_fut_qty})")

    # ---- printing ----
    def dump_positions_pretty(self) -> str:
        with self._lock:
            pairs = list(self._pairs.values())

        lines = []
        lines.append("\n=== POSITIONS ===")
        for ps in pairs:
            def _lf(x: LegFill) -> str:
                oid = x.order_id if x.order_id else "-"
                return f"{x.status}/{x.filled_qty}@{x.avg_price:.2f} oid={oid}"

            lines.append(
                f"{ps.stock_code}/{ps.fut_code} "
                f"state={ps.state.name:<12} "
                f"pos(S)={ps.pos_stock_shares:<6} pos(F)={ps.pos_fut_qty:<4} "
                f"open(S:{_lf(ps.open_stock)} F:{_lf(ps.open_future)}) "
                f"close(S:{_lf(ps.close_stock)} F:{_lf(ps.close_future)}) "
                f"net(last)={ps.last_signal_net:,.0f}"
            )
        lines.append("")
        return "\n".join(lines)


# =========================
# --- Keyboard monitor ---
# =========================
try:
    import termios, tty, select  # mac/linux
    _HAS_TERMIOS = True
except Exception:
    _HAS_TERMIOS = False

try:
    import msvcrt  # windows
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
                    f"{r['time']} oid={r['order_id']} "
                    f"code={r['code'] or '-'} {r['phase'] or '-'} {r['leg'] or '-'} "
                    f"st={r['status']} qty={r['deal_qty']} px={r['deal_price']} msg={r['msg'] or ''}"
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
# --- Account Monitor ---
# =========================
class AccountMonitor(threading.Thread):
    """
    定時 list_positions 同步到 PositionManager，用來偵測 UNHEDGED
    """
    def __init__(
        self,
        api: sj.Shioaji,
        pos_mgr: PositionManager,
        stock_shares_per_lot: int,
        poll_sec: float = 5.0,
    ):
        super().__init__(daemon=True)
        self.api = api
        self.pos_mgr = pos_mgr
        self.stock_shares_per_lot = int(stock_shares_per_lot)
        self.poll_sec = float(poll_sec)
        self.running = True

        self.stock_account = api.stock_account
        self.futopt_account = api.futopt_account

    def stop(self):
        self.running = False

    def run(self):
        print(">>> Account Monitor Started")
        while self.running:
            try:
                self.sync_positions()
            except Exception as e:
                print(f"[Monitor Error] Sync failed: {e}")

            n = int(max(1, self.poll_sec * 10))
            for _ in range(n):
                if not self.running:
                    break
                time.sleep(0.1)

    def sync_positions(self):
        try:
            stock_positions = self.api.list_positions(account=self.stock_account)
        except Exception:
            stock_positions = []

        try:
            future_positions = self.api.list_positions(account=self.futopt_account)
        except Exception:
            future_positions = []

        real_stock_shares: Dict[str, int] = {}
        real_future_qty: Dict[str, int] = {}

        for pos in stock_positions:
            code = getattr(pos, "code", None)
            if not code:
                continue
            qty_lot = int(getattr(pos, "quantity", 0) or 0)
            direction = getattr(pos, "direction", None)
            sign = 1 if direction == sj.constant.Action.Buy else -1
            real_stock_shares[str(code)] = real_stock_shares.get(str(code), 0) + sign * qty_lot * self.stock_shares_per_lot

        for pos in future_positions:
            code = getattr(pos, "code", None)
            if not code:
                continue
            qty = int(getattr(pos, "quantity", 0) or 0)
            direction = getattr(pos, "direction", None)
            sign = 1 if direction == sj.constant.Action.Buy else -1
            real_future_qty[str(code)] = real_future_qty.get(str(code), 0) + sign * qty

        for ps in self.pos_mgr.all_pairs():
            s = ps.stock_code
            f = ps.fut_code

            s_sh = real_stock_shares.get(s, 0)
            f_qty = real_future_qty.get(f, 0) if f else 0

            if ps.state in (PositionState.PENDING_ENTRY, PositionState.PENDING_EXIT):
                continue

            self.pos_mgr.monitor_sync(s, s_sh, f_qty)


# =========================
# --- VolumeManager / PairDiscoverer ---
# =========================
class VolumeManager:
    def __init__(self, filename=VOLUME_HISTORY_FILE):
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
    掃描期貨合約，找出 underlying_code + delivery_month=當月 的個股期貨，
    然後用 stock/future 的 volume 取 min 做 score，回傳 top N。
    """
    def __init__(self, api: sj.Shioaji):
        self.api = api
        self.vol_mgr = VolumeManager()
        self.daily_quotes_map: Dict[str, int] = {}
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
        if contract_obj:
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

        if code in self.daily_quotes_map:
            dq_vol = int(self.daily_quotes_map[code] or 0)
            if dq_vol > MIN_VOLUME_THRESHOLD:
                self.vol_mgr.update_window_init(cache_key, [dq_vol])
                return dq_vol

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

    def find_active_pairs(self, top_n: int = TOP_N_PAIRS) -> List[Tuple[str, str]]:
        print("[PairDiscoverer] scanning active stock-future pairs...")
        today = datetime.datetime.now()
        target_month_str = today.strftime('%Y%m')

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
                if isinstance(contract.code, str) and contract.code.endswith("R1"):
                    continue
                if getattr(contract, "category", "") == "QEF":
                    continue

                seen_contracts.add(contract.code)

                try:
                    is_futures = str(contract.security_type) in ['FUT', 'Future', str(sj.constant.SecurityType.Future)]
                    if (
                        is_futures
                        and hasattr(contract, 'underlying_code')
                        and contract.underlying_code
                        and len(contract.underlying_code) == 4
                        and getattr(contract, "delivery_month", "") == target_month_str
                    ):
                        candidates.append(contract)
                except Exception:
                    continue

        # --- Batch Optimization Start ---
        # 1. 收集所有候選者的現貨 Contract
        stock_contracts_map = {}
        for contract in candidates:
            scode = str(contract.underlying_code)
            try:
                # 預先抓取 contract 物件，準備做 batch snapshot
                if scode not in stock_contracts_map:
                    stock_contracts_map[scode] = self.api.Contracts.Stocks[scode]
            except: pass
            
        print(f"  [PairDiscoverer] Batch querying {len(stock_contracts_map)} stocks...")
        
        # 2. 批次 Snapshot (分批，例如一次 200 檔)
        stock_info_cache = {} # code -> (price, volume)
        all_stk_contracts = list(stock_contracts_map.values())
        
        chunk_size = 200
        for i in range(0, len(all_stk_contracts), chunk_size):
            chunk = all_stk_contracts[i:i+chunk_size]
            try:
                snaps = self.api.snapshots(chunk)
                for s in snaps:
                    price = float(getattr(s, "close", 0) or getattr(s, "reference_price", 0) or 0)
                    vol = int(getattr(s, "total_volume", 0) or 0)
                    stock_info_cache[s.code] = (price, vol)
            except Exception as e:
                print(f"  [PairDiscoverer] Snapshot chunk failed: {e}")
                time.sleep(0.5)

        print(f"  [PairDiscoverer] Got info for {len(stock_info_cache)} stocks.")

        best_by_pair = {}
        for contract in candidates:
            stock_code = str(contract.underlying_code)
            future_code = str(contract.code)

            # --- Fast Filter by Cache ---
            # 從剛剛的 Batch Snapshot 結果中查資料
            info = stock_info_cache.get(stock_code)
            
            # 如果沒有 Snapshot 資料 (可能沒開盤或冷門)，才走舊的慢速查詢 (Fallback)
            if info:
                s_price, s_vol = info
                # 1. Volume Check
                if s_vol < MIN_VOLUME_THRESHOLD:
                    # 如果 Snapshot 量不足，也許可以查 KBar (但通常 Snapshot 量不足就代表不活躍)
                    # 為了效能，這裡從嚴認定：Snapshot 量不夠就跳過 (盤中)
                    # 若是盤前，Snapshot vol 可能是 0，這時才 fallback
                    if s_vol == 0 and not self._is_day_market_closed():
                         pass # 盤中量為0 -> 跳過
                    elif s_vol > 0:
                         pass # 繼續檢查
                    else:
                         # 盤前或無量，嘗試查 Avg (舊邏輯)
                         s_vol = self._get_avg_volume_smart(stock_code, contract_obj=None)
                else:
                    # Snapshot 量足夠，直接用
                    pass
                
                # 更新 cache 供後續使用
                if s_vol > 0:
                    stock_avg = s_vol
                else:
                    stock_avg = self._get_avg_volume_smart(stock_code, contract_obj=None)

                # 2. Price Check (Fast)
                if s_price > 500:
                    continue
            else:
                # Fallback: 沒抓到 Snapshot，走舊流程 (慢)
                stock_avg = self._get_avg_volume_smart(stock_code, contract_obj=None)
                # 這裡略過 Price Check (為了省時)，或者再 call 一次單一 snapshot
                pass

            if stock_avg < MIN_VOLUME_THRESHOLD:
                continue

            # --- Future Liquidity Check ---
            # 期貨通常量比現貨少，這裡可以維持原樣，或也做 batch (期貨 batch snapshot 比較容易失敗，暫維持單一)
            future_avg = self._get_avg_volume_smart(future_code, contract_obj=contract)
            if future_avg < MIN_VOLUME_THRESHOLD:
                continue

            score = min(stock_avg, future_avg)
            key = (stock_code, future_code)
            rec = (score, stock_avg, future_avg, stock_code, future_code)
            if key not in best_by_pair or rec[0] > best_by_pair[key][0]:
                best_by_pair[key] = rec

        ranked = list(best_by_pair.values())
        ranked.sort(key=lambda x: x[0], reverse=True)

        top = ranked[:top_n]
        print(f"[PairDiscoverer] done. matched={len(ranked)} return={len(top)}")
        return [(s_code, f_code) for (_, _, _, s_code, f_code) in top]


# =========================
# --- SubscriptionManager (diff + refcount) ---
# =========================
class SubscriptionManager:
    def __init__(
        self,
        api: sj.Shioaji,
        max_subs: int = 80,
        quote_type=None,
        quote_version=None,
        verbose: bool = True,
    ):
        self.api = api
        self.max_subs = int(max_subs)
        self.verbose = verbose

        self.quote_type = quote_type or sj.constant.QuoteType.BidAsk
        self.quote_version = quote_version or sj.constant.QuoteVersion.v1

        self._refcnt: Counter[str] = Counter()
        self._kind: Dict[str, str] = {}
        self._subscribed: Set[str] = set()

        self._lock = threading.RLock()

    def apply_pairs(self, pairs: List[Tuple[str, Optional[str]]], log_prefix: str = "") -> None:
        with self._lock:
            desired = self._build_desired_refcnt(pairs)
            desired_kind = self._infer_kind_map(pairs)

            to_sub = []
            to_unsub = []

            for code, new_cnt in desired.items():
                old_cnt = self._refcnt.get(code, 0)
                if old_cnt == 0 and new_cnt > 0:
                    to_sub.append(code)

            for code, old_cnt in list(self._refcnt.items()):
                new_cnt = desired.get(code, 0)
                if old_cnt > 0 and new_cnt == 0:
                    to_unsub.append(code)

            projected = (self._subscribed - set(to_unsub)) | set(to_sub)
            if len(projected) > self.max_subs:
                raise RuntimeError(f"[Subscription] projected_subs={len(projected)} exceeds max_subs={self.max_subs}")

            self._batch_unsubscribe(to_unsub, log_prefix=log_prefix)
            self._batch_subscribe(to_sub, desired_kind, log_prefix=log_prefix)

            self._refcnt = desired
            self._kind = desired_kind
            self._subscribed = {c for c, cnt in self._refcnt.items() if cnt > 0}

            if self.verbose:
                print(f"{log_prefix} now subscribed={len(self._subscribed)} (limit={self.max_subs})")

    def force_unsubscribe_all(self, log_prefix: str = ""):
        with self._lock:
            codes = list(self._subscribed)
            self._batch_unsubscribe(codes, log_prefix=log_prefix)
            self._refcnt.clear()
            self._kind.clear()
            self._subscribed.clear()

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
            return self.api.Contracts.Futures[code]
        raise ValueError(f"Unknown kind={kind} code={code}")

    def _batch_subscribe(self, codes: List[str], desired_kind: Dict[str, str], log_prefix: str = ""):
        ok = 0
        for code in codes:
            kind = desired_kind.get(code) or self._guess_kind_by_contracts(code)
            try:
                c = self._get_contract(code, kind)
                self.api.quote.subscribe(c, quote_type=self.quote_type, version=self.quote_version)
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
                ok += 1
            except Exception as e:
                print(f"{log_prefix} -unsubscribe {code} FAILED: {e}")
        if self.verbose and codes:
            print(f"{log_prefix} -unsubscribe {len(codes)} (ok={ok})")


# =========================
# --- CapitalManager (資金控管) ---
# =========================
class CapitalManager:
    def __init__(self, max_capital: float, pos_mgr: PositionManager, market_data: MarketData):
        self.max_capital = float(max_capital)
        self.pos_mgr = pos_mgr
        self.market_data = market_data

    def get_usage(self) -> Tuple[float, float, float]:
        """
        計算目前資金使用狀況。
        回傳: (總使用資金, 持倉市值, 圈存/處理中資金)
        """
        holding_val = 0.0
        pending_val = 0.0
        
        # 為了避免頻繁 lock，我們 copy 一份 pairs
        pairs = self.pos_mgr.all_pairs()
        
        for ps in pairs:
            # 1. 持倉部位 (HOLDING, UNHEDGED)
            # 只計算現貨成本 (期貨是保證金，這裡先簡化只控管股票總市值)
            if ps.pos_stock_shares > 0:
                # 嘗試取得即時報價來計算市值
                quote = self.market_data.get_stock(ps.stock_code)
                # 若無即時報價，理想上應有成本價，這裡暫用 Snapshot 邏輯或 0 (保守)
                # 實務上建議 PairState 裡要記 avg_price，這裡先用即時價估算
                price = _get_bid(quote)
                if price <= 0:
                    # Fallback: 如果完全沒報價，可能要用漲停價或參考價估，這裡先忽略避免阻擋
                    # 或者從 ps.open_stock.avg_price 拿 (如果有)
                    price = ps.open_stock.avg_price or 0.0
                
                holding_val += (ps.pos_stock_shares * price)

            # 2. 處理中部位 (PENDING_ENTRY)
            # 這筆錢還沒成交，但我們已經送單，必須預留
            if ps.state == PositionState.PENDING_ENTRY:
                # 預估買進金額
                quote = self.market_data.get_stock(ps.stock_code)
                price = _get_ask(quote)
                if price <= 0:
                    # 嘗試用下單時的價格 (如果有記錄) 
                    # 這裡暫時無法取得 order price，只好用 0 或略過
                    pass
                
                # target_stock_shares 應該在 prepare_phase 被設定了
                pending_val += (ps.target_stock_shares * price)

        total_used = holding_val + pending_val
        return total_used, holding_val, pending_val

    def check_available(self, required_amount: float) -> bool:
        used, _, _ = self.get_usage()
        remaining = self.max_capital - used
        if remaining >= required_amount:
            return True
        return False

    def log_status(self):
        used, h, p = self.get_usage()
        print(f"[Capital] Max: {self.max_capital/10000:.0f}萬 | Used: {used/10000:.0f}萬 (Hold:{h/10000:.0f} Pending:{p/10000:.0f}) | Rem: {(self.max_capital-used)/10000:.0f}萬")


# =========================
# --- StrategyEngine ---
# =========================
class TradeSignal(dict):
    pass

class StrategyEngine:
    def __init__(self, market_data: MarketData, pos_mgr: PositionManager, capital_mgr: CapitalManager):
        self.market_data = market_data
        self.pos_mgr = pos_mgr
        self.capital_mgr = capital_mgr  # Inject
        self._pairs_lock = threading.RLock()
        self.pairs: List[Tuple[str, str]] = []
        self.pair_map: Dict[str, List[str]] = {}
        self.future_to_stock: Dict[str, str] = {}

    def update_pairs(self, pairs: List[Tuple[str, str]]):
        with self._pairs_lock:
            self.pairs = list(pairs)
            pair_map: Dict[str, List[str]] = {}
            future_to_stock: Dict[str, str] = {}
            for s, f in self.pairs:
                pair_map.setdefault(s, []).append(f)
                future_to_stock[f] = s
            self.pair_map = pair_map
            self.future_to_stock = future_to_stock
        print(f"[Strategy] pairs updated: {len(self.pairs)}")

    def calculate_cost(self, stock_price: float, future_price: float) -> float:
        stock_notional = stock_price * STOCK_SHARES_PER_LOT * STOCK_QTY
        future_notional = future_price * FUTURE_SHARES_EQUIV * FUTURE_QTY
        stock_fee = stock_notional * STOCK_FEE_RATE
        future_fee = FUT_FEE_PER_CONTRACT * FUTURE_QTY
        tax_stock = stock_notional * STOCK_TAX_RATE
        tax_future = future_notional * FUT_TAX_RATE
        return stock_fee + future_fee + tax_stock + tax_future

    def on_quote(self, quote: Any) -> Optional[TradeSignal]:
        code = _get_code(quote)
        if not code:
            return None

        with self._pairs_lock:
            pair_map = self.pair_map
            future_to_stock = self.future_to_stock

        target_stock = None
        futures = []

        if code in pair_map:
            target_stock = code
            futures = pair_map[code]
        elif code in future_to_stock:
            target_stock = future_to_stock[code]
            futures = [code]
        else:
            return None

        ps = self.pos_mgr.get_pair(target_stock)
        if not ps:
            return None

        if ps.state in (PositionState.PENDING_ENTRY, PositionState.PENDING_EXIT, PositionState.UNHEDGED):
            return None

        # ---- ENTRY ----
        if ps.state == PositionState.EMPTY:
            for f_code in futures:
                s_q, f_q = self.market_data.get_quotes(target_stock, f_code)
                if not s_q or not f_q:
                    continue
                s_ask = _get_ask(s_q)
                f_bid = _get_bid(f_q)
                if s_ask <= 0 or f_bid <= 0:
                    continue

                # --- Volume Filter ---
                # 確保市場流動性足夠吃下我們的單，避免滑價
                s_ask_vol = _get_ask_vol(s_q)
                f_bid_vol = _get_bid_vol(f_q)
                
                if s_ask_vol < MIN_TOP_VOLUME or f_bid_vol < MIN_TOP_VOLUME:
                    # print(f"[Strategy] Skip {target_stock}: Low Volume (S_Ask:{s_ask_vol} F_Bid:{f_bid_vol})")
                    continue
                # ---------------------

                stock_notional = s_ask * STOCK_SHARES_PER_LOT * STOCK_QTY
                
                # --- Capital Check ---
                if not self.capital_mgr.check_available(stock_notional):
                    # 降低 log 頻率，或是只印一次
                    # print(f"[Strategy] Skip {target_stock}: Capital Insufficient (Need {stock_notional:,.0f})")
                    continue
                # ---------------------

                future_notional = f_bid * FUTURE_SHARES_EQUIV * FUTURE_QTY
                spread_value = future_notional - stock_notional
                cost = self.calculate_cost(s_ask, f_bid)
                net = spread_value - cost

                if net > ENTRY_THRESHOLD:
                    self.pos_mgr.set_future_pair(target_stock, f_code)
                    self.pos_mgr.set_state(target_stock, PositionState.PENDING_ENTRY)
                    return TradeSignal({
                        "type": "OPEN",
                        "stock_code": target_stock,
                        "future_code": f_code,
                        "stock_px": s_ask,
                        "fut_px": f_bid,
                        "net": net,
                    })
            return None

        # ---- EXIT ----
        if ps.state == PositionState.HOLDING:
            f_code = ps.fut_code
            if not f_code:
                self.pos_mgr.set_state(target_stock, PositionState.UNHEDGED)
                return None

            s_q, f_q = self.market_data.get_quotes(target_stock, f_code)
            if not s_q or not f_q:
                return None

            s_bid = _get_bid(s_q)
            f_ask = _get_ask(f_q)
            
            # --- Volume Check (New) ---
            s_bid_vol = _get_bid_vol(s_q)
            f_ask_vol = _get_ask_vol(f_q)

            if s_bid <= 0 or f_ask <= 0: return None
            
            if s_bid_vol < MIN_ORDER_QTY_THRESHOLD or f_ask_vol < MIN_ORDER_QTY_THRESHOLD:
                # print(f"[Strategy] Skip CLOSE {target_stock}: Low Liquidity (S_BidVol:{s_bid_vol} F_AskVol:{f_ask_vol})")
                return None
            # --------------------------

            stock_notional = s_bid * STOCK_SHARES_PER_LOT * STOCK_QTY
            future_notional = f_ask * FUTURE_SHARES_EQUIV * FUTURE_QTY
            current_spread = future_notional - stock_notional

            if current_spread <= EXIT_THRESHOLD:
                self.pos_mgr.set_state(target_stock, PositionState.PENDING_EXIT)
                return TradeSignal({
                    "type": "CLOSE",
                    "stock_code": target_stock,
                    "future_code": f_code,
                    "stock_px": s_bid,
                    "fut_px": f_ask,
                    "net": current_spread,
                })

        return None


# =========================
# --- ExecutionEngine ---
# =========================
class ExecutionEngine:
    def __init__(self, market_data: MarketData, pos_mgr: PositionManager, order_tracker: OrderTracker):
        self.market_data = market_data
        self.pos_mgr = pos_mgr
        self.tracker = order_tracker

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
        if side == "buy":
            q2 = q.to_integral_value(rounding=ROUND_CEILING)
        else:
            q2 = q.to_integral_value(rounding=ROUND_FLOOR)
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
        n = int(max(0, protect_ticks))
        for _ in range(n):
            px = self._step_one_tick_strict(px, side)
        px = self._round_to_tick_by_its_own_tier(px, side)
        return px

    def _cancel_best_effort(self, api: sj.Shioaji, trade_obj) -> bool:
        try:
            api.cancel_order(trade_obj)
            return True
        except Exception:
            return False

    def _place_stock_order(self, api: sj.Shioaji, stock_code: str, action, price: float, qty_lot: int):
        contract_s = api.Contracts.Stocks[stock_code]
        order_s = api.Order(
            price=float(price),
            quantity=int(qty_lot),
            action=action,
            price_type=sj.constant.StockPriceType.LMT,
            order_type=sj.constant.OrderType.ROD if hasattr(sj.constant, "OrderType") else "ROD",
            order_lot=sj.constant.StockOrderLot.Common if hasattr(sj.constant, "StockOrderLot") else "Common",
            account=api.stock_account
        )
        return api.place_order(contract_s, order_s)

    def _place_future_order(self, api: sj.Shioaji, future_code: str, action, price: float, qty: int, phase_open: bool):
        contract_f = api.Contracts.Futures[future_code]
        octype = sj.constant.FuturesOCType.New if phase_open else sj.constant.FuturesOCType.Cover \
            if hasattr(sj.constant, "FuturesOCType") else ("New" if phase_open else "Cover")
        price_type_f = sj.constant.FuturesPriceType.LMT if hasattr(sj.constant, "FuturesPriceType") else sj.constant.StockPriceType.LMT
        order_type_f = sj.constant.FuturesOrderType.ROD if hasattr(sj.constant, "FuturesOrderType") else (
            sj.constant.OrderType.ROD if hasattr(sj.constant, "OrderType") else "ROD"
        )

        order_f = api.Order(
            action=action,
            price=float(price),
            quantity=int(qty),
            price_type=price_type_f,
            order_type=order_type_f,
            octype=octype,
            account=api.futopt_account
        )
        return api.place_order(contract_f, order_f)

    def _calc_cost(self, stock_price: float, future_price: float) -> float:
        stock_notional = stock_price * STOCK_SHARES_PER_LOT * STOCK_QTY
        future_notional = future_price * FUTURE_SHARES_EQUIV * FUTURE_QTY
        stock_fee = stock_notional * STOCK_FEE_RATE
        future_fee = FUT_FEE_PER_CONTRACT * FUTURE_QTY
        tax_stock = stock_notional * STOCK_TAX_RATE
        tax_future = future_notional * FUT_TAX_RATE
        return stock_fee + future_fee + tax_stock + tax_future

    def _calc_open_net(self, stock_buy: float, future_sell: float) -> float:
        spread = (future_sell * FUTURE_SHARES_EQUIV * FUTURE_QTY) - (stock_buy * STOCK_SHARES_PER_LOT * STOCK_QTY)
        return spread - self._calc_cost(stock_buy, future_sell)

    def _get_top_prices(self, stock_code: str, future_code: str):
        s_q, f_q = self.market_data.get_quotes(stock_code, future_code)
        if not s_q or not f_q:
            return None
        s_ask = _get_ask(s_q)
        s_bid = _get_bid(s_q)
        f_ask = _get_ask(f_q)
        f_bid = _get_bid(f_q)
        return s_ask, s_bid, f_ask, f_bid

    def on_signal(self, sig: Dict[str, Any], api: sj.Shioaji):
        typ = sig.get("type")
        stock_code = str(sig.get("stock_code"))
        future_code = str(sig.get("future_code"))
        init_stock_px = float(sig.get("stock_px") or 0.0)
        init_fut_px = float(sig.get("fut_px") or 0.0)
        net = float(sig.get("net") or 0.0)

        is_open = (typ == "OPEN")
        phase = "open" if is_open else "close"

        self.pos_mgr.prepare_phase(
            stock_code=stock_code,
            phase=phase,
            target_stock_shares=STOCK_QTY * STOCK_SHARES_PER_LOT,
            target_fut_qty=FUTURE_QTY,
            last_signal_net=net,
        )

        stock_action = sj.constant.Action.Buy if is_open else sj.constant.Action.Sell
        future_action = sj.constant.Action.Sell if is_open else sj.constant.Action.Buy

        if is_open:
            stock_px = self._mk_mktable_price_strict("buy", init_stock_px, STOCK_BUY_TICKS)
            fut_px   = self._mk_mktable_price_strict("sell", init_fut_px, FUT_SELL_TICKS)
        else:
            stock_px = self._mk_mktable_price_strict("sell", init_stock_px, STOCK_SELL_TICKS)
            fut_px   = self._mk_mktable_price_strict("buy", init_fut_px, FUT_BUY_TICKS)

        trade_s = None
        trade_f = None

        try:
            with concurrent.futures.ThreadPoolExecutor(max_workers=2) as ex:
                fs = ex.submit(self._place_stock_order, api, stock_code, stock_action, stock_px, STOCK_QTY)
                ff = ex.submit(self._place_future_order, api, future_code, future_action, fut_px, FUTURE_QTY, is_open)
                trade_s = fs.result()
                trade_f = ff.result()

            # --- Record to Persistence ---
            self.tracker.record(
                str(trade_s.order.id), stock_code, future_code, phase, "stock", 
                stock_action, stock_px, STOCK_QTY
            )
            self.tracker.record(
                str(trade_f.order.id), stock_code, future_code, phase, "future", 
                future_action, fut_px, FUTURE_QTY
            )
            # -----------------------------

            self.pos_mgr.register_order(str(trade_s.order.id), stock_code, phase, "stock")
            self.pos_mgr.register_order(str(trade_f.order.id), stock_code, phase, "future")

            print(f"[{phase.upper()}] orders sent: stock={trade_s.order.id}, future={trade_f.order.id}")
        except Exception as e:
            print(f"[{phase.upper()}] send orders failed: {e}")
            self.pos_mgr.set_state(stock_code, PositionState.EMPTY if is_open else PositionState.HOLDING)
            return

        sync = self.pos_mgr.get_phase_sync(stock_code, phase)
        if not sync:
            self.pos_mgr.set_state(stock_code, PositionState.UNHEDGED)
            return

        t0 = time.time()
        while True:
            if sync.done.is_set():
                self.pos_mgr.set_state(stock_code, PositionState.HOLDING if is_open else PositionState.EMPTY)
                return
            if sync.failed.is_set():
                break
            if time.time() - t0 > HEDGE_TIMEOUT_SEC:
                break
            time.sleep(0.02)

        ps = self.pos_mgr.get_pair(stock_code)
        if not ps:
            self.pos_mgr.set_state(stock_code, PositionState.UNHEDGED)
            return

        if phase == "open":
            st = ps.open_stock.status
            ft = ps.open_future.status
        else:
            st = ps.close_stock.status
            ft = ps.close_future.status

        filled_s = (st == str(sj.constant.Status.Filled)) or (str(st).upper() == "FILLED")
        filled_f = (ft == str(sj.constant.Status.Filled)) or (str(ft).upper() == "FILLED")

        if (not filled_s) and (not filled_f):
            print(f"[{phase.upper()}] no leg filled -> cancel both (best effort)")
            if trade_s: self._cancel_best_effort(api, trade_s)
            if trade_f: self._cancel_best_effort(api, trade_f)
            self.pos_mgr.set_state(stock_code, PositionState.EMPTY if is_open else PositionState.HOLDING)
            return

        print(f"[{phase.upper()}] single-leg filled -> REPAIR/UNWIND")

        top = self._get_top_prices(stock_code, future_code)
        if not top:
            print("  [Repair] no quotes -> set UNHEDGED")
            self.pos_mgr.set_state(stock_code, PositionState.UNHEDGED)
            return

        s_ask, s_bid, f_ask, f_bid = top

        if is_open:
            if filled_f and (not filled_s):
                hedge_stock_buy = self._mk_mktable_price_strict("buy", s_ask, STOCK_BUY_TICKS)
                est_net = self._calc_open_net(hedge_stock_buy, f_bid)
                if est_net >= MIN_PROFIT_TO_REPAIR:
                    print(f"  [Repair] hedge stock buy @ {hedge_stock_buy:.2f}, est_net={est_net:.0f}")
                    self.pos_mgr.reset_phase_events_only(stock_code, phase)
                    try:
                        trade_fix = self._place_stock_order(api, stock_code, sj.constant.Action.Buy, hedge_stock_buy, STOCK_QTY)
                        self.pos_mgr.register_order(str(trade_fix.order.id), stock_code, phase, "stock")
                    except Exception as e:
                        print(f"  [Repair] stock hedge order failed: {e}")
                        self.pos_mgr.set_state(stock_code, PositionState.UNHEDGED)
                        return
                    if sync.done.wait(timeout=REPAIR_TIMEOUT_SEC):
                        self.pos_mgr.set_state(stock_code, PositionState.HOLDING)
                        return

                unwind_fut_buy = self._mk_mktable_price_strict("buy", f_ask, FUT_BUY_TICKS)
                print(f"  [Unwind] buy back future @ {unwind_fut_buy:.2f}")
                try:
                    self._place_future_order(api, future_code, sj.constant.Action.Buy, unwind_fut_buy, FUTURE_QTY, phase_open=False)
                except Exception as e:
                    print(f"  [Unwind] future buyback failed: {e}")
                    self.pos_mgr.set_state(stock_code, PositionState.UNHEDGED)
                    return
                self.pos_mgr.set_state(stock_code, PositionState.EMPTY)
                return

            if filled_s and (not filled_f):
                hedge_fut_sell = self._mk_mktable_price_strict("sell", f_bid, FUT_SELL_TICKS)
                est_net = self._calc_open_net(s_ask, hedge_fut_sell)
                if est_net >= MIN_PROFIT_TO_REPAIR:
                    print(f"  [Repair] hedge future sell @ {hedge_fut_sell:.2f}, est_net={est_net:.0f}")
                    self.pos_mgr.reset_phase_events_only(stock_code, phase)
                    try:
                        trade_fix = self._place_future_order(api, future_code, sj.constant.Action.Sell, hedge_fut_sell, FUTURE_QTY, phase_open=True)
                        self.pos_mgr.register_order(str(trade_fix.order.id), stock_code, phase, "future")
                    except Exception as e:
                        print(f"  [Repair] future hedge order failed: {e}")
                        self.pos_mgr.set_state(stock_code, PositionState.UNHEDGED)
                        return
                    if sync.done.wait(timeout=REPAIR_TIMEOUT_SEC):
                        self.pos_mgr.set_state(stock_code, PositionState.HOLDING)
                        return

                unwind_stock_sell = self._mk_mktable_price_strict("sell", s_bid, STOCK_SELL_TICKS)
                print(f"  [Unwind] sell stock @ {unwind_stock_sell:.2f}")
                try:
                    self._place_stock_order(api, stock_code, sj.constant.Action.Sell, unwind_stock_sell, STOCK_QTY)
                except Exception as e:
                    print(f"  [Unwind] stock sell failed: {e}")
                    self.pos_mgr.set_state(stock_code, PositionState.UNHEDGED)
                    return
                self.pos_mgr.set_state(stock_code, PositionState.EMPTY)
                return

        else:
            if filled_f and (not filled_s):
                hedge_stock_sell = self._mk_mktable_price_strict("sell", s_bid, STOCK_SELL_TICKS)
                print(f"  [Repair] sell stock @ {hedge_stock_sell:.2f}")
                self.pos_mgr.reset_phase_events_only(stock_code, phase)
                try:
                    trade_fix = self._place_stock_order(api, stock_code, sj.constant.Action.Sell, hedge_stock_sell, STOCK_QTY)
                    self.pos_mgr.register_order(str(trade_fix.order.id), stock_code, phase, "stock")
                except Exception as e:
                    print(f"  [Repair] stock close failed: {e}")
                    self.pos_mgr.set_state(stock_code, PositionState.UNHEDGED)
                    return

                if sync.done.wait(timeout=REPAIR_TIMEOUT_SEC):
                    self.pos_mgr.set_state(stock_code, PositionState.EMPTY)
                    return

                unwind_fut_sell = self._mk_mktable_price_strict("sell", f_bid, FUT_SELL_TICKS)
                print(f"  [Unwind-to-HOLDING] sell future again @ {unwind_fut_sell:.2f}")
                try:
                    self._place_future_order(api, future_code, sj.constant.Action.Sell, unwind_fut_sell, FUTURE_QTY, phase_open=True)
                except Exception as e:
                    print(f"  [Unwind] future resell failed: {e}")
                    self.pos_mgr.set_state(stock_code, PositionState.UNHEDGED)
                    return

                self.pos_mgr.set_state(stock_code, PositionState.HOLDING)
                return

            if filled_s and (not filled_f):
                hedge_fut_buy = self._mk_mktable_price_strict("buy", f_ask, FUT_BUY_TICKS)
                print(f"  [Repair] buy future @ {hedge_fut_buy:.2f}")
                self.pos_mgr.reset_phase_events_only(stock_code, phase)
                try:
                    trade_fix = self._place_future_order(api, future_code, sj.constant.Action.Buy, hedge_fut_buy, FUTURE_QTY, phase_open=False)
                    self.pos_mgr.register_order(str(trade_fix.order.id), stock_code, phase, "future")
                except Exception as e:
                    print(f"  [Repair] future close failed: {e}")
                    self.pos_mgr.set_state(stock_code, PositionState.UNHEDGED)
                    return

                if sync.done.wait(timeout=REPAIR_TIMEOUT_SEC):
                    self.pos_mgr.set_state(stock_code, PositionState.EMPTY)
                    return

                unwind_stock_buy = self._mk_mktable_price_strict("buy", s_ask, STOCK_BUY_TICKS)
                print(f"  [Unwind-to-HOLDING] buy stock back @ {unwind_stock_buy:.2f}")
                try:
                    self._place_stock_order(api, stock_code, sj.constant.Action.Buy, unwind_stock_buy, STOCK_QTY)
                except Exception as e:
                    print(f"  [Unwind] stock buyback failed: {e}")
                    self.pos_mgr.set_state(stock_code, PositionState.UNHEDGED)
                    return

                self.pos_mgr.set_state(stock_code, PositionState.HOLDING)
                return

        self.pos_mgr.set_state(stock_code, PositionState.UNHEDGED)


# =========================
# --- PairRefresher ---
# =========================
class PairRefresher(threading.Thread):
    def __init__(
        self,
        discoverer: PairDiscoverer,
        strategy: StrategyEngine,
        pos_mgr: PositionManager,
        sub_mgr: SubscriptionManager,
        top_n: int = TOP_N_PAIRS,
        interval_sec: int = REFRESH_PAIRS_EVERY_SEC,
    ):
        super().__init__(daemon=True)
        self.discoverer = discoverer
        self.strategy = strategy
        self.pos_mgr = pos_mgr
        self.sub_mgr = sub_mgr
        self.top_n = int(top_n)
        self.interval_sec = int(interval_sec)
        self.running = True
        self._last_sig = None

    def stop(self):
        self.running = False

    def _signature(self, pairs: List[Tuple[str, str]]) -> str:
        pairs2 = sorted(pairs, key=lambda x: (x[0], x[1]))
        return "|".join([f"{s}:{f}" for s, f in pairs2])

    def run(self):
        print(f">>> Pair Refresher Started (every {self.interval_sec}s)")
        while self.running:
            n = int(max(1, self.interval_sec * 10))
            for _ in range(n):
                if not self.running:
                    return
                time.sleep(0.1)

            try:
                pairs = self.discoverer.find_active_pairs(top_n=self.top_n)
                if not pairs:
                    print("[Pairs] refresh got empty, skip")
                    continue

                sig = self._signature(pairs)
                if sig == self._last_sig:
                    print("[Pairs] unchanged")
                    continue

                self._last_sig = sig
                print(f"[Pairs] changed -> apply (pairs={len(pairs)})")

                self.pos_mgr.set_active_pairs(pairs)
                self.strategy.update_pairs(pairs)
                
                # --- Merge Existing Positions for Subscription ---
                # 確保所有手上有部位 (HOLDING/UNHEDGED/PENDING) 的 pair 都在訂閱清單中
                # 不然無法平倉
                existing_pairs = []
                for ps in self.pos_mgr.all_pairs():
                    if ps.state != PositionState.EMPTY:
                        existing_pairs.append((ps.stock_code, ps.fut_code))
                
                # 合併清單 (Existing + New Discovered)
                # 使用 dict 去重，並讓 Existing 優先 (如果 SubscriptionManager 有上限邏輯)
                # 這裡簡單 set 去重
                final_subs = list(set(pairs + existing_pairs))
                print(f"[Pairs] Merged: {len(pairs)} new + {len(existing_pairs)} existing -> {len(final_subs)} total subs")
                
                self.sub_mgr.apply_pairs(final_subs, log_prefix="[Subscription]")
                # -------------------------------------------------

            except Exception as e:
                print(f"[Pairs] refresh failed: {e}")


# =========================
# --- ArbitrageSystem ---
# =========================
class ArbitrageSystem:
    def __init__(self):
        self.api = sj.Shioaji(simulation=SIMULATION)

        self.market_data = MarketData()
        self.pos_mgr = PositionManager()
        self.order_tracker = OrderTracker() # Init Tracker

        self.quote_queue = queue.Queue(maxsize=20000)
        self.exec_queue = queue.Queue(maxsize=20000)

        self.sub_mgr = SubscriptionManager(self.api, max_subs=MAX_SUBS)

        self.capital_mgr = CapitalManager(MAX_CAPITAL, self.pos_mgr, self.market_data) # Init Capital Mgr
        self.strategy = StrategyEngine(self.market_data, self.pos_mgr, self.capital_mgr) # Pass to Strategy
        self.execution = ExecutionEngine(self.market_data, self.pos_mgr, self.order_tracker) # Pass Tracker

        self.account_monitor: Optional[AccountMonitor] = None
        self.keyboard: Optional[KeyboardMonitor] = None
        self.pair_refresher: Optional[PairRefresher] = None

        self.active_pairs: List[Tuple[str, str]] = []
        self.running = False

        self._setup_callbacks_done = False

        self.DBG_WATCH_STOCK = DBG_WATCH_STOCK
        self.DBG_WATCH_FUT = DBG_WATCH_FUT

    def login(self):
        if not CA_API_KEY or not CA_SECRET_KEY:
            raise RuntimeError("Missing Sinopack_CA_API_KEY / Sinopack_CA_SECRET_KEY in env")

        if SIMULATION:
            print("[System] login (simulation)...")
            # ✅ 重要：subscribe_trade=True，才比較穩收到委託/成交回報
            self.api.login(
                api_key=CA_API_KEY,
                secret_key=CA_SECRET_KEY,
                contracts_cb=print,
                subscribe_trade=True,
            )
            try:
                if CA_PATH and CA_PASSWORD and PERSON_ID:
                    self.api.activate_ca(CA_PATH, CA_PASSWORD, PERSON_ID)
            except Exception as e:
                print(f"[System] activate_ca (simulation) failed (ignored): {e}")
        else:
            print("[System] login (real)...")
            self.api.login(
                api_key=CA_API_KEY,
                secret_key=CA_SECRET_KEY,
                contracts_cb=lambda security_type: print(f"{repr(security_type)} fetch done."),
                subscribe_trade=True
            )
            print("[System] activate_ca...")
            self.api.activate_ca(ca_path=CA_PATH, ca_passwd=CA_PASSWORD, person_id=PERSON_ID)

        print("[System] Login Success, waiting contracts ready...")
        time.sleep(3)

    def _setup_callbacks(self):
        @self.api.on_bidask_stk_v1(bind=True)
        def on_stock_quote(self_api, exchange, quote):
            self.market_data.update_stock(quote)
            try:
                self.quote_queue.put_nowait(quote)
            except queue.Full:
                pass

            if _get_code(quote) == self.DBG_WATCH_STOCK:
                self._dbg_print_watch_line()

        @self.api.on_bidask_fop_v1(bind=True)
        def on_future_quote(self_api, exchange, quote):
            self.market_data.update_future(quote)
            try:
                self.quote_queue.put_nowait(quote)
            except queue.Full:
                pass

            if self.DBG_WATCH_FUT and _get_code(quote) == self.DBG_WATCH_FUT:
                self._dbg_print_watch_line()

        # ✅ order callback
        self.api.set_order_callback(self._on_order_status)

        self._setup_callbacks_done = True
        print("[System] callbacks ready")

    def _on_order_status(self, order_state, trade_dict):
        # --- DEBUG RAW CALLBACK ---
        print(f"[Callback RAW] type(state)={type(order_state)} type(trade)={type(trade_dict)}")
        try:
             print(f"  state: {order_state}")
             print(f"  trade: {trade_dict}")
        except: pass
        # --------------------------

        self.pos_mgr.record_order_event(order_state, trade_dict)

        # 1. 解析 Order ID
        order_id = ""
        # 尝试从 trade_dict (dict) 取得
        if isinstance(trade_dict, dict):
            # 常见结构: trade_dict['order']['id'] 或 trade_dict['trade_id']
            order_id = (trade_dict.get("order") or {}).get("id") or trade_dict.get("trade_id")
        
        # 尝试从 trade_dict (object, e.g. Trade) 取得
        if not order_id and hasattr(trade_dict, "order"):
             order_id = getattr(trade_dict.order, "id", "")

        # 尝试从 order_state (object) 取得 (部分版本 OrderState 包含 order 对象)
        if not order_id and hasattr(order_state, "order"):
             order_id = getattr(order_state.order, "id", "")
        
        if not order_id:
            # print("[Callback] ❌ Could not extract order_id, skipping.")
            return

        # 2. 查找 Order Context
        info = self.pos_mgr.lookup_order(str(order_id))
        if not info:
            # print(f"[Callback] Order {order_id} not found in manager.")
            return

        stock_code, phase, leg = info

        # 3. 解析 Status, Qty, Price
        status = ""
        deal_qty = 0
        deal_price = 0.0

        # 优先使用 order_state (OrderState 对象)
        if hasattr(order_state, "status"):
            s_raw = order_state.status
            # 处理 Enum (e.g. Status.Filled)
            if hasattr(s_raw, "value"): 
                # 尝试转回 Shioaji Enum string
                status = str(s_raw) 
            else:
                status = str(s_raw)
            
            deal_qty = int(getattr(order_state, "deal_quantity", 0))
            deal_price = float(getattr(order_state, "deal_price", 0.0))
        
        # Fallback: 使用 trade_dict (dict)
        elif isinstance(trade_dict, dict):
            st_node = trade_dict.get("status")
            if isinstance(st_node, dict):
                status = st_node.get("status", "")
                deal_qty = int(st_node.get("deal_quantity", 0))
                deal_price = float(st_node.get("deal_price", 0.0))
            else:
                status = str(st_node)
        
        # 最后的清理
        status = str(status) # Ensure string
        # 移除 "Status." 前缀以防万一
        if "Status." in status:
            status = status.split("Status.")[-1]
            
        print(f"[Callback Parsed] {stock_code} {leg} status='{status}' qty={deal_qty}")

        # 4. 更新 PosMgr
        self.pos_mgr.update_leg_status(
            stock_code=stock_code,
            phase=phase,
            leg=leg,
            status=status,
            filled_qty=deal_qty,
            deal_price=deal_price,
            last_event=trade_dict if isinstance(trade_dict, dict) else str(trade_dict),
        )

    def _snapshot_init(self, pairs: List[Tuple[str, str]]):
        print("[System] fetching initial snapshots...")
        contracts = []
        seen = set()

        for s, f in pairs:
            if s not in seen:
                try:
                    contracts.append(self.api.Contracts.Stocks[s])
                    seen.add(s)
                except Exception:
                    pass
            if f not in seen:
                try:
                    contracts.append(self.api.Contracts.Futures[f])
                    seen.add(f)
                except Exception:
                    pass

        if not contracts:
            return

        try:
            chunk = 50
            for i in range(0, len(contracts), chunk):
                snaps = self.api.snapshots(contracts[i:i+chunk])
                for snap in snaps:
                    w = SnapshotWrapper(snap)
                    code = w.code

                    is_future = any(code == f for _, f in pairs)
                    is_stock = any(code == s for s, _ in pairs)

                    if is_future:
                        self.market_data.update_future(w)
                    elif is_stock:
                        self.market_data.update_stock(w)

                    try:
                        self.quote_queue.put_nowait(w)
                    except queue.Full:
                        pass
        except Exception as e:
            print(f"[System] snapshot init failed: {e}")

    def _quote_loop(self):
        while self.running:
            try:
                q = self.quote_queue.get(timeout=0.2)
            except queue.Empty:
                continue

            try:
                sig = self.strategy.on_quote(q)
                if sig:
                    try:
                        self.exec_queue.put_nowait(sig)
                    except queue.Full:
                        pass
            except Exception as e:
                print(f"[QuoteLoop] error: {e}")

    def _exec_loop(self):
        while self.running:
            try:
                sig = self.exec_queue.get(timeout=0.2)
            except queue.Empty:
                continue

            try:
                self.execution.on_signal(sig, api=self.api)
            except Exception as e:
                print(f"[ExecLoop] error: {e}")

    def _dbg_print_watch_line(self):
        global DBG_LAST_LINE
        if not self.DBG_WATCH_FUT:
            return

        s_q, f_q = self.market_data.get_quotes(self.DBG_WATCH_STOCK, self.DBG_WATCH_FUT)
        if not s_q or not f_q:
            return

        s_ask = _get_ask(s_q)
        s_bid = _get_bid(s_q)
        f_ask = _get_ask(f_q)
        f_bid = _get_bid(f_q)

        key = (s_ask, s_bid, f_ask, f_bid)
        if DBG_LAST_LINE == key:
            return
        DBG_LAST_LINE = key

        stock_notional_buy = s_ask * STOCK_SHARES_PER_LOT * STOCK_QTY
        future_notional_sell = f_bid * FUTURE_SHARES_EQUIV * FUTURE_QTY
        spread_value = future_notional_sell - stock_notional_buy
        cost = self.strategy.calculate_cost(s_ask, f_bid)
        net = spread_value - cost

        msg = (
            f"[WATCH] {self.DBG_WATCH_STOCK}/{self.DBG_WATCH_FUT} "
            f"STK(b/a)={s_bid:.2f}/{s_ask:.2f} "
            f"FUT(b/a)={f_bid:.2f}/{f_ask:.2f} "
            f"net={net:,.0f}      "
        )
        with DBG_LINE_LOCK:
            sys.stdout.write("\r" + " " * 160 + "\r" + msg)
            sys.stdout.flush()

    def start(self, top_n_pairs: int = TOP_N_PAIRS, refresh_interval_sec: int = REFRESH_PAIRS_EVERY_SEC):
        if not self._setup_callbacks_done:
            self._setup_callbacks()

        discoverer = PairDiscoverer(self.api)
        pairs = discoverer.find_active_pairs(top_n=top_n_pairs)

        if not pairs:
            print("[System] no pairs found, fallback to DEBUG single pair if possible...")
            pairs = []
            try:
                today = datetime.datetime.now()
                target_month_str = today.strftime('%Y%m')
                found = None
                for category in self.api.Contracts.Futures:
                    try:
                        iter(category)
                    except TypeError:
                        continue
                    for c in category:
                        if isinstance(c, tuple):
                            c = c[1]
                        if getattr(c, "underlying_code", None) == self.DBG_WATCH_STOCK and getattr(c, "delivery_month", "") == target_month_str:
                            found = str(c.code)
                            break
                    if found:
                        break
                if found:
                    pairs = [(self.DBG_WATCH_STOCK, found)]
            except Exception:
                pass

        if not pairs:
            print("[System] still empty pairs -> exit")
            return

        self.active_pairs = pairs
        self.pos_mgr.set_active_pairs(self.active_pairs)
        
        # --- RECONCILIATION START ---
        # 在開始策略之前，先執行狀態重建
        # 這會讀取 order history, updates status, 並查庫存
        reconciler = Reconciler(self.api, self.pos_mgr, self.order_tracker)
        reconciler.reconcile()
        # --- RECONCILIATION END ---

        self.DBG_WATCH_FUT = None
        for s, f in self.active_pairs:
            if s == self.DBG_WATCH_STOCK:
                self.DBG_WATCH_FUT = f
                break
        print(f"\n[DBG] watch pair: {self.DBG_WATCH_STOCK}/{self.DBG_WATCH_FUT}\n")

        self.pos_mgr.set_active_pairs(self.active_pairs)
        self.strategy.update_pairs(self.active_pairs)

        self.sub_mgr.apply_pairs(self.active_pairs, log_prefix="[Subscription]")

        self._snapshot_init(self.active_pairs)

        self.running = True

        self.account_monitor = AccountMonitor(
            api=self.api,
            pos_mgr=self.pos_mgr,
            stock_shares_per_lot=STOCK_SHARES_PER_LOT,
            poll_sec=5.0,
        )
        self.account_monitor.start()

        threading.Thread(target=self._quote_loop, daemon=True).start()
        threading.Thread(target=self._exec_loop, daemon=True).start()

        self.pair_refresher = PairRefresher(
            discoverer=discoverer,
            strategy=self.strategy,
            pos_mgr=self.pos_mgr,
            sub_mgr=self.sub_mgr,
            top_n=top_n_pairs,
            interval_sec=refresh_interval_sec
        )
        self.pair_refresher.start()

        self.keyboard = KeyboardMonitor(self)
        self.keyboard.start()

        print("[System] started\n")

        try:
            while self.running:
                time.sleep(1)
        except KeyboardInterrupt:
            self.stop()

    def stop(self):
        if not self.running:
            return
        self.running = False
        print("\n[System] stopping...")

        try:
            if self.pair_refresher:
                self.pair_refresher.stop()
        except Exception:
            pass

        try:
            if self.keyboard:
                self.keyboard.stop()
        except Exception:
            pass

        try:
            if self.account_monitor:
                self.account_monitor.stop()
        except Exception:
            pass

        try:
            self.sub_mgr.force_unsubscribe_all(log_prefix="[Subscription]")
        except Exception:
            pass

        try:
            self.api.logout()
        except Exception as e:
            print(f"[System] logout error: {e}")

        print("[System] stopped\n")


# =========================
# --- main ---
# =========================
if __name__ == "__main__":
    system = ArbitrageSystem()
    system.login()
    system.start(top_n_pairs=TOP_N_PAIRS, refresh_interval_sec=REFRESH_PAIRS_EVERY_SEC)
