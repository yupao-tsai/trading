import shioaji as sj
import datetime
import threading
import queue
import time
import pandas as pd
from typing import Dict, List, Tuple, Optional, Any
from enum import Enum
import concurrent.futures
from decimal import Decimal, ROUND_FLOOR, ROUND_CEILING
import json
import os
from dotenv import load_dotenv
load_dotenv()

# =========================
# --- 全域設定 ---
# =========================
SIMULATION = True      # 是否為模擬環境
STOCK_QTY = 2          # 現貨口數 (張)
FUTURE_QTY = 1         # 期貨口數 (口)
ENTRY_THRESHOLD = 5000 # 進場價差門檻 (金額)
EXIT_THRESHOLD = 0     # 出場價差門檻 (金額)

MIN_VOLUME_THRESHOLD = 5  # 自動篩選：期貨日均量門檻 (口) [模擬環境調整]
VOLUME_HISTORY_FILE = "volume_history.json"  # 交易量記錄檔

# ✅ 新增：動態換監控組合參數
TOP_N_PAIRS = 20               # 程式一開始抓成交量最大的 N 組
REFRESH_PAIRS_EVERY_SEC = 60   # 每隔幾秒重找一次更適合的 pair（可調）
MAX_SUBS = 70                  # 訂閱上限（保守值；若你確認可更大自行改）

# 簡易帳號設定
PERSON_ID = os.getenv("Sinopack_PERSON_ID")
PASSWORD = os.getenv("Sinopack_PASSWORD")
CA_API_KEY = os.getenv("Sinopack_CA_API_KEY")
CA_SECRET_KEY = os.getenv("Sinopack_CA_SECRET_KEY")
CA_PATH = os.getenv("Sinopack_CA_PATH")
CA_PASSWORD = os.getenv("Sinopack_CA_PASSWORD")

# --- Hedge / Repair 參數 ---
HEDGE_TIMEOUT_SEC = 0.8
REPAIR_TIMEOUT_SEC = 0.8
MIN_PROFIT_TO_REPAIR = 0

# --- Tick-based 保護：幾檔 ---
STOCK_BUY_TICKS = 1
STOCK_SELL_TICKS = 1
FUT_BUY_TICKS = 1
FUT_SELL_TICKS = 1

# --- 合約換算（1口=2000股；現股1張=1000股）---
STOCK_SHARES_PER_LOT = 1000
FUTURE_SHARES_EQUIV = 2000

def _check_hedge_ratio():
    stock_shares = STOCK_QTY * STOCK_SHARES_PER_LOT
    future_shares = FUTURE_QTY * FUTURE_SHARES_EQUIV
    if stock_shares != future_shares:
        print(f"[⚠️ Hedge Ratio Warning] 股票={stock_shares}股 vs 期貨={future_shares}股，不是 1:1 對沖！")
    else:
        print(f"[Hedge Ratio OK] 股票={stock_shares}股 == 期貨={future_shares}股")
_check_hedge_ratio()

# =========================
# --- 狀態列舉/資料結構 ---
# =========================
from dataclasses import dataclass, field

class PositionState(Enum):
    EMPTY = 0
    PENDING_ENTRY = 1
    HOLDING = 2
    PENDING_EXIT = 3
    UNHEDGED = 4

@dataclass
class LegFill:
    status: str = ""
    filled_qty: int = 0
    deal_price: float = 0.0

@dataclass
class PhaseSync:
    done: threading.Event = field(default_factory=threading.Event)
    failed: threading.Event = field(default_factory=threading.Event)
    def reset(self):
        self.done.clear()
        self.failed.clear()

@dataclass
class PositionInfo:
    stock_code: str
    future_code: str | None = None
    state: PositionState = PositionState.EMPTY
    open_stock: LegFill = field(default_factory=LegFill)
    open_future: LegFill = field(default_factory=LegFill)
    close_stock: LegFill = field(default_factory=LegFill)
    close_future: LegFill = field(default_factory=LegFill)
    open_sync: PhaseSync = field(default_factory=PhaseSync)
    close_sync: PhaseSync = field(default_factory=PhaseSync)

class PositionManager:
    """
    - stock_code 為主鍵
    - order_id -> (stock_code, phase(open/close), leg(stock/future))
    - callback 更新 LegFill + 設定 done/failed event
    """
    def __init__(self):
        self.lock = threading.RLock()
        self._pos: dict[str, PositionInfo] = {}
        self._order_map: dict[str, tuple[str, str, str]] = {}

    def _get_nolock(self, stock_code: str) -> PositionInfo:
        if stock_code not in self._pos:
            self._pos[stock_code] = PositionInfo(stock_code=stock_code)
        return self._pos[stock_code]

    def get(self, stock_code: str) -> PositionInfo:
        with self.lock:
            return self._get_nolock(stock_code)

    def set_state(self, stock_code: str, state: PositionState):
        with self.lock:
            p = self._get_nolock(stock_code)
            p.state = state
        print(f"[State Update] {stock_code} -> {state.name}")

    def set_future_pair(self, stock_code: str, future_code: str):
        with self.lock:
            self._get_nolock(stock_code).future_code = future_code

    def register_order(self, order_id: str, stock_code: str, phase: str, leg: str):
        with self.lock:
            self._order_map[str(order_id)] = (stock_code, phase, leg)

    def lookup_order(self, order_id: str):
        with self.lock:
            return self._order_map.get(str(order_id))

    def prepare_phase(self, stock_code: str, phase: str):
        with self.lock:
            p = self._get_nolock(stock_code)
            if phase == "open":
                p.open_stock = LegFill()
                p.open_future = LegFill()
                p.open_sync.reset()
            else:
                p.close_stock = LegFill()
                p.close_future = LegFill()
                p.close_sync.reset()

    def reset_phase_events_only(self, stock_code: str, phase: str):
        with self.lock:
            p = self._get_nolock(stock_code)
            (p.open_sync if phase == "open" else p.close_sync).reset()

    def update_leg_status(
        self,
        stock_code: str,
        phase: str,
        leg: str,
        status: str,
        filled_qty: int = 0,
        deal_price: float = 0.0,
    ):
        with self.lock:
            p = self._get_nolock(stock_code)
            if phase == "open":
                tgt = p.open_stock if leg == "stock" else p.open_future
                sync = p.open_sync
            else:
                tgt = p.close_stock if leg == "stock" else p.close_future
                sync = p.close_sync

            tgt.status = str(status)
            if filled_qty:
                tgt.filled_qty = max(tgt.filled_qty, int(filled_qty))
            if deal_price:
                tgt.deal_price = float(deal_price)

            st_filled = str(sj.constant.Status.Filled)
            st_failed = str(sj.constant.Status.Failed)
            st_cancel = str(sj.constant.Status.Cancelled)

            if tgt.status in [st_failed, st_cancel]:
                sync.failed.set()
                return

            if phase == "open":
                a, b = p.open_stock.status, p.open_future.status
            else:
                a, b = p.close_stock.status, p.close_future.status

            if a == st_filled and b == st_filled:
                sync.done.set()

    def get_phase_sync(self, stock_code: str, phase: str) -> PhaseSync:
        with self.lock:
            p = self._get_nolock(stock_code)
            return p.open_sync if phase == "open" else p.close_sync


# =========================
# --- 市場資料與 Snapshot 包裝 ---
# =========================
class SnapshotWrapper:
    """
    把 snapshot 包裝成類似 BidAskSTKv1/FOPv1 介面。
    """
    def __init__(self, snapshot):
        self.code = snapshot.code
        self.ask_price = [float(snapshot.sell_price)] if snapshot.sell_price else [0.0]
        self.bid_price = [float(snapshot.buy_price)] if snapshot.buy_price else [0.0]
        self.ask_volume = [int(snapshot.sell_volume)] if snapshot.sell_volume else [0]
        self.bid_volume = [int(snapshot.buy_volume)] if snapshot.buy_volume else [0]
        self.datetime = datetime.datetime.now()
        if hasattr(snapshot, 'ts') and snapshot.ts:
            try:
                self.datetime = datetime.datetime.fromtimestamp(snapshot.ts / 1e9)
            except:
                pass

class MarketData:
    def __init__(self):
        self.lock = threading.Lock()
        self.stock_quotes: Dict[str, Any] = {}
        self.future_quotes: Dict[str, Any] = {}

    def update_stock(self, quote: Any):
        with self.lock:
            self.stock_quotes[quote.code] = quote

    def update_future(self, quote: Any):
        with self.lock:
            self.future_quotes[quote.code] = quote

    def get_quotes(self, stock_code: str, future_code: str):
        with self.lock:
            return self.stock_quotes.get(stock_code), self.future_quotes.get(future_code)


# =========================
# --- Account Monitor ---
# =========================
class AccountMonitor(threading.Thread):
    def __init__(self, api: sj.Shioaji, position_manager: 'PositionManager'):
        super().__init__()
        self.api = api
        self.pos_mgr = position_manager
        self.running = True
        self.stock_account = api.stock_account
        self.futopt_account = api.futopt_account

    def run(self):
        print(">>> Account Monitor Started")
        while self.running:
            try:
                self.sync_positions()
            except Exception as e:
                print(f"[Monitor Error] Sync failed: {e}")

            for _ in range(50):
                if not self.running:
                    break
                time.sleep(0.1)

    def sync_positions(self):
        try:
            future_positions = self.api.list_positions(account=self.futopt_account)
        except Exception:
            future_positions = []

        try:
            stock_positions = self.api.list_positions(account=self.stock_account)
        except Exception:
            stock_positions = []

        real_stock = {}
        real_future = {}

        for pos in stock_positions:
            code = pos.code
            vol_lot = int(pos.quantity)
            direction = pos.direction
            sign = 1 if direction == sj.constant.Action.Buy else -1
            real_stock[code] = real_stock.get(code, 0) + sign * (vol_lot * STOCK_SHARES_PER_LOT)

        for pos in future_positions:
            code = pos.code
            vol = int(pos.quantity)
            direction = pos.direction
            sign = 1 if direction == sj.constant.Action.Buy else -1
            real_future[code] = real_future.get(code, 0) + sign * vol

        with self.pos_mgr.lock:
            for stock_code, p in self.pos_mgr._pos.items():
                state = p.state
                f_code = p.future_code

                stock_shares = real_stock.get(stock_code, 0)
                fut_contracts = real_future.get(f_code, 0) if f_code else 0

                expected_stock = STOCK_QTY * STOCK_SHARES_PER_LOT
                expected_fut = -FUTURE_QTY

                if state in [PositionState.PENDING_ENTRY, PositionState.PENDING_EXIT]:
                    continue

                if stock_shares == 0 and fut_contracts == 0:
                    if state != PositionState.EMPTY:
                        p.state = PositionState.EMPTY
                        print(f"[Monitor Sync] {stock_code} -> EMPTY (no real positions)")
                    continue

                if (stock_shares == expected_stock) and (fut_contracts == expected_fut):
                    if state != PositionState.HOLDING:
                        p.state = PositionState.HOLDING
                        print(f"[Monitor Sync] {stock_code} -> HOLDING (real matched)")
                    continue

                if state != PositionState.UNHEDGED:
                    p.state = PositionState.UNHEDGED
                    print(f"[Monitor Sync] {stock_code} -> UNHEDGED (stock={stock_shares}, fut={fut_contracts})")

    def stop(self):
        self.running = False


# =========================
# --- 成交量快取/PairDiscoverer ---
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
                        if 'volumes' not in data[first_key]:
                            return {}
                    return data
            except:
                return {}
        return {}

    def _save(self):
        with open(self.filename, 'w', encoding='utf-8') as f:
            json.dump(self.data, f, indent=2)

    def get_5ma(self, code: str, is_market_closed: bool) -> int:
        record = self.data.get(code)
        if not record or 'volumes' not in record:
            return 0

        saved_date = record['date']
        volumes = record['volumes']
        if not volumes:
            return 0

        valid_volumes = [v for v in volumes if v > 0]
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
        valid = [v for v in record['volumes'] if v > 0]
        if not valid:
            return 0
        return int(sum(valid) / len(valid))

    def update_window_init(self, code: str, vol_list: list):
        final_list = vol_list[-5:]
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
    可重複呼叫 find_active_pairs()，用 smart cache 抓 top N。
    """
    def __init__(self, api: sj.Shioaji):
        self.api = api
        self.vol_mgr = VolumeManager()
        self.daily_quotes_map = {}
        self._fetch_daily_quotes_cache()

    def _fetch_daily_quotes_cache(self):
        print("正在建立 Daily Quotes 快取 (作為 KBar 備案)...")
        try:
            dq = self.api.daily_quotes(date=datetime.date.today())
            if not dq or not dq.Code:
                yesterday = datetime.date.today() - datetime.timedelta(days=1)
                dq = self.api.daily_quotes(date=yesterday)

            if dq and dq.Code:
                for i, code in enumerate(dq.Code):
                    self.daily_quotes_map[code] = int(dq.Volume[i])
                print(f"Daily Quotes 快取建立完成，共 {len(self.daily_quotes_map)} 檔資料。")
            else:
                print("Daily Quotes 抓取失敗或無資料。")
        except Exception as e:
            print(f"Daily Quotes Cache Error: {e}")

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
            stype = str(contract_obj.security_type)
            if stype in ['FUT', 'Future', str(sj.constant.SecurityType.Future)] and hasattr(contract_obj, 'underlying_code'):
                cache_key = f"F_{contract_obj.underlying_code}"

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
                        today_vol = int(snaps[0].total_volume)
                except:
                    today_vol = 0

            if today_vol > 0 and cache_key in self.vol_mgr.data:
                self.vol_mgr.slide_window(cache_key, today_vol)
                refreshed = self.vol_mgr.get_5ma(cache_key, is_market_closed=True)
                if refreshed > 0:
                    return refreshed

        target = contract_obj if contract_obj else self.api.Contracts.Stocks[code]

        snapshot_vol = 0
        try:
            snaps = self.api.snapshots([target])
            if snaps and len(snaps) > 0:
                snapshot_vol = int(snaps[0].total_volume)
                if snapshot_vol > (MIN_VOLUME_THRESHOLD * 0.5):
                    self.vol_mgr.update_window_init(cache_key, [snapshot_vol])
                    return snapshot_vol
        except:
            pass

        if code in self.daily_quotes_map:
            dq_vol = int(self.daily_quotes_map[code])
            if dq_vol > MIN_VOLUME_THRESHOLD:
                self.vol_mgr.update_window_init(cache_key, [dq_vol])
                return dq_vol

        vol_list = self._fetch_last_n_days_volumes(target, n_days=5)
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
        print("正在掃描活躍的股票期貨配對 (Smart Sliding Window)...")
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
                # 只留一般股票期貨，排除小型
                if getattr(contract, "category", "") == "QEF":
                    continue
                seen_contracts.add(contract.code)

                is_futures = str(contract.security_type) in ['FUT', 'Future', str(sj.constant.SecurityType.Future)]
                if (
                    is_futures
                    and hasattr(contract, 'underlying_code')
                    and len(contract.underlying_code) == 4
                    and contract.delivery_month == target_month_str
                ):
                    candidates.append(contract)

        best_by_pair = {}

        for contract in candidates:
            stock_code = contract.underlying_code
            future_code = contract.code

            stock_avg = self._get_avg_volume_smart(stock_code, contract_obj=None)
            if stock_avg < MIN_VOLUME_THRESHOLD:
                continue

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
        print(f"完成：符合條件 {len(ranked)} 組，回傳前 {len(top)} 組。")
        return [(s_code, f_code) for (_, _, _, s_code, f_code) in top]


# =========================
# --- 策略/執行 ---
# =========================
class TradeSignal:
    def __init__(self, action_type: str, stock_code: str, future_code: str,
                 stock_price: float, future_price: float):
        self.action_type = action_type
        self.stock_code = stock_code
        self.future_code = future_code
        self.stock_price = stock_price
        self.future_price = future_price


class StrategyEngine(threading.Thread):
    def __init__(self, quote_queue, market_data, execution_queue, position_manager, pairs):
        super().__init__()
        self.quote_queue = quote_queue
        self.market_data = market_data
        self.execution_queue = execution_queue
        self.pos_mgr = position_manager

        self.running = True

        self._pairs_lock = threading.RLock()
        self.pairs: List[Tuple[str, str]] = []
        self.pair_map: Dict[str, List[str]] = {}
        self.future_to_stock: Dict[str, str] = {}

        self.update_pairs(pairs, log_prefix="[Strategy] init")

    def update_pairs(self, pairs: List[Tuple[str, str]], log_prefix: str = "[Strategy]"):
        with self._pairs_lock:
            self.pairs = list(pairs)

            pair_map: Dict[str, List[str]] = {}
            future_to_stock: Dict[str, str] = {}

            for s_code, f_code in self.pairs:
                pair_map.setdefault(s_code, []).append(f_code)
                future_to_stock[f_code] = s_code

            self.pair_map = pair_map
            self.future_to_stock = future_to_stock

        print(f"{log_prefix} pairs updated: {len(self.pairs)} pairs")

    def calculate_cost(self, stock_price: float, future_price: float) -> float:
        stock_notional = stock_price * STOCK_SHARES_PER_LOT * STOCK_QTY
        future_notional = future_price * FUTURE_SHARES_EQUIV * FUTURE_QTY

        stock_fee = stock_notional * 0.001425 * 0.3
        future_fee = 20 * FUTURE_QTY
        tax_stock = stock_notional * 0.003
        tax_future = future_notional * 0.00002

        return stock_fee + future_fee + tax_stock + tax_future

    def run(self):
        print(">>> Strategy Engine Started (Event-Driven Mode + Dynamic Pairs)")
        while self.running:
            try:
                quote = self.quote_queue.get(timeout=1)

                # 取當下 pairs map（避免更新中讀到一半）
                with self._pairs_lock:
                    pair_map = self.pair_map
                    future_to_stock = self.future_to_stock

                target_futures = []
                target_stock = None

                if quote.code in pair_map:
                    target_stock = quote.code
                    target_futures = pair_map[quote.code]
                elif quote.code in future_to_stock:
                    target_stock = future_to_stock[quote.code]
                    target_futures = [quote.code]
                else:
                    continue

                stock_code = target_stock
                pos = self.pos_mgr.get(stock_code)
                state = pos.state

                # ✅ UNHEDGED / PENDING 時一律不動作
                if state in [PositionState.PENDING_ENTRY, PositionState.PENDING_EXIT, PositionState.UNHEDGED]:
                    continue

                # --- 進場：空手才做，任選一個 future 觸發就 break ---
                if state == PositionState.EMPTY:
                    for future_code in target_futures:
                        s_quote, f_quote = self.market_data.get_quotes(stock_code, future_code)
                        if not s_quote or not f_quote:
                            continue

                        stock_ask = float(s_quote.ask_price[0])
                        future_bid = float(f_quote.bid_price[0])
                        if stock_ask == 0 or future_bid == 0:
                            continue

                        stock_notional = stock_ask * STOCK_SHARES_PER_LOT * STOCK_QTY
                        future_notional = future_bid * FUTURE_SHARES_EQUIV * FUTURE_QTY
                        spread_value = future_notional - stock_notional

                        cost = self.calculate_cost(stock_ask, future_bid)
                        net_profit = spread_value - cost

                        if net_profit > ENTRY_THRESHOLD:
                            print(f"[訊號-開倉] {stock_code}/{future_code} | 預期淨利: {net_profit:.0f}")
                            self.pos_mgr.set_future_pair(stock_code, future_code)
                            self.pos_mgr.set_state(stock_code, PositionState.PENDING_ENTRY)
                            self.execution_queue.put(TradeSignal('OPEN', stock_code, future_code, stock_ask, future_bid))
                            break

                # --- 出場：只針對「當初開倉的 future」判斷 ---
                elif state == PositionState.HOLDING:
                    future_code = pos.future_code
                    if not future_code:
                        print(f"[⚠️] {stock_code} HOLDING 但沒有 future_code，切 UNHEDGED")
                        self.pos_mgr.set_state(stock_code, PositionState.UNHEDGED)
                        continue

                    s_quote, f_quote = self.market_data.get_quotes(stock_code, future_code)
                    if not s_quote or not f_quote:
                        continue

                    stock_bid = float(s_quote.bid_price[0])
                    future_ask = float(f_quote.ask_price[0])
                    if stock_bid == 0 or future_ask == 0:
                        continue

                    stock_notional = stock_bid * STOCK_SHARES_PER_LOT * STOCK_QTY
                    future_notional = future_ask * FUTURE_SHARES_EQUIV * FUTURE_QTY
                    current_spread = future_notional - stock_notional

                    if current_spread <= EXIT_THRESHOLD:
                        print(f"[訊號-平倉] {stock_code}/{future_code} | 收斂價差: {current_spread:.0f}")
                        self.pos_mgr.set_state(stock_code, PositionState.PENDING_EXIT)
                        self.execution_queue.put(TradeSignal('CLOSE', stock_code, future_code, stock_bid, future_ask))

            except queue.Empty:
                continue
            except Exception as e:
                print(f"Strategy Error: {e}")

    def stop(self):
        self.running = False

class ExecutionEngine(threading.Thread):
    def __init__(self, api: sj.Shioaji, execution_queue: queue.Queue, position_manager: PositionManager, market_data: MarketData):
        super().__init__()
        self.api = api
        self.execution_queue = execution_queue
        self.pos_mgr = position_manager
        self.market_data = market_data
        self.running = True

    def _calc_cost(self, stock_price: float, future_price: float) -> float:
        stock_notional = stock_price * STOCK_SHARES_PER_LOT * STOCK_QTY
        future_notional = future_price * FUTURE_SHARES_EQUIV * FUTURE_QTY
        stock_fee = stock_notional * 0.001425 * 0.3
        future_fee = 20 * FUTURE_QTY
        tax_stock = stock_notional * 0.003
        tax_future = future_notional * 0.00002
        return stock_fee + future_fee + tax_stock + tax_future

    def _calc_open_net(self, stock_buy: float, future_sell: float) -> float:
        spread = (future_sell * FUTURE_SHARES_EQUIV * FUTURE_QTY) - (stock_buy * STOCK_SHARES_PER_LOT * STOCK_QTY)
        return spread - self._calc_cost(stock_buy, future_sell)

    def _cancel_best_effort(self, trade_obj):
        try:
            self.api.cancel_order(trade_obj)
            return True
        except:
            return False

    def _place_stock_order(self, stock_code: str, action, price: float, qty: int):
        contract_s = self.api.Contracts.Stocks[stock_code]
        if hasattr(sj.constant, 'OrderType'):
            order_type_s = sj.constant.OrderType.ROD
        elif hasattr(sj.constant, 'TFTOrderType'):
            order_type_s = sj.constant.TFTOrderType.ROD
        else:
            order_type_s = "ROD"

        if hasattr(sj.constant, 'StockOrderLot'):
            order_lot_s = sj.constant.StockOrderLot.Common
        elif hasattr(sj.constant, 'TFTStockOrderLot'):
            order_lot_s = sj.constant.TFTStockOrderLot.Common
        else:
            order_lot_s = "Common"

        order_s = self.api.Order(
            price=price,
            quantity=qty,
            action=action,
            price_type=sj.constant.StockPriceType.LMT,
            order_type=order_type_s,
            order_lot=order_lot_s,
            account=self.api.stock_account
        )
        return self.api.place_order(contract_s, order_s)

    def _place_future_order(self, future_code: str, action, price: float, qty: int, phase_open: bool):
        contract_f = self.api.Contracts.Futures[future_code]

        octype = "New"
        if hasattr(sj.constant, 'FuturesOCType'):
            octype = sj.constant.FuturesOCType.New if phase_open else sj.constant.FuturesOCType.Cover

        price_type_f = sj.constant.StockPriceType.LMT
        if hasattr(sj.constant, 'FuturesPriceType'):
            price_type_f = sj.constant.FuturesPriceType.LMT

        if hasattr(sj.constant, 'OrderType'):
            order_type_f = sj.constant.OrderType.ROD
        elif hasattr(sj.constant, 'FuturesOrderType'):
            order_type_f = sj.constant.FuturesOrderType.ROD
        else:
            order_type_f = "ROD"

        order_f = self.api.Order(
            action=action,
            price=price,
            quantity=qty,
            price_type=price_type_f,
            order_type=order_type_f,
            octype=octype,
            account=self.api.futopt_account
        )
        return self.api.place_order(contract_f, order_f)

    def _get_top_prices(self, stock_code: str, future_code: str):
        s_quote, f_quote = self.market_data.get_quotes(stock_code, future_code)
        if not s_quote or not f_quote:
            return None
        s_ask = float(s_quote.ask_price[0])
        s_bid = float(s_quote.bid_price[0])
        f_ask = float(f_quote.ask_price[0])
        f_bid = float(f_quote.bid_price[0])
        return s_ask, s_bid, f_ask, f_bid

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

    def _execute_phase(self, phase: str, stock_code: str, future_code: str, init_stock_px: float, init_fut_px: float):
        is_open = (phase == "open")
        self.pos_mgr.prepare_phase(stock_code, phase)

        if is_open:
            self.pos_mgr.set_future_pair(stock_code, future_code)
            self.pos_mgr.set_state(stock_code, PositionState.PENDING_ENTRY)
        else:
            self.pos_mgr.set_state(stock_code, PositionState.PENDING_EXIT)

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
                fs = ex.submit(self._place_stock_order, stock_code, stock_action, stock_px, STOCK_QTY)
                ff = ex.submit(self._place_future_order, future_code, future_action, fut_px, FUTURE_QTY, is_open)
                trade_s = fs.result()
                trade_f = ff.result()

            self.pos_mgr.register_order(trade_s.order.id, stock_code, phase, "stock")
            self.pos_mgr.register_order(trade_f.order.id, stock_code, phase, "future")
            print(f"[{phase.upper()}] orders sent: stock={trade_s.order.id}, future={trade_f.order.id}")
        except Exception as e:
            print(f"[{phase.upper()}] send orders failed: {e}")
            self.pos_mgr.set_state(stock_code, PositionState.EMPTY if is_open else PositionState.HOLDING)
            return

        sync = self.pos_mgr.get_phase_sync(stock_code, phase)
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

        pos = self.pos_mgr.get(stock_code)
        if is_open:
            st = pos.open_stock.status
            ft = pos.open_future.status
        else:
            st = pos.close_stock.status
            ft = pos.close_future.status

        st_filled = str(sj.constant.Status.Filled)

        if st != st_filled and ft != st_filled:
            print(f"[{phase.upper()}] no leg filled -> cancel both (best effort)")
            if trade_s: self._cancel_best_effort(trade_s)
            if trade_f: self._cancel_best_effort(trade_f)
            self.pos_mgr.set_state(stock_code, PositionState.EMPTY if is_open else PositionState.HOLDING)
            return

        print(f"[{phase.upper()}] single-leg filled -> REPAIR/UNWIND")
        top = self._get_top_prices(stock_code, future_code)
        if not top:
            print("  [Repair] no quotes -> set UNHEDGED (manual)")
            self.pos_mgr.set_state(stock_code, PositionState.UNHEDGED)
            return
        s_ask, s_bid, f_ask, f_bid = top

        if is_open:
            if ft == st_filled and st != st_filled:
                hedge_stock_buy = self._mk_mktable_price_strict("buy", s_ask, STOCK_BUY_TICKS)
                est_net = self._calc_open_net(hedge_stock_buy, f_bid)
                if est_net >= MIN_PROFIT_TO_REPAIR:
                    print(f"  [Repair] hedge stock buy @ {hedge_stock_buy:.2f}, est_net={est_net:.0f}")
                    self.pos_mgr.reset_phase_events_only(stock_code, phase)
                    try:
                        trade_fix = self._place_stock_order(stock_code, sj.constant.Action.Buy, hedge_stock_buy, STOCK_QTY)
                        self.pos_mgr.register_order(trade_fix.order.id, stock_code, phase, "stock")
                    except Exception as e:
                        print(f"  [Repair] stock hedge order failed: {e}")
                        self.pos_mgr.set_state(stock_code, PositionState.UNHEDGED)
                        return
                    sync = self.pos_mgr.get_phase_sync(stock_code, phase)
                    if sync.done.wait(timeout=REPAIR_TIMEOUT_SEC):
                        self.pos_mgr.set_state(stock_code, PositionState.HOLDING)
                        return

                unwind_fut_buy = self._mk_mktable_price_strict("buy", f_ask, FUT_BUY_TICKS)
                print(f"  [Unwind] buy back future @ {unwind_fut_buy:.2f}")
                try:
                    self._place_future_order(future_code, sj.constant.Action.Buy, unwind_fut_buy, FUTURE_QTY, phase_open=False)
                except Exception as e:
                    print(f"  [Unwind] future buyback failed: {e}")
                    self.pos_mgr.set_state(stock_code, PositionState.UNHEDGED)
                    return
                self.pos_mgr.set_state(stock_code, PositionState.EMPTY)
                return

            if st == st_filled and ft != st_filled:
                hedge_fut_sell = self._mk_mktable_price_strict("sell", f_bid, FUT_SELL_TICKS)
                est_net = self._calc_open_net(s_ask, hedge_fut_sell)
                if est_net >= MIN_PROFIT_TO_REPAIR:
                    print(f"  [Repair] hedge future sell @ {hedge_fut_sell:.2f}, est_net={est_net:.0f}")
                    self.pos_mgr.reset_phase_events_only(stock_code, phase)
                    try:
                        trade_fix = self._place_future_order(future_code, sj.constant.Action.Sell, hedge_fut_sell, FUTURE_QTY, phase_open=True)
                        self.pos_mgr.register_order(trade_fix.order.id, stock_code, phase, "future")
                    except Exception as e:
                        print(f"  [Repair] future hedge order failed: {e}")
                        self.pos_mgr.set_state(stock_code, PositionState.UNHEDGED)
                        return
                    sync = self.pos_mgr.get_phase_sync(stock_code, phase)
                    if sync.done.wait(timeout=REPAIR_TIMEOUT_SEC):
                        self.pos_mgr.set_state(stock_code, PositionState.HOLDING)
                        return

                unwind_stock_sell = self._mk_mktable_price_strict("sell", s_bid, STOCK_SELL_TICKS)
                print(f"  [Unwind] sell stock @ {unwind_stock_sell:.2f}")
                try:
                    self._place_stock_order(stock_code, sj.constant.Action.Sell, unwind_stock_sell, STOCK_QTY)
                except Exception as e:
                    print(f"  [Unwind] stock sell failed: {e}")
                    self.pos_mgr.set_state(stock_code, PositionState.UNHEDGED)
                    return
                self.pos_mgr.set_state(stock_code, PositionState.EMPTY)
                return

        else:
            if ft == st_filled and st != st_filled:
                hedge_stock_sell = self._mk_mktable_price_strict("sell", s_bid, STOCK_SELL_TICKS)
                print(f"  [Repair] sell stock @ {hedge_stock_sell:.2f}")
                self.pos_mgr.reset_phase_events_only(stock_code, phase)
                try:
                    trade_fix = self._place_stock_order(stock_code, sj.constant.Action.Sell, hedge_stock_sell, STOCK_QTY)
                    self.pos_mgr.register_order(trade_fix.order.id, stock_code, phase, "stock")
                except Exception as e:
                    print(f"  [Repair] stock close failed: {e}")
                    self.pos_mgr.set_state(stock_code, PositionState.UNHEDGED)
                    return
                sync = self.pos_mgr.get_phase_sync(stock_code, phase)
                if sync.done.wait(timeout=REPAIR_TIMEOUT_SEC):
                    self.pos_mgr.set_state(stock_code, PositionState.EMPTY)
                    return

                unwind_fut_sell = self._mk_mktable_price_strict("sell", f_bid, FUT_SELL_TICKS)
                print(f"  [Unwind-to-HOLDING] sell future again @ {unwind_fut_sell:.2f}")
                try:
                    self._place_future_order(future_code, sj.constant.Action.Sell, unwind_fut_sell, FUTURE_QTY, phase_open=True)
                except Exception as e:
                    print(f"  [Unwind] future resell failed: {e}")
                    self.pos_mgr.set_state(stock_code, PositionState.UNHEDGED)
                    return
                self.pos_mgr.set_state(stock_code, PositionState.HOLDING)
                return

            if st == st_filled and ft != st_filled:
                hedge_fut_buy = self._mk_mktable_price_strict("buy", f_ask, FUT_BUY_TICKS)
                print(f"  [Repair] buy future @ {hedge_fut_buy:.2f}")
                self.pos_mgr.reset_phase_events_only(stock_code, phase)
                try:
                    trade_fix = self._place_future_order(future_code, sj.constant.Action.Buy, hedge_fut_buy, FUTURE_QTY, phase_open=False)
                    self.pos_mgr.register_order(trade_fix.order.id, stock_code, phase, "future")
                except Exception as e:
                    print(f"  [Repair] future close failed: {e}")
                    self.pos_mgr.set_state(stock_code, PositionState.UNHEDGED)
                    return
                sync = self.pos_mgr.get_phase_sync(stock_code, phase)
                if sync.done.wait(timeout=REPAIR_TIMEOUT_SEC):
                    self.pos_mgr.set_state(stock_code, PositionState.EMPTY)
                    return

                unwind_stock_buy = self._mk_mktable_price_strict("buy", s_ask, STOCK_BUY_TICKS)
                print(f"  [Unwind-to-HOLDING] buy stock back @ {unwind_stock_buy:.2f}")
                try:
                    self._place_stock_order(stock_code, sj.constant.Action.Buy, unwind_stock_buy, STOCK_QTY)
                except Exception as e:
                    print(f"  [Unwind] stock buyback failed: {e}")
                    self.pos_mgr.set_state(stock_code, PositionState.UNHEDGED)
                    return
                self.pos_mgr.set_state(stock_code, PositionState.HOLDING)
                return

        self.pos_mgr.set_state(stock_code, PositionState.UNHEDGED)

    def run(self):
        print(">>> Execution Engine Started (Async Dual-Leg + Hedge Repair)")
        while self.running:
            try:
                signal = self.execution_queue.get(timeout=1)
                phase = "open" if signal.action_type == "OPEN" else "close"
                self._execute_phase(
                    phase=phase,
                    stock_code=signal.stock_code,
                    future_code=signal.future_code,
                    init_stock_px=signal.stock_price,
                    init_fut_px=signal.future_price
                )
            except queue.Empty:
                continue
            except Exception as e:
                print(f"Execution Error: {e}")

    def stop(self):
        self.running = False


# =========================
# ✅ 訂閱管理：一次訂閱 + 動態差集替換
# =========================
class SubscriptionManager:
    """
    目的：
    - 避免「重複訂閱」造成 Subscription Already Exists
    - 支援動態替換 pairs：只對差集做 subscribe/unsubscribe
    - 自動裁切 pairs 以符合 max_subs（每組 pair 需要 2 個 instrument：stock+future）

    狀態：
    - self.subscribed_codes：目前程式認為已訂閱的 code（只管 BidAsk v1）
    """
    def __init__(self, api: sj.Shioaji, max_subs: int = MAX_SUBS):
        self.api = api
        self.max_subs = int(max_subs)
        self.lock = threading.RLock()
        self.subscribed_codes: set[str] = set()

    def _is_stock_code(self, code: str) -> bool:
        # 台股現貨通常為 4 碼數字
        return isinstance(code, str) and code.isdigit() and len(code) == 4

    def _contract_of(self, code: str):
        if self._is_stock_code(code):
            return self.api.Contracts.Stocks[code]
        else:
            return self.api.Contracts.Futures[code]

    def _subscribe_one(self, code: str) -> bool:
        print(f"[DBG subscribe] t={time.time():.3f} tid={threading.get_ident()} code={code}")
        try:
            c = self._contract_of(code)
            self.api.quote.subscribe(
                c,
                quote_type=sj.constant.QuoteType.BidAsk,
                version=sj.constant.QuoteVersion.v1,  # ✅ 用 Enum，不用 "v1"
            )
            return True
        except Exception as e:
            print(f"[Subscription] subscribe failed {code}: {e}")
            return False

    def _unsubscribe_one(self, code: str) -> bool:
        try:
            c = self._contract_of(code)
            self.api.quote.unsubscribe(
                c,
                quote_type=sj.constant.QuoteType.BidAsk,
                version=sj.constant.QuoteVersion.v1,  # ✅
            )
            return True
        except Exception as e:
            print(f"[Subscription] unsubscribe failed {code}: {e}")
            return False

    def _trim_pairs_to_limit(self, pairs: list[tuple[str, str]]) -> list[tuple[str, str]]:
        # 每組 pairs 需要 2 個訂閱（stock + future）
        max_pairs = max(1, self.max_subs // 2)
        if len(pairs) > max_pairs:
            print(f"[⚠️] pairs={len(pairs)} will exceed MAX_SUBS={self.max_subs}. Trim to top {max_pairs} pairs.")
            return pairs[:max_pairs]
        return pairs

    def _target_codes_from_pairs(self, pairs: list[tuple[str, str]]) -> set[str]:
        codes = set()
        for s_code, f_code in pairs:
            codes.add(s_code)
            codes.add(f_code)
        return codes

    def apply_pairs(self, pairs: list[tuple[str, str]], log_prefix: str = "[Subscription]"):
        """
        核心：只做差集更新，杜絕重複 subscribe
        - pairs 需為「已排序」(成交量大到小)，這樣 trim 才合理
        """
        pairs = self._trim_pairs_to_limit(list(pairs))
        # ✅ DBG：檢查 pairs 裡是否有「同一檔 stock 重複出現」
        from collections import Counter
        stocks = [s for s, _ in pairs]
        c = Counter(stocks)
        dups = {k: v for k, v in c.items() if v > 1}
        print("[DBG] pair_cnt=", len(pairs), "unique_stock=", len(set(stocks)), "dup_stock=", dups)
        if dups:
            for s in dups.keys():
                futs = [f for ss, f in pairs if ss == s]
                print(f"[DBG] dup detail: stock={s} futures={futs}")
                for f in futs:
                    c = self.api.Contracts.Futures[f]
                    print(f, "name=", getattr(c, 'name', None),
                            "delivery_month=", getattr(c, 'delivery_month', None),
                            "delivery_date=", getattr(c, 'delivery_date', None),
                            "underlying=", getattr(c, 'underlying_code', None),
                            "exchange=", getattr(c, 'exchange', None),
                            "category=", getattr(c, 'category', None))        
        target_codes = self._target_codes_from_pairs(pairs)

        with self.lock:
            current = set(self.subscribed_codes)

        to_add = target_codes - current
        to_remove = current - target_codes

        if not to_add and not to_remove:
            print(f"{log_prefix} unchanged: now subscribed={len(current)} (limit={self.max_subs})")
            return

        # 先退訂（避免上限卡住）
        if to_remove:
            ok_codes = set()
            for code in sorted(to_remove):
                if self._unsubscribe_one(code):
                    ok_codes.add(code)

            with self.lock:
                self.subscribed_codes -= ok_codes

            print(f"{log_prefix} -unsubscribe {len(to_remove)} (ok={len(ok_codes)})")


        # 再訂閱新增
        if to_add:
            ok_codes = set()
            for code in sorted(to_add):
                if self._subscribe_one(code):
                    ok_codes.add(code)

            with self.lock:
                self.subscribed_codes |= ok_codes

            print(f"{log_prefix} +subscribe {len(to_add)} (ok={len(ok_codes)})")



        with self.lock:
            print(f"{log_prefix} now subscribed={len(self.subscribed_codes)} (limit={self.max_subs})")

# =========================
# ✅ PairRefresher：定期重找更適合 pairs，交給 SubscriptionManager 替換
# =========================
class PairRefresher(threading.Thread):
    """
    固定時間掃描 find_active_pairs(top_n)
    只有 pairs 真改變才：
      - strategy.update_pairs()
      - subscription.apply_pairs()
    """
    def __init__(
        self,
        discoverer: PairDiscoverer,
        strategy: StrategyEngine,
        sub_mgr: SubscriptionManager,
        top_n: int = TOP_N_PAIRS,
        interval_sec: int = 60,
    ):
        super().__init__()
        self.discoverer = discoverer
        self.strategy = strategy
        self.sub_mgr = sub_mgr
        self.top_n = int(top_n)
        self.interval_sec = int(interval_sec)

        self.running = True
        self._last_sig = None

    def _signature(self, pairs: List[Tuple[str, str]]) -> str:
        # pairs 已排序，signature 直接 join 就很穩
        return "|".join([f"{s}:{f}" for s, f in pairs])

    def run(self):
        print(f">>> Pair Refresher Started (every {self.interval_sec}s)")

        # 記錄 init signature
        try:
            init_pairs = self.discoverer.find_active_pairs(top_n=self.top_n)
            self._last_sig = self._signature(init_pairs)
            print("[Pairs] refresher init signature recorded")
        except Exception as e:
            print(f"[Pairs] refresher init failed: {e}")

        while self.running:
            # sleep（用小步進方便 stop）
            for _ in range(self.interval_sec * 10):
                if not self.running:
                    return
                time.sleep(0.1)

            try:
                pairs = self.discoverer.find_active_pairs(top_n=self.top_n)
                sig = self._signature(pairs)

                if sig == self._last_sig:
                    print("[Pairs] unchanged")
                    continue

                print(f"[Pairs] changed -> apply (pairs={len(pairs)})")
                self._last_sig = sig

                self.strategy.update_pairs(pairs, log_prefix="[Strategy] refresher")
                self.sub_mgr.apply_pairs(pairs, log_prefix="[Subscription]")

            except Exception as e:
                print(f"[Pairs] refresh failed: {e}")

    def stop(self):
        self.running = False

# =========================
# --- ArbitrageSystem ---
# =========================

class ArbitrageSystem:
    def __init__(self):
        self.api = sj.Shioaji(simulation=SIMULATION)

        self.market_data = MarketData()
        self.pos_mgr = PositionManager()

        self.quote_queue = queue.Queue()
        self.execution_queue = queue.Queue()

        self.active_pairs: List[Tuple[str, str]] = []

        # 訂閱管理器（負責 diff + 去重）
        self.sub_mgr = SubscriptionManager(self.api, max_subs=MAX_SUBS)

        # threads placeholders
        self.strategy_engine = None
        self.execution_engine = None
        self.account_monitor = None
        self.pair_refresher = None

    def login(self):
        if not SIMULATION:
            print("正在嘗試登入正式環境...")
            try:
                self.api.login(
                    api_key=CA_API_KEY,
                    secret_key=CA_SECRET_KEY,
                    contracts_cb=lambda security_type: print(f"{repr(security_type)} fetch done."),
                    subscribe_trade=True
                )
                print("API Key 登入成功，正在啟動 CA 憑證...")
                self.api.activate_ca(ca_path=CA_PATH, ca_passwd=CA_PASSWORD, person_id=PERSON_ID)
                print("CA 憑證啟動成功！")
            except Exception as e:
                print(f"正式環境登入或 CA 啟動失敗: {e}")
                raise e
        else:
            print("正在嘗試登入模擬環境...")
            try:
                self.api.login(api_key=CA_API_KEY, secret_key=CA_SECRET_KEY, contracts_cb=print)
                self.api.activate_ca(CA_PATH, CA_PASSWORD, PERSON_ID)
            except Exception as e:
                print(f"API Key 登入模擬環境失敗 (或 CA 載入失敗): {e}")
                raise e

        print("Login Success, fetching contracts...")
        time.sleep(5)

    def _setup_callbacks(self):
        @self.api.on_bidask_stk_v1(bind=True)
        def on_stock_quote(self_api, exchange, quote):
            self.market_data.update_stock(quote)
            self.quote_queue.put(quote)

        @self.api.on_bidask_fop_v1(bind=True)
        def on_future_quote(self_api, exchange, quote):
            self.market_data.update_future(quote)
            self.quote_queue.put(quote)

        self.api.set_order_callback(self._on_order_status)

    def _on_order_status(self, order_state, trade_dict):
        try:
            order_id = trade_dict.get('order', {}).get('id') or trade_dict.get('trade_id')
            status = trade_dict.get('status', {}).get('status') or trade_dict.get('status')
            if not order_id:
                return

            info = self.pos_mgr.lookup_order(order_id)
            if not info:
                return

            stock_code, phase, leg = info

            filled_qty = 0
            try:
                filled_qty = int(trade_dict.get('status', {}).get('deal_quantity') or 0)
            except:
                filled_qty = 0

            deal_price = 0.0
            try:
                deal_price = float(
                    trade_dict.get('status', {}).get('deal_price')
                    or trade_dict.get('status', {}).get('price')
                    or trade_dict.get('deal_price')
                    or 0.0
                )
            except:
                deal_price = 0.0

            status_s = str(status)

            self.pos_mgr.update_leg_status(
                stock_code=stock_code,
                phase=phase,
                leg=leg,
                status=status_s,
                filled_qty=filled_qty,
                deal_price=deal_price,
            )

            if status_s == str(sj.constant.Status.Filled):
                print(f"[成交] {phase.upper()} {leg.upper()} {stock_code} order={order_id} FILLED qty={filled_qty} px={deal_price}")
            elif status_s in [str(sj.constant.Status.Cancelled), str(sj.constant.Status.Failed)]:
                print(f"[失敗/取消] {phase.upper()} {leg.upper()} {stock_code} order={order_id} status={status_s}")

        except Exception as e:
            print(f"Callback Error: {e}")

    def _test_kbar_connectivity(self):
        print("\n--- [診斷] KBar 連線測試 (Target: 2330 台積電) ---")
        try:
            target = self.api.Contracts.Stocks['2330']
            print(f"合約物件檢查: {target}")
            print(f"交易所: {target.exchange}, 類別: {target.security_type}")

            today = datetime.datetime.now()
            start = (today - datetime.timedelta(days=10)).strftime('%Y-%m-%d')
            end = today.strftime('%Y-%m-%d')
            print(f"1. 嘗試抓取 KBar: {start} 到 {end}")
            kbars = self.api.kbars(target, start=start, end=end)
            df = pd.DataFrame({**kbars})
            print(f"   -> KBar 回傳資料是否為空: {df.empty}")

            today_str = datetime.datetime.now().strftime('%Y-%m-%d')
            print(f"2. 嘗試抓取今日 Ticks ({today_str})...")
            ticks = self.api.ticks(contract=target, date=today_str)
            df_ticks = pd.DataFrame({**ticks})
            print(f"   -> Ticks 回傳資料是否為空: {df_ticks.empty}")

            print(f"3. 嘗試抓取 Snapshot...")
            snaps = self.api.snapshots([target])
            if snaps:
                print(f"   -> Snapshot 回傳成功: TotalVol={snaps[0].total_volume} Price={snaps[0].close}")
            else:
                print(f"   -> Snapshot 回傳為空!")

            print(f"4. 嘗試抓取 Daily Quotes (全市場)...")
            try:
                dq = self.api.daily_quotes(date=today.date())
                if dq and dq.Code:
                    print(f"   -> DailyQuotes 回傳成功, 筆數: {len(dq.Code)}")
                    if '2330' in dq.Code:
                        idx = dq.Code.index('2330')
                        print(f"   -> 找到 2330: Vol={dq.Volume[idx]}")
                    else:
                        print("   -> DailyQuotes 中找不到 2330 (可能還未收盤或資料未產出)")
                else:
                    print("   -> DailyQuotes 回傳為空 (可能時間太早/非交易日)")
            except Exception as dq_e:
                print(f"   -> DailyQuotes 失敗: {dq_e}")

        except Exception as e:
            print(f"測試發生錯誤: {e}")
        print("------------------------------------------------\n")

    def _snapshot_init(self, pairs: List[Tuple[str, str]]):
        """
        用 snapshots 把 market_data 先塞一點初值，避免策略剛起來 quote 還沒來時全是 None
        """
        print("Fetching initial snapshots...")

        contracts_to_snapshot = []
        seen = set()

        for s_code, f_code in pairs:
            if s_code not in seen:
                try:
                    contracts_to_snapshot.append(self.api.Contracts.Stocks[s_code])
                    seen.add(s_code)
                except:
                    pass
            if f_code not in seen:
                try:
                    contracts_to_snapshot.append(self.api.Contracts.Futures[f_code])
                    seen.add(f_code)
                except:
                    pass

        if not contracts_to_snapshot:
            print("No contracts to snapshot.")
            return

        try:
            chunk_size = 50
            for i in range(0, len(contracts_to_snapshot), chunk_size):
                chunk = contracts_to_snapshot[i:i+chunk_size]
                snapshots = self.api.snapshots(chunk)

                for snap in snapshots:
                    wrapped_quote = SnapshotWrapper(snap)

                    # 用策略的 map 判斷是 stock 還 future
                    with self.strategy_engine._pairs_lock:
                        is_future = wrapped_quote.code in self.strategy_engine.future_to_stock
                        is_stock = wrapped_quote.code in self.strategy_engine.pair_map

                    if is_future:
                        self.market_data.update_future(wrapped_quote)
                        # print(f"  [Snapshot] Init Future {wrapped_quote.code}: Ask={wrapped_quote.ask_price[0]} Bid={wrapped_quote.bid_price[0]}")
                    elif is_stock:
                        self.market_data.update_stock(wrapped_quote)
                        # print(f"  [Snapshot] Init Stock  {wrapped_quote.code}: Ask={wrapped_quote.ask_price[0]} Bid={wrapped_quote.bid_price[0]}")

                    self.quote_queue.put(wrapped_quote)

        except Exception as e:
            print(f"Initial snapshot fetch failed: {e}")

    def start(self, top_n_pairs: int = TOP_N_PAIRS, refresh_interval_sec: int = 60):
        """
        你要的流程：
        1) setup callbacks
        2) optional diagnostic
        3) discover top N pairs
        4) init strategy/execution/monitor threads
        5) 一次訂閱（diff式，避免重複）
        6) snapshot init
        7) 啟動所有 threads
        8) 啟動 refresher：pairs 變才替換 + 動態更新訂閱
        """
        self._setup_callbacks()
        self._test_kbar_connectivity()

        discoverer = PairDiscoverer(self.api)

        # 1) 初次抓 top N
        self.active_pairs = discoverer.find_active_pairs(top_n=top_n_pairs)
        if not self.active_pairs:
            print("未發現符合成交量條件的配對，程式結束。")
            return

        # 2) 建立 engines
        self.strategy_engine = StrategyEngine(
            self.quote_queue, self.market_data, self.execution_queue, self.pos_mgr, self.active_pairs
        )
        self.execution_engine = ExecutionEngine(
            self.api, self.execution_queue, self.pos_mgr, self.market_data
        )
        self.account_monitor = AccountMonitor(self.api, self.pos_mgr)

        # 3) 先一次訂閱（最重要：只在這裡做一次 init apply）
        self.sub_mgr.apply_pairs(self.active_pairs, log_prefix="[Subscription]")

        # 4) snapshot init（在 threads 跑之前先塞入）
        self._snapshot_init(self.active_pairs)

        # 5) start threads
        self.strategy_engine.start()
        self.execution_engine.start()
        self.account_monitor.start()

        # 6) Pair refresher：只有 pairs 變才更新 strategy + subscription
        self.pair_refresher = PairRefresher(
            discoverer=discoverer,
            strategy=self.strategy_engine,
            sub_mgr=self.sub_mgr,
            top_n=top_n_pairs,
            interval_sec=refresh_interval_sec
        )
        self.pair_refresher.start()

        # 7) main loop
        try:
            while True:
                time.sleep(1)
        except KeyboardInterrupt:
            self.stop()

    def stop(self):
        print("Stopping System...")

        try:
            if self.pair_refresher:
                self.pair_refresher.stop()
                self.pair_refresher.join()
        except Exception as e:
            print(f"Stop pair_refresher error: {e}")

        try:
            if self.strategy_engine:
                self.strategy_engine.stop()
                self.strategy_engine.join()
        except Exception as e:
            print(f"Stop strategy_engine error: {e}")

        try:
            if self.execution_engine:
                self.execution_engine.stop()
                self.execution_engine.join()
        except Exception as e:
            print(f"Stop execution_engine error: {e}")

        try:
            if self.account_monitor:
                self.account_monitor.stop()
                self.account_monitor.join()
        except Exception as e:
            print(f"Stop account_monitor error: {e}")

        try:
            self.api.logout()
        except Exception as e:
            print(f"Logout error: {e}")

if __name__ == "__main__":
    system = ArbitrageSystem()
    system.login()
    system.start()
