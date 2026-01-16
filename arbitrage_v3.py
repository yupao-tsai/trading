import shioaji as sj
import datetime
import threading
import queue
import time
import pandas as pd
from typing import Dict, List, Tuple, Optional
from enum import Enum
import concurrent.futures


from decimal import Decimal, ROUND_FLOOR, ROUND_CEILING


import json
import os
from dotenv import load_dotenv
load_dotenv()

# --- 全域設定 ---
SIMULATION = True      # 是否為模擬環境
STOCK_QTY = 2          # 現貨口數 (張)
FUTURE_QTY = 1         # 期貨口數 (口)
ENTRY_THRESHOLD = 5000 # 進場價差門檻 (金額)
EXIT_THRESHOLD = 0     # 出場價差門檻 (金額)
MIN_VOLUME_THRESHOLD = 5 # 自動篩選：期貨日均量門檻 (口) [模擬環境調整]
VOLUME_HISTORY_FILE = "volume_history.json" # 交易量記錄檔

# 簡易帳號設定
PERSON_ID = os.getenv("Sinopack_PERSON_ID")
PASSWORD = os.getenv("Sinopack_PASSWORD")
CA_API_KEY = os.getenv("Sinopack_CA_API_KEY")
CA_SECRET_KEY = os.getenv("Sinopack_CA_SECRET_KEY")
CA_PATH = os.getenv("Sinopack_CA_PATH")
CA_PASSWORD = os.getenv("Sinopack_CA_PASSWORD")


# --- Hedge / Repair 參數 ---
HEDGE_TIMEOUT_SEC = 0.8      # 等雙腿成交的最長時間（模擬環境可拉長）
REPAIR_TIMEOUT_SEC = 0.8     # 補腿後再等成交的時間
MIN_PROFIT_TO_REPAIR = 0     # 只成交一腿時，若補腿後「最壞估算」仍>=此值才補（可設 >0 更保守）

# 追價保護（用百分比做 marketable limit，避免 tick size 問題；你可之後改成 tick-based）
STOCK_BUY_BUFFER_PCT = 0.003   # +0.3% 買入保護
STOCK_SELL_BUFFER_PCT = 0.003  # -0.3% 賣出保護
FUT_BUY_BUFFER_PCT = 0.001     # +0.1% 買回保護
FUT_SELL_BUFFER_PCT = 0.001    # -0.1% 賣出保護

# --- Tick-based 保護：幾檔 ---
STOCK_BUY_TICKS = 1     # 買股票：在 ask 上再加幾檔，確保可成交
STOCK_SELL_TICKS = 1    # 賣股票：在 bid 下再減幾檔
FUT_BUY_TICKS = 1       # 買回期貨：在 ask 上再加幾檔
FUT_SELL_TICKS = 1      # 賣出期貨：在 bid 下再減幾檔

# --- 合約換算（你提供：1口=2000股；現股1張=1000股）---
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

# --- 狀態列舉 ---
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
    deal_price: float = 0.0   # ✅ 新增：可選，用來做更精準 repair 估算

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

    # 開/平 倉 雙腿成交狀態
    open_stock: LegFill = field(default_factory=LegFill)
    open_future: LegFill = field(default_factory=LegFill)
    close_stock: LegFill = field(default_factory=LegFill)
    close_future: LegFill = field(default_factory=LegFill)

    # phase 等待事件（ExecutionEngine 用）
    open_sync: PhaseSync = field(default_factory=PhaseSync)
    close_sync: PhaseSync = field(default_factory=PhaseSync)
# =========================
# 1) PositionManager (完整)
# =========================
class PositionManager:
    """
    - 以 stock_code 為主鍵
    - order_id -> (stock_code, phase(open/close), leg(stock/future))
    - callback 更新 LegFill + 設定 done/failed event
    """
    def __init__(self):
        self.lock = threading.RLock()  # ✅ 避免 get() 巢狀呼叫時死鎖
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
        """
        ✅【新的一輪 phase】開始前要呼叫：
        - 清掉上一輪 leg 狀態（避免舊的 Filled/Cancelled 影響）
        - reset event
        """
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
        """
        ✅【repair 專用】：
        只 reset done/failed event（為了等待補腿的回報）
        ❗不要清 open_stock/open_future 或 close_stock/close_future
          否則會把已成交那腿清掉，done 永遠不會來
        """
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

            # 取目標 leg 與 sync
            if phase == "open":
                tgt = p.open_stock if leg == "stock" else p.open_future
                sync = p.open_sync
            else:
                tgt = p.close_stock if leg == "stock" else p.close_future
                sync = p.close_sync

            # 更新 leg 狀態
            tgt.status = str(status)
            if filled_qty:
                tgt.filled_qty = max(tgt.filled_qty, int(filled_qty))
            if deal_price:
                tgt.deal_price = float(deal_price)

            # 統一 status 字串比較
            st_filled = str(sj.constant.Status.Filled)
            st_failed = str(sj.constant.Status.Failed)
            st_cancel = str(sj.constant.Status.Cancelled)

            # 有任何一腿 Failed/Cancelled => 直接 failed
            if tgt.status in [st_failed, st_cancel]:
                sync.failed.set()
                return

            # 兩腿都 Filled => done
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


class SnapshotWrapper:
    """
    將 Snapshot 物件包裝成類似 BidAskSTKv1/FOPv1 的介面，
    以便 StrategyEngine 可以統一處理。
    """
    def __init__(self, snapshot):
        self.code = snapshot.code
        # Snapshot 的 buy_price/sell_price 對應 Bid/Ask
        # 注意：Snapshot 只有最佳一檔，所以 list 長度為 1
        self.ask_price = [float(snapshot.sell_price)] if snapshot.sell_price else [0.0]
        self.bid_price = [float(snapshot.buy_price)] if snapshot.buy_price else [0.0]
        
        # 為了相容性，提供 ask_volume/bid_volume (Snapshot 有 buy_volume/sell_volume)
        # 注意: snapshot.sell_volume 是 int, snapshot.buy_volume 是 float (在某些版本定義可能有誤，這裡轉 int)
        self.ask_volume = [int(snapshot.sell_volume)] if snapshot.sell_volume else [0]
        self.bid_volume = [int(snapshot.buy_volume)] if snapshot.buy_volume else [0]
        
        # 模擬 datetime
        self.datetime = datetime.datetime.now()
        if hasattr(snapshot, 'ts') and snapshot.ts:
             try:
                 self.datetime = datetime.datetime.fromtimestamp(snapshot.ts / 1e9)
             except: pass

class AccountMonitor(threading.Thread):
    """
    負責定期查詢實際帳戶庫存，並更新 PositionManager 的狀態。
    確保策略狀態與實際帳戶一致。
    """
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
            
            # Poll every 5 seconds
            for _ in range(50):
                if not self.running: break
                time.sleep(0.1)

    def sync_positions(self):
        # 查期貨
        try:
            future_positions = self.api.list_positions(account=self.futopt_account)
        except Exception as e:
            future_positions = []

        # 查現貨
        try:
            stock_positions = self.api.list_positions(account=self.stock_account)
        except Exception as e:
            stock_positions = []

        real_stock = {}   # stock_code -> net shares(張換股數後)
        real_future = {}  # future_code -> net contracts

        # 現貨：quantity 通常是「張」，我們轉成股數方便比
        for pos in stock_positions:
            code = pos.code
            vol_lot = int(pos.quantity)
            direction = pos.direction
            sign = 1 if direction == sj.constant.Action.Buy else -1
            real_stock[code] = real_stock.get(code, 0) + sign * (vol_lot * STOCK_SHARES_PER_LOT)

        # 期貨：quantity 是口數
        for pos in future_positions:
            code = pos.code
            vol = int(pos.quantity)
            direction = pos.direction
            sign = 1 if direction == sj.constant.Action.Buy else -1
            real_future[code] = real_future.get(code, 0) + sign * vol

        # ✅ 對 PositionManager 內已知的股票做對帳
        # 只修正「明確矛盾」的狀態，避免覆蓋 PENDING 的短暫狀態
        with self.pos_mgr.lock:
            for stock_code, p in self.pos_mgr._pos.items():
                state = p.state
                f_code = p.future_code

                stock_shares = real_stock.get(stock_code, 0)
                fut_contracts = real_future.get(f_code, 0) if f_code else 0

                # 我們期待：
                # 開倉狀態：股票 +2000股（買） & 期貨 -1口（賣）
                expected_stock = STOCK_QTY * STOCK_SHARES_PER_LOT
                expected_fut = -FUTURE_QTY

                if state in [PositionState.PENDING_ENTRY, PositionState.PENDING_EXIT]:
                    continue

                # 1) 兩邊都沒有：應是 EMPTY
                if stock_shares == 0 and fut_contracts == 0:
                    if state != PositionState.EMPTY:
                        p.state = PositionState.EMPTY
                        print(f"[Monitor Sync] {stock_code} -> EMPTY (no real positions)")
                    continue

                # 2) 兩邊都有且符合方向/口數：應是 HOLDING
                if (stock_shares == expected_stock) and (fut_contracts == expected_fut):
                    if state != PositionState.HOLDING:
                        p.state = PositionState.HOLDING
                        print(f"[Monitor Sync] {stock_code} -> HOLDING (real matched)")
                    continue

                # 3) 其他狀況：視為 UNHEDGED（不讓策略再動）
                if state != PositionState.UNHEDGED:
                    p.state = PositionState.UNHEDGED
                    print(f"[Monitor Sync] {stock_code} -> UNHEDGED (stock={stock_shares}, fut={fut_contracts})")


    def stop(self):
        self.running = False

class VolumeManager:
    """
    負責管理成交量的歷史記錄 (Sliding Window 模式)。
    記錄檔格式: 
    { 
      "stock_code": { 
          "date": "YYYY-MM-DD",           # 最後更新日期
          "volumes": [v1, v2, v3, v4, v5] # 最近 5 個完整交易日的量
      } 
    }
    """
    def __init__(self, filename=VOLUME_HISTORY_FILE):
        self.filename = filename
        self.today_str = datetime.datetime.now().strftime('%Y-%m-%d')
        self.data = self._load()

    def _load(self):
        if os.path.exists(self.filename):
            try:
                with open(self.filename, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    # 簡單檢查格式，如果是舊版格式 (無 volumes 欄位)，則回傳空 dict 強制重抓
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
        """
        取得 5日均量。
        """
        record = self.data.get(code)
        if not record or 'volumes' not in record:
            return 0
            
        saved_date = record['date']
        volumes = record['volumes']
        
        if not volumes: return 0

        # 過濾掉 0 的成交量 (針對新合約)
        valid_volumes = [v for v in volumes if v > 0]
        if not valid_volumes:
            return 0
            
        avg_vol = int(sum(valid_volumes) / len(valid_volumes))

        # 情境 1: 盤中 (或是今天已經更新過了)
        if not is_market_closed or saved_date == self.today_str:
            return avg_vol
        
        # 情境 2: 盤後，但資料是昨天的 -> 需要 Slide Update
        return 0

    def peek_avg(self, code: str) -> int:
        """只讀取記錄中的平均值，不管日期是否過期 (用於 Fallback)"""
        record = self.data.get(code)
        if not record or 'volumes' not in record: return 0
        
        valid = [v for v in record['volumes'] if v > 0]
        if not valid: return 0
        return int(sum(valid) / len(valid))


    def update_window_init(self, code: str, vol_list: list):
        """初始化或重建視窗 (直接存入 5 筆資料)"""
        # 確保只存最後 5 筆
        final_list = vol_list[-5:]
        self.data[code] = {
            "date": self.today_str,
            "volumes": final_list
        }
        self._save()

    def slide_window(self, code: str, new_vol: int):
        """滑動視窗：加入今天，踢掉最舊"""
        record = self.data.get(code)
        if record and 'volumes' in record:
            volumes = record['volumes']
            volumes.append(int(new_vol))
            if len(volumes) > 5:
                volumes.pop(0) # 踢掉最舊
            
            self.data[code] = {
                "date": self.today_str,
                "volumes": volumes
            }
            self._save()

class PairDiscoverer:
    """
    負責自動發現流動性足夠的股票期貨配對
    """
    def __init__(self, api: sj.Shioaji):
        self.api = api
        self.vol_mgr = VolumeManager()
        self.daily_quotes_map = {}  # Cache for Daily Quotes
        self._fetch_daily_quotes_cache()

    def _fetch_daily_quotes_cache(self):
        """嘗試預先抓取 Daily Quotes 作為全市場成交量快取"""
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
        """判斷日盤是否已收盤 (08:45 ~ 13:45 之外視為收盤)"""
        now = datetime.datetime.now().time()
        start = datetime.time(8, 45)
        end = datetime.time(13, 45)
        return not (start <= now <= end)

    def _fetch_last_n_days_volumes(self, contract_or_code, n_days=5) -> List[int]:
        """Helper: 抓取過去 N 個完整交易日的成交量列表"""
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

            debug_codes = ['1303', '2002', '2330']
            t_code = getattr(target, 'code', '')
            if t_code in debug_codes:
                print(f"  [Debug KBar] {t_code} Empty? {df.empty}")
                if not df.empty:
                    print(f"  [Debug KBar] {t_code} First Row: {df.iloc[0].to_dict() if len(df)>0 else 'None'}")
                    print(f"  [Debug KBar] {t_code} Last Row: {df.iloc[-1].to_dict() if len(df)>0 else 'None'}")

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
        """
        智慧型成交量取得 (現貨/期貨通用)：
        1) Cache 5MA
        2) Snapshot 今日量
        3) Daily Quotes (股票)
        4) KBar 歷史 5 天
        5) fallback 舊 cache / snapshot
        """
        cache_key = code
        if contract_obj:
            stype = str(contract_obj.security_type)
            if stype in ['FUT', 'Future', str(sj.constant.SecurityType.Future)] and hasattr(contract_obj, 'underlying_code'):
                cache_key = f"F_{contract_obj.underlying_code}"

        is_closed = self._is_day_market_closed()

        # 1) Cache
        cached_avg = self.vol_mgr.get_5ma(cache_key, is_closed)
        if cached_avg > 0:
            return cached_avg

        # ✅ 盤後且 cache 過期：把「今天量」塞進 sliding window
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

                # ✅ FIX：參數名要用 is_market_closed
                refreshed = self.vol_mgr.get_5ma(cache_key, is_market_closed=True)
                if refreshed > 0:
                    return refreshed

        target = contract_obj if contract_obj else self.api.Contracts.Stocks[code]

        # 2) Snapshot
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

        # 2.5) Daily Quotes (股票 fallback)
        if code in self.daily_quotes_map:
            dq_vol = int(self.daily_quotes_map[code])
            if dq_vol > MIN_VOLUME_THRESHOLD:
                self.vol_mgr.update_window_init(cache_key, [dq_vol])
                return dq_vol

        # 3) KBar
        vol_list = self._fetch_last_n_days_volumes(target, n_days=5)
        if len(vol_list) > 0:
            self.vol_mgr.update_window_init(cache_key, vol_list)
            valid_vols = [v for v in vol_list if v > 0]
            if valid_vols:
                return int(sum(valid_vols) / len(valid_vols))
            return 0

        # 4) fallback 舊 cache
        last_avg = self.vol_mgr.peek_avg(cache_key)
        if last_avg > 0:
            return last_avg

        # 5) 最後用 snapshot
        if snapshot_vol > 0:
            return snapshot_vol

        return 0

    def find_active_pairs(self, top_n: int = 30) -> List[Tuple[str, str]]:
        print("正在掃描活躍的股票期貨配對 (Smart Sliding Window)...")

        today = datetime.datetime.now()
        target_month_str = today.strftime('%Y%m')

        candidates = []
        seen_contracts = set()

        print("1. 篩選近月期貨合約...")
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

                is_futures = str(contract.security_type) in ['FUT', 'Future', str(sj.constant.SecurityType.Future)]

                if (
                    is_futures
                    and hasattr(contract, 'underlying_code')
                    and len(contract.underlying_code) == 4
                    and contract.delivery_month == target_month_str
                ):
                    candidates.append(contract)

        total_candidates = len(candidates)
        print(f"找到 {total_candidates} 檔近月合約，開始檢查流動性...")
        print("提示：首次執行會建立快取，第二次執行將大幅加速。")

        # 改：收集 (score, stock_avg, future_avg, stock_code, future_code)
        best_by_pair = {}  # key=(stock_code,future_code) -> best record (避免 set() 亂序)

        count = 0
        for contract in candidates:
            stock_code = contract.underlying_code
            future_code = contract.code

            stock_avg = self._get_avg_volume_smart(stock_code, contract_obj=None)
            if count < 5 or stock_avg > 0:
                print(f"  [Debug] {stock_code}: Stock 5MA = {stock_avg}")

            if stock_avg < MIN_VOLUME_THRESHOLD:
                count += 1
                continue

            future_avg = self._get_avg_volume_smart(future_code, contract_obj=contract)
            if future_avg > 0:
                print(f"  [Debug] {future_code}: Future 5MA = {future_avg}")

            if future_avg >= MIN_VOLUME_THRESHOLD:
                # ✅ 排序用「瓶頸腿」：兩邊都要有量才好成交
                score = min(stock_avg, future_avg)

                print(f"  [加入監控候選] {stock_code}/{future_code} | score(min)={score} | 期貨5MA:{future_avg} | 現貨5MA:{stock_avg}")

                key = (stock_code, future_code)
                rec = (score, stock_avg, future_avg, stock_code, future_code)

                # 同一組出現多次時，保留 score 最大那筆
                if key not in best_by_pair or rec[0] > best_by_pair[key][0]:
                    best_by_pair[key] = rec

            count += 1
            if count % 10 == 0:
                print(f"  進度: {count}/{total_candidates}...", end='\r')
                time.sleep(0.01)

        ranked = list(best_by_pair.values())
        ranked.sort(key=lambda x: x[0], reverse=True)  # score 由大到小

        top = ranked[:top_n]

        print(f"\n篩選完成，共 {len(ranked)} 組符合條件。回傳前 {len(top)} 組（按成交量排序）。")
        for i, (score, s_avg, f_avg, s_code, f_code) in enumerate(top, 1):
            print(f"  [Top{i:02d}] {s_code}/{f_code} | score(min)={score} | stock5MA={s_avg} fut5MA={f_avg}")

        # ✅ 最終只回傳前 top_n 組
        return [(s_code, f_code) for (_, _, _, s_code, f_code) in top]


class MarketData:
    def __init__(self):
        self.lock = threading.Lock()
        self.stock_quotes: Dict[str, sj.BidAskSTKv1] = {}
        self.future_quotes: Dict[str, sj.BidAskFOPv1] = {}

    def update_stock(self, quote: sj.BidAskSTKv1):
        with self.lock:
            self.stock_quotes[quote.code] = quote

    def update_future(self, quote: sj.BidAskFOPv1):
        with self.lock:
            self.future_quotes[quote.code] = quote

    def get_quotes(self, stock_code: str, future_code: str):
        with self.lock:
            return (
                self.stock_quotes.get(stock_code),
                self.future_quotes.get(future_code)
            )

class TradeSignal:
    def __init__(self, action_type: str, stock_code: str, future_code: str, 
                 stock_price: float, future_price: float):
        self.action_type = action_type # 'OPEN' or 'CLOSE'
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
        self.pairs = pairs
        self.running = True

        self.pair_map: Dict[str, List[str]] = {}
        self.future_to_stock: Dict[str, str] = {}
        for s_code, f_code in self.pairs:
            self.pair_map.setdefault(s_code, []).append(f_code)
            self.future_to_stock[f_code] = s_code

    def calculate_cost(self, stock_price: float, future_price: float) -> float:
        """
        ✅ 你補充的背景已反映：股票2張=2000股、期貨1口=2000股
        成本用「實際成交名目」算：
        - 股票: stock_price * 1000 * STOCK_QTY
        - 期貨: future_price * 2000 * FUTURE_QTY
        """
        stock_notional = stock_price * STOCK_SHARES_PER_LOT * STOCK_QTY
        future_notional = future_price * FUTURE_SHARES_EQUIV * FUTURE_QTY

        stock_fee = stock_notional * 0.001425 * 0.3
        future_fee = 20 * FUTURE_QTY
        tax_stock = stock_notional * 0.003
        tax_future = future_notional * 0.00002  # 依你原本假設

        return stock_fee + future_fee + tax_stock + tax_future

    def run(self):
        print(">>> Strategy Engine Started (Event-Driven Mode)")
        while self.running:
            try:
                quote = self.quote_queue.get(timeout=1)

                target_futures = []
                target_stock = None

                if quote.code in self.pair_map:
                    target_stock = quote.code
                    target_futures = self.pair_map[quote.code]
                elif quote.code in self.future_to_stock:
                    target_stock = self.future_to_stock[quote.code]
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

                        # ✅ 以等價股數的 notional 比（兩邊都是 2000股）
                        stock_notional = stock_ask * STOCK_SHARES_PER_LOT * STOCK_QTY
                        future_notional = future_bid * FUTURE_SHARES_EQUIV * FUTURE_QTY
                        spread_value = future_notional - stock_notional

                        cost = self.calculate_cost(stock_ask, future_bid)
                        net_profit = spread_value - cost

                        if net_profit > ENTRY_THRESHOLD:
                            print(f"[訊號-開倉] {stock_code}/{future_code} | 預期淨利: {net_profit:.0f}")
                            # ✅ 先記住是哪個期貨
                            self.pos_mgr.set_future_pair(stock_code, future_code)
                            self.pos_mgr.set_state(stock_code, PositionState.PENDING_ENTRY)
                            self.execution_queue.put(TradeSignal('OPEN', stock_code, future_code, stock_ask, future_bid))
                            break

                # --- 出場：只針對「當初開倉的 future」判斷 ---
                elif state == PositionState.HOLDING:
                    future_code = pos.future_code
                    if not future_code:
                        # 理論上不該發生，保守起見
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

# =========================
# 2) ExecutionEngine (完整)
# =========================
class ExecutionEngine(threading.Thread):
    def __init__(self, api: sj.Shioaji, execution_queue: queue.Queue, position_manager: PositionManager, market_data: MarketData):
        super().__init__()
        self.api = api
        self.execution_queue = execution_queue
        self.pos_mgr = position_manager
        self.market_data = market_data
        self.running = True

    # ---------- helpers ----------
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

    def _marketable_price(self, side: str, px: float, buffer_pct: float) -> float:
        # side: "buy" or "sell"
        if px <= 0:
            return px
        if side == "buy":
            return float(px) * (1.0 + buffer_pct)
        else:
            return float(px) * (1.0 - buffer_pct)

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
        trade_s = self.api.place_order(contract_s, order_s)
        return trade_s

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
        trade_f = self.api.place_order(contract_f, order_f)
        return trade_f

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
        """
        台股/個股期貨常見的分級 tick（STF 規格也採同級距）
        """
        if px < 10: return 0.01
        if px < 50: return 0.05
        if px < 100: return 0.1
        if px < 500: return 0.5
        if px < 1000: return 1.0
        return 5.0

    def _round_to_tick(self, px: float, tick: float, mode: str) -> float:
        """
        mode: 'ceil' or 'floor'
        用 Decimal 避免浮點誤差造成違規 tick
        """
        if tick <= 0 or px <= 0:
            return float(px)
        dpx = Decimal(str(px))
        dt = Decimal(str(tick))
        q = (dpx / dt)
        if mode == "ceil":
            q2 = q.to_integral_value(rounding=ROUND_CEILING)
        else:
            q2 = q.to_integral_value(rounding=ROUND_FLOOR)
        return float(q2 * dt)

    def _round_to_tick_by_its_own_tier(self, px: float, side: str) -> float:
        """
        用「px 自己所在 tier 的 tick」去做合法對齊。
        - buy:  ceil
        - sell: floor
        """
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
        """
        嚴謹地走 1 檔：
        1) 用「目前 px 所在 tier 的 tick」往上/下走一步
        2) 走完後，再用「新 px 自己的 tier」對齊一次（處理跨 tier 的刻度變化）
        """
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
        """
        最嚴謹的「加/減 N 檔」：
        - 每走一檔就重新判定 tick (跨 tier 時會自動換 tick)
        - 最終價格一定是「以自身 tier」合法刻度對齊
        """
        if ref_px <= 0:
            return ref_px

        px = self._round_to_tick_by_its_own_tier(ref_px, "buy" if side == "buy" else "sell")

        n = int(max(0, protect_ticks))
        for _ in range(n):
            px = self._step_one_tick_strict(px, side)

        px = self._round_to_tick_by_its_own_tier(px, side)
        return px

    def _mk_mktable_price(self, side: str, ref_px: float, protect_ticks: int) -> float:
        """
        side: 'buy' or 'sell'
        buy  : 用 ask 當 ref，往上加 ticks，再做 ceil 對齊
        sell : 用 bid 當 ref，往下減 ticks，再做 floor 對齊
        """
        if ref_px <= 0:
            return ref_px

        tick = self._tick_size_by_price(ref_px)
        if side == "buy":
            px = ref_px + protect_ticks * tick
            return self._round_to_tick(px, tick, "ceil")
        else:
            px = ref_px - protect_ticks * tick
            px = max(px, tick)
            return self._round_to_tick(px, tick, "floor")

    # ---------- core: async dual-leg with repair ----------
    def _execute_phase(self, phase: str, stock_code: str, future_code: str, init_stock_px: float, init_fut_px: float):
        """
        phase: "open" or "close"
        - open: stock buy + future sell
        - close: stock sell + future buy
        """
        is_open = (phase == "open")

        # ✅ FIX：新的一輪 phase 開始，要清掉上一輪 leg 狀態 + reset events
        # ❗repair 才能用 reset_phase_events_only
        self.pos_mgr.prepare_phase(stock_code, phase)

        # 設定狀態
        if is_open:
            self.pos_mgr.set_future_pair(stock_code, future_code)
            self.pos_mgr.set_state(stock_code, PositionState.PENDING_ENTRY)
        else:
            self.pos_mgr.set_state(stock_code, PositionState.PENDING_EXIT)

        # 初始下單（用策略給的價格）
        stock_action = sj.constant.Action.Buy if is_open else sj.constant.Action.Sell
        future_action = sj.constant.Action.Sell if is_open else sj.constant.Action.Buy

        # marketable tick 保護
        if is_open:
            stock_px = self._mk_mktable_price_strict("buy", init_stock_px, STOCK_BUY_TICKS)
            fut_px   = self._mk_mktable_price_strict("sell", init_fut_px, FUT_SELL_TICKS)
        else:
            stock_px = self._mk_mktable_price_strict("sell", init_stock_px, STOCK_SELL_TICKS)
            fut_px   = self._mk_mktable_price_strict("buy", init_fut_px, FUT_BUY_TICKS)

        trade_s = None
        trade_f = None

        # 並行送單
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
            if is_open:
                self.pos_mgr.set_state(stock_code, PositionState.EMPTY)
            else:
                self.pos_mgr.set_state(stock_code, PositionState.HOLDING)
            return

        # 等回報（done/failed/timeout）
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

        # 走到這：failed 或 timeout → 判斷目前雙腿狀態
        pos = self.pos_mgr.get(stock_code)
        if is_open:
            st = pos.open_stock.status
            ft = pos.open_future.status
        else:
            st = pos.close_stock.status
            ft = pos.close_future.status

        st_filled = str(sj.constant.Status.Filled)

        # Case 1：兩腿都沒成交 → 撤單，回到原狀態
        if st != st_filled and ft != st_filled:
            print(f"[{phase.upper()}] no leg filled -> cancel both (best effort)")
            if trade_s: self._cancel_best_effort(trade_s)
            if trade_f: self._cancel_best_effort(trade_f)
            self.pos_mgr.set_state(stock_code, PositionState.EMPTY if is_open else PositionState.HOLDING)
            return

        # Case 2：只成交一腿 → repair 或 unwind
        print(f"[{phase.upper()}] single-leg filled -> REPAIR/UNWIND")
        top = self._get_top_prices(stock_code, future_code)
        if not top:
            print("  [Repair] no quotes -> set UNHEDGED (manual)")
            self.pos_mgr.set_state(stock_code, PositionState.UNHEDGED)
            return
        s_ask, s_bid, f_ask, f_bid = top

        if is_open:
            # 開倉：目標最後要 HOLDING（long stock + short future）
            if ft == st_filled and st != st_filled:
                # 期貨已賣出，缺股票買入
                hedge_stock_buy = self._mk_mktable_price_strict("buy", s_ask, STOCK_BUY_TICKS)
                est_net = self._calc_open_net(hedge_stock_buy, f_bid)
                if est_net >= MIN_PROFIT_TO_REPAIR:
                    print(f"  [Repair] hedge stock buy @ {hedge_stock_buy:.2f}, est_net={est_net:.0f}")

                    # ✅ repair：只 reset events，不清掉已成交那腿
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

                # 不划算或補腿失敗 → 把期貨沖掉（買回）
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
                # 股票已買入，缺期貨賣出
                hedge_fut_sell = self._mk_mktable_price_strict("sell", f_bid, FUT_SELL_TICKS)
                est_net = self._calc_open_net(s_ask, hedge_fut_sell)
                if est_net >= MIN_PROFIT_TO_REPAIR:
                    print(f"  [Repair] hedge future sell @ {hedge_fut_sell:.2f}, est_net={est_net:.0f}")

                    # ✅ repair：只 reset events
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

                # 不划算或補腿失敗 → 把股票沖掉（賣出）
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
            # 平倉：目標最後要 EMPTY（sell stock + buy future back）
            if ft == st_filled and st != st_filled:
                # 期貨已買回，缺股票賣出
                hedge_stock_sell = self._mk_mktable_price_strict("sell", s_bid, STOCK_SELL_TICKS)
                print(f"  [Repair] sell stock @ {hedge_stock_sell:.2f}")

                # ✅ repair：只 reset events
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

                # 補不到 → 反向把期貨再賣出回去（回到 HOLDING）
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
                # 股票已賣出，缺期貨買回
                hedge_fut_buy = self._mk_mktable_price_strict("buy", f_ask, FUT_BUY_TICKS)
                print(f"  [Repair] buy future @ {hedge_fut_buy:.2f}")

                # ✅ repair：只 reset events
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

                # 補不到 → 把股票再買回去回到 HOLDING
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

        # 其他情形保守處理
        self.pos_mgr.set_state(stock_code, PositionState.UNHEDGED)

    # ---------- thread loop ----------
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
# 3) ArbitrageSystem (完整)
# =========================
class ArbitrageSystem:
    def __init__(self):
        self.api = sj.Shioaji(simulation=SIMULATION)
        self.market_data = MarketData()
        self.pos_mgr = PositionManager()
        self.quote_queue = queue.Queue()
        self.execution_queue = queue.Queue()
        self.active_pairs: List[Tuple[str, str]] = []

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
        """
        ✅ FIX：
        - status 統一轉字串比對，避免版本差異導致 log/判斷失準
        - 嘗試抓 deal_price 傳到 PositionManager（可讓 repair 估算更精準）
        """
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
                    print("   -> DailyQuotes 回傳為空 (可能時間太早)")
            except Exception as dq_e:
                print(f"   -> DailyQuotes 失敗: {dq_e}")

        except Exception as e:
            print(f"測試發生錯誤: {e}")
        print("------------------------------------------------\n")

    def start(self):
        self._setup_callbacks()
        self._test_kbar_connectivity()

        discoverer = PairDiscoverer(self.api)
        self.active_pairs = discoverer.find_active_pairs(top_n=30)

        if not self.active_pairs:
            print("未發現符合成交量條件的配對，程式結束。")
            return

        self.strategy_engine = StrategyEngine(
            self.quote_queue, self.market_data, self.execution_queue, self.pos_mgr, self.active_pairs
        )

        self.execution_engine = ExecutionEngine(
            self.api, self.execution_queue, self.pos_mgr, self.market_data
        )

        self.account_monitor = AccountMonitor(self.api, self.pos_mgr)

        self.strategy_engine.start()
        self.execution_engine.start()
        self.account_monitor.start()

        # =========================
        # ✅ 修改點：訂閱不再 set() 亂序 + 自動裁切避免超過訂閱上限
        # =========================
        print("Subscribing quotes...")

        # 你可以調整：模擬環境常見上限 50（保守值；若你確定可更大可自行改）
        MAX_SUBS = 50

        # ✅ 保留 find_active_pairs 的排序結果
        pairs_ordered = list(self.active_pairs)

        # ✅ 每組需要 2 個訂閱（stock + future），超過就裁切 pair 數
        max_pairs_can_subscribe = max(1, MAX_SUBS // 2)
        if len(pairs_ordered) > max_pairs_can_subscribe:
            print(
                f"[⚠️] pairs={len(pairs_ordered)} -> will exceed MAX_SUBS={MAX_SUBS}. "
                f"Trim to top {max_pairs_can_subscribe} pairs."
            )
            pairs_ordered = pairs_ordered[:max_pairs_can_subscribe]

        subscribed_codes = set()
        contracts_to_snapshot = []

        # （可選）先嘗試把舊訂閱清掉（尤其你在同一個 process 重跑時會累積）
        def _unsubscribe_best_effort(contract, quote_type, version):
            try:
                self.api.quote.unsubscribe(contract, quote_type=quote_type, version=version)
            except:
                pass

        for s_code, f_code in pairs_ordered:
            try:
                contract_s = self.api.Contracts.Stocks[s_code]
                contract_f = self.api.Contracts.Futures[f_code]
                _unsubscribe_best_effort(contract_s, sj.constant.QuoteType.BidAsk, "v1")
                _unsubscribe_best_effort(contract_f, sj.constant.QuoteType.BidAsk, "v1")
            except:
                pass

        # ✅ 依排序順序訂閱
        for s_code, f_code in pairs_ordered:
            try:
                if s_code not in subscribed_codes:
                    contract_s = self.api.Contracts.Stocks[s_code]
                    self.api.quote.subscribe(contract_s, quote_type=sj.constant.QuoteType.BidAsk, version='v1')
                    subscribed_codes.add(s_code)
                    contracts_to_snapshot.append(contract_s)

                if f_code not in subscribed_codes:
                    contract_f = self.api.Contracts.Futures[f_code]
                    self.api.quote.subscribe(contract_f, quote_type=sj.constant.QuoteType.BidAsk, version='v1')
                    subscribed_codes.add(f_code)
                    contracts_to_snapshot.append(contract_f)

            except Exception as e:
                print(f"Error subscribing {s_code}/{f_code}: {e}")

        print(f"Subscribed instruments = {len(subscribed_codes)} (limit={MAX_SUBS})")

        # =========================
        # Snapshot 初始化（原本邏輯不改）
        # =========================
        print("Fetching initial snapshots...")
        try:
            chunk_size = 50
            for i in range(0, len(contracts_to_snapshot), chunk_size):
                chunk = contracts_to_snapshot[i:i+chunk_size]
                snapshots = self.api.snapshots(chunk)

                for snap in snapshots:
                    wrapped_quote = SnapshotWrapper(snap)

                    if wrapped_quote.code in self.strategy_engine.future_to_stock:
                        self.market_data.update_future(wrapped_quote)
                        print(f"  [Snapshot] Init Future {wrapped_quote.code}: Ask={wrapped_quote.ask_price[0]} Bid={wrapped_quote.bid_price[0]}")
                    elif wrapped_quote.code in self.strategy_engine.pair_map:
                        self.market_data.update_stock(wrapped_quote)
                        print(f"  [Snapshot] Init Stock {wrapped_quote.code}: Ask={wrapped_quote.ask_price[0]} Bid={wrapped_quote.bid_price[0]}")

                    self.quote_queue.put(wrapped_quote)

        except Exception as e:
            print(f"Initial snapshot fetch failed: {e}")

        try:
            while True:
                time.sleep(1)
        except KeyboardInterrupt:
            self.stop()

    def stop(self):
        print("Stopping System...")
        if hasattr(self, 'strategy_engine'):
            self.strategy_engine.stop()
            self.strategy_engine.join()
        if hasattr(self, 'execution_engine'):
            self.execution_engine.stop()
            self.execution_engine.join()
        if hasattr(self, 'account_monitor'):
            self.account_monitor.stop()
            self.account_monitor.join()
        self.api.logout()


if __name__ == "__main__":
    system = ArbitrageSystem()
    system.login()
    system.start()
