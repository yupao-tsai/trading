import shioaji as sj
import datetime
import threading
import queue
import time
import pandas as pd
from typing import Dict, List, Tuple, Optional
from enum import Enum
import concurrent.futures

from decimal import Decimal

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

# --- 狀態列舉 ---
class PositionState(Enum):
    EMPTY = 0           # 空手
    PENDING_ENTRY = 1   # 進場委託中 (已送單，未成交)
    HOLDING = 2         # 持倉中
    PENDING_EXIT = 3    # 出場委託中 (已送單，未成交)

class PositionManager:
    """
    執行緒安全的倉位狀態管理器。
    Strategy 讀取此狀態決定是否動作。
    Execution/Callback 更新此狀態。
    狀態以 stock_code 為 Key 進行隔離，確保不同組合互不干擾。
    """
    def __init__(self):
        self.lock = threading.Lock()
        # Key: Stock Code, Value: PositionState
        self._states: Dict[str, PositionState] = {} 
        # 用來記錄下單後的 Order ID 與 Stock Code 的對應，方便 Callback 查找
        self._order_map: Dict[str, str] = {} 

    def get_state(self, stock_code: str) -> PositionState:
        with self.lock:
            return self._states.get(stock_code, PositionState.EMPTY)

    def set_state(self, stock_code: str, state: PositionState):
        with self.lock:
            self._states[stock_code] = state
            print(f"[State Update] {stock_code} -> {state.name}")

    def register_pending_order(self, order_id: str, stock_code: str):
        with self.lock:
            self._order_map[order_id] = stock_code

    def get_code_by_order_id(self, order_id: str) -> Optional[str]:
        with self.lock:
            return self._order_map.get(order_id)

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
        # 1. 查詢期貨庫存
        # product_type="1" (Future), query_type="1" (Summary)
        try:
            # 嘗試使用 ShioajiAPIrevise.py 中 ThreadSafeShioajiAPI 的方法簽章
            # 或者直接呼叫 list_positions (因為 get_account_openposition 可能不存在於基本 Shioaji 物件)
            # 在基本的 Shioaji SDK 中，list_positions 也可用於期貨帳戶
            future_positions = self.api.list_positions(account=self.futopt_account)
        except Exception as e:
            # 備用方案：如果 list_positions 對期貨帳戶的行為不同，嘗試其他可能的方法
            # 但根據官方文件，list_positions 是通用的
            # 這裡保留原始錯誤訊息以便除錯，或者嘗試忽略
            # print(f"[Monitor Warning] Future position query failed: {e}")
            future_positions = [] # 假裝沒庫存，避免程式崩潰

        
        # 2. 查詢現貨庫存
        stock_positions = self.api.list_positions(self.api.stock_account)

        # 整理庫存數據
        # Key: Code, Value: Net Quantity (Buy +, Sell -)
        real_positions = {}

        # 處理期貨
        # 注意：標準 shioaji.list_positions 回傳的是 Position 物件列表
        # 而非 get_account_openposition 回傳的 Response 物件
        if future_positions:
            for pos in future_positions:
                code = pos.code
                vol = int(pos.quantity)
                direction = pos.direction # Action.Buy or Action.Sell
                sign = 1 if direction == sj.constant.Action.Buy else -1
                if code:
                    real_positions[code] = real_positions.get(code, 0) + (vol * sign)

        # 處理現貨
        for pos in stock_positions:
            code = pos.code
            vol = int(pos.quantity)
            direction = pos.direction # Action.Buy or Action.Sell
            sign = 1 if direction == sj.constant.Action.Buy else -1
            real_positions[code] = real_positions.get(code, 0) + (vol * sign)

        # 更新 PositionManager
        # 這裡的邏輯需要與 StrategyEngine 的期望一致
        # 假設我們主要關注"配對是否持倉中"
        
        # 目前 PositionManager 以 stock_code 為 key
        # 如果對應的 stock_code 有庫存 (且對應的 future 也有反向庫存)，則視為 HOLDING
        # 這裡做一個簡化的假設：只要有庫存就是 HOLDING，沒有就是 EMPTY
        # (忽略 PENDING 狀態，因為 PENDING 是短暫的，Monitor 週期較長)
        
        # 為了避免覆蓋正在 PENDING 的狀態，我們只在確定有變化時更新
        
        # TODO: 更精確的配對檢查需要知道哪個 stock 對應哪個 future
        pass 

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
        self.daily_quotes_map = {} # Cache for Daily Quotes
        self._fetch_daily_quotes_cache()

    def _fetch_daily_quotes_cache(self):
        """嘗試預先抓取 Daily Quotes 作為全市場成交量快取"""
        print("正在建立 Daily Quotes 快取 (作為 KBar 備案)...")
        try:
            # 嘗試抓今天
            dq = self.api.daily_quotes(date=datetime.date.today())
            if not dq or not dq.Code:
                # 嘗試抓昨天 (如果今天是剛開盤或假日)
                yesterday = datetime.date.today() - datetime.timedelta(days=1)
                dq = self.api.daily_quotes(date=yesterday)
            
            if dq and dq.Code:
                for i, code in enumerate(dq.Code):
                    # 建立 Code -> Volume 映射
                    # 注意: DailyQuotes 回傳的是全部股票，這裡只存成交量
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
            # 判斷傳入的是 contract 物件還是 stock_code 字串
            if isinstance(contract_or_code, str):
                target = self.api.Contracts.Stocks[contract_or_code]
            else:
                target = contract_or_code

            # 抓過去 25 天 (保險起見)
            end_date = today.strftime('%Y-%m-%d')
            start_date = (today - datetime.timedelta(days=25)).strftime('%Y-%m-%d')
            
            kbars = self.api.kbars(target, start=start_date, end=end_date)
            df = pd.DataFrame({**kbars})
            
            # [Debug] 強制印出 1303/2002 的狀態
            debug_codes = ['1303', '2002', '2330']
            t_code = getattr(target, 'code', '')
            if t_code in debug_codes:
                 print(f"  [Debug KBar] {t_code} Empty? {df.empty}")
                 if not df.empty:
                     print(f"  [Debug KBar] {t_code} First Row: {df.iloc[0].to_dict() if len(df)>0 else 'None'}")
                     print(f"  [Debug KBar] {t_code} Last Row: {df.iloc[-1].to_dict() if len(df)>0 else 'None'}")

            if df.empty: return []
            df['ts'] = pd.to_datetime(df['ts'])
            
            # 篩選完整交易日
            if is_closed:
                valid_df = df[df['ts'].dt.date <= today.date()]
            else:
                # 盤中排除今日
                valid_df = df[df['ts'].dt.date < today.date()]
            
            if valid_df.empty: return []
            
            # 取最後 n_days 筆
            recent_df = valid_df.tail(n_days)
            return recent_df['Volume'].astype(int).tolist()

        except Exception as e:
            # print(f"Fetch KBar Error: {e}")
            return []

    def _get_avg_volume_smart(self, code: str, contract_obj=None) -> int:
        """
        智慧型成交量取得 (現貨/期貨通用)：
        策略順序：
        1. Cache Check (Window Avg).
        2. Snapshot Check (Today's Volume) - "Streaming/Current" check.
        3. Historical Check (KBar 5 days) - "Historical" check.
        """
        # 0. 決定 Cache Key
        cache_key = code
        if contract_obj:
            stype = str(contract_obj.security_type)
            # 如果是期貨，改用 F_{Underlying} 作為 Key
            if stype in ['FUT', 'Future', str(sj.constant.SecurityType.Future)] and \
               hasattr(contract_obj, 'underlying_code'):
                cache_key = f"F_{contract_obj.underlying_code}"

        is_closed = self._is_day_market_closed()
        
        # 1. 嘗試從 Cache 拿平均 (使用 cache_key)
        # 如果是盤中且有 Cache，直接用 (加速)
        cached_avg = self.vol_mgr.get_5ma(cache_key, is_closed)
        if cached_avg > 0:
            return cached_avg

        target = contract_obj if contract_obj else self.api.Contracts.Stocks[code]
        
        # 2. Snapshot Check (優先取得當下最新狀態)
        # 這對應使用者的要求：Streaming (Conceptually Current) -> Snapshot
        # 如果 Snapshot 有量，代表今日有交易，視為有效
        snapshot_vol = 0
        try:
            snaps = self.api.snapshots([target])
            if snaps and len(snaps) > 0:
                # 取得今日總量
                snapshot_vol = int(snaps[0].total_volume)
                # Debug: 印出 snapshot 狀態
                # if code == '2330':
                #     print(f"  [Debug 2330] Snapshot Vol: {snapshot_vol}")
                
                # 如果今日量已經夠大 (例如 > 門檻的一半)，直接視為活躍，不用再抓 KBar (省時間)
                # 這裡設定比較寬鬆，只要今日有相當成交量就接受
                if snapshot_vol > (MIN_VOLUME_THRESHOLD * 0.5):
                    # 將 Snapshot 量也視為一個 sample 更新進 Cache
                    self.vol_mgr.update_window_init(cache_key, [snapshot_vol]) 
                    return snapshot_vol
        except Exception as e:
            # print(f"Snapshot failed: {e}")
            pass

        # 2.5 Daily Quotes Check (Fallback for Stocks)
        # 如果 Snapshot 失敗 (例如回傳 0)，嘗試查 Daily Quotes Cache
        if code in self.daily_quotes_map:
            dq_vol = self.daily_quotes_map[code]
            if dq_vol > MIN_VOLUME_THRESHOLD:
                # 視為有效
                self.vol_mgr.update_window_init(cache_key, [dq_vol])
                return dq_vol

        # 3. Historical Check (KBar)
        # 如果 Snapshot 沒資料 (e.g. 剛開盤) 或失敗，則查歷史
        vol_list = self._fetch_last_n_days_volumes(target, n_days=5)
        
        if len(vol_list) > 0:
            self.vol_mgr.update_window_init(cache_key, vol_list)
            
            # 計算平均 (排除 0)
            valid_vols = [v for v in vol_list if v > 0]
            if valid_vols:
                return int(sum(valid_vols) / len(valid_vols))
            return 0
        
        # 4. 如果所有即時/歷史都失敗，但有舊 Cache，沿用舊的 (Fallback)
        last_avg = self.vol_mgr.peek_avg(cache_key)
        if last_avg > 0:
            return last_avg
            
        # 5. 最後手段：如果 Snapshot 有任何值 (即使很小)，就用它
        if snapshot_vol > 0:
             return snapshot_vol
        
        return 0

    def find_active_pairs(self) -> List[Tuple[str, str]]:
        print("正在掃描活躍的股票期貨配對 (Smart Sliding Window)...")
        active_pairs = []
        
        today = datetime.datetime.now()
        target_month_str = today.strftime('%Y%m') 
        
        candidates = []
        seen_contracts = set()

        print("1. 篩選近月期貨合約...")
        for category in self.api.Contracts.Futures:
            try:
                iter(category)
            except TypeError: continue

            for contract in category:
                if isinstance(contract, tuple): contract = contract[1]
                if not hasattr(contract, 'security_type'): continue
                
                if contract.code in seen_contracts: continue
                seen_contracts.add(contract.code)

                is_futures = str(contract.security_type) in ['FUT', 'Future', str(sj.constant.SecurityType.Future)]

                if is_futures and \
                   hasattr(contract, 'underlying_code') and \
                   len(contract.underlying_code) == 4 and \
                   contract.delivery_month == target_month_str:
                        candidates.append(contract)

        total_candidates = len(candidates)
        print(f"找到 {total_candidates} 檔近月合約，開始檢查流動性...")
        print("提示：首次執行會建立快取，第二次執行將大幅加速。")

        count = 0
        for contract in candidates:
            stock_code = contract.underlying_code
            future_code = contract.code
            
            # 1. Check Stock Liquidity (Smart 5MA)
            stock_avg = self._get_avg_volume_smart(stock_code, contract_obj=None)
            
            # Debug: 印出部分資訊以供除錯
            if count < 5 or stock_avg > 0:
                print(f"  [Debug] {stock_code}: Stock 5MA = {stock_avg}")

            if stock_avg < MIN_VOLUME_THRESHOLD:
                count += 1
                continue

            # 2. Check Future Liquidity (Smart 5MA)
            future_avg = self._get_avg_volume_smart(future_code, contract_obj=contract)
            
            # Debug
            if future_avg > 0:
                 print(f"  [Debug] {future_code}: Future 5MA = {future_avg}")

            if future_avg >= MIN_VOLUME_THRESHOLD:
                print(f"  [加入監控] {stock_code}/{future_code} | 期貨5MA:{future_avg} | 現貨5MA:{stock_avg}")
                active_pairs.append((stock_code, future_code))
            
            count += 1
            if count % 10 == 0:
                print(f"  進度: {count}/{total_candidates}...", end='\r')
                time.sleep(0.01)

        print(f"\n篩選完成，共 {len(active_pairs)} 組符合條件。")
        return list(set(active_pairs))



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
    def __init__(self, 
                 quote_queue: queue.Queue, 
                 market_data: MarketData, 
                 execution_queue: queue.Queue,
                 position_manager: PositionManager,
                 pairs: List[Tuple[str, str]]):
        super().__init__()
        self.quote_queue = quote_queue
        self.market_data = market_data
        self.execution_queue = execution_queue
        self.pos_mgr = position_manager
        self.pairs = pairs 
        self.running = True
        
        # 優化：建立快速查找表 (Lookup Table)
        # 支援一檔股票對應多檔期貨 (One-to-Many)
        self.pair_map: Dict[str, List[str]] = {} 
        self.future_to_stock: Dict[str, str] = {}

        for s_code, f_code in self.pairs:
            if s_code not in self.pair_map:
                self.pair_map[s_code] = []
            self.pair_map[s_code].append(f_code)
            
            self.future_to_stock[f_code] = s_code

    def calculate_cost(self, stock_price, future_price):
        stock_fee = (stock_price * 1000 * STOCK_QTY) * 0.001425 * 0.3
        future_fee = 20 * FUTURE_QTY 
        tax_stock = (stock_price * 1000 * STOCK_QTY) * 0.003
        tax_future = (future_price * 2000 * FUTURE_QTY) * 0.00002
        return stock_fee + future_fee + tax_stock + tax_future

    def run(self):
        print(">>> Strategy Engine Started (Event-Driven Mode)")
        while self.running:
            try:
                # 取得最新的報價 (可能是股票或期貨)
                quote = self.quote_queue.get(timeout=1)
                
                # 判斷是股票還是期貨報價，找出對應的配對
                target_futures = []
                target_stock = None

                if quote.code in self.pair_map:
                    # 收到股票報價 -> 檢查所有對應的期貨
                    target_stock = quote.code
                    target_futures = self.pair_map[quote.code]
                elif quote.code in self.future_to_stock:
                    # 收到期貨報價 -> 檢查對應的現貨
                    target_stock = self.future_to_stock[quote.code]
                    target_futures = [quote.code]
                else:
                    continue
                
                stock_code = target_stock
                
                # 2. 檢查該特定股票的狀態
                state = self.pos_mgr.get_state(stock_code)
                if state in [PositionState.PENDING_ENTRY, PositionState.PENDING_EXIT]:
                    continue

                # 3. 針對每一組期貨進行計算
                for future_code in target_futures:
                    s_quote, f_quote = self.market_data.get_quotes(stock_code, future_code)
                    if not s_quote or not f_quote: continue
                    
                    # --- 進場邏輯 (EMPTY -> PENDING_ENTRY) ---
                    if state == PositionState.EMPTY:
                        stock_ask = float(s_quote.ask_price[0])
                        future_bid = float(f_quote.bid_price[0])
                        if stock_ask == 0 or future_bid == 0: continue

                        spread_value = (future_bid * 2000 * FUTURE_QTY) - (stock_ask * 1000 * STOCK_QTY)
                        cost = self.calculate_cost(stock_ask, future_bid)
                        net_profit = spread_value - cost

                        if net_profit > ENTRY_THRESHOLD:
                            print(f"[訊號-開倉] {stock_code}/{future_code} | 預期淨利: {net_profit:.0f}")
                            self.pos_mgr.set_state(stock_code, PositionState.PENDING_ENTRY)
                            signal = TradeSignal('OPEN', stock_code, future_code, stock_ask, future_bid)
                            self.execution_queue.put(signal)
                            break # 同一時間同一股票只做一組

                    # --- 出場邏輯 (HOLDING -> PENDING_EXIT) ---
                    elif state == PositionState.HOLDING:
                        # 注意：這裡假設 Holding 的是目前遍歷到的這個組合
                        # 在更複雜的系統中，應該要記錄當初是跟哪個期貨開倉的
                        # 這裡簡化為：只要有任何一組配對達到出場條件就出場
                        stock_bid = float(s_quote.bid_price[0])
                        future_ask = float(f_quote.ask_price[0])
                        if stock_bid == 0 or future_ask == 0: continue

                        current_spread = (future_ask * 2000 * FUTURE_QTY) - (stock_bid * 1000 * STOCK_QTY)
                        
                        if current_spread <= EXIT_THRESHOLD:
                            print(f"[訊號-平倉] {stock_code}/{future_code} | 收斂價差: {current_spread:.0f}")
                            self.pos_mgr.set_state(stock_code, PositionState.PENDING_EXIT)
                            signal = TradeSignal('CLOSE', stock_code, future_code, stock_bid, future_ask)
                            self.execution_queue.put(signal)
                            break

            except queue.Empty:
                continue
            except Exception as e:
                print(f"Strategy Error: {e}")

    def stop(self):
        self.running = False

class ExecutionEngine(threading.Thread):
    def __init__(self, api: sj.Shioaji, execution_queue: queue.Queue, position_manager: PositionManager):
        super().__init__()
        self.api = api
        self.execution_queue = execution_queue
        self.pos_mgr = position_manager
        self.running = True

    def place_orders(self, signal: TradeSignal):
        # 1. 現貨訂單
        s_action = sj.constant.Action.Buy if signal.action_type == 'OPEN' else sj.constant.Action.Sell
        contract_s = self.api.Contracts.Stocks[signal.stock_code]
        
        # 兼容性檢查：OrderType
        # 優先嘗試新的 OrderType，其次嘗試舊的 TFTOrderType，最後使用字串
        if hasattr(sj.constant, 'OrderType'):
            order_type_s = sj.constant.OrderType.ROD
        elif hasattr(sj.constant, 'TFTOrderType'):
            order_type_s = sj.constant.TFTOrderType.ROD
        else:
            order_type_s = "ROD"
            
        # 兼容性檢查：StockOrderLot
        if hasattr(sj.constant, 'StockOrderLot'):
            order_lot_s = sj.constant.StockOrderLot.Common
        elif hasattr(sj.constant, 'TFTStockOrderLot'):
            order_lot_s = sj.constant.TFTStockOrderLot.Common
        else:
            order_lot_s = "Common"

        order_s = self.api.Order(
            price=signal.stock_price,
            quantity=STOCK_QTY,
            action=s_action,
            price_type=sj.constant.StockPriceType.LMT,
            order_type=order_type_s, 
            order_lot=order_lot_s,
            account=self.api.stock_account
        )
        try:
            print(f"  [Debug] Placing Stock Order: {contract_s.code} {s_action} {STOCK_QTY} @ {signal.stock_price}")
            trade_s = self.api.place_order(contract_s, order_s)
            self.pos_mgr.register_pending_order(trade_s.order.id, signal.stock_code)
            print(f"  -> Stock Order Sent: {trade_s.order.id}")
        except Exception as e:
            print(f"  [Error] Stock Order Failed: {e}")
            return # Stop if stock order fails

        # 2. 期貨訂單
        f_action = sj.constant.Action.Sell if signal.action_type == 'OPEN' else sj.constant.Action.Buy
        
        # 兼容性檢查：FuturesOCType
        octype = "New" 
        if hasattr(sj.constant, 'FuturesOCType'):
             octype = sj.constant.FuturesOCType.New if signal.action_type == 'OPEN' else sj.constant.FuturesOCType.Cover
        
        contract_f = self.api.Contracts.Futures[signal.future_code]
        
        # 兼容性檢查：FuturesPriceType
        price_type_f = sj.constant.StockPriceType.LMT # Default fallback
        if hasattr(sj.constant, 'FuturesPriceType'):
             price_type_f = sj.constant.FuturesPriceType.LMT
             
        # OrderType for Futures
        if hasattr(sj.constant, 'OrderType'):
            order_type_f = sj.constant.OrderType.ROD
        elif hasattr(sj.constant, 'FuturesOrderType'):
            order_type_f = sj.constant.FuturesOrderType.ROD
        else:
            order_type_f = "ROD"

        order_f = self.api.Order(
            action=f_action,
            price=signal.future_price,
            quantity=FUTURE_QTY,
            price_type=price_type_f,
            order_type=order_type_f,
            octype=octype,
            account=self.api.futopt_account
        )
        try:
            trade_f = self.api.place_order(contract_f, order_f)
            print(f"  -> Future Order Sent: {trade_f.order.id}")
        except Exception as e:
            print(f"  [Error] Future Order Failed: {e}")

    def run(self):
        print(">>> Execution Engine Started")
        while self.running:
            try:
                signal = self.execution_queue.get(timeout=1)
                self.place_orders(signal)
            except queue.Empty:
                continue
            except Exception as e:
                print(f"Execution Error: {e}")

    def stop(self):
        self.running = False

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
                self.api.login(api_key=CA_API_KEY, secret_key=CA_SECRET_KEY, contracts_cb=lambda security_type: print(f"{repr(security_type)} fetch done."), subscribe_trade=True)
                print("API Key 登入成功，正在啟動 CA 憑證...")
                self.api.activate_ca(ca_path=CA_PATH, ca_passwd=CA_PASSWORD, person_id=PERSON_ID)
                print("CA 憑證啟動成功！")
            except Exception as e:
                print(f"正式環境登入或 CA 啟動失敗: {e}")
                raise e
        else:
            # 根據官方文件，新版 Shioaji 的模擬環境也需要使用 API Key 登入
            print("正在嘗試登入模擬環境...")
            try:
                self.api.login(api_key=CA_API_KEY, secret_key=CA_SECRET_KEY, contracts_cb=print)
                # 即使是模擬環境，部分操作仍可能需要 CA 簽章，嘗試載入 CA
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
            
            if not order_id: return

            stock_code = self.pos_mgr.get_code_by_order_id(order_id)
            if not stock_code: return 

            current_pos_state = self.pos_mgr.get_state(stock_code)

            if status == sj.constant.Status.Filled:
                print(f"[成交回報] Order {order_id} Filled!")
                if current_pos_state == PositionState.PENDING_ENTRY:
                    self.pos_mgr.set_state(stock_code, PositionState.HOLDING)
                elif current_pos_state == PositionState.PENDING_EXIT:
                    self.pos_mgr.set_state(stock_code, PositionState.EMPTY)
            
            elif status in [sj.constant.Status.Cancelled, sj.constant.Status.Failed]:
                print(f"[下單失敗] Order {order_id} {status}")
                if current_pos_state == PositionState.PENDING_ENTRY:
                    self.pos_mgr.set_state(stock_code, PositionState.EMPTY)
                elif current_pos_state == PositionState.PENDING_EXIT:
                    self.pos_mgr.set_state(stock_code, PositionState.HOLDING)
                    
        except Exception as e:
            print(f"Callback Error: {e}")

    def _test_kbar_connectivity(self):
        print("\n--- [診斷] KBar 連線測試 (Target: 2330 台積電) ---")
        try:
            target = self.api.Contracts.Stocks['2330']
            print(f"合約物件檢查: {target}")
            print(f"交易所: {target.exchange}, 類別: {target.security_type}")
            
            # Test 1: KBar
            today = datetime.datetime.now()
            start = (today - datetime.timedelta(days=10)).strftime('%Y-%m-%d')
            end = today.strftime('%Y-%m-%d')
            print(f"1. 嘗試抓取 KBar: {start} 到 {end}")
            kbars = self.api.kbars(target, start=start, end=end)
            df = pd.DataFrame({**kbars})
            print(f"   -> KBar 回傳資料是否為空: {df.empty}")

            # Test 2: Ticks
            today_str = datetime.datetime.now().strftime('%Y-%m-%d')
            print(f"2. 嘗試抓取今日 Ticks ({today_str})...")
            ticks = self.api.ticks(contract=target, date=today_str)
            df_ticks = pd.DataFrame({**ticks})
            print(f"   -> Ticks 回傳資料是否為空: {df_ticks.empty}")

            # Test 3: Snapshot
            print(f"3. 嘗試抓取 Snapshot...")
            snaps = self.api.snapshots([target])
            if snaps:
                print(f"   -> Snapshot 回傳成功: TotalVol={snaps[0].total_volume} Price={snaps[0].close}")
            else:
                print(f"   -> Snapshot 回傳為空!")

            # Test 4: Daily Quotes
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
        
        # 執行連線診斷
        self._test_kbar_connectivity()

        discoverer = PairDiscoverer(self.api)
        self.active_pairs = discoverer.find_active_pairs()
        
        if not self.active_pairs:
            print("未發現符合成交量條件的配對，程式結束。")
            return

        self.strategy_engine = StrategyEngine(
            self.quote_queue, self.market_data, self.execution_queue, self.pos_mgr, self.active_pairs
        )
        self.execution_engine = ExecutionEngine(
            self.api, self.execution_queue, self.pos_mgr
        )
        self.account_monitor = AccountMonitor(self.api, self.pos_mgr)
        
        self.strategy_engine.start()
        self.execution_engine.start()
        self.account_monitor.start()

        print("Subscribing quotes...")
        subscribed_codes = set()
        
        # 再次過濾重複，確保 active_pairs 本身不包含重複項目 (PairDiscoverer 已做，但雙重保險)
        unique_pairs = list(set(self.active_pairs))
        
        contracts_to_snapshot = []

        for s_code, f_code in unique_pairs:
            try:
                # 訂閱現貨
                if s_code not in subscribed_codes:
                    contract_s = self.api.Contracts.Stocks[s_code]
                    self.api.quote.subscribe(contract_s, quote_type=sj.constant.QuoteType.BidAsk, version='v1')
                    subscribed_codes.add(s_code)
                    contracts_to_snapshot.append(contract_s)
                    # print(f"Subscribed Stock: {s_code}") # Debug
                
                # 訂閱期貨
                if f_code not in subscribed_codes:
                    contract_f = self.api.Contracts.Futures[f_code]
                    self.api.quote.subscribe(contract_f, quote_type=sj.constant.QuoteType.BidAsk, version='v1')
                    subscribed_codes.add(f_code)
                    contracts_to_snapshot.append(contract_f)
                    # print(f"Subscribed Future: {f_code}") # Debug
                    
            except Exception as e:
                print(f"Error subscribing {s_code}/{f_code}: {e}")

        # --- Initial Data Fetch (Snapshot Fallback) ---
        # 如果 Streaming 資料還沒進來，先抓一次 Snapshot 填補初始狀態
        print("Fetching initial snapshots...")
        try:
            # 分批抓取以免一次過多
            chunk_size = 50
            for i in range(0, len(contracts_to_snapshot), chunk_size):
                chunk = contracts_to_snapshot[i:i+chunk_size]
                snapshots = self.api.snapshots(chunk)
                
                for snap in snapshots:
                    # 包裝成與 BidAsk 兼容的物件
                    wrapped_quote = SnapshotWrapper(snap)
                    
                    # 判斷是現貨還是期貨 (簡單用 code 長度或是否在 pair_map 判斷)
                    # 這裡我們用 loose check: 如果在 future_to_stock map 中是期貨，否則當現貨 (假設)
                    # 更嚴謹的方式是用 contract object 比對，但 snapshot 只回傳 code
                    
                    if wrapped_quote.code in self.strategy_engine.future_to_stock:
                        # 視為期貨
                        self.market_data.update_future(wrapped_quote)
                        print(f"  [Snapshot] Init Future {wrapped_quote.code}: Ask={wrapped_quote.ask_price[0]} Bid={wrapped_quote.bid_price[0]}")
                    elif wrapped_quote.code in self.strategy_engine.pair_map:
                        # 視為現貨
                        self.market_data.update_stock(wrapped_quote)
                        print(f"  [Snapshot] Init Stock {wrapped_quote.code}: Ask={wrapped_quote.ask_price[0]} Bid={wrapped_quote.bid_price[0]}")
                    
                    # 同時推送到 queue 讓策略引擎有機會立即評估 (如果 Snapshot 價差已經達標)
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
