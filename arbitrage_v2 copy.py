import shioaji as sj
import datetime
import threading
import queue
import time
import pandas as pd
from typing import Dict, List, Tuple, Optional
from enum import Enum

from decimal import Decimal

import json
import os

# --- 全域設定 ---
SIMULATION = True      # 是否為模擬環境
STOCK_QTY = 2          # 現貨口數 (張)
FUTURE_QTY = 1         # 期貨口數 (口)
ENTRY_THRESHOLD = 5000 # 進場價差門檻 (金額)
EXIT_THRESHOLD = 0     # 出場價差門檻 (金額)
MIN_VOLUME_THRESHOLD = 1000 # 自動篩選：期貨日均量門檻 (口)
VOLUME_HISTORY_FILE = "volume_history.json" # 交易量記錄檔

# 簡易帳號設定
PERSON_ID = "S122352189"
PASSWORD = "baupas123"
CA_API_KEY = "CwXchQfoSPgUhKaRJDwv8LBJVyJwHcLWBwKazrJ23rdm"
CA_SECRET_KEY = "A934Lj5kRZRh2sWxLvP66qDGKk3qWdYmdcJhFJUYBEZ"
CA_PATH = "/Users/yptsai/Documents/certificate_key/Sinopac.pfx"
CA_PASSWORD = "S122352189"

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
    負責管理期貨成交量的歷史記錄，避免每日重複計算或受當日開盤初期成交量低影響。
    記錄檔格式: { "stock_code": { "date": "YYYY-MM-DD", "avg_vol": 1234 } }
    """
    def __init__(self, filename=VOLUME_HISTORY_FILE):
        self.filename = filename
        self.data = self._load()
        self.today_str = datetime.datetime.now().strftime('%Y-%m-%d')

    def _load(self):
        if os.path.exists(self.filename):
            try:
                with open(self.filename, 'r', encoding='utf-8') as f:
                    return json.load(f)
            except:
                return {}
        return {}

    def _save(self):
        with open(self.filename, 'w', encoding='utf-8') as f:
            json.dump(self.data, f, indent=2)

    def check_and_update_volume(self, stock_code: str, future_code: str, current_vol: int) -> int:
        """
        檢查並更新成交量記錄。
        回傳: 用於判斷的平均成交量 (avg_vol)
        邏輯:
        1. 如果今天已經更新過 -> 直接回傳記錄中的 avg_vol (跳過更新)
        2. 如果沒更新過:
           - 若是新合約(無記錄) -> avg_vol = current_vol (記錄昨天/今日快照)
           - 若有舊記錄 -> avg_vol = (舊 avg_vol + current_vol) / 2
           - 更新日期與數值
        """
        record = self.data.get(stock_code)
        
        # 1. 檢查今天是否已更新
        if record and record.get('date') == self.today_str:
            return record.get('avg_vol', 0)

        # 2. 計算新平均量
        if record:
            # 沿用之前的結果跟昨天的成交量做平均
            prev_avg = record.get('avg_vol', 0)
            new_avg = int((prev_avg + current_vol) / 2)
        else:
            # 第一次執行 (或新合約)，直接使用傳入的成交量
            new_avg = int(current_vol)

        # 3. 更新記錄
        self.data[stock_code] = {
            "date": self.today_str,
            "avg_vol": new_avg,
            "ref_future": future_code # 方便除錯
        }
        self._save()
        
        return new_avg

class PairDiscoverer:
    """
    負責自動發現流動性足夠的股票期貨配對
    """
    def __init__(self, api: sj.Shioaji):
        self.api = api
        self.vol_mgr = VolumeManager()

    def find_active_pairs(self) -> List[Tuple[str, str]]:
        print("正在掃描活躍的股票期貨配對 (這可能需要一點時間)...")
        active_pairs = []
        
        today = datetime.datetime.now()
        target_month_str = today.strftime('%Y%m') # e.g., "202601"
        
        candidates = []
        seen_contracts = set()

        # 1. 篩選符合日期的股票期貨合約
        print("開始遍歷期貨合約...")
        
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
                
                # 避免重複處理同一檔合約 (Shioaji SDK 有時會在不同分類下出現同一合約)
                if contract.code in seen_contracts:
                    continue
                seen_contracts.add(contract.code)

                is_futures = False
                if hasattr(sj.constant.SecurityType, 'Future'):
                    if contract.security_type == sj.constant.SecurityType.Future:
                        is_futures = True
                elif hasattr(sj.constant.SecurityType, 'FUT'):
                    if contract.security_type == sj.constant.SecurityType.FUT:
                        is_futures = True
                else:
                    if str(contract.security_type) in ['FUT', 'Future']:
                        is_futures = True

                if is_futures and \
                   hasattr(contract, 'underlying_code') and \
                   len(contract.underlying_code) == 4:
                    
                    # 只關心近月合約
                    if contract.delivery_month == target_month_str:
                        candidates.append(contract)

        print(f"找到 {len(candidates)} 檔近月股票期貨，開始檢查成交量 (使用 Snapshot 批量查詢)...")

        # 2. 改用 Snapshot 批量查詢成交量
        batch_size = 200
        for i in range(0, len(candidates), batch_size):
            batch = candidates[i : i + batch_size]
            try:
                snapshots = self.api.snapshots(batch)
                
                for snap in snapshots:
                    # 找出對應的 contract 物件
                    contract = next((c for c in batch if c.code == snap.code), None)
                    if contract:
                        stock_code = contract.underlying_code
                        future_code = contract.code
                        current_vol = snap.total_volume
                        
                        # 使用 VolumeManager 進行檢查與平均計算
                        # 這裡的 current_vol 視為 "昨天的成交量" (若是在盤後執行) 或是 "今天的量"
                        # 根據需求：第一次執行記錄成交量，之後做平均
                        avg_vol = self.vol_mgr.check_and_update_volume(stock_code, future_code, current_vol)

                        if avg_vol >= MIN_VOLUME_THRESHOLD:
                            print(f"  [加入監控] 現貨:{stock_code} 期貨:{future_code} (日均量:{avg_vol})")
                            active_pairs.append((stock_code, future_code))
            
            except Exception as e:
                print(f"Snapshot 查詢失敗: {e}")
                continue
            
            time.sleep(0.5)

        # 再次去重，確保回傳的列表沒有重複的配對
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
            self.api.login(api_key=CA_API_KEY, secret_key=CA_SECRET_KEY, contracts_cb=print)
            self.api.activate_ca(CA_PATH, CA_PASSWORD, PERSON_ID)
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

    def start(self):
        self._setup_callbacks()

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
        
        for s_code, f_code in unique_pairs:
            try:
                # 訂閱現貨
                if s_code not in subscribed_codes:
                    contract_s = self.api.Contracts.Stocks[s_code]
                    self.api.quote.subscribe(contract_s, quote_type=sj.constant.QuoteType.BidAsk, version='v1')
                    subscribed_codes.add(s_code)
                    # print(f"Subscribed Stock: {s_code}") # Debug
                
                # 訂閱期貨
                if f_code not in subscribed_codes:
                    contract_f = self.api.Contracts.Futures[f_code]
                    self.api.quote.subscribe(contract_f, quote_type=sj.constant.QuoteType.BidAsk, version='v1')
                    subscribed_codes.add(f_code)
                    # print(f"Subscribed Future: {f_code}") # Debug
                    
            except Exception as e:
                print(f"Error subscribing {s_code}/{f_code}: {e}")

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

