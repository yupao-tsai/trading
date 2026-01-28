# Source Code Interpretation: arbitrage_vb06.py

這份文件是對 `arbitrage_vb06.py` 原始碼的深度解讀，幫助你理解各個模組的功能與互動方式。

## 1. 核心架構 (System Architecture)

程式碼結構採用 **Layered Architecture (分層架構)**，由下而上分別是：

1.  **Driver Layer**: `ExecutionEngine` (負責與 Shioaji API 溝通)
2.  **Data Layer**: `MarketData` (負責維護行情報價快照)
3.  **Strategy Layer**: `StrategyEngine` (純邏輯運算，計算價差與訊號，包含配對邏輯)
4.  **Orchestration Layer**: `TransactionManager` (總指揮，協調資金、策略、交易與回報，包含非同步報價處理)
5.  **State Layer**: `BaseTransaction` 及其子類 (負責單筆交易的生命週期管理)
6.  **Presentation Layer**: `ArbitrageDashboard` (GUI) / `AccountReporter` (Console)
7.  **Discovery Layer**: `PairDiscovery` (負責系統啟動時的掃描與配對)

---

## 2. 重要類別詳解 (Key Classes)

### A. SystemConfig (設定檔)
*   **用途**: 集中管理所有靜態常數 (Timeout 秒數、冷卻時間、Lot Size 等)。
*   **關鍵參數**: 
    *   `ZOMBIE_TIMEOUT`: 交易卡死多久後強制清除。
    *   `REPAIR_COOLDOWN`: 觸發修復後要冷靜多久。

### B. MarketData (行情資料庫)
*   **用途**: Thread-safe 的行情快照儲存。
*   **特點**:
    *   **Locking**: 寫入時用 `_lock` 保護，確保 snapshot 不會讀到一半的數據。
    *   **Optimization (優化)**: `get_stock` 在讀取時**不鎖** (No Lock)，這是為了讓 GUI 繪圖極快，犧牲一點點微觀的一致性換取效能。
    *   **Size Gate (新功能)**: 在 `update` 時，會檢查掛單量是否「跨越門檻」。只有當流動性從不足變充足時，才觸發策略。

### C. ExecutionEngine (執行引擎)
*   **用途**: 包裝 Shioaji API，處理登入、憑證、帳號選取。
*   **關鍵**:
    *   `_order_cb`: API 的回調入口。這裡**禁止執行複雜邏輯**，只負責把資料打包，然後透過 Callback 傳給上層。
    *   `_lock`: 保護內部的 `_order_cache`，確保 Polling 和 Callback 不會打架。

### D. TransactionManager (總指揮)
*   **用途**: 系統的心臟。
*   **關鍵成員**:
    *   `event_queue`: **[新!]** Producer-Consumer 模式核心。
    *   `_worker_thread`: 背景執行緒，消化事件並更新狀態。
    *   `active_transactions`: 存放進行中的交易。
    *   `ledger`: 資金庫 (`PortfolioLedger`)。
    *   `reporter`: 負責產生報告文字 (`AccountReporter`)。
*   **關鍵流程**:
    *   `run_step()`: Main Loop 每秒呼叫。
    *   `run_step()`: Main Loop 每秒呼叫。
    *   `_on_market_tick()`: **[優化!]** 僅做為 Producer，將報價放入 Buffer。
    *   `_quote_worker_loop()`: **[新!]** Consumer Thread，負責從 Buffer 取出報價並進行「合併 (Coalescing)」運算，解決快市延遲問題。

### E. PortfolioLedger (資金/庫存帳本)
*   **用途**: 本地端的資金管理 (Capital Management)。
*   **功能**:
    *   `cash_total`: 總現金 (來自 API Sync + 本地估算)。
    *   `reserved_stock`/`reserved_margin`: 圈存資金 (交易發出但未成交)。
    *   `est_holding_cost()`: 計算目前部位佔用的資金與保證金。

### F. AccountReporter / ArbitrageDashboard (報告與介面)
*   **AccountReporter**: 簡單的 Text Formatter，負責把部位狀態轉成字串供 Console 或 Log 使用。
*   **ArbitrageDashboard**: (Tkinter) 負責圖形化介面。
    *   **Threading**: 運行在 Main Thread。業務邏輯 (`TransactionManager`) 則被移到 Background Thread 避免卡住介面。

### G. PairDiscovery (配對掃描器)
*   **用途**: 負責在系統啟動時掃描並建立監控清單 (`monitored_pairs`)。
*   **流程**:
    1.  **Sync**: 取得現有庫存 (優先監控)。
    2.  **Filter**: 篩選出成交易量大於門檻 (`MIN_STOCK_VOL`/`MIN_FUTURE_VOL`) 的標的。
    3.  **Rank**: 依照「股票量+期權量」總分排序。
    4.  **Select**: 選取前 N 名 (Top 20) 回傳給 Manager 進行訂閱。

### H. InstrumentRegistry (商品註冊表) - **[New!]**
*   **用途**: 集中管理所有商品的合約規格與單位換算，確保全系統單位統一。
*   **關鍵變數**:
    *   `_specs`: 字典結構 `{code: {'type': 'Stock'|'Future', 'multiplier': int}}`。
        *   **Stock**: `multiplier` 通常為 1000 (股)。
        *   **Future**: `multiplier` 為合約乘數 (例如台指期 200, 個股期 2000)。
*   **功能**:
    *   `get_ratio(stock, future)`: 動態計算避險比例。例如 `Fut(2000) / Stk(1000) = 2.0`。
    *   確保系統能支援各種不同規格的商品 (如不同倍數的個股期)，而不需要硬編碼。
---

## 3. GUI 架構與互動設計 (GUI Architecture) - **New!**
本系統包含一個基於 `tkinter` 的圖形化介面 (`gui_dashboard.py`)，提供即時監控與控制功能。

### A. 介面佈局 (Layout)
採用 `Dark Theme` 風格，視窗分割為三個主要區域：

1.  **控制列 (Top Control Bar)**
    *   **Status Label**: 顯示系統狀態 (INIT / RUNNING / STOPPED)。
    *   **Fund Label**: **[New!]** 即時顯示現金水位 (`Cash`) 與預估佔用保證金 (`Est.Cost`)。
    *   **Buttons**:
        *   `SYNC POS`: 強制與券商伺服器同步部位 (開新執行緒執行，不卡頓 UI)。
        *   `FORCE REPAIR`: 手動觸發 `trigger_repair`，強制補足缺腳。
        *   `STOP SYSTEM`: 安全停止所有執行緒並關閉視窗。

2.  **監控面板 (Monitored Pairs - Top Pane)**
    *   **元件**: `Treeview` 表格。
    *   **內容**: 顯示所有訂閱的「股票-期貨」對。
    *   **關鍵欄位**:
        *   `Spread` (價差): `Bid_Fut - Ask_Stk`。
        *   `Z-Score`: 動態統計指標，判斷是否過熱。
        *   `Signal`: 顯示當前策略訊號 (Active / Wait)。

3.  **狀態面板 (Status Pane - Bottom Split)**
    *   **左側 (Holdings)**: 當前持倉部位 (`PortfolioLedger`)。
    *   **右側 (Orders)**: 最近 25 筆交易與委託單狀態 (`Active/Completed Transactions`)。

### B. 更新機制 (Update Loop)
為了保持介面流暢度 (Responsiveness)，GUI 採用 **Polling (輪詢)** 機制，與核心邏輯分離。

*   **Main Thread (GUI)**:
    *   `update_loop()`: 每 **50ms** 觸發一次。
    *   `_refresh_ui()`: 每 **500ms** (0.5s) 執行一次重繪。
    *   **Read-Only**: 從 `TransactionManager` 讀取 `market_data`, `active_transactions`, `ledger` 等共用物件。因為 Python 的 GIL 與 `Dict` 原子性，讀取通常無需重鎖 (或僅需極短的鎖)，確保 GUI 不會因為策略運算卡住。

*   **Background Thread (Logic)**:
    *   負責 API 交互、策略運算、下單。
    *   GUI 按鈕 (如 Sync) 若涉及 API 呼叫，會啟動 **第三個臨時執行緒** 來執行，避免按鈕按下後視窗凍結。

### C. 介面示意圖 (UI Wireframe)
由於無法直接截圖，以下為介面佈局示意：

```text
+-----------------------------------------------------------------------+
| [⚡ Arbitrage Bot] Status: RUNNING   Cash: 1,500,000 | Est.Cost: 450,000 |
|                                       [SYNC POS] [FORCE REPAIR] [STOP] |
+-----------------------------------------------------------------------+
| Monitored Pairs (Treeview)                                           |
| +-------+--------+-------+-------+--------+-------+--------+--------+ |
| | Stock | Future | Spread| Z-Scr | Signal | S.Vol | F.Vol  | ...    | |
| +-------+--------+-------+-------+--------+-------+--------+--------+ |
| | 2330  | CDF    |  0.5  |  0.1  | WAIT   | 500   | 1200   | ...    | |
| | 2317  | CHN    | -0.2  | -1.5  | OPEN   | 8000  | 500    | ...    | |
| | ...   | ...    | ...   | ...   | ...    | ...   | ...    | ...    | |
| +-------+--------+-------+-------+--------+-------+--------+--------+ |
+-----------------------------------------------------------------------+
| [Current Holdings]                 | [Active Orders]                  |
| Code | Prod | Qty | AvgPrice       | TxID | State | Stock | Future    |
| 2330 | Stock| 2000| 540.0          | 101  | CLOSING| Filled| Filled   |
| CDF  | Fut  | -1  | 540.5          | 102  | OPEN   | New   | New      |
+-----------------------------------------------------------------------+
| System Ready.                                                10:30:45 |
+-----------------------------------------------------------------------+
```

### D. 設計決策：為何選擇 Polling 而非 Event-Driven？
使用者常問：「為什麼不直接在 API 回調 (Callback) 中更新 UI？」

1.  **頻率不匹配 (Frequency Mismatch)**：
    *   **Market Data**: 高頻交易時，Tick 可能每秒湧入數百筆。
    *   **Human Eye**: 人眼僅需 24~60Hz (約 16ms~50ms) 的更新率。
    *   **UI Framework**: `tkinter` (及大多數 GUI) 的重繪成本昂貴。若每一筆 Tick 都觸發重繪，主執行緒 (Main Thread) 會瞬間被 GUI 繪製工作卡死，導致整個程式凍結 (Freezing)。

2.  **執行緒安全 (Thread Safety)**：
    *   API 回調通常發生在 **非主執行緒** (API Thread)。
    *   `tkinter` 只能在 **主執行緒** 操作。若直接在 Callback 更新 UI，會引發 `RuntimeError` 或不可預期的崩潰。
    *   雖然可以用 `queue` 轉發事件，但若生產速度 (Tick) 遠大於消費速度 (Draw)，Queue 會迅速爆滿 (Backpressure)，造成記憶體溢出或嚴重延遲。

3.  **解決方案 (Our Solution)**：
    *   **Logic Thread**: 全速處理 Tick，更新記憶體中的 `latest_state` (使用 Lock 保護)。
    *   **GUI Thread**: 使用 `polling (50ms)` 或是更慢的頻率，去「採樣 (Sample)」當前的 `latest_state` 並顯示。
    *   這樣做既保留了策略的高頻運算能力，又保證了介面的流暢與穩定。

---

## 4. 類別關係圖 (Class Diagram)

```mermaid
classDiagram
    class TransactionManager {
        +event_queue: Queue
        +_pending_quote_codes: Set
        +_quote_event: Event
        +active_transactions: Dict
        +run_step()
        +_on_market_tick()
        +_quote_worker_loop()
        +on_execution_event()
        +start()
    }

    class ExecutionEngine {
        +api: Shioaji
        +login()
        +place_order()
        +cancel_order()
        +check_connection()
    }

    class MarketData {
        +_stk: Dict[MarketSnapshot]
        +_fut: Dict[MarketSnapshot]
        +update_stock()
        +update_future()
        +get_stock()
        +get_future()
    }

    class StrategyEngine {
        +_lock: Lock
        +on_tick(stock, future) -> TradeIntent
        +calc_spread()
    }

    class PortfolioLedger {
        +positions: Dict
        +cash_total: Float
        +reserved_stock: Float
        +reserved_margin: Float
        +check_funds(cash, margin) -> bool
        +reserve_funds(cash, margin)
        +release_reserved(cash, margin)
        +est_holding_cost()
        +upsert()
    }

    class AccountReporter {
        +summary_text()
    }

    class ArbitrageDashboard {
        +run()
        +update_loop()
    }

    class BaseTransaction {
        <<Abstract>>
        +state: TransactionState
        +stock_order: OrderStub
        +future_order: OrderStub
        +update()
        +_timeout_and_hedge()
    }

    class SimultaneousOpenTransaction {
        +_step_open_simultaneous_submit()
    }

    class SimultaneousCloseTransaction {
        +_step_close_simultaneous_submit()
    }
    
    class PairDiscovery {
        +scan() -> List
        +_sync_positions()
    }

    TransactionManager *-- ExecutionEngine : 擁有 (Has-a)
    TransactionManager *-- MarketData : 擁有 (Has-a)
    TransactionManager *-- StrategyEngine : 擁有 (Has-a)
    TransactionManager *-- PortfolioLedger : 擁有 (Has-a)
    TransactionManager *-- AccountReporter : 擁有 (Has-a)
    TransactionManager "1" *-- "*" BaseTransaction : 管理 (Manages)

    StrategyEngine --> MarketData : 讀取 (Reads)
    
    BaseTransaction <|-- SimultaneousOpenTransaction : 繼承 (Inherits)
    BaseTransaction <|-- SimultaneousCloseTransaction : 繼承 (Inherits)
    
    BaseTransaction --> TransactionManager : 呼叫 (Callbacks/Access)
    
    TransactionManager ..> PairDiscovery : 使用 (Uses Temporary)
    ArbitrageDashboard o-- TransactionManager : 觀察 (Observes)
```

---

## 5. 關鍵調用流程 (Key Call Flows)

### A. 系統啟動與註冊流程 (System Startup & Registration) - **Deep Dive**
這一節描述了系統從 `main()` 入口到完全運作的每一步，特別是 **Worker Queue** 與 **Background Logic** 的初始化。

```mermaid
sequenceDiagram
    participant Main as Main.py
    participant TM as TransManager
    participant QW as QuoteWorker(Thread)
    participant EW as EventWorker(Thread)
    participant Exec as ExecutionEngine
    participant MD as MarketData
    participant API as ShioajiAPI
    
    Note over Main: 1. Core Initialization
    Main->>TM: TransactionManager()
    TM->>TM: Init Locks (_lock, _quote_lock)
    TM->>TM: Init Queues (event_queue, _pending_quote_codes)
    
    TM->>EW: Start EventWorker Thread
    Note right of EW: Waiting for Order Events...
    
    TM->>QW: Start QuoteWorker Thread
    Note right of QW: Waiting for Tick Signals...
    
    TM->>Exec: ExecutionEngine()
    TM->>MD: MarketData()
    
    Note over Main: 2. Login & Connection
    Main->>TM: start()
    TM->>Exec: login()
    Exec->>API: shioaji.Shioaji(simulation=True)
    Exec->>API: login(...) & activate_ca(...)
    Exec->>API: set_order_callback(self._order_cb)
    
    Note over Main: 3. Quote Registration
    Main->>TM: start()
    TM->>API: set_on_tick_stk_v1_callback(_on_tick_stk)
    TM->>API: set_on_tick_fop_v1_callback(_on_tick_fop)
    TM->>API: set_on_bidask_stk_v1_callback(_on_quote_stk)
    TM->>API: set_on_bidask_fop_v1_callback(_on_quote_fop)
    TM->>TM: sync_held_positions()
    
    Note over Main: 4. Pair Discovery (Blocking)
    Main->>TM: discover_and_subscribe()
    TM->>API: List Positions (Sync Held)
    TM->>API: Scan Contracts & Volumes
    TM->>API: quote.subscribe(Top N Pairs)
    
    Note over Main: 4. GUI & Logic Loop
    Main->>Main: Launch GUI (Dashboard)
    
    rect rgb(240, 255, 240)
        Note right of Main: Background Logic Thread
        Main->>Main: Start Thread(background_logic)
        loop Every 0.1s
            Main->>TM: run_step()
            TM->>TM: Check Timeouts
            TM->>TM: Sync Balance (Every 2s/30s)
            TM->>TM: Connection Watchdog
        end
    end
    
    Main->>Main: GUI.run() (Main Thread)
```

### B. 配對掃描與訂閱流程 (Pair Discovery)
`discover_and_subscribe` 的內部細節。

```mermaid
sequenceDiagram
    participant TM as TransManager
    participant Disc as PairDiscovery
    participant API as Shioaji
    
    TM->>Disc: scan(limit=20)
    
    Disc->>Disc: Sync Held Positions (Risk First)
    Disc->>API: Fetch Contracts (Stock/Fut/Opt)
    
    Disc->>API: Fetch Snapshots (Volume)
    Disc->>Disc: Filter by MIN_VOL (500/2000)
    Disc->>Disc: Rank by Liquidity Score
    
    Disc-->>TM: List[ (Stock, Future) ]
    
    TM->>TM: _build_pair_map()
    TM->>API: subscribe(Top 20 Pairs)
```

### C. 資金控管流程 (Capital Control)
`PortfolioLedger` 的生命週期：Check -> Reserve -> Realize。

```mermaid
sequenceDiagram
    participant Strat as Strategy
    participant TM as TransManager
    participant Ledger as PortfolioLedger
    
    Note over Strat: Signal Generated
    Strat->>TM: TradeIntent (e.g. OPEN 1 Lot)
    
    TM->>Ledger: est_holding_cost() (Get current usage)
    TM->>Ledger: check_funds(Required Cost + Margin)
    
    alt Not Enough Funds
        Ledger-->>TM: False
        TM-->>Strat: REJECT Signal
    else Sufficient Funds
        Ledger-->>TM: True
        TM->>Ledger: reserve_funds(Required) 
        Note right of Ledger: Locked Reserved Amount
        
        TM->>TM: Execute Transaction...
        
        opt Transaction Completed (Filled)
            TM->>Ledger: upsert(Real Position)
            TM->>Ledger: release_reserved()
            Ledger->>Ledger: Recalculate Free Cash
        end
    end
```

### D. 行情觸發與下單流程 (Synchronous Tick-to-Order) - **Deep Dive**
最核心的策略觸發路徑，包含 **Fast Lookup**, **Capital Gate**, **Unlock-IO-Lock** 等所有細節。

```mermaid
sequenceDiagram
    participant Ex as Exchange
    participant API as ShioajiAPI
    participant TM as TxManager
    participant MD as MarketData
    participant Strat as Strategy
    participant Ledger as PortfolioLedger
    participant Tx as Transaction

    Ex->>API: Tick Data (Quote)
    API->>TM: _on_tick_stk/quote_stk/_on_tick_fop/quote_fop(code)
    TM->>MD: update_stock/future(tick/quote)
    Note right of MD: Check Threshold (Size Gate)
    MD->>TM: _on_market_tick(code)
    
    rect rgb(255, 248, 220)
        Note right of TM: 1. Async Buffer (Producer)
        TM->>TM: _pending_quote_codes.add(code)
        TM->>TM: _quote_event.set()
    end
    
    Note over TM: API Thread Released Immediately
    
    rect rgb(240, 248, 255)
        Note right of TM: 2. Worker Thread (Consumer)
        TM->>TM: _quote_worker_loop() wakes up
        TM->>TM: Coalesce (De-duplicate) pending codes
        TM->>TM: Find Unique Pairs map.get(code)
    end

    loop For Each Unique Pair
        TM->>Strat: on_tick(stock, future)
        Strat->>MD: get_snapshot()
        Strat->>Strat: calc Z-Score
        
        alt Signal Triggered (Valid)
            Strat-->>TM: TradeIntent (e.g. OPEN)
            Note over TM: request_new_transaction
            rect rgb(230, 230, 255)
                Note right of TM: Capital Gate
                TM->>Ledger: check_funds(Cost)
                alt Insufficient
                     TM-->>Strat: Reject Signal
                else Sufficient
                     TM->>Ledger: reserve_funds(Cost)
                end
            end
            
            TM->>Tx: Create Transaction Object
            TM->>Tx: update() -> Start Logic
            
            rect rgb(255, 230, 230)
                Note right of Tx: Unlock-IO-Lock Pattern
                Tx->>API: place_order(Stock)
                Tx->>API: place_order(Future)
            end
            
            Tx-->>TM: Transaction Started
        end
    end
```

### E. 回報處理流程 (Asynchronous Callback Loop) - **Deep Dive**
完全非同步的狀態更新路徑，包含 **Orphan Race** 重試與 **Capital Realization**。

```mermaid
sequenceDiagram
    participant API as API Callback Thread
    participant Q as Event Queue
    participant W as Worker Thread
    participant Tx as Transaction Object
    participant Ledger as PortfolioLedger
    
    Note over API, Q: Non-Blocking (Producer)
    API->>API: Receive Order Status
    Note right of API: ExecutionEngine._order_cb
    API->>Q: put(Event Dict)
    
    Note over Q, W: Async Processing (Consumer)
    loop Worker Loop
        W->>Q: get()
        W->>W: Find Active Tx by SeqNo
        
        alt Tx Found
            W->>Tx: Acquire Lock
            Tx->>Tx: on_order_update()
            
            alt All Filled
                Tx->>Tx: State = COMPLETED
                
                rect rgb(230, 255, 230)
                   Note right of Tx: Capital Realization
                   Tx->>W: Update Ledger
                   W->>Ledger: Release Reserved / Update Real
                end
                
            else Partial Fill
                 Tx->>Tx: State = PARTIAL
            end
            
            Tx->>W: Release Lock
        else Tx Not Found (Orphan Race)
            # ORPHAN RACE DETAIL
            W->>W: Retry / Re-queue (Delay)
        end
    end
```

### F. 缺腳與救援流程 (Legging Risk / Failover Logic)
當一腳成交一腳未成交 (超時) 時的救援邏輯。這是 `BaseTransaction._timeout_and_hedge` 的具體實作。
**避險比例 (RATIO) 由 `InstrumentRegistry` 動態提供 (例如 1 Future = 2 Stocks)。**

```mermaid
sequenceDiagram
    participant Main as Main Loop
    participant Tx as Transaction
    participant API as ShioajiAPI
    
    loop Every Step
        Main->>Tx: update()
        Tx->>Tx: Check Age > 3.0s?
        
        alt Timeout Triggered
             Tx->>Tx: Check Fills (Stock vs Future)
             
             alt Balanced (S = F * 2)
                 Tx->>Tx: No Action / Wait
             else Unbalanced (Missing Leg)
                 Note right of Tx: FORCE COMPLETION
                 Tx->>API: Cancel Both Orders
                 
                 Tx->>Tx: Get RATIO = Registry.get_ratio(S, F)
                 Tx->>Tx: Calc Excess Stock = S - (F * RATIO)
                 
                 alt Excess >= RATIO (Full Future Lots)
                    rect rgb(200, 255, 200)
                        Note right of Tx: HEDGE (Sell Future)
                        Tx->>API: Sell Future (Excess // RATIO)
                    end
                 else 0 < Excess < RATIO (Odd Lot)
                    rect rgb(255, 200, 200)
                        Note right of Tx: RETREAT (Sell Stock)
                        Tx->>API: Sell Excess Stock (Close Position)
                    end
                 end
                 
                 Tx->>Tx: State = FAILED (Repaired)
             end
        end
    end
```

### G. GUI 與背景邏輯模型 (Threading Model)
```mermaid
sequenceDiagram
    participant Main as Main Thread (GUI)
    participant BG as Background Logic Thread
    participant TM as TransactionManager
    
    Note over Main: GUI Loop Starts
    
    par Parallel Execution
        loop GUI Update (Main Thread)
            Main->>Main: Handle User Input
            Main->>TM: Read Status (No Lock)
            Main->>Main: Redraw Screen
        end
    and
        loop Business Logic (BG Thread)
            BG->>TM: run_step()
            TM->>TM: Check Timeouts
            TM->>TM: Auto-Repair Logic
            TM->>TM: Sync Balance
        end
    end
```

---

### H. 背景邏輯與 Run Step 細節 (Background Logic & RunStep Detail) - **New Request**
這是系統的「心跳」循環，由 `main.py` 中的 `background_logic` 驅動，每 0.1 秒跳一次。

```mermaid
sequenceDiagram
    participant BG as Background Thread
    participant TM as TransManager
    participant API as ShioajiAPI
    participant Tx as Transaction (Each)
    
    Note over BG: background_logic() Started
    
    loop While Running (Interval: 0.1s)
        BG->>TM: run_step()
        
        rect rgb(240, 255, 240)
            Note right of TM: 1. Status Refresh (Every 2s)
            
            opt Time elapsed > 2.0s
                TM->>API: update_status(StockAccount)
                TM->>API: update_status(FutOptAccount)
            end
        end

        rect rgb(240, 255, 255)
            Note right of TM: 2. Position Sync (Every 30s)
            opt Time elapsed > 30s
                TM->>TM: _sync_held_positions_fast()
            end
        end

        rect rgb(255, 255, 240)
             Note right of TM: 3. Connection Watchdog
             opt Time elapsed > CheckInterval
                 TM->>API: check_connection() (Ping)
                 alt Failed
                     TM->>API: relogin()
                 end
             end
        end
        
        rect rgb(255, 240, 240)
            Note right of TM: 4. Transaction Update Loop
            
            TM->>TM: Copy Active Tx Keys (Lock)
            
            loop For Each Tx
                TM->>Tx: update()
                Tx->>Tx: Check Timeout / Legging
                
                alt State is Terminal (Completed/Failed)
                    TM->>TM: archive_transaction(tx)
                else Age > ZOMBIE_TIMEOUT
                    TM->>Tx: set_state(FAILED)
                    TM->>TM: archive_transaction(tx)
                end
            end
        end
        
        rect rgb(230, 230, 230)
            Note right of TM: 5. Repair Trigger
            opt trigger_repair == True & Cooldown Passed
                 TM->>TM: repair_positions()
            end
        end

        BG->>BG: sleep(0.1)
    end
```

---

## 6. 總結
1.  **分層明確**：GUI / Logic / Data / Driver 各司其職。
2.  **執行緒安全**：GUI 讀取與 Logic 寫入透過 `_lock` 或 Copy 保護。
3.  **完整生命週期**: 從 Startup -> Discovery -> Capital Check -> Order Execution -> Callback Update -> Termination/Failover.
