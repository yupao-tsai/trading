#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import datetime
from ib_insync import IB, Stock, Option, util
from typing import List, Optional, Dict, Tuple
import math
import statistics
import re
try:
    from moomoo import OpenSecTradeContext, TrdMarket, TrdEnv, SecurityFirm, RET_OK, OptionType
except ImportError:
    OpenSecTradeContext = None
    OptionType = None

# ====== 設定 ======
HOST = "127.0.0.1"
PORT = 7496
CLIENT_ID = 11

SYMBOL = "BRK B"
PRIMARY_EXCHANGE = "NYSE"
CURRENCY = "USD"

# 篩選條件
MIN_DTE = 7           # 最小到期天數
MAX_DTE = 60           # 最大到期天數
STRIKE_PCT_MIN = 0.88  # 履約價下限 (現價 * 0.90)
STRIKE_PCT_MAX = 0.99  # 履約價上限 (現價 * 0.99)
MIN_ANNUAL_RETURN = 0.05 # 最小年化報酬率過濾 (5%)
MIN_SHARPE_RATIO = 0.2   # 最小夏普值過濾 (0.2)

# 市場數據類型: 1=Live, 3=Delayed
MARKET_DATA_TYPE = 3 
# 定存無風險利率
RISK_FREE_RATE = 0.033  # 3.3%

# 交易費用設定 (每口合約 Per Contract)
# 根據券商與監管機構收費調整
TRANS_FEE_ACTIVITY = 0.01      # Trading Activity Fee
TRANS_FEE_REGULATORY = 0.01 * 2 # Options Regulatory Fee (預估)
TRANS_FEE_OCC = 0.03 * 2       # OCC Clearing Fee (預估，開倉+平倉)
TRANS_FEE_COMMISSION = 0.65    # 券商佣金 (預設 IB 費率，可自行修改)

# 總費用 (每口)
FEES_PER_CONTRACT = TRANS_FEE_ACTIVITY + TRANS_FEE_REGULATORY + TRANS_FEE_OCC + TRANS_FEE_COMMISSION

# ANSI 顏色代碼
COLOR_RED = "\033[91m"
COLOR_GREEN = "\033[92m"
COLOR_YELLOW = "\033[93m"
COLOR_BLUE = "\033[94m"
COLOR_RESET = "\033[0m"

# ==================

def connect_ib() -> IB:
    ib = IB()
    print(f"Connecting to TWS/IBG {HOST}:{PORT} clientId={CLIENT_ID} ...")
    try:
        ib.connect(HOST, PORT, clientId=CLIENT_ID, timeout=10)
    except Exception as e:
        print(f"Connection failed: {e}")
        raise
    print("Connected:", ib.isConnected())
    return ib

def qualify_stock(ib: IB) -> Stock:
    stk = Stock(SYMBOL, "SMART", currency=CURRENCY, primaryExchange=PRIMARY_EXCHANGE)
    ib.qualifyContracts(stk)
    print(f"Qualified stock: {stk.symbol} (conId: {stk.conId})")
    return stk

def get_current_price(ib: IB, contract: Stock) -> float:
    """取得股票當前價格 (Delayed)"""
    ib.reqMarketDataType(MARKET_DATA_TYPE)
    ticker = ib.reqMktData(contract, "", False, False)
    
    # 等待數據填充 (最多 5 秒)
    print("Waiting for stock price...")
    price = 0.0
    
    for _ in range(20):
        ib.sleep(0.25)
        # 優先取 last, 如果沒有則取 close, 再沒有取 (bid+ask)/2
        # Check if last is not nan and > 0
        if ticker.last and not math.isnan(ticker.last) and ticker.last > 0:
            price = ticker.last
        elif ticker.close and not math.isnan(ticker.close) and ticker.close > 0:
            price = ticker.close
            
        if price > 0:
            print(f"Current Price ({'Delayed' if MARKET_DATA_TYPE==3 else 'Live'}): {price}")
            return price
            
    print("⚠️ Warning: Could not get valid price from IB. Trying Moomoo fallback...")
    
    # Fallback to Moomoo
    try:
        from moomoo import OpenQuoteContext, RET_OK
        moomoo_host = "127.0.0.1"
        moomoo_port = 11111
        
        # IB Symbol -> Moomoo Symbol (e.g. "BRK B" -> "US.BRK.B")
        moomoo_symbol = contract.symbol.replace(" ", ".")
        if "US." not in moomoo_symbol:
            moomoo_symbol = "US." + moomoo_symbol
            
        # BRK.B fix
        if "BRK.B" in moomoo_symbol:
             moomoo_symbol = moomoo_symbol.replace("BRK.B", "BRKB")
             
        quote_ctx = OpenQuoteContext(host=moomoo_host, port=moomoo_port)
        ret, df = quote_ctx.get_stock_quote([moomoo_symbol])
        quote_ctx.close()
        
        if ret == RET_OK and not df.empty:
            row = df.iloc[0]
            if 'last_price' in row and row['last_price'] > 0:
                price = float(row['last_price'])
            elif 'close_price' in row and row['close_price'] > 0:
                price = float(row['close_price'])
                
            if price > 0:
                print(f"Current Price (Moomoo): {price}")
                return price
                
    except ImportError:
        print("Moomoo API not installed.")
    except Exception as e:
        print(f"Moomoo fallback failed: {e}")

    return 0.0

def get_option_chain(ib: IB, stk: Stock) -> List:
    """取得選擇權鏈 (回傳 Raw Chains)"""
    print("Fetching option chains...")
    chains = ib.reqSecDefOptParams(stk.symbol, "", stk.secType, stk.conId)
    if not chains:
        print("No option chains found.")
        return []
    
    print(f"Found {len(chains)} raw chains.")
    return chains

def get_historical_volatility(ib: IB, contract: Stock) -> tuple[float, float]:
    """計算歷史波動率 (HV)
    返回: (hv_1week, hv_6months)
    """
    print("Fetching historical data for HV calculation...")
    
    # 定義時間範圍
    end_date = ""
    duration_6m = "6 M"
    bar_size = "1 day"
    what_to_show = "TRADES"
    
    # 抓取 6 個月的日線資料
    bars = ib.reqHistoricalData(
        contract, 
        endDateTime=end_date, 
        durationStr=duration_6m, 
        barSizeSetting=bar_size, 
        whatToShow=what_to_show, 
        useRTH=1, 
        formatDate=1,
        keepUpToDate=False
    )
    
    if not bars:
        print("Warning: Could not fetch historical data.")
        return 0.18, 0.18 # Fallback
        
    closes = [b.close for b in bars]
    
    # 計算 Log Return
    log_returns = []
    for i in range(1, len(closes)):
        if closes[i-1] > 0:
            log_returns.append(math.log(closes[i] / closes[i-1]))
        else:
            log_returns.append(0.0)
            
    if not log_returns:
        return 0.18, 0.18

    # 計算年化波動率
    def calc_hv(returns):
        if len(returns) < 2: return 0.0
        daily_std = statistics.stdev(returns)
        return daily_std * math.sqrt(252) # 年化
    
    # HV 6 Months (全部數據)
    hv_6m = calc_hv(log_returns)
    
    # HV 1 Week (最後 5 個交易日)
    hv_1w = calc_hv(log_returns[-5:]) if len(log_returns) >= 5 else hv_6m
    
    print(f"Historical Volatility: 1-Week={hv_1w:.2%}, 6-Month={hv_6m:.2%}")
    return hv_1w, hv_6m

def filter_options(
    ib: IB, 
    stk: Stock, 
    chains: List, 
    current_price: float
) -> List[Option]:
    """根據演算法篩選適合 Sell Put 的合約 (使用 Chain 內部結構避免盲猜)"""
    
    today = datetime.date.today()
    candidates = []

    # 計算目標履約價範圍
    target_strike_min = current_price * STRIKE_PCT_MIN
    target_strike_max = current_price * STRIKE_PCT_MAX
    
    print(f"Scanning chains for DTE {MIN_DTE}-{MAX_DTE} and Strike {target_strike_min:.2f}-{target_strike_max:.2f}...")

    # 用集合避免重複 (同一個合約可能出現在多個 chain)
    seen_contracts = set()
    count_chains_processed = 0

    for chain in chains:
        # 只處理 SMART
        if chain.exchange != 'SMART':
            continue
            
        count_chains_processed += 1
        
        # 1. 篩選到期日
        valid_expiries = []
        for exp_str in chain.expirations:
            year = int(exp_str[:4])
            month = int(exp_str[4:6])
            day = int(exp_str[6:])
            exp_date = datetime.date(year, month, day)
            days_to_expire = (exp_date - today).days
            
            if MIN_DTE <= days_to_expire <= MAX_DTE:
                valid_expiries.append(exp_str)
        
        if not valid_expiries:
            continue

        # 2. 篩選履約價 (使用該 Chain 自己的 Strikes)
        for strike in chain.strikes:
            if target_strike_min <= strike <= target_strike_max:
                
                # 針對每個符合的到期日建立合約
                for exp_str in valid_expiries:
                    # Key for deduplication (since we don't specify tradingClass anymore, ignore it in key too)
                    key = (exp_str, strike)
                    if key in seen_contracts:
                        continue
                    
                    seen_contracts.add(key)
                    
                    opt = Option(
                        symbol=stk.symbol,
                        lastTradeDateOrContractMonth=exp_str,
                        strike=strike,
                        right='P',
                        exchange='SMART',
                        currency=CURRENCY,
                        # tradingClass=chain.tradingClass, # 移除以避免 Error 200 (讓 IB 自動匹配)
                        multiplier=chain.multiplier
                    )
                    candidates.append(opt)
                    
    print(f"Processed {count_chains_processed} SMART chains.")
    print(f"Generated {len(candidates)} verified candidate contracts.")
    return candidates

def calculate_kelly_fraction(annualized_ret, risk_free_rate, win_prob, iv, hv_short, hv_long):
    """
    凱利公式計算投資比例 (綜合 IV 與 HV 風險)
    
    風險參數 (Sigma) 的選擇邏輯：
    我們取 max(IV, HV_Short, HV_Long)
    這是最保守的做法，確保無論是市場預期 (IV) 還是歷史實踐 (HV)，
    只要有任一指標顯示風險很高，我們就降低部位。
    """
    
    # 1. 使用 Delta (win_prob = 1 - Delta) 修正預期報酬率
    expected_annual_ret = annualized_ret * win_prob
    
    if expected_annual_ret <= risk_free_rate:
        return 0.0

    # 2. 處理波動率 - 取最大值作為風險定價
    sigma = max(iv, hv_short, hv_long)
    
    if sigma < 0.1: sigma = 0.1

    # 3. Merton's Fraction
    # f = (mu - r) / (sigma^2)
    fraction = (expected_annual_ret - risk_free_rate) / (sigma ** 2)
    
    # 4. Half-Kelly (安全邊際)
    return max(0, fraction * 0.5) 

# --- Black-Scholes Utility Functions ---
from math import log, sqrt, exp
from scipy.stats import norm

def bs_put_price(S, K, T, r, sigma):
    """計算 Put 權利金 (BS 模型)"""
    if T <= 0 or sigma <= 0: return 0.0
    d1 = (log(S/K) + (r + 0.5 * sigma**2) * T) / (sigma * sqrt(T))
    d2 = d1 - sigma * sqrt(T)
    put_price = K * exp(-r * T) * norm.cdf(-d2) - S * norm.cdf(-d1)
    return put_price

def bs_call_price(S, K, T, r, sigma):
    """計算 Call 權利金 (BS 模型)"""
    if T <= 0 or sigma <= 0: return 0.0
    d1 = (log(S/K) + (r + 0.5 * sigma**2) * T) / (sigma * sqrt(T))
    d2 = d1 - sigma * sqrt(T)
    call_price = S * norm.cdf(d1) - K * exp(-r * T) * norm.cdf(d2)
    return call_price

def bs_put_delta(S, K, T, r, sigma):
    """計算 Put Delta (BS 模型)"""
    if T <= 0 or sigma <= 0: return 0.0
    d1 = (log(S/K) + (r + 0.5 * sigma**2) * T) / (sigma * sqrt(T))
    return norm.cdf(d1) - 1.0

def bs_call_delta(S, K, T, r, sigma):
    """計算 Call Delta (BS 模型)"""
    if T <= 0 or sigma <= 0: return 0.0
    d1 = (log(S/K) + (r + 0.5 * sigma**2) * T) / (sigma * sqrt(T))
    return norm.cdf(d1)

def implied_volatility(price, S, K, T, r, right='P'):
    """反推隱含波動率 (二分法)"""
    if price <= 0: return 0.0
    
    low = 0.001
    high = 5.0
    for _ in range(50):
        mid = (low + high) / 2
        if right == 'C':
            est_price = bs_call_price(S, K, T, r, mid)
        else:
            est_price = bs_put_price(S, K, T, r, mid)
            
        if abs(est_price - price) < 0.001:
            return mid
        if est_price < price:
            low = mid
        else:
            high = mid
    return (low + high) / 2

# ---------------------------------------

def get_moomoo_quote_for_options(ib_symbol_base: str, min_dte: int, max_dte: int) -> Dict[Tuple[str, float], float]:
    """
    使用 Moomoo get_option_chain 直接獲取並篩選選擇權報價 (支援分頁處理日期範圍)
    Returns: Dict[(ExpiryYYYYMMDD, Strike), price]
    """
    if OpenSecTradeContext is None:
        return {}

    moomoo_host = "127.0.0.1"
    moomoo_port = 11111
    
    # IB Symbol -> Moomoo Symbol Base (e.g. "BRK B" -> "US.BRK.B")
    moomoo_base = ib_symbol_base.replace(" ", ".")
    if "US." not in moomoo_base:
        moomoo_base = "US." + moomoo_base
    
    # 針對 BRK.B 這種特殊情況，Moomoo 代碼可能是 US.BRKB 而不是 US.BRK.B
    adjusted_base = moomoo_base
    if "BRK.B" in moomoo_base:
        adjusted_base = moomoo_base.replace("BRK.B", "BRKB")
    
    # 計算日期範圍
    today = datetime.date.today()
    start_date_overall = today + datetime.timedelta(days=min_dte)
    end_date_overall = today + datetime.timedelta(days=max_dte)
    
    # Result mapping: (ExpiryStr, Strike) -> Price
    price_map = {}
    
    try:
        from moomoo import OpenQuoteContext, SubType
        quote_ctx = OpenQuoteContext(host=moomoo_host, port=moomoo_port)
        
        # --- 1. 分段獲取 Option Chain (每段最多 30 天) ---
        current_start = start_date_overall
        all_codes = []
        
        while current_start <= end_date_overall:
            # Calculate chunk end (start + 29 days or overall end)
            current_end = min(current_start + datetime.timedelta(days=29), end_date_overall)
            
            s_str = current_start.strftime("%Y-%m-%d")
            e_str = current_end.strftime("%Y-%m-%d")
            
            # print(f"DEBUG: Fetching chain chunk: {s_str} to {e_str}")
            
            # Try adjusted base first
            ret, df_chain = quote_ctx.get_option_chain(
                code=adjusted_base,
                start=s_str,
                end=e_str,
                option_type=OptionType.PUT
            )
            
            # Fallback to original base
            if (ret != RET_OK or df_chain.empty) and adjusted_base != moomoo_base:
                ret, df_chain = quote_ctx.get_option_chain(
                    code=moomoo_base,
                    start=s_str,
                    end=e_str,
                    option_type=OptionType.PUT
                )
            
            if ret == RET_OK and not df_chain.empty:
                all_codes.extend(df_chain['code'].tolist())
            
            # Move to next chunk
            current_start = current_end + datetime.timedelta(days=1)

        # Remove duplicates
        all_codes = list(set(all_codes))
        # print(f"DEBUG: Total option codes found: {len(all_codes)}")
        
        if not all_codes:
            quote_ctx.close()
            return {}

        # --- 2. 批量獲取報價 (因為代碼都是有效的，可以大膽批量) ---
        chunk_size = 100 # Can be up to 200-400 usually
        for i in range(0, len(all_codes), chunk_size):
            batch = all_codes[i:i+chunk_size]
            quote_ctx.subscribe(batch, [SubType.QUOTE], subscribe_push=False)
            ret_q, df_quote = quote_ctx.get_stock_quote(batch)
            
            if ret_q == RET_OK and not df_quote.empty:
                for _, row in df_quote.iterrows():
                    code = row['code']
                    price = 0.0
                    if 'last_price' in row and row['last_price'] > 0:
                        price = row['last_price']
                    elif 'nominal_price' in row and row['nominal_price'] > 0:
                        price = row['nominal_price']
                    
                    if price > 0:
                        # 解析 Expiry 和 Strike
                        # 格式: US.BRKB260123P485000
                        # Regex match
                        # 假設前面的 ticker 長度不固定，但後面固定是 YYMMDD(6) + Type(1) + Strike(variable)
                        match = re.search(r'(\d{6})([CP])(\d+)$', code)
                        if match:
                            date_str, type_char, strike_str = match.groups()
                            # Convert date_str (YYMMDD) to YYYYMMDD
                            expiry_full = "20" + date_str # Assuming 20xx
                            strike = int(strike_str) / 1000.0
                            
                            price_map[(expiry_full, strike)] = price
            
            quote_ctx.unsubscribe(batch, [SubType.QUOTE])
            
        quote_ctx.close()
        
    except Exception as e:
        print(f"Error fetching Moomoo option chain quotes: {e}")
        
    return price_map

def analyze_candidates(ib: IB, candidates: List[Option], current_price: float, hv_1w: float, hv_6m: float) -> List[Dict]:
    """對候選合約請求報價並計算指標"""
    results = []
    
    # --- Step 0: 預先抓取 Moomoo 報價作為備援 ---
    print("Fetching backup quotes from Moomoo (via Option Chain)...")
    # 傳入 Symbol 和 DTE 範圍，讓 Moomoo 自己抓 Chain
    moomoo_prices = get_moomoo_quote_for_options(SYMBOL, MIN_DTE, MAX_DTE)
    print(f"Got {len(moomoo_prices)} quotes from Moomoo.")
    
    # 分批處理以避免請求過多
    chunk_size = 50
    for i in range(0, len(candidates), chunk_size):
        chunk = candidates[i:i+chunk_size]
        print(f"Processing batch {i+1} to {min(i+chunk_size, len(candidates))}...")
        
        # Qualify contracts first
        ib.qualifyContracts(*chunk)
        
        # Request market data (Stream instead of Snapshot to better catch delayed data)
        # 對於延遲數據，Snapshot 有時會回傳空值，用 Stream 觀察幾秒通常比較穩
        ib.reqMarketDataType(MARKET_DATA_TYPE)
        tickers = []
        for opt in chunk:
            # 請求額外的 Greeks 欄位 (Generic Tick Tags)
            # 100: Option Volume, 101: Option Open Interest, 106: Option Implied Volatility
            t = ib.reqMktData(opt, "100,101,106", False, False) 
            tickers.append((opt, t))
        
        # Wait for data to populate
        # 迴圈檢查直到大部分 ticker 都有數據或超時
        print("Waiting for option quotes...")
        for _ in range(40): # Wait up to 10 seconds (原為 3 秒)
            ib.sleep(0.25)
            # 檢查是否有任何一個 ticker 有效 (避免全空)
            valid_count = sum(1 for _, t in tickers if (t.bid and t.bid > 0) or (t.close and t.close > 0) or (t.last and t.last > 0))
            if valid_count > len(chunk) * 0.99: # 等到 95% 都有數據 (原為 80%)
                break
        
        today = datetime.date.today()
        
        for opt, ticker in tickers:
            # 停止訂閱，避免塞爆
            ib.cancelMktData(opt)

            # 嘗試取得價格: Bid > Last > Close
            # 注意: Delayed data 常常 Bid/Ask 是 nan/-1，這時可能只有 close 或 last
            # 我們記錄價格來源以供參考
            raw_bid = ticker.bid
            raw_ask = ticker.ask
            last = ticker.last
            close = ticker.close
            
            final_price = 0.0
            price_src = ""

            # --- 價格選取邏輯 (優先順序: IB Bid > IB Last > Moomoo > IB Close) ---
            
            # 1. 嘗試抓取 IB Last (最新成交)
            if last and not math.isnan(last) and last > 0:
                final_price = last
                price_src = "L"
            
            # 2. 如果有 IB Bid，且 Bid 合理 (比如 Bid > Last * 0.8)，則優先用 Bid
            if raw_bid and not math.isnan(raw_bid) and raw_bid > 0:
                if final_price > 0:
                    if raw_bid > final_price * 0.8:
                        final_price = raw_bid
                        price_src = "B"
                    else:
                        price_src = "L(LowB)" 
                else:
                    final_price = raw_bid
                    price_src = "B"
            
            # 3. 如果 IB Bid 和 Last 都無效 (final_price 為 0)，嘗試用 Moomoo 數據
            #    這比 IB Close 優先，因為 Moomoo 數據通常包含盤後或更即時的 Last
            if final_price == 0:
                # 查表: (Expiry, Strike)
                key = (opt.lastTradeDateOrContractMonth, opt.strike)
                if key in moomoo_prices and moomoo_prices[key] > 0:
                    final_price = moomoo_prices[key]
                    price_src = "M" # Moomoo

            # 4. 如果以上都沒有，才用 IB Close (昨收)
            if final_price == 0:
                if close and not math.isnan(close) and close > 0:
                    final_price = close
                    price_src = "C"

            if final_price <= 0.05: 
                continue
                
            # 這裡我們將 final_price 視為 "可賣出的估計價格" (Bid)
            bid = final_price
                
            # 計算 DTE
            year = int(opt.lastTradeDateOrContractMonth[:4])
            month = int(opt.lastTradeDateOrContractMonth[4:6])
            day = int(opt.lastTradeDateOrContractMonth[6:])
            exp_date = datetime.date(year, month, day)
            dte = (exp_date - today).days
            if dte == 0: dte = 1
            T = dte / 365.0 # 年化時間
            
            # 交易成本 (Interactive Brokers + 監管費)
            # 使用上方定義的 FEES_PER_CONTRACT (每口)
            # 換算成每股成本需除以 100 (1口 = 100股)
            fees_per_share = FEES_PER_CONTRACT / 100.0
            
            # 計算指標 (扣除費用後的淨回報)
            net_bid = bid - fees_per_share
            if net_bid < 0: net_bid = 0.0
            
            ret = net_bid / opt.strike
            annualized_ret = ret * (365 / dte)
            
            # 計算如果要達到年化 5% 報酬率，需要的權利金價格
            # 0.05 = (TargetBid / Strike) * (365 / DTE)
            target_bid_5pct = opt.strike * 0.05 * dte / 365.0
            
            # --- 取得或估算 Greeks (IV & Delta) ---
            iv = 0.0
            delta = 0.0
            is_iv_estimated = False
            is_delta_estimated = False

            # 嘗試從 TWS 取得 Greeks (若有訂閱即時數據或有 Snapshot)
            if ticker.modelGreeks and ticker.modelGreeks.impliedVol:
                 iv = ticker.modelGreeks.impliedVol
                 delta = ticker.modelGreeks.delta
            else:
                 # 延遲數據通常沒有 Greeks，需自行估算
                 is_iv_estimated = True
                 is_delta_estimated = True
                 # 使用 Black-Scholes 反推 IV 與 Delta
                 iv = implied_volatility(bid, current_price, opt.strike, T, RISK_FREE_RATE)
                 delta = bs_put_delta(current_price, opt.strike, T, RISK_FREE_RATE, iv)

            # --- 計算理想價格 (Ideal Price) ---
            # 根據 Kelly Criterion 反推：如果我們希望投入 5% 資金 (Kelly=0.05)，需要多少報酬率？
            # 假設目標 Kelly% = 5% (0.05)
            # Kelly(Half) = 0.5 * Fraction = 0.05  => Fraction = 0.10
            # Fraction = (Exp.Ret - RiskFree) / Sigma^2
            # 0.10 = (Exp.Ret - 0.033) / (Sigma^2)
            # Exp.Ret = 0.10 * Sigma^2 + 0.033
            # 而 Exp.Ret = Annualized_Ret * (1 - |Delta|)
            # 所以 Target_Annual_Ret = (0.10 * Sigma^2 + 0.033) / (1 - |Delta|)
            # Target_Bid = Strike * Target_Annual_Ret * (DTE / 365)
            
            target_kelly_allocation = 0.10  # 理想配置 10% (Half-Kelly 5%)
            # 這裡我們設定一個較積極的目標：希望能配置 10% 資金 (即 Fraction=0.2)
            target_fraction = target_kelly_allocation * 2 # 還原成 Full Kelly
            
            # 使用保守波動率
            conservative_vol = max(iv, hv_1w, hv_6m)
            
            required_exp_ret = target_fraction * (conservative_vol ** 2) + RISK_FREE_RATE
            
            abs_delta = abs(delta) # 確保有定義
            
            # 加上 Delta 修正 (還原)
            if (1.0 - abs_delta) > 0.1: # 避免除以 0
                required_ann_ret = required_exp_ret / (1.0 - abs_delta)
            else:
                required_ann_ret = required_exp_ret / 0.1 # 保守
                
            ideal_bid = opt.strike * required_ann_ret * (dte / 365.0)

            # 計算勝率
            win_prob = 1.0 - abs_delta
            if win_prob < 0: win_prob = 0

            # 計算 Kelly 建議比例 (相對於定存)
            # 使用修正版 Merton: (Exp.Ret(adj) - Risk Free) / (Sigma^2)
            kelly_pct = calculate_kelly_fraction(annualized_ret, RISK_FREE_RATE, win_prob, iv, hv_1w, hv_6m)

            # 計算 Sharpe Ratio (輔助指標)
            # Sharpe = (Ann Return - Risk Free) / Volatility
            # Volatility 使用 max(iv, hv) 作為保守估計
            vol_risk = max(iv, hv_1w, hv_6m)
            sharpe = 0.0
            if vol_risk > 0.01:
                sharpe = (annualized_ret - RISK_FREE_RATE) / vol_risk

            if annualized_ret < MIN_ANNUAL_RETURN:
                continue
                
            if sharpe < MIN_SHARPE_RATIO:
                continue

            results.append({
                "symbol": opt.symbol,
                "expiry": opt.lastTradeDateOrContractMonth,
                "strike": opt.strike,
                "bid": bid,
                "price_src": price_src,
                "ask": ticker.ask if ticker.ask else 0.0,
                "last": ticker.last if ticker.last else 0.0,
                "dte": dte,
                "return": ret,
                "annualized": annualized_ret,
                "target_bid": target_bid_5pct,
                "ideal_bid": ideal_bid,
                "iv": iv,
                "delta": delta,
                "is_iv_est": is_iv_estimated,
                "is_delta_est": is_delta_estimated,
                "kelly": kelly_pct,
                "sharpe": sharpe,
                "conId": opt.conId
            })
            
    # 根據 Sharpe Ratio 排序 (高到低)，這是更科學的「性價比」指標
    def sort_key(item):
        return item['sharpe']

    return sorted(results, key=sort_key, reverse=True)

def get_moomoo_positions(ib_symbol: str, underlying_price: float, hv_1w: float, hv_6m: float):
    """從 Moomoo 獲取與該標的相關的持倉 (包含正股與選擇權) 並進行分析"""
    if OpenSecTradeContext is None:
        print("\n[Moomoo] API package not found, skipping position check.")
        return

    print(f"\n====== Moomoo Positions for {ib_symbol} ======")
    moomoo_host = "127.0.0.1"
    moomoo_port = 11111
    
    # 轉換 Symbol: IB "BRK B" -> Moomoo "US.BRK.B"
    # 簡單規則：取代空白為點，並加上 US. (假設是美股)
    moomoo_symbol_base = ib_symbol.replace(" ", ".")
    if "US." not in moomoo_symbol_base:
        moomoo_symbol_base = "US." + moomoo_symbol_base

    try:
        # 建立交易 Context，並指定市場為 US
        # SecurityFirm Enum:
        # FUTUSECURITIES = 'FUTUSECURITIES' (Futu HK)
        # FUTUINC = 'FUTUINC' (Moomoo US)
        # FUTUSG = 'FUTUSG' (Moomoo SG)
        # FUTUAU = 'FUTUAU' (Moomoo AU)
        
        firm = getattr(SecurityFirm, 'FUTUINC', None) # Moomoo US
        if not firm:
             firm = getattr(SecurityFirm, 'FUTUSG', None)
        
        print(f"Connecting to Moomoo with SecurityFirm: {firm}...")
        
        if firm:
             trd_ctx = OpenSecTradeContext(filter_trdmarket=TrdMarket.US, host=moomoo_host, port=moomoo_port, security_firm=firm)
        else:
             trd_ctx = OpenSecTradeContext(filter_trdmarket=TrdMarket.US, host=moomoo_host, port=moomoo_port)

        # 獲取美股持倉
        ret, df = trd_ctx.position_list_query(trd_env=TrdEnv.REAL)
        trd_ctx.close()

        if ret != RET_OK:
            print(f"Moomoo Error: {df}")
            return

        if df.empty:
            print("No positions found in Moomoo.")
            return

        # 篩選相關持倉 (Code 包含 Symbol)
        # 轉大寫比對
        target = moomoo_symbol_base.upper().replace("US.", "") # 移除 US. 只比對代號部分比較寬鬆
        # 例如 BRK.B -> BRK.B
        # 檢查 code 是否包含 BRK.B (US.BRK.B...)
        
        # Debug: 顯示所有持倉以便確認
        # print("All positions:", df['code'].tolist())
        
        filtered = df[df['code'].str.contains(target, case=False, regex=False)]
        
        if filtered.empty:
            # 嘗試更寬鬆的匹配，例如只匹配 BRK
            simple_target = target.split('.')[0]
            filtered = df[df['code'].str.contains(simple_target, case=False, regex=False)]
            
        if filtered.empty:
            print(f"No positions found for {ib_symbol} (Target: {target}).")
            # print("Available positions:", df['code'].tolist())
            return
        else:
            # 顯示特定欄位
            # Note: Moomoo API sometimes returns different column names or misses current_price for options
            # Check available columns first
            cols = ['code', 'stock_name', 'qty', 'cost_price', 'current_price', 'pl_ratio', 'nominal_price']
            cols = [c for c in cols if c in filtered.columns]
            
            # Sort Raw Positions by code (which includes Expiry Date: e.g. US.BRKB260123...)
            filtered_sorted = filtered.sort_values('code')
            
            print("--- Raw Positions (Sorted by Code/Expiry) ---")
            print(filtered_sorted[cols].to_string(index=False))
            
            # ====== 分析持倉 (仿照 Sell Put Recommendations) ======
            print("\n--- Position Analysis (Calculated based on Current Market Price) ---")
            print("Note: 'Ann%' and 'Kelly%' are calculated as if opening a NEW position at current price.")
            print(f"{'Expiry':<10} | {'DTE':<4} | {'Strike':<8} | {'Price':<8} | {'Qty':<4} | {'P/L%':<7} | {'Ann%':<6} | {'Ideal':<8} | {'Ratio':<6} | {'Shp':<4} | {'IV':<6} | {'Delta':<7} | {'Kelly%':<7}")
            print("-" * 138)

            today = datetime.date.today()
            
            # Regex to parse option code: e.g., US.BRK.B260116P492500 -> Date:260116, Type:P, Strike:492500
            # 假設最後是 6位日期 + 1位類型 + 8位履約價
            # 但實際看到的是 US.BRKB260123P485000 (BRK.B 變成 BRKB)
            # 這裡我們放寬前面的匹配，並且 strike 可能為 任意位數 (PYPL 可能是 5 位，如 57000)
            opt_pattern = re.compile(r'(\d{6})([CP])(\d+)$')

            position_rows = []

            for _, row in filtered.iterrows():
                code = row['code']
                match = opt_pattern.search(code)
                if not match:
                    # Not an option or unknown format
                    continue
                
                date_str, opt_type, strike_str = match.groups()
                
                # Parse Expiry
                year = 2000 + int(date_str[:2])
                month = int(date_str[2:4])
                day = int(date_str[4:])
                exp_date = datetime.date(year, month, day)
                expiry_display = exp_date.strftime("%Y%m%d")
                
                # Parse Strike
                strike = int(strike_str) / 1000.0
                
                # Calculate DTE
                dte = (exp_date - today).days
                if dte == 0: dte = 1
                T = dte / 365.0
                
                # Price (Try 'current_price' first, then 'nominal_price', then 'cost_price' as fallback if market closed/unavailable)
                current_opt_price = 0.0
                if 'current_price' in row:
                    current_opt_price = row['current_price']
                elif 'nominal_price' in row: # Sometimes nominal_price is used
                     current_opt_price = row['nominal_price']
                
                # If price is 0 or NaN, maybe use cost_price just to avoid crash, but label it? 
                # Or just skip/show 0.
                if math.isnan(current_opt_price): current_opt_price = 0.0
                
                # Metrics Calculation (Same as analyze_candidates)
                fees_per_share = FEES_PER_CONTRACT / 100.0
                net_bid = current_opt_price - fees_per_share
                if net_bid < 0: net_bid = 0.0
                
                ret = net_bid / strike
                annualized_ret = ret * (365 / dte)
                
                # IV & Delta
                # Use estimated IV/Delta since we don't have real-time Greeks in position_list_query
                iv = implied_volatility(current_opt_price, underlying_price, strike, T, RISK_FREE_RATE, right=opt_type)
                
                if opt_type == 'C':
                    delta = bs_call_delta(underlying_price, strike, T, RISK_FREE_RATE, iv)
                else:
                    delta = bs_put_delta(underlying_price, strike, T, RISK_FREE_RATE, iv)
                
                # Ideal Price & Kelly
                target_kelly_allocation = 0.10
                target_fraction = target_kelly_allocation * 2
                conservative_vol = max(iv, hv_1w, hv_6m)
                required_exp_ret = target_fraction * (conservative_vol ** 2) + RISK_FREE_RATE
                
                abs_delta = abs(delta)
                if (1.0 - abs_delta) > 0.1:
                    required_ann_ret = required_exp_ret / (1.0 - abs_delta)
                else:
                    required_ann_ret = required_exp_ret / 0.1
                
                ideal_bid = strike * required_ann_ret * (dte / 365.0)
                
                win_prob = 1.0 - abs_delta
                if win_prob < 0: win_prob = 0
                
                kelly_pct = calculate_kelly_fraction(annualized_ret, RISK_FREE_RATE, win_prob, iv, hv_1w, hv_6m)
                
                # Ratio
                ratio = 0.0
                if ideal_bid > 0:
                    ratio = current_opt_price / ideal_bid
                
                # Formatting
                pl_val = row.get('pl_ratio', 0.0) 
                pl_str = f"{pl_val:.1f}%"
                
                position_rows.append({
                    'expiry': expiry_display,
                    'exp_date_obj': exp_date,
                    'dte': dte,
                    'strike': strike,
                    'type': opt_type,
                    'price': current_opt_price,
                    'qty': row['qty'],
                    'pl_str': pl_str,
                    'ann': annualized_ret,
                    'ideal': ideal_bid,
                    'ratio': ratio,
                    'iv': iv,
                    'delta': delta,
                    'kelly': kelly_pct
                })

            # Sort: Put first (Type 'P'), then Call (Type 'C') is default sort order? No, C < P.
            # We want Put first. So sort key for Type: P=0, C=1.
            position_rows.sort(key=lambda x: (0 if x['type']=='P' else 1, x['exp_date_obj'], x['strike']))

            last_type = None

            for row in position_rows:
                # Add separator between Put and Call
                if last_type is not None and row['type'] != last_type:
                    print("-" * 130)
                last_type = row['type']

                strike_display = f"{row['strike']}{row['type']}"
                row_str = f"{row['expiry']:<10} | {row['dte']:<4} | {strike_display:<8} | {row['price']:<8.2f} | {row['qty']:<4} | {row['pl_str']:<7} | {row['ann']*100:<6.1f} | {row['ideal']:<8.2f} | {row['ratio']:<6.2f} | {row['iv']:.2f}*  | {row['delta']:>7.2f}* | {row['kelly']*100:>6.1f}%"
                
                # Apply same coloring logic as main table
                # Good Deal: Ratio > 1.0 (Price > Ideal)
                if row['ratio'] > 1.0:
                    abs_delta = abs(row['delta'])
                    row_color = COLOR_GREEN
                    if abs_delta < 0.20:
                        row_color = COLOR_RED
                    elif 0.20 <= abs_delta <= 0.30:
                        row_color = COLOR_GREEN
                    elif abs_delta > 0.30:
                        row_color = COLOR_BLUE
                        
                    print(f"{row_color}{row_str}{COLOR_RESET}")
                else:
                    print(row_str)


    except Exception as e:
        print(f"Failed to fetch Moomoo positions: {e}")

def main():
    util.patchAsyncio()
    ib = connect_ib()
    
    try:
        # 1. 取得標的與現價
        stk = qualify_stock(ib)
        price = get_current_price(ib, stk)
        if price <= 0:
            print("❌ Cannot retrieve underlying price. Aborting.")
            return

        # 1.5 取得歷史波動率
        hv_1w, hv_6m = get_historical_volatility(ib, stk)

        # 2. 取得選擇權鏈
        chains = get_option_chain(ib, stk)
        if not chains:
            return

        # 3. 篩選合約 (根據時間與價外程度)
        candidates = filter_options(ib, stk, chains, price)
        if not candidates:
            print("No candidates match the filter criteria.")
            return

        # 4. 分析與計算回報 (Sell Put 演算法核心)
        print("\nAnalyzing candidates (fetching delayed quotes)...")
        ranked_results = analyze_candidates(ib, candidates, price, hv_1w, hv_6m)

        # 5. 輸出結果
        print(f"\n====== Sell Put Recommendations for {SYMBOL} (Price: {price}) ======")
        print(f"Risk Free Rate: {RISK_FREE_RATE:.1%}")
        print(f"Risk Metrics: IV(Implied) vs HV(Hist): 1W={hv_1w:.1%}, 6M={hv_6m:.1%}")
        print("Note: Ideal Bid assumes a target Kelly allocation of 10% (Half-Kelly).")
        print("Note: Price Source: B=Bid, L=Last, C=Close. 'L'/'C' may be stale!")
        print("WARNING: '*' indicates Greeks (IV/Delta) are ESTIMATED due to delayed data.")
        print(f"{'Expiry':<10} | {'DTE':<4} | {'Strike':<8} | {'Bid(Src)':<16} | {'Ret%':<6} | {'Ann%':<6} | {'Ideal':<8} | {'Ratio':<6} | {'Shp':<4} | {'IV':<6} | {'HV':<6} | {'Delta':<7} | {'Kelly%':<7}")
        print("-" * 158)
        
        top_n = 20
        for res in ranked_results[:top_n]:
            iv_str = f"{res['iv']:.2f}" + ("*" if res['is_iv_est'] else "")
            delta_str = f"{res['delta']:.2f}" + ("*" if res['is_delta_est'] else "")
            # 使用保守波動率作為參考，但這裡我們想顯示實際的 HV (取 1W 和 6M 較大者，或顯示其中一個)
            # 為了版面簡潔，我們顯示 max(hv_1w, hv_6m) 
            hv_val = max(hv_1w, hv_6m)
            hv_str = f"{hv_val:.2f}"
            
            # 判斷是否為優質交易 (Bid > Ideal)
            ratio = 0.0
            if res['ideal_bid'] > 0:
                 ratio = res['bid'] / res['ideal_bid']
            
            is_good_deal = res['bid'] > res['ideal_bid']
            
            price_content = f"{res['bid']:.2f}({res['price_src']})"
            if is_good_deal:
                 price_content += " !"
            
            # 手動 Padding 價格欄位 (因為稍後要整行上色，這裡先處理純文字對齊)
            price_padding = 16 - len(price_content)
            if price_padding < 0: price_padding = 0
            price_field = price_content + " " * price_padding

            # 組合物件
            row_str = f"{res['expiry']:<10} | {res['dte']:<4} | {res['strike']:<8.1f} | {price_field} | {res['return']*100:<6.1f} | {res['annualized']*100:<6.1f} | {res['ideal_bid']:<8.2f} | {ratio:<6.2f} | {res['sharpe']:<4.2f} | {iv_str:<6} | {hv_str:<6} | {delta_str:>7} | {res['kelly']*100:>6.1f}%"

            # 整行上色
            if is_good_deal:
                # Color logic based on absolute Delta
                abs_delta = abs(res['delta'])
                row_color = COLOR_GREEN # Default
                
                if abs_delta < 0.20:
                    row_color = COLOR_RED
                elif 0.20 <= abs_delta <= 0.30:
                    row_color = COLOR_GREEN
                elif abs_delta > 0.30:
                    row_color = COLOR_BLUE
                
                print(f"{row_color}{row_str}{COLOR_RESET}")
            else:
                print(row_str)

        if not ranked_results:
            print("No options passed the return criteria.")

        # 6. 查詢 Moomoo 持倉
        get_moomoo_positions(SYMBOL, price, hv_1w, hv_6m)

    finally:
        ib.disconnect()
        print("\nDisconnected.")

if __name__ == "__main__":
    main()
