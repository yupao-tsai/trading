#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import sys
import argparse
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

try:
    import robin_stocks.robinhood as rh
    import pyotp
except ImportError:
    rh = None
    pyotp = None

def fetch_missing_option_details(rh_client, position):
    """
    Fetches missing instrument details (strike, type, expiration) for a position
    if they are not present, using the 'option' instrument URL.
    """
    if 'strike_price' not in position and 'option' in position:
        try:
            # option field is a URL like https://api.robinhood.com/options/instruments/<id>/
            instr_url = position.get('option', '')
            if instr_url:
                instr_id = instr_url.rstrip('/').split('/')[-1]
                # Use the function exposed in __init__.py
                instr_data = rh_client.get_option_instrument_data_by_id(instr_id)
                if instr_data:
                    # Update position with instrument details
                    if 'strike_price' in instr_data:
                        position['strike_price'] = instr_data['strike_price']
                    if 'expiration_date' in instr_data:
                        position['expiration_date'] = instr_data['expiration_date']
                    if 'type' in instr_data:
                        position['type'] = instr_data['type']
        except Exception:
            pass # Fail silently or log if needed
    return position

# ====== 設定 ======
HOST = "127.0.0.1"
PORT = 7496
CLIENT_ID = 11

# Argument Parsing
parser = argparse.ArgumentParser(description="Sell Put Recommendations")
parser.add_argument("symbol_positional", nargs="?", help="Stock Symbol (e.g. AAPL)")
parser.add_argument("--symbol", help="Stock Symbol (e.g. AAPL)")
args = parser.parse_args()

arg_symbol = None
if args.symbol:
    arg_symbol = args.symbol.upper()
elif args.symbol_positional:
    arg_symbol = args.symbol_positional.upper()

if arg_symbol:
    # Normalize for IBKR: "BRK.B" -> "BRK B", "BRKB" -> "BRK B"
    if arg_symbol == "BRK.B" or arg_symbol == "BRKB":
        SYMBOL = "BRK B"
    else:
        SYMBOL = arg_symbol
else:
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

# 顯示選擇權數量
NUM_OPTIONS_TO_SHOW = 40
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
        from moomoo import OpenQuoteContext, RET_OK, SubType
        moomoo_host = "127.0.0.1"
        moomoo_port = 11111
        
        # IB Symbol -> Moomoo Symbol (e.g. "BRK B" -> "US.BRK.B")
        base_symbol = contract.symbol.replace(" ", ".")
        if "US." not in base_symbol:
            base_symbol = "US." + base_symbol
            
        # Prepare list of symbols to try (Handle BRK.B case)
        symbols_to_try = [base_symbol]
        if "BRK.B" in base_symbol:
             # Try US.BRKB (common in Moomoo) and US.BRK.B
             symbols_to_try = [base_symbol.replace("BRK.B", "BRKB"), base_symbol]
             
        quote_ctx = OpenQuoteContext(host=moomoo_host, port=moomoo_port)
        
        for sym in symbols_to_try:
            print(f"Trying Moomoo symbol: {sym}")
            # Must subscribe first!
            quote_ctx.subscribe([sym], [SubType.QUOTE], subscribe_push=False)
            
            # Wait a bit for subscription to take effect
            import time
            time.sleep(1.0)
            
            ret, df = quote_ctx.get_stock_quote([sym])
            
            if ret == RET_OK and not df.empty:
                row = df.iloc[0]
                if 'last_price' in row and row['last_price'] > 0:
                    price = float(row['last_price'])
                elif 'close_price' in row and row['close_price'] > 0:
                    price = float(row['close_price'])
                    
                if price > 0:
                    print(f"Current Price (Moomoo): {price}")
                    quote_ctx.close()
                    return price
            else:
                 print(f"Moomoo quote failed for {sym}: ret={ret}")
                 # Moomoo return df can be a string error msg sometimes if ret != RET_OK
                 if hasattr(df, 'empty') and not df.empty:
                     print(f"DataFrame content: {df.to_dict()}")
                 else:
                     print(f"Error detail: {df}")

        quote_ctx.close()
                
    except ImportError:
        print("Moomoo API not installed.")
    except Exception as e:
        print(f"Moomoo fallback failed: {e}")

    # Final Fallback: Robinhood (if installed)
    if price == 0 and rh:
         print("Trying Robinhood fallback for price...")
         try:
             # Credentials (hardcoded for now as per get_robinhood_positions)
             username = "yptsai@gmail.com"
             password = "773250@Robinhood"
             
             # Avoid re-login if already logged in (check global or just try/except)
             try:
                 rh.get_account_info()
             except:
                 rh.login(username, password)
             
             rh_symbol = contract.symbol.replace(" ", ".")
             # BRK.B handling for RH
             if "BRK" in rh_symbol and "B" in rh_symbol and "BRK.B" not in rh_symbol:
                  pass # Already dots usually
                  
             quotes = rh.get_quotes(rh_symbol)
             if quotes and len(quotes) > 0:
                 q = quotes[0]
                 # Try last trade price, then last extended hours, then previous close
                 if 'last_trade_price' in q and q['last_trade_price']:
                      price = float(q['last_trade_price'])
                 elif 'last_extended_hours_trade_price' in q and q['last_extended_hours_trade_price']:
                      price = float(q['last_extended_hours_trade_price'])
                 elif 'previous_close' in q and q['previous_close']:
                      price = float(q['previous_close'])
                 
                 if price > 0:
                     print(f"Current Price (Robinhood): {price}")
                     return price
         except Exception as e:
             print(f"Robinhood price fetch failed: {e}")

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

def bs_put_theta(S, K, T, r, sigma):
    """計算 Put Theta (BS 模型，年化)"""
    if T <= 0 or sigma <= 0: return 0.0
    d1 = (log(S/K) + (r + 0.5 * sigma**2) * T) / (sigma * sqrt(T))
    d2 = d1 - sigma * sqrt(T)
    
    term1 = -(S * norm.pdf(d1) * sigma) / (2 * sqrt(T))
    term2 = r * K * exp(-r * T) * norm.cdf(-d2)
    return term1 + term2

def bs_call_theta(S, K, T, r, sigma):
    """計算 Call Theta (BS 模型，年化)"""
    if T <= 0 or sigma <= 0: return 0.0
    d1 = (log(S/K) + (r + 0.5 * sigma**2) * T) / (sigma * sqrt(T))
    d2 = d1 - sigma * sqrt(T)
    
    term1 = -(S * norm.pdf(d1) * sigma) / (2 * sqrt(T))
    term2 = r * K * exp(-r * T) * norm.cdf(d2)
    return term1 - term2

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

# Global cache for option prices to ensure consistency across tables
# Key: (symbol, expiry_YYYYMMDD, strike, right)
# Value: (price, src, iv, delta)
OPTION_PRICE_CACHE = {}

def get_unified_option_price(ib: IB, symbol: str, expiry_str: str, strike: float, right: str, moomoo_prices: Dict = None) -> Tuple[float, str, float, float]:
    """
    統一獲取選擇權價格與 Greeks (IV/Delta)
    優先級: IB Bid (IB) > IB Last (IL) > Moomoo (ML) > IB Close (IC)
    回傳: (price, source_label, iv, delta)
    """
    # Check global cache first
    cache_key = (symbol, expiry_str.replace("-", ""), strike, right)
    if cache_key in OPTION_PRICE_CACHE:
        return OPTION_PRICE_CACHE[cache_key]

    price = 0.0
    src = ""
    iv = 0.0
    delta = 0.0
    
    # 1. IB Data
    try:
        # Format expiry for IB: YYYYMMDD
        ib_expiry = expiry_str.replace("-", "")
        
        # Qualify contract
        temp_opt = Option(symbol, ib_expiry, strike, right, "SMART", CURRENCY, "100")
        ib.qualifyContracts(temp_opt)
        
        # Request streaming data (Snapshot=False for better reliability on delayed data)
        # Generic ticks: 100(Vol), 101(OI), 106(IV)
        ticker = ib.reqMktData(temp_opt, "100,101,106", False, False)
        
        # Wait briefly
        start_wait = datetime.datetime.now()
        while (datetime.datetime.now() - start_wait).total_seconds() < 2.0:
            ib.sleep(0.1)
            # Break early if we have good data
            if (ticker.bid and ticker.bid > 0) or (ticker.last and ticker.last > 0):
                break
                
        ib.cancelMktData(temp_opt)
        
        # Greeks from IB if available
        if ticker.modelGreeks:
            if ticker.modelGreeks.impliedVol: iv = ticker.modelGreeks.impliedVol
            if ticker.modelGreeks.delta: delta = ticker.modelGreeks.delta

        # Priority 1: Bid
        if ticker.bid and ticker.bid > 0:
            price = ticker.bid
            src = "IB" # IB Bid
        
        # Priority 2: Last (Only if Bid is missing or seemingly invalid? Let's stick to strict priority)
        # User said: "ib bid then moomoo last"
        # But commonly we check Last if Bid is 0.
        if price == 0 and ticker.last and ticker.last > 0:
            price = ticker.last
            src = "IL" # IB Last
            
        # Priority 4 (Last Resort): Close
        # We will check Moomoo before this
        ib_close = 0.0
        if ticker.close and ticker.close > 0:
            ib_close = ticker.close
            
    except Exception as e:
        # print(f"IB Price Error: {e}")
        pass

    # Priority 3: Moomoo (ML)
    if price == 0:
        # Try passed-in cache first
        if moomoo_prices:
             # Note: get_moomoo_quote_for_options returns YYYYMMDD keys
             # Ensure expiry_str is YYYYMMDD
             key_clean = expiry_str.replace("-", "")
             if (key_clean, strike) in moomoo_prices:
                 price = moomoo_prices[(key_clean, strike)]
                 src = "ML"
        
        # If still 0, try fetching single quote if needed (skip for bulk scan usually)
        # But for single position checks (RH/Moomoo positions), we might want to fetch here?
        # For now, let's assume bulk scan uses cache, and position checks might need direct fetch.
        pass

    # Priority 4: IB Close (IC) - apply if Moomoo also failed
    if price == 0 and 'ib_close' in locals() and ib_close > 0:
        price = ib_close
        src = "IC"

    # Save to global cache if we found a price
    if price > 0:
        OPTION_PRICE_CACHE[cache_key] = (price, src, iv, delta)

    return price, src, iv, delta


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
        # 使用 context manager 確保資源釋放，但 OpenQuoteContext 不一定支援，故維持手動 close
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
        ib.reqMarketDataType(MARKET_DATA_TYPE)
        tickers = []
        for opt in chunk:
            t = ib.reqMktData(opt, "100,101,106", False, False) 
            tickers.append((opt, t))
        
        # Wait for data to populate
        print("Waiting for option quotes...")
        for _ in range(40): 
            ib.sleep(0.25)
            # Check for generic tick availability to reduce error noise from trying to access empty fields
            valid_count = sum(1 for _, t in tickers if (t.bid and t.bid > 0) or (t.close and t.close > 0) or (t.last and t.last > 0))
            if valid_count > len(chunk) * 0.99: 
                break
                
        # --- Clean up IB Errors from stdout ---
        # Note: We can't easily suppress IB wrapper errors printed to stdout directly from here without deep hooks
        # But we can try to improve our own logging.
        
        today = datetime.date.today()
        
        for opt, ticker in tickers:
            ib.cancelMktData(opt)

            # --- 使用統一價格獲取邏輯 ---
            # 直接呼叫 get_unified_option_price (現在有 Cache 支援)
            # 傳遞 moomoo_prices 作為 Cache
            expiry_display = opt.lastTradeDateOrContractMonth # YYYYMMDD
            
            # 使用 unified 函數獲取價格，這會自動寫入全局 Cache
            # 注意: unified 函數內部會重新 qualify 一個 temp contract，這對於已有的 ticker 可能多此一舉
            # 為了效率，我們可以在這裡手動檢查 Cache，如果沒有才用 ticker 更新 Cache，或者直接使用 unified 函數
            # 考慮到我們已經有 ticker，我們應該手動模擬 unified 邏輯並更新 Cache
            
            final_price = 0.0
            price_src = ""
            iv = 0.0
            delta = 0.0
            theta = 0.0
            
            # Greeks
            if ticker.modelGreeks:
                if ticker.modelGreeks.impliedVol: iv = ticker.modelGreeks.impliedVol
                if ticker.modelGreeks.delta: delta = ticker.modelGreeks.delta
                if ticker.modelGreeks.theta: theta = ticker.modelGreeks.theta

            # Priority 1: IB Bid
            if ticker.bid and ticker.bid > 0:
                final_price = ticker.bid
                price_src = "IB"
            
            # Priority 2: IB Last
            if final_price == 0 and ticker.last and ticker.last > 0:
                final_price = ticker.last
                price_src = "IL"
            
            # Priority 3: Moomoo
            if final_price == 0:
                key = (opt.lastTradeDateOrContractMonth, opt.strike)
                if key in moomoo_prices and moomoo_prices[key] > 0:
                    final_price = moomoo_prices[key]
                    price_src = "ML"

            # Priority 4: IB Close
            if final_price == 0:
                if ticker.close and ticker.close > 0:
                    final_price = ticker.close
                    price_src = "IC"

            if final_price > 0:
                # Update Cache so later calls (Moomoo/RH positions) use this same price
                cache_key = (SYMBOL, expiry_display, opt.strike, opt.right)
                OPTION_PRICE_CACHE[cache_key] = (final_price, price_src, iv, delta)

            if final_price <= 0.05: 
                continue
                
            bid = final_price
                
            # 計算 DTE
            year = int(opt.lastTradeDateOrContractMonth[:4])
            month = int(opt.lastTradeDateOrContractMonth[4:6])
            day = int(opt.lastTradeDateOrContractMonth[6:])
            exp_date = datetime.date(year, month, day)
            dte = (exp_date - today).days
            if dte == 0: dte = 1
            T = dte / 365.0 # 年化時間
            
            # 交易成本
            fees_per_share = FEES_PER_CONTRACT / 100.0
            
            # 計算指標
            net_bid = bid - fees_per_share
            if net_bid < 0: net_bid = 0.0
            
            ret = net_bid / opt.strike
            annualized_ret = ret * (365 / dte)
            
            # Buffer (Safety Margin)
            # For Puts: (Price - Strike) / Price
            buffer_pct = 0.0
            if current_price > 0:
                buffer_pct = (current_price - opt.strike) / current_price
            
            target_bid_5pct = opt.strike * 0.05 * dte / 365.0
            
            # 補算 Greeks
            is_iv_estimated = False
            is_delta_estimated = False

            if iv == 0:
                 is_iv_estimated = True
                 iv = implied_volatility(bid, current_price, opt.strike, T, RISK_FREE_RATE)
            
            if delta == 0:
                 is_delta_estimated = True
                 delta = bs_put_delta(current_price, opt.strike, T, RISK_FREE_RATE, iv)
                 
            # 如果沒有從 IB 拿到 Theta，我們可以簡單估算 (BS Model Theta)
            if theta == 0:
                 # Theta is typically negative (decay). BS formula gives decay.
                 # Calculate Annual Theta
                 theta_annual = bs_put_theta(current_price, opt.strike, T, RISK_FREE_RATE, iv)
                 # Convert to Daily
                 theta = theta_annual / 365.0
            
            # Theta Yield (Daily Return from Time Decay)
            # Theta 通常是負數 (e.g., -0.05 代表每天跌 0.05)
            # 我們要看絕對值佔權利金的比例
            theta_yield = 0.0
            if bid > 0 and theta != 0:
                theta_yield = abs(theta) / bid
            
            # Probability of Profit (POP) roughly 1 - |Delta| for OTM options
            pop = 1.0 - abs(delta)
            if pop < 0: pop = 0

            # 計算理想價格
            target_kelly_allocation = 0.10
            target_fraction = target_kelly_allocation * 2
            conservative_vol = max(iv, hv_1w, hv_6m)
            required_exp_ret = target_fraction * (conservative_vol ** 2) + RISK_FREE_RATE
            
            abs_delta = abs(delta)
            if (1.0 - abs_delta) > 0.1:
                required_ann_ret = required_exp_ret / (1.0 - abs_delta)
            else:
                required_ann_ret = required_exp_ret / 0.1
                
            ideal_bid = opt.strike * required_ann_ret * (dte / 365.0)

            win_prob = 1.0 - abs_delta
            if win_prob < 0: win_prob = 0
            
            kelly_pct = calculate_kelly_fraction(annualized_ret, RISK_FREE_RATE, win_prob, iv, hv_1w, hv_6m)

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
                "pop": pop,
                "buffer": buffer_pct,
                "theta": theta,
                "theta_yield": theta_yield,
                "is_iv_est": is_iv_estimated,
                "is_delta_est": is_delta_estimated,
                "kelly": kelly_pct,
                "sharpe": sharpe,
                "conId": opt.conId
            })
            
    def sort_key(item):
        return item['kelly']

    return sorted(results, key=sort_key, reverse=True)

def get_moomoo_positions(ib: IB, ib_symbol: str, underlying_price: float, hv_1w: float, hv_6m: float) -> List[str]:
    """從 Moomoo 獲取持倉並回傳格式化後的報告字串列表 (延遲輸出)"""
    report_lines = []
    
    if OpenSecTradeContext is None:
        report_lines.append("\n[Moomoo] API package not found, skipping position check.")
        return report_lines

    report_lines.append(f"\n====== Moomoo Positions for {ib_symbol} ======")
    moomoo_host = "127.0.0.1"
    moomoo_port = 11111
    
    # 轉換 Symbol
    moomoo_symbol_base = ib_symbol.replace(" ", ".")
    if "US." not in moomoo_symbol_base:
        moomoo_symbol_base = "US." + moomoo_symbol_base

    try:
        # ... (connection logic) ...
        firm = getattr(SecurityFirm, 'FUTUINC', None) # Moomoo US
        if not firm:
             firm = getattr(SecurityFirm, 'FUTUSG', None)
        
        # print(f"Connecting to Moomoo with SecurityFirm: {firm}...") # Reduce noise
        
        if firm:
             trd_ctx = OpenSecTradeContext(filter_trdmarket=TrdMarket.US, host=moomoo_host, port=moomoo_port, security_firm=firm)
        else:
             trd_ctx = OpenSecTradeContext(filter_trdmarket=TrdMarket.US, host=moomoo_host, port=moomoo_port)

        ret, df = trd_ctx.position_list_query(trd_env=TrdEnv.REAL)
        trd_ctx.close()

        if ret != RET_OK:
            report_lines.append(f"Moomoo Error: {df}")
            return report_lines

        if df.empty:
            report_lines.append("No positions found in Moomoo.")
            return report_lines

        # 篩選相關持倉
        target = moomoo_symbol_base.upper().replace("US.", "") 
        filtered = df[df['code'].str.contains(target, case=False, regex=False)]
        
        if filtered.empty:
            simple_target = target.split('.')[0]
            filtered = df[df['code'].str.contains(simple_target, case=False, regex=False)]
            
        if filtered.empty:
            report_lines.append(f"No positions found for {ib_symbol} (Target: {target}).")
            return report_lines
        else:
            # 顯示 Raw Positions
            cols = ['code', 'stock_name', 'qty', 'cost_price', 'current_price', 'pl_ratio', 'nominal_price']
            cols = [c for c in cols if c in filtered.columns]
            filtered_sorted = filtered.sort_values('code')
            
            report_lines.append("--- Raw Positions (Sorted by Code/Expiry) ---")
            report_lines.append(filtered_sorted[cols].to_string(index=False))
            
            # ====== 分析持倉 ======
            report_lines.append("\n--- Position Analysis (Calculated based on Current Market Price) ---")
            report_lines.append("Note: 'Ann%' and 'Kelly%' are calculated as if opening a NEW position at current price.")
            report_lines.append(f"{'Expiry':<10} | {'DTE':<4} | {'Strike':<8} | {'Price(Src)':<12} | {'Qty':<4} | {'P/L%':<7} | {'Ann%':<6} | {'Ideal':<8} | {'Ratio':<6} | {'Shp':<4} | {'IV':<6} | {'Delta':<7} | {'Kelly%':<7}")
            report_lines.append("-" * 145)

            today = datetime.date.today()
            opt_pattern = re.compile(r'(\d{6})([CP])(\d+)$')

            position_rows = []

            for _, row in filtered.iterrows():
                code = row['code']
                match = opt_pattern.search(code)
                if not match:
                    continue
                
                date_str, opt_type, strike_str = match.groups()
                
                year = 2000 + int(date_str[:2])
                month = int(date_str[2:4])
                day = int(date_str[4:])
                exp_date = datetime.date(year, month, day)
                expiry_display = exp_date.strftime("%Y%m%d")
                
                strike = int(strike_str) / 1000.0
                
                dte = (exp_date - today).days
                if dte == 0: dte = 1
                T = dte / 365.0
                
                # --- 使用 Unified Price ---
                moomoo_fallback_price = 0.0
                if 'current_price' in row and row['current_price'] > 0:
                    moomoo_fallback_price = row['current_price']
                elif 'nominal_price' in row and row['nominal_price'] > 0:
                    moomoo_fallback_price = row['nominal_price']
                
                # Cache for unified function
                mm_cache = { (expiry_display, strike): moomoo_fallback_price }
                
                # Unified Call
                current_opt_price, price_src, iv, delta = get_unified_option_price(
                    ib, ib_symbol, expiry_display, strike, opt_type, mm_cache
                )
                
                if current_opt_price <= 0:
                    current_opt_price = moomoo_fallback_price
                    if current_opt_price > 0: price_src = "M(Raw)"

                # Metrics calculation...
                fees_per_share = FEES_PER_CONTRACT / 100.0
                net_bid = current_opt_price - fees_per_share
                if net_bid < 0: net_bid = 0.0
                
                ret = net_bid / strike
                annualized_ret = ret * (365 / dte)
                
                if iv == 0:
                    iv = implied_volatility(current_opt_price, underlying_price, strike, T, RISK_FREE_RATE, right=opt_type)
                
                if delta == 0:
                    if opt_type == 'C':
                        delta = bs_call_delta(underlying_price, strike, T, RISK_FREE_RATE, iv)
                    else:
                        delta = bs_put_delta(underlying_price, strike, T, RISK_FREE_RATE, iv)
                
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
                
                vol_risk = max(iv, hv_1w, hv_6m)
                sharpe = 0.0
                if vol_risk > 0.01:
                    sharpe = (annualized_ret - RISK_FREE_RATE) / vol_risk

                ratio = 0.0
                if ideal_bid > 0:
                    ratio = current_opt_price / ideal_bid
                
                pl_val = row.get('pl_ratio', 0.0) 
                pl_str = f"{pl_val:.1f}%"
                
                position_rows.append({
                    'expiry': expiry_display,
                    'exp_date_obj': exp_date,
                    'dte': dte,
                    'strike': strike,
                    'type': opt_type,
                    'price': current_opt_price,
                    'src': f"({price_src})",
                    'qty': row['qty'],
                    'pl_str': pl_str,
                    'ann': annualized_ret,
                    'ideal': ideal_bid,
                    'ratio': ratio,
                    'sharpe': sharpe,
                    'iv': iv,
                    'delta': delta,
                    'kelly': kelly_pct
                })

            position_rows.sort(key=lambda x: (0 if x['type']=='P' else 1, x['exp_date_obj'], x['strike']))

            last_type = None

            for row in position_rows:
                if last_type is not None and row['type'] != last_type:
                    report_lines.append("-" * 130)
                last_type = row['type']

                strike_display = f"{row['strike']}{row['type']}"
                price_disp = f"{row['price']:.2f}{row['src']}"

                row_str = f"{row['expiry']:<10} | {row['dte']:<4} | {strike_display:<8} | {price_disp:<12} | {row['qty']:<4} | {row['pl_str']:<7} | {row['ann']*100:<6.1f} | {row['ideal']:<8.2f} | {row['ratio']:<6.2f} | {row['sharpe']:<4.2f} | {row['iv']:<6.2f} | {row['delta']:>7.2f} | {row['kelly']*100:>6.1f}%"
                
                if row['ratio'] > 1.0:
                    abs_delta = abs(row['delta'])
                    row_color = COLOR_GREEN
                    if abs_delta < 0.20:
                        row_color = COLOR_RED
                    elif 0.20 <= abs_delta <= 0.30:
                        row_color = COLOR_GREEN
                    elif abs_delta > 0.30:
                        row_color = COLOR_BLUE
                        
                    report_lines.append(f"{row_color}{row_str}{COLOR_RESET}")
                else:
                    report_lines.append(row_str)

    except Exception as e:
        report_lines.append(f"Failed to fetch Moomoo positions: {e}")
        
    return report_lines

def get_robinhood_positions(ib: IB, ib_symbol: str, underlying_price: float, hv_1w: float, hv_6m: float):
    """從 Robinhood 獲取與該標的相關的持倉 (包含正股與選擇權) 並進行分析"""
    if rh is None:
        print("\n[Robinhood] API package not found (robin_stocks), skipping position check.")
        return

    print(f"\n====== Robinhood Positions for {ib_symbol} ======")
    
    # Credentials
    username = "yptsai@gmail.com"
    password = "773250@Robinhood"
    totp_key = None 

    # Symbol conversion: IB "BRK B" -> Robinhood "BRK.B"
    rh_symbol = ib_symbol.replace(" ", ".")
    if "BRK" in rh_symbol and "B" in rh_symbol and "BRK.B" not in rh_symbol:
         pass 

    # -------------------------------------
    # Helper to get unified price within RH loop
    # -------------------------------------
    def get_price_for_rh_position(ib: IB, ib_symbol: str, exp_date_str: str, strike: float, opt_type: str, rh_symbol: str):
        # YYYY-MM-DD -> YYYYMMDD
        expiry_display = exp_date_str.replace("-", "")
        
        # Moomoo fallback cache: Try to fetch single Moomoo quote if needed
        # But `get_unified_option_price` expects a dict cache.
        # We can construct a small one-item cache by fetching Moomoo price here if we want strictly consistent logic.
        # Or let `get_unified_option_price` handle the fallback if cache is missing (it currently doesn't fetch single, only uses cache).
        # Let's quickly fetch Moomoo price here to populate cache.
        
        mm_price = 0.0
        try:
            # Re-use the logic from old fetch_moomoo_price_fallback
            # Or better: make a small helper that returns the price
            from moomoo import OpenQuoteContext, RET_OK, SubType
            moomoo_host = "127.0.0.1"
            moomoo_port = 11111
            
            # Date: 2026-01-16 -> 260116
            yy = exp_date_str[2:4]
            mm = exp_date_str[5:7]
            dd = exp_date_str[8:10]
            date_part = f"{yy}{mm}{dd}"
            strike_int = int(strike * 1000)
            strike_part = f"{strike_int:08d}"
            
            base_sym = rh_symbol.replace(" ", ".")
            if "BRK.B" in base_sym: base_sym = "BRKB"
            
            full_code = f"US.{base_sym}{date_part}{opt_type}{strike_part}"
            
            q_ctx = OpenQuoteContext(host=moomoo_host, port=moomoo_port)
            q_ctx.subscribe([full_code], [SubType.QUOTE], subscribe_push=False)
            ret, df = q_ctx.get_stock_quote([full_code])
            q_ctx.close()
            
            if ret == RET_OK and not df.empty:
                row = df.iloc[0]
                if 'last_price' in row and row['last_price'] > 0:
                    mm_price = float(row['last_price'])
                elif 'nominal_price' in row and row['nominal_price'] > 0:
                    mm_price = float(row['nominal_price'])
        except: pass

        mm_cache = {(expiry_display, strike): mm_price}
        
        return get_unified_option_price(ib, ib_symbol, expiry_display, strike, opt_type, mm_cache)


    try:
        # Login
        if totp_key:
            totp = pyotp.TOTP(totp_key).now()
            rh.login(username, password, mfa_code=totp)
        else:
            rh.login(username, password)
            
        # 1. Stocks
        holdings = rh.build_holdings()
        
        if rh_symbol in holdings:
            data = holdings[rh_symbol]
            print("--- Stock Position ---")
            print(f"Symbol: {rh_symbol}")
            print(f"Qty: {data.get('quantity')}")
            print(f"Avg Buy Price: {data.get('average_buy_price')}")
            print(f"Current Price: {data.get('price')}")
            print(f"Equity: {data.get('equity')}")
            print(f"Percent Change: {data.get('percent_change')}%")
            print(f"Equity Change: {data.get('equity_change')}")
        else:
             print(f"No stock position found for {rh_symbol}.")

        # 2. Options
        print("\n--- Option Positions ---")
        
        option_positions = rh.get_open_option_positions()
        
        if not option_positions:
            try:
                all_positions = rh.get_all_option_positions()
                option_positions = [p for p in all_positions if float(p.get('quantity', 0)) > 0]
            except AttributeError:
                pass
            except Exception as e:
                pass # Silently fail fallback

        filtered_opts = []
        for p in option_positions:
            chain_sym = p.get('chain_symbol', '')
            if chain_sym == rh_symbol:
                filtered_opts.append(p)
            elif chain_sym.replace(".", "") == rh_symbol.replace(".", ""):
                filtered_opts.append(p)
            elif chain_sym == ib_symbol:
                 filtered_opts.append(p)

        if not filtered_opts:
            print(f"No option positions found for {rh_symbol} (IB: {ib_symbol}).")
        else:
            today = datetime.date.today()
            
            position_rows = []

            for op in filtered_opts:
                fetch_missing_option_details(rh, op)

                if 'strike_price' not in op: continue

                strike = float(op['strike_price'])
                opt_type = op['type'].upper()
                if opt_type == 'CALL': opt_type = 'C'
                if opt_type == 'PUT': opt_type = 'P'
                
                exp_date_str = op['expiration_date']
                year, month, day = map(int, exp_date_str.split('-'))
                exp_date = datetime.date(year, month, day)
                expiry_display = exp_date.strftime("%Y%m%d")
                
                dte = (exp_date - today).days
                if dte == 0: dte = 1
                T = dte / 365.0
                
                qty = float(op['quantity'])
                avg_cost = float(op['average_price'])
                
                # --- Unified Price ---
                current_opt_price, price_src_label, iv, delta = get_price_for_rh_position(
                    ib, ib_symbol, exp_date_str, strike, opt_type, rh_symbol
                )
                
                # Fallback to RH Mark Price if unified returned 0
                if current_opt_price == 0:
                    market_data = None
                    if 'option' in op:
                        try:
                            instr_url = op.get('option', '')
                            if instr_url:
                                instr_id = instr_url.rstrip('/').split('/')[-1]
                                market_data = rh.get_option_market_data_by_id(instr_id)
                        except: pass
                    
                    if not market_data:
                         market_data = rh.get_option_market_data(rh_symbol, exp_date_str, strike, op['type'])
                        
                    if market_data:
                        if isinstance(market_data, list) and len(market_data) > 0: md = market_data[0]
                        elif isinstance(market_data, dict): md = market_data
                        
                        if 'adjusted_mark_price' in md:
                             current_opt_price = float(md['adjusted_mark_price'])
                             price_src_label = "R" # RH Mark
                        
                        if iv == 0 and 'implied_volatility' in md and md['implied_volatility']:
                             try: iv = float(md['implied_volatility'])
                             except: pass
                        if delta == 0 and 'delta' in md and md['delta']:
                             try: delta = float(md['delta'])
                             except: pass

                # Metrics
                fees_per_share = FEES_PER_CONTRACT / 100.0
                net_bid = current_opt_price - fees_per_share
                if net_bid < 0: net_bid = 0.0
                
                ret = net_bid / strike
                annualized_ret = ret * (365 / dte)
                
                if iv == 0:
                     iv = implied_volatility(current_opt_price, underlying_price, strike, T, RISK_FREE_RATE, right=opt_type)
                if delta == 0:
                     if opt_type == 'C':
                        delta = bs_call_delta(underlying_price, strike, T, RISK_FREE_RATE, iv)
                     else:
                        delta = bs_put_delta(underlying_price, strike, T, RISK_FREE_RATE, iv)

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
                
                # Sharpe
                vol_risk = max(iv, hv_1w, hv_6m)
                sharpe = 0.0
                if vol_risk > 0.01:
                    sharpe = (annualized_ret - RISK_FREE_RATE) / vol_risk

                ratio = 0.0
                if ideal_bid > 0:
                    ratio = current_opt_price / ideal_bid
                    
                pl_pct = 0.0
                if avg_cost > 0:
                    # Note: avg_cost in RH is positive. price is positive. 
                    # If long (qty>0), profit if price > cost.
                    # If short (qty<0), profit if price < cost.
                    if qty > 0:
                        pl_pct = (current_opt_price - avg_cost) / avg_cost
                    else:
                        # Short position: Sold at avg_cost, buy back at current_price.
                        # Profit = (Entry - Exit) / Entry
                        # Entry = avg_cost, Exit = current_opt_price
                        pl_pct = (avg_cost - current_opt_price) / avg_cost
                
                position_rows.append({
                    'expiry': expiry_display,
                    'exp_date_obj': exp_date,
                    'dte': dte,
                    'strike': strike,
                    'type': opt_type,
                    'price': current_opt_price,
                    'src': f"({price_src_label})",
                    'qty': qty,
                    'pl_pct': pl_pct,
                    'ann': annualized_ret,
                    'ideal': ideal_bid,
                    'ratio': ratio,
                    'sharpe': sharpe,
                    'iv': iv,
                    'delta': delta,
                    'kelly': kelly_pct
                })

            # Sort P then C
            position_rows.sort(key=lambda x: (0 if x['type']=='P' else 1, x['exp_date_obj'], x['strike']))

            # Header matching Moomoo
            print(f"{'Expiry':<10} | {'DTE':<4} | {'Strike':<8} | {'Price(Src)':<12} | {'Qty':<4} | {'P/L%':<7} | {'Ann%':<6} | {'Ideal':<8} | {'Ratio':<6} | {'Shp':<4} | {'IV':<6} | {'Delta':<7} | {'Kelly%':<7}")
            print("-" * 145)

            last_type = None
            for row in position_rows:
                if last_type is not None and row['type'] != last_type:
                    print("-" * 130)
                last_type = row['type']
                
                price_disp = f"{row['price']:.2f}{row['src']}"
                strike_disp = f"{row['strike']}{row['type']}"
                
                pl_str = f"{row['pl_pct']*100:.1f}%"
                
                row_str = f"{row['expiry']:<10} | {row['dte']:<4} | {strike_disp:<8} | {price_disp:<12} | {row['qty']:<4.1f} | {pl_str:<7} | {row['ann']*100:<6.1f} | {row['ideal']:<8.2f} | {row['ratio']:<6.2f} | {row['sharpe']:<4.2f} | {row['iv']:<6.2f} | {row['delta']:>7.2f} | {row['kelly']*100:>6.1f}%"
                
                if row['ratio'] > 1.0:
                    abs_d = abs(row['delta'])
                    row_color = COLOR_GREEN
                    if abs_d < 0.20: row_color = COLOR_RED
                    elif 0.20 <= abs_d <= 0.30: row_color = COLOR_GREEN
                    elif abs_d > 0.30: row_color = COLOR_BLUE
                    print(f"{row_color}{row_str}{COLOR_RESET}")
                else:
                    print(row_str)

    except Exception as e:
        print(f"Robinhood Error: {e}")

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

        # 6. 查詢 Moomoo 持倉 (Fetch BEFORE Printing)
        print("\nFetching Moomoo positions (please wait)...") 
        moomoo_report_lines = get_moomoo_positions(ib, SYMBOL, price, hv_1w, hv_6m)

        # 7. 查詢 Robinhood 持倉 (Fetch BEFORE Printing)
        print("Fetching Robinhood positions (please wait)...") 
        rh_report_lines = get_robinhood_positions(ib, SYMBOL, price, hv_1w, hv_6m)
        
        # --- ALL FETCHING DONE ---
        # Disconnect here to prevent further IB async error messages from interrupting printing
        ib.disconnect()
        print("\nData fetching complete. Generating report...\n")

        # 5. 輸出結果 (Now Printing ALL tables)
        print(f"\n====== Sell Put Recommendations for {SYMBOL} (Price: {price}) ======")
        print(f"Risk Free Rate: {RISK_FREE_RATE:.1%}")
        print(f"Risk Metrics: IV(Implied) vs HV(Hist): 1W={hv_1w:.1%}, 6M={hv_6m:.1%}")
        print("Note: Ideal Bid assumes a target Kelly allocation of 10% (Half-Kelly).")
        print("Note: Price Source: B=Bid, L=Last, C=Close. 'L'/'C' may be stale!")
        print("Note: Th% = Daily Theta Decay / Bid Price.")
        print("WARNING: '*' indicates Greeks (IV/Delta) are ESTIMATED due to delayed data.")
        print(f"{'Expiry':<10} | {'DTE':<4} | {'Strike':<8} | {'Bid(Src)':<16} | {'Ret%':<6} | {'Ann%':<6} | {'Ideal':<8} | {'Ratio':<6} | {'Shp':<4} | {'IV':<6} | {'HV':<6} | {'Delta':<7} | {'Th%':<5} | {'Kelly%':<7}")
        print("-" * 158)
        
        top_n = NUM_OPTIONS_TO_SHOW
        for res in ranked_results[:top_n]:
            iv_str = f"{res['iv']:.2f}" + ("*" if res['is_iv_est'] else "")
            delta_str = f"{res['delta']:.2f}" + ("*" if res['is_delta_est'] else "")
            
            hv_val = max(hv_1w, hv_6m)
            hv_str = f"{hv_val:.2f}"
            
            # Theta String
            theta_pct_str = "-"
            if res.get('theta_yield'):
                theta_pct_str = f"{res['theta_yield']*100:.1f}%"

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
            row_str = f"{res['expiry']:<10} | {res['dte']:<4} | {res['strike']:<8.1f} | {price_field} | {res['return']*100:<6.1f} | {res['annualized']*100:<6.1f} | {res['ideal_bid']:<8.2f} | {ratio:<6.2f} | {res['sharpe']:<4.2f} | {iv_str:<6} | {hv_str:<6} | {delta_str:>7} | {theta_pct_str:<5} | {res['kelly']*100:>6.1f}%"

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
        
        # 統一輸出所有持倉報告，避免中間夾雜 fetch logs
        # 此處輸出的錯誤訊息來自於 fetch 過程中的 IB 錯誤，若要完全乾淨，可能需要自定義 EWrapper
        
        # Output Moomoo Report
        if moomoo_report_lines:
            for line in moomoo_report_lines:
                print(line)
                
        # Output Robinhood Report
        if rh_report_lines:
            for line in rh_report_lines:
                print(line)

    finally:
        if ib.isConnected():
            ib.disconnect()
            print("\nDisconnected.")

if __name__ == "__main__":
    main()
