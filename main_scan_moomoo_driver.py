#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import sys
import argparse
import datetime
import math
import statistics
import re
import time
from typing import List, Optional, Dict, Tuple
from ib_insync import IB, Stock, Option, util

# Attempt imports for Moomoo
try:
    from moomoo import OpenSecTradeContext, TrdMarket, TrdEnv, SecurityFirm, RET_OK, OptionType, OpenQuoteContext, SubType
except ImportError:
    OpenSecTradeContext = None
    OpenQuoteContext = None
    OptionType = None

try:
    import robin_stocks.robinhood as rh
    import pyotp
except ImportError:
    rh = None
    pyotp = None

# ====== 設定 ======
HOST = "127.0.0.1"
PORT = 7496
CLIENT_ID = 14  # Use a unique client ID

# Target List
TARGET_ETFS = ["SPY", "QQQ", "IWM", "DIA", "GLD", "SLV", "TLT", "HYG", "EEM", "FXI"]
TARGET_STOCKS = [
    "AAPL", "MSFT", "AMZN", "GOOGL", "META", "TSLA", "NVDA", "BRK B", "JPM", "JNJ", 
    "V", "PG", "MA", "UNH", "HD", "CVX", "MRK", "ABBV", "KO", "PEP", 
    "BAC", "AVGO", "COST", "TMO", "MCD", "CSCO", "WMT", "XOM", "DIS", "PFE", 
    "VZ", "CMCSA", "ADBE", "NKE", "INTC", "WFC", "CRM", "ACN", "AMD", "NFLX"
]
ALL_TARGETS = TARGET_ETFS + TARGET_STOCKS

PRIMARY_EXCHANGE = "NYSE"
CURRENCY = "USD"

# 篩選條件
MIN_DTE = 7           
MAX_DTE = 60           
STRIKE_PCT_MIN = 0.88  
STRIKE_PCT_MAX = 0.99  
MIN_ANNUAL_RETURN = 0.05 
MIN_SHARPE_RATIO = 0.2   

MARKET_DATA_TYPE = 3 
RISK_FREE_RATE = 0.033

# 交易費用設定
TRANS_FEE_ACTIVITY = 0.01      
TRANS_FEE_REGULATORY = 0.01 * 2 
TRANS_FEE_OCC = 0.03 * 2       
TRANS_FEE_COMMISSION = 0.65    
FEES_PER_CONTRACT = TRANS_FEE_ACTIVITY + TRANS_FEE_REGULATORY + TRANS_FEE_OCC + TRANS_FEE_COMMISSION

# ANSI Colors
COLOR_RED = "\033[91m"
COLOR_GREEN = "\033[92m"
COLOR_YELLOW = "\033[93m"
COLOR_BLUE = "\033[94m"
COLOR_RESET = "\033[0m"

NUM_OPTIONS_TO_SHOW = 40

# --- Black-Scholes Utility Functions ---
from math import log, sqrt, exp
from scipy.stats import norm

def bs_put_price(S, K, T, r, sigma):
    if T <= 0 or sigma <= 0: return 0.0
    d1 = (log(S/K) + (r + 0.5 * sigma**2) * T) / (sigma * sqrt(T))
    d2 = d1 - sigma * sqrt(T)
    return K * exp(-r * T) * norm.cdf(-d2) - S * norm.cdf(-d1)

def bs_put_delta(S, K, T, r, sigma):
    if T <= 0 or sigma <= 0: return 0.0
    d1 = (log(S/K) + (r + 0.5 * sigma**2) * T) / (sigma * sqrt(T))
    return norm.cdf(d1) - 1.0

def bs_put_theta(S, K, T, r, sigma):
    if T <= 0 or sigma <= 0: return 0.0
    d1 = (log(S/K) + (r + 0.5 * sigma**2) * T) / (sigma * sqrt(T))
    d2 = d1 - sigma * sqrt(T)
    term1 = -(S * norm.pdf(d1) * sigma) / (2 * sqrt(T))
    term2 = r * K * exp(-r * T) * norm.cdf(-d2)
    return term1 + term2

def implied_volatility(price, S, K, T, r):
    if price <= 0: return 0.0
    low = 0.001
    high = 5.0
    for _ in range(50):
        mid = (low + high) / 2
        est_price = bs_put_price(S, K, T, r, mid)
        if abs(est_price - price) < 0.001: return mid
        if est_price < price: low = mid
        else: high = mid
    return (low + high) / 2

def calculate_kelly_fraction(annualized_ret, risk_free_rate, win_prob, iv, hv_short, hv_long):
    expected_annual_ret = annualized_ret * win_prob
    if expected_annual_ret <= risk_free_rate: return 0.0
    sigma = max(iv, hv_short, hv_long)
    if sigma < 0.1: sigma = 0.1
    fraction = (expected_annual_ret - risk_free_rate) / (sigma ** 2)
    return max(0, fraction * 0.5)

# --- Core Functions ---

def connect_ib() -> IB:
    ib = IB()
    print(f"Connecting to TWS/IBG {HOST}:{PORT} clientId={CLIENT_ID} ...")
    try:
        ib.connect(HOST, PORT, clientId=CLIENT_ID, timeout=10)
    except Exception as e:
        print(f"Connection failed: {e}")
        sys.exit(1)
    return ib

def qualify_stock(ib: IB, symbol: str) -> Stock:
    stk = Stock(symbol, "SMART", currency=CURRENCY)
    if symbol == "BRK B" or symbol == "BRK.B":
        stk.symbol = "BRK B"
        stk.primaryExchange = "NYSE"
    
    try:
        ib.qualifyContracts(stk)
        print(f"Qualified: {stk.symbol} (ID: {stk.conId})")
        return stk
    except Exception as e:
        print(f"Failed to qualify {symbol}: {e}")
        return None

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

def get_historical_volatility(ib: IB, contract: Stock) -> Tuple[float, float]:
    end_date = ""
    bars = ib.reqHistoricalData(contract, endDateTime=end_date, durationStr="6 M", barSizeSetting="1 day", whatToShow="TRADES", useRTH=1, formatDate=1, keepUpToDate=False)
    
    if not bars: return 0.18, 0.18
    
    closes = [b.close for b in bars]
    log_returns = []
    for i in range(1, len(closes)):
        if closes[i-1] > 0: log_returns.append(math.log(closes[i] / closes[i-1]))
        else: log_returns.append(0.0)
            
    if not log_returns: return 0.18, 0.18

    def calc_hv(returns):
        if len(returns) < 2: return 0.0
        return statistics.stdev(returns) * math.sqrt(252)
    
    hv_6m = calc_hv(log_returns)
    hv_1w = calc_hv(log_returns[-5:]) if len(log_returns) >= 5 else hv_6m
    return hv_1w, hv_6m

def get_iv_rank_data(ib: IB, contract: Stock) -> Tuple[float, float, float]:
    try:
        bars = ib.reqHistoricalData(contract, endDateTime='', durationStr='1 Y', barSizeSetting='1 day', whatToShow='OPTION_IMPLIED_VOLATILITY', useRTH=1, formatDate=1, keepUpToDate=False)
        if not bars: return 0.0, 0.0, 0.0
        closes = [b.close for b in bars]
        return min(closes), max(closes), closes[-1]
    except:
        return 0.0, 0.0, 0.0

def get_next_earnings_date(ib: IB, contract: Stock) -> Optional[datetime.date]:
    try:
        xml_str = ib.reqFundamentalData(contract, 'CalendarReport')
        if not xml_str: return None
        import xml.etree.ElementTree as ET
        root = ET.fromstring(xml_str)
        today = datetime.date.today()
        for earnings in root.findall(".//Earnings"):
            date_elem = earnings.find("Date")
            if date_elem is not None and date_elem.text:
                try:
                    e_date = datetime.datetime.strptime(date_elem.text, "%Y-%m-%d").date()
                    if e_date >= today: return e_date
                except: pass
    except: pass
    return None

def get_moomoo_option_chain_and_quotes(ib_symbol: str, current_price: float, min_dte: int, max_dte: int) -> List[Dict]:
    """
    1. 從 Moomoo 獲取 Option Chain (Put)
    2. 篩選符合日期和 Strike 範圍的合約
    3. 批量獲取 Moomoo 報價 (作為基底)
    """
    if OpenQuoteContext is None: return []
    
    moomoo_host = "127.0.0.1"
    moomoo_port = 11111
    
    moomoo_base = ib_symbol.replace(" ", ".")
    if "US." not in moomoo_base: moomoo_base = "US." + moomoo_base
    adjusted_base = moomoo_base.replace("BRK.B", "BRKB") if "BRK.B" in moomoo_base else moomoo_base
    
    today = datetime.date.today()
    start_date = today + datetime.timedelta(days=min_dte)
    end_date = today + datetime.timedelta(days=max_dte)
    
    target_strike_min = current_price * STRIKE_PCT_MIN
    target_strike_max = current_price * STRIKE_PCT_MAX
    
    candidates = [] # List of dicts: {code, expiry, strike, price, src}
    
    try:
        quote_ctx = OpenQuoteContext(host=moomoo_host, port=moomoo_port)
        current_start = start_date
        all_codes = []
        
        # 1. Fetch Chain Codes
        print(f"Fetching Moomoo Option Chain for {ib_symbol}...")
        while current_start <= end_date:
            current_end = min(current_start + datetime.timedelta(days=29), end_date)
            s_str = current_start.strftime("%Y-%m-%d")
            e_str = current_end.strftime("%Y-%m-%d")
            
            ret, df_chain = quote_ctx.get_option_chain(code=adjusted_base, start=s_str, end=e_str, option_type=OptionType.PUT)
            if (ret != RET_OK or df_chain.empty) and adjusted_base != moomoo_base:
                ret, df_chain = quote_ctx.get_option_chain(code=moomoo_base, start=s_str, end=e_str, option_type=OptionType.PUT)
            
            if ret == RET_OK and not df_chain.empty:
                # Filter by Strike here to reduce quota usage
                # Moomoo chain df usually has 'strike_price' column? Check docs.
                # Assuming simple list of codes first, filtering by code parsing is safer if column not reliable
                all_codes.extend(df_chain['code'].tolist())
                
            current_start = current_end + datetime.timedelta(days=1)
            
        all_codes = list(set(all_codes))
        
        # 2. Filter Codes by Strike & Parse Details
        valid_codes = []
        code_details = {} # code -> (expiry_yyyymmdd, strike)
        
        for code in all_codes:
            # Parse: US.BRKB260123P485000
            match = re.search(r'(\d{6})([P])(\d+)$', code)
            if match:
                d_str, t, s_str = match.groups()
                strike = int(s_str) / 1000.0
                expiry = "20" + d_str
                
                if target_strike_min <= strike <= target_strike_max:
                    valid_codes.append(code)
                    code_details[code] = (expiry, strike)
        
        print(f"Found {len(valid_codes)} valid contracts in range.")
        if not valid_codes:
            quote_ctx.close()
            return []

        # 3. Batch Fetch Moomoo Quotes
        chunk_size = 100
        for i in range(0, len(valid_codes), chunk_size):
            batch = valid_codes[i:i+chunk_size]
            quote_ctx.subscribe(batch, [SubType.QUOTE], subscribe_push=False)
            ret_q, df_quote = quote_ctx.get_stock_quote(batch)
            
            if ret_q == RET_OK and not df_quote.empty:
                for _, row in df_quote.iterrows():
                    code = row['code']
                    val = 0.0
                    src = ""
                    if row.get('last_price') and row['last_price'] > 0:
                        val = row['last_price']
                        src = "ML" # Moomoo Last
                    elif row.get('nominal_price') and row['nominal_price'] > 0:
                        val = row['nominal_price']
                        src = "MN" # Moomoo Nominal
                        
                    if val > 0 and code in code_details:
                        exp, strike = code_details[code]
                        candidates.append({
                            "symbol": ib_symbol,
                            "expiry": exp, # YYYYMMDD
                            "strike": strike,
                            "price": val,
                            "src": src,
                            "right": 'P'
                        })
            quote_ctx.unsubscribe(batch, [SubType.QUOTE])
            
        quote_ctx.close()
        
    except Exception as e:
        print(f"Moomoo error: {e}")
        
    return candidates

def enrich_with_ib_data(ib: IB, candidates: List[Dict], current_price: float, hv_1w: float, hv_6m: float, iv_rank_info: Tuple, earnings_date) -> List[Dict]:
    """
    使用 IB 獲取更精確的報價 (Bid優先) 和 Greeks
    邏輯:
    1. 用 Moomoo 抓到的 candidates 列表建立 IB Contracts
    2. 請求 IB 報價
    3. Update Price: IB Bid > IB Last > Moomoo (Keep) > IB Close
    4. Update Greeks: IB Model > BS Calc
    """
    results = []
    today = datetime.date.today()
    iv_low, iv_high, iv_curr = iv_rank_info
    
    # Batch processing
    chunk_size = 50
    skip_ib_enrichment = False
    
    for i in range(0, len(candidates), chunk_size):
        chunk = candidates[i:i+chunk_size]
        
        # If IB failed previously, skip IB requests and just use Moomoo data
        if skip_ib_enrichment:
            for c in chunk:
                # Use Moomoo data directly
                res = create_result_entry(c, c['price'], c['src'], 0, 0, 0, 0, current_price, today, hv_1w, hv_6m, iv_low, iv_high, earnings_date)
                if res: results.append(res)
            continue

        # Create IB Contracts
        ib_contracts = []
        for c in chunk:
            # c has: symbol, expiry(YYYYMMDD), strike, price, src
            # Use keyword arguments to avoid positional args mismatch (especially multiplier vs currency)
            contract = Option(
                symbol=c['symbol'], 
                lastTradeDateOrContractMonth=c['expiry'], 
                strike=c['strike'], 
                right='P', 
                exchange='SMART', 
                currency=CURRENCY,
                multiplier='100' 
            )
            ib_contracts.append((c, contract))
            
        # Qualify (Might fail for some if not in IB, but we try)
        contracts_only = [p[1] for p in ib_contracts]
        ib.qualifyContracts(*contracts_only)
        
        qualified_count = sum(1 for c in contracts_only if c.conId > 0)
        print(f"  Batch {i//chunk_size + 1}: Qualified {qualified_count}/{len(contracts_only)} contracts.")

        # Request Data
        ib.reqMarketDataType(MARKET_DATA_TYPE)
        tickers = []
        for _, contract in ib_contracts:
            if contract.conId > 0: # Only valid contracts
                # Use Streaming (snapshot=False) to match main_union.py behavior and avoid subscription errors
                t = ib.reqMktData(contract, "", False, False) 
                tickers.append((contract, t))
            else:
                tickers.append((contract, None))
        
        # Wait
        print(f"  Fetching IB data (Streaming)...")
        got_any_ib_data = False
        for _ in range(20): # Wait up to 4-5s
            ib.sleep(0.25)
            # Check for > 0 to avoid NaN issues
            valid = sum(1 for _, t in tickers if t and ((t.bid and t.bid > 0) or (t.last and t.last > 0) or (t.close and t.close > 0)))
            if valid > 0: got_any_ib_data = True
            if valid >= len(tickers) * 0.9: break
            
        # Check if we should disable IB enrichment
        if not got_any_ib_data and qualified_count > 0:
            print("  ⚠️ No IB data received. Disabling IB enrichment for remaining batches to save time.")
            skip_ib_enrichment = True
            
        # Process Results
        for (base_data, _), (contract, ticker) in zip(ib_contracts, tickers):
            if ticker: ib.cancelMktData(contract)
            
            # Start with Moomoo data
            final_price = base_data['price']
            final_src = base_data['src']
            iv = 0.0
            delta = 0.0
            theta = 0.0
            
            # IB Data available?
            if ticker:
                # Greeks
                if ticker.modelGreeks:
                    if ticker.modelGreeks.impliedVol: iv = ticker.modelGreeks.impliedVol
                    if ticker.modelGreeks.delta: delta = ticker.modelGreeks.delta
                    if ticker.modelGreeks.theta: theta = ticker.modelGreeks.theta
                
                # Price Logic: IB Bid > IB Last > IB Close > Moomoo
                if ticker.bid and ticker.bid > 0:
                    final_price = ticker.bid
                    final_src = "IB"
                elif ticker.last and ticker.last > 0:
                    final_price = ticker.last
                    final_src = "IL"
                elif ticker.close and ticker.close > 0:
                    final_price = ticker.close
                    final_src = "IC"
                # If IB has no price, we keep Moomoo's price
            
            res = create_result_entry(base_data, final_price, final_src, iv, delta, theta, 0, current_price, today, hv_1w, hv_6m, iv_low, iv_high, earnings_date)
            if res: results.append(res)
            
    return sorted(results, key=lambda x: x['kelly'], reverse=True)

def create_result_entry(base_data, final_price, final_src, iv, delta, theta, theta_yield, current_price, today, hv_1w, hv_6m, iv_low, iv_high, earn_date):
    if final_price <= 0.05: return None
    
    # Calculations
    exp_date = datetime.datetime.strptime(base_data['expiry'], "%Y%m%d").date()
    dte = (exp_date - today).days
    if dte == 0: dte = 1
    T = dte / 365.0
    
    net_bid = max(0, final_price - FEES_PER_CONTRACT/100.0)
    ret = net_bid / base_data['strike']
    annualized_ret = ret * (365 / dte)
    
    # BS Fallbacks
    if iv == 0: iv = implied_volatility(final_price, current_price, base_data['strike'], T, RISK_FREE_RATE)
    if delta == 0: delta = bs_put_delta(current_price, base_data['strike'], T, RISK_FREE_RATE, iv)
    if theta == 0: theta = bs_put_theta(current_price, base_data['strike'], T, RISK_FREE_RATE, iv) / 365.0
    
    if final_price > 0:
        theta_yield = abs(theta) / final_price
    
    # Advanced Metrics
    target_kelly = 0.10
    conservative_vol = max(iv, hv_1w, hv_6m)
    req_ret = target_kelly * 2 * (conservative_vol**2) + RISK_FREE_RATE
    abs_delta = abs(delta)
    req_ann = req_ret / (1.0 - abs_delta) if (1.0 - abs_delta) > 0.1 else req_ret / 0.1
    ideal_bid = base_data['strike'] * req_ann * (dte/365.0)
    
    win_prob = max(0, 1.0 - abs_delta)
    kelly = calculate_kelly_fraction(annualized_ret, RISK_FREE_RATE, win_prob, iv, hv_1w, hv_6m)
    
    vol_risk = max(iv, hv_1w, hv_6m)
    sharpe = (annualized_ret - RISK_FREE_RATE) / vol_risk if vol_risk > 0.01 else 0.0
    
    ivr = 0.0
    if iv_high > iv_low: ivr = (iv - iv_low) / (iv_high - iv_low) * 100
    
    expected_move = current_price * iv * math.sqrt(T)
    safety = (current_price - base_data['strike']) / expected_move if expected_move > 0 else 99.9
    
    td_ratio = abs(theta) / abs(delta) if abs(delta) > 0.001 else 0.0
    
    earn = False
    if earn_date:
        if today <= earn_date <= exp_date: earn = True
    
    return {
        "symbol": base_data['symbol'], "expiry": base_data['expiry'], "strike": base_data['strike'],
        "bid": final_price, "price_src": final_src, "dte": dte, "annualized": annualized_ret,
        "ideal_bid": ideal_bid, "iv": iv, "delta": delta, "theta": theta,
        "kelly": kelly, "sharpe": sharpe, "ivr": ivr, "safety": safety,
        "td_ratio": td_ratio, "earn": earn, "theta_yield": theta_yield,
        "hv": max(hv_1w, hv_6m),
        "is_iv_est": iv == 0 or iv is None, 
        "is_delta_est": delta == 0 or delta is None
    }

def append_to_html(filename: str, results: List[Dict], iv_rank_info: Tuple, hv_1w: float, hv_6m: float):
    """
    Append formatted results to an HTML file.
    Note: Since we are appending symbol by symbol, we need a structure that allows multiple tables or one big table.
    For simplicity, we will create one big table and append rows.
    However, HTML structure needs </body> at the end. 
    Streaming HTML is tricky. We'll append <tbody> rows.
    The main() function will handle the opening and closing tags of the file.
    """
    
    with open(filename, "a") as f:
        # Section Header
        if results:
            symbol = results[0]['symbol']
            iv_curr = iv_rank_info[2]
            f.write(f"""
            <tr class="section-header">
                <td colspan="17">
                    <strong>{symbol}</strong> 
                    <span style="font-size:0.9em; margin-left:10px;">
                        (IV: {iv_curr:.2f}, HV1W: {hv_1w:.1%}, HV6M: {hv_6m:.1%})
                    </span>
                </td>
            </tr>
            """)

        for res in results:
            # Logic for styling
            pass_ivr = res['ivr'] > 30
            pass_ann = res['annualized'] > 0.15
            pass_td = res['td_ratio'] > 0.4
            pass_delta = abs(res['delta']) < 0.3
            pass_iv_hv = res['iv'] > res['hv']
            
            is_perfect = pass_ivr and pass_ann and pass_td and pass_delta and pass_iv_hv
            
            row_class = "perfect-row" if is_perfect else ""
            if res['earn']: row_class += " earn-row"
            
            def cell_style(condition):
                return 'class="pass"' if condition else ''
            
            # Format values
            expiry = res['expiry']
            dte = res['dte']
            strike = f"{res['strike']:.1f}"
            bid_display = f"{res['bid']:.2f}<span class='src'>({res['price_src']})</span>"
            
            ann_val = res['annualized'] * 100
            ann_str = f"{ann_val:.1f}"
            
            ideal = f"{res['ideal_bid']:.2f}"
            
            ratio = res['bid']/res['ideal_bid'] if res['ideal_bid'] > 0 else 0
            ratio_str = f"{ratio:.2f}"
            
            ivr = f"{res['ivr']:.0f}"
            safe = f"{res['safety']:.1f}"
            td = f"{res['td_ratio']:.2f}"
            
            iv_str = f"{res['iv']:.2f}" + ("*" if res.get('is_iv_est') else "")
            hv_str = f"{res['hv']:.2f}"
            
            delta_str = f"{res['delta']:.2f}" + ("*" if res.get('is_delta_est') else "")
            
            shp = f"{res['sharpe']:.2f}"
            theta_pct = f"{res['theta_yield']*100:.1f}%"
            kelly = f"{res['kelly']*100:.1f}%"
            
            earn_tag = '<span class="earn-tag">EARN</span>' if res['earn'] else ''

            row_html = f"""
            <tr class="{row_class}">
                <td>{symbol}</td>
                <td>{expiry}</td>
                <td>{dte}</td>
                <td>{strike}</td>
                <td>{bid_display}</td>
                <td {cell_style(pass_ann)}>{ann_str}</td>
                <td>{ideal}</td>
                <td {cell_style(ratio > 1.0)}>{ratio_str}</td>
                <td {cell_style(pass_ivr)}>{ivr}</td>
                <td {cell_style(res['safety'] > 1.0)}>{safe}</td>
                <td {cell_style(pass_td)}>{td}</td>
                <td {cell_style(pass_iv_hv)}>{iv_str}</td>
                <td>{hv_str}</td>
                <td {cell_style(pass_delta)}>{delta_str}</td>
                <td {cell_style(res['sharpe'] > 0.2)}>{shp}</td>
                <td>{theta_pct}</td>
                <td {cell_style(res['kelly'] > 0)}>{kelly} {earn_tag}</td>
            </tr>
            """
            f.write(row_html)

def init_html_file(filename: str):
    """Initialize HTML file with headers and CSS"""
    with open(filename, "w") as f:
        f.write("""<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Sell Put Candidates</title>
    <style>
        body { font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, Helvetica, Arial, sans-serif; margin: 20px; background-color: #f5f5f5; }
        h1 { color: #333; }
        .timestamp { color: #666; font-size: 0.9em; margin-bottom: 20px; }
        table { border-collapse: collapse; width: 100%; background-color: white; box-shadow: 0 1px 3px rgba(0,0,0,0.1); font-size: 14px; }
        th, td { padding: 8px 12px; text-align: right; border-bottom: 1px solid #ddd; }
        th { background-color: #f8f9fa; color: #495057; font-weight: 600; text-align: right; position: sticky; top: 0; }
        th:first-child, td:first-child { text-align: left; } /* Symbol */
        th:nth-child(2), td:nth-child(2) { text-align: left; } /* Expiry */
        
        /* Column Widths */
        th { white-space: nowrap; }
        
        /* Threshold Highlights */
        .pass { color: #28a745; font-weight: bold; }
        .fail { color: #dc3545; }
        
        /* Special Rows */
        .perfect-row { background-color: #e8f5e9; } /* Light Green BG for perfect matches */
        .section-header { background-color: #e9ecef; font-weight: bold; text-align: left; }
        .section-header td { text-align: left; padding-top: 15px; padding-bottom: 5px; border-bottom: 2px solid #dee2e6; }
        
        .src { font-size: 0.8em; color: #888; }
        .earn-tag { background-color: #dc3545; color: white; padding: 2px 4px; border-radius: 3px; font-size: 0.7em; margin-left: 5px; }
        
        /* Hover Effect */
        tr:hover { background-color: #f1f1f1; }
    </style>
</head>
<body>
    <h1>Sell Put Opportunities</h1>
    <div class="timestamp">Generated on: """ + datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S") + """</div>
    
    <table>
        <thead>
            <tr>
                <th>Symbol</th>
                <th>Expiry</th>
                <th>DTE</th>
                <th>Strike</th>
                <th>Bid(Src)</th>
                <th>Ann%<br><span style="font-size:0.8em;font-weight:normal">>15%</span></th>
                <th>Ideal</th>
                <th>Ratio<br><span style="font-size:0.8em;font-weight:normal">>1.0</span></th>
                <th>IVR<br><span style="font-size:0.8em;font-weight:normal">>30</span></th>
                <th>Safe<br><span style="font-size:0.8em;font-weight:normal">>1.0</span></th>
                <th>T/D<br><span style="font-size:0.8em;font-weight:normal">>0.4</span></th>
                <th>IV<br><span style="font-size:0.8em;font-weight:normal">>HV</span></th>
                <th>HV</th>
                <th>Delta<br><span style="font-size:0.8em;font-weight:normal"><.30</span></th>
                <th>Shp<br><span style="font-size:0.8em;font-weight:normal">>.2</span></th>
                <th>Th%<br><span style="font-size:0.8em;font-weight:normal">High</span></th>
                <th>Kelly%<br><span style="font-size:0.8em;font-weight:normal">High</span></th>
            </tr>
        </thead>
        <tbody>
""")

def close_html_file(filename: str):
    with open(filename, "a") as f:
        f.write("""
        </tbody>
    </table>
</body>
</html>
""")

def main():
    util.patchAsyncio()
    ib = connect_ib()
    
    # Initialize files
    with open("candidates.txt", "w") as f:
         header_file = f"{'Symbol':<6} | {'Expiry':<10} | {'DTE':<4} | {'Strike':<8} | {'Bid(Src)':<16} | {'Ann%':<6} | {'Ideal':<8} | {'Ratio':<6} | {'IVR':<4} | {'Safe':<5} | {'T/D':<5} | {'IV':<6} | {'HV':<6} | {'Delta':<7} | {'Shp':<4} | {'Th%':<5} | {'Kelly%':<7}"
         sub_header = f"{'':<6} | {'':<10} | {'':<4} | {'':<8} | {'':<16} | {'>15%':<6} | {'':<8} | {'>1.0':<6} | {'>30':<4} | {'>1.0':<5} | {'>0.4':<5} | {'>HV':<6} | {'':<6} | {'<.30':<7} | {'>.2':<4} | {'High':<5} | {'High':<7}"
         f.write(header_file + "\n")
         f.write(sub_header + "\n")
         f.write("-" * 170 + "\n")
    
    init_html_file("candidates.html")

    try:
        for symbol in ALL_TARGETS:
            print(f"\n[{symbol}] Processing...")
            
            stk = qualify_stock(ib, symbol)
            if not stk: continue
            
            price = get_current_price(ib, stk)
            if price <= 0:
                print(f"Skipping {symbol}: No Price")
                continue
            
            print(f"  Underlying Price: {price}")
            hv_1w, hv_6m = get_historical_volatility(ib, stk)
            iv_low, iv_high, iv_curr = get_iv_rank_data(ib, stk)
            earn_date = get_next_earnings_date(ib, stk)
            
            # 1. Get Candidates from Moomoo (Driver)
            candidates_base = get_moomoo_option_chain_and_quotes(symbol, price, MIN_DTE, MAX_DTE)
            if not candidates_base:
                print("  No valid candidates found from Moomoo.")
                continue
                
            print(f"  Got {len(candidates_base)} candidates from Moomoo. Enriching with IB data...")
            
            # 2. Enrich with IB Data (Price Priority Logic applied here)
            final_results = enrich_with_ib_data(ib, candidates_base, price, hv_1w, hv_6m, (iv_low, iv_high, iv_curr), earn_date)
            
            # 3. Output
            if final_results:
                # HTML Output
                append_to_html("candidates.html", final_results, (iv_low, iv_high, iv_curr), hv_1w, hv_6m)
                
                # TXT Output
                with open("candidates.txt", "a") as f:
                    for res in final_results:
                        sym = res['symbol']
                        expiry_s = f"{res['expiry']}"
                        dte_s = f"{res['dte']}"
                        strike_s = f"{res['strike']:.1f}"
                        
                        price_content = f"{res['bid']:.2f}({res['price_src']})"
                        price_padding = 16 - len(price_content)
                        if price_padding < 0: price_padding = 0
                        price_field = price_content + " " * price_padding
                        
                        ann_s = f"{res['annualized']*100:.1f}"
                        ideal_s = f"{res['ideal_bid']:.2f}"
                        ratio_val = res['bid'] / res['ideal_bid'] if res['ideal_bid'] > 0 else 0
                        ratio_s = f"{ratio_val:.2f}"
                        ivr_s = f"{res['ivr']:.0f}"
                        safe_s = f"{res['safety']:.1f}"
                        td_s = f"{res['td_ratio']:.2f}"
                        iv_s = f"{res['iv']:.2f}"
                        hv_s = f"{res['hv']:.2f}"
                        delta_s = f"{res['delta']:.2f}"
                        shp_s = f"{res['sharpe']:.2f}"
                        theta_pct_str = f"{res['theta_yield']*100:.1f}%"
                        kelly_s = f"{res['kelly']*100:.1f}%"
                        
                        row_str = (
                            f"{sym:<6} | {expiry_s:<10} | {dte_s:<4} | {strike_s:<8} | {price_field} | {ann_s:<6} | "
                            f"{ideal_s:<8} | {ratio_s:<6} | {ivr_s:<4} | {safe_s:<5} | {td_s:<5} | "
                            f"{iv_s:<6} | {hv_s:<6} | {delta_s:<7} | {shp_s:<4} | {theta_pct_str:<5} | {kelly_s:>7}"
                        )
                        
                        if res['earn']: row_str += " (EARN)"
                        f.write(row_str + "\n")
                
                print(f"  Saved {len(final_results)} records.")
                
                # Console Print
                print(f"Risk Metrics: IV={iv_curr:.2f} HV={max(hv_1w, hv_6m):.2f}")
                header = f"{'Expiry':<10} | {'DTE':<4} | {'Strike':<8} | {'Bid(Src)':<16} | {'Ann%':<6} | {'Ideal':<8} | {'Ratio':<6} | {'IVR':<4} | {'Safe':<5} | {'T/D':<5} | {'IV':<6} | {'HV':<6} | {'Delta':<7} | {'Shp':<4} | {'Th%':<5} | {'Kelly%':<7}"
                sub_header = f"{'':<10} | {'':<4} | {'':<8} | {'':<16} | {'>15%':<6} | {'':<8} | {'>1.0':<6} | {'>30':<4} | {'>1.0':<5} | {'>0.4':<5} | {'>HV':<6} | {'':<6} | {'<.30':<7} | {'>.2':<4} | {'High':<5} | {'High':<7}"
                print(header)
                print(sub_header)
                print("-" * 170)
                
                for res in final_results[:NUM_OPTIONS_TO_SHOW]:
                    pass_ivr = res['ivr'] > 30
                    pass_ann = res['annualized'] > 0.15
                    pass_td = res['td_ratio'] > 0.4
                    pass_delta = abs(res['delta']) < 0.3
                    pass_iv_hv = res['iv'] > res['hv']
                    is_perfect = pass_ivr and pass_ann and pass_td and pass_delta and pass_iv_hv
                    
                    row_str = (
                        f"{res['expiry']:<10} | {res['dte']:<4} | {res['strike']:<8.1f} | "
                        f"{res['bid']:.2f}({res['price_src']}){' '*max(0, 16-len(f'{res['bid']:.2f}({res['price_src']})'))} | "
                        f"{res['annualized']*100:<6.1f} | {res['ideal_bid']:<8.2f} | "
                        f"{res['bid']/res['ideal_bid'] if res['ideal_bid']>0 else 0:<6.2f} | "
                        f"{res['ivr']:<4.0f} | {res['safety']:<5.1f} | {res['td_ratio']:<5.2f} | "
                        f"{res['iv']:<6.2f} | {res['hv']:<6.2f} | {res['delta']:>7.2f} | "
                        f"{res['sharpe']:<4.2f} | {res['theta_yield']*100:<5.1f}% | {res['kelly']*100:>7.1f}%"
                    )
                    
                    if is_perfect:
                        print(f"{COLOR_RED}{row_str}{COLOR_RESET}")
                    else:
                        print(row_str)

    finally:
        close_html_file("candidates.html")
        ib.disconnect()

if __name__ == "__main__":
    main()
