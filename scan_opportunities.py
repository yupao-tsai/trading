#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import sys
import datetime
import math
import statistics
import re
from typing import List, Optional, Dict, Tuple
from ib_insync import IB, Stock, Option, util

# Attempt imports for fallback data sources
try:
    from moomoo import OpenSecTradeContext, TrdMarket, TrdEnv, SecurityFirm, RET_OK, OptionType, OpenQuoteContext, SubType
except ImportError:
    OpenSecTradeContext = None
    OpenQuoteContext = None
    OptionType = None

# ====== 設定 ======
HOST = "127.0.0.1"
PORT = 7496
CLIENT_ID = 12  # Use a different client ID to avoid conflict with main_union.py

# 篩選條件 (Same as main_union.py)
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

PRIMARY_EXCHANGE = "NYSE"
CURRENCY = "USD"

# ANSI Colors
COLOR_RED = "\033[91m"
COLOR_GREEN = "\033[92m"
COLOR_YELLOW = "\033[93m"
COLOR_BLUE = "\033[94m"
COLOR_RESET = "\033[0m"

# Target List
TARGET_ETFS = ["SPY", "QQQ", "IWM", "DIA", "GLD", "SLV", "TLT", "HYG", "EEM", "FXI"]
TARGET_STOCKS = [
    "AAPL", "MSFT", "AMZN", "GOOGL", "META", "TSLA", "NVDA", "BRK B", "JPM", "JNJ", 
    "V", "PG", "MA", "UNH", "HD", "CVX", "MRK", "ABBV", "KO", "PEP", 
    "BAC", "AVGO", "COST", "TMO", "MCD", "CSCO", "WMT", "XOM", "DIS", "PFE", 
    "VZ", "CMCSA", "ADBE", "NKE", "INTC", "WFC", "CRM", "ACN", "AMD", "NFLX"
]

ALL_TARGETS = TARGET_ETFS + TARGET_STOCKS

# Global cache
OPTION_PRICE_CACHE = {}

# --- Black-Scholes Utility Functions ---
from math import log, sqrt, exp
from scipy.stats import norm

def bs_put_price(S, K, T, r, sigma):
    if T <= 0 or sigma <= 0: return 0.0
    d1 = (log(S/K) + (r + 0.5 * sigma**2) * T) / (sigma * sqrt(T))
    d2 = d1 - sigma * sqrt(T)
    return K * exp(-r * T) * norm.cdf(-d2) - S * norm.cdf(-d1)

def bs_call_price(S, K, T, r, sigma):
    if T <= 0 or sigma <= 0: return 0.0
    d1 = (log(S/K) + (r + 0.5 * sigma**2) * T) / (sigma * sqrt(T))
    d2 = d1 - sigma * sqrt(T)
    return S * norm.cdf(d1) - K * exp(-r * T) * norm.cdf(d2)

def bs_put_delta(S, K, T, r, sigma):
    if T <= 0 or sigma <= 0: return 0.0
    d1 = (log(S/K) + (r + 0.5 * sigma**2) * T) / (sigma * sqrt(T))
    return norm.cdf(d1) - 1.0

def bs_call_delta(S, K, T, r, sigma):
    if T <= 0 or sigma <= 0: return 0.0
    d1 = (log(S/K) + (r + 0.5 * sigma**2) * T) / (sigma * sqrt(T))
    return norm.cdf(d1)

def bs_put_theta(S, K, T, r, sigma):
    if T <= 0 or sigma <= 0: return 0.0
    d1 = (log(S/K) + (r + 0.5 * sigma**2) * T) / (sigma * sqrt(T))
    d2 = d1 - sigma * sqrt(T)
    term1 = -(S * norm.pdf(d1) * sigma) / (2 * sqrt(T))
    term2 = r * K * exp(-r * T) * norm.cdf(-d2)
    return term1 + term2

def implied_volatility(price, S, K, T, r, right='P'):
    if price <= 0: return 0.0
    low = 0.001
    high = 5.0
    for _ in range(50):
        mid = (low + high) / 2
        est_price = bs_put_price(S, K, T, r, mid) if right == 'P' else bs_call_price(S, K, T, r, mid)
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
    # Handle specific exchange requirements if needed, defaulting to SMART
    stk = Stock(symbol, "SMART", currency=CURRENCY) # Removed primaryExchange to let IB decide or use SMART broadly
    if symbol == "BRK B":
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
    ib.reqMarketDataType(MARKET_DATA_TYPE)
    ticker = ib.reqMktData(contract, "", False, False)
    
    # Quick wait
    for _ in range(10):
        ib.sleep(0.2)
        if ticker.last and ticker.last > 0: return ticker.last
        if ticker.close and ticker.close > 0: return ticker.close
    
    # Fallback to Moomoo if IB fails
    try:
        if OpenQuoteContext:
            moomoo_host = "127.0.0.1"
            moomoo_port = 11111
            base_symbol = contract.symbol.replace(" ", ".")
            if "US." not in base_symbol: base_symbol = "US." + base_symbol
            if "BRK.B" in base_symbol: base_symbol = base_symbol.replace("BRK.B", "BRKB")
            
            q_ctx = OpenQuoteContext(host=moomoo_host, port=moomoo_port)
            q_ctx.subscribe([base_symbol], [SubType.QUOTE], subscribe_push=False)
            ret, df = q_ctx.get_stock_quote([base_symbol])
            q_ctx.close()
            
            if ret == RET_OK and not df.empty:
                row = df.iloc[0]
                val = row.get('last_price', 0.0) or row.get('close_price', 0.0)
                if val > 0: return float(val)
    except:
        pass
        
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

def get_option_chain(ib: IB, stk: Stock) -> List:
    chains = ib.reqSecDefOptParams(stk.symbol, "", stk.secType, stk.conId)
    if not chains: return []
    return chains

def filter_options(ib: IB, stk: Stock, chains: List, current_price: float) -> List[Option]:
    today = datetime.date.today()
    candidates = []
    target_strike_min = current_price * STRIKE_PCT_MIN
    target_strike_max = current_price * STRIKE_PCT_MAX
    
    seen_contracts = set()

    for chain in chains:
        if chain.exchange != 'SMART': continue
        
        valid_expiries = []
        for exp_str in chain.expirations:
            year = int(exp_str[:4])
            month = int(exp_str[4:6])
            day = int(exp_str[6:])
            exp_date = datetime.date(year, month, day)
            days_to_expire = (exp_date - today).days
            if MIN_DTE <= days_to_expire <= MAX_DTE:
                valid_expiries.append(exp_str)
        
        if not valid_expiries: continue

        for strike in chain.strikes:
            if target_strike_min <= strike <= target_strike_max:
                for exp_str in valid_expiries:
                    key = (exp_str, strike)
                    if key in seen_contracts: continue
                    seen_contracts.add(key)
                    
                    candidates.append(Option(stk.symbol, exp_str, strike, 'P', 'SMART', CURRENCY, chain.multiplier))
    
    return candidates

def get_moomoo_quote_for_options(ib_symbol_base: str, min_dte: int, max_dte: int) -> Dict[Tuple[str, float], float]:
    if OpenQuoteContext is None: return {}
    moomoo_host = "127.0.0.1"
    moomoo_port = 11111
    
    moomoo_base = ib_symbol_base.replace(" ", ".")
    if "US." not in moomoo_base: moomoo_base = "US." + moomoo_base
    adjusted_base = moomoo_base.replace("BRK.B", "BRKB") if "BRK.B" in moomoo_base else moomoo_base
    
    today = datetime.date.today()
    start_date = today + datetime.timedelta(days=min_dte)
    end_date = today + datetime.timedelta(days=max_dte)
    price_map = {}
    
    try:
        quote_ctx = OpenQuoteContext(host=moomoo_host, port=moomoo_port)
        current_start = start_date
        all_codes = []
        
        while current_start <= end_date:
            current_end = min(current_start + datetime.timedelta(days=29), end_date)
            s_str = current_start.strftime("%Y-%m-%d")
            e_str = current_end.strftime("%Y-%m-%d")
            
            ret, df_chain = quote_ctx.get_option_chain(code=adjusted_base, start=s_str, end=e_str, option_type=OptionType.PUT)
            if (ret != RET_OK or df_chain.empty) and adjusted_base != moomoo_base:
                ret, df_chain = quote_ctx.get_option_chain(code=moomoo_base, start=s_str, end=e_str, option_type=OptionType.PUT)
            
            if ret == RET_OK and not df_chain.empty:
                all_codes.extend(df_chain['code'].tolist())
            current_start = current_end + datetime.timedelta(days=1)
            
        all_codes = list(set(all_codes))
        chunk_size = 100
        for i in range(0, len(all_codes), chunk_size):
            batch = all_codes[i:i+chunk_size]
            quote_ctx.subscribe(batch, [SubType.QUOTE], subscribe_push=False)
            ret_q, df_quote = quote_ctx.get_stock_quote(batch)
            if ret_q == RET_OK and not df_quote.empty:
                for _, row in df_quote.iterrows():
                    val = row.get('last_price') or row.get('nominal_price')
                    if val and val > 0:
                        match = re.search(r'(\d{6})([CP])(\d+)$', row['code'])
                        if match:
                            d_str, t, s_str = match.groups()
                            price_map[("20"+d_str, int(s_str)/1000.0)] = val
            quote_ctx.unsubscribe(batch, [SubType.QUOTE])
        quote_ctx.close()
    except Exception as e:
        print(f"Moomoo error: {e}")
        
    return price_map

def get_unified_option_price(ib: IB, symbol: str, expiry_str: str, strike: float, right: str, moomoo_prices: Dict = None) -> Tuple[float, str, float, float]:
    cache_key = (symbol, expiry_str.replace("-", ""), strike, right)
    if cache_key in OPTION_PRICE_CACHE: return OPTION_PRICE_CACHE[cache_key]
    
    price, src, iv, delta = 0.0, "", 0.0, 0.0
    
    # Check Moomoo Cache first (faster than IB wait)
    if moomoo_prices:
        k = (expiry_str.replace("-", ""), strike)
        if k in moomoo_prices:
            price, src = moomoo_prices[k], "ML"

    # IB Check
    if price == 0:
        try:
            opt = Option(symbol, expiry_str.replace("-", ""), strike, right, "SMART", CURRENCY)
            ib.qualifyContracts(opt)
            ticker = ib.reqMktData(opt, "100,101,106", False, False)
            start = datetime.datetime.now()
            while (datetime.datetime.now() - start).total_seconds() < 1.5:
                ib.sleep(0.1)
                if (ticker.bid and ticker.bid > 0) or (ticker.last and ticker.last > 0): break
            ib.cancelMktData(opt)
            
            if ticker.modelGreeks:
                iv = ticker.modelGreeks.impliedVol or 0.0
                delta = ticker.modelGreeks.delta or 0.0
                
            if ticker.bid and ticker.bid > 0: price, src = ticker.bid, "IB"
            elif ticker.last and ticker.last > 0: price, src = ticker.last, "IL"
            elif ticker.close and ticker.close > 0: price, src = ticker.close, "IC"
        except: pass

    if price > 0:
        OPTION_PRICE_CACHE[cache_key] = (price, src, iv, delta)
    
    return price, src, iv, delta

def analyze_candidates(ib: IB, candidates: List[Option], current_price: float, hv_1w: float, hv_6m: float, iv_rank_info: Tuple[float, float, float], earnings_date) -> List[Dict]:
    results = []
    iv_low, iv_high, iv_curr_hist = iv_rank_info
    
    # Pre-fetch Moomoo
    symbol = candidates[0].symbol
    moomoo_prices = get_moomoo_quote_for_options(symbol, MIN_DTE, MAX_DTE)
    
    chunk_size = 50
    for i in range(0, len(candidates), chunk_size):
        chunk = candidates[i:i+chunk_size]
        ib.qualifyContracts(*chunk)
        ib.reqMarketDataType(MARKET_DATA_TYPE)
        tickers = [(opt, ib.reqMktData(opt, "100,101,106", False, False)) for opt in chunk]
        
        for _ in range(20):
            ib.sleep(0.1)
            if sum(1 for _, t in tickers if t.bid or t.last or t.close) >= len(chunk) * 0.9: break
            
        today = datetime.date.today()
        
        for opt, ticker in tickers:
            ib.cancelMktData(opt)
            
            price, src, iv, delta = get_unified_option_price(ib, opt.symbol, opt.lastTradeDateOrContractMonth, opt.strike, opt.right, moomoo_prices)
            
            # Use Ticker Greeks if available and better
            theta = 0.0
            if ticker.modelGreeks:
                if ticker.modelGreeks.impliedVol: iv = ticker.modelGreeks.impliedVol
                if ticker.modelGreeks.delta: delta = ticker.modelGreeks.delta
                if ticker.modelGreeks.theta: theta = ticker.modelGreeks.theta
            
            if price <= 0.05: continue
            
            bid = price
            
            # Metrics
            dte = (datetime.datetime.strptime(opt.lastTradeDateOrContractMonth, "%Y%m%d").date() - today).days
            if dte == 0: dte = 1
            T = dte / 365.0
            
            net_bid = max(0, bid - FEES_PER_CONTRACT/100.0)
            ret = net_bid / opt.strike
            annualized_ret = ret * (365 / dte)
            
            if iv == 0: iv = implied_volatility(bid, current_price, opt.strike, T, RISK_FREE_RATE)
            if delta == 0: delta = bs_put_delta(current_price, opt.strike, T, RISK_FREE_RATE, iv)
            if theta == 0: theta = bs_put_theta(current_price, opt.strike, T, RISK_FREE_RATE, iv) / 365.0
            
            theta_yield = abs(theta) / bid if bid > 0 else 0.0
            
            target_kelly = 0.10
            conservative_vol = max(iv, hv_1w, hv_6m)
            req_ret = target_kelly * 2 * (conservative_vol**2) + RISK_FREE_RATE
            abs_delta = abs(delta)
            req_ann = req_ret / (1.0 - abs_delta) if (1.0 - abs_delta) > 0.1 else req_ret / 0.1
            ideal_bid = opt.strike * req_ann * (dte/365.0)
            
            win_prob = max(0, 1.0 - abs_delta)
            kelly = calculate_kelly_fraction(annualized_ret, RISK_FREE_RATE, win_prob, iv, hv_1w, hv_6m)
            
            vol_risk = max(iv, hv_1w, hv_6m)
            sharpe = (annualized_ret - RISK_FREE_RATE) / vol_risk if vol_risk > 0.01 else 0.0
            
            # Filtering
            # if annualized_ret < MIN_ANNUAL_RETURN: continue
            # if sharpe < MIN_SHARPE_RATIO: continue
            
            ivr = 0.0
            if iv_high > iv_low: ivr = (iv - iv_low) / (iv_high - iv_low) * 100
            
            expected_move = current_price * iv * math.sqrt(T)
            safety = (current_price - opt.strike) / expected_move if expected_move > 0 else 99.9
            
            td_ratio = abs(theta) / abs(delta) if abs(delta) > 0.001 else 0.0
            
            earn = False
            if earnings_date:
                e_date = datetime.datetime.strptime(opt.lastTradeDateOrContractMonth, "%Y%m%d").date()
                if today <= earnings_date <= e_date: earn = True
            
            results.append({
                "symbol": opt.symbol, "expiry": opt.lastTradeDateOrContractMonth, "strike": opt.strike,
                "bid": bid, "price_src": src, "dte": dte, "annualized": annualized_ret,
                "ideal_bid": ideal_bid, "iv": iv, "delta": delta, "theta": theta,
                "kelly": kelly, "sharpe": sharpe, "ivr": ivr, "safety": safety,
                "td_ratio": td_ratio, "earn": earn, "theta_yield": theta_yield,
                "hv": max(hv_1w, hv_6m)
            })
            
    return sorted(results, key=lambda x: x['kelly'], reverse=True)

def main():
    util.patchAsyncio()
    ib = connect_ib()
    
    print(f"Scanning {len(ALL_TARGETS)} symbols: {ALL_TARGETS}")
    
    all_opportunities = []
    
    try:
        for symbol in ALL_TARGETS:
            print(f"\n[{symbol}] Processing...")
            stk = qualify_stock(ib, symbol)
            if not stk: continue
            
            price = get_current_price(ib, stk)
            if price <= 0:
                print(f"Skipping {symbol}: No Price")
                continue
            print(f"  Price: {price}")
            
            hv_1w, hv_6m = get_historical_volatility(ib, stk)
            iv_low, iv_high, iv_curr = get_iv_rank_data(ib, stk)
            earn_date = get_next_earnings_date(ib, stk)
            
            chains = get_option_chain(ib, stk)
            if not chains: continue
            
            candidates = filter_options(ib, stk, chains, price)
            if not candidates: continue
            
            print(f"  Analyzing {len(candidates)} candidates...")
            results = analyze_candidates(ib, candidates, price, hv_1w, hv_6m, (iv_low, iv_high, iv_curr), earn_date)
            
            all_opportunities.extend(results)
            print(f"  Found {len(results)} opportunities.")

        print("\n\n====== FINAL REPORT: TOP OPPORTUNITIES ACROSS MARKET ======")
        print(f"Sorted by Kelly Fraction (Top 50 shown)")
        print(f"Full report written to candidates.txt")
        
        # Sort all findings
        all_opportunities.sort(key=lambda x: x['kelly'], reverse=True)
        
        # Write to file
        with open("candidates.txt", "w") as f:
            header_file = f"{'Symbol':<6} | {'Expiry':<10} | {'DTE':<4} | {'Strike':<8} | {'Bid(Src)':<16} | {'Ann%':<6} | {'Ideal':<8} | {'Ratio':<6} | {'IVR':<4} | {'Safe':<5} | {'T/D':<5} | {'IV':<6} | {'HV':<6} | {'Delta':<7} | {'Shp':<4} | {'Th%':<5} | {'Kelly%':<7}"
            f.write(header_file + "\n")
            f.write("-" * 170 + "\n")
            
            for res in all_opportunities:
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
                
                if res['earn']:
                    row_str += " (EARN)"
                    
                f.write(row_str + "\n")
        
        # Print simple table to stdout
        header = f"{'Symbol':<6} | {'Expiry':<8} | {'Strike':<8} | {'Bid':<8} | {'Ann%':<6} | {'Ideal':<8} | {'Ratio':<5} | {'IVR':<3} | {'Safe':<4} | {'T/D':<4} | {'Kelly%':<6}"
        print("-" * 100)
        print(header)
        print("-" * 100)
        
        for res in all_opportunities[:50]:
             sym = res['symbol']
             exp = res['expiry'][2:] # YYMMDD
             strike = f"{res['strike']:.1f}"
             bid = f"{res['bid']:.2f}"
             ann = f"{res['annualized']*100:.1f}"
             ideal = f"{res['ideal_bid']:.2f}"
             ratio = f"{res['bid']/res['ideal_bid']:.2f}" if res['ideal_bid']>0 else "0"
             ivr = f"{res['ivr']:.0f}"
             safe = f"{res['safety']:.1f}"
             td = f"{res['td_ratio']:.2f}"
             kelly = f"{res['kelly']*100:.1f}%"
             
             # Color logic
             pass_ivr = res['ivr'] > 30
             pass_ann = res['annualized'] > 0.15
             pass_td = res['td_ratio'] > 0.4
             pass_delta = abs(res['delta']) < 0.3
             is_perfect = pass_ivr and pass_ann and pass_td and pass_delta
             
             row_str = f"{sym:<6} | {exp:<8} | {strike:<8} | {bid:<8} | {ann:<6} | {ideal:<8} | {ratio:<5} | {ivr:<3} | {safe:<4} | {td:<4} | {kelly:<6}"
             
             if is_perfect:
                 print(f"{COLOR_GREEN}{row_str}{COLOR_RESET}")
             else:
                 print(row_str)

    finally:
        ib.disconnect()

if __name__ == "__main__":
    main()

