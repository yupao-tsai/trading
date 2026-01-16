#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import datetime
from ib_insync import IB, Stock, Option, util
from typing import List, Optional, Dict, Tuple
import math
import statistics
import re
import argparse
from scipy.stats import norm

try:
    import robin_stocks.robinhood as rh
    import pyotp
except ImportError:
    rh = None
    pyotp = None

# ====== 設定 ======
HOST = "127.0.0.1"
PORT = 7496
CLIENT_ID = 12 # 使用不同的 Client ID 避免衝突

# 預設參數 (可透過參數覆蓋)
SYMBOL = "PYPL"
RISK_FREE_RATE = 0.045
MIN_ROLL_YIELD = 0.15 # 如果新合約年化殖利率低於此則不推薦
MIN_PROFIT_TO_CLOSE = 0.70 # 當前持倉獲利超過 70% 建議考慮平倉

# 顏色代碼
COLOR_RED = "\033[91m"
COLOR_GREEN = "\033[92m"
COLOR_YELLOW = "\033[93m"
COLOR_BLUE = "\033[94m"
COLOR_RESET = "\033[0m"

try:
    from moomoo import OpenSecTradeContext, TrdMarket, TrdEnv, SecurityFirm, RET_OK, OptionType, OpenQuoteContext
except ImportError:
    OpenSecTradeContext = None

# --- Black-Scholes Utility Functions ---
from math import log, sqrt, exp
from scipy.stats import norm

def bs_call_price(S, K, T, r, sigma):
    """計算 Call 權利金 (BS 模型)"""
    if T <= 0 or sigma <= 0: return 0.0
    d1 = (log(S/K) + (r + 0.5 * sigma**2) * T) / (sigma * sqrt(T))
    d2 = d1 - sigma * sqrt(T)
    call_price = S * norm.cdf(d1) - K * exp(-r * T) * norm.cdf(d2)
    return call_price

def bs_call_delta(S, K, T, r, sigma):
    """計算 Call Delta (BS 模型)"""
    if T <= 0 or sigma <= 0: return 0.0
    d1 = (log(S/K) + (r + 0.5 * sigma**2) * T) / (sigma * sqrt(T))
    return norm.cdf(d1)

def implied_volatility(price, S, K, T, r, right='C'):
    """反推隱含波動率 (二分法)"""
    if price <= 0: return 0.0
    
    low = 0.001
    high = 5.0
    for _ in range(50):
        mid = (low + high) / 2
        # Covered Call only considers Calls
        est_price = bs_call_price(S, K, T, r, mid)
            
        if abs(est_price - price) < 0.001:
            return mid
        if est_price < price:
            low = mid
        else:
            high = mid
    return (low + high) / 2

def calculate_kelly_fraction(annualized_ret, risk_free_rate, win_prob, iv, hv_short, hv_long):
    """
    凱利公式計算投資比例 (綜合 IV 與 HV 風險)
    """
    # 1. 使用 Delta (win_prob = 1 - Delta) 修正預期報酬率
    expected_annual_ret = annualized_ret * win_prob
    
    if expected_annual_ret <= risk_free_rate:
        return 0.0

    # 2. 處理波動率 - 取最大值作為風險定價
    # 在本腳本中如果沒有 HV 數據，暫時用 IV 或預設值
    sigma = max(iv, hv_short, hv_long)
    
    if sigma < 0.1: sigma = 0.1

    # 3. Merton's Fraction
    # f = (mu - r) / (sigma^2)
    fraction = (expected_annual_ret - risk_free_rate) / (sigma ** 2)
    
    # 4. Half-Kelly (安全邊際)
    return max(0, fraction * 0.5) 

def get_historical_volatility(ib: IB, contract: Stock) -> tuple[float, float]:
    """計算歷史波動率 (HV)"""
    print("Fetching historical data for HV calculation...")
    end_date = ""
    duration_6m = "6 M"
    bar_size = "1 day"
    what_to_show = "TRADES"
    
    bars = ib.reqHistoricalData(
        contract, endDateTime=end_date, durationStr=duration_6m, 
        barSizeSetting=bar_size, whatToShow=what_to_show, useRTH=1, 
        formatDate=1, keepUpToDate=False
    )
    
    if not bars:
        return 0.18, 0.18
        
    closes = [b.close for b in bars]
    log_returns = []
    for i in range(1, len(closes)):
        if closes[i-1] > 0:
            log_returns.append(math.log(closes[i] / closes[i-1]))
        else:
            log_returns.append(0.0)
            
    if not log_returns: return 0.18, 0.18

    def calc_hv(returns):
        if len(returns) < 2: return 0.0
        daily_std = statistics.stdev(returns)
        return daily_std * math.sqrt(252)
    
    hv_6m = calc_hv(log_returns)
    hv_1w = calc_hv(log_returns[-5:]) if len(log_returns) >= 5 else hv_6m
    
    print(f"Historical Volatility: 1-Week={hv_1w:.2%}, 6-Month={hv_6m:.2%}")
    return hv_1w, hv_6m

def calculate_kelly_fraction(annualized_ret, risk_free_rate, win_prob, iv, hv_short, hv_long):
    expected_annual_ret = annualized_ret * win_prob
    if expected_annual_ret <= risk_free_rate: return 0.0
    sigma = max(iv, hv_short, hv_long)
    if sigma < 0.1: sigma = 0.1
    fraction = (expected_annual_ret - risk_free_rate) / (sigma ** 2)
    return max(0, fraction * 0.5)

# --- Black-Scholes Utility Functions ---
from math import log, sqrt, exp

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

def implied_volatility(price, S, K, T, r, right='P'):
    if price <= 0: return 0.0
    low = 0.001
    high = 5.0
    for _ in range(50):
        mid = (low + high) / 2
        if right == 'C':
            est_price = bs_call_price(S, K, T, r, mid)
        else:
            est_price = bs_put_price(S, K, T, r, mid)
        if abs(est_price - price) < 0.001: return mid
        if est_price < price: low = mid
        else: high = mid
    return (low + high) / 2

def connect_ib() -> IB:
    ib = IB()
    print(f"Connecting to IB {HOST}:{PORT}...")
    try:
        ib.connect(HOST, PORT, clientId=CLIENT_ID, timeout=10)
    except Exception as e:
        print(f"IB Connection failed: {e}")
        raise
    return ib

def get_current_price(ib: IB, symbol: str) -> float:
    print(f"Fetching current price for {symbol}...")
    price = 0.0
    
    # 1. Try IB
    try:
        stk = Stock(symbol, "SMART", "USD")
        ib.qualifyContracts(stk)
        ib.reqMarketDataType(3) # Delayed
        ticker = ib.reqMktData(stk, "", False, False)
        
        # Wait up to 3 seconds
        for _ in range(12):
            ib.sleep(0.25)
            if ticker.last and not math.isnan(ticker.last) and ticker.last > 0:
                price = ticker.last
                print(f"Got price from IB (Last): {price}")
                break
            elif ticker.close and not math.isnan(ticker.close) and ticker.close > 0:
                price = ticker.close
                print(f"Got price from IB (Close): {price}")
                break
                
    except Exception as e:
        print(f"IB Price Fetch Error: {e}")

    # 2. Fallback to Moomoo
    if price <= 0:
        print("IB price unavailable, trying Moomoo fallback...")
        price = get_moomoo_price(symbol)
        
    return price

def get_moomoo_price(symbol: str) -> float:
    price = 0.0
    try:
        moomoo_symbol = symbol.replace(" ", ".")
        if "US." not in moomoo_symbol: moomoo_symbol = "US." + moomoo_symbol
        if "BRK.B" in moomoo_symbol: moomoo_symbol = moomoo_symbol.replace("BRK.B", "BRKB")
        
        ctx = OpenQuoteContext(host="127.0.0.1", port=11111)
        
        # Must subscribe first!
        from moomoo import SubType
        ctx.subscribe([moomoo_symbol], [SubType.QUOTE], subscribe_push=False)
        
        ret, df = ctx.get_stock_quote([moomoo_symbol])
        ctx.close()
        
        if ret == RET_OK and not df.empty:
            row = df.iloc[0]
            # print(f"DEBUG: Moomoo Quote Row: {row.to_dict()}")
            if 'last_price' in row and row['last_price'] > 0: 
                price = float(row['last_price'])
            elif 'close_price' in row and row['close_price'] > 0:
                price = float(row['close_price'])
                
            if price > 0:
                print(f"Got price from Moomoo: {price}")
        else:
            print(f"Moomoo quote failed: ret={ret}")
            
    except Exception as e:
        print(f"Moomoo Price Fetch Error: {e}")
    return price

def get_moomoo_positions(ib: IB, symbol_filter: str) -> List[Dict]:
    """獲取特定股票的 Call 持倉"""
    if not OpenSecTradeContext:
        print("Moomoo API not available.")
        return []

    print(f"Fetching Moomoo positions for {symbol_filter}...")
    positions = []
    
    try:
        firm = getattr(SecurityFirm, 'FUTUINC', getattr(SecurityFirm, 'FUTUSG', None))
        ctx = OpenSecTradeContext(filter_trdmarket=TrdMarket.US, host="127.0.0.1", port=11111, security_firm=firm)
        ret, df = ctx.position_list_query(trd_env=TrdEnv.REAL)
        ctx.close()

        if ret != RET_OK or df.empty:
            return []

        # Filter
        target = symbol_filter.replace(" ", "").upper() # e.g. PYPL, BRKB
        if "BRK" in target: target = "BRK" # 簡化 BRK 匹配
        
        # Regex for Option Code: e.g. US.PYPL260220C70000
        # 假設格式：US.CODE + 6位日期 + C/P + Strike
        opt_pattern = re.compile(r'US\.([A-Z]+)(\d{6})([C])(\d+)')

        today = datetime.date.today()

        for _, row in df.iterrows():
            code = row['code']
            # Basic check if it relates to our symbol
            if target not in code: continue
            
            match = opt_pattern.search(code)
            if not match: continue # Not a Call option
            
            sym, date_str, type_char, strike_str = match.groups()
            
            # Double check symbol match
            if sym not in target and target not in sym: continue

            # Parse details
            year = 2000 + int(date_str[:2])
            month = int(date_str[2:4])
            day = int(date_str[4:])
            exp_date = datetime.date(year, month, day)
            dte = (exp_date - today).days
            if dte == 0: dte = 1
            
            strike = float(strike_str) / 1000.0
            
            # Price info
            cost = row['cost_price']
            
            # Unified Price Logic (Inline here to avoid big refactor of shared logic)
            # Priority: IB Bid > IB Last > Moomoo > IB Close
            current = 0.0
            price_src = ""
            
            try:
                # IB Qualify
                ib_symbol = symbol_filter
                if "BRKB" in ib_symbol: ib_symbol = "BRK B"
                
                temp_opt = Option(ib_symbol, exp_date.strftime("%Y%m%d"), strike, 'C', 'SMART')
                ib.qualifyContracts(temp_opt)
                
                # Stream data briefly
                ticker = ib.reqMktData(temp_opt, "", False, False)
                start_wait = datetime.datetime.now()
                while (datetime.datetime.now() - start_wait).total_seconds() < 2.0:
                    ib.sleep(0.1)
                    if (ticker.last and ticker.last > 0) or (ticker.bid and ticker.bid > 0):
                        break
                ib.cancelMktData(temp_opt)
                
                # 1. IB Bid
                if ticker.bid and ticker.bid > 0:
                    current = ticker.bid
                    price_src = "IB"
                # 2. IB Last
                elif ticker.last and ticker.last > 0:
                    current = ticker.last
                    price_src = "IL"
                # 4. IB Close (Fallback later)
                elif ticker.close and ticker.close > 0:
                    current = ticker.close # Stored for later
                    price_src = "IC"
            except: pass

            # 3. Moomoo (Fallback if IB Bid/Last failed)
            if current == 0 or price_src == "IC":
                # Check Moomoo row data
                mm_price = 0.0
                if 'last_price' in row and row['last_price'] > 0:
                    mm_price = float(row['last_price'])
                elif 'current_price' in row and row['current_price'] > 0:
                    mm_price = float(row['current_price'])
                elif 'nominal_price' in row and row['nominal_price'] > 0:
                    mm_price = float(row['nominal_price'])
                
                # If we only had IC (Close) but have a valid MM price, prefer MM?
                # User rule: IB Bid > Moomoo Last > IB Close
                # So if current is from IC, we overwrite with MM if available
                if mm_price > 0:
                    current = mm_price
                    price_src = "ML"
            
            # If still 0, allow IC if we had it
            if current == 0 and 'ticker' in locals() and ticker.close and ticker.close > 0:
                 current = ticker.close
                 price_src = "IC"

            # Profit %
            profit_pct = 0.0
            if cost > 0:
                profit_pct = (cost - current) / cost # 對於賣方，跌就是賺
            
            positions.append({
                'code': code,
                'expiry': exp_date.strftime("%Y%m%d"),
                'dte': dte,
                'strike': strike,
                'cost': cost,
                'current': current,
                'profit_pct': profit_pct,
                'qty': row['qty'],
                'source': f'Moomoo({price_src})'
            })
            
    except Exception as e:
        print(f"Error getting positions: {e}")
        
    return positions

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

def get_robinhood_positions(ib: IB, symbol_filter: str) -> List[Dict]:
    """獲取特定股票的 Call 持倉 (Robinhood)"""
    if not rh:
        print("Robinhood API package not found.")
        return []

    print(f"Fetching Robinhood positions for {symbol_filter}...")
    positions = []
    
    # Credentials
    username = "yptsai@gmail.com"
    password = "773250@Robinhood"
    
    try:
        rh.login(username, password)
        
        # Filter symbol
        target = symbol_filter.replace(" ", ".")
        if "BRK" in target and "B" in target and "BRK.B" not in target:
             pass 
        
        option_positions = rh.get_open_option_positions()
        if not option_positions:
            try:
                all_positions = rh.get_all_option_positions()
                option_positions = [p for p in all_positions if float(p.get('quantity', 0)) > 0]
            except Exception: pass
            
        filtered_opts = []
        for p in option_positions:
            chain_sym = p.get('chain_symbol', '')
            if chain_sym == target:
                filtered_opts.append(p)
            elif chain_sym.replace(".", "") == target.replace(".", ""):
                filtered_opts.append(p)
            elif chain_sym == symbol_filter:
                 filtered_opts.append(p)

        today = datetime.date.today()
        
        for op in filtered_opts:
            fetch_missing_option_details(rh, op)
            
            # Filter for Call only
            if op.get('type') != 'call': continue
            if 'strike_price' not in op: continue
            
            strike = float(op['strike_price'])
            exp_date_str = op['expiration_date']
            year, month, day = map(int, exp_date_str.split('-'))
            exp_date = datetime.date(year, month, day)
            
            dte = (exp_date - today).days
            if dte == 0: dte = 1
            
            qty = float(op['quantity'])
            cost = float(op['average_price'])
            
            # Fetch current price (Mark Price)
            # Priority: IB Bid (IB) > IB Last (IL) > Moomoo (ML) > IB Close (IC) > RH Mark (R)
            current = 0.0
            price_src = ""
            
            # 1. Try IB Realtime (Bid/Last)
            try:
                ib_symbol = symbol_filter
                if "BRKB" in ib_symbol: ib_symbol = "BRK B"
                
                temp_opt = Option(ib_symbol, exp_date.strftime("%Y%m%d"), strike, 'C', 'SMART')
                ib.qualifyContracts(temp_opt)
                
                # Request streaming data briefly
                ticker = ib.reqMktData(temp_opt, "", False, False)
                start_wait = datetime.datetime.now()
                while (datetime.datetime.now() - start_wait).total_seconds() < 2.0:
                    ib.sleep(0.1)
                    if (ticker.last and ticker.last > 0) or (ticker.bid and ticker.bid > 0):
                        break
                ib.cancelMktData(temp_opt)
                
                if ticker.bid and ticker.bid > 0:
                    current = ticker.bid
                    price_src = "IB"
                elif ticker.last and ticker.last > 0:
                    current = ticker.last
                    price_src = "IL"
                elif ticker.close and ticker.close > 0:
                    current = ticker.close # Store for fallback
                    price_src = "IC"
            except: pass

            # 2. Moomoo Fallback (Prefer over IC)
            if current == 0:
                 try:
                    from moomoo import OpenQuoteContext, RET_OK, SubType
                    moomoo_host = "127.0.0.1"
                    moomoo_port = 11111
                    
                    yy = exp_date_str[2:4]
                    mm = exp_date_str[5:7]
                    dd = exp_date_str[8:10]
                    date_part = f"{yy}{mm}{dd}"
                    strike_int = int(strike * 1000)
                    strike_part = f"{strike_int:08d}"
                    
                    base_sym = target
                    if "BRK.B" in base_sym: base_sym = "BRKB"
                    
                    full_code = f"US.{base_sym}{date_part}C{strike_part}"
                    
                    ctx = OpenQuoteContext(host=moomoo_host, port=moomoo_port)
                    ctx.subscribe([full_code], [SubType.QUOTE], subscribe_push=False)
                    ret, df = ctx.get_stock_quote([full_code])
                    ctx.close()
                    
                    if ret == RET_OK and not df.empty:
                        row = df.iloc[0]
                        mm_price = 0.0
                        if 'last_price' in row and row['last_price'] > 0:
                            mm_price = float(row['last_price'])
                        elif 'nominal_price' in row and row['nominal_price'] > 0:
                            mm_price = float(row['nominal_price'])
                        elif 'close_price' in row and row['close_price'] > 0:
                             mm_price = float(row['close_price'])
                        
                        if mm_price > 0:
                            current = mm_price
                            price_src = "ML"
                 except: pass

            # 3. Fallback to IB Close (if stored and no better price found)
            if current == 0 and 'ticker' in locals() and ticker.close and ticker.close > 0:
                current = ticker.close
                price_src = "IC"

            # 4. Fallback to RH Mark Price (Last Resort)
            if current == 0:
                try:
                    md = rh.get_option_market_data(target, exp_date_str, strike, 'call')
                    if md:
                         if isinstance(md, list): md = md[0]
                         if 'adjusted_mark_price' in md:
                             current = float(md['adjusted_mark_price'])
                             price_src = "R"
                except: pass

            # Profit %
            profit_pct = 0.0
            if cost > 0:
                profit_pct = (cost - current) / cost # 對於賣方，跌就是賺
                
            positions.append({
                'code': f"RH.{target}",
                'expiry': exp_date.strftime("%Y%m%d"),
                'dte': dte,
                'strike': strike,
                'cost': cost,
                'current': current,
                'profit_pct': profit_pct,
                'qty': qty,
                'source': f'RH({price_src})'
            })
            
    except Exception as e:
        print(f"Robinhood Error: {e}")
        
    return positions

def analyze_rolling_opportunities(ib: IB, current_price: float, positions: List[Dict], hv_1w: float, hv_6m: float):
    """分析現有持倉並尋找轉倉機會"""
    
    # --- 1. 分析持倉 (Collecting Data) ---
    position_report = []
    contracts_to_scan = []
    
    for pos in positions:
        residual_yield = 0.0
        if current_price > 0 and pos['dte'] > 0:
            residual_yield = (pos['current'] / current_price) * (365 / pos['dte'])
            
        pos['residual_yield'] = residual_yield
        
        action = "Hold"
        action_color = ""
        
        if pos['profit_pct'] > MIN_PROFIT_TO_CLOSE:
            action = "Close(Win)"
            action_color = COLOR_GREEN
        elif residual_yield < 0.08: 
            action = "Roll(LowY)"
            action_color = COLOR_YELLOW
            
        source = pos.get('source', 'Moomoo')
        source_color = COLOR_BLUE if source == 'RH' else ""
        
        # Store row for later printing
        row_str = f"{source_color}{source[:2]:<2}{COLOR_RESET} | {pos['expiry']:<10} | {pos['strike']:<8} | {pos['cost']:<6.2f} | {pos['current']:<6.2f} | {pos['profit_pct']*100:<7.1f}% | {residual_yield*100:<9.1f}% | {action_color}{action:<10}{COLOR_RESET}"
        position_report.append(row_str)
        
        if "Close" in action or "Roll" in action:
            contracts_to_scan.append(pos)

    # --- 2. 掃描新機會 (Scanning Market) ---
    # (Silent scanning...)
    stk = Stock(SYMBOL, "SMART", "USD")
    ib.qualifyContracts(stk)
    
    chains = ib.reqSecDefOptParams(stk.symbol, "", stk.secType, stk.conId)
    candidates = []
    today = datetime.date.today()
    
    for chain in chains:
        if chain.exchange != 'SMART': continue
        for exp in chain.expirations:
            y, m, d = int(exp[:4]), int(exp[4:6]), int(exp[6:])
            dte = (datetime.date(y, m, d) - today).days
            if 25 <= dte <= 65:
                for strike in chain.strikes:
                    # Relaxed filter: ATM to OTM 25% (1.00 instead of 1.05)
                    if current_price * 1.00 <= strike <= current_price * 1.25:
                        candidates.append(Option(SYMBOL, exp, strike, 'C', 'SMART'))
    
    unique_candidates = []
    seen = set()
    for c in candidates:
        k = (c.lastTradeDateOrContractMonth, c.strike)
        if k not in seen:
            seen.add(k)
            unique_candidates.append(c)
            
    # Batch request market data
    ib.qualifyContracts(*unique_candidates)
    ib.reqMarketDataType(3)
    tickers = []
    for i in range(0, len(unique_candidates), 50):
        batch = unique_candidates[i:i+50]
        batch_tickers = [ib.reqMktData(c, "", False, False) for c in batch]
        ib.sleep(2)
        for c, t in zip(batch, batch_tickers):
            tickers.append((c, t))

    # Analyze scan results
    scan_results = []
    
    # Calculate min residual yield from current positions to beat
    min_yield_to_beat = 0.0
    for pos in positions:
        if pos.get('residual_yield', 0) > 0:
            min_yield_to_beat = max(min_yield_to_beat, pos.get('residual_yield', 0))

    # --- Black-Scholes Helpers ---
    from math import log, sqrt, exp
    from scipy.stats import norm

    def bs_call_delta(S, K, T, r, sigma):
        if T <= 0 or sigma <= 0: return 0.0
        d1 = (log(S/K) + (r + 0.5 * sigma**2) * T) / (sigma * sqrt(T))
        return norm.cdf(d1)

    def bs_call_price(S, K, T, r, sigma):
        if T <= 0 or sigma <= 0: return 0.0
        d1 = (log(S/K) + (r + 0.5 * sigma**2) * T) / (sigma * sqrt(T))
        d2 = d1 - sigma * sqrt(T)
        return S * norm.cdf(d1) - K * exp(-r * T) * norm.cdf(d2)

    def implied_volatility(price, S, K, T, r):
        if price <= 0: return 0.0
        low, high = 0.001, 5.0
        for _ in range(30):
            mid = (low + high) / 2
            est = bs_call_price(S, K, T, r, mid)
            if abs(est - price) < 0.001: return mid
            if est < price: low = mid
            else: high = mid
        return (low + high) / 2

    # Pre-calculate HV (using static conservative value if not calculated, or passed in)
    # Ideally we should fetch HV, but for now we use a fallback or estimation
    hv_conservative = 0.15 

    for opt, ticker in tickers:
        bid = 0.0
        if ticker.bid and ticker.bid > 0: bid = ticker.bid
        elif ticker.last and ticker.last > 0: bid = ticker.last
        elif ticker.close and ticker.close > 0: bid = ticker.close
        
        if bid <= 0.05: continue
        
        dte = (datetime.datetime.strptime(opt.lastTradeDateOrContractMonth, "%Y%m%d").date() - today).days
        if dte == 0: dte = 1
        T = dte / 365.0
        
        yield_annual = (bid / current_price) * (365 / dte)
        buffer_pct = (opt.strike - current_price) / current_price
        
        # Calculate Greeks
        iv = 0.0
        delta = 0.0
        if ticker.modelGreeks:
            if ticker.modelGreeks.impliedVol: iv = ticker.modelGreeks.impliedVol
            if ticker.modelGreeks.delta: delta = ticker.modelGreeks.delta
        
        if iv == 0: iv = implied_volatility(bid, current_price, opt.strike, T, RISK_FREE_RATE)
        if delta == 0: delta = bs_call_delta(current_price, opt.strike, T, RISK_FREE_RATE, iv)
        
        # Calculate Ideal & Kelly & Sharpe
        # Logic adapted from main_union.py
        target_kelly_allocation = 0.10
        target_fraction = target_kelly_allocation * 2
        conservative_vol = max(iv, hv_conservative)
        required_exp_ret = target_fraction * (conservative_vol ** 2) + RISK_FREE_RATE
        
        # For Covered Call: Win Prob is roughly Prob(Stock < Strike) = 1 - Delta (ITM Prob)
        # But this is "selling" a call.
        # Simple Ideal Bid calculation:
        abs_delta = abs(delta)
        if (1.0 - abs_delta) > 0.1:
            required_ann_ret = required_exp_ret / (1.0 - abs_delta)
        else:
            required_ann_ret = required_exp_ret / 0.1
            
        ideal_bid = opt.strike * required_ann_ret * (dte / 365.0)
        
        ratio = 0.0
        if ideal_bid > 0: ratio = bid / ideal_bid
        
        # Sharpe
        sharpe = 0.0
        if conservative_vol > 0.01:
            sharpe = (yield_annual - RISK_FREE_RATE) / conservative_vol
            
        # Kelly %
        # Annual Return here is (Bid / StockPrice) * (365/DTE) which is basically yield
        # We use yield_annual as "Annualized Return"
        kelly_pct = 0.0
        expected_ret = yield_annual * (1 - abs_delta)
        if expected_ret > RISK_FREE_RATE and conservative_vol > 0:
             fraction = (expected_ret - RISK_FREE_RATE) / (conservative_vol ** 2)
             kelly_pct = max(0, fraction * 0.5)

        # Modified filter: > 10% OR better than current position's yield
        threshold = 0.10
        if min_yield_to_beat > 0 and min_yield_to_beat < 0.10:
             threshold = min_yield_to_beat 
        
        if yield_annual > threshold:
            scan_results.append({
                'expiry': opt.lastTradeDateOrContractMonth,
                'dte': dte,
                'strike': opt.strike,
                'bid': bid,
                'yield': yield_annual,
                'buffer': buffer_pct,
                'ideal': ideal_bid,
                'ratio': ratio,
                'sharpe': sharpe,
                'iv': iv,
                'delta': delta,
                'kelly': kelly_pct
            })
            
    scan_results.sort(key=lambda x: x['yield'], reverse=True)

    # --- 3. 統一輸出報表 (Final Printing) ---
    print("\n" + "="*80)
    print(f"COVERED CALL REPORT: {SYMBOL} (Price: {current_price:.2f})")
    print("="*80)
    
    print(f"\n[1] Current Positions Analysis")
    print(f"Src| {'Expiry':<10} | {'Strike':<8} | {'Cost':<6} | {'Curr':<6} | {'Profit%':<8} | {'Res.Yield%':<10} | {'Action':<10}")
    print("-" * 80)
    for row in position_report:
        print(row)
        
    print(f"\n[2] Rolling Opportunities (Yield Enhancement)")
    # Updated Header
    print(f"{'Expiry':<10} | {'DTE':<4} | {'Strike':<8} | {'Price':<8} | {'Qty':<4} | {'P/L%':<7} | {'Ann%':<6} | {'Ideal':<6} | {'Ratio':<5} | {'Shp':<4} | {'IV':<5} | {'Delta':<5} | {'Kelly%':<6}")
    print("-" * 115)
    
    top_n = 15
    for res in scan_results[:top_n]:
        is_good = res['yield'] > 0.15 and res['buffer'] > 0.08
        
        # Format rows
        qty_str = "-"
        pl_str = "-"
        row_str = f"{res['expiry']:<10} | {res['dte']:<4} | {res['strike']:<8.1f} | {res['bid']:<8.2f} | {qty_str:<4} | {pl_str:<7} | {res['yield']*100:<6.1f} | {res['ideal']:<6.2f} | {res['ratio']:<5.2f} | {res['sharpe']:<4.2f} | {res['iv']:<5.2f} | {res['delta']:<5.2f} | {res['kelly']*100:<6.1f}"
        
        if is_good:
            print(f"{COLOR_GREEN}{row_str}{COLOR_RESET}")
        else:
            print(f"{row_str}")
    print("="*115 + "\n")

def main():
    parser = argparse.ArgumentParser(description='Covered Call Manager')
    parser.add_argument('--symbol', type=str, default='PYPL', help='Symbol to analyze (default: PYPL)')
    args = parser.parse_args()
    
    global SYMBOL
    SYMBOL = args.symbol.upper()
    
    ib = connect_ib()
    try:
        price = get_current_price(ib, SYMBOL)
        if price <= 0:
            print("Cannot get underlying price.")
            return
            
        # 1. 檢視持倉
        positions = get_moomoo_positions(ib, SYMBOL)
        rh_positions = get_robinhood_positions(ib, SYMBOL)
        if rh_positions:
            positions.extend(rh_positions)
            
        if not positions:
            print("No existing Call positions found.")
            # 就算沒持倉，也直接進去掃描新機會
        
        # Calculate HV
        stk = Stock(SYMBOL, "SMART", "USD")
        ib.qualifyContracts(stk)
        hv_1w, hv_6m = get_historical_volatility(ib, stk)
            
        # 2. 分析與推薦
        analyze_rolling_opportunities(ib, price, positions, hv_1w, hv_6m)
        
    finally:
        ib.disconnect()

if __name__ == "__main__":
    main()

