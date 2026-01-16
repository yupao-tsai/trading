#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import datetime
from ib_insync import IB, Stock, Option, util
from typing import List, Optional, Dict, Tuple
import math
import statistics
import re
import argparse

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

def get_moomoo_positions(symbol_filter: str) -> List[Dict]:
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
            current = row.get('current_price', 0.0)
            if math.isnan(current) or current <= 0:
                current = row.get('nominal_price', 0.0)
            
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
                'qty': row['qty']
            })
            
    except Exception as e:
        print(f"Error getting positions: {e}")
        
    return positions

def analyze_rolling_opportunities(ib: IB, current_price: float, positions: List[Dict]):
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
            
        # Store row for later printing
        row_str = f"{pos['expiry']:<10} | {pos['strike']:<8} | {pos['cost']:<6.2f} | {pos['current']:<6.2f} | {pos['profit_pct']*100:<7.1f}% | {residual_yield*100:<9.1f}% | {action_color}{action:<10}{COLOR_RESET}"
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
                    if current_price * 1.05 <= strike <= current_price * 1.25:
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
    for opt, ticker in tickers:
        bid = 0.0
        if ticker.bid and ticker.bid > 0: bid = ticker.bid
        elif ticker.last and ticker.last > 0: bid = ticker.last
        elif ticker.close and ticker.close > 0: bid = ticker.close
        
        if bid <= 0.05: continue
        
        dte = (datetime.datetime.strptime(opt.lastTradeDateOrContractMonth, "%Y%m%d").date() - today).days
        yield_annual = (bid / current_price) * (365 / dte)
        buffer_pct = (opt.strike - current_price) / current_price
        
        if yield_annual > 0.10:
            scan_results.append({
                'expiry': opt.lastTradeDateOrContractMonth,
                'dte': dte,
                'strike': opt.strike,
                'bid': bid,
                'yield': yield_annual,
                'buffer': buffer_pct
            })
            
    scan_results.sort(key=lambda x: x['yield'], reverse=True)

    # --- 3. 統一輸出報表 (Final Printing) ---
    print("\n" + "="*80)
    print(f"COVERED CALL REPORT: {SYMBOL} (Price: {current_price:.2f})")
    print("="*80)
    
    print(f"\n[1] Current Positions Analysis")
    print(f"{'Expiry':<10} | {'Strike':<8} | {'Cost':<6} | {'Curr':<6} | {'Profit%':<8} | {'Res.Yield%':<10} | {'Action':<10}")
    print("-" * 80)
    for row in position_report:
        print(row)
        
    print(f"\n[2] Rolling Opportunities (Yield Enhancement)")
    print(f"{'Expiry':<10} | {'DTE':<4} | {'Strike':<8} | {'Bid':<6} | {'Ann.Yield%':<10} | {'Buffer%':<8} | {'Score'}")
    print("-" * 80)
    
    top_n = 15
    for res in scan_results[:top_n]:
        is_good = res['yield'] > 0.15 and res['buffer'] > 0.08
        row_str = f"{res['expiry']:<10} | {res['dte']:<4} | {res['strike']:<8.1f} | {res['bid']:<6.2f} | {res['yield']*100:<9.1f}% | {res['buffer']*100:<7.1f}% |"
        if is_good:
            print(f"{COLOR_GREEN}{row_str} ★★★{COLOR_RESET}")
        else:
            print(f"{row_str}")
    print("="*80 + "\n")

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
        positions = get_moomoo_positions(SYMBOL)
        if not positions:
            print("No existing Call positions found.")
            # 就算沒持倉，也直接進去掃描新機會
            
        # 2. 分析與推薦
        analyze_rolling_opportunities(ib, price, positions)
        
    finally:
        ib.disconnect()

if __name__ == "__main__":
    main()

