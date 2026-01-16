#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import datetime
from ib_insync import IB, Stock, Option, util
from typing import List, Optional, Dict
import math
import statistics

# ====== 設定 ======
HOST = "127.0.0.1"
PORT = 7496
CLIENT_ID = 11

SYMBOL = "PYPL"
PRIMARY_EXCHANGE = "NYSE"
CURRENCY = "USD"

# 篩選條件
MIN_DTE = 7           # 最小到期天數
MAX_DTE = 60           # 最大到期天數
STRIKE_PCT_MIN = 0.88  # 履約價下限 (現價 * 0.88)
STRIKE_PCT_MAX = 0.98  # 履約價上限 (現價 * 0.96)
MIN_ANNUAL_RETURN = 0.03 # 最小年化報酬率過濾 (3%)

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
    for _ in range(20):
        ib.sleep(0.25)
        # 優先取 last, 如果沒有則取 close, 再沒有取 (bid+ask)/2
        price = ticker.last if ticker.last and ticker.last > 0 else ticker.close
        if price and price > 0:
            print(f"Current Price ({'Delayed' if MARKET_DATA_TYPE==3 else 'Live'}): {price}")
            return price
            
    print("⚠️ Warning: Could not get valid price, using ticker.close or 0")
    return ticker.close if ticker.close else 0.0

def get_option_chain(ib: IB, stk: Stock) -> Optional[object]:
    """取得選擇權鏈"""
    print("Fetching option chains...")
    chains = ib.reqSecDefOptParams(stk.symbol, "", stk.secType, stk.conId)
    if not chains:
        print("No option chains found.")
        return None
    
    # 優先選擇 SMART 且 tradingClass 符合預期 (通常是主代碼) 的鏈
    # 這裡簡單取 strike 數量最多的一個鏈
    chain = sorted(chains, key=lambda c: len(c.strikes), reverse=True)[0]
    print(f"Selected chain: exch={chain.exchange}, tradingClass={chain.tradingClass}, multiplier={chain.multiplier}")
    return chain

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
    chain, 
    current_price: float
) -> List[Option]:
    """根據演算法篩選適合 Sell Put 的合約"""
    
    today = datetime.date.today()
    candidates = []

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
        print(f"No expirations found between {MIN_DTE} and {MAX_DTE} days.")
        return []

    # 2. 篩選履約價 (Put: Strike < Current Price)
    # 計算目標履約價範圍
    target_strike_min = current_price * STRIKE_PCT_MIN
    target_strike_max = current_price * STRIKE_PCT_MAX
    
    valid_strikes = [
        s for s in chain.strikes 
        if target_strike_min <= s <= target_strike_max
    ]
    
    print(f"Scanning {len(valid_expiries)} expiries and {len(valid_strikes)} strikes...")
    print(f"Strike range: {target_strike_min:.2f} - {target_strike_max:.2f}")

    # 3. 建立合約物件
    for exp in valid_expiries:
        for strike in valid_strikes:
            # 建立 Option 合約 (尚未 qualify，為了速度先不 qualify 全部，等到要報價再處理)
            # 但為了 reqMktData 需要 conId，這裡建議先建立好基本資訊
            # 為了效率，我們這裡只建立物件，稍後批量 qualify 或逐個請求
            opt = Option(
                symbol=stk.symbol,
                lastTradeDateOrContractMonth=exp,
                strike=strike,
                right='P',
                exchange='SMART',
                currency=CURRENCY,
                tradingClass=chain.tradingClass
            )
            candidates.append(opt)

    print(f"Generated {len(candidates)} candidate contracts.")
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

def bs_put_delta(S, K, T, r, sigma):
    """計算 Put Delta (BS 模型)"""
    if T <= 0 or sigma <= 0: return 0.0
    d1 = (log(S/K) + (r + 0.5 * sigma**2) * T) / (sigma * sqrt(T))
    return norm.cdf(d1) - 1.0

def implied_volatility(price, S, K, T, r):
    """反推隱含波動率 (二分法)"""
    if price <= 0: return 0.0
    
    low = 0.001
    high = 5.0
    for _ in range(50):
        mid = (low + high) / 2
        est_price = bs_put_price(S, K, T, r, mid)
        if abs(est_price - price) < 0.001:
            return mid
        if est_price < price:
            low = mid
        else:
            high = mid
    return (low + high) / 2

# ---------------------------------------

def analyze_candidates(ib: IB, candidates: List[Option], current_price: float, hv_1w: float, hv_6m: float) -> List[Dict]:
    """對候選合約請求報價並計算指標"""
    results = []
    
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

            # 1. 優先使用 Bid (如果有且 > 0)
            if raw_bid and not math.isnan(raw_bid) and raw_bid > 0:
                final_price = raw_bid
                price_src = "B" # Bid
            # 2. 其次使用 Last (如果有且 > 0)
            elif last and not math.isnan(last) and last > 0:
                final_price = last
                price_src = "L" # Last
            # 3. 最後使用 Close (如果有且 > 0)
            elif close and not math.isnan(close) and close > 0:
                final_price = close
                price_src = "C" # Close
            
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

            if annualized_ret < MIN_ANNUAL_RETURN:
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
                "conId": opt.conId
            })
            
    # 根據 "溢價率" (Bid / Ideal) 排序 (高到低)
    # 這代表當前市場價 (Bid) 相對於我們算出的理想價 (Ideal) 溢價多少倍
    # 越高代表該筆交易越 "划算" (相對於風險)
    def premium_ratio(item):
        if item['ideal_bid'] <= 0: return 0.0
        return item['bid'] / item['ideal_bid']

    return sorted(results, key=premium_ratio, reverse=True)

def main():
    util.patchAsyncio()
    ib = connect_ib()
    
    # ANSI 顏色代碼
    COLOR_RED = "\033[91m"
    COLOR_GREEN = "\033[92m"
    COLOR_BLUE = "\033[94m"
    COLOR_RESET = "\033[0m"

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
        chain = get_option_chain(ib, stk)
        if not chain:
            return

        # 3. 篩選合約 (根據時間與價外程度)
        candidates = filter_options(ib, stk, chain, price)
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
        print(f"{'Expiry':<10} | {'DTE':<4} | {'Strike':<8} | {'Bid(Src)':<13} | {'Ret%':<6} | {'Ann%':<6} | {'Ideal':<8} | {'Ratio':<6} | {'IV':<6} | {'HV':<6} | {'Delta':<7} | {'Kelly%':<7}")
        print("-" * 150)
        
        # 只要年化報酬率 > 3% 的都顯示，不再只取前 20 筆
        # top_n = 20
        for res in ranked_results:
            # 再次確認過濾條件 (雖然 analyze_candidates 已過濾，但雙重確認無妨)
            if res['annualized'] < MIN_ANNUAL_RETURN:
                 continue

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
            
            # 顏色邏輯：
            # 1. 基礎條件：Ratio > 1.0 (否則不變色)
            # 2. 若滿足條件，根據 Delta 決定顏色：
            #    - Delta < 0.20: 紅色 (Red) - 非常安全
            #    - 0.20 <= Delta < 0.30: 綠色 (Green) - 適中
            #    - Delta >= 0.30: 藍色 (Blue) - 風險較高
            
            price_content = f"{res['bid']:.2f}({res['price_src']})"
            price_str = f"{price_content}"
            is_colored = False
            
            if ratio > 1.0:
                abs_delta = abs(res['delta'])
                if abs_delta < 0.20:
                    price_str = f"{COLOR_RED}{price_content} !{COLOR_RESET}"
                    is_colored = True
                elif 0.20 <= abs_delta < 0.30:
                    price_str = f"{COLOR_GREEN}{price_content} !{COLOR_RESET}"
                    is_colored = True
                elif abs_delta >= 0.30:
                    price_str = f"{COLOR_BLUE}{price_content} !{COLOR_RESET}"
                    is_colored = True

            # 為了對齊，因為 ANSI code 不佔寬度但佔字元，需手動調整 padding
            # 簡單做法：先算出無顏色的長度，再補空格
            # 原本格式: {price_str:<10}
            # 這裡我們手動排版
            base_len = len(price_content) + (2 if is_colored else 0) # "! " 加了2個字元
            padding = 13 - base_len # 預留 13 格 (對應表頭)
            if padding < 0: padding = 0
            
            final_price_str = price_str + " " * padding

            print(f"{res['expiry']:<10} | {res['dte']:<4} | {res['strike']:<8.1f} | {final_price_str} | {res['return']*100:<6.1f} | {res['annualized']*100:<6.1f} | {res['ideal_bid']:<8.2f} | {ratio:<6.2f} | {iv_str:<6} | {hv_str:<6} | {delta_str:>7} | {res['kelly']*100:>6.1f}%")
        
        if not ranked_results:
            print("No options passed the return criteria.")

    finally:
        ib.disconnect()
        print("\nDisconnected.")

if __name__ == "__main__":
    main()
