import time
import pandas as pd
import numpy as np
from moomoo import *
from datetime import datetime, timedelta

# ====== 參數設定 ======
TARGET_SYMBOL = 'US.AAPL'   # 目標股票 (需加市場前綴 US.)
CHECK_INTERVAL = 5          # 檢查間隔 (秒)

# 篩選條件閾值
MIN_ANNUAL_RETURN = 0.05    # 年化報酬率 > 5%
MIN_STRIKE_DIFF = 0.05      # 履約價與現價差 > 5% (Out-of-the-Money 程度)
MAX_GAMMA = 0.3             # Gamma < 0.3
# IV 相關條件在邏輯中動態計算 (IV > Mean IV, IV > HV)

# 連線設定
HOST = '127.0.0.1'
PORT = 11111

def get_option_chain_data(ctx, symbol, current_price):
    """
    獲取選擇權鏈並篩選出 Put 合約，包含 Greeks 數據
    """
    # 1. 獲取選擇權鏈列表 (近月與次月，這裡先抓未來 3 個月內的過濾)
    # 為了簡化，我們先抓取所有日期的鏈，然後只處理 Put
    ret, chain_data = ctx.get_option_chain(stock=symbol, index_option_type=IndexOptionType.NORMAL)
    if ret != RET_OK:
        print(f"❌ Failed to get option chain: {chain_data}")
        return None

    # 過濾出 Put
    puts = chain_data[chain_data['option_type'] == OptionType.PUT]
    
    # 2. 初步過濾：履約價距離
    # 根據策略：Strike Diff > 5% (對於 Sell Put，通常是指 Strike < Current Price * 0.95)
    # Strike Price Diff % = (Current - Strike) / Current
    # 你的需求是 "差 > 5%"，如果是 OTM Put，Strike 應該比現價低
    target_strike_max = current_price * (1 - MIN_STRIKE_DIFF)
    
    # 篩選 Strike <= target_strike_max 的合約
    puts = puts[puts['strike_price'] <= target_strike_max]
    
    if puts.empty:
        print(f"No puts found with strike <= {target_strike_max:.2f}")
        return pd.DataFrame()

    # 3. 獲取即時報價與 Greeks (需要訂閱)
    # 為了避免一次訂閱太多導致額度不足，我們分批處理或只取前 N 個最接近的
    # 這裡示範取最接近 target_strike_max 的前 20 個合約來檢查
    puts = puts.sort_values('strike_price', ascending=False).head(20)
    
    codes = puts['code'].tolist()
    
    # 訂閱報價 (SubType.QUOTE 包含 Greeks 如 Gamma, IV)
    # 注意：需確保你的 OpenD 帳號有選擇權即時報價權限
    ret, err = ctx.subscribe(codes, [SubType.QUOTE], subscribe_push=False)
    if ret != RET_OK:
        print(f"❌ Subscribe failed: {err}")
        return pd.DataFrame()

    # 等待一點點時間讓訂閱生效 (有時需要)
    time.sleep(1)
    
    # 獲取即時數據
    ret, market_data = ctx.get_stock_quote(codes)
    if ret != RET_OK:
        print(f"❌ Failed to get quotes: {market_data}")
        return pd.DataFrame()

    # 合併靜態鏈資訊與動態報價
    # chain_data columns: [code, strike_price, expiry_date, ...]
    # market_data columns: [code, last_price, implied_volatility, gamma, ...]
    
    merged = pd.merge(puts, market_data, on='code', how='inner')
    return merged

def get_historical_volatility(ctx, symbol):
    """
    獲取歷史波動率 (HV)
    注意：Moomoo API 沒有直接給 'HV' 數值，通常需要自己算或從 snapshot 拿
    這裡嘗試從 get_stock_quote 的 snapshot 拿，如果沒有則需用 K 線計算
    為簡化，這裡暫時模擬或計算簡單的 30日 HV
    """
    # 簡單用過去 30 天收盤價計算 HV
    end_date = datetime.now().strftime("%Y-%m-%d")
    start_date = (datetime.now() - timedelta(days=45)).strftime("%Y-%m-%d")
    
    ret, k_data = ctx.get_history_kline(symbol, start=start_date, end=end_date, ktype=KLType.K_DAY)
    if ret != RET_OK:
        print("Failed to get K-line for HV")
        return 0.0
    
    if len(k_data) < 20:
        return 0.0
        
    # 計算 Log Return
    k_data['log_ret'] = np.log(k_data['close'] / k_data['close'].shift(1))
    # 標準差 * sqrt(252)
    hv = k_data['log_ret'].std() * np.sqrt(252) * 100 # 轉為百分比
    return hv

def calculate_metrics(df, current_price):
    """
    計算策略所需的指標
    """
    # 年化報酬率 (Annualized Return)
    # AR = (Premium / Strike) * (365 / Days_to_Expiry)
    # 這裡假設保證金約等於 Strike (Cash Secured Put)
    # 實際上分母應該是 (Strike - Premium) 或保證金，這裡用 Strike 當分母估算
    
    today = datetime.now().date()
    
    def get_days_to_expiry(expiry_str):
        exp = datetime.strptime(expiry_str, "%Y-%m-%d").date()
        days = (exp - today).days
        return max(1, days)

    df['days_to_expiry'] = df['expiry_date'].apply(get_days_to_expiry)
    
    # 計算年化 (Premium 使用 bid price 比較保守，如果沒有 bid 則用 last)
    # 注意 Moomoo 的價格單位
    price_col = 'last_price' # 也可以改用 (bid + ask) / 2
    
    df['annualized_return'] = (df[price_col] / df['strike_price']) * (365 / df['days_to_expiry'])
    
    return df

def main():
    print(f"Connecting to Moomoo OpenD at {HOST}:{PORT} ...")
    quote_ctx = OpenQuoteContext(host=HOST, port=PORT)
    
    try:
        # 1. 訂閱正股以獲取現價
        quote_ctx.subscribe([TARGET_SYMBOL], [SubType.QUOTE], subscribe_push=False)
        
        while True:
            print(f"\n[{datetime.now().strftime('%H:%M:%S')}] Scanning {TARGET_SYMBOL} options...")
            
            # A. 獲取現價
            ret, stock_data = quote_ctx.get_stock_quote([TARGET_SYMBOL])
            if ret != RET_OK:
                print("Failed to get stock price.")
                time.sleep(CHECK_INTERVAL)
                continue
                
            current_price = stock_data['last_price'][0]
            print(f"Current Price: {current_price}")
            
            # B. 獲取 HV (可以不用每次迴圈都算，但為了簡單先這樣寫)
            hv = get_historical_volatility(quote_ctx, TARGET_SYMBOL)
            print(f"Historical Volatility (30D): {hv:.2f}%")
            
            # C. 獲取選擇權數據
            options_df = get_option_chain_data(quote_ctx, TARGET_SYMBOL, current_price)
            
            if options_df is None or options_df.empty:
                print("No candidate options found initially.")
                time.sleep(CHECK_INTERVAL)
                continue
            
            # D. 計算衍生指標
            options_df = calculate_metrics(options_df, current_price)
            
            # E. 篩選邏輯
            # 1. Annualized Return > 5%
            # 2. Strike Diff > 5% (已在 get_option_chain_data 初步篩選)
            # 3. Gamma < 0.3
            # 4. IV > HV
            # 5. IV > Mean IV (這裡計算當前這批候選合約的 IV 平均)
            
            mean_iv = options_df['implied_volatility'].mean()
            print(f"Mean IV of candidates: {mean_iv:.2f}%")
            
            candidates = options_df[
                (options_df['annualized_return'] > MIN_ANNUAL_RETURN) &
                (options_df['gamma'] < MAX_GAMMA) &
                (options_df['implied_volatility'] > hv) &
                (options_df['implied_volatility'] > mean_iv)
            ].copy()
            
            if not candidates.empty:
                print(f"✅ Found {len(candidates)} valid candidates:")
                # 顯示重要欄位
                cols = ['code', 'expiry_date', 'strike_price', 'last_price', 
                        'annualized_return', 'implied_volatility', 'gamma']
                print(candidates[cols].to_string(index=False))
                
                # 在這裡可以加入下單邏輯 (place order)
                # best_option = candidates.iloc[0]
                # ...
            else:
                print("No options matched all criteria.")

            time.sleep(CHECK_INTERVAL)

    except KeyboardInterrupt:
        print("\nStopped.")
    finally:
        quote_ctx.close()

if __name__ == "__main__":
    main()


