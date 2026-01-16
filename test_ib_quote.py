#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from ib_insync import IB, Stock, Option, util
import sys
import datetime

# 設定
HOST = "127.0.0.1"
PORT = 7496
CLIENT_ID = 99  # 使用一個獨立的 Client ID

def main():
    ib = IB()
    print(f"Connecting to IB {HOST}:{PORT} clientId={CLIENT_ID}...")
    try:
        ib.connect(HOST, PORT, clientId=CLIENT_ID)
    except Exception as e:
        print(f"Connection failed: {e}")
        sys.exit(1)

    print("Connected.")
    
    # 1. 測試 underlying (PYPL)
    symbol = "PYPL"
    print(f"\n1. Testing Underlying: {symbol}")
    stk = Stock(symbol, "SMART", "USD")
    ib.qualifyContracts(stk)
    print(f"Contract Qualified: {stk}")
    
    ib.reqMarketDataType(3) # Delayed
    print("Requesting Delayed Market Data (Type 3)...")
    
    ticker_stk = ib.reqMktData(stk, "", False, False)
    
    print("Waiting for stock data...")
    import math
    for i in range(20): # increased wait time
        ib.sleep(0.5)
        # Check if last/bid/close is valid (not NaN and > 0)
        has_last = ticker_stk.last and not math.isnan(ticker_stk.last) and ticker_stk.last > 0
        has_bid = ticker_stk.bid and not math.isnan(ticker_stk.bid) and ticker_stk.bid > 0
        has_close = ticker_stk.close and not math.isnan(ticker_stk.close) and ticker_stk.close > 0
        
        if has_last or has_bid or has_close:
            print(f"Stock Data Received! Last={ticker_stk.last}, Bid={ticker_stk.bid}, Close={ticker_stk.close}")
            break
    else:
        print("❌ Stock data timeout.")

    ib.cancelMktData(stk)

    # 2. 測試 Option (SPY Put)
    print(f"\n2. Testing Option for {symbol}")
    
    # 找一個大概的平價或價外合約
    current_price = ticker_stk.last if ticker_stk.last else (ticker_stk.close if ticker_stk.close else 600.0)
    target_strike = int(current_price * 0.95) # 5% OTM
    
    # 找最近的到期日 (約 30 天後)
    target_date = datetime.date.today() + datetime.timedelta(days=30)
    # 轉換格式 YYYYMMDD (大略猜測一個週五)
    # 為了精確，我們使用 reqSecDefOptParams 找一個真實存在的合約
    
    print("Fetching option chains to find a valid contract...")
    chains = ib.reqSecDefOptParams(stk.symbol, "", stk.secType, stk.conId)
    
    valid_contract = None
    if chains:
        chain = chains[0] # 取第一個 chain (通常是 SMART)
        # 找一個合適的到期日和履約價
        exp = sorted(chain.expirations)[len(chain.expirations)//2] # 取中間的到期日
        strikes = [k for k in chain.strikes if k < current_price]
        if strikes:
            strike = strikes[-1] # 最接近現價的價外 Put
            
            # 正確的合約定義方式 (參考 main_union.py)
            valid_contract = Option(
                symbol=stk.symbol,
                lastTradeDateOrContractMonth=exp,
                strike=strike,
                right='P',
                exchange='SMART',
                currency='USD',
                multiplier=chain.multiplier # 這是關鍵
            )
            print(f"Constructed Contract: {valid_contract}")
    
    if valid_contract:
        try:
            ib.qualifyContracts(valid_contract)
            print(f"Option Qualified: {valid_contract.localSymbol} (ID: {valid_contract.conId})")
            
            print("Requesting Option Market Data...")
            ticker_opt = ib.reqMktData(valid_contract, "100,101,106", False, False) # Generic ticks
            
            for i in range(20):
                ib.sleep(0.5)
                if ticker_opt.bid or ticker_opt.last or ticker_opt.close:
                    print(f"✅ Option Data Received!")
                    print(f"   Bid: {ticker_opt.bid}")
                    print(f"   Ask: {ticker_opt.ask}")
                    print(f"   Last: {ticker_opt.last}")
                    print(f"   Close: {ticker_opt.close}")
                    if ticker_opt.modelGreeks:
                        print(f"   IV: {ticker_opt.modelGreeks.impliedVol}")
                        print(f"   Delta: {ticker_opt.modelGreeks.delta}")
                    break
            else:
                print("❌ Option data timeout. (Check subscription or delayed data permissions)")
                print(f"   Ticker state: {ticker_opt}")
                
        except Exception as e:
            print(f"Error qualifying/fetching option: {e}")
    else:
        print("Could not find a valid contract chain to test.")

    ib.disconnect()

if __name__ == "__main__":
    main()

