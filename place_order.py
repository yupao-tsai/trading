#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from ib_insync import IB, Stock, Option, LimitOrder, MarketOrder
from typing import Optional, List
import time

# ====== 參數設定 ======
HOST = "127.0.0.1"
PORT = 7496          # Paper: 7497, Live: 7496
CLIENT_ID = 12       # 避免跟 test.py 的 clientId 衝突

SYMBOL = "AAPL"
CURRENCY = "USD"
RIGHT = "P"          # 'P' put / 'C' call
TARGET_EXPIRY = "20260116"
TARGET_STRIKE = 250.0

# 測試用的下單數量與價格
ORDER_ACTION = "BUY" # 'BUY' or 'SELL'
QUANTITY = 1
# 使用 Limit Order 以免成交在意外價格，這裡設一個明顯無法成交的低價來測試掛單
LIMIT_PRICE = 0.05   
# =======================

def connect_ib() -> IB:
    ib = IB()
    print(f"Connecting to {HOST}:{PORT} clientId={CLIENT_ID} ...")
    ib.connect(HOST, PORT, clientId=CLIENT_ID)
    print("Connected.")
    return ib

def get_target_option(ib: IB) -> Optional[Option]:
    """
    直接嘗試抓取我們已經知道存在的 AAPL Put 選項
    """
    stk = Stock(SYMBOL, "SMART", CURRENCY)
    ib.qualifyContracts(stk)
    
    # 這裡直接構建 Option 合約，不再遍歷所有鏈 (假設我們已經從 test.py 確認過這個合約存在)
    opt = Option(
        symbol=stk.symbol,
        lastTradeDateOrContractMonth=TARGET_EXPIRY,
        strike=TARGET_STRIKE,
        right=RIGHT,
        exchange="SMART",
        currency=CURRENCY
    )
    
    print(f"Qualifying option: {opt.symbol} {opt.lastTradeDateOrContractMonth} {opt.right} {opt.strike}...")
    ib.qualifyContracts(opt)
    
    if opt.conId > 0:
        print(f"Found Option: {opt.localSymbol} (conId={opt.conId})")
        return opt
    else:
        print("Could not qualify option contract.")
        return None

def place_test_order(ib: IB, contract: Option):
    """
    下一個測試單 (Limit Order)
    """
    print(f"\nPlacing {ORDER_ACTION} Limit Order for {QUANTITY} contract(s) at ${LIMIT_PRICE}...")
    
    order = LimitOrder(ORDER_ACTION, QUANTITY, LIMIT_PRICE)
    # 若要下市價單: order = MarketOrder(ORDER_ACTION, QUANTITY)
    
    trade = ib.placeOrder(contract, order)
    
    print(f"Order placed. Trade status: {trade.orderStatus.status}")
    print("Waiting for order updates (Ctrl+C to cancel and exit)...")
    
    # 監控訂單狀態
    try:
        while not trade.isDone():
            ib.sleep(1)
            print(f"Status: {trade.orderStatus.status} | Filled: {trade.orderStatus.filled}/{trade.orderStatus.remaining} | LastFillPrice: {trade.orderStatus.avgFillPrice}")
            
            # 如果訂單狀態是 'Submitted' 或 'PreSubmitted'，表示已成功掛單
            if trade.orderStatus.status in ['Submitted', 'PreSubmitted']:
                # 為了演示，我們可以在這裡讓它跑一下，然後詢問用戶是否取消
                pass
                
    except KeyboardInterrupt:
        print("\nKeyboardInterrupt detected. Cancelling order...")
        ib.cancelOrder(order)
        # 等待取消確認
        while not trade.isDone():
            ib.sleep(0.5)
        print(f"Final Status: {trade.orderStatus.status}")

def main():
    ib = connect_ib()
    try:
        opt = get_target_option(ib)
        if opt:
            # 再次確認
            confirm = input(f"Ready to place LIMIT {ORDER_ACTION} order for {opt.localSymbol} @ {LIMIT_PRICE}? (y/n): ")
            if confirm.lower() == 'y':
                place_test_order(ib, opt)
            else:
                print("Operation cancelled.")
    finally:
        ib.disconnect()
        print("Disconnected.")

if __name__ == "__main__":
    main()


