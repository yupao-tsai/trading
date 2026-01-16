import shioaji as sj
import os
from dotenv import load_dotenv
import time

load_dotenv()

api = sj.Shioaji(simulation=True)
api.login(
    api_key=os.getenv("Sinopack_CA_API_KEY"),
    secret_key=os.getenv("Sinopack_CA_SECRET_KEY"),
)

print("Logged in. Waiting for sync...")
time.sleep(3.0)

CODE = "6239" # 力成 (TSE)
QTY = 1

print(f"Fetching contract for {CODE}...")
try:
    contract = api.Contracts.Stocks[CODE]
    print(f"Contract Found: {contract}")
    print(f"Exchange: {contract.exchange}")
except Exception as e:
    print(f"Contract Lookup Failed: {e}")
    exit(1)

print(f"Placing Test Order (Buy {QTY} share of {CODE})...")
try:
    order = api.Order(
        price=10.0, # Low price for Limit, or use MKT if testing MKT
        quantity=QTY, 
        action=sj.constant.Action.Buy,
        price_type=sj.constant.StockPriceType.LMT,
        order_type=sj.constant.OrderType.ROD, 
        order_lot=sj.constant.StockOrderLot.IntradayOdd, # Odd lot (shares)
        account=api.stock_account
    )
    
    # We use IntradayOdd (Zero share) because QTY=1. 
    # The bot uses Common (1000 shares) for repair. 
    # Let's try to match the Bot's logic exactly? 
    # Bot uses Common (ROUND lot). So let's try 1000 shares (1 lot).
    # SIMULATION money is fake, so it's fine.
    
    print("  -> Constructing Order Object (Common Lot)...")
    order_common = api.Order(
        price=0,
        quantity=1, # 1 Lot = 1000 shares
        action=sj.constant.Action.Sell, # Try SELL like the bot
        price_type=sj.constant.StockPriceType.MKT, # Try MKT like the bot
        order_type=sj.constant.OrderType.IOC, # Try IOC like the bot
        order_lot=sj.constant.StockOrderLot.Common,
        account=api.stock_account
    )
    
    print("  -> Sending Order...")
    trade = api.place_order(contract, order_common)
    print(f"Order Sent! Trade Object: {trade}")
    print(f"Order Status: {trade.status}")
    print(f"Order Msg: {trade.order.msg if hasattr(trade.order, 'msg') else ''}")
    
    time.sleep(2.0)
    api.update_status()
    print("Final Status Check:")
    # need to find the order in updates
    
except Exception as e:
    print(f"Order Placement Failed: {e}")

input("Press Enter to Logout...")
api.logout()
