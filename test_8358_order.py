#!/usr/bin/env python3
"""
測試 8358 實際下單
檢查為什麼會出現「無此商品代碼」錯誤
"""

import os
import sys
import time
from dotenv import load_dotenv
import shioaji as sj

load_dotenv()

SIMULATION = True  # 使用模擬環境
CA_API_KEY = os.getenv("Sinopack_CA_API_KEY")
CA_SECRET_KEY = os.getenv("Sinopack_CA_SECRET_KEY")

if not CA_API_KEY or not CA_SECRET_KEY:
    print("錯誤：請設定 Sinopack_CA_API_KEY 和 Sinopack_CA_SECRET_KEY 在 .env 檔案中")
    sys.exit(1)

def test_place_order(api, contract, action, qty=1):
    """測試實際下單"""
    print(f"\n=== 測試下單 ===")
    print(f"Contract: {contract}")
    print(f"Action: {action}")
    print(f"Quantity: {qty}")
    
    try:
        # 建立訂單
        order = api.Order(
            price=0.0,  # MKT order
            quantity=qty,
            action=action,
            price_type=sj.constant.StockPriceType.MKT,
            order_type=sj.constant.OrderType.ROD,
            order_lot=sj.constant.StockOrderLot.Common,
            account=api.stock_account,
        )
        print(f"Order created: {order}")
        
        # 實際送出訂單
        print(f"\n[送出] 送出訂單...")
        trade = api.place_order(contract, order)
        print(f"Trade response: {trade}")
        print(f"Order ID: {trade.order.id}")
        print(f"Status: {trade.status}")
        
        # 等待一下看結果
        print(f"\n[等待] 等待 3 秒看訂單狀態...")
        time.sleep(3)
        
        return trade
        
    except Exception as e:
        print(f"  ✗ 下單失敗: {e}")
        print(f"  錯誤類型: {type(e).__name__}")
        import traceback
        traceback.print_exc()
        return None

def check_order_status(api, order_id):
    """檢查訂單狀態"""
    print(f"\n=== 檢查訂單狀態 ===")
    print(f"Order ID: {order_id}")
    
    try:
        # 嘗試取得訂單狀態
        orders = api.list_orders(api.stock_account)
        print(f"總訂單數: {len(orders)}")
        for o in orders:
            oid = str(getattr(o, "id", "") or "")
            if oid == str(order_id):
                print(f"  ✓ 找到訂單: {o}")
                status = getattr(o, "status", "N/A")
                print(f"    Status: {status}")
                return o
        print(f"  ✗ 未找到訂單 {order_id}")
    except Exception as e:
        print(f"  ✗ 檢查失敗: {e}")

def main():
    print("=== 8358 實際下單測試 ===")
    print(f"模擬環境: {SIMULATION}")
    print("⚠ 警告：這會實際送出訂單！")
    
    # 登入
    print("\n[登入] 連接 Shioaji...")
    api = sj.Shioaji(simulation=SIMULATION)
    
    try:
        api.login(
            api_key=CA_API_KEY,
            secret_key=CA_SECRET_KEY,
            contracts_cb=lambda x: print(f"  Load {x} done.") if x else None,
        )
        print("[登入] 成功")
    except Exception as e:
        print(f"[登入] 失敗: {e}")
        return
    
    # Fetch contracts
    print("\n[載入] 取得合約...")
    try:
        api.fetch_contracts()
        print("[載入] 完成")
    except Exception as e:
        print(f"[載入] 失敗: {e}")
    
    # 取得合約
    code = "8358"
    print(f"\n[取得合約] 查詢 {code}...")
    try:
        contract = api.Contracts.Stocks[code]
        print(f"✓ 找到合約: {contract}")
        print(f"  Exchange: {getattr(contract, 'exchange', 'N/A')}")
        print(f"  Code: {getattr(contract, 'code', 'N/A')}")
        print(f"  Name: {getattr(contract, 'name', 'N/A')}")
    except Exception as e:
        print(f"✗ 取得合約失敗: {e}")
        return
    
    # 檢查倉位
    print(f"\n[檢查倉位] 查看當前 8358 倉位...")
    try:
        stk_pos = api.list_positions(api.stock_account)
        for p in stk_pos:
            p_code = str(getattr(p, "code", "") or "")
            if p_code == code:
                qty = int(getattr(p, "quantity", 0) or 0)
                direction = getattr(p, "direction", None)
                print(f"  當前倉位: {qty} {direction}")
                if direction == sj.constant.Action.Sell:
                    print(f"  → 需要買回 {qty} 張來平倉")
                    # 測試買回
                    trade = test_place_order(api, contract, sj.constant.Action.Buy, qty)
                    if trade:
                        check_order_status(api, trade.order.id)
                break
    except Exception as e:
        print(f"  檢查倉位失敗: {e}")
    
    print("\n=== 測試完成 ===")

if __name__ == "__main__":
    main()
