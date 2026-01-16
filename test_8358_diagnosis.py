#!/usr/bin/env python3
"""
8358 問題診斷與解決方案測試
"""

import os
import sys
import time
from dotenv import load_dotenv
import shioaji as sj

load_dotenv()

SIMULATION = True
CA_API_KEY = os.getenv("Sinopack_CA_API_KEY")
CA_SECRET_KEY = os.getenv("Sinopack_CA_SECRET_KEY")

if not CA_API_KEY or not CA_SECRET_KEY:
    print("錯誤：請設定環境變數")
    sys.exit(1)

# Order callback to capture errors
order_results = {}

def order_callback(topic, quote):
    """訂單回報 callback"""
    try:
        raw = quote if isinstance(quote, dict) else quote.__dict__ if hasattr(quote, '__dict__') else {}
        op = raw.get("operation", {}) or {}
        op_code = str(op.get("op_code", ""))
        op_msg = str(op.get("op_msg", ""))
        
        order_id = raw.get("order_id") or raw.get("id") or raw.get("order", {}).get("id", "")
        code = raw.get("code") or raw.get("contract", {}).get("code", "")
        
        if op_code and op_code != "00":
            order_results[str(order_id)] = {
                "code": str(code),
                "op_code": op_code,
                "op_msg": op_msg,
                "status": "Failed"
            }
            print(f"[ORDER_CB] stat=Failed id={order_id} op_code={op_code} op_msg={op_msg} contract={code}")
        else:
            order_results[str(order_id)] = {
                "code": str(code),
                "op_code": op_code,
                "op_msg": op_msg,
                "status": "OK"
            }
            print(f"[ORDER_CB] stat=OK id={order_id} op_code={op_code}")
    except Exception as e:
        print(f"[ORDER_CB] Error: {e}")

def main():
    print("=== 8358 問題診斷 ===")
    
    # 登入
    api = sj.Shioaji(simulation=SIMULATION)
    api.login(
        api_key=CA_API_KEY,
        secret_key=CA_SECRET_KEY,
        contracts_cb=lambda x: print(f"  Load {x} done.") if x else None,
    )
    api.fetch_contracts()
    api.set_order_callback(order_callback)
    
    code = "8358"
    
    # 1. 取得合約
    print(f"\n[1] 取得合約 {code}...")
    try:
        contract = api.Contracts.Stocks[code]
        print(f"  ✓ 合約: {contract.exchange}:{contract.code} {contract.name}")
    except Exception as e:
        print(f"  ✗ 失敗: {e}")
        return
    
    # 2. 檢查倉位
    print(f"\n[2] 檢查倉位...")
    try:
        positions = api.list_positions(api.stock_account)
        for p in positions:
            if str(p.code) == code:
                qty = int(p.quantity)
                direction = p.direction
                print(f"  倉位: {qty} {direction}")
                break
    except Exception as e:
        print(f"  ✗ 失敗: {e}")
    
    # 3. 測試多種下單方式
    print(f"\n[3] 測試下單方式...")
    
    test_cases = [
        {
            "name": "MKT + ROD + Common",
            "price": 0.0,
            "price_type": sj.constant.StockPriceType.MKT,
            "order_type": sj.constant.OrderType.ROD,
            "order_lot": sj.constant.StockOrderLot.Common,
        },
        {
            "name": "MKT + IOC + Common",
            "price": 0.0,
            "price_type": sj.constant.StockPriceType.MKT,
            "order_type": sj.constant.OrderType.IOC,
            "order_lot": sj.constant.StockOrderLot.Common,
        },
        {
            "name": "LMT + IOC + Common (參考價)",
            "price": float(getattr(contract, "reference_price", 258.5) or 258.5),
            "price_type": sj.constant.StockPriceType.LMT,
            "order_type": sj.constant.OrderType.IOC,
            "order_lot": sj.constant.StockOrderLot.Common,
        },
    ]
    
    for i, test in enumerate(test_cases, 1):
        print(f"\n  測試 {i}: {test['name']}")
        try:
            order = api.Order(
                action=sj.constant.Action.Buy,
                price=test["price"],
                quantity=1,  # 只測試 1 張
                price_type=test["price_type"],
                order_type=test["order_type"],
                order_lot=test["order_lot"],
                account=api.stock_account,
            )
            
            print(f"    送出訂單...")
            trade = api.place_order(contract, order)
            order_id = str(trade.order.id)
            print(f"    Order ID: {order_id}")
            
            # 等待 callback
            time.sleep(2)
            
            if order_id in order_results:
                result = order_results[order_id]
                if result["status"] == "Failed":
                    print(f"    ✗ 失敗: {result['op_code']} {result['op_msg']}")
                    if result['op_code'] == '88' or '無此' in result['op_msg']:
                        print(f"    ⚠ 確認：模擬環境中 8358 不可交易")
                else:
                    print(f"    ✓ 成功")
            else:
                print(f"    ? 未收到 callback（可能還在處理）")
                
        except Exception as e:
            print(f"    ✗ 異常: {e}")
            import traceback
            traceback.print_exc()
    
    # 4. 總結
    print(f"\n=== 總結 ===")
    print(f"合約存在: ✓")
    print(f"合約資訊: {contract.exchange}:{contract.code} {contract.name}")
    print(f"\n結論：")
    print(f"  如果所有下單方式都失敗（op_code=88），")
    print(f"  表示模擬環境中 8358 雖然合約存在，但被標記為不可交易。")
    print(f"  解決方案：標記為 is_ignored，跳過此倉位。")

if __name__ == "__main__":
    main()
