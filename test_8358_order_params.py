#!/usr/bin/env python3
"""
測試 8358 放空倉位平倉的不同 order 參數組合
檢查是否需要設定 order_cond、daytrade_short 等參數
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
        
        order_results[str(order_id)] = {
            "code": str(code),
            "op_code": op_code,
            "op_msg": op_msg,
            "status": "Failed" if (op_code and op_code != "00") else "OK"
        }
        
        if op_code and op_code != "00":
            print(f"[ORDER_CB] ✗ Failed: id={order_id} op_code={op_code} op_msg={op_msg}")
        else:
            print(f"[ORDER_CB] ✓ OK: id={order_id}")
    except Exception as e:
        print(f"[ORDER_CB] Error: {e}")

def test_order_params(api, contract, action, test_name, **order_kwargs):
    """測試不同的 order 參數組合"""
    print(f"\n  [{test_name}]")
    print(f"    參數: {order_kwargs}")
    
    try:
        # 基本參數
        base_params = {
            "action": action,
            "price": 0.0,  # MKT
            "quantity": 1,  # 只測試 1 張
            "price_type": sj.constant.StockPriceType.MKT,
            "order_type": sj.constant.OrderType.ROD,
            "order_lot": sj.constant.StockOrderLot.Common,
            "account": api.stock_account,
        }
        
        # 合併參數
        base_params.update(order_kwargs)
        
        order = api.Order(**base_params)
        print(f"    建立訂單...")
        
        trade = api.place_order(contract, order)
        order_id = str(trade.order.id)
        print(f"    Order ID: {order_id}")
        
        # 等待 callback
        time.sleep(2)
        
        if order_id in order_results:
            result = order_results[order_id]
            if result["status"] == "Failed":
                print(f"    ✗ 失敗: {result['op_code']} {result['op_msg']}")
                return False, result['op_code'], result['op_msg']
            else:
                print(f"    ✓ 成功")
                return True, "00", "OK"
        else:
            print(f"    ? 未收到 callback（可能還在處理）")
            return None, None, None
            
    except Exception as e:
        print(f"    ✗ 異常: {e}")
        return False, "EXCEPTION", str(e)

def main():
    print("=== 8358 放空倉位平倉參數測試 ===")
    print(f"模擬環境: {SIMULATION}")
    print("⚠ 警告：這會實際送出訂單！")
    
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
    
    # 取得合約
    print(f"\n[取得合約] {code}...")
    try:
        contract = api.Contracts.Stocks[code]
        print(f"  ✓ {contract.exchange}:{contract.code} {contract.name}")
    except Exception as e:
        print(f"  ✗ 失敗: {e}")
        return
    
    # 檢查倉位
    print(f"\n[檢查倉位]...")
    try:
        positions = api.list_positions(api.stock_account)
        for p in positions:
            if str(p.code) == code:
                qty = int(p.quantity)
                direction = p.direction
                print(f"  倉位: {qty} {direction}")
                print(f"  → 需要買回 {qty} 張來平倉")
                break
    except Exception as e:
        print(f"  ✗ 失敗: {e}")
    
    # 測試不同的參數組合
    print(f"\n[測試] 不同的 order 參數組合...")
    
    # 因為是放空倉位（負的），要平倉需要買回（Action.Buy）
    action = sj.constant.Action.Buy
    
    test_cases = [
        {
            "name": "預設（無特殊參數）",
            "params": {}
        },
        {
            "name": "現股 (Cash)",
            "params": {
                "order_cond": sj.constant.StockOrderCond.Cash
            }
        },
        {
            "name": "融資 (MarginTrading)",
            "params": {
                "order_cond": sj.constant.StockOrderCond.MarginTrading
            }
        },
        {
            "name": "融券 (ShortSelling)",
            "params": {
                "order_cond": sj.constant.StockOrderCond.ShortSelling
            }
        },
        {
            "name": "當沖賣出關閉 (daytrade_short=False)",
            "params": {
                "daytrade_short": False,
                "order_cond": sj.constant.StockOrderCond.Cash
            }
        },
        {
            "name": "當沖賣出開啟 (daytrade_short=True)",
            "params": {
                "daytrade_short": True,
                "order_cond": sj.constant.StockOrderCond.Cash
            }
        },
        {
            "name": "現股 + 當沖關閉",
            "params": {
                "order_cond": sj.constant.StockOrderCond.Cash,
                "daytrade_short": False
            }
        },
    ]
    
    success_count = 0
    for i, test in enumerate(test_cases, 1):
        print(f"\n測試 {i}/{len(test_cases)}: {test['name']}")
        ok, op_code, op_msg = test_order_params(api, contract, action, test['name'], **test['params'])
        
        if ok:
            success_count += 1
            print(f"  ✓✓✓ 成功！這個參數組合可以下單")
            break
        elif ok is None:
            print(f"  ? 未確定（需要更長時間等待）")
    
    # 總結
    print(f"\n=== 總結 ===")
    if success_count > 0:
        print(f"✓ 找到可以下單的參數組合！")
    else:
        print(f"✗ 所有參數組合都失敗")
        print(f"  可能原因：")
        print(f"  1. 模擬環境中 8358 被標記為不可交易")
        print(f"  2. 需要其他特殊參數")
        print(f"  3. 模擬環境的限制")

if __name__ == "__main__":
    main()
