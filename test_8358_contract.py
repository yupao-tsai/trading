#!/usr/bin/env python3
"""
測試 8358 合約查詢問題
檢查為什麼會出現「無此商品代碼」錯誤
"""

import os
import sys
from dotenv import load_dotenv
import shioaji as sj

load_dotenv()

SIMULATION = True  # 使用模擬環境
CA_API_KEY = os.getenv("Sinopack_CA_API_KEY")
CA_SECRET_KEY = os.getenv("Sinopack_CA_SECRET_KEY")

if not CA_API_KEY or not CA_SECRET_KEY:
    print("錯誤：請設定 Sinopack_CA_API_KEY 和 Sinopack_CA_SECRET_KEY 在 .env 檔案中")
    sys.exit(1)

def try_get(container, key):
    """安全地取得字典或物件的值"""
    try:
        if hasattr(container, '__getitem__'):
            return container[key]
        elif hasattr(container, '__getattr__'):
            return getattr(container, key)
    except (KeyError, AttributeError, TypeError):
        pass
    return None

def scan_contracts(api, code: str):
    """掃描所有可能的合約查詢方式"""
    print(f"\n=== 掃描 8358 合約 ===")
    results = {}
    
    # 1. 直接查詢
    print(f"\n[1] 直接查詢: api.Contracts.Stocks['{code}']")
    try:
        c = api.Contracts.Stocks[code]
        results['direct'] = c
        print(f"  ✓ 成功: {c}")
        print(f"    Exchange: {getattr(c, 'exchange', 'N/A')}")
        print(f"    Code: {getattr(c, 'code', 'N/A')}")
        print(f"    Name: {getattr(c, 'name', 'N/A')}")
    except Exception as e:
        print(f"  ✗ 失敗: {e}")
        results['direct'] = None
    
    # 2. TSE 查詢
    print(f"\n[2] TSE 查詢: api.Contracts.Stocks.TSE['{code}']")
    try:
        if hasattr(api.Contracts.Stocks, 'TSE'):
            tse = api.Contracts.Stocks.TSE
            if code in tse:
                c = tse[code]
                results['tse'] = c
                print(f"  ✓ 成功: {c}")
                print(f"    Exchange: {getattr(c, 'exchange', 'N/A')}")
                print(f"    Code: {getattr(c, 'code', 'N/A')}")
                print(f"    Name: {getattr(c, 'name', 'N/A')}")
            else:
                print(f"  ✗ 不在 TSE 字典中")
                # 嘗試掃描
                found = False
                for k, v in list(tse.items())[:100]:  # 只掃描前 100 個
                    if hasattr(v, 'code') and str(getattr(v, 'code', '')) == code:
                        results['tse'] = v
                        print(f"  ✓ 找到 (key={k}): {v}")
                        found = True
                        break
                if not found:
                    print(f"  ✗ 掃描前 100 個未找到")
                    results['tse'] = None
        else:
            print(f"  ✗ TSE 屬性不存在")
            results['tse'] = None
    except Exception as e:
        print(f"  ✗ 失敗: {e}")
        results['tse'] = None
    
    # 3. OTC 查詢
    print(f"\n[3] OTC 查詢: api.Contracts.Stocks.OTC['{code}']")
    try:
        if hasattr(api.Contracts.Stocks, 'OTC'):
            otc = api.Contracts.Stocks.OTC
            if code in otc:
                c = otc[code]
                results['otc'] = c
                print(f"  ✓ 成功: {c}")
                print(f"    Exchange: {getattr(c, 'exchange', 'N/A')}")
                print(f"    Code: {getattr(c, 'code', 'N/A')}")
                print(f"    Name: {getattr(c, 'name', 'N/A')}")
            else:
                print(f"  ✗ 不在 OTC 字典中")
                # 嘗試掃描
                found = False
                for k, v in list(otc.items())[:100]:  # 只掃描前 100 個
                    if hasattr(v, 'code') and str(getattr(v, 'code', '')) == code:
                        results['otc'] = v
                        print(f"  ✓ 找到 (key={k}): {v}")
                        found = True
                        break
                if not found:
                    print(f"  ✗ 掃描前 100 個未找到")
                    results['otc'] = None
        else:
            print(f"  ✗ OTC 屬性不存在")
            results['otc'] = None
    except Exception as e:
        print(f"  ✗ 失敗: {e}")
        results['otc'] = None
    
    # 4. 掃描所有 Stocks
    print(f"\n[4] 掃描所有 Stocks (前 500 個)...")
    try:
        found = False
        count = 0
        for k, v in api.Contracts.Stocks.items():
            count += 1
            if count > 500:
                break
            if hasattr(v, 'code') and str(getattr(v, 'code', '')) == code:
                results['scanned'] = v
                print(f"  ✓ 找到 (key={k}): {v}")
                print(f"    Exchange: {getattr(v, 'exchange', 'N/A')}")
                found = True
                break
        if not found:
            print(f"  ✗ 前 500 個中未找到")
            results['scanned'] = None
    except Exception as e:
        print(f"  ✗ 失敗: {e}")
        results['scanned'] = None
    
    return results

def check_positions(api):
    """檢查實際倉位"""
    print(f"\n=== 檢查實際倉位 ===")
    try:
        stk_pos = api.list_positions(api.stock_account)
        print(f"股票倉位數量: {len(stk_pos)}")
        for p in stk_pos:
            code = str(getattr(p, "code", "") or "")
            qty = int(getattr(p, "quantity", 0) or 0)
            direction = getattr(p, "direction", None)
            print(f"  {code}: {qty} {direction}")
            if code == "8358":
                print(f"    ⚠ 找到 8358 倉位！")
                print(f"    Quantity: {qty}")
                print(f"    Direction: {direction}")
    except Exception as e:
        print(f"  ✗ 失敗: {e}")

def test_order(api, contract):
    """測試下單（不實際送出）"""
    print(f"\n=== 測試下單（不實際送出）===")
    if contract is None:
        print("  ✗ 沒有合約，無法測試")
        return
    
    try:
        # 建立訂單物件（不送出）
        order = api.Order(
            price=0.0,
            quantity=1,
            action=sj.constant.Action.Buy,
            price_type=sj.constant.StockPriceType.MKT,
            order_type=sj.constant.OrderType.ROD,
            order_lot=sj.constant.StockOrderLot.Common,
            account=api.stock_account,
        )
        print(f"  ✓ 訂單物件建立成功")
        print(f"    Contract: {contract}")
        print(f"    Order: {order}")
        print(f"    Account: {api.stock_account}")
    except Exception as e:
        print(f"  ✗ 建立訂單物件失敗: {e}")

def main():
    print("=== 8358 合約診斷工具 ===")
    print(f"模擬環境: {SIMULATION}")
    
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
    
    # 檢查實際倉位
    check_positions(api)
    
    # 掃描合約
    code = "8358"
    results = scan_contracts(api, code)
    
    # 找出可用的合約
    print(f"\n=== 總結 ===")
    valid_contract = None
    for method, contract in results.items():
        if contract is not None:
            print(f"✓ {method}: 找到合約")
            if valid_contract is None:
                valid_contract = contract
        else:
            print(f"✗ {method}: 未找到")
    
    if valid_contract:
        print(f"\n找到可用合約: {valid_contract}")
        print(f"  Exchange: {getattr(valid_contract, 'exchange', 'N/A')}")
        print(f"  Code: {getattr(valid_contract, 'code', 'N/A')}")
        print(f"  Name: {getattr(valid_contract, 'name', 'N/A')}")
        
        # 測試下單
        test_order(api, valid_contract)
    else:
        print(f"\n⚠ 所有方法都找不到 8358 的合約")
        print(f"這表示模擬環境中可能沒有 8358 的合約資料")
        print(f"但實際倉位中卻有 8358 的倉位，這是異常情況")
    
    # 檢查合約總數
    print(f"\n=== 合約統計 ===")
    try:
        total_stocks = len(api.Contracts.Stocks)
        print(f"總股票數: {total_stocks}")
        
        if hasattr(api.Contracts.Stocks, 'TSE'):
            tse_count = len(api.Contracts.Stocks.TSE)
            print(f"TSE 股票數: {tse_count}")
        
        if hasattr(api.Contracts.Stocks, 'OTC'):
            otc_count = len(api.Contracts.Stocks.OTC)
            print(f"OTC 股票數: {otc_count}")
    except Exception as e:
        print(f"統計失敗: {e}")
    
    print("\n=== 診斷完成 ===")

if __name__ == "__main__":
    main()
