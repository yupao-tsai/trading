# -*- coding: utf-8 -*-
import os
import time
import sys
import shioaji as sj
from dotenv import load_dotenv

# 載入 .env
load_dotenv()

def verify():
    # Fix Shioaji write permission error in sandbox environment
    # Shioaji tries to write contracts to current directory
    # Set to a temp directory or ensure current dir is writable
    import tempfile
    os.environ["SJ_CONTRACTS_PATH"] = tempfile.gettempdir()
    
    print("=== [Step 1] 初始化 Shioaji (Simulation=True) ===")
    api = sj.Shioaji(simulation=True)
    
    print("=== [Step 2] 執行 Login ===")
    api_key = os.getenv("Sinopack_CA_API_KEY")
    secret_key = os.getenv("Sinopack_CA_SECRET_KEY")
    
    if not api_key or not secret_key:
        print("❌ 錯誤: 找不到 API KEY，請檢查 .env")
        return

    try:
        api.login(
            api_key=api_key,
            secret_key=secret_key,
            contracts_cb=lambda x: print(f"  [Callback] Contracts update: {x}"),
            subscribe_trade=True
        )
        print("✅ Login 指令已發送，等待 5 秒建立連線...")
        time.sleep(5)
    except Exception as e:
        print(f"❌ Login 失敗: {e}")
        return

    print("\n=== [Step 3] 查詢股票持倉 (Stock Positions) ===")
    try:
        # 使用 update_status 確保資料最新
        print("  正在同步帳戶狀態 (update_status)...")
        api.update_status(api.stock_account)
        time.sleep(1)
        
        positions = api.list_positions(api.stock_account)
        print(f"✅ 查詢成功，共 {len(positions)} 檔持倉：")
        for p in positions:
            code = getattr(p, "code", "N/A")
            qty = getattr(p, "quantity", 0)
            price = getattr(p, "price", 0)
            print(f"  - 股票 {code}: 數量={qty}, 成本={price}")
            
    except Exception as e:
        print(f"❌ 查詢股票持倉失敗: {e}")

    print("\n=== [Step 4] 查詢期貨持倉 (Future Positions) ===")
    try:
        # 使用 update_status
        api.update_status(api.futopt_account)
        time.sleep(1)
        
        positions = api.list_positions(api.futopt_account)
        print(f"✅ 查詢成功，共 {len(positions)} 檔持倉：")
        for p in positions:
            code = getattr(p, "code", "N/A")
            qty = getattr(p, "quantity", 0)
            price = getattr(p, "price", 0)
            print(f"  - 期貨 {code}: 數量={qty}, 成本={price}")

    except Exception as e:
        print(f"❌ 查詢期貨持倉失敗: {e}")

    print("\n=== [Step 5] 登出與結束 ===")
    try:
        api.logout()
        print("✅ 已登出")
    except:
        pass

if __name__ == "__main__":
    verify()
