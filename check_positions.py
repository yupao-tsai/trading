# -*- coding: utf-8 -*-
import os
import ssl
# Bypass SSL verification for legacy macOS Python environments
ssl._create_default_https_context = ssl._create_unverified_context
import shioaji as sj
import pandas as pd
from dotenv import load_dotenv

# 1. 載入環境變數
load_dotenv()

# 設定
SIMULATION = True  # 與 arbitrage_v5 保持一致
PERSON_ID = os.getenv("Sinopack_PERSON_ID")
CA_API_KEY = os.getenv("Sinopack_CA_API_KEY")
CA_SECRET_KEY = os.getenv("Sinopack_CA_SECRET_KEY")
CA_PATH = os.getenv("Sinopack_CA_PATH")
CA_PASSWORD = os.getenv("Sinopack_CA_PASSWORD")

def main():
    print("=== Shioaji Position Checker ===")
    print(f"Mode: {'SIMULATION' if SIMULATION else 'REAL'}")
    
    api = sj.Shioaji(simulation=SIMULATION)
    
    import time
    
    # 2. 登入
    print("\n[Login] Connecting...")
    try:
        api.login(
            api_key=CA_API_KEY, 
            secret_key=CA_SECRET_KEY, 
            contracts_cb=lambda x: print(f"  Load {x} done.")
        )
        print("[Login] Success.")

    except Exception as e:
        print(f"[Login] Failed: {e}")
        return

    def get_positions_with_retry(acc):
        for i in range(3):
            try:
                print(f"  [Attempt {i+1}] List Code...")
                return api.list_positions(account=acc)
            except Exception as e:
                print(f"  [Attempt {i+1}] Retry due to: {e}")
                time.sleep(2.0)
        return []

    # 3. 檢查庫存
    print("\n[Query] Fetching positions...")
    
    # --- Stock Positions ---
    print("\n--- 1. Stock Positions (現貨) ---")
    try:
        stk_acc = api.stock_account
        print(f"Account: {stk_acc}")
        
        positions = get_positions_with_retry(stk_acc)
        
        if not positions:
            print("  (No Stock Positions or Fetch Failed)")
        else:
            try:
                df = pd.DataFrame([p.__dict__ for p in positions])
                print(f"  Count: {len(positions)}")
                print(df.to_string())
            except:
                print(f"  (Raw): {positions}")
            
    except Exception as e:
        print(f"  Error fetching stock positions: {e}")

    # --- Future Positions ---
    print("\n--- 2. Future Positions (期貨) ---")
    try:
        fut_acc = api.futopt_account
        print(f"Account: {fut_acc}")
        
        positions = get_positions_with_retry(fut_acc)
        
        if not positions:
            print("  (No Future Positions or Fetch Failed)")
        else:
            # 轉換為 DataFrame 方便閱讀
            data = []
            for p in positions:
                item = {
                    "code": getattr(p, "code", ""),
                    "direction": getattr(p, "direction", ""),
                    "quantity": getattr(p, "quantity", 0),
                    "price": getattr(p, "price", 0.0),
                    "pnl": getattr(p, "pnl", 0.0),
                    "type": type(p).__name__
                }
                data.append(item)
            
            df = pd.DataFrame(data)
            print(f"  Count: {len(positions)}")
            print(df.to_string())

    except Exception as e:
        print(f"  Error fetching future positions: {e}")

    # 4. 登出
    print("\n[System] Logging out...")
    try:
        time.sleep(1.0)
        api.logout()
        print("[System] Logout success.")
    except Exception as e:
        print(f"[System] Logout ignored error: {e}")
    print("\n[System] Done.")

if __name__ == "__main__":
    main()

