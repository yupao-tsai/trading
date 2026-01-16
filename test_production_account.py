import shioaji as sj
import pandas as pd
import datetime
import time
import os

# --- 設定 (與 arbitrage_v2.py 同步) ---
PERSON_ID = "S122352189"
PASSWORD = "baupas123"
CA_API_KEY = "CwXchQfoSPgUhKaRJDwv8LBJVyJwHcLWBwKazrJ23rdm"
CA_SECRET_KEY = "A934Lj5kRZRh2sWxLvP66qDGKk3qWdYmdcJhFJUYBEZ"
CA_PATH = "/Users/yptsai/Documents/certificate_key/Sinopac.pfx"
CA_PASSWORD = "S122352189"

def test_production_status():
    print("=== [Shioaji 正式環境帳號診斷工具] ===")
    print(f"API Key: {CA_API_KEY[:5]}...{CA_API_KEY[-5:]}")
    print(f"CA Path: {CA_PATH}")
    
    # 1. 初始化 (Simulation=False)
    api = sj.Shioaji(simulation=False)
    
    try:
        # 2. 登入
        print("\n1. [登入] 正在登入正式環境...")
        api.login(api_key=CA_API_KEY, secret_key=CA_SECRET_KEY, contracts_cb=lambda x: print(f"   [合約下載] {x}"))
        print("   -> 登入成功！")
        
        # 3. 啟動 CA
        print("\n2. [CA 驗證] 正在啟動 CA 憑證...")
        if not os.path.exists(CA_PATH):
            print(f"   [錯誤] CA 檔案不存在於: {CA_PATH}")
            return
            
        api.activate_ca(ca_path=CA_PATH, ca_passwd=CA_PASSWORD, person_id=PERSON_ID)
        print("   -> CA 憑證啟動成功！ (交易與憑證相關權限 OK)")
        
        # 4. 檢查帳號列表
        print("\n3. [帳號狀態] 檢查帳號列表與簽署狀態...")
        accounts = api.list_accounts()
        for acc in accounts:
            print(f"   - {acc}")
            if not acc.signed:
                print(f"     [警告] 帳號 {acc.account_id} 尚未簽署 (Signed=False)！")
        
        time.sleep(2) # 等待連線穩定
        
        # 5. 測試現貨行情 (Ticks)
        print("\n4. [現貨行情] 測試台積電 (2330) Ticks...")
        target_code = "2330"
        contract = api.Contracts.Stocks[target_code]
        print(f"   合約物件: {contract}")
        
        today_str = datetime.datetime.now().strftime('%Y-%m-%d')
        # 嘗試抓取 Ticks
        ticks = api.ticks(contract, date=today_str)
        df_ticks = pd.DataFrame({**ticks})
        
        if df_ticks.empty:
            print("   [失敗] Ticks 回傳空資料。")
            print("   -> 可能原因：1. 非盤中時間且無歷史Ticks權限 2. 尚未簽署行情同意書 3. API 行情權限未開通")
        else:
            print(f"   [成功] 抓到 {len(df_ticks)} 筆 Ticks。")
            print(f"   最新一筆: {df_ticks.iloc[-1].to_dict()}")

        # 6. 測試歷史 KBar
        print("\n5. [歷史資料] 測試台積電 (2330) KBar (近 5 日)...")
        end_date = datetime.datetime.now().strftime('%Y-%m-%d')
        start_date = (datetime.datetime.now() - datetime.timedelta(days=7)).strftime('%Y-%m-%d')
        
        kbars = api.kbars(contract, start=start_date, end=end_date)
        df_kbars = pd.DataFrame({**kbars})
        
        if df_kbars.empty:
            print("   [失敗] KBar 回傳空資料。")
            print("   -> 可能原因：帳號無歷史資料查詢權限 (需確認是否已開通)。")
        else:
            print(f"   [成功] 抓到 {len(df_kbars)} 筆 KBar。")
            print(df_kbars.tail(3))

        # 7. 測試期貨行情 (近月台指期)
        print("\n6. [期貨行情] 測試台指期 (TXF) ...")
        # 尋找近月台指期
        future_target = None
        for cat in api.Contracts.Futures.TXF:
            if cat.delivery_month == datetime.datetime.now().strftime('%Y%m'):
                future_target = cat
                break
        
        if future_target:
            print(f"   目標合約: {future_target.code} ({future_target.name})")
            # 嘗試抓 Snapshots
            snaps = api.snapshots([future_target])
            if snaps:
                print(f"   [成功] Snapshot: {snaps[0]}")
            else:
                print("   [失敗] Snapshot 回傳空值。")
        else:
            print("   [跳過] 找不到當月台指期合約。")

    except Exception as e:
        print(f"\n[嚴重錯誤] 測試過程中發生例外: {e}")
        import traceback
        traceback.print_exc()
    
    print("\n=== 診斷結束 ===")

if __name__ == "__main__":
    test_production_status()

