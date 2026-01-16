import shioaji as sj
import os

def test_login():
    # 使用您在 arbitrage_v2.py 中設定的帳號資訊
    PERSON_ID = "S122352189"
    PASSWORD = "baupas123"
    CA_PATH = "/Users/yptsai/Documents/certificate_key/Sinopac.pfx"
    CA_PASSWORD = "S122352189"
    CA_API_KEY = "CwXchQfoSPgUhKaRJDwv8LBJVyJwHcLWBwKazrJ23rdm"
    CA_SECRET_KEY = "A934Lj5kRZRh2sWxLvP66qDGKk3qWdYmdcJhFJUYBEZ"
    

    print("="*50)
    print(f"正在測試 Shioaji 登入...")
    print(f"Person ID: {PERSON_ID}")
    print(f"CA Path: {CA_PATH}")
    
    # 檢查憑證檔案是否存在
    if not os.path.exists(CA_PATH):
        print(f"錯誤: 找不到憑證檔案: {CA_PATH}")
        return

    api = sj.Shioaji()

    try:
        # 1. 登入
        print("-" * 20)
        print("1. 執行 api.login()...")
        accounts = api.login(api_key=CA_API_KEY, secret_key=CA_SECRET_KEY)
        print("登入成功！")
        print("帳號列表:")
        for acc in accounts:
            print(f"  - {acc}")

        # 2. 憑證驗證
        print("-" * 20)
        print("2. 執行 api.activate_ca()...")
        result = api.activate_ca(
            ca_path=CA_PATH,
            ca_passwd=CA_PASSWORD,
            person_id=PERSON_ID
        )
        print(f"憑證驗證結果: {result}")
        print("憑證驗證成功！")
        
        print("="*50)
        print("測試完成：登入與憑證皆正常。")

    except Exception as e:
        print("="*50)
        print(f"發生錯誤: {e}")
        print("請檢查帳號、密碼或憑證路徑是否正確。")

if __name__ == "__main__":
    test_login()
