import robin_stocks.robinhood as r
import pyotp # 如果你有開啟雙重驗證 (2FA)

# --- 登入設定 ---
username = "yptsai@gmail.com"
password = "773250@Robinhood"
# 如果你有開啟 2FA，需要你的 TOTP 金鑰 (通常是一串像 'JBSWY3DPEHPK3PXP' 的代碼)
# 如果沒有開啟 2FA，登入時系統可能會發送簡訊驗證碼，程式會跳出輸入框讓你輸入
totp_key = None #"YOUR_2FA_TOTP_KEY_IF_ENABLED" 

if totp_key:
    totp = pyotp.TOTP(totp_key).now()
    login = r.login(username, password, mfa_code=totp)
else:
    login = r.login(username, password)

# --- 獲取持股資訊 ---
# build_holdings() 會回傳一個字典，包含你持有的每一檔股票詳細資訊
my_stocks = r.build_holdings()

print("=== 我的持股 ===")
for symbol, data in my_stocks.items():
    qty = data['quantity']
    price = data['price']
    equity = data['equity']
    percent = data['percentage']
    print(f"代碼: {symbol} | 股數: {qty} | 現價: ${price} | 總值: ${equity} | 佔比: {percent}%")

# --- 獲取期權持倉 (如果有) ---
# my_options = r.get_open_option_positions()
# print(my_options)