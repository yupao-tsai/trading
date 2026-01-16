import shioaji as sj
from shioaji import TickSTKv1, Exchange

# api = sj.Shioaji(simulation=True) # 模擬模式
# api.login(
#     api_key="CwXchQfoSPgUhKaRJDwv8LBJVyJwHcLWBwKazrJ23rdm",     # 請修改此處
#     secret_key="A934Lj5kRZRh2sWxLvP66qDGKk3qWdYmdcJhFJUYBEZ"   # 請修改此處
# )

# # 商品檔 - 請修改此處
# contract = api.Contracts.Stocks.TSE["2890"]

# # 證券委託單 - 請修改此處
# order = api.Order(
#     price=18,                                       # 價格
#     quantity=1,                                     # 數量
#     action=sj.constant.Action.Buy,                  # 買賣別
#     price_type=sj.constant.StockPriceType.LMT,      # 委託價格類別
#     order_type=sj.constant.OrderType.ROD,           # 委託條件
#     account=api.stock_account                       # 下單帳號
# )

# # 下單
# trade = api.place_order(contract, order)
# print(trade)

# input("Press Enter to continue...")

# # 商品檔 - 近月台指期貨, 請修改此處
# contract = min(
#     [
#         x for x in api.Contracts.Futures.TXF 
#         if x.code[-2:] not in ["R1", "R2"]
#     ],
#     key=lambda x: x.delivery_date
# )

# # 期貨委託單 - 請修改此處
# order = api.Order(
#     action=sj.constant.Action.Buy,                   # 買賣別
#     price=15000,                                     # 價格
#     quantity=1,                                      # 數量
#     price_type=sj.constant.FuturesPriceType.LMT,     # 委託價格類別
#     order_type=sj.constant.OrderType.ROD,            # 委託條件
#     octype=sj.constant.FuturesOCType.Auto,           # 倉別
#     account=api.futopt_account                       # 下單帳號
# )

# # 下單
# trade = api.place_order(contract, order)
# print(trade)

# input("Press Enter to continue...")

CA_PATH = "/Users/yptsai/Documents/certificate_key/Sinopac.pfx"
CA_PASSWORD = "S122352189"

api = sj.Shioaji(simulation=False)   # Production Mode
accounts = api.login(
    api_key="CwXchQfoSPgUhKaRJDwv8LBJVyJwHcLWBwKazrJ23rdm",     # 請修改此處
    secret_key="A934Lj5kRZRh2sWxLvP66qDGKk3qWdYmdcJhFJUYBEZ",   # 請修改此處
    contracts_cb=lambda security_type: print(f"{repr(security_type)} fetch done.")
)
api.activate_ca(
    ca_path=CA_PATH,
    ca_passwd=CA_PASSWORD,
)
print(accounts)

@api.on_tick_stk_v1()
def quote_callback(exchange: Exchange, tick:TickSTKv1):
    print(f"Exchange: {exchange}, Tick: {tick}")

input("Press Enter to continue...")

# api.quote.subscribe(api.Contracts.Stocks["2330"], quote_type="tick")
# api.quote.subscribe(api.Contracts.Stocks["2330"], quote_type="bidask")
# api.quote.subscribe(api.Contracts.Futures["TXFC0"], quote_type="tick")

# input("Press Enter to continue...")

# api.fetch_contracts(contract_download=True)
# print(api.Contracts)
print(api.Contracts.Stocks.TSE["2330"])
print(api.Contracts.Stocks["2330"])

api.quote.subscribe(
    api.Contracts.Stocks.TSE["2330"], 
    quote_type = sj.constant.QuoteType.Tick, 
    version = sj.constant.QuoteVersion.v1
)

input("Press Enter to continue...") 

ticks = api.ticks(
    contract=api.Contracts.Stocks["2330"], 
    date="2026-01-02",cb=quote_callback)
print(ticks)

input("Press Enter to continue...") 
# print(api.Contracts.Stocks["2890"])
# print(api.Contracts.Futures.TXF["TXF202601"])
# print(api.Contracts.Options.TXO["TXO202601"])
# print(api.Contracts.Options.TXO["TXO202601"])

kbars = api.kbars(
    contract=api.Contracts.Stocks.TSE["2330"], 
    start="2025-12-15", 
    end="2025-12-31", 
)
print(kbars)


input("Press Enter to continue...") 

snapshots=api.snapshots(
    contracts=[api.Contracts.Stocks.TSE["2330"]]
)
print(snapshots)