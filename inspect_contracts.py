import shioaji as sj
import os
from dotenv import load_dotenv

load_dotenv()

def inspect():
    import tempfile
    os.environ["SJ_CONTRACTS_PATH"] = tempfile.gettempdir()
    
    api = sj.Shioaji(simulation=True)
    api.login(
        api_key=os.getenv("Sinopack_CA_API_KEY"),
        secret_key=os.getenv("Sinopack_CA_SECRET_KEY"),
    )
    print("Login success. Fetching contracts...")
    api.fetch_contracts(contract_download=True)

    print("\n=== Inspecting Stock 8358 ===")
    try:
        stk = api.Contracts.Stocks['8358']
        print(f"Code: {stk.code}")
        print(f"Name: {stk.name}")
        print(f"Category: {stk.category}")
        print(f"Ref Price: {stk.reference}")
    except Exception as e:
        print(f"Error: {e}")

    print("\n=== Inspecting Future QEFA6 ===")
    target_fut = None
    # Scan for QEFA6
    for cat in api.Contracts.Futures:
        for c in cat:
            if c.code == 'QEFA6':
                target_fut = c
                break
        if target_fut: break
    
    if target_fut:
        print(f"Code: {target_fut.code}")
        print(f"Name: {target_fut.name}")
        print(f"Underlying Code: {target_fut.underlying_code}")
        print(f"Delivery Month: {target_fut.delivery_date}")
        print(f"Sec Type: {target_fut.security_type}")
    else:
        print("Future QEFA6 not found in contracts.")

    print("\n=== Inspecting Future PQFA6 (Reference) ===")
    target_fut_p = None
    for cat in api.Contracts.Futures:
        for c in cat:
            if c.code == 'PQFA6':
                target_fut_p = c
                break
        if target_fut_p: break
    
    if target_fut_p:
        print(f"Code: {target_fut_p.code}")
        print(f"Name: {target_fut_p.name}")
        print(f"Underlying Code: {target_fut_p.underlying_code}")
    else:
        print("Future PQFA6 not found.")

    api.logout()

if __name__ == "__main__":
    inspect()
