import os
import time
import shioaji as sj
from shioaji import constant as sj_constant
from shioaji.constant import Unit
from dotenv import load_dotenv

# --- Helpers from arb bot ---
def _iter_contract_container(container):
    if container is None:
        return
    if hasattr(container, "code") and getattr(container, "code", ""):
        yield container
        return
    try:
        keys = list(container.keys())
    except Exception:
        keys = None
    if keys is not None:
        for k in keys:
            try:
                v = container.get(k)
            except Exception:
                continue
            yield from _iter_contract_container(v)
        return
    try:
        for item in container:
            yield from _iter_contract_container(item)
    except Exception:
        return

def iter_future_contracts(api: sj.Shioaji):
    fut_root = api.Contracts.Futures
    for name in dir(fut_root):
        if name.startswith("_"): continue
        node = getattr(fut_root, name, None)
        if callable(node) or node is None: continue
        yield from _iter_contract_container(node)

def resolve_future_contract(api, code):
    # 1. Direct
    try:
        c = api.Contracts.Futures.get(code)
        if c: return c
    except: pass
    # 2. Search
    for c in iter_future_contracts(api):
        try:
            if getattr(c, "code", "") == code: return c
        except: pass
    return None
# ----------------------------

def main():
    print("=== Shioaji Unit Verification Test (V2) ===")
    
    load_dotenv()
    api_key = os.getenv("Sinopack_CA_API_KEY", "")
    secret_key = os.getenv("Sinopack_CA_SECRET_KEY", "")
    
    api = sj.Shioaji(simulation=True)
    try:
        api.login(api_key=api_key, secret_key=secret_key, subscribe_trade=False)
        print("[Login] Success. Fetching contracts...")
    except Exception as e:
        print(f"[Login] Failed: {e}")
        return

    # explicitly wait and check status
    for i in range(10):
        print(f"  Waiting for contracts ({i+1}/10)... Status: {api.Contracts.status}")
        if str(api.Contracts.status) == "FetchStatus.Fetched":
             break
        time.sleep(1)

    # 3. Search for TSMC Futures
    print("\n[Searching for TSMC Futures (台積電)]")
    found_count = 0
    for c in iter_future_contracts(api):
        name = getattr(c, 'name', '')
        if "台積電" in name:
            code = getattr(c, 'code', '')
            mult = getattr(c, 'multiplier', 'N/A')
            cat = getattr(c, 'category', 'N/A')
            print(f"  Found: {name} (Code: {code}) | Mult: {mult} | Cat: {cat}")
            found_count += 1
            if found_count >= 5: break
            
    if found_count == 0:
        print("  No '台積電' futures found in simulation contracts.")
    
    # 4. Search for Mini Futures (Small)
    print("\n[Searching for any 'Mini' Futures (小型)]")
    found_count = 0
    for c in iter_future_contracts(api):
        name = getattr(c, 'name', '')
        if "小型" in name and "台指" not in name: # Exclude Index if possible, or include
             code = getattr(c, 'code', '')
             mult = getattr(c, 'multiplier', 'N/A')
             print(f"  Found: {name} (Code: {code}) | Mult: {mult}")
             found_count += 1
             if found_count >= 3: break

    print("\n=== Test Complete ===")
    api.logout()

if __name__ == "__main__":
    main()
