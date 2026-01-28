
import os
import time
import shioaji as sj
from dotenv import load_dotenv

# No monkeypatch import here!

load_dotenv()

def test_crash():
    print("[Test] Initializing Shioaji (No Patch)...")
    api = sj.Shioaji(simulation=True)
    
    person_id = os.getenv("Sinopack_PERSON_ID") or os.getenv("SHIOAJI_USER_ID")
    ca_api_key = os.getenv("Sinopack_CA_API_KEY")
    ca_secret_key = os.getenv("Sinopack_CA_SECRET_KEY")
    
    if not ca_api_key:
        print("[Test] API Key missing, checking .env...")
        return

    print("[Test] Logging in...")
    try:
        api.login(
            api_key=ca_api_key,
            secret_key=ca_secret_key,
            subscribe_trade=True
        )
        print("[Test] Login success.")
    except Exception as e:
        print(f"[Test] Login failed: {e}")
        return

    print("[Test] Triggering fetch_contracts(contract_download=True)...")
    print("[Test] This frequently causes IndexError/Crash in raw pysolace.")
    
    try:
        # potentially dangerous call without patch
        api.fetch_contracts(contract_download=True)
        print("[Test] Contracts fetched successfully? (If you see this, maybe you got lucky)")
    except IndexError as e:
        print(f"\n[EVIDENCE] Caught Expected Crash: IndexError: {e}")
        print(">> This confirms the buffer overflow issue in pysolace C-extension.")
    except Exception as e:
        print(f"\n[Test] Caught other error: {e}")

if __name__ == "__main__":
    test_crash()
