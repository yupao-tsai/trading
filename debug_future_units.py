import shioaji as sj
import os
from dotenv import load_dotenv

load_dotenv()

api = sj.Shioaji(simulation=True)
api.login(
    api_key=os.getenv("Sinopack_CA_API_KEY"),
    secret_key=os.getenv("Sinopack_CA_SECRET_KEY")
)

target = "CDFA6" # TSMC 01
print(f"Inspecting {target}...")

found = False
for category in api.Contracts.Futures:
    try: iter(category)
    except: continue
    for c in category:
        if isinstance(c, tuple): c = c[1]
        if getattr(c, 'code', '') == target:
            print(f"Found {target}:")
            print(f"Name: {c.name}")
            print(f"Unit: {c.unit}")
            print(f"Symbol: {c.symbol}")
            print("--- Attributes ---")
            for k, v in c.__dict__.items():
                print(f"{k}: {v}")
            found = True
            break
    if found: break

api.logout()
