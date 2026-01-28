import shioaji as sj
import os
from dotenv import load_dotenv

load_dotenv()

api = sj.Shioaji(simulation=True)
try:
    api.login(
        api_key=os.getenv("Sinopack_CA_API_KEY"),
        secret_key=os.getenv("Sinopack_CA_SECRET_KEY")
    )
except:
    pass

def scan_and_print(target_code_prefix):
    print(f"Scanning for prefix: {target_code_prefix} ...")
    for category in api.Contracts.Futures:
        try: iter(category)
        except: continue
        for c in category:
            if isinstance(c, tuple): c = c[1]
            if c.code.startswith(target_code_prefix):
                print(f"\nFOUND {c.code} ({c.name}):")
                print(f"  Unit: {getattr(c, 'unit', 'N/A')}")
                print(f"  Underlying: {getattr(c, 'underlying_code', 'N/A')}")
                # Print all public attributes
                for k, v in c.__dict__.items():
                    if not k.startswith('_'):
                        print(f"  {k}: {v}")
                return # Only print one example per prefix

# 3008 Largan
# IJF: Standard (2000 shares)
# OLF: Mini (100 shares)
scan_and_print("IJF")
scan_and_print("OLF")

# 2330 TSMC
# CDF: Standard (2000 shares)
# QFF: Mini (100 shares)
scan_and_print("CDF")
scan_and_print("QFF")

api.logout()
