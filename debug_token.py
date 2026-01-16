import base64
import json
import datetime
import time
import sys

# Token from the log
token = "eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJuYmYiOjE3Njg1MzMwMTksImV4cCI6MTc2ODYxOTQxOSwic2ltdWxhdGlvbiI6dHJ1ZSwicGVyc29uX2lkIjoiUzEyMjM1MjE4OSIsInZlcnNpb24iOiIxLjMuMSIsInAycCI6IiNQMlAvdjpiY3NvbGFjZTAxL09yeWZrcXFtL1BZQVBJL1MxMjIzNTIxODkvMDExNi8wMzEwMTgvNjI2NzQ4LzYxLjIxNi4xNDMuMTMvIyIsImlwIjoiNjEuMjE2LjE0My4xMyIsInBlcm1pc3Npb25zIjpbIkRhdGEiLCJQb3J0Zm9saW8iLCJUcmFkZSJdLCJsZXZlbCI6MSwiY2FfcmVxdWlyZWQiOnRydWV9.7Ql9SRFeDdObGAdlZ20ZLEuD_ZErcbPSmlmYPtCpZB0"

def analyze_token(token):
    try:
        parts = token.split('.')
        if len(parts) != 3:
            print("Invalid token format")
            return

        payload = parts[1]
        # Padding
        payload += '=' * (-len(payload) % 4)
        
        data = json.loads(base64.urlsafe_b64decode(payload.encode()))
        print(json.dumps(data, indent=2, ensure_ascii=False))

        now = int(time.time())
        print(f"\nCurrent Local Time (Unix): {now}")
        print(f"Current Local Time (UTC): {datetime.datetime.utcfromtimestamp(now)}")

        for k in ["nbf", "exp"]:
            if k in data:
                ts = data[k]
                dt = datetime.datetime.utcfromtimestamp(ts)
                delta = ts - now
                status = ""
                if k == "exp":
                    status = "EXPIRED" if delta < 0 else "VALID"
                elif k == "nbf":
                    status = "NOT YET VALID" if delta > 0 else "VALID"
                
                print(f"{k}: {ts} ({dt}) | Delta: {delta} sec | Status: {status}")

    except Exception as e:
        print(f"Error decoding token: {e}")

if __name__ == "__main__":
    analyze_token(token)
