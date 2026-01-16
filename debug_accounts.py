import shioaji as sj
import time

SIMULATION = True
CA_API_KEY = "CwXchQfoSPgUhKaRJDwv8LBJVyJwHcLWBwKazrJ23rdm"
CA_SECRET_KEY = "A934Lj5kRZRh2sWxLvP66qDGKk3qWdYmdcJhFJUYBEZ"

def test_accounts():
    api = sj.Shioaji(simulation=SIMULATION)
    print("Logging in...")
    try:
        api.login(api_key=CA_API_KEY, secret_key=CA_SECRET_KEY)
        print("Login success.")
        api.activate_ca("/Users/yptsai/Documents/certificate_key/Sinopac.pfx", "S122352189", "S122352189")
        print("CA Activated.")
    except Exception as e:
        print(f"Login failed: {e}")
        return

    time.sleep(2)
    print("\n--- Stock Accounts ---")
    for acc in api.list_accounts():
        print(f"{acc}, signed={getattr(acc, 'signed', 'N/A')}")
    
    print("\n--- Future Accounts ---")
    for acc in api.list_accounts():
         if isinstance(acc, sj.account.FutureAccount):
             print(f"{acc}, signed={getattr(acc, 'signed', 'N/A')}")


if __name__ == "__main__":
    test_accounts()

