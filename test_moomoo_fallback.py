from moomoo import OpenQuoteContext, RET_OK

def test_moomoo_quote():
    host = "127.0.0.1"
    port = 11111
    
    # Target: TLT 2026-01-16 Call 91.0
    symbol = "TLT"
    date_str = "260116" # YYMMDD
    opt_type = "C"
    strike = 91.0
    
    strike_int = int(strike * 1000)
    strike_part = f"{strike_int:08d}"
    
    full_code = f"US.{symbol}{date_str}{opt_type}{strike_part}"
    print(f"Testing code: {full_code}")
    
    try:
        ctx = OpenQuoteContext(host=host, port=port)
        ret, df = ctx.get_stock_quote([full_code])
        ctx.close()
        
        print(f"Ret: {ret}")
        if not df.empty:
            print(df.T)
        else:
            print("Empty DataFrame returned")
            
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    test_moomoo_quote()

