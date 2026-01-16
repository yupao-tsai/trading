#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import pandas as pd
import requests
import io

URL = "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies"

def main():
    headers = {
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.114 Safari/537.36"
    }
    
    try:
        response = requests.get(URL, headers=headers)
        response.raise_for_status()
        
        # Wrap in StringIO for pandas
        tables = pd.read_html(io.StringIO(response.text))
        
        # First table is the components list
        df = tables[0]
        
        # Symbol column
        symbols = df["Symbol"].astype(str).str.strip()
        
        # Replace dots with dashes for yfinance (e.g. BRK.B -> BRK-B)
        symbols = symbols.str.replace('.', '-', regex=False)
        
        # Filter empty
        symbols = symbols[symbols.str.len() > 0]
        
        # Sort and unique
        symbols = sorted(set(symbols.tolist()))
        
        with open("symbols.txt", "w", encoding="utf-8") as f:
            for s in symbols:
                f.write(s + "\n")
                
        print(f"Generated symbols.txt with {len(symbols)} symbols.")
        
    except Exception as e:
        print(f"Error generating symbols: {e}")

if __name__ == "__main__":
    main()
