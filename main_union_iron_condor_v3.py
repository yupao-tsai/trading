#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import sys
import argparse
import datetime
import math
import statistics
import time
import re
import os
from typing import List, Optional, Dict, Tuple

from ib_insync import IB, Stock, Option, util

from dotenv import load_dotenv
load_dotenv()

# Optional deps
try:
    from moomoo import OpenSecTradeContext, TrdMarket, TrdEnv, SecurityFirm, RET_OK, OptionType
except ImportError:
    OpenSecTradeContext = None
    OptionType = None
    RET_OK = None

try:
    import robin_stocks.robinhood as rh
    import pyotp
except ImportError:
    rh = None
    pyotp = None

from math import log, sqrt, exp
from scipy.stats import norm


# =========================
# Config / Defaults
# =========================
HOST = "127.0.0.1"
PORT = 7496
CLIENT_ID = 11

PRIMARY_EXCHANGE = "NYSE"
CURRENCY = "USD"

# Moomoo connection (可調整)
MOOMOO_HOST = "127.0.0.1"
MOOMOO_PORT = 11111

# Filters / thresholds
# [MODIFIED] Changed MIN_DTE from 7 to 30 to avoid high-gamma short-term risk
MIN_DTE = 7
MAX_DTE = 60
# [MODIFIED] Lowered Min Strike PCT to capture Long Legs for Spreads
STRIKE_PCT_MIN = 0.60
STRIKE_PCT_MAX = 1.40 # Adjusted for Call side scanning

MIN_ANNUAL_RETURN = 0.05
MIN_SHARPE_RATIO = 0.1

# --- NEW: Open Interest filter (累計未平倉口數) ---
# 少於 500 口就不要（硬門檻）
MIN_OPEN_INTEREST = 200

# For "perfect" candidates written to file (same as your console highlight logic)
PERFECT_MIN_ANN = 0.15
PERFECT_MAX_ABS_DELTA = 0.35
PERFECT_MIN_IVR = 20
PERFECT_MIN_TD = 0.30
PERFECT_MIN_SAFE = 1.0  # optional, we still print safe but won't hard filter unless you want

# Market Data Type: 1=Live, 3=Delayed
MARKET_DATA_TYPE = 3

# Risk free rate
RISK_FREE_RATE = 0.033

# Fees per contract
TRANS_FEE_ACTIVITY = 0.01
TRANS_FEE_REGULATORY = 0.01 * 2
TRANS_FEE_OCC = 0.03 * 2
TRANS_FEE_COMMISSION = 0.65
FEES_PER_CONTRACT = TRANS_FEE_ACTIVITY + TRANS_FEE_REGULATORY + TRANS_FEE_OCC + TRANS_FEE_COMMISSION

# ANSI colors
COLOR_RED = "\033[91m"
COLOR_GREEN = "\033[92m"
COLOR_YELLOW = "\033[93m"
COLOR_BLUE = "\033[94m"
COLOR_RESET = "\033[0m"

NUM_OPTIONS_TO_SHOW_DEFAULT = 40

# Global cache: (symbol, expiry_YYYYMMDD, strike, right) -> (price, src, iv, delta)
OPTION_PRICE_CACHE: Dict[Tuple[str, str, float, str], Tuple[float, str, float, float]] = {}


# =========================
# Utilities
# =========================
def normalize_symbol_for_ib(symbol: str) -> str:
    s = symbol.strip().upper()
    # IBKR: "BRK.B" -> "BRK B", "BRKB" -> "BRK B"
    if s in ("BRK.B", "BRKB"):
        return "BRK B"
    return s

def read_symbols_file(path: str) -> List[str]:
    syms = []
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            if line.startswith("#"):
                continue
            # allow inline comment
            if "#" in line:
                line = line.split("#", 1)[0].strip()
            if not line:
                continue
            syms.append(normalize_symbol_for_ib(line))
    # de-dup preserve order
    seen = set()
    out = []
    for s in syms:
        if s not in seen:
            seen.add(s)
            out.append(s)
    return out

def connect_ib() -> IB:
    ib = IB()
    print(f"Connecting to TWS/IBG {HOST}:{PORT} clientId={CLIENT_ID} ...")
    ib.connect(HOST, PORT, clientId=CLIENT_ID, timeout=10)
    print("Connected:", ib.isConnected())
    # 確保 market data type 設定在預期值
    if ib.isConnected():
        ib.reqMarketDataType(MARKET_DATA_TYPE)
    return ib


def ensure_ib_connected(ib: IB, *, context: str = ""):
    """
    簡易 keep-alive：若發現連線掉了，立即嘗試重連並重新設定 market data type。
    """
    if ib is None:
        return
    if not ib.isConnected():
        ctx = f" ({context})" if context else ""
        print(f"[IB] Disconnected{ctx}, reconnecting...")
        try:
            ib.disconnect()
        except Exception:
            pass
        ib.connect(HOST, PORT, clientId=CLIENT_ID, timeout=10)
        if ib.isConnected():
            ib.reqMarketDataType(MARKET_DATA_TYPE)
            print("[IB] Reconnected.")
        else:
            print("[IB] Reconnect failed.")


def _open_moomoo_quote_context(max_retries: int = 2, delay: float = 1.0):
    """
    建立 Moomoo Quote Context，失敗時重試數次。
    """
    if OpenSecTradeContext is None:
        return None
    try:
        from moomoo import OpenQuoteContext
    except Exception:
        return None

    last_err = None
    for attempt in range(max_retries):
        try:
            return OpenQuoteContext(host=MOOMOO_HOST, port=MOOMOO_PORT)
        except Exception as e:
            last_err = e
            print(f"[Moomoo] create quote context failed (attempt {attempt+1}): {e}")
            time.sleep(delay)
    if last_err:
        print(f"[Moomoo] giving up creating quote context: {last_err}")
    return None

def qualify_stock(ib: IB, symbol: str) -> Stock:
    stk = Stock(symbol, "SMART", currency=CURRENCY, primaryExchange=PRIMARY_EXCHANGE)
    ib.qualifyContracts(stk)
    print(f"Qualified stock: {stk.symbol} (conId: {stk.conId})")
    return stk

def get_current_price(ib: IB, contract: Stock) -> float:
    """Get underlying price (Delayed/Live). Fallback to Moomoo, then Robinhood."""
    def _try_ib_price() -> float:
        ensure_ib_connected(ib, context="get_current_price")
        ib.reqMarketDataType(MARKET_DATA_TYPE)
        ticker_local = ib.reqMktData(contract, "", False, False)
        price_local = 0.0
        for _ in range(20):
            ib.sleep(0.25)
            if ticker_local.last and not math.isnan(ticker_local.last) and ticker_local.last > 0:
                price_local = ticker_local.last
            elif ticker_local.close and not math.isnan(ticker_local.close) and ticker_local.close > 0:
                price_local = ticker_local.close
            if price_local > 0:
                break
        ib.cancelMktData(contract)
        return price_local

    print("Waiting for stock price...")
    price = _try_ib_price()

    # 如果 IB 取價失敗，嘗試重連一次後再試
    if price <= 0:
        print("⚠️ IB price unavailable, retry after reconnect...")
        ensure_ib_connected(ib, context="price_retry")
        price = _try_ib_price()

    if price > 0:
        print(f"Current Price ({'Delayed' if MARKET_DATA_TYPE==3 else 'Live'}): {price}")
        return price

    print("⚠️ Warning: Could not get valid price from IB. Trying Moomoo fallback...")

    # Moomoo fallback
    try:
        try:
            from moomoo import SubType, RET_OK
        except Exception:
            SubType = None
            RET_OK = None

        if SubType is None or RET_OK is None:
            raise RuntimeError("Moomoo sdk not available for fallback")

        base_symbol = contract.symbol.replace(" ", ".")
        if "US." not in base_symbol:
            base_symbol = "US." + base_symbol

        symbols_to_try = [base_symbol]
        if "BRK.B" in base_symbol:
            symbols_to_try = [base_symbol.replace("BRK.B", "BRKB"), base_symbol]

        quote_ctx = _open_moomoo_quote_context()
        if quote_ctx:
            for sym in symbols_to_try:
                print(f"Trying Moomoo symbol: {sym}")
                quote_ctx.subscribe([sym], [SubType.QUOTE], subscribe_push=False)
                time.sleep(1.0)

                ret, df = quote_ctx.get_stock_quote([sym])
                if ret == RET_OK and not df.empty:
                    row = df.iloc[0]
                    if 'last_price' in row and row['last_price'] > 0:
                        price = float(row['last_price'])
                    elif 'close_price' in row and row['close_price'] > 0:
                        price = float(row['close_price'])

                    if price > 0:
                        print(f"Current Price (Moomoo): {price}")
                        quote_ctx.close()
                        return price

            quote_ctx.close()
    except Exception as e:
        print(f"Moomoo fallback failed: {e}")

    # Robinhood fallback
    if rh:
        print("Trying Robinhood fallback for price...")
        try:
            username = os.getenv("RH_USERNAME")
            password = os.getenv("RH_PASSWORD")
            totp_key = os.getenv("RH_TOTP_KEY")

            if not username or not password:
                print("[Robinhood] Missing RH_USERNAME/RH_PASSWORD, skip RH price.")
                return 0.0

            try:
                rh.get_account_info()
            except Exception:
                if totp_key and pyotp:
                    rh.login(username, password, mfa_code=pyotp.TOTP(totp_key).now())
                else:
                    rh.login(username, password)

            rh_symbol = contract.symbol.replace(" ", ".")
            quotes = rh.get_quotes(rh_symbol)
            if quotes and len(quotes) > 0:
                q = quotes[0]
                for k in ("last_trade_price", "last_extended_hours_trade_price", "previous_close"):
                    if k in q and q[k]:
                        price = float(q[k])
                        break
                if price > 0:
                    print(f"Current Price (Robinhood): {price}")
                    return price
        except Exception as e:
            print(f"Robinhood price fetch failed: {e}")

    return 0.0

def get_option_chain(ib: IB, stk: Stock) -> List:
    print("Fetching option chains...")
    chains = ib.reqSecDefOptParams(stk.symbol, "", stk.secType, stk.conId)
    if not chains:
        print("No option chains found.")
        return []
    print(f"Found {len(chains)} raw chains.")
    return chains

def get_historical_volatility(ib: IB, contract: Stock) -> Tuple[float, float]:
    print("Fetching historical data for HV calculation...")
    bars = ib.reqHistoricalData(
        contract,
        endDateTime="",
        durationStr="6 M",
        barSizeSetting="1 day",
        whatToShow="TRADES",
        useRTH=1,
        formatDate=1,
        keepUpToDate=False
    )
    if not bars:
        print("Warning: Could not fetch historical data.")
        return 0.18, 0.18

    closes = [b.close for b in bars]
    log_returns = []
    for i in range(1, len(closes)):
        if closes[i-1] > 0:
            log_returns.append(math.log(closes[i] / closes[i-1]))
        else:
            log_returns.append(0.0)

    if len(log_returns) < 2:
        return 0.18, 0.18

    def calc_hv(rs):
        if len(rs) < 2:
            return 0.0
        return statistics.stdev(rs) * math.sqrt(252)

    hv_6m = calc_hv(log_returns)
    hv_1w = calc_hv(log_returns[-5:]) if len(log_returns) >= 5 else hv_6m
    print(f"Historical Volatility: 1-Week={hv_1w:.2%}, 6-Month={hv_6m:.2%}")
    return hv_1w, hv_6m

def get_iv_rank_data(ib: IB, contract: Stock) -> Tuple[float, float, float]:
    print("Fetching 1-year IV history for IV Rank...")
    try:
        bars = ib.reqHistoricalData(
            contract,
            endDateTime='',
            durationStr='1 Y',
            barSizeSetting='1 day',
            whatToShow='OPTION_IMPLIED_VOLATILITY',
            useRTH=1,
            formatDate=1,
            keepUpToDate=False
        )
        if not bars:
            return 0.0, 0.0, 0.0
        vals = [b.close for b in bars]
        return min(vals), max(vals), vals[-1]
    except Exception as e:
        print(f"Failed to fetch IV history: {e}")
        return 0.0, 0.0, 0.0

def get_next_earnings_date(ib: IB, contract: Stock) -> Optional[datetime.date]:
    print("Fetching earnings calendar...")
    try:
        xml_str = ib.reqFundamentalData(contract, 'CalendarReport')
        if not xml_str:
            return None
        import xml.etree.ElementTree as ET
        root = ET.fromstring(xml_str)
        today = datetime.date.today()
        for earnings in root.findall(".//Earnings"):
            date_elem = earnings.find("Date")
            if date_elem is not None and date_elem.text:
                try:
                    e_date = datetime.datetime.strptime(date_elem.text, "%Y-%m-%d").date()
                    if e_date >= today:
                        return e_date
                except Exception:
                    pass
        return None
    except Exception:
        return None

def filter_options(ib: IB, stk: Stock, chains: List, current_price: float) -> List[Option]:
    today = datetime.date.today()
    candidates: List[Option] = []
    target_strike_min = current_price * STRIKE_PCT_MIN
    target_strike_max = current_price * STRIKE_PCT_MAX

    print(f"Scanning chains for DTE {MIN_DTE}-{MAX_DTE} and Strike {target_strike_min:.2f}-{target_strike_max:.2f}...")
    seen = set()
    processed = 0

    for chain in chains:
        if chain.exchange != 'SMART':
            continue
        processed += 1

        valid_expiries = []
        for exp_str in chain.expirations:
            year = int(exp_str[:4]); month = int(exp_str[4:6]); day = int(exp_str[6:])
            exp_date = datetime.date(year, month, day)
            dte = (exp_date - today).days
            if MIN_DTE <= dte <= MAX_DTE:
                valid_expiries.append(exp_str)
        if not valid_expiries:
            continue

        for strike in chain.strikes:
            if target_strike_min <= strike <= target_strike_max:
                for exp_str in valid_expiries:
                    # We need both PUTs and CALLs for Iron Condor
                    # Scan both rights for each valid expiry/strike combo
                    for right in ['P', 'C']:
                        key = (exp_str, strike, right)
                        if key in seen:
                            continue
                        seen.add(key)
                        opt = Option(
                            symbol=stk.symbol,
                            lastTradeDateOrContractMonth=exp_str,
                            strike=strike,
                            right=right,
                            exchange='SMART',
                            currency=CURRENCY,
                            multiplier=chain.multiplier
                        )
                        candidates.append(opt)

    print(f"Processed {processed} SMART chains.")
    print(f"Generated {len(candidates)} candidate contracts (Calls & Puts).")
    return candidates


# =========================
# Black-Scholes / IV
# =========================
def bs_put_price(S, K, T, r, sigma):
    if T <= 0 or sigma <= 0:
        return 0.0
    d1 = (log(S/K) + (r + 0.5 * sigma**2) * T) / (sigma * sqrt(T))
    d2 = d1 - sigma * sqrt(T)
    return K * exp(-r * T) * norm.cdf(-d2) - S * norm.cdf(-d1)

def bs_call_price(S, K, T, r, sigma):
    if T <= 0 or sigma <= 0:
        return 0.0
    d1 = (log(S/K) + (r + 0.5 * sigma**2) * T) / (sigma * sqrt(T))
    d2 = d1 - sigma * sqrt(T)
    return S * norm.cdf(d1) - K * exp(-r * T) * norm.cdf(d2)

def bs_put_delta(S, K, T, r, sigma):
    if T <= 0 or sigma <= 0:
        return 0.0
    d1 = (log(S/K) + (r + 0.5 * sigma**2) * T) / (sigma * sqrt(T))
    return norm.cdf(d1) - 1.0

def bs_call_delta(S, K, T, r, sigma):
    if T <= 0 or sigma <= 0:
        return 0.0
    d1 = (log(S/K) + (r + 0.5 * sigma**2) * T) / (sigma * sqrt(T))
    return norm.cdf(d1)

def bs_put_theta(S, K, T, r, sigma):
    if T <= 0 or sigma <= 0:
        return 0.0
    d1 = (log(S/K) + (r + 0.5 * sigma**2) * T) / (sigma * sqrt(T))
    d2 = d1 - sigma * sqrt(T)
    term1 = -(S * norm.pdf(d1) * sigma) / (2 * sqrt(T))
    term2 = r * K * exp(-r * T) * norm.cdf(-d2)
    return term1 + term2

def bs_gamma(S, K, T, r, sigma):
    """
    Gamma = N'(d1) / (S * sigma * sqrt(T))
    Same for Call and Put.
    """
    if T <= 0 or sigma <= 0 or S <= 0:
        return 0.0
    d1 = (log(S/K) + (r + 0.5 * sigma**2) * T) / (sigma * sqrt(T))
    return norm.pdf(d1) / (S * sigma * sqrt(T))

def implied_volatility(price, S, K, T, r, right='P'):
    if price <= 0:
        return 0.0
    low, high = 0.001, 5.0
    for _ in range(50):
        mid = (low + high) / 2
        if right == 'C':
            est = bs_call_price(S, K, T, r, mid)
        else:
            est = bs_put_price(S, K, T, r, mid)
            
        if abs(est - price) < 0.001:
            return mid
        if est < price:
            low = mid
        else:
            high = mid
    return (low + high) / 2

def calculate_kelly_fraction(annualized_ret, risk_free_rate, win_prob, iv, hv_short, hv_long):
    expected_annual_ret = annualized_ret * win_prob
    if expected_annual_ret <= risk_free_rate:
        return 0.0
    sigma = max(iv, hv_short, hv_long)
    sigma = max(sigma, 0.1)
    frac = (expected_annual_ret - risk_free_rate) / (sigma ** 2)
    return max(0.0, frac * 0.5)  # half-kelly


# =========================
# Moomoo option quote bulk
# =========================
def get_moomoo_quote_for_options(ib_symbol_base: str, min_dte: int, max_dte: int) -> Dict[Tuple[str, float, str], float]:
    """Returns: Dict[(ExpiryYYYYMMDD, Strike, Right), price]"""
    if OpenSecTradeContext is None or OptionType is None:
        return {}

    moomoo_host = "127.0.0.1"
    moomoo_port = 11111

    moomoo_base = ib_symbol_base.replace(" ", ".")
    if "US." not in moomoo_base:
        moomoo_base = "US." + moomoo_base

    adjusted_base = moomoo_base
    if "BRK.B" in moomoo_base:
        adjusted_base = moomoo_base.replace("BRK.B", "BRKB")

    today = datetime.date.today()
    start_date_overall = today + datetime.timedelta(days=min_dte)
    end_date_overall = today + datetime.timedelta(days=max_dte)

    price_map: Dict[Tuple[str, float, str], float] = {}

    try:
        from moomoo import SubType
        quote_ctx = _open_moomoo_quote_context()
        if quote_ctx is None:
            return {}

        current_start = start_date_overall
        all_codes = []

        while current_start <= end_date_overall:
            current_end = min(current_start + datetime.timedelta(days=29), end_date_overall)
            s_str = current_start.strftime("%Y-%m-%d")
            e_str = current_end.strftime("%Y-%m-%d")

            # Fetch Puts
            ret, df_chain_p = quote_ctx.get_option_chain(
                code=adjusted_base,
                start=s_str,
                end=e_str,
                option_type=OptionType.PUT
            )
            # Fetch Calls
            ret2, df_chain_c = quote_ctx.get_option_chain(
                code=adjusted_base,
                start=s_str,
                end=e_str,
                option_type=OptionType.CALL
            )

            if ret == RET_OK and not df_chain_p.empty:
                all_codes.extend(df_chain_p['code'].tolist())
            if ret2 == RET_OK and not df_chain_c.empty:
                all_codes.extend(df_chain_c['code'].tolist())

            current_start = current_end + datetime.timedelta(days=1)

        all_codes = list(set(all_codes))
        if not all_codes:
            quote_ctx.close()
            return {}

        chunk_size = 100
        for i in range(0, len(all_codes), chunk_size):
            batch = all_codes[i:i+chunk_size]
            for attempt in range(2):
                try:
                    quote_ctx.subscribe(batch, [SubType.QUOTE], subscribe_push=False)
                    ret_q, df_quote = quote_ctx.get_stock_quote(batch)

                    if ret_q == RET_OK and not df_quote.empty:
                        for _, row in df_quote.iterrows():
                            code = row['code']
                            price = 0.0
                            if 'last_price' in row and row['last_price'] > 0:
                                price = float(row['last_price'])
                            elif 'nominal_price' in row and row['nominal_price'] > 0:
                                price = float(row['nominal_price'])

                            if price > 0:
                                m = re.search(r'(\d{6})([CP])(\d+)$', code)
                                if m:
                                    date_str, type_char, strike_str = m.groups()
                                    expiry_full = "20" + date_str  # YYMMDD -> YYYYMMDD
                                    strike = int(strike_str) / 1000.0
                                    right = type_char # 'C' or 'P'
                                    price_map[(expiry_full, strike, right)] = price

                    quote_ctx.unsubscribe(batch, [SubType.QUOTE])
                    break
                except Exception as e:
                    print(f"[Moomoo] batch fetch failed (attempt {attempt+1}): {e}")
                    try:
                        quote_ctx.unsubscribe(batch, [SubType.QUOTE])
                    except Exception:
                        pass
                    if attempt == 1:
                        continue
                    time.sleep(0.5)

        quote_ctx.close()

    except Exception as e:
        print(f"Error fetching Moomoo option chain quotes: {e}")

    return price_map


# =========================
# Candidate analysis
# =========================
def analyze_candidates(
    ib: IB,
    stk_symbol: str,
    candidates: List[Option],
    current_price: float,
    hv_1w: float,
    hv_6m: float,
    iv_rank_info: Tuple[float, float, float],
    earnings_date: Optional[datetime.date],
    min_oi: int
) -> List[Dict]:
    results: List[Dict] = []
    ensure_ib_connected(ib, context="analyze_candidates:init")
    iv_low, iv_high, iv_curr_hist = iv_rank_info

    print("Fetching backup quotes from Moomoo (via Option Chain)...")
    moomoo_prices = get_moomoo_quote_for_options(stk_symbol, MIN_DTE, MAX_DTE)
    print(f"Got {len(moomoo_prices)} quotes from Moomoo.")

    # Phase 1: Collect Data for ALL candidates (Short and Long legs)
    chunk_size = 50
    today = datetime.date.today()
    
    # Store data: expiry -> right -> strike -> dict of stats
    option_data: Dict[str, Dict[str, Dict[float, Dict]]] = {}

    for i in range(0, len(candidates), chunk_size):
        chunk = candidates[i:i+chunk_size]
        print(f"Processing batch {i+1} to {min(i+chunk_size, len(candidates))}...")

        ensure_ib_connected(ib, context="analyze_candidates:batch")
        ib.qualifyContracts(*chunk)
        ib.reqMarketDataType(MARKET_DATA_TYPE)

        tickers = []
        for opt in chunk:
            t = ib.reqMktData(opt, "100,101,106", False, False)
            tickers.append((opt, t))

        print("Waiting for option quotes...")
        for _ in range(40):
            ib.sleep(0.25)
            # We want either Bid (for short) or Ask (for long) or Last
            valid_count = 0
            for _, t in tickers:
                 if (t.bid and t.bid > 0) or (t.ask and t.ask > 0) or (t.close and t.close > 0) or (t.last and t.last > 0):
                     valid_count += 1
            if valid_count > len(chunk) * 0.99:
                break

        for opt, ticker in tickers:
            ib.cancelMktData(opt)

            expiry_str = opt.lastTradeDateOrContractMonth
            
            # Calculate T (Time to Expiry in Years) upfront to avoid UnboundLocalError
            year_l = int(expiry_str[:4])
            month_l = int(expiry_str[4:6])
            day_l = int(expiry_str[6:])
            exp_date_l = datetime.date(year_l, month_l, day_l)
            dte_l = (exp_date_l - today).days
            if dte_l <= 0: dte_l = 1
            T = dte_l / 365.0
            
            # Basic parsing
            bid = 0.0
            ask = 0.0
            src = ""
            iv = 0.0
            delta = 0.0
            theta = 0.0
            gamma = 0.0 
            
            if ticker.modelGreeks:
                iv = ticker.modelGreeks.impliedVol or 0.0
                delta = ticker.modelGreeks.delta or 0.0
                theta = ticker.modelGreeks.theta or 0.0
                gamma = ticker.modelGreeks.gamma or 0.0

            # Price extraction (Bid for Short, Ask for Long)
            if ticker.bid and ticker.bid > 0:
                bid = float(ticker.bid)
                src = "IB"
            if ticker.ask and ticker.ask > 0:
                ask = float(ticker.ask)
            
            # Fallbacks
            last = ticker.last if (ticker.last and ticker.last > 0) else 0.0
            close = ticker.close if (ticker.close and ticker.close > 0) else 0.0
            
            # If bid/ask missing, estimate from Last/Close/Moomoo
            # Conservative for Short (Bid): use lower of Last/Close
            # Conservative for Long (Ask): use higher of Last/Close
            
            # Moomoo fallback
            moomoo_p = 0.0
            key = (expiry_str, opt.strike, opt.right)
            if key in moomoo_prices and moomoo_prices[key] > 0:
                moomoo_p = float(moomoo_prices[key])
                if not src: src = "ML"

            # Fill Bid
            if bid == 0:
                if last > 0: bid = last; src = src or "IL"
                elif moomoo_p > 0: bid = moomoo_p; src = src or "ML"
                elif close > 0: bid = close; src = src or "IC"

            # Fill Ask (Default to Bid + spread or just Bid if desperate, but better to be conservative)
            if ask == 0:
                # If we have no ask, assume a spread. 
                # E.g. Ask = Bid * 1.05 or Bid + 0.10?
                # Better to just use Last or close if available
                 if last > 0: ask = last
                 elif moomoo_p > 0: ask = moomoo_p
                 elif close > 0: ask = close
                 else: ask = bid * 1.1 # Penalize missing ask

            # If still 0, skip
            if bid <= 0.05 and ask <= 0.05:
                continue

            # OI
            oi = 0
            try:
                oi = int(getattr(ticker, "putOpenInterest", 0) or 0)
                if oi <= 0: oi = int(getattr(ticker, "callOpenInterest", 0) or 0)
                if oi <= 0: oi = int(getattr(ticker, "openInterest", 0) or 0)
            except Exception: pass
            
            if iv == 0:
                iv = implied_volatility(bid, current_price, opt.strike, T, RISK_FREE_RATE, right=opt.right)

            if delta == 0:
                if opt.right == 'C':
                    delta = bs_call_delta(current_price, opt.strike, T, RISK_FREE_RATE, iv if iv > 0 else 0.5)
                else:
                    delta = bs_put_delta(current_price, opt.strike, T, RISK_FREE_RATE, iv if iv > 0 else 0.5)

            if abs(delta) < 0.001: 
                continue # Skip if Delta is essentially zero (untradeable or bad data)

            if expiry_str not in option_data:
                option_data[expiry_str] = {'P': {}, 'C': {}}
            
            option_data[expiry_str][opt.right][opt.strike] = {
                "bid": bid,
                "ask": ask,
                "src": src,
                "iv": iv,
                "delta": delta,
                "theta": theta,
                "gamma": gamma,
                "oi": oi
            }

    # Phase 2: Generate Iron Condors
    # Bull Put Spread (Put Credit Spread) + Bear Call Spread (Call Credit Spread)
    print("Generating Iron Condor Spreads...")
    
    spread_results = []
    expiry_stats = {}
    
    for expiry, rights_data in option_data.items():
        puts = rights_data.get('P', {})
        calls = rights_data.get('C', {})
        
        # Track initial counts
        if expiry not in expiry_stats: 
            expiry_stats[expiry] = {"valid_puts": len(puts), "valid_calls": len(calls), "generated": 0}

        sorted_put_strikes = sorted(puts.keys())
        sorted_call_strikes = sorted(calls.keys())
        
        year = int(expiry[:4]); month = int(expiry[4:6]); day = int(expiry[6:])
        exp_date = datetime.date(year, month, day)
        dte = (exp_date - today).days
        if dte <= 0: dte = 1
        T = dte / 365.0
        
        earnings_collision = False
        if earnings_date and today <= earnings_date <= exp_date:
            earnings_collision = True
        
        # 1. Find Valid Bull Put Spreads
        valid_put_spreads = []
        for i in range(len(sorted_put_strikes)):
            k_short_p = sorted_put_strikes[i]
            d_short_p = puts[k_short_p]
            
            # Filter Short Put
            if d_short_p["oi"] < min_oi: continue
            if abs(d_short_p["delta"]) > 0.30: continue # Conservative Delta for IC (usually < 0.30 or 0.20)
            if d_short_p["bid"] < 0.15: continue 
            
            for j in range(i - 1, -1, -1):
                k_long_p = sorted_put_strikes[j]
                d_long_p = puts[k_long_p]
                
                if d_long_p["oi"] < min_oi: continue
                width_p = k_short_p - k_long_p
                if width_p < 1.0: continue
                
                mid_short_p = (d_short_p["bid"] + d_short_p["ask"]) / 2.0
                mid_long_p = (d_long_p["bid"] + d_long_p["ask"]) / 2.0
                
                credit_gross_p = mid_short_p - mid_long_p
                if credit_gross_p <= 0: continue
                
                # Fees
                fees_p = (FEES_PER_CONTRACT * 2.0) / 100.0
                credit_net_p = credit_gross_p - fees_p
                # if credit_net_p <= 0: continue
                
                valid_put_spreads.append({
                    "short_k": k_short_p, "long_k": k_long_p, "width": width_p, "credit": credit_net_p,
                    "delta_short": d_short_p["delta"], "delta_long": d_long_p["delta"],
                    "short_bid": d_short_p["bid"], "long_ask": d_long_p["ask"],
                    "oi_short": d_short_p["oi"], "oi_long": d_long_p["oi"]
                })

        # 2. Find Valid Bear Call Spreads
        valid_call_spreads = []
        for i in range(len(sorted_call_strikes)):
            k_short_c = sorted_call_strikes[i]
            d_short_c = calls[k_short_c]
            
            # Filter Short Call
            if d_short_c["oi"] < min_oi: continue
            if abs(d_short_c["delta"]) > 0.30: continue # Conservative Delta
            if d_short_c["bid"] < 0.15: continue
            
            # Look for Long Call (Higher Strike)
            for j in range(i + 1, len(sorted_call_strikes)):
                k_long_c = sorted_call_strikes[j]
                d_long_c = calls[k_long_c]
                
                if d_long_c["oi"] < min_oi: continue
                width_c = k_long_c - k_short_c
                if width_c < 1.0: continue
                
                mid_short_c = (d_short_c["bid"] + d_short_c["ask"]) / 2.0
                mid_long_c = (d_long_c["bid"] + d_long_c["ask"]) / 2.0
                
                credit_gross_c = mid_short_c - mid_long_c
                if credit_gross_c <= 0: continue
                
                # Fees
                fees_c = (FEES_PER_CONTRACT * 2.0) / 100.0
                credit_net_c = credit_gross_c - fees_c
                # if credit_net_c <= 0: continue
                
                valid_call_spreads.append({
                    "short_k": k_short_c, "long_k": k_long_c, "width": width_c, "credit": credit_net_c,
                    "delta_short": d_short_c["delta"], "delta_long": d_long_c["delta"],
                    "short_bid": d_short_c["bid"], "long_ask": d_long_c["ask"],
                    "oi_short": d_short_c["oi"], "oi_long": d_long_c["oi"]
                })
        
        # print(f"DEBUG: Expiry {expiry} found {len(valid_put_spreads)} Put Spreads and {len(valid_call_spreads)} Call Spreads.")

        # 3. Combine into Iron Condors
        # Requirement: Call Short Strike > Put Short Strike (No inversion)
        # Ideally: Gap between Put Short and Call Short > X%
        
        for ps in valid_put_spreads:
            for cs in valid_call_spreads:
                if cs["short_k"] <= ps["short_k"]:
                    continue # Inverted or overlapping strikes, invalid for standard IC
                
                # Distance Check (optional, but good for safety)
                # if (cs["short_k"] - ps["short_k"]) / current_price < 0.05: continue 
                
                # Total Credit
                total_credit = ps["credit"] + cs["credit"]
                
                # Max Risk = Max(Width_Put, Width_Call) - Total_Credit
                # (Assuming non-overlapping risk for margin)
                max_width = max(ps["width"], cs["width"])
                max_loss = max_width - total_credit
                
                if max_loss <= 0: continue # Arbitrage or bad data
                
                roi = total_credit / max_loss
                ann_roi = roi * (365.0 / dte)
                
                # Probabilities
                # Win = 1 - Prob(Put ITM) - Prob(Call ITM)
                # Approx using Deltas
                prob_put_itm = abs(ps["delta_short"])
                prob_call_itm = abs(cs["delta_short"])
                win_prob = 1.0 - max(prob_put_itm, prob_call_itm)
                
                if win_prob <= 0: 
                    continue
                
                # EV Calculation (5-Zone Exact Method)
                # Zone 1: Put Max Loss (S < Put Long)
                # Zone 2: Put Partial Loss (Put Long < S < Put Short)
                # Zone 3: Max Profit (Put Short < S < Call Short)
                # Zone 4: Call Partial Loss (Call Short < S < Call Long)
                # Zone 5: Call Max Loss (S > Call Long)

                # Probabilities via Delta approximation
                # Note: IB Delta is typically negative for Puts, positive for Calls.
                # |Delta_Put| ~= Prob(ITM) ~= Prob(S < Strike)
                # Delta_Call ~= Prob(ITM) ~= Prob(S > Strike)
                
                prob_s_lt_pl = abs(ps["delta_long"])   # Prob(S < Put Long)
                prob_s_lt_ps = abs(ps["delta_short"])  # Prob(S < Put Short)
                prob_s_gt_cs = abs(cs["delta_short"])  # Prob(S > Call Short)
                prob_s_gt_cl = abs(cs["delta_long"])   # Prob(S > Call Long)

                # Zone Probabilities
                p1 = prob_s_lt_pl                                      # Deep ITM Put
                p2 = max(0.0, prob_s_lt_ps - prob_s_lt_pl)             # Between Put Strikes
                p5 = prob_s_gt_cl                                      # Deep ITM Call
                p4 = max(0.0, prob_s_gt_cs - prob_s_gt_cl)             # Between Call Strikes
                p3 = max(0.0, 1.0 - (p1 + p2 + p4 + p5))               # Profit Zone

                # Zone Payoffs
                # Credit is collected upfront.
                # Max Loss = Width - Credit
                
                payoff_1 = -(ps["width"] - total_credit)
                # Average loss in partial zone is roughly half the width minus credit? 
                # Actually, payoff goes linearly from -MaxLoss to +Credit.
                # Average Payoff ~= (Payoff_at_Long + Payoff_at_Short) / 2
                # Payoff_at_Long = -(Width - Credit)
                # Payoff_at_Short = +Credit
                # Avg = (Credit - (Width - Credit)) / 2 = (2*Credit - Width) / 2 = Credit - Width/2
                payoff_2 = total_credit - (ps["width"] / 2.0)
                
                payoff_3 = total_credit
                
                payoff_4 = total_credit - (cs["width"] / 2.0)
                payoff_5 = -(cs["width"] - total_credit)

                ev = (p1 * payoff_1) + (p2 * payoff_2) + (p3 * payoff_3) + (p4 * payoff_4) + (p5 * payoff_5)
                
                # if ev <= 0: 
                #    continue

                # EV Annualized
                ev_roi = ev / max_loss if max_loss > 0 else 0
                ev_ann_roi = ev_roi * (365.0 / dte)

                # Filters
                if ann_roi < MIN_ANNUAL_RETURN: 
                    continue
                if win_prob < 0.50: 
                    continue # High probability trade preferred
                
                spread_results.append({
                    "symbol": stk_symbol,
                    "expiry": expiry,
                    "dte": dte,
                    "put_short": ps["short_k"],
                    "put_long": ps["long_k"],
                    "call_short": cs["short_k"],
                    "call_long": cs["long_k"],
                    "width_p": ps["width"],
                    "width_c": cs["width"],
                    "credit": total_credit,
                    "max_loss": max_loss,
                    "roi": roi,
                    "ann_roi": ann_roi,
                    "ev": ev,
                    "ev_ann_roi": ev_ann_roi,
                    "win_prob": win_prob,
                    "earn": earnings_collision,
                    "delta_p": ps["delta_short"],
                    "delta_c": cs["delta_short"],
                    "oi_ps": ps["oi_short"],
                    "oi_pl": ps["oi_long"],
                    "oi_cs": cs["oi_short"],
                    "oi_cl": cs["oi_long"]
                })
                expiry_stats[expiry]["generated"] += 1

    # Print expiry stats
    print("\n[Expiry Stats] Candidates generated per expiry:")
    for exp, stats in sorted(expiry_stats.items()):
        print(f"  {exp}: Puts={stats['valid_puts']}, Calls={stats['valid_calls']} -> Generated={stats['generated']}")
    print("")

    # Sort by EV Annualized Return descending
    return sorted(spread_results, key=lambda x: x["ev_ann_roi"], reverse=True)

def get_moomoo_positions(ib_symbol: str, underlying_price: float, hv_1w: float, hv_6m: float, iv_rank_info: Tuple[float, float, float]):
    # ... existing implementation (simplified for new file) ...
    pass 

# =========================
# File output (DO NOT change console format)
# =========================
def format_candidate_line(r: Dict, hv_val: float) -> str:
    # TSV columns for Iron Condor
    # Symbol, Expiry, DTE, PutShort, PutLong, CallShort, CallLong, Cred, MaxRisk, AnnROI%, EVAnn%, EV, WinProb, DeltaP, DeltaC
    
    return "\t".join([
        str(r["symbol"]),
        str(r["expiry"]),
        str(r["dte"]),
        f"{r['put_short']:.1f}",
        f"{r['put_long']:.1f}",
        f"{r['call_short']:.1f}",
        f"{r['call_long']:.1f}",
        f"{r['credit']:.2f}",
        f"{r['max_loss']:.2f}",
        f"{r['ann_roi']*100:.1f}",
        f"{r['ev_ann_roi']*100:.1f}",
        f"{r['ev']:.2f}",
        f"{r['win_prob']*100:.1f}",
        f"{r['delta_p']:.2f}",
        f"{r['delta_c']:.2f}",
        str(r['oi_ps']),
        str(r['oi_pl']),
        str(r['oi_cs']),
        str(r['oi_cl'])
    ])

def is_perfect_candidate(r: Dict, hv_val: float) -> bool:
    if r["ev_ann_roi"] < 0.20: return False
    if r["win_prob"] < 0.60: return False
    return True


# =========================
# Main scan
# =========================
def scan_one_symbol(ib: IB, symbol: str, top_n: int, min_oi: int) -> Tuple[List[Dict], float, Optional[datetime.date]]:
    ensure_ib_connected(ib, context=f"scan {symbol}")
    stk = qualify_stock(ib, symbol)
    price = get_current_price(ib, stk)
    if price <= 0:
        print(f"❌ Cannot retrieve underlying price for {symbol}. Skip.")
        return [], 0.0, None, 0.0, 0.0, 0.0, (0.0, 0.0, 0.0)

    hv_1w, hv_6m = get_historical_volatility(ib, stk)
    hv_val = max(hv_1w, hv_6m)

    iv_low, iv_high, iv_curr_hist = get_iv_rank_data(ib, stk)
    earnings_date = get_next_earnings_date(ib, stk)

    chains = get_option_chain(ib, stk)
    if not chains:
        return [], hv_val, earnings_date, hv_1w, hv_6m, price, (iv_low, iv_high, iv_curr_hist)

    candidates = filter_options(ib, stk, chains, price)
    if not candidates:
        print(f"No candidates match filter criteria for {symbol}.")
        return [], hv_val, earnings_date, hv_1w, hv_6m, price, (iv_low, iv_high, iv_curr_hist)

    print(f"\nAnalyzing Iron Condor candidates for {symbol} (Price: {price:.2f}) ...")
    ranked = analyze_candidates(ib, symbol, candidates, price, hv_1w, hv_6m, (iv_low, iv_high, iv_curr_hist), earnings_date, min_oi)

    # Console table (top N)
    print(f"\n====== Iron Condor Recommendations for {symbol} (Price: {price:.2f}) ======")
    print("Strategy: Iron Condor (Sell Put Spread + Sell Call Spread)")
    print("Ranking: By EV Annualized Return (EVAnn%)")
    print(f"Risk Free Rate: {RISK_FREE_RATE:.1%}")
    if earnings_date:
        print(f"Next Earnings Date: {earnings_date} (Check for collision!)")
    
    header = (
        f"{'Expiry':<10} | {'DTE':<4} | {'PutSh':<7} | {'PutLg':<7} | {'CallSh':<7} | {'CallLg':<7} | {'Cred':<6} | "
        f"{'MaxRisk':<7} | {'Ann%':<6} | {'EVAnn%':<7} | {'EV':<6} | {'Win%':<5} | {'DelP':<5} | {'DelC':<5}"
    )
    print(header)
    print("-" * len(header))

    for r in ranked[:top_n]:
        row = (
            f"{r['expiry']:<10} | {r['dte']:<4} | {r['put_short']:<7.1f} | {r['put_long']:<7.1f} | "
            f"{r['call_short']:<7.1f} | {r['call_long']:<7.1f} | {r['credit']:<6.2f} | "
            f"{r['max_loss']:<7.2f} | {r['ann_roi']*100:<6.1f} | {r['ev_ann_roi']*100:<7.1f} | {r['ev']:<6.2f} | {r['win_prob']*100:<5.1f} | {abs(r['delta_p']):<5.2f} | {abs(r['delta_c']):<5.2f}"
        )
        
        tag = ""
        if r['earn']: tag = f" {COLOR_RED}(EARN){COLOR_RESET}"
        
        if is_perfect_candidate(r, hv_val):
             print(f"{COLOR_GREEN}{row}{tag}{COLOR_RESET}")
        else:
             print(f"{row}{tag}")

    return ranked, hv_val, earnings_date, hv_1w, hv_6m, price, (iv_low, iv_high, iv_curr_hist)


def main():
    util.patchAsyncio()

    parser = argparse.ArgumentParser(description="Union Iron Condor Scanner - EV Optimized")
    parser.add_argument("--symbol", help="Single stock symbol (e.g. AAPL)")
    parser.add_argument("--symbols-file", help="Symbols file path, one symbol per line (e.g. symbols.txt)")
    parser.add_argument("--out-file", default="candidate_iron_condor.txt", help="Output candidate file")
    parser.add_argument("--append", action="store_true", help="Append to out-file (default: overwrite)")
    parser.add_argument("--top", type=int, default=NUM_OPTIONS_TO_SHOW_DEFAULT, help=f"Show top N per symbol (default: {NUM_OPTIONS_TO_SHOW_DEFAULT})")
    parser.add_argument("--open-interest", type=int, default=MIN_OPEN_INTEREST, help=f"Min Open Interest (default: {MIN_OPEN_INTEREST})")
    
    args = parser.parse_args()

    # Update global config from args if needed
    # global MIN_OPEN_INTEREST -- removed to avoid SyntaxError, we will just use args or modify the logic if needed
    # But since MIN_OPEN_INTEREST is used in analyze_candidates which is called later, we can just assign it to the module level variable
    # However, Python doesn't like 'global' keyword if the variable was used in the same scope before.
    # In 'main', MIN_OPEN_INTEREST is not used before this line. Wait, it IS used in the default value of add_argument!
    # "default=MIN_OPEN_INTEREST" reads the global variable.
    # So we cannot declare it global here.
    # We should pass it as an argument to scan_one_symbol -> analyze_candidates
    # OR, just modify the module-level variable using globals() dict or just rely on the fact that we are in the same module.
    # Actually, simplest fix is to just use a different variable name or pass it down.
    # Let's pass it down or set it via a helper function.
    
    scan_min_oi = args.open_interest

    symbols: List[str] = []
    if args.symbol:
        symbols = [normalize_symbol_for_ib(args.symbol)]
    elif args.symbols_file:
        symbols = read_symbols_file(args.symbols_file)
    else:
        # Default fallback
        symbols = [normalize_symbol_for_ib("BRK.B")]

    if not symbols:
        print("No symbols to scan.")
        return

    out_file = args.out_file

    TSV_HEADER = "\t".join([
        "Symbol", "Expiry", "DTE", "PutShort", "PutLong", "CallShort", "CallLong", "Cred", "MaxRisk", "AnnROI%", "EVAnn%", "EV", "WinProb", "DeltaP", "DeltaC", "OI_PS", "OI_PL", "OI_CS", "OI_CL"
    ])
    if not args.append:
        with open(out_file, "w", encoding="utf-8") as f:
            f.write(f"{TSV_HEADER}\n")

    print(f"\n[Output] Candidates will be written to: {out_file}  (mode={'APPEND' if args.append else 'OVERWRITE'})")
    
    ib = connect_ib()
    try:
        total_written = 0

        for idx, sym in enumerate(symbols, 1):
            print(f"\n==============================")
            print(f"[{idx}/{len(symbols)}] Scanning: {sym}")
            print(f"==============================")

            try:
                ranked, hv_val, earnings_date, hv_1w, hv_6m, underlying_price, iv_rank_info = scan_one_symbol(ib, sym, args.top, scan_min_oi)
            except Exception as e:
                print(f"❌ Error scanning {sym}: {e}")
                continue

            if not ranked:
                continue

            # Write candidates to file
            written_this_symbol = 0
            with open(out_file, "a", encoding="utf-8") as f:
                for r in ranked:
                    if r["ev"] > 0:
                        line = format_candidate_line(r, hv_val)
                        earn_flag = "EARN" if r.get("earn") else ""
                        f.write(line + (f"\t{earn_flag}" if earn_flag else "") + "\n")
                        written_this_symbol += 1

            total_written += written_this_symbol
            print(f"\n[File] {sym}: wrote {written_this_symbol} IC candidates -> {out_file}")

        print(f"\n✅ Done. Total Iron Condor candidates written: {total_written} -> {out_file}")

    finally:
        if ib.isConnected():
            ib.disconnect()
            print("\nDisconnected.")


if __name__ == "__main__":
    main()
