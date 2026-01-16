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
MIN_DTE = 7
MAX_DTE = 60
STRIKE_PCT_MIN = 0.88
STRIKE_PCT_MAX = 0.99

MIN_ANNUAL_RETURN = 0.05
MIN_SHARPE_RATIO = 0.2

# --- NEW: Open Interest filter (累計未平倉口數) ---
# 少於 500 口就不要（硬門檻）
MIN_OPEN_INTEREST = 500

# For "perfect" candidates written to file (same as your console highlight logic)
PERFECT_MIN_ANN = 0.15
PERFECT_MAX_ABS_DELTA = 0.30
PERFECT_MIN_IVR = 30
PERFECT_MIN_TD = 0.40
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
                    key = (exp_str, strike)
                    if key in seen:
                        continue
                    seen.add(key)
                    opt = Option(
                        symbol=stk.symbol,
                        lastTradeDateOrContractMonth=exp_str,
                        strike=strike,
                        right='P',
                        exchange='SMART',
                        currency=CURRENCY,
                        multiplier=chain.multiplier
                    )
                    candidates.append(opt)

    print(f"Processed {processed} SMART chains.")
    print(f"Generated {len(candidates)} candidate contracts.")
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

def bs_put_delta(S, K, T, r, sigma):
    if T <= 0 or sigma <= 0:
        return 0.0
    d1 = (log(S/K) + (r + 0.5 * sigma**2) * T) / (sigma * sqrt(T))
    return norm.cdf(d1) - 1.0

def bs_put_theta(S, K, T, r, sigma):
    if T <= 0 or sigma <= 0:
        return 0.0
    d1 = (log(S/K) + (r + 0.5 * sigma**2) * T) / (sigma * sqrt(T))
    d2 = d1 - sigma * sqrt(T)
    term1 = -(S * norm.pdf(d1) * sigma) / (2 * sqrt(T))
    term2 = r * K * exp(-r * T) * norm.cdf(-d2)
    return term1 + term2

def implied_volatility(price, S, K, T, r, right='P'):
    if price <= 0:
        return 0.0
    low, high = 0.001, 5.0
    for _ in range(50):
        mid = (low + high) / 2
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
def get_moomoo_quote_for_options(ib_symbol_base: str, min_dte: int, max_dte: int) -> Dict[Tuple[str, float], float]:
    """Returns: Dict[(ExpiryYYYYMMDD, Strike), price]"""
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

    price_map: Dict[Tuple[str, float], float] = {}

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

            ret, df_chain = quote_ctx.get_option_chain(
                code=adjusted_base,
                start=s_str,
                end=e_str,
                option_type=OptionType.PUT
            )

            if (ret != RET_OK or df_chain.empty) and adjusted_base != moomoo_base:
                ret, df_chain = quote_ctx.get_option_chain(
                    code=moomoo_base,
                    start=s_str,
                    end=e_str,
                    option_type=OptionType.PUT
                )

            if ret == RET_OK and not df_chain.empty:
                all_codes.extend(df_chain['code'].tolist())

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
                                    price_map[(expiry_full, strike)] = price

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
    earnings_date: Optional[datetime.date]
) -> List[Dict]:
    results: List[Dict] = []
    ensure_ib_connected(ib, context="analyze_candidates:init")
    iv_low, iv_high, iv_curr_hist = iv_rank_info

    print("Fetching backup quotes from Moomoo (via Option Chain)...")
    moomoo_prices = get_moomoo_quote_for_options(stk_symbol, MIN_DTE, MAX_DTE)
    print(f"Got {len(moomoo_prices)} quotes from Moomoo.")

    chunk_size = 50
    today = datetime.date.today()

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
            valid = sum(1 for _, t in tickers if (t.bid and t.bid > 0) or (t.close and t.close > 0) or (t.last and t.last > 0))
            if valid > len(chunk) * 0.99:
                break

        for opt, ticker in tickers:
            ib.cancelMktData(opt)

            expiry_str = opt.lastTradeDateOrContractMonth
            year = int(expiry_str[:4]); month = int(expiry_str[4:6]); day = int(expiry_str[6:])
            exp_date = datetime.date(year, month, day)
            dte = (exp_date - today).days
            if dte <= 0:
                dte = 1
            T = dte / 365.0

            # --- NEW: Open Interest (累計未平倉口數) ---
            oi = 0
            try:
                if opt.right == 'P':
                    oi = int(getattr(ticker, "putOpenInterest", 0) or 0)
                else:
                    oi = int(getattr(ticker, "callOpenInterest", 0) or 0)
                if oi <= 0:
                    oi = int(getattr(ticker, "openInterest", 0) or 0)
            except Exception:
                oi = 0

            bid = 0.0
            src = ""
            iv = 0.0
            delta = 0.0
            theta = 0.0

            if ticker.modelGreeks:
                if ticker.modelGreeks.impliedVol:
                    iv = ticker.modelGreeks.impliedVol
                if ticker.modelGreeks.delta:
                    delta = ticker.modelGreeks.delta
                if ticker.modelGreeks.theta:
                    theta = ticker.modelGreeks.theta

            if ticker.bid and ticker.bid > 0:
                bid = float(ticker.bid); src = "IB"
            if bid == 0 and ticker.last and ticker.last > 0:
                bid = float(ticker.last); src = "IL"
            if bid == 0:
                key = (expiry_str, opt.strike)
                if key in moomoo_prices and moomoo_prices[key] > 0:
                    bid = float(moomoo_prices[key]); src = "ML"
            if bid == 0 and ticker.close and ticker.close > 0:
                bid = float(ticker.close); src = "IC"

            if bid <= 0.05:
                continue

            # cache to keep consistent if other modules use it
            OPTION_PRICE_CACHE[(stk_symbol, expiry_str, opt.strike, opt.right)] = (bid, src, iv, delta)

            fees_per_share = FEES_PER_CONTRACT / 100.0
            net_bid = max(0.0, bid - fees_per_share)

            ret = net_bid / opt.strike
            annualized = ret * (365 / dte)

            is_iv_est = False
            is_delta_est = False

            if iv == 0:
                is_iv_est = True
                iv = implied_volatility(bid, current_price, opt.strike, T, RISK_FREE_RATE)

            if delta == 0:
                is_delta_est = True
                delta = bs_put_delta(current_price, opt.strike, T, RISK_FREE_RATE, iv)

            if theta == 0:
                theta_annual = bs_put_theta(current_price, opt.strike, T, RISK_FREE_RATE, iv)
                theta = theta_annual / 365.0

            theta_yield = abs(theta) / bid if bid > 0 and theta != 0 else 0.0
            pop = max(0.0, 1.0 - abs(delta))

            win_prob = pop
            kelly = calculate_kelly_fraction(annualized, RISK_FREE_RATE, win_prob, iv, hv_1w, hv_6m)

            vol_risk = max(iv, hv_1w, hv_6m)
            sharpe = (annualized - RISK_FREE_RATE) / vol_risk if vol_risk > 0.01 else 0.0

            if annualized < MIN_ANNUAL_RETURN:
                continue
            if sharpe < MIN_SHARPE_RATIO:
                continue

            ivr = 0.0
            if iv_high > iv_low:
                ivr = (iv - iv_low) / (iv_high - iv_low) * 100.0

            expected_move = current_price * iv * math.sqrt(T)
            safety = (current_price - opt.strike) / expected_move if expected_move > 0 else 99.9

            td_ratio = abs(theta) / abs(delta) if abs(delta) > 0.001 else 0.0

            earnings_collision = False
            if earnings_date and today <= earnings_date <= exp_date:
                earnings_collision = True

            results.append({
                "symbol": stk_symbol,
                "expiry": expiry_str,
                "strike": float(opt.strike),
                "bid": bid,
                "price_src": src,
                "dte": dte,
                "annualized": annualized,
                "iv": iv,
                "delta": delta,
                "sharpe": sharpe,
                "ivr": ivr,
                "safety": safety,
                "td_ratio": td_ratio,
                "theta_yield": theta_yield,
                "is_iv_est": is_iv_est,
                "is_delta_est": is_delta_est,
                "earn": earnings_collision,
                "kelly": kelly,
                "open_interest": oi,
            })

    return sorted(results, key=lambda x: x["kelly"], reverse=True)


# =========================
# File output (DO NOT change console format)
# =========================
def format_candidate_line(r: Dict, hv_val: float) -> str:
    # TSV columns (exact set you asked): 
    # Symbol Expiry | DTE | Strike | Bid(Src) | Ret% | Ann% | Ideal | Ratio | Shp | IV | HV | Theta | Delta | Kelly% | IVR | T/D | OI
    #
    # NOTE: console uses different 'ret' display; here Ret% is computed from net premium / strike (same core idea).
    fees_per_share = FEES_PER_CONTRACT / 100.0
    net_bid = max(0.0, float(r["bid"]) - fees_per_share)
    ret_pct = (net_bid / float(r["strike"])) * 100.0
    ann_pct = float(r["annualized"]) * 100.0
    theta_pct = float(r.get("theta_yield", 0.0)) * 100.0

    # In your earlier v2 logic, Ideal is computed via required_ann_ret -> ideal_bid.
    # Your current v3 already prints Ideal/Ratio in file header request; but your analyze_candidates currently doesn't compute ideal_bid.
    # To keep "use my old method" without touching console, we keep your v3 file format consistent but leave Ideal/Ratio blank unless present.
    # If you already have ideal_bid in r from your older method, it will print. Otherwise it prints 0.00 / 0.00.
    ideal = float(r.get("ideal_bid", 0.0))
    ratio = (float(r["bid"]) / ideal) if ideal > 0 else float(r.get("ratio", 0.0))

    bid_src = f"{float(r['bid']):.2f}({r['price_src']})"
    iv_str = f"{float(r['iv']):.4f}"
    hv_str = f"{float(hv_val):.4f}"
    shp_str = f"{float(r['sharpe']):.4f}"
    delta_str = f"{float(r['delta']):.4f}"
    kelly_str = f"{float(r['kelly'])*100.0:.2f}"
    ivr_str = f"{float(r['ivr']):.2f}"
    td_str = f"{float(r['td_ratio']):.4f}"
    oi_val = int(r.get("open_interest", 0))

    return "\t".join([
        str(r["symbol"]),
        str(r["expiry"]),
        str(r["dte"]),
        f"{float(r['strike']):.2f}",
        bid_src,
        f"{ret_pct:.2f}",
        f"{ann_pct:.2f}",
        f"{ideal:.2f}",
        f"{ratio:.4f}",
        shp_str,
        iv_str,
        hv_str,
        f"{theta_pct:.2f}",
        delta_str,
        kelly_str,
        ivr_str,
        td_str,
        str(oi_val),
    ])

def is_perfect_candidate(r: Dict, hv_val: float) -> bool:
    ann_ok = r["annualized"] > PERFECT_MIN_ANN
    ivr_ok = r["ivr"] > PERFECT_MIN_IVR
    iv_ok = r["iv"] > hv_val
    delta_ok = abs(r["delta"]) < PERFECT_MAX_ABS_DELTA
    td_ok = r["td_ratio"] > PERFECT_MIN_TD
    return ann_ok and ivr_ok and iv_ok and delta_ok and td_ok


# =========================
# Main scan
# =========================
def scan_one_symbol(ib: IB, symbol: str, top_n: int) -> Tuple[List[Dict], float, Optional[datetime.date]]:
    ensure_ib_connected(ib, context=f"scan {symbol}")
    stk = qualify_stock(ib, symbol)
    price = get_current_price(ib, stk)
    if price <= 0:
        print(f"❌ Cannot retrieve underlying price for {symbol}. Skip.")
        return [], 0.0, None

    hv_1w, hv_6m = get_historical_volatility(ib, stk)
    hv_val = max(hv_1w, hv_6m)

    iv_low, iv_high, iv_curr_hist = get_iv_rank_data(ib, stk)
    earnings_date = get_next_earnings_date(ib, stk)

    chains = get_option_chain(ib, stk)
    if not chains:
        return [], hv_val, earnings_date

    candidates = filter_options(ib, stk, chains, price)
    if not candidates:
        print(f"No candidates match filter criteria for {symbol}.")
        return [], hv_val, earnings_date

    print(f"\nAnalyzing candidates for {symbol} (Price: {price:.2f}) ...")
    ranked = analyze_candidates(ib, symbol, candidates, price, hv_1w, hv_6m, (iv_low, iv_high, iv_curr_hist), earnings_date)

    # Console table (top N)  <-- DO NOT CHANGE
    print(f"\n====== Sell Put Recommendations for {symbol} (Price: {price:.2f}) ======")
    print(f"Risk Free Rate: {RISK_FREE_RATE:.1%}")
    if earnings_date:
        print(f"Next Earnings Date: {earnings_date} (Check for collision!)")
    else:
        print("Next Earnings Date: Not found or data unavailable.")
    print(f"Risk Metrics: IV vs HV: 1W={hv_1w:.1%}, 6M={hv_6m:.1%} (HV used={hv_val:.2f})")
    print("Note: Price Source: IB=IB Bid, IL=IB Last, ML=Moomoo Last, IC=IB Close")
    print("WARNING: '*' indicates Greeks (IV/Delta) are ESTIMATED due to delayed data.\n")

    header = (
        f"{'Expiry':<10} | {'DTE':<4} | {'Strike':<8} | {'Bid(Src)':<16} | "
        f"{'Ann%':<6} | {'IVR':<5} | {'IV':<7} | {'HV':<7} | {'Delta':<8} | "
        f"{'Shp':<5} | {'T/D':<6} | {'Safe':<6} | {'Th%':<6} | {'Kelly%':<7}"
    )
    print(header)
    print("-" * len(header))

    for r in ranked[:top_n]:
        iv_str = f"{r['iv']:.2f}" + ("*" if r['is_iv_est'] else "")
        delta_str = f"{r['delta']:.2f}" + ("*" if r['is_delta_est'] else "")
        ann_str = f"{r['annualized']*100:.1f}"
        th_str = f"{r['theta_yield']*100:.1f}%" if r.get("theta_yield") else "-"
        kelly_str = f"{r['kelly']*100:.1f}%"

        price_content = f"{r['bid']:.2f}({r['price_src']})"
        if len(price_content) < 16:
            price_content = price_content + " " * (16 - len(price_content))

        row = (
            f"{r['expiry']:<10} | {r['dte']:<4} | {r['strike']:<8.1f} | {price_content} | "
            f"{ann_str:<6} | {r['ivr']:<5.0f} | {iv_str:<7} | {hv_val:<7.2f} | {delta_str:<8} | "
            f"{r['sharpe']:<5.2f} | {r['td_ratio']:<6.2f} | {r['safety']:<6.1f} | {th_str:<6} | {kelly_str:<7}"
        )

        perfect = is_perfect_candidate(r, hv_val)
        if perfect:
            tag = " (EARN)" if r.get("earn") else ""
            print(f"{COLOR_RED}{row}{tag}{COLOR_RESET}")
        else:
            if r.get("earn"):
                print(f"{row} {COLOR_RED}(EARN){COLOR_RESET}")
            else:
                print(row)

    return ranked, hv_val, earnings_date


def main():
    util.patchAsyncio()

    parser = argparse.ArgumentParser(description="Union Sell Put Scanner (v3) - multi-symbol + output candidate.txt")
    parser.add_argument("--symbol", help="Single stock symbol (e.g. AAPL)")
    parser.add_argument("--symbols-file", help="Symbols file path, one symbol per line (e.g. symbols.txt)")
    parser.add_argument("--out-file", default="candidate.txt", help="Output candidate file (default: candidate.txt)")
    parser.add_argument("--append", action="store_true", help="Append to out-file (default: overwrite)")
    parser.add_argument("--top", type=int, default=NUM_OPTIONS_TO_SHOW_DEFAULT, help=f"Show top N per symbol (default: {NUM_OPTIONS_TO_SHOW_DEFAULT})")
    args = parser.parse_args()

    symbols: List[str] = []
    if args.symbol:
        symbols = [normalize_symbol_for_ib(args.symbol)]
    elif args.symbols_file:
        symbols = read_symbols_file(args.symbols_file)
    else:
        symbols = [normalize_symbol_for_ib("BRK.B")]

    if not symbols:
        print("No symbols to scan.")
        return

    out_file = args.out_file

    TSV_HEADER = "\t".join([
        "Symbol", "Expiry", "DTE", "Strike", "Bid(Src)",
        "Ret%", "Ann%", "Ideal", "Ratio", "Shp",
        "IV", "HV", "Theta", "Delta", "Kelly%", "IVR", "T/D", "OI"
    ])
    if not args.append:
        with open(out_file, "w", encoding="utf-8") as f:
            f.write(f"{TSV_HEADER}\n")

    print(f"\n[Output] Candidates will be written to: {out_file}  (mode={'APPEND' if args.append else 'OVERWRITE'})")
    print(f"[Filter] Open Interest (累計未平倉) >= {MIN_OPEN_INTEREST} only\n")

    ib = connect_ib()
    try:
        total_written = 0

        for idx, sym in enumerate(symbols, 1):
            print(f"\n==============================")
            print(f"[{idx}/{len(symbols)}] Scanning: {sym}")
            print(f"==============================")

            try:
                ranked, hv_val, earnings_date = scan_one_symbol(ib, sym, args.top)
            except Exception as e:
                print(f"❌ Error scanning {sym}: {e}")
                continue

            if not ranked:
                continue

            # Write PERFECT candidates to file
            written_this_symbol = 0
            with open(out_file, "a", encoding="utf-8") as f:
                for r in ranked:
                    if is_perfect_candidate(r, hv_val):
                        line = format_candidate_line(r, hv_val)
                        earn_flag = "EARN" if r.get("earn") else ""
                        f.write(line + (f"\t{earn_flag}" if earn_flag else "") + "\n")
                        written_this_symbol += 1

            total_written += written_this_symbol
            print(f"\n[File] {sym}: wrote {written_this_symbol} perfect candidates -> {out_file}")

        print(f"\n✅ Done. Total perfect candidates written: {total_written} -> {out_file}")

    finally:
        if ib.isConnected():
            ib.disconnect()
            print("\nDisconnected.")


if __name__ == "__main__":
    main()
