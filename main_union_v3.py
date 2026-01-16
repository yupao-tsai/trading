#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import sys
import argparse
import datetime
from ib_insync import IB, Stock, Option, util
from typing import List, Optional, Dict, Tuple
import math
import statistics
import re
from dotenv import load_dotenv
import os

load_dotenv()  # 會自動讀取專案根目錄的 .env

# Optional imports
try:
    from moomoo import OpenSecTradeContext, TrdMarket, TrdEnv, SecurityFirm, RET_OK, OptionType
except ImportError:
    OpenSecTradeContext = None
    OptionType = None
    RET_OK = None
    TrdMarket = None
    TrdEnv = None
    SecurityFirm = None

try:
    import robin_stocks.robinhood as rh
    import pyotp
except ImportError:
    rh = None
    pyotp = None

# --- Black-Scholes Utility Functions ---
from math import log, sqrt, exp
from scipy.stats import norm

# ====== IBKR 設定 ======
HOST = "127.0.0.1"
PORT = 7496
CLIENT_ID = 11

PRIMARY_EXCHANGE = "NYSE"
CURRENCY = "USD"

# ====== 策略/篩選條件 (沿用 v2) ======
MIN_DTE = 7             # 最小到期天數
MAX_DTE = 60            # 最大到期天數
STRIKE_PCT_MIN = 0.88   # 履約價下限 (現價 * 0.88)
STRIKE_PCT_MAX = 0.99   # 履約價上限 (現價 * 0.99)
MIN_ANNUAL_RETURN = 0.05  # 最小年化報酬率過濾 (5%)
MIN_SHARPE_RATIO = 0.2    # 最小夏普值過濾 (0.2)

# 市場數據類型: 1=Live, 3=Delayed
MARKET_DATA_TYPE = 3

# 定存無風險利率
RISK_FREE_RATE = 0.033  # 3.3%

# 交易費用設定 (每口合約 Per Contract)
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

NUM_OPTIONS_TO_SHOW = 40


# ================== Robinhood helper ==================
def fetch_missing_option_details(rh_client, position):
    """
    Fetches missing instrument details (strike, type, expiration) for a position
    if they are not present, using the 'option' instrument URL.
    """
    if 'strike_price' not in position and 'option' in position:
        try:
            instr_url = position.get('option', '')
            if instr_url:
                instr_id = instr_url.rstrip('/').split('/')[-1]
                instr_data = rh_client.get_option_instrument_data_by_id(instr_id)
                if instr_data:
                    if 'strike_price' in instr_data:
                        position['strike_price'] = instr_data['strike_price']
                    if 'expiration_date' in instr_data:
                        position['expiration_date'] = instr_data['expiration_date']
                    if 'type' in instr_data:
                        position['type'] = instr_data['type']
        except Exception:
            pass
    return position


# ================== IB / Pricing ==================
def connect_ib() -> IB:
    ib = IB()
    print(f"Connecting to TWS/IBG {HOST}:{PORT} clientId={CLIENT_ID} ...")
    try:
        ib.connect(HOST, PORT, clientId=CLIENT_ID, timeout=10)
    except Exception as e:
        print(f"Connection failed: {e}")
        raise
    print("Connected:", ib.isConnected())
    return ib


def normalize_symbol_for_ib(sym: str) -> str:
    s = (sym or "").strip().upper()
    if s == "":
        return "BRK B"
    # Normalize: "BRK.B" -> "BRK B", "BRKB" -> "BRK B"
    if s in ("BRK.B", "BRKB"):
        return "BRK B"
    return s


def normalize_symbol_for_display(sym_ib: str) -> str:
    # IB uses "BRK B", display as "BRK.B"
    if sym_ib == "BRK B":
        return "BRK.B"
    return sym_ib


def qualify_stock(ib: IB, symbol_ib: str) -> Stock:
    stk = Stock(symbol_ib, "SMART", currency=CURRENCY, primaryExchange=PRIMARY_EXCHANGE)
    ib.qualifyContracts(stk)
    print(f"Qualified stock: {stk.symbol} (conId: {stk.conId})")
    return stk


def get_current_price(ib: IB, contract: Stock) -> float:
    """取得股票當前價格 (Delayed)；失敗則 fallback moomoo / robinhood"""
    ib.reqMarketDataType(MARKET_DATA_TYPE)
    ticker = ib.reqMktData(contract, "", False, False)

    print("Waiting for stock price...")
    price = 0.0

    for _ in range(20):
        ib.sleep(0.25)
        if ticker.last and not math.isnan(ticker.last) and ticker.last > 0:
            price = ticker.last
        elif ticker.close and not math.isnan(ticker.close) and ticker.close > 0:
            price = ticker.close

        if price > 0:
            print(f"Current Price ({'Delayed' if MARKET_DATA_TYPE==3 else 'Live'}): {price}")
            return price

    print("⚠️ Warning: Could not get valid price from IB. Trying Moomoo fallback...")

    # Fallback to Moomoo
    try:
        from moomoo import OpenQuoteContext, RET_OK, SubType
        moomoo_host = "127.0.0.1"
        moomoo_port = 11111

        base_symbol = contract.symbol.replace(" ", ".")
        if "US." not in base_symbol:
            base_symbol = "US." + base_symbol

        symbols_to_try = [base_symbol]
        if "BRK.B" in base_symbol:
            symbols_to_try = [base_symbol.replace("BRK.B", "BRKB"), base_symbol]

        quote_ctx = OpenQuoteContext(host=moomoo_host, port=moomoo_port)

        for sym in symbols_to_try:
            print(f"Trying Moomoo symbol: {sym}")
            quote_ctx.subscribe([sym], [SubType.QUOTE], subscribe_push=False)

            import time
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
            else:
                print(f"Moomoo quote failed for {sym}: ret={ret}")
                if hasattr(df, 'empty') and not df.empty:
                    print(f"DataFrame content: {df.to_dict()}")
                else:
                    print(f"Error detail: {df}")

        quote_ctx.close()

    except ImportError:
        print("Moomoo API not installed.")
    except Exception as e:
        print(f"Moomoo fallback failed: {e}")

    # Final Fallback: Robinhood
    if price == 0 and rh:
        print("Trying Robinhood fallback for price...")
        try:
            username = os.getenv("RH_USERNAME")
            password = os.getenv("RH_PASSWORD")

            if not username or not password:
                print("[Robinhood] Missing RH_USERNAME/RH_PASSWORD in environment, skip Robinhood price fallback.")
            else:
                try:
                    rh.get_account_info()
                except:
                    rh.login(username, password)

                rh_symbol = contract.symbol.replace(" ", ".")
                quotes = rh.get_quotes(rh_symbol)
                if quotes and len(quotes) > 0:
                    q = quotes[0]
                    if 'last_trade_price' in q and q['last_trade_price']:
                        price = float(q['last_trade_price'])
                    elif 'last_extended_hours_trade_price' in q and q['last_extended_hours_trade_price']:
                        price = float(q['last_extended_hours_trade_price'])
                    elif 'previous_close' in q and q['previous_close']:
                        price = float(q['previous_close'])

                    if price > 0:
                        print(f"Current Price (Robinhood): {price}")
                        return price
        except Exception as e:
            print(f"Robinhood price fetch failed: {e}")

    return 0.0


# ================== Vol / IV Rank / Earnings ==================
def get_option_chain(ib: IB, stk: Stock) -> List:
    print("Fetching option chains...")
    chains = ib.reqSecDefOptParams(stk.symbol, "", stk.secType, stk.conId)
    if not chains:
        print("No option chains found.")
        return []
    print(f"Found {len(chains)} raw chains.")
    return chains


def get_historical_volatility(ib: IB, contract: Stock) -> tuple[float, float]:
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

    if not log_returns:
        return 0.18, 0.18

    def calc_hv(returns):
        if len(returns) < 2:
            return 0.0
        daily_std = statistics.stdev(returns)
        return daily_std * math.sqrt(252)

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

        closes = [b.close for b in bars]
        iv_low = min(closes)
        iv_high = max(closes)
        iv_curr = closes[-1]
        return iv_low, iv_high, iv_curr
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
                e_date_str = date_elem.text
                try:
                    e_date = datetime.datetime.strptime(e_date_str, "%Y-%m-%d").date()
                    if e_date >= today:
                        return e_date
                except:
                    pass
        return None
    except Exception:
        return None


# ================== Strategy Core ==================
def filter_options(ib: IB, stk: Stock, chains: List, current_price: float) -> List[Option]:
    today = datetime.date.today()
    candidates = []

    target_strike_min = current_price * STRIKE_PCT_MIN
    target_strike_max = current_price * STRIKE_PCT_MAX

    print(f"Scanning chains for DTE {MIN_DTE}-{MAX_DTE} and Strike {target_strike_min:.2f}-{target_strike_max:.2f}...")

    seen_contracts = set()
    count_chains_processed = 0

    for chain in chains:
        if chain.exchange != 'SMART':
            continue

        count_chains_processed += 1

        valid_expiries = []
        for exp_str in chain.expirations:
            year = int(exp_str[:4])
            month = int(exp_str[4:6])
            day = int(exp_str[6:])
            exp_date = datetime.date(year, month, day)
            days_to_expire = (exp_date - today).days

            if MIN_DTE <= days_to_expire <= MAX_DTE:
                valid_expiries.append(exp_str)

        if not valid_expiries:
            continue

        for strike in chain.strikes:
            if target_strike_min <= strike <= target_strike_max:
                for exp_str in valid_expiries:
                    key = (exp_str, strike)
                    if key in seen_contracts:
                        continue
                    seen_contracts.add(key)

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

    print(f"Processed {count_chains_processed} SMART chains.")
    print(f"Generated {len(candidates)} verified candidate contracts.")
    return candidates


def calculate_kelly_fraction(annualized_ret, risk_free_rate, win_prob, iv, hv_short, hv_long):
    expected_annual_ret = annualized_ret * win_prob
    if expected_annual_ret <= risk_free_rate:
        return 0.0

    sigma = max(iv, hv_short, hv_long)
    if sigma < 0.1:
        sigma = 0.1

    fraction = (expected_annual_ret - risk_free_rate) / (sigma ** 2)
    return max(0, fraction * 0.5)


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


def bs_call_theta(S, K, T, r, sigma):
    if T <= 0 or sigma <= 0:
        return 0.0
    d1 = (log(S/K) + (r + 0.5 * sigma**2) * T) / (sigma * sqrt(T))
    d2 = d1 - sigma * sqrt(T)
    term1 = -(S * norm.pdf(d1) * sigma) / (2 * sqrt(T))
    term2 = r * K * exp(-r * T) * norm.cdf(d2)
    return term1 - term2


def implied_volatility(price, S, K, T, r, right='P'):
    if price <= 0:
        return 0.0

    low = 0.001
    high = 5.0
    for _ in range(50):
        mid = (low + high) / 2
        if right == 'C':
            est_price = bs_call_price(S, K, T, r, mid)
        else:
            est_price = bs_put_price(S, K, T, r, mid)

        if abs(est_price - price) < 0.001:
            return mid
        if est_price < price:
            low = mid
        else:
            high = mid
    return (low + high) / 2


# Per-run cache (隔離不同 symbol，避免污染)
OPTION_PRICE_CACHE_BY_SYMBOL: Dict[str, Dict[Tuple[str, str, float, str], Tuple[float, str, float, float]]] = {}


def _get_cache(symbol_ib: str):
    if symbol_ib not in OPTION_PRICE_CACHE_BY_SYMBOL:
        OPTION_PRICE_CACHE_BY_SYMBOL[symbol_ib] = {}
    return OPTION_PRICE_CACHE_BY_SYMBOL[symbol_ib]


def get_unified_option_price(
    ib: IB,
    symbol_ib: str,
    expiry_str: str,
    strike: float,
    right: str,
    moomoo_prices: Dict = None
) -> Tuple[float, str, float, float]:
    """
    統一獲取選擇權價格與 Greeks (IV/Delta)
    優先級: IB Bid (IB) > IB Last (IL) > Moomoo (ML) > IB Close (IC)
    回傳: (price, source_label, iv, delta)
    """
    cache = _get_cache(symbol_ib)
    cache_key = (symbol_ib, expiry_str.replace("-", ""), strike, right)
    if cache_key in cache:
        return cache[cache_key]

    price = 0.0
    src = ""
    iv = 0.0
    delta = 0.0
    ib_close = 0.0

    try:
        ib_expiry = expiry_str.replace("-", "")
        temp_opt = Option(symbol_ib, ib_expiry, strike, right, "SMART", CURRENCY, "100")
        ib.qualifyContracts(temp_opt)
        ticker = ib.reqMktData(temp_opt, "100,101,106", False, False)

        start_wait = datetime.datetime.now()
        while (datetime.datetime.now() - start_wait).total_seconds() < 2.0:
            ib.sleep(0.1)
            if (ticker.bid and ticker.bid > 0) or (ticker.last and ticker.last > 0) or (ticker.close and ticker.close > 0):
                break

        ib.cancelMktData(temp_opt)

        if ticker.modelGreeks:
            if ticker.modelGreeks.impliedVol:
                iv = ticker.modelGreeks.impliedVol
            if ticker.modelGreeks.delta:
                delta = ticker.modelGreeks.delta

        if ticker.bid and ticker.bid > 0:
            price = ticker.bid
            src = "IB"

        if price == 0 and ticker.last and ticker.last > 0:
            price = ticker.last
            src = "IL"

        if ticker.close and ticker.close > 0:
            ib_close = ticker.close

    except Exception:
        pass

    if price == 0 and moomoo_prices:
        key_clean = expiry_str.replace("-", "")
        if (key_clean, strike) in moomoo_prices:
            price = moomoo_prices[(key_clean, strike)]
            src = "ML"

    if price == 0 and ib_close > 0:
        price = ib_close
        src = "IC"

    if price > 0:
        cache[cache_key] = (price, src, iv, delta)

    return price, src, iv, delta


def get_moomoo_quote_for_options(symbol_ib: str, min_dte: int, max_dte: int) -> Dict[Tuple[str, float], float]:
    """
    使用 Moomoo get_option_chain 直接獲取並篩選選擇權報價 (支援分頁處理日期範圍)
    Returns: Dict[(ExpiryYYYYMMDD, Strike), price]
    """
    if OpenSecTradeContext is None or OptionType is None:
        return {}

    moomoo_host = "127.0.0.1"
    moomoo_port = 11111

    moomoo_base = symbol_ib.replace(" ", ".")
    if "US." not in moomoo_base:
        moomoo_base = "US." + moomoo_base

    adjusted_base = moomoo_base
    if "BRK.B" in moomoo_base:
        adjusted_base = moomoo_base.replace("BRK.B", "BRKB")

    today = datetime.date.today()
    start_date_overall = today + datetime.timedelta(days=min_dte)
    end_date_overall = today + datetime.timedelta(days=max_dte)

    price_map = {}

    try:
        from moomoo import OpenQuoteContext, SubType
        quote_ctx = OpenQuoteContext(host=moomoo_host, port=moomoo_port)

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
            quote_ctx.subscribe(batch, [SubType.QUOTE], subscribe_push=False)
            ret_q, df_quote = quote_ctx.get_stock_quote(batch)

            if ret_q == RET_OK and not df_quote.empty:
                for _, row in df_quote.iterrows():
                    code = row['code']
                    price = 0.0
                    if 'last_price' in row and row['last_price'] > 0:
                        price = row['last_price']
                    elif 'nominal_price' in row and row['nominal_price'] > 0:
                        price = row['nominal_price']

                    if price > 0:
                        match = re.search(r'(\d{6})([CP])(\d+)$', code)
                        if match:
                            date_str, type_char, strike_str = match.groups()
                            expiry_full = "20" + date_str
                            strike = int(strike_str) / 1000.0
                            price_map[(expiry_full, strike)] = price

            quote_ctx.unsubscribe(batch, [SubType.QUOTE])

        quote_ctx.close()

    except Exception as e:
        print(f"Error fetching Moomoo option chain quotes: {e}")

    return price_map


def analyze_candidates(
    ib: IB,
    symbol_ib: str,
    candidates: List[Option],
    current_price: float,
    hv_1w: float,
    hv_6m: float,
    iv_rank_info: Tuple[float, float, float] = (0, 0, 0),
    earnings_date: Optional[datetime.date] = None
) -> List[Dict]:
    results = []

    iv_low, iv_high, iv_curr_hist = iv_rank_info

    print("Fetching backup quotes from Moomoo (via Option Chain)...")
    moomoo_prices = get_moomoo_quote_for_options(symbol_ib, MIN_DTE, MAX_DTE)
    print(f"Got {len(moomoo_prices)} quotes from Moomoo.")

    chunk_size = 50
    for i in range(0, len(candidates), chunk_size):
        chunk = candidates[i:i+chunk_size]
        print(f"Processing batch {i+1} to {min(i+chunk_size, len(candidates))}...")

        ib.qualifyContracts(*chunk)

        ib.reqMarketDataType(MARKET_DATA_TYPE)
        tickers = []
        for opt in chunk:
            t = ib.reqMktData(opt, "100,101,106", False, False)
            tickers.append((opt, t))

        print("Waiting for option quotes...")
        for _ in range(40):
            ib.sleep(0.25)
            valid_count = sum(1 for _, t in tickers if (t.bid and t.bid > 0) or (t.close and t.close > 0) or (t.last and t.last > 0))
            if valid_count > len(chunk) * 0.99:
                break

        today = datetime.date.today()

        for opt, ticker in tickers:
            ib.cancelMktData(opt)

            expiry_display = opt.lastTradeDateOrContractMonth

            final_price = 0.0
            price_src = ""
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
                final_price = ticker.bid
                price_src = "IB"

            if final_price == 0 and ticker.last and ticker.last > 0:
                final_price = ticker.last
                price_src = "IL"

            if final_price == 0:
                key = (opt.lastTradeDateOrContractMonth, opt.strike)
                if key in moomoo_prices and moomoo_prices[key] > 0:
                    final_price = moomoo_prices[key]
                    price_src = "ML"

            if final_price == 0:
                if ticker.close and ticker.close > 0:
                    final_price = ticker.close
                    price_src = "IC"

            if final_price <= 0.05:
                continue

            # cache for consistency
            cache = _get_cache(symbol_ib)
            cache_key = (symbol_ib, expiry_display, opt.strike, opt.right)
            cache[cache_key] = (final_price, price_src, iv, delta)

            bid = final_price

            year = int(opt.lastTradeDateOrContractMonth[:4])
            month = int(opt.lastTradeDateOrContractMonth[4:6])
            day = int(opt.lastTradeDateOrContractMonth[6:])
            exp_date = datetime.date(year, month, day)
            dte = (exp_date - today).days
            if dte == 0:
                dte = 1
            T = dte / 365.0

            fees_per_share = FEES_PER_CONTRACT / 100.0
            net_bid = bid - fees_per_share
            if net_bid < 0:
                net_bid = 0.0

            ret = net_bid / opt.strike
            annualized_ret = ret * (365 / dte)

            buffer_pct = 0.0
            if current_price > 0:
                buffer_pct = (current_price - opt.strike) / current_price

            target_bid_5pct = opt.strike * 0.05 * dte / 365.0

            is_iv_estimated = False
            is_delta_estimated = False

            if iv == 0:
                is_iv_estimated = True
                iv = implied_volatility(bid, current_price, opt.strike, T, RISK_FREE_RATE)

            if delta == 0:
                is_delta_estimated = True
                delta = bs_put_delta(current_price, opt.strike, T, RISK_FREE_RATE, iv)

            if theta == 0:
                theta_annual = bs_put_theta(current_price, opt.strike, T, RISK_FREE_RATE, iv)
                theta = theta_annual / 365.0

            theta_yield = 0.0
            if bid > 0 and theta != 0:
                theta_yield = abs(theta) / bid

            pop = 1.0 - abs(delta)
            if pop < 0:
                pop = 0

            target_kelly_allocation = 0.10
            target_fraction = target_kelly_allocation * 2
            conservative_vol = max(iv, hv_1w, hv_6m)
            required_exp_ret = target_fraction * (conservative_vol ** 2) + RISK_FREE_RATE

            abs_delta = abs(delta)
            if (1.0 - abs_delta) > 0.1:
                required_ann_ret = required_exp_ret / (1.0 - abs_delta)
            else:
                required_ann_ret = required_exp_ret / 0.1

            ideal_bid = opt.strike * required_ann_ret * (dte / 365.0)

            win_prob = 1.0 - abs_delta
            if win_prob < 0:
                win_prob = 0

            kelly_pct = calculate_kelly_fraction(annualized_ret, RISK_FREE_RATE, win_prob, iv, hv_1w, hv_6m)

            vol_risk = max(iv, hv_1w, hv_6m)
            sharpe = 0.0
            if vol_risk > 0.01:
                sharpe = (annualized_ret - RISK_FREE_RATE) / vol_risk

            if annualized_ret < MIN_ANNUAL_RETURN:
                continue
            if sharpe < MIN_SHARPE_RATIO:
                continue

            ivr = 0.0
            if iv_high > iv_low:
                ivr = (iv - iv_low) / (iv_high - iv_low) * 100

            expected_move = current_price * iv * math.sqrt(T)
            safety_factor = 99.9
            if expected_move > 0:
                safety_factor = (current_price - opt.strike) / expected_move

            td_ratio = 0.0
            if abs(delta) > 0.001:
                td_ratio = abs(theta) / abs(delta)

            earnings_collision = False
            if earnings_date:
                if today <= earnings_date <= exp_date:
                    earnings_collision = True

            results.append({
                "symbol": symbol_ib,
                "expiry": opt.lastTradeDateOrContractMonth,
                "strike": opt.strike,
                "bid": bid,
                "price_src": price_src,
                "ask": ticker.ask if ticker.ask else 0.0,
                "last": ticker.last if ticker.last else 0.0,
                "dte": dte,
                "return": ret,
                "annualized": annualized_ret,
                "target_bid": target_bid_5pct,
                "ideal_bid": ideal_bid,
                "iv": iv,
                "delta": delta,
                "pop": pop,
                "buffer": buffer_pct,
                "theta": theta,
                "theta_yield": theta_yield,
                "is_iv_est": is_iv_estimated,
                "is_delta_est": is_delta_estimated,
                "kelly": kelly_pct,
                "sharpe": sharpe,
                "ivr": ivr,
                "safety": safety_factor,
                "td_ratio": td_ratio,
                "earn": earnings_collision,
                "conId": opt.conId
            })

    return sorted(results, key=lambda x: x['kelly'], reverse=True)


# ================== Moomoo / Robinhood Positions (沿用 v2，僅把 SYMBOL 改成參數) ==================
def get_moomoo_positions(ib: IB, symbol_ib: str, underlying_price: float, hv_1w: float, hv_6m: float) -> List[str]:
    report_lines = []

    if OpenSecTradeContext is None:
        report_lines.append("\n[Moomoo] API package not found, skipping position check.")
        return report_lines

    report_lines.append(f"\n====== Moomoo Positions for {normalize_symbol_for_display(symbol_ib)} ======")
    moomoo_host = "127.0.0.1"
    moomoo_port = 11111

    moomoo_symbol_base = symbol_ib.replace(" ", ".")
    if "US." not in moomoo_symbol_base:
        moomoo_symbol_base = "US." + moomoo_symbol_base

    try:
        firm = getattr(SecurityFirm, 'FUTUINC', None)
        if not firm:
            firm = getattr(SecurityFirm, 'FUTUSG', None)

        if firm:
            trd_ctx = OpenSecTradeContext(filter_trdmarket=TrdMarket.US, host=moomoo_host, port=moomoo_port, security_firm=firm)
        else:
            trd_ctx = OpenSecTradeContext(filter_trdmarket=TrdMarket.US, host=moomoo_host, port=moomoo_port)

        ret, df = trd_ctx.position_list_query(trd_env=TrdEnv.REAL)
        trd_ctx.close()

        if ret != RET_OK:
            report_lines.append(f"Moomoo Error: {df}")
            return report_lines

        if df.empty:
            report_lines.append("No positions found in Moomoo.")
            return report_lines

        target = moomoo_symbol_base.upper().replace("US.", "")
        filtered = df[df['code'].str.contains(target, case=False, regex=False)]

        if filtered.empty:
            simple_target = target.split('.')[0]
            filtered = df[df['code'].str.contains(simple_target, case=False, regex=False)]

        if filtered.empty:
            report_lines.append(f"No positions found for {symbol_ib} (Target: {target}).")
            return report_lines

        cols = ['code', 'stock_name', 'qty', 'cost_price', 'current_price', 'pl_ratio', 'nominal_price']
        cols = [c for c in cols if c in filtered.columns]
        filtered_sorted = filtered.sort_values('code')

        report_lines.append("--- Raw Positions (Sorted by Code/Expiry) ---")
        report_lines.append(filtered_sorted[cols].to_string(index=False))

        report_lines.append("\n--- Position Analysis (Calculated based on Current Market Price) ---")
        report_lines.append("Note: 'Ann%' and 'Kelly%' are calculated as if opening a NEW position at current price.")
        report_lines.append(f"{'Expiry':<10} | {'DTE':<4} | {'Strike':<8} | {'Price(Src)':<12} | {'Qty':<4} | {'P/L%':<7} | {'Ann%':<6} | {'Ideal':<8} | {'Ratio':<6} | {'Shp':<4} | {'IV':<6} | {'Delta':<7} | {'Kelly%':<7}")
        report_lines.append("-" * 145)

        today = datetime.date.today()
        opt_pattern = re.compile(r'(\d{6})([CP])(\d+)$')

        position_rows = []

        for _, row in filtered.iterrows():
            code = row['code']
            match = opt_pattern.search(code)
            if not match:
                continue

            date_str, opt_type, strike_str = match.groups()

            year = 2000 + int(date_str[:2])
            month = int(date_str[2:4])
            day = int(date_str[4:])
            exp_date = datetime.date(year, month, day)
            expiry_display = exp_date.strftime("%Y%m%d")

            strike = int(strike_str) / 1000.0

            dte = (exp_date - today).days
            if dte == 0:
                dte = 1
            T = dte / 365.0

            moomoo_fallback_price = 0.0
            if 'current_price' in row and row['current_price'] > 0:
                moomoo_fallback_price = row['current_price']
            elif 'nominal_price' in row and row['nominal_price'] > 0:
                moomoo_fallback_price = row['nominal_price']

            mm_cache = {(expiry_display, strike): moomoo_fallback_price}

            current_opt_price, price_src, iv, delta = get_unified_option_price(
                ib, symbol_ib, expiry_display, strike, opt_type, mm_cache
            )

            if current_opt_price <= 0:
                current_opt_price = moomoo_fallback_price
                if current_opt_price > 0:
                    price_src = "M(Raw)"

            fees_per_share = FEES_PER_CONTRACT / 100.0
            net_bid = current_opt_price - fees_per_share
            if net_bid < 0:
                net_bid = 0.0

            ret_val = net_bid / strike
            annualized_ret = ret_val * (365 / dte)

            if iv == 0:
                iv = implied_volatility(current_opt_price, underlying_price, strike, T, RISK_FREE_RATE, right=opt_type)

            if delta == 0:
                if opt_type == 'C':
                    delta = bs_call_delta(underlying_price, strike, T, RISK_FREE_RATE, iv)
                else:
                    delta = bs_put_delta(underlying_price, strike, T, RISK_FREE_RATE, iv)

            target_kelly_allocation = 0.10
            target_fraction = target_kelly_allocation * 2
            conservative_vol = max(iv, hv_1w, hv_6m)
            required_exp_ret = target_fraction * (conservative_vol ** 2) + RISK_FREE_RATE

            abs_delta = abs(delta)
            if (1.0 - abs_delta) > 0.1:
                required_ann_ret = required_exp_ret / (1.0 - abs_delta)
            else:
                required_ann_ret = required_exp_ret / 0.1

            ideal_bid = strike * required_ann_ret * (dte / 365.0)

            win_prob = 1.0 - abs_delta
            if win_prob < 0:
                win_prob = 0

            kelly_pct = calculate_kelly_fraction(annualized_ret, RISK_FREE_RATE, win_prob, iv, hv_1w, hv_6m)

            vol_risk = max(iv, hv_1w, hv_6m)
            sharpe = 0.0
            if vol_risk > 0.01:
                sharpe = (annualized_ret - RISK_FREE_RATE) / vol_risk

            ratio = 0.0
            if ideal_bid > 0:
                ratio = current_opt_price / ideal_bid

            pl_val = row.get('pl_ratio', 0.0)
            pl_str = f"{pl_val:.1f}%"

            position_rows.append({
                'expiry': expiry_display,
                'exp_date_obj': exp_date,
                'dte': dte,
                'strike': strike,
                'type': opt_type,
                'price': current_opt_price,
                'src': f"({price_src})",
                'qty': row['qty'],
                'pl_str': pl_str,
                'ann': annualized_ret,
                'ideal': ideal_bid,
                'ratio': ratio,
                'sharpe': sharpe,
                'iv': iv,
                'delta': delta,
                'kelly': kelly_pct
            })

        position_rows.sort(key=lambda x: (0 if x['type'] == 'P' else 1, x['exp_date_obj'], x['strike']))

        last_type = None
        for row in position_rows:
            if last_type is not None and row['type'] != last_type:
                report_lines.append("-" * 130)
            last_type = row['type']

            strike_display = f"{row['strike']}{row['type']}"
            price_disp = f"{row['price']:.2f}{row['src']}"
            row_str = f"{row['expiry']:<10} | {row['dte']:<4} | {strike_display:<8} | {price_disp:<12} | {row['qty']:<4} | {row['pl_str']:<7} | {row['ann']*100:<6.1f} | {row['ideal']:<8.2f} | {row['ratio']:<6.2f} | {row['sharpe']:<4.2f} | {row['iv']:<6.2f} | {row['delta']:>7.2f} | {row['kelly']*100:>6.1f}%"

            if row['ratio'] > 1.0:
                abs_delta = abs(row['delta'])
                row_color = COLOR_GREEN
                if abs_delta < 0.20:
                    row_color = COLOR_RED
                elif 0.20 <= abs_delta <= 0.30:
                    row_color = COLOR_GREEN
                elif abs_delta > 0.30:
                    row_color = COLOR_BLUE
                report_lines.append(f"{row_color}{row_str}{COLOR_RESET}")
            else:
                report_lines.append(row_str)

    except Exception as e:
        report_lines.append(f"Failed to fetch Moomoo positions: {e}")

    return report_lines


def get_robinhood_positions(ib: IB, symbol_ib: str, underlying_price: float, hv_1w: float, hv_6m: float):
    if rh is None:
        print("\n[Robinhood] API package not found (robin_stocks), skipping position check.")
        return

    print(f"\n====== Robinhood Positions for {normalize_symbol_for_display(symbol_ib)} ======")

    username = os.getenv("RH_USERNAME")
    password = os.getenv("RH_PASSWORD")
    totp_key = os.getenv("RH_TOTP_KEY")  # optional

    if not username or not password:
        print("[Robinhood] Missing RH_USERNAME/RH_PASSWORD in environment, skipping Robinhood positions.")
        return

    rh_symbol = symbol_ib.replace(" ", ".")
    if "BRK" in rh_symbol and "B" in rh_symbol and "BRK.B" not in rh_symbol:
        pass

    def get_price_for_rh_position(ib: IB, symbol_ib: str, exp_date_str: str, strike: float, opt_type: str, rh_symbol: str):
        expiry_display = exp_date_str.replace("-", "")

        mm_price = 0.0
        try:
            from moomoo import OpenQuoteContext, RET_OK, SubType
            moomoo_host = "127.0.0.1"
            moomoo_port = 11111

            yy = exp_date_str[2:4]
            mm = exp_date_str[5:7]
            dd = exp_date_str[8:10]
            date_part = f"{yy}{mm}{dd}"
            strike_int = int(strike * 1000)
            strike_part = f"{strike_int:08d}"

            base_sym = rh_symbol.replace(" ", ".")
            if "BRK.B" in base_sym:
                base_sym = "BRKB"

            full_code = f"US.{base_sym}{date_part}{opt_type}{strike_part}"

            q_ctx = OpenQuoteContext(host=moomoo_host, port=moomoo_port)
            q_ctx.subscribe([full_code], [SubType.QUOTE], subscribe_push=False)
            ret, df = q_ctx.get_stock_quote([full_code])
            q_ctx.close()

            if ret == RET_OK and not df.empty:
                row = df.iloc[0]
                if 'last_price' in row and row['last_price'] > 0:
                    mm_price = float(row['last_price'])
                elif 'nominal_price' in row and row['nominal_price'] > 0:
                    mm_price = float(row['nominal_price'])
        except:
            pass

        mm_cache = {(expiry_display, strike): mm_price}
        return get_unified_option_price(ib, symbol_ib, expiry_display, strike, opt_type, mm_cache)

    try:
        if totp_key:
            totp = pyotp.TOTP(totp_key).now()
            rh.login(username, password, mfa_code=totp)
        else:
            rh.login(username, password)

        holdings = rh.build_holdings()

        if rh_symbol in holdings:
            data = holdings[rh_symbol]
            print("--- Stock Position ---")
            print(f"Symbol: {rh_symbol}")
            print(f"Qty: {data.get('quantity')}")
            print(f"Avg Buy Price: {data.get('average_buy_price')}")
            print(f"Current Price: {data.get('price')}")
            print(f"Equity: {data.get('equity')}")
            print(f"Percent Change: {data.get('percent_change')}%")
            print(f"Equity Change: {data.get('equity_change')}")
        else:
            print(f"No stock position found for {rh_symbol}.")

        print("\n--- Option Positions ---")

        option_positions = rh.get_open_option_positions()

        if not option_positions:
            try:
                all_positions = rh.get_all_option_positions()
                option_positions = [p for p in all_positions if float(p.get('quantity', 0)) > 0]
            except AttributeError:
                pass
            except Exception:
                pass

        filtered_opts = []
        for p in option_positions:
            chain_sym = p.get('chain_symbol', '')
            if chain_sym == rh_symbol:
                filtered_opts.append(p)
            elif chain_sym.replace(".", "") == rh_symbol.replace(".", ""):
                filtered_opts.append(p)
            elif chain_sym == symbol_ib:
                filtered_opts.append(p)

        if not filtered_opts:
            print(f"No option positions found for {rh_symbol} (IB: {symbol_ib}).")
            return

        today = datetime.date.today()
        position_rows = []

        for op in filtered_opts:
            fetch_missing_option_details(rh, op)

            if 'strike_price' not in op:
                continue

            strike = float(op['strike_price'])
            opt_type = op['type'].upper()
            if opt_type == 'CALL':
                opt_type = 'C'
            if opt_type == 'PUT':
                opt_type = 'P'

            exp_date_str = op['expiration_date']
            year, month, day = map(int, exp_date_str.split('-'))
            exp_date = datetime.date(year, month, day)
            expiry_display = exp_date.strftime("%Y%m%d")

            dte = (exp_date - today).days
            if dte == 0:
                dte = 1
            T = dte / 365.0

            qty = float(op['quantity'])
            avg_cost = float(op['average_price'])

            current_opt_price, price_src_label, iv, delta = get_price_for_rh_position(
                ib, symbol_ib, exp_date_str, strike, opt_type, rh_symbol
            )

            if current_opt_price == 0:
                market_data = None
                if 'option' in op:
                    try:
                        instr_url = op.get('option', '')
                        if instr_url:
                            instr_id = instr_url.rstrip('/').split('/')[-1]
                            market_data = rh.get_option_market_data_by_id(instr_id)
                    except:
                        pass

                if not market_data:
                    market_data = rh.get_option_market_data(rh_symbol, exp_date_str, strike, op['type'])

                if market_data:
                    if isinstance(market_data, list) and len(market_data) > 0:
                        md = market_data[0]
                    elif isinstance(market_data, dict):
                        md = market_data
                    else:
                        md = None

                    if md:
                        if 'adjusted_mark_price' in md:
                            current_opt_price = float(md['adjusted_mark_price'])
                            price_src_label = "R"

                        if iv == 0 and 'implied_volatility' in md and md['implied_volatility']:
                            try:
                                iv = float(md['implied_volatility'])
                            except:
                                pass
                        if delta == 0 and 'delta' in md and md['delta']:
                            try:
                                delta = float(md['delta'])
                            except:
                                pass

            fees_per_share = FEES_PER_CONTRACT / 100.0
            net_bid = current_opt_price - fees_per_share
            if net_bid < 0:
                net_bid = 0.0

            ret_val = net_bid / strike
            annualized_ret = ret_val * (365 / dte)

            if iv == 0:
                iv = implied_volatility(current_opt_price, underlying_price, strike, T, RISK_FREE_RATE, right=opt_type)
            if delta == 0:
                if opt_type == 'C':
                    delta = bs_call_delta(underlying_price, strike, T, RISK_FREE_RATE, iv)
                else:
                    delta = bs_put_delta(underlying_price, strike, T, RISK_FREE_RATE, iv)

            target_kelly_allocation = 0.10
            target_fraction = target_kelly_allocation * 2
            conservative_vol = max(iv, hv_1w, hv_6m)
            required_exp_ret = target_fraction * (conservative_vol ** 2) + RISK_FREE_RATE

            abs_delta = abs(delta)
            if (1.0 - abs_delta) > 0.1:
                required_ann_ret = required_exp_ret / (1.0 - abs_delta)
            else:
                required_ann_ret = required_exp_ret / 0.1

            ideal_bid = strike * required_ann_ret * (dte / 365.0)

            win_prob = 1.0 - abs_delta
            if win_prob < 0:
                win_prob = 0

            kelly_pct = calculate_kelly_fraction(annualized_ret, RISK_FREE_RATE, win_prob, iv, hv_1w, hv_6m)

            vol_risk = max(iv, hv_1w, hv_6m)
            sharpe = 0.0
            if vol_risk > 0.01:
                sharpe = (annualized_ret - RISK_FREE_RATE) / vol_risk

            ratio = 0.0
            if ideal_bid > 0:
                ratio = current_opt_price / ideal_bid

            pl_pct = 0.0
            if avg_cost > 0:
                if qty > 0:
                    pl_pct = (current_opt_price - avg_cost) / avg_cost
                else:
                    pl_pct = (avg_cost - current_opt_price) / avg_cost

            position_rows.append({
                'expiry': expiry_display,
                'exp_date_obj': exp_date,
                'dte': dte,
                'strike': strike,
                'type': opt_type,
                'price': current_opt_price,
                'src': f"({price_src_label})",
                'qty': qty,
                'pl_pct': pl_pct,
                'ann': annualized_ret,
                'ideal': ideal_bid,
                'ratio': ratio,
                'sharpe': sharpe,
                'iv': iv,
                'delta': delta,
                'kelly': kelly_pct
            })

        position_rows.sort(key=lambda x: (0 if x['type'] == 'P' else 1, x['exp_date_obj'], x['strike']))

        print(f"{'Expiry':<10} | {'DTE':<4} | {'Strike':<8} | {'Price(Src)':<12} | {'Qty':<4} | {'P/L%':<7} | {'Ann%':<6} | {'Ideal':<8} | {'Ratio':<6} | {'Shp':<4} | {'IV':<6} | {'Delta':<7} | {'Kelly%':<7}")
        print("-" * 145)

        last_type = None
        for row in position_rows:
            if last_type is not None and row['type'] != last_type:
                print("-" * 130)
            last_type = row['type']

            price_disp = f"{row['price']:.2f}{row['src']}"
            strike_disp = f"{row['strike']}{row['type']}"
            pl_str = f"{row['pl_pct']*100:.1f}%"

            row_str = f"{row['expiry']:<10} | {row['dte']:<4} | {strike_disp:<8} | {price_disp:<12} | {row['qty']:<4.1f} | {pl_str:<7} | {row['ann']*100:<6.1f} | {row['ideal']:<8.2f} | {row['ratio']:<6.2f} | {row['sharpe']:<4.2f} | {row['iv']:<6.2f} | {row['delta']:>7.2f} | {row['kelly']*100:>6.1f}%"

            if row['ratio'] > 1.0:
                abs_d = abs(row['delta'])
                row_color = COLOR_GREEN
                if abs_d < 0.20:
                    row_color = COLOR_RED
                elif 0.20 <= abs_d <= 0.30:
                    row_color = COLOR_GREEN
                elif abs_d > 0.30:
                    row_color = COLOR_BLUE
                print(f"{row_color}{row_str}{COLOR_RESET}")
            else:
                print(row_str)

    except Exception as e:
        print(f"Robinhood Error: {e}")


# ================== Output candidate.txt (v3) ==================
def write_candidates(path: str, rows: List[Dict], symbol_ib: str, underlying_price: float, top_n: int):
    """
    每行包含 symbol（你的需求）
    """
    disp = normalize_symbol_for_display(symbol_ib)
    now = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    header = (
        f"# Generated at {now}\n"
        f"# Columns: symbol, underlying, expiry, right, strike, bid, src, dte, ann, ideal, ratio, iv, ivr, delta, sharpe, kelly, earn\n"
    )

    # 檔案策略：如果檔案不存在就加 header；存在就 append（多 symbol 用同一份）
    need_header = not os.path.exists(path)
    with open(path, "a", encoding="utf-8") as f:
        if need_header:
            f.write(header)

        for r in rows[:top_n]:
            ratio = (r['bid'] / r['ideal_bid']) if r.get('ideal_bid', 0) > 0 else 0.0
            line = (
                f"{disp},"
                f"{underlying_price:.4f},"
                f"{r['expiry']},"
                f"{'P'},"
                f"{r['strike']:.4f},"
                f"{r['bid']:.4f},"
                f"{r['price_src']},"
                f"{r['dte']},"
                f"{r['annualized']:.6f},"
                f"{r['ideal_bid']:.4f},"
                f"{ratio:.4f},"
                f"{r['iv']:.6f},"
                f"{r['ivr']:.2f},"
                f"{r['delta']:.6f},"
                f"{r['sharpe']:.6f},"
                f"{r['kelly']:.6f},"
                f"{1 if r['earn'] else 0}\n"
            )
            f.write(line)


# ================== Single symbol runner ==================
def run_for_symbol(ib: IB, symbol_ib: str, output_file: str, top_n: int):
    stk = qualify_stock(ib, symbol_ib)
    price = get_current_price(ib, stk)
    if price <= 0:
        print(f"❌ Cannot retrieve underlying price for {normalize_symbol_for_display(symbol_ib)}. Skip.")
        return

    hv_1w, hv_6m = get_historical_volatility(ib, stk)
    iv_low, iv_high, iv_curr_hist = get_iv_rank_data(ib, stk)
    earnings_date = get_next_earnings_date(ib, stk)

    chains = get_option_chain(ib, stk)
    if not chains:
        return

    candidates = filter_options(ib, stk, chains, price)
    if not candidates:
        print("No candidates match the filter criteria.")
        return

    print("\nAnalyzing candidates (fetching delayed quotes)...")
    ranked_results = analyze_candidates(
        ib, symbol_ib, candidates, price, hv_1w, hv_6m,
        (iv_low, iv_high, iv_curr_hist),
        earnings_date
    )

    # v3: write candidate file with symbol column
    if ranked_results:
        write_candidates(output_file, ranked_results, symbol_ib, price, top_n)

    print("\nFetching Moomoo positions (please wait)...")
    moomoo_report_lines = get_moomoo_positions(ib, symbol_ib, price, hv_1w, hv_6m)

    print("Fetching Robinhood positions (please wait)...")
    get_robinhood_positions(ib, symbol_ib, price, hv_1w, hv_6m)

    print("\nData fetching complete. Generating report...\n")

    disp_sym = normalize_symbol_for_display(symbol_ib)
    print(f"\n====== Sell Put Recommendations for {disp_sym} (Price: {price}) ======")
    print(f"Risk Free Rate: {RISK_FREE_RATE:.1%}")
    if earnings_date:
        print(f"Next Earnings Date: {earnings_date} (Check for collision!)")
    else:
        print("Next Earnings Date: Not found or data unavailable.")

    print(f"Risk Metrics: IV(Implied) vs HV(Hist): 1W={hv_1w:.1%}, 6M={hv_6m:.1%}")
    print("Note: Ideal Bid assumes a target Kelly allocation of 10% (Half-Kelly).")
    print("Note: Price Source: IB=IB Bid, IL=IB Last, ML=Moomoo Last, IC=IB Close, R=Robinhood Mark, M(Raw)=Moomoo position price.")
    print("      IL/IC/ML/R/M(Raw) may be stale compared to live Bid.")
    print("Note: Th% = Daily Theta Decay / Bid Price.")
    print("WARNING: '*' indicates Greeks (IV/Delta) are ESTIMATED due to delayed data.")

    header = f"{'Expiry':<10} | {'DTE':<4} | {'Strike':<8} | {'Bid(Src)':<16} | {'Ann%':<6} | {'Ideal':<8} | {'Ratio':<6} | {'IVR':<4} | {'Safe':<5} | {'T/D':<5} | {'IV':<6} | {'HV':<6} | {'Delta':<7} | {'Shp':<4} | {'Th%':<5} | {'Kelly%':<7}"
    sub_header = f"{'':<10} | {'':<4} | {'':<8} | {'':<16} | {'>15%':<6} | {'':<8} | {'>1.0':<6} | {'>30':<4} | {'>1.0':<5} | {'>0.4':<5} | {'>HV':<6} | {'':<6} | {'<.30':<7} | {'>.2':<4} | {'High':<5} | {'High':<7}"

    print(header)
    print(sub_header)
    print("-" * 170)

    hv_val = max(hv_1w, hv_6m)

    for res in ranked_results[:top_n]:
        iv_val = res['iv']
        delta_val = res['delta']
        ann_val = res['annualized']
        ivr_val = res['ivr']
        td_val = res['td_ratio']

        pass_ivr = ivr_val > 30
        pass_iv_hv = iv_val > hv_val
        pass_delta = abs(delta_val) < 0.30
        pass_ann = ann_val > 0.15
        pass_td = td_val > 0.4

        is_perfect = pass_ivr and pass_iv_hv and pass_delta and pass_ann and pass_td

        def color_cell(text, condition):
            if is_perfect:
                return text
            if condition:
                return f"{COLOR_GREEN}{text}{COLOR_RESET}"
            return text

        iv_str = f"{iv_val:.2f}" + ("*" if res['is_iv_est'] else "")
        delta_str = f"{delta_val:.2f}" + ("*" if res['is_delta_est'] else "")
        hv_str = f"{hv_val:.2f}"

        theta_pct_str = "-"
        if res.get('theta_yield'):
            theta_pct_str = f"{res['theta_yield']*100:.1f}%"

        price_content = f"{res['bid']:.2f}({res['price_src']})"
        ratio = 0.0
        if res['ideal_bid'] > 0:
            ratio = res['bid'] / res['ideal_bid']

        is_good_deal = res['bid'] > res['ideal_bid']
        if is_good_deal:
            price_content += " !"

        price_padding = 16 - len(price_content)
        if price_padding < 0:
            price_padding = 0
        price_field = price_content + " " * price_padding

        expiry_s = f"{res['expiry']}"
        dte_s = f"{res['dte']}"
        strike_s = f"{res['strike']:.1f}"
        ann_s = f"{ann_val*100:.1f}"
        ideal_s = f"{res['ideal_bid']:.2f}"
        ratio_s = f"{ratio:.2f}"
        ivr_s = f"{ivr_val:.0f}"
        safe_s = f"{res['safety']:.1f}"
        td_s = f"{res['td_ratio']:.2f}"
        shp_s = f"{res['sharpe']:.2f}"
        kelly_s = f"{res['kelly']*100:.1f}%"

        ann_cell = color_cell(f"{ann_s:<6}", pass_ann)
        ivr_cell = color_cell(f"{ivr_s:<4}", pass_ivr)
        iv_cell = color_cell(f"{iv_str:<6}", pass_iv_hv)
        delta_cell = color_cell(f"{delta_str:>7}", pass_delta)
        td_cell = color_cell(f"{td_s:<5}", pass_td)

        row_str = (
            f"{expiry_s:<10} | {dte_s:<4} | {strike_s:<8} | {price_field} | {ann_cell} | "
            f"{ideal_s:<8} | {ratio_s:<6} | {ivr_cell} | {safe_s:<5} | {td_cell} | "
            f"{iv_cell} | {hv_str:<6} | {delta_cell} | {shp_s:<4} | {theta_pct_str:<5} | {kelly_s:>7}"
        )

        if is_perfect:
            if res['earn']:
                row_str += " (EARN)"
            print(f"{COLOR_RED}{row_str}{COLOR_RESET}")
        else:
            if res['earn']:
                row_str += f" {COLOR_RED}(EARN){COLOR_RESET}"
            print(row_str)

    if not ranked_results:
        print("No options passed the return criteria.")

    if moomoo_report_lines:
        for line in moomoo_report_lines:
            print(line)


# ================== Main ==================
def parse_symbols(args) -> List[str]:
    syms = []
    if args.symbols:
        # comma-separated
        parts = [p.strip() for p in args.symbols.split(",") if p.strip()]
        syms.extend(parts)

    if args.symbol:
        syms.append(args.symbol.strip())

    if args.symbol_positional:
        syms.append(args.symbol_positional.strip())

    if args.symbols_file:
        try:
            with open(args.symbols_file, "r", encoding="utf-8") as f:
                for line in f:
                    s = line.strip()
                    if s and not s.startswith("#"):
                        syms.append(s)
        except Exception as e:
            print(f"Failed to read symbols file: {e}")

    if not syms:
        syms = ["BRK.B"]

    # normalize + dedupe (preserve order)
    out = []
    seen = set()
    for s in syms:
        s_ib = normalize_symbol_for_ib(s)
        if s_ib not in seen:
            seen.add(s_ib)
            out.append(s_ib)
    return out


def main():
    util.patchAsyncio()

    parser = argparse.ArgumentParser(description="Sell Put Recommendations (v3 multi-symbol)")
    parser.add_argument("symbol_positional", nargs="?", help="Stock Symbol (e.g. AAPL)")
    parser.add_argument("--symbol", help="Stock Symbol (e.g. AAPL) (single)")
    parser.add_argument("--symbols", help="Comma-separated symbols, e.g. AAPL,MSFT,BRK.B")
    parser.add_argument("--symbols-file", help="Text file with symbols (one per line)")
    parser.add_argument("--out", default="candidate.txt", help="Output candidate file path (default: candidate.txt)")
    parser.add_argument("--top", type=int, default=NUM_OPTIONS_TO_SHOW, help=f"Top N candidates per symbol (default: {NUM_OPTIONS_TO_SHOW})")
    args = parser.parse_args()

    symbols_ib = parse_symbols(args)
    output_file = args.out
    top_n = max(1, int(args.top))

    ib = connect_ib()
    try:
        for sym_ib in symbols_ib:
            print("\n" + "=" * 90)
            print(f"Running for symbol: {normalize_symbol_for_display(sym_ib)} (IB: {sym_ib})")
            print("=" * 90)
            run_for_symbol(ib, sym_ib, output_file, top_n)
    finally:
        if ib.isConnected():
            ib.disconnect()
            print("\nDisconnected.")


if __name__ == "__main__":
    main()
