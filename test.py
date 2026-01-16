#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from ib_insync import IB, Stock, Option
from typing import Optional, List
import math

# ====== 你可改這裡 ======
HOST = "127.0.0.1"
PORT = 7497          # paper 通常 7497；你之前 lsof 也顯示 7497 LISTEN
CLIENT_ID = 11

SYMBOL = "BRK B"
PRIMARY_EXCHANGE = "NYSE"
CURRENCY = "USD"

RIGHT = "P"          # 'P' put / 'C' call
TARGET_EXPIRY = "20260116"
TARGET_STRIKE = 490.0

MAX_STRIKE_TRIES = 25
MAX_EXPIRY_TRIES = 10
SNAPSHOT_WAIT_SEC = 2.0
# =======================


def connect_ib() -> IB:
    ib = IB()
    print(f"Connecting to TWS/IBG {HOST}:{PORT} clientId={CLIENT_ID} ...")
    ib.connect(HOST, PORT, clientId=CLIENT_ID, timeout=10)
    sv = ib.client.serverVersion() if hasattr(ib.client, "serverVersion") else None
    ct = getattr(ib.client, "connTime", None)  # 有些版本沒有 connTime
    print(f"Connected: {ib.isConnected()}  serverVersion={sv}  connTime={ct}")
    return ib


def qualify_stock(ib: IB) -> Stock:
    stk = Stock(SYMBOL, "SMART", currency=CURRENCY, primaryExchange=PRIMARY_EXCHANGE)
    ib.qualifyContracts(stk)
    print("Qualified stock:", stk)
    return stk


def rank_chains(chains):
    ranked = sorted(chains, key=lambda c: (len(c.expirations), len(c.strikes)), reverse=True)
    print(f"\nOption chains: {len(chains)} (show top 10)")
    for i, c in enumerate(ranked[:10]):
        exp_sample = sorted(list(c.expirations))[:3]
        strike_sample = sorted([float(s) for s in c.strikes])[:5]
        print(
            f"[{i:02d}] exch={c.exchange:10s} tradingClass={c.tradingClass:6s} "
            f"mult={str(c.multiplier):>4s} exp={len(c.expirations):3d} strikes={len(c.strikes):3d} "
            f"exp_sample={exp_sample} strike_sample={strike_sample}"
        )
    return ranked


def pick_chain(chains, preferred_trading_class: str = "BRKB"):
    for c in chains:
        if getattr(c, "tradingClass", None) == preferred_trading_class:
            return c
    return sorted(chains, key=lambda c: (len(c.expirations), len(c.strikes)), reverse=True)[0]


def order_expiries(expiries: List[str], target: str) -> List[str]:
    def dist(e: str) -> int:
        try:
            return abs(int(e) - int(target))
        except Exception:
            return 10**18
    return sorted(expiries, key=dist)


def nearest_strikes(strikes: List[float], target: float, k: int) -> List[float]:
    return sorted(strikes, key=lambda s: abs(float(s) - float(target)))[:k]


def try_qualify_option(
    ib: IB,
    stk: Stock,
    expiry: str,
    strike: float,
    right: str,
    trading_class: str,
    multiplier: str,
    exchange_candidates: List[str],
) -> Optional[Option]:
    for exch in exchange_candidates:
        opt = Option(
            symbol=stk.symbol,
            lastTradeDateOrContractMonth=expiry,
            strike=float(strike),
            right=right,
            exchange=exch,
            currency=CURRENCY,
            tradingClass=trading_class,
            multiplier=multiplier,
        )
        try:
            ib.qualifyContracts(opt)
            if getattr(opt, "conId", 0):
                return opt
        except Exception:
            pass
    return None


def find_option(ib: IB, stk: Stock) -> Option:
    chains = ib.reqSecDefOptParams(stk.symbol, "", stk.secType, stk.conId)
    rank_chains(chains)

    chain = pick_chain(chains, preferred_trading_class="BRKB")
    print(f"\nChosen chain: exch={chain.exchange}, tradingClass={chain.tradingClass}, mult={chain.multiplier}")

    expiries_all = sorted(list(chain.expirations))
    strikes_all = sorted([float(s) for s in chain.strikes])

    expiries = order_expiries(expiries_all, TARGET_EXPIRY)[:MAX_EXPIRY_TRIES]
    strikes = nearest_strikes(strikes_all, TARGET_STRIKE, MAX_STRIKE_TRIES)

    exch_candidates = ["SMART"]
    if chain.exchange not in exch_candidates:
        exch_candidates.append(chain.exchange)

    print(f"\nWill try expiries (top {len(expiries)}): {expiries}")
    print(f"Will try strikes  (top {len(strikes)} nearest {TARGET_STRIKE}): {strikes[:10]} ...")
    print(f"Exchange candidates: {exch_candidates}\n")

    for exp in expiries:
        for strike in strikes:
            opt = try_qualify_option(
                ib=ib,
                stk=stk,
                expiry=exp,
                strike=strike,
                right=RIGHT,
                trading_class=chain.tradingClass,
                multiplier=str(chain.multiplier),
                exchange_candidates=exch_candidates,
            )
            if opt:
                print(f"✅ Found valid option: expiry={exp} strike={strike} right={RIGHT} exch={opt.exchange}")
                print("Qualified option:", opt)
                print("localSymbol:", opt.localSymbol)
                return opt

    raise RuntimeError("No valid option found within tested expiries/strikes.")


def snapshot_one(ib: IB, contract, label: str, md_type: int):
    """
    md_type:
      1 = Live
      2 = Frozen
      3 = Delayed
      4 = Delayed Frozen
    """
    ib.reqMarketDataType(md_type)
    t = ib.reqMktData(contract, "", snapshot=True)
    ib.sleep(SNAPSHOT_WAIT_SEC)

    def fmt(x):
        if x is None:
            return "None"
        if isinstance(x, float) and (math.isnan(x) or math.isinf(x)):
            return "nan"
        return str(x)

    md_name = {1: "LIVE(1)", 2: "FROZEN(2)", 3: "DELAYED(3)", 4: "DELAYED_FROZEN(4)"}.get(md_type, str(md_type))
    print(f"[{label}] {md_name} -> bid={fmt(t.bid)} ask={fmt(t.ask)} last={fmt(t.last)} close={fmt(t.close)} greeks={t.modelGreeks}")


def main():
    ib = connect_ib()
    try:
        stk = qualify_stock(ib)

        print("\n--- Snapshot Market Data (stock) ---")
        for md_type in (1, 3, 4):
            snapshot_one(ib, stk, "STK", md_type)

        opt = find_option(ib, stk)

        print("\n--- Snapshot Market Data (option) ---")
        for md_type in (1, 3, 4):
            snapshot_one(ib, opt, "OPT", md_type)

        print("\n=== 提醒 ===")
        print("如果看到 354/10168：通常是「未訂閱 + delayed 沒啟用」。")
        print("如果看到 10089/10091：通常是「需要額外 market data subscription」。")
        print("你可在 TWS 的 Connections 視窗確認 Market Data Connections 都是 connected。")

    finally:
        if ib.isConnected():
            ib.disconnect()
            print("Disconnected.")


if __name__ == "__main__":
    main()
