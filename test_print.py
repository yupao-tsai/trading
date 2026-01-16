# -*- coding: utf-8 -*-
"""
arbitrage_v7.py (throttled + cache + dashboard)

✅ 目的：
- 解決夜盤期貨 BidAsk 洗版（例如 NYFA6 一直刷）
- Quote callback 不 print，每筆行情只更新 cache
- 主迴圈每秒顯示各 pair 的最新價差 (stock_mid vs future_mid) / 基差
- 盤別 gating：現貨不在盤中就不訂閱股票（避免你誤以為股票沒行情）

⚠️ 注意：
- 這是一份「可直接跑」的單檔範本，你需要 .env (或環境變數) 放登入資訊
- 合約 lookup 在不同 Shioaji 版本可能略有差異：若你原本已有 get_contract()，用你自己的即可
"""

import os
import sys
import time
import math
import threading
import datetime as dt
from dataclasses import dataclass
from decimal import Decimal
from typing import Dict, Optional, Tuple, List

import shioaji as sj
from dotenv import load_dotenv

try:
    load_dotenv()
except Exception:
    pass
# ----------------------------
# Config
# ----------------------------
SIMULATION = True

# 每個 code 最多 N 秒印一次（若你想完全不印，設 None）
PRINT_THROTTLE_SEC = 0.3

# Dashboard 刷新頻率（秒）
DASHBOARD_INTERVAL_SEC = 1.0

# 只在 best bid/ask 改變時才更新 "last_change_ts"（減少噪音）
ONLY_ON_BBO_CHANGE = True

# 你目前示例的 pairs（stock_code, future_code）
DEFAULT_PAIRS = [
    ("2344", "FZFA6"),
    ("3481", "DQFA6"),
    ("6770", "QZFA6"),
    ("2408", "CYFA6"),
    ("2337", "DIFA6"),
    ("1303", "CAFA6"),
    ("8358", "PQFA6"),
    ("1605", "CSFA6"),
    ("3189", "IXFA6"),
    ("2317", "DHFA6"),
    ("3706", "PSFA6"),
    ("2449", "GRFA6"),
    ("2327", "LXFA6"),
    ("2313", "FTFA6"),
    ("3231", "DXFA6"),
    ("3037", "IRFA6"),
    ("3006", "IIFA6"),
    ("2409", "CHFA6"),
    ("0050", "NYFA6"),
    ("3711", "OZFA6"),
    ("3264", "NEFA6"),
    ("6443", "RLFA6"),
    ("1802", "KUFA6"),
    ("6239", "KCFA6"),
    ("6282", "KFFA6"),
    ("3105", "NAFA6"),
    ("2382", "DKFA6"),
    ("3260", "NDFA6"),
    ("8150", "QUFA6"),
    ("8112", "SKFA6"),
]


# ----------------------------
# Market session helpers (TW time)
# ----------------------------
def tw_now() -> dt.datetime:
    # 你的 log 看起來是台灣時間；這裡直接用 local time（你在台灣就 OK）
    return dt.datetime.now()


def is_weekday(d: dt.datetime) -> bool:
    return d.weekday() < 5


def in_range(t: dt.time, start: dt.time, end: dt.time) -> bool:
    """Return True if t is in [start, end] when start <= end; if start > end, treat as overnight range."""
    if start <= end:
        return start <= t <= end
    # overnight, e.g. 15:00 - 05:00
    return t >= start or t <= end


def stock_session_open(now: dt.datetime) -> bool:
    if not is_weekday(now):
        return False
    # 台股現貨：09:00 - 13:30 (粗略)
    return in_range(now.time(), dt.time(9, 0), dt.time(13, 30))


def future_session_open(now: dt.datetime) -> bool:
    if not is_weekday(now):
        return False
    # 台灣期貨：日盤 08:45 - 13:45；夜盤 15:00 - 05:00 (粗略)
    t = now.time()
    return in_range(t, dt.time(8, 45), dt.time(13, 45)) or in_range(t, dt.time(15, 0), dt.time(5, 0))


# ----------------------------
# Quote cache
# ----------------------------
@dataclass
class BBO:
    bid: Optional[Decimal] = None
    ask: Optional[Decimal] = None
    bid_vol: Optional[int] = None
    ask_vol: Optional[int] = None
    ts: Optional[dt.datetime] = None
    last_change_ts: Optional[dt.datetime] = None  # only updates when bid/ask changes


class QuoteCache:
    def __init__(self):
        self._lock = threading.Lock()
        self._bbo: Dict[str, BBO] = {}

    def update_bbo(
        self,
        code: str,
        ts: dt.datetime,
        bid: Optional[Decimal],
        ask: Optional[Decimal],
        bid_vol: Optional[int] = None,
        ask_vol: Optional[int] = None,
    ):
        with self._lock:
            cur = self._bbo.get(code, BBO())
            changed = (cur.bid != bid) or (cur.ask != ask)
            cur.bid = bid
            cur.ask = ask
            cur.bid_vol = bid_vol
            cur.ask_vol = ask_vol
            cur.ts = ts
            if cur.last_change_ts is None:
                cur.last_change_ts = ts
            elif (not ONLY_ON_BBO_CHANGE) or changed:
                cur.last_change_ts = ts
            self._bbo[code] = cur

    def get_bbo(self, code: str) -> Optional[BBO]:
        with self._lock:
            return self._bbo.get(code)

    @staticmethod
    def mid(bbo: Optional[BBO]) -> Optional[Decimal]:
        if not bbo or bbo.bid is None or bbo.ask is None:
            return None
        return (bbo.bid + bbo.ask) / Decimal("2")


# ----------------------------
# Throttled printer (optional)
# ----------------------------
class ThrottledPrinter:
    def __init__(self, throttle_sec: Optional[float]):
        self.throttle_sec = throttle_sec
        self._lock = threading.Lock()
        self._last_print: Dict[str, float] = {}
        self._last_bbo: Dict[str, Tuple[Optional[Decimal], Optional[Decimal]]] = {}

    def maybe_print(self, code: str, bid: Optional[Decimal], ask: Optional[Decimal], line: str):
        if self.throttle_sec is None:
            return

        now = time.time()
        with self._lock:
            last = self._last_print.get(code, 0.0)
            if now - last < self.throttle_sec:
                return

            prev = self._last_bbo.get(code, (None, None))
            if ONLY_ON_BBO_CHANGE and (prev == (bid, ask)):
                return

            self._last_print[code] = now
            self._last_bbo[code] = (bid, ask)

        print(line)


# ----------------------------
# Contract lookup helpers
# ----------------------------
def get_stock_contract(api: sj.Shioaji, code: str):
    # 嘗試 TSE / OTC
    # 你的 log 有：TSE/2344、OTC/8358
    # 這裡用 try/except 兼容
    if code in api.Contracts.Stocks.TSE:
        return api.Contracts.Stocks.TSE[code]
    if code in api.Contracts.Stocks.OTC:
        return api.Contracts.Stocks.OTC[code]
    # 可能還有 OES / ESB ... 先掃一次
    for mkt in [api.Contracts.Stocks.TSE, api.Contracts.Stocks.OTC]:
        if code in mkt:
            return mkt[code]
    raise KeyError(f"Stock contract not found: {code}")


def get_future_contract(api: sj.Shioaji, code: str):
    # 你的 log 出現：QUO/v1/FOP/*/TFE/NYFA6 之類
    # 多數情況 api.Contracts.Futures[code] 可用；不行就嘗試 api.Contracts.Futures.TFE
    try:
        return api.Contracts.Futures[code]
    except Exception:
        pass

    # 有些版本是 api.Contracts.Futures.TFE[code]
    try:
        if hasattr(api.Contracts.Futures, "TFE") and code in api.Contracts.Futures.TFE:
            return api.Contracts.Futures.TFE[code]
    except Exception:
        pass

    # 最後退而求其次：掃描 Futures 物件（若可迭代）
    try:
        for c in api.Contracts.Futures:
            if getattr(c, "code", None) == code:
                return c
    except Exception:
        pass

    raise KeyError(f"Future contract not found: {code}")


# ----------------------------
# Keyboard (p/h/q)
# ----------------------------
class KeyboardMonitor(threading.Thread):
    def __init__(self):
        super().__init__(daemon=True)
        self._quit = threading.Event()
        self._last_cmd = None

    def run(self):
        print("\n[Keyboard] p=positions, o=orders, h=help, q=quit\n")
        while not self._quit.is_set():
            try:
                cmd = sys.stdin.readline().strip().lower()
            except Exception:
                continue
            if not cmd:
                continue
            self._last_cmd = cmd
            if cmd == "q":
                self._quit.set()

    def quit_requested(self) -> bool:
        return self._quit.is_set()

    def pop_cmd(self) -> Optional[str]:
        c = self._last_cmd
        self._last_cmd = None
        return c


# ----------------------------
# Main
# ----------------------------
def main():
    # --- credentials ---
    # 你可用 .env export 好這些變數
    person_id = os.environ.get("Sinopack_PERSON_ID") or os.environ.get("SINO_PERSON_ID")
    passwd = os.environ.get("Sinopack_PASSWORD") or os.environ.get("SINO_PASSWORD")
    ca_path = os.environ.get("Sinopack_CA_PATH")
    ca_passwd = os.environ.get("Sinopack_CA_PASSWORD")

    if not person_id or not passwd:
        print("❌ Missing env: Sinopack_PERSON_ID / Sinopack_PASSWORD (or SINO_PERSON_ID / SINO_PASSWORD)")
        sys.exit(1)

    now = tw_now()
    st_open = stock_session_open(now)
    fu_open = future_session_open(now)

    if not st_open and not fu_open:
        print(f"⚠️ 目前時間 {now:%Y-%m-%d %H:%M:%S} 非交易時段（現貨/期貨都關），直接結束。")
        return

    print(f"[System] init Shioaji (simulation={SIMULATION})")
    api = sj.Shioaji(simulation=SIMULATION)

    print("[System] login...")
    api.login(person_id=person_id, passwd=passwd)

    print("[System] fetch_contracts...")
    api.fetch_contracts()

    # 憑證（正式下單才需要；SIMULATION 可不裝）
    if (not SIMULATION) and ca_path and ca_passwd:
        api.activate_ca(ca_path=ca_path, ca_passwd=ca_passwd, person_id=person_id)

    quote_cache = QuoteCache()
    printer = ThrottledPrinter(PRINT_THROTTLE_SEC)

    # --- callbacks ---
    def on_bidask(exchange: sj.constant.Exchange, bidask):
        """
        bidask 可能是：
        - Exchange.TAIFEX BidAsk(...)
        - Exchange.TSE/OTC BidAsk(...)
        Shioaji 的 bidask 結構通常包含 bid_price/ask_price list (level1在[0])。
        """
        try:
            code = getattr(bidask, "code", None) or getattr(bidask, "symbol", None)
            if not code:
                return
            ts = getattr(bidask, "datetime", None) or tw_now()

            bid = None
            ask = None
            bid_vol = None
            ask_vol = None

            # 常見欄位：bid_price[0], ask_price[0], bid_volume[0], ask_volume[0]
            if hasattr(bidask, "bid_price") and bidask.bid_price:
                bid = bidask.bid_price[0]
            if hasattr(bidask, "ask_price") and bidask.ask_price:
                ask = bidask.ask_price[0]
            if hasattr(bidask, "bid_volume") and bidask.bid_volume:
                bid_vol = int(bidask.bid_volume[0])
            if hasattr(bidask, "ask_volume") and bidask.ask_volume:
                ask_vol = int(bidask.ask_volume[0])

            quote_cache.update_bbo(code=code, ts=ts, bid=bid, ask=ask, bid_vol=bid_vol, ask_vol=ask_vol)

            # ✅ 不再每筆 print；只做節流/變動時才印（可關掉）
            printer.maybe_print(
                code=code,
                bid=bid,
                ask=ask,
                line=f"{exchange} BBO {code} bid={bid}({bid_vol}) ask={ask}({ask_vol}) ts={ts:%H:%M:%S.%f}",
            )
        except Exception:
            # callback 內不要炸掉整個系統
            return

    print("[System] quote callback installed (BidAsk)")
    api.quote.set_callback(sj.constant.QuoteType.BidAsk, on_bidask)

    # --- subscribe pairs ---
    pairs = DEFAULT_PAIRS[:]  # 你可以換成你的 PairDiscoverer 結果
    subscribed = []

    # 盤別 gating：非現貨盤就不訂閱股票，避免你晚上以為股票沒行情
    do_stock = st_open
    do_future = fu_open

    print(f"[System] session: stock_open={do_stock}, future_open={do_future} now={now:%Y-%m-%d %H:%M:%S}")

    for s_code, f_code in pairs:
        if do_stock:
            try:
                s_contract = get_stock_contract(api, s_code)
                api.quote.subscribe(
                    s_contract,
                    quote_type=sj.constant.QuoteType.BidAsk,
                    version=sj.constant.QuoteVersion.v1,
                )
                subscribed.append(("STK", s_contract))
            except Exception as e:
                print(f"[WARN] subscribe stock {s_code} failed: {e}")

        if do_future:
            try:
                f_contract = get_future_contract(api, f_code)
                api.quote.subscribe(
                    f_contract,
                    quote_type=sj.constant.QuoteType.BidAsk,
                    version=sj.constant.QuoteVersion.v1,
                )
                subscribed.append(("FUT", f_contract))
            except Exception as e:
                print(f"[WARN] subscribe future {f_code} failed: {e}")

    print(f"[System] subscribed={len(subscribed)} (pairs={len(pairs)})")
    print("[System] running. type 'h' then Enter for help (stdin).")

    kb = KeyboardMonitor()
    kb.start()

    def print_help():
        print("\n[Help]")
        print("  p : show pairs dashboard now")
        print("  h : help")
        print("  q : quit\n")

    def print_pairs_dashboard():
        # 顯示你最在意的：每個 pair 的 stock_mid vs future_mid
        lines = []
        for s_code, f_code in pairs:
            sb = quote_cache.get_bbo(s_code)
            fb = quote_cache.get_bbo(f_code)

            sm = QuoteCache.mid(sb)
            fm = QuoteCache.mid(fb)

            if sm is None and fm is None:
                continue

            # 基差 / 價差（只是示例，你可換成你策略的 spread 定義）
            spread = None
            if sm is not None and fm is not None:
                spread = fm - sm

            s_ts = sb.ts.strftime("%H:%M:%S") if (sb and sb.ts) else "--:--:--"
            f_ts = fb.ts.strftime("%H:%M:%S") if (fb and fb.ts) else "--:--:--"

            lines.append(
                f"{s_code:>4} | smid={str(sm) if sm is not None else 'NA':>8} ({s_ts})  "
                f"{f_code:>6} | fmid={str(fm) if fm is not None else 'NA':>8} ({f_ts})  "
                f"spread(f-s)={str(spread) if spread is not None else 'NA'}"
            )

        if not lines:
            print("[Pairs] no data yet.")
            return

        print("\n=== Pairs Dashboard (latest) ===")
        for ln in lines[:50]:
            print(ln)
        if len(lines) > 50:
            print(f"... {len(lines)-50} more")
        print("===============================\n")

    last_dash = 0.0

    try:
        while True:
            if kb.quit_requested():
                print("\n[Keyboard] quit requested")
                break

            cmd = kb.pop_cmd()
            if cmd == "h":
                print_help()
            elif cmd == "p":
                print_pairs_dashboard()
            # elif cmd == "o":  # 你若有 orders/positions manager，可接回來
            #     ...
            # elif cmd == "positions":
            #     ...

            # 定時 dashboard
            now_ts = time.time()
            if now_ts - last_dash >= DASHBOARD_INTERVAL_SEC:
                last_dash = now_ts
                # 你也可以把 dashboard 關掉，只保留策略 loop
                # print_pairs_dashboard()
                pass

            time.sleep(0.02)

    finally:
        print("\n[System] stopping...")
        # unsubscribe
        ok = 0
        for kind, c in subscribed:
            try:
                api.quote.unsubscribe(c, quote_type=sj.constant.QuoteType.BidAsk, version=sj.constant.QuoteVersion.v1)
                ok += 1
            except Exception:
                pass
        print(f"[System] -unsubscribe {len(subscribed)} (ok={ok})")
        try:
            api.logout()
        except Exception:
            pass
        print("[System] stopped.")


if __name__ == "__main__":
    main()
