# #!/usr/bin/env python3
# # -*- coding: utf-8 -*-

# import argparse
# import time
# from dataclasses import dataclass
# from typing import Optional, Tuple

# from moomoo import OpenQuoteContext, RET_OK


# # ----------------------------
# # Helpers / Classification
# # ----------------------------
# @dataclass
# class CheckResult:
#     name: str
#     ok: bool
#     status: str
#     detail: str
#     suggestion: str = ""


# def _as_str(err) -> str:
#     return str(err) if err is not None else ""


# def classify_moomoo_error(err: str) -> Tuple[str, str]:
#     """
#     Return (status, suggestion)
#     """
#     e = (err or "").strip()

#     # Common patterns
#     if "No right to get the quote" in e or "No Authority" in e or "NO_RIGHT" in e:
#         return ("NO_RIGHT", "缺該市場行情權限/Quote Right（或 OpenAPI Market Data 未購買/未生效）。")

#     if "please subscribe to the Basic data first" in e:
#         return ("NEED_SUBSCRIBE_BASIC", "需要先 subscribe（QUOTE/Basic）再呼叫 quote/kline。")

#     if "please subscribe to the KL_1Min data first" in e or "Candlestick" in e and "subscribe" in e:
#         return ("NEED_SUBSCRIBE_KLINE", "需要先 subscribe 對應 K 線類型（例如 KL_1Min）再呼叫 get_cur_kline。")

#     if "quota exceeded" in e.lower():
#         return ("QUOTA_EXCEEDED", "OpenAPI quota 用完/超額，請檢查配額或降頻。")

#     if "param is wrong" in e or "type of" in e and "param" in e:
#         return ("BAD_PARAM", "參數格式不符合 moomoo 要求（例如 option code / index_option_type）。")

#     return ("ERROR", "未知錯誤：請檢查 OpenD 是否登入同帳號、版本、Market Data 訂閱是否生效。")


# def ok_result(name: str, detail: str = "") -> CheckResult:
#     return CheckResult(name=name, ok=True, status="OK", detail=detail)


# def fail_result(name: str, err: str) -> CheckResult:
#     status, sug = classify_moomoo_error(err)
#     return CheckResult(name=name, ok=False, status=status, detail=err, suggestion=sug)


# # ----------------------------
# # API wrappers
# # ----------------------------
# def call_ret(tag: str, ret, data, ok_detail: str = "") -> CheckResult:
#     if ret == RET_OK:
#         return ok_result(tag, ok_detail)
#     return fail_result(tag, _as_str(data))


# def must_subscribe(qc: OpenQuoteContext, code: str, sub_types, name: str) -> CheckResult:
#     ret, data = qc.subscribe([code], sub_types)
#     if ret == RET_OK:
#         return ok_result(name, f"subscribed {code} {sub_types}")
#     return fail_result(name, _as_str(data))


# def try_unsubscribe(qc: OpenQuoteContext, code: str, sub_types, name: str) -> CheckResult:
#     ret, data = qc.unsubscribe([code], sub_types)
#     if ret == RET_OK:
#         return ok_result(name, f"unsubscribed {code} {sub_types}")
#     return fail_result(name, _as_str(data))


# # ----------------------------
# # Checks
# # ----------------------------
# def check_stock_quote(qc: OpenQuoteContext, code: str, label: str) -> CheckResult:
#     ret, data = qc.get_stock_quote([code])
#     if ret == RET_OK:
#         # 只顯示關鍵欄位避免輸出爆炸
#         try:
#             brief = data[["code", "last_price", "data_date", "data_time"]].to_string(index=False)
#         except Exception:
#             brief = str(data)
#         return ok_result(label, brief)
#     return fail_result(label, _as_str(data))


# def check_kline_1m(qc: OpenQuoteContext, code: str, label: str, count: int = 5) -> CheckResult:
#     ret, data = qc.get_cur_kline(code, count, "K_1M")
#     if ret == RET_OK:
#         try:
#             brief = data[["code", "time_key", "open", "close", "high", "low", "volume"]].tail(3).to_string(index=False)
#         except Exception:
#             brief = str(data)
#         return ok_result(label, brief)
#     return fail_result(label, _as_str(data))


# # ----------------------------
# # Orchestrator
# # ----------------------------
# def main():
#     ap = argparse.ArgumentParser(description="Moomoo OpenAPI Market Data Health Check")
#     ap.add_argument("--host", default="127.0.0.1")
#     ap.add_argument("--port", type=int, default=11111)

#     ap.add_argument("--hk-code", default="HK.00700")
#     ap.add_argument("--us-code", default="US.AAPL")
#     ap.add_argument("--cn-code", default="SH.600519")

#     # options: you can pass one option code if you have it, e.g. US.AAPL260116P00250000
#     ap.add_argument("--us-opt-code", default="")

#     ap.add_argument("--unsubscribe", action="store_true",
#                     help="Attempt to unsubscribe at end (will wait >= 65s to satisfy moomoo rule).")
#     ap.add_argument("--unsubscribe-wait", type=int, default=65,
#                     help="Seconds to wait before unsubscribe (>=60 recommended).")

#     args = ap.parse_args()

#     qc = OpenQuoteContext(host=args.host, port=args.port)

#     results = []

#     try:
#         print("\n================ Market Data Health Check (moomoo) ================")

#         # --- Basicinfo (HK) ---
#         ret, data = qc.get_stock_basicinfo(market="HK", stock_type="STOCK")
#         if ret == RET_OK:
#             # show a tiny sample
#             sample = data[["code", "name"]].head(3).to_string(index=False)
#             results.append(ok_result("HK basicinfo", sample))
#         else:
#             results.append(fail_result("HK basicinfo", _as_str(data)))

#         # --- HK Quote (requires subscribe QUOTE) ---
#         r = check_stock_quote(qc, args.hk_code, f"HK quote {args.hk_code} (before subscribe)")
#         results.append(r)

#         if not r.ok and r.status in ("NEED_SUBSCRIBE_BASIC", "ERROR"):
#             # auto subscribe QUOTE and retry
#             results.append(must_subscribe(qc, args.hk_code, ["QUOTE"], f"HK subscribe QUOTE {args.hk_code}"))
#             results.append(check_stock_quote(qc, args.hk_code, f"HK quote {args.hk_code} (after subscribe)"))

#         # --- HK Kline 1m (requires subscribe KL_1Min) ---
#         rk = check_kline_1m(qc, args.hk_code, f"HK kline1m {args.hk_code} (before subscribe)")
#         results.append(rk)

#         if not rk.ok and rk.status in ("NEED_SUBSCRIBE_KLINE", "ERROR"):
#             results.append(must_subscribe(qc, args.hk_code, ["K_1M"], f"HK subscribe K_1M {args.hk_code}"))
#             results.append(check_kline_1m(qc, args.hk_code, f"HK kline1m {args.hk_code} (after subscribe)"))

#         # --- US Stock Quote ---
#         results.append(check_stock_quote(qc, args.us_code, f"US quote {args.us_code}"))

#         # --- CN(A-Share) Stock Quote ---
#         results.append(check_stock_quote(qc, args.cn_code, f"CN quote {args.cn_code}"))

#         # --- US Option Quote (optional) ---
#         if args.us_opt_code:
#             # Options in moomoo also usually require subscribe
#             r1 = check_stock_quote(qc, args.us_opt_code, f"US option quote {args.us_opt_code} (before subscribe)")
#             results.append(r1)
#             if not r1.ok and r1.status in ("NEED_SUBSCRIBE_BASIC", "ERROR"):
#                 results.append(must_subscribe(qc, args.us_opt_code, ["QUOTE"], f"US option subscribe QUOTE {args.us_opt_code}"))
#                 results.append(check_stock_quote(qc, args.us_opt_code, f"US option quote {args.us_opt_code} (after subscribe)"))
#         else:
#             results.append(CheckResult(
#                 name="US option quote",
#                 ok=False,
#                 status="SKIPPED",
#                 detail="--us-opt-code not provided",
#                 suggestion="在 moomoo app 找任一個 US option，複製 code（例如 US.AAPL260116P00250000）後用 --us-opt-code 測。"
#             ))

#         # -------- Summary --------
#         print("-------------------------------------------------------------------")
#         any_fail = False
#         for r in results:
#             mark = "✅" if r.ok else "❌"
#             print(f"{mark} {r.name}: {r.status}")
#             if r.detail:
#                 print(f"   {r.detail}")
#             if (not r.ok) and r.suggestion:
#                 print(f"   ↳ 建議: {r.suggestion}")
#             if not r.ok and r.status not in ("SKIPPED",):
#                 any_fail = True

#         print("-------------------------------------------------------------------")
#         if any_fail:
#             print("❌ Health Check: 有失敗項目（多半是缺權限或需先 subscribe）。")
#         else:
#             print("✅ Health Check: 全部通過。")

#         # -------- Optional unsubscribe --------
#         if args.unsubscribe:
#             print(f"\n[Unsubscribe] Waiting {args.unsubscribe_wait}s to satisfy moomoo minimum 1 minute rule...")
#             time.sleep(max(0, args.unsubscribe_wait))

#             # best-effort unsubscribe for HK quote/kline
#             results_unsub = []
#             results_unsub.append(try_unsubscribe(qc, args.hk_code, ["QUOTE"], f"HK unsubscribe QUOTE {args.hk_code}"))
#             results_unsub.append(try_unsubscribe(qc, args.hk_code, ["K_1M"], f"HK unsubscribe K_1M {args.hk_code}"))

#             if args.us_opt_code:
#                 results_unsub.append(try_unsubscribe(qc, args.us_opt_code, ["QUOTE"], f"US option unsubscribe QUOTE {args.us_opt_code}"))

#             print("\n[Unsubscribe results]")
#             for r in results_unsub:
#                 mark = "✅" if r.ok else "❌"
#                 print(f"{mark} {r.name}: {r.status}")
#                 if r.detail:
#                     print(f"   {r.detail}")
#                 if (not r.ok) and r.suggestion:
#                     print(f"   ↳ 建議: {r.suggestion}")

#     finally:
#         qc.close()


# if __name__ == "__main__":
#     main()


#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import argparse
import time
from moomoo import (
    OpenQuoteContext,
    RET_OK,
    SubType,
)

HOST = "127.0.0.1"
PORT = 11111

# 用來判斷你是否已具備「US 市場 quote right」(股票通常最先反映權限)
PROBE_US_STOCK = "US.AAPL"


def _ok(ret, data) -> bool:
    return ret == RET_OK


def fmt_fail(prefix: str, data) -> str:
    return f"{prefix}: FAIL -> {data}"


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--host", default=HOST)
    ap.add_argument("--port", type=int, default=PORT)
    ap.add_argument(
        "--us-opt-code",
        required=True,
        help="例如：US.AAPL260116P00250000",
    )
    ap.add_argument(
        "--use-delayed",
        action="store_true",
        help="moomoo 是否能用延遲，取決於帳號權限；此參數僅作為你自己的標記/輸出提示",
    )
    args = ap.parse_args()

    print(f"Connecting to Moomoo OpenD at {args.host}:{args.port} ...")
    quote_ctx = OpenQuoteContext(host=args.host, port=args.port)

    try:
        print("\n================ US Options Health Check (moomoo) ================")

        # 1) 先測 US stock quote right（若 NO_RIGHT，options 也不用測了）
        ret, data = quote_ctx.get_stock_quote([PROBE_US_STOCK])
        if not _ok(ret, data):
            msg = str(data)
            print(f"❌ US Stock QuoteRight: NO_RIGHT/FAIL -> {msg}")
            print("   ↳ 你目前沒有 US 市場行情權限（OpenD 視窗通常會顯示 US Securities: No Authority）")
            print("   ↳ 請先在 moomoo 開通/購買 US 行情（常見是 US National LV1），重登 + 重啟 OpenD 後再測。")
            print("===============================================================")
            return

        # 有權限的話，印一行確認
        row = data.iloc[0]
        print(f"✅ US Stock QuoteRight: OK ({row['code']}) last={row.get('last_price')} time={row.get('update_time')}")

        # 2) 訂閱 option QUOTE（很多介面要求先 subscribe）
        opt_code = args.us_opt_code.strip()
        print(f"\n--- Subscribe OPTION QUOTE: {opt_code} ---")
        ret, data = quote_ctx.subscribe([opt_code], [SubType.QUOTE], subscribe_push=False)
        if not _ok(ret, data):
            print(f"❌ subscribe({opt_code}, QUOTE): FAIL -> {data}")
            print("   ↳ 若這裡就失敗：通常是 options 權限未開 / 合約 code 不存在 / OpenD 未同步權限")
            print("===============================================================")
            return
        print(f"✅ subscribe({opt_code}, QUOTE): OK")

        # 3) 拉一次 option quote（realtime）
        print(f"\n--- Get Option Quote (realtime): {opt_code} ---")
        ret, data = quote_ctx.get_stock_quote([opt_code])
        if _ok(ret, data):
            r = data.iloc[0]
            print(f"✅ option quote OK: code={r['code']} last={r.get('last_price')} time={r.get('update_time')}")
        else:
            print(f"❌ option quote FAIL -> {data}")
            print("   ↳ 若顯示 NO_RIGHT：options 行情權限仍未開通。")
            print("   ↳ 若顯示需 basic data：代表必須先 subscribe（我們已做）但仍可能缺權限。")

        # 4) 等 65 秒再 unsubscribe（moomoo 限制：訂閱至少要 1 分鐘才可取消）
        print("\n--- Waiting 65s then unsubscribe (moomoo requires >= 1 min) ---")
        time.sleep(65)

        ret, data = quote_ctx.unsubscribe([opt_code], [SubType.QUOTE], unsubscribe_all=False)
        if _ok(ret, data):
            print(f"✅ unsubscribe({opt_code}, QUOTE): OK")
        else:
            print(f"⚠️ unsubscribe({opt_code}, QUOTE): FAIL -> {data} (可忽略，不影響檢查結果)")

        print("\n================ Result ================")
        print("✅ 若上面 US Stock OK + option quote OK：代表你已具備 US option 行情能力。")
        print("❌ 若 US Stock 仍 NO_RIGHT：先處理 US 行情權限，options 才會通。")
        if args.use_delayed:
            print("（你開了 --use-delayed，但是否能拿延遲行情仍取決於帳號權限/方案）")
        print("========================================")

    finally:
        quote_ctx.close()


if __name__ == "__main__":
    main()
