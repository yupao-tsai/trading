# -*- coding: utf-8 -*-
import os
import sys
import time
import shioaji as sj
from dotenv import load_dotenv

load_dotenv()

SIMULATION = True
if "--real" in sys.argv:
    SIMULATION = False
if "--sim" in sys.argv:
    SIMULATION = True
DRY_RUN = ("--dry" in sys.argv)

PERSON_ID   = os.getenv("Sinopack_PERSON_ID")
CA_API_KEY  = os.getenv("Sinopack_CA_API_KEY")
CA_SECRET   = os.getenv("Sinopack_CA_SECRET_KEY")
CA_PATH     = os.getenv("Sinopack_CA_PATH")
CA_PASS     = os.getenv("Sinopack_CA_PASSWORD")

# ---------------------------
# Tick helpers
# ---------------------------
def tick_size(price: float) -> float:
    if price < 10: return 0.01
    if price < 50: return 0.05
    if price < 100: return 0.1
    if price < 500: return 0.5
    if price < 1000: return 1.0
    return 5.0

def round_to_tick(price: float) -> float:
    if price <= 0:
        return price
    t = tick_size(price)
    return round(round(price / t) * t, 2)

# ---------------------------
# Callback (must accept (stat, msg) in many versions)
# ---------------------------
def order_cb(stat, msg):
    try:
        # Safety for msg being Object or Dict
        def _safe_get(obj, key, default=None):
            if isinstance(obj, dict):
                return obj.get(key, default)
            return getattr(obj, key, default)

        # Basic attribute extraction
        op = _safe_get(msg, "operation") or {}
        od = _safe_get(msg, "order") or {}
        ct = _safe_get(msg, "contract") or {}
        
        # Nested extraction needs care if op/od/ct are objects
        op_code = _safe_get(op, "op_code")
        op_msg = _safe_get(op, "op_msg")
        oid = _safe_get(od, "id")
        
        ex = _safe_get(ct, "exchange")
        code = _safe_get(ct, "code")

        print(f"[ORDER_CB] stat={stat} id={oid} op_code={op_code} op_msg={op_msg} "
              f"contract={ex}:{code}")
    except Exception as e:
        print(f"[ORDER_CB] parse failed: {e} raw={msg}")

# ---------------------------
# Robust contract lookup
# ---------------------------
def _try_get(mapping, key):
    """mapping may be dict-like or custom; try best."""
    try:
        return mapping[key]
    except Exception:
        return None

def _iter_contracts(container):
    """
    container might be:
    - dict-like (has .values())
    - list-like (iterates contracts)
    - custom mapping that iterates keys (rare)
    """
    if container is None:
        return
    # dict-like
    if hasattr(container, "values"):
        try:
            for c in container.values():
                yield c
            return
        except Exception:
            pass
    # iterable
    try:
        for c in container:
            # if iterating keys, skip non-contract
            if hasattr(c, "code"):
                yield c
    except Exception:
        return

def get_stock_contract(api: sj.Shioaji, code: str):
    """
    兼容各種 Shioaji Contracts 結構。
    會嘗試：
    1) api.Contracts.Stocks[code] / [OTCcode] / [TSEcode]
    2) Stocks.TSE / Stocks.OTC 內以不同 key 取得
    3) 最後掃描所有股票合約物件找 code==目標
    """
    code = str(code).strip()
    keys = [code, f"TSE{code}", f"OTC{code}"]

    # 1) direct Stocks mapping
    try:
        stocks = api.Contracts.Stocks
        for k in keys:
            c = _try_get(stocks, k)
            if c is not None:
                return c
    except Exception:
        pass

    # 2) buckets
    for ex_name in ("TSE", "OTC"):
        bucket = None
        try:
            bucket = getattr(api.Contracts.Stocks, ex_name, None)
        except Exception:
            bucket = None
        if bucket is None:
            continue

        # try keys in this bucket
        for k in keys:
            c = _try_get(bucket, k)
            if c is not None and getattr(c, "code", None) == code:
                return c
            if c is not None:
                # 有些 bucket 用 code 當 key，取到就直接回
                return c

        # scan bucket objects
        for c in _iter_contracts(bucket):
            if getattr(c, "code", None) == code:
                return c

    # 3) final: scan all stocks (slow but sure)
    try:
        for c in _iter_contracts(api.Contracts.Stocks):
            if getattr(c, "code", None) == code:
                return c
    except Exception:
        pass

    # Debug: print sample keys if possible
    try:
        otc = getattr(api.Contracts.Stocks, "OTC", None)
        if otc is not None and hasattr(otc, "keys"):
            sample = list(otc.keys())[:20]
            print("[DEBUG] OTC keys sample:", sample)
    except Exception:
        pass

    raise KeyError(f"Stock contract not found for code={code}")

# ---------------------------
# Position fetch (stock must work; futopt best-effort)
# ---------------------------
def list_stock_positions(api: sj.Shioaji, timeout_ms=10000, retries=3):
    last = None
    for i in range(retries):
        try:
            return api.list_positions(api.stock_account, timeout=timeout_ms)
        except Exception as e:
            last = e
            print(f"[WARN] list_positions(stock) failed ({i+1}/{retries}): {e}")
            time.sleep(1 + i)
    raise RuntimeError(f"list_positions(stock) failed: {last}")

def list_fut_positions_best_effort(api: sj.Shioaji, timeout_ms=8000, retries=2):
    last = None
    for i in range(retries):
        try:
            return api.list_positions(api.futopt_account, timeout=timeout_ms)
        except Exception as e:
            last = e
            print(f"[WARN] list_positions(futopt) failed ({i+1}/{retries}): {e}")
            time.sleep(1 + i)
    return []

# ---------------------------
# Main
# ---------------------------
def main():
    print("=== Clear All Positions (Robust contract lookup) ===")
    print(f"SIMULATION = {SIMULATION}  (use --real for real trading)")
    if DRY_RUN:
        print("DRY_RUN = True (won't place orders)")

    api = sj.Shioaji(simulation=SIMULATION)

    print("[Login] ...")
    api.login(api_key=CA_API_KEY, secret_key=CA_SECRET)
    print("[Login] OK")

    api.set_order_callback(order_cb)

    # 你要正式才一定需要 CA；但你如果想在 sim 也啟用，可以把這段改成「只要 env 有就 activate」
    if not SIMULATION:
        print("[Activate CA] ...")
        api.activate_ca(ca_path=CA_PATH, ca_passwd=CA_PASS, person_id=PERSON_ID)
        print("[Activate CA] OK")
    else:
        print("[Activate CA] ...")
        api.activate_ca(ca_path=CA_PATH, ca_passwd=CA_PASS, person_id=PERSON_ID)
        print("[Activate CA] skipped (SIMULATION=True)")

    print("[Fetch contracts] ...")
    api.fetch_contracts(contract_download=True)
    print("[Fetch contracts] OK")

    print("[Cancel Open Orders] ...")


    max_loops = 10

    for loop in range(1, max_loops + 1):
        print(f"\n[Loop {loop}] Scan positions ...")

        stock_pos = list_stock_positions(api, timeout_ms=10000, retries=3)
        
        # --- Cancellation (Moved Inside Loop) ---
        print("  [Check Open Orders] ...")
        try:
            api.update_status()
            for trade in api.list_trades():
                if trade.status.status in ("PreSubmitted", "Submitted", "PartFilled"):
                    api.cancel_order(trade)
                    print(f"    Cancelled: {trade.contract.code} {trade.order.action}")
            time.sleep(2.0) # Wait for cancellation effect
        except Exception as e: 
            print(f"    Cancel check failed: {e}")
        # ----------------------------------------
        stock_pos = [p for p in stock_pos if int(getattr(p, "quantity", 0)) != 0]

        fut_pos = list_fut_positions_best_effort(api, timeout_ms=8000, retries=2)
        fut_pos = [p for p in fut_pos if int(getattr(p, "quantity", 0)) != 0]

        if not stock_pos and not fut_pos:
            print(">>> All positions CLEARED.")
            return

        if stock_pos:
            print("Stock positions:", [(p.code, int(p.quantity), str(p.direction)) for p in stock_pos])

        # ---- Stocks ----
        for p in stock_pos:
            code = str(p.code)
            qty  = int(p.quantity)
            direction = str(p.direction)

            # if code in sent_codes:
            #     continue

            # Direction: Buy -> Sell, Sell -> Buy
            action = sj.constant.Action.Sell if "Buy" in direction else sj.constant.Action.Buy

            # Fallback Retry Logic
            # Tier 1: Standard (LMT, Common)
            # Tier 2: MKT (Common) - if Price error
            # Tier 3: Odd Lot (MKT, Shares) - if Commodity error (sim glitch)
            
            success = False
            
            def attempt_order(qty_use, lot_type, px_type, px_val, tag="Tier1"):
                print(f"[Send] {code}: {action.value} {qty_use} ({lot_type}) @ {px_type}({px_val}) [{tag}]")
                if DRY_RUN: return True, "00", "OK"
                
                try:
                    ord_s = api.Order(
                        action=action,
                        price=px_val,
                        quantity=qty_use,
                        price_type=px_type,
                        order_type=sj.constant.OrderType.IOC, # Always IOC for clearing
                        order_cond=sj.constant.StockOrderCond.Cash,
                        order_lot=lot_type,
                        daytrade_short=False,
                        account=api.stock_account,
                    )
                    t = api.place_order(contract, ord_s, timeout=10000) # Added timeout
                except Exception as e:
                    print(f"  -> ERROR ({tag}): {e}")
                    return False, "Ex", str(e)
                
                # Check immediate rejection
                op = getattr(t, "operation", {}) or {}
                if isinstance(op, dict):
                    c = str(op.get("op_code", ""))
                    m = str(op.get("op_msg", ""))
                    if c and c != "00":
                        print(f"  -> REJECTED ({tag}): {c} {m}")
                        return False, c, m
                print(f"  -> SENT ({tag})")
                return True, "00", "OK"

            contract = get_stock_contract(api, code)
            print(f"[DEBUG] Valid Contract: {contract.exchange}:{contract.code}")

            # 1. Tier 1: Market Price (MKT + IOC) - FASTEST
            # Note: Simulation MKT support varies, but we try it first.
            print(f"[Attempt] MKT IOC for {code} {action}")
            # Try MKT first. Shioaji Position Quantity is ALREADY in Lots for Stocks.
            # Do NOT divide by 1000.
            qty_lots = qty 
            
            ok, c, m = attempt_order(
                qty_use=qty_lots, 
                lot_type=sj.constant.StockOrderLot.Common,
                px_type=sj.constant.StockPriceType.MKT,
                px_val=0, # MKT ignores price
                tag="MKT_IOC"
            )
            
            if ok:
                pass # sent_codes.add(code)
                continue

            # 2. Tier 2: Limit Price (Limit Up/Down) with IOC
            # If MKT fails (e.g. Sim restriction), use aggressive Limit
            if action == sj.constant.Action.Sell:
                lmt_px = float(getattr(contract, "limit_down", 0) or 0)
            else:
                lmt_px = float(getattr(contract, "limit_up", 0) or 0)
            
            # Safety: If no limit price found, use reference +/- 10%
            if lmt_px == 0:
                 ref = float(getattr(contract, "reference_price", 0) or 0)
                 if ref > 0:
                     lmt_px = ref * 0.9 if action == sj.constant.Action.Sell else ref * 1.1
            
            lmt_px = round_to_tick(lmt_px)
            
            print(f"[Attempt] Aggressive LMT IOC for {code} {action} @ {lmt_px}")
            ok, c, m = attempt_order(
                qty_use=qty_lots,
                lot_type=sj.constant.StockOrderLot.Common,
                px_type=sj.constant.StockPriceType.LMT,
                px_val=lmt_px,
                tag="LMT_IOC"
            )

            if ok:
                pass # sent_codes.add(code)
            else:
                print(f"[Error] All attempts failed for stock {code}")
            


            if ok:
                sent_codes.add(code)
            else:
                 print(f"  -> GAVE UP on {code} after retries.")
                 sent_codes.add(code) # Block future retries to prevent spam check loop

                 sent_codes.add(code) # Block future retries to prevent spam check loop

        # ---- Futures ----
        if fut_pos:
            print("Future positions:", [(p.code, int(p.quantity), str(p.direction)) for p in fut_pos])
        
        for p in fut_pos:
            code = str(p.code)
            qty  = int(p.quantity)
            direction = str(p.direction)

            # if code in sent_codes:
            #     continue
            
            # Direction: Buy -> Sell, Sell -> Buy
            action = sj.constant.Action.Sell if "Buy" in direction else sj.constant.Action.Buy
            
            # Helper for Futures
            def attempt_future_order(q, px_type, px_val, tag="FUT"):
                print(f"[Send FUT] {code}: {action.value} {q} @ {px_type}({px_val}) [{tag}]")
                if DRY_RUN: return True
                
                try:
                    # Resolve contract (Basic)
                    contract = api.Contracts.Futures[code]
                except Exception:
                    # Fallback scan
                    contract = None
                    for cat in api.Contracts.Futures:
                         try:
                             for c in cat:
                                 if getattr(c, "code", "") == code:
                                     contract = c
                                     break
                         except: pass
                         if contract: break
                    if not contract:
                        print(f"  -> Contract {code} NOT FOUND")
                        return False

                # Fix: Check if FuturesOrderType exists, else use OrderType
                fut_order_type_cls = getattr(sj.constant, "FuturesOrderType", sj.constant.OrderType)

                try:
                    ord_f = api.Order(
                        action=action,
                        price=px_val,
                        quantity=q,
                        price_type=px_type,
                        order_type=fut_order_type_cls.IOC, # Always IOC for clearing
                        octype=sj.constant.FuturesOCType.Auto,
                        account=api.futopt_account
                    )
                    t = api.place_order(contract, ord_f, timeout=10000) # Added timeout
                except Exception as e:
                     print(f"  -> ERROR ({tag}): {e}")
                     return False

                op = getattr(t, "operation", {}) or {}
                if isinstance(op, dict):
                    c = str(op.get("op_code", ""))
                    m = str(op.get("op_msg", ""))
                    if c and c != "00":
                        print(f"  -> REJECTED ({tag}): {c} {m}")
                        return False
                print(f"  -> SENT ({tag})")
                return True

            # 1. Tier 1: MKT IOC
            print(f"[Attempt] FUT MKT IOC for {code} {action}")
            ok = attempt_future_order(
                q=qty,
                px_type=sj.constant.FuturesPriceType.MKT,
                px_val=0,
                tag="FUT_MKT_IOC"
            )
            
            if ok:
                pass # sent_codes.add(code)
                continue
            
            # 2. Tier 2: Aggressive Limit IOC (Limit Up/Down)
            # Need limit price
            try:
                contract = api.Contracts.Futures[code]
                if action == sj.constant.Action.Sell:
                    lmt_px = float(getattr(contract, "limit_down", 0) or 0)
                else:
                    lmt_px = float(getattr(contract, "limit_up", 0) or 0)
            except:
                lmt_px = 0
            
            if lmt_px > 0:
                 print(f"[Attempt] FUT LMT IOC for {code} {action} @ {lmt_px}")
                 ok = attempt_future_order(
                    q=qty,
                    px_type=sj.constant.FuturesPriceType.LMT,
                    px_val=lmt_px,
                    tag="FUT_LMT_IOC"
                 )
                 if ok: pass # sent_codes.add(code)

        print("\n[Wait] 8s then rescan ...")
        time.sleep(8)

    print("\n[STOP] Reached max loops. Please check ORDER_CB rejects and remaining positions.")

if __name__ == "__main__":
    main()
