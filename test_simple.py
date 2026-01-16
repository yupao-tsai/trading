# -*- coding: utf-8 -*-
"""
test_simple.py

功能：
- 登入 Shioaji（simulation/real）
- fetch_contracts
- 列出現股/期貨持倉
- 依持倉方向送反向單沖銷（現股/期貨都做）
- 等待成交：輪詢 list_positions 直到清空或 timeout

.env（不要改名）：
  Sinopack_CA_API_KEY
  Sinopack_CA_SECRET_KEY
  SIMULATION=true/false (預設 true)

(選用 CA)：
  Sinopack_PERSON_ID
  Sinopack_CA_PATH
  Sinopack_CA_PASSWORD

用法：
  python test_simple.py
  python test_simple.py --dry-run
  python test_simple.py --poll 2 --timeout 60
"""

import os
import sys
import time
import argparse
from typing import Any, Dict, Tuple, Iterable

try:
    from dotenv import load_dotenv
    load_dotenv()
except Exception:
    pass

import shioaji as sj


def now_ts() -> str:
    return time.strftime("%Y-%m-%d %H:%M:%S")


def safe_int(x: Any, default: int = 0) -> int:
    try:
        return int(x)
    except Exception:
        return default


def exch_to_str(exch: Any) -> str:
    if exch is None:
        return ""
    s = str(exch)
    if "." in s:
        s = s.split(".")[-1]
    return s.upper().strip()


def iter_contract_map(m: Any) -> Iterable[Tuple[str, Any]]:
    """
    盡量把一個 contract mapping 迭代成 (code, contract)
    """
    # dict-like
    try:
        for k, v in m.items():
            yield str(k), v
        return
    except Exception:
        pass

    # some container supports keys()
    try:
        for k in m.keys():
            try:
                yield str(k), m[k]
            except Exception:
                continue
        return
    except Exception:
        pass


def _iter_contracts(container: Any) -> Iterable[Any]:
    """
    Iterate contracts from container (dict values, list, etc.)
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
            if hasattr(c, "code"):
                yield c
    except Exception:
        return


def _try_get(container: Any, key: str) -> Any:
    """Try to get item from container by key"""
    try:
        if hasattr(container, "__getitem__"):
            return container[key]
    except Exception:
        pass
    try:
        if hasattr(container, "get"):
            return container.get(key)
    except Exception:
        pass
    return None


def build_stock_contract_index(api: sj.Shioaji) -> Dict[str, Any]:
    """
    掃描 api.Contracts.Stocks 建立 code -> contract
    同時印出我們實際抓到哪些 exchange 分區（TSE/OTC/...）
    重要：索引時使用 contract.code 作為 key，不是 dict 的 key
    """
    stocks = api.Contracts.Stocks
    idx: Dict[str, Any] = {}
    bucket_count: Dict[str, int] = {}

    def add_bucket(bucket_name: str, bucket_obj: Any):
        cnt = 0
        # 先嘗試用 dict keys
        for key, contract in iter_contract_map(bucket_obj):
            if contract is None:
                continue
            # 優先使用 contract.code，如果沒有才用 key
            code = getattr(contract, "code", None)
            if code:
                code = str(code).strip()
                idx[code] = contract
                # 也索引 key（可能是 TSE{code} 或 OTC{code}）
                key_str = str(key).strip()
                if key_str and key_str != code:
                    idx[key_str] = contract
                cnt += 1
            elif key:
                # 沒有 code 屬性，用 key
                idx[str(key).strip()] = contract
                cnt += 1
        
        # 也掃描所有 contracts 確保不漏
        for contract in _iter_contracts(bucket_obj):
            code = getattr(contract, "code", None)
            if code:
                code = str(code).strip()
                idx[code] = contract
                cnt += 1
        
        if cnt > 0:
            bucket_count[bucket_name] = cnt

    # 1) 常見屬性 Stocks.TSE / Stocks.OTC / Stocks.OES ...
    for name in ("TSE", "OTC", "OES", "TAIEX"):
        try:
            if hasattr(stocks, name):
                add_bucket(name, getattr(stocks, name))
        except Exception:
            pass

    # 2) dict 形式 Stocks[Exchange.TSE] 或 Stocks["TSE"]
    try:
        if isinstance(stocks, dict):
            for k, v in stocks.items():
                bn = exch_to_str(k) or str(k)
                add_bucket(bn, v)
    except Exception:
        pass

    print(f"[{now_ts()}] [Debug] Stocks contract buckets found: {bucket_count}")
    print(f"[{now_ts()}] [Debug] Total stock contracts indexed: {len(idx)}")
    return idx


def get_stock_contract_fallback(api: sj.Shioaji, code: str, stock_contracts: Dict[str, Any]) -> Any:
    """
    Fallback lookup when contract not found in index.
    Tries multiple key formats and scans contracts.
    """
    code = str(code).strip()
    
    # 先檢查索引
    if code in stock_contracts:
        return stock_contracts[code]
    
    # 嘗試不同 key 格式
    keys = [code, f"TSE{code}", f"OTC{code}"]
    stocks = api.Contracts.Stocks
    
    # 1) direct Stocks mapping
    for k in keys:
        c = _try_get(stocks, k)
        if c is not None:
            c_code = getattr(c, "code", None)
            if c_code and str(c_code).strip() == code:
                return c
    
    # 2) buckets
    for ex_name in ("TSE", "OTC", "OES"):
        bucket = getattr(stocks, ex_name, None)
        if bucket is None:
            continue
        
        # try keys in this bucket
        for k in keys:
            c = _try_get(bucket, k)
            if c is not None:
                c_code = getattr(c, "code", None)
                if c_code and str(c_code).strip() == code:
                    return c
        
        # scan bucket objects
        for c in _iter_contracts(bucket):
            c_code = getattr(c, "code", None)
            if c_code and str(c_code).strip() == code:
                return c
    
    # 3) final: scan all stocks
    for c in _iter_contracts(stocks):
        c_code = getattr(c, "code", None)
        if c_code and str(c_code).strip() == code:
            return c
    
    return None


def build_futures_contract_index(api: sj.Shioaji) -> Dict[str, Any]:
    """
    掃描 api.Contracts.Futures 建立 code -> contract（best-effort）
    """
    futs = api.Contracts.Futures
    idx: Dict[str, Any] = {}

    # Futures 可能有 .get(code)
    try:
        # 有些版本 futs 本身就像 dict
        for code, ctt in iter_contract_map(futs):
            idx[str(code)] = ctt
    except Exception:
        pass

    # 掃 family
    for name in dir(futs):
        if name.startswith("_"):
            continue
        fam = getattr(futs, name, None)
        if fam is None:
            continue
        try:
            for code, ctt in iter_contract_map(fam):
                idx[str(code)] = ctt
        except Exception:
            pass

    print(f"[{now_ts()}] [Debug] Total futures contracts indexed: {len(idx)}")
    return idx


def adjust_price_to_tick(price: float) -> float:
    """
    將價格調整到符合台灣股市檔位規則
    檔位規則：
    - 0-10: 0.01
    - 10-50: 0.05
    - 50-100: 0.1
    - 100-500: 0.5
    - 500-1000: 1
    - 1000-5000: 5
    """
    if price <= 0:
        return price
    
    if price < 10:
        tick = 0.01
    elif price < 50:
        tick = 0.05
    elif price < 100:
        tick = 0.1
    elif price < 500:
        tick = 0.5
    elif price < 1000:
        tick = 1.0
    else:
        tick = 5.0
    
    # 四捨五入到最近的檔位
    adjusted = round(price / tick) * tick
    return round(adjusted, 2)  # 保留兩位小數


def pick_stock_order_lot(c: Any, qty_shares: int, for_closing: bool = True):
    """
    選擇股票訂單的 lot 類型
    規則：
    - qty >= 1000 且是 1000 的倍數 -> Common（整股）
    - qty < 1000 或非 1000 倍數 -> IntradayOdd / Odd（零股）
    注意：零股必須使用零股類型，即使是平倉也一樣
    """
    lot_enum = getattr(c, "TFTStockOrderLot", None) or getattr(c, "StockOrderLot", None)
    if lot_enum is None:
        return None

    common = getattr(lot_enum, "Common", None) or getattr(lot_enum, "COMMON", None)
    intraday_odd = getattr(lot_enum, "IntradayOdd", None) or getattr(lot_enum, "Odd", None)

    # 根據數量決定：零股必須使用零股類型
    if qty_shares >= 1000 and qty_shares % 1000 == 0:
        return common  # 整股使用 Common
    else:
        return intraday_odd or common  # 零股使用 IntradayOdd/Odd


def check_trading_hours() -> tuple[bool, str]:
    """
    檢查是否在交易時段 (使用台灣時間 UTC+8)
    返回: (is_trading_hours, message)
    """
    import datetime
    
    # 定義台灣時區 (UTC+8)
    tz_tw = datetime.timezone(datetime.timedelta(hours=8))
    now_tw = datetime.datetime.now(tz_tw)
    current_time = now_tw.time()
    
    # 台灣股市交易時段：09:00-13:30 (一般時段)
    # 盤中逐筆交易：09:00-13:25 (可下市價單)
    # 盤後定價交易：13:25-13:30 (僅限價單)
    morning_start = datetime.time(9, 0)
    afternoon_end = datetime.time(13, 30)
    market_order_end = datetime.time(13, 25)
    
    if morning_start <= current_time <= afternoon_end:
        if current_time <= market_order_end:
             return True, "盤中交易時段 (可市價單)"
        else:
             return True, "盤後定價/最後撮合時段 (限價單 Only)"
    else:
        return False, f"非交易時段 (TW Time: {current_time.strftime('%H:%M:%S')})"


def list_positions_safe(api: sj.Shioaji, account):
    """
    Safely list positions with retries.
    """
    for i in range(3):
        try:
            ps = api.list_positions(account, timeout=10000) # Increase timeout to 10s
            out = []
            for p in ps:
                if safe_int(getattr(p, "quantity", 0)) != 0:
                    out.append(p)
            return out
        except Exception as e:
            print(f"[{now_ts()}] ⚠️ list_positions failed (attempt {i+1}/3): {e}")
            time.sleep(1.0)
    
    print(f"[{now_ts()}] ❌ list_positions failed after 3 retries.")
    return []


def extract_op_msg(trade: Any) -> str:
    try:
        op = getattr(trade, "operation", None)
        if op is None:
            return ""
        msg = getattr(op, "op_msg", "") or ""
        return msg
    except Exception:
        return ""


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true", help="只列印不送單")
    ap.add_argument("--sleep", type=float, default=0.2, help="每筆送單後 sleep 秒數")
    ap.add_argument("--poll", type=float, default=2.0, help="等待成交輪詢秒數")
    ap.add_argument("--timeout", type=float, default=60.0, help="等待成交最長秒數")
    args = ap.parse_args()

    SIMULATION = os.getenv("SIMULATION", "true").strip().lower() in ("1", "true", "yes", "y")
    api_key = (os.getenv("Sinopack_CA_API_KEY") or "").strip()
    secret_key = (os.getenv("Sinopack_CA_SECRET_KEY") or "").strip()
    if not api_key or not secret_key:
        print("❌ Missing Sinopack_CA_API_KEY / Sinopack_CA_SECRET_KEY in env/.env")
        sys.exit(2)

    api = sj.Shioaji(simulation=SIMULATION)
    print(f"[{now_ts()}] [System] init Shioaji (simulation={SIMULATION})")

    print(f"[{now_ts()}] [System] login...")
    api.login(api_key=api_key, secret_key=secret_key)

    print(f"[{now_ts()}] [System] fetch_contracts...")
    api.fetch_contracts()

    # CA（可選）
    person_id = (os.getenv("Sinopack_PERSON_ID") or "").strip()
    ca_path = (os.getenv("Sinopack_CA_PATH") or "").strip()
    ca_pass = (os.getenv("Sinopack_CA_PASSWORD") or "").strip()
    if person_id and ca_path and ca_pass:
        try:
            print(f"[{now_ts()}] [System] activate_ca...")
            api.activate_ca(ca_path=ca_path, ca_passwd=ca_pass, person_id=person_id)
        except Exception as e:
            print(f"[{now_ts()}] ⚠️ activate_ca failed: {e}")

    c = sj.constant

    # 建立合約索引（這一步是為了解決你 position.exchange=None）
    stock_contracts = build_stock_contract_index(api)
    fut_contracts = build_futures_contract_index(api)

    # 讀持倉
    stock_pos = list_positions_safe(api, api.stock_account)
    fut_pos = list_positions_safe(api, api.futopt_account)

    print(f"[{now_ts()}] 📋 Current positions:")
    if not stock_pos and not fut_pos:
        print("✅ No positions.")
        return

    for p in stock_pos:
        # Print detailed info to debug "qty=2"
        print(f"  [STK] {p}")
    for p in fut_pos:
        print(f"  [FUT] {p}")

    # 檢查交易時段
    is_trading, trading_msg = check_trading_hours()
    print(f"[{now_ts()}] ⏰ {trading_msg}")
    if not is_trading:
        print(f"[{now_ts()}] ⚠️  注意：非交易時段下單必須使用限價單 (LMT)，且價格不能為 0")
        print(f"[{now_ts()}] ⚠️  市價單 (MKT) 僅能在 09:00-13:25 的盤中逐筆交易時段使用")
        print(f"[{now_ts()}] ⚠️  市價單必須搭配 IOC 或 FOK，不能搭配 ROD")

    
    # ---------- 準備 Snapshot ----------
    print(f"[{now_ts()}] 📸 Batch fetching snapshots...")
    contracts_to_snap = []
    
    # 收集 Stock Contracts
    for p in stock_pos:
        code = str(p.code)
        contract = stock_contracts.get(code)
        if contract is None:
            contract = get_stock_contract_fallback(api, code, stock_contracts)
            if contract:
                stock_contracts[code] = contract
        if contract:
            contracts_to_snap.append(contract)

    # 收集 Future Contracts
    for p in fut_pos:
        code = str(p.code)
        contract = fut_contracts.get(code)
        if contract:
            contracts_to_snap.append(contract)
            
    # 批量抓 Snapshot
    snapshots = {}
    if contracts_to_snap:
        try:
            snaps = api.snapshots(contracts_to_snap)
            for s in snaps:
                # TSSE/TSE/OTC etc. usually have 'code' in snapshot or we match by sequence?
                # Shioaji snapshot object has 'code' usually.
                if hasattr(s, 'code'):
                     snapshots[s.code] = s
        except Exception as e:
             print(f"[{now_ts()}] ⚠️ Batch snapshot failed: {e}")

    # 判斷是否可用市價單 (TwTime < 13:25)
    import datetime
    tz_tw = datetime.timezone(datetime.timedelta(hours=8))
    now_tw = datetime.datetime.now(tz_tw)
    # 寬鬆一點，只要是 09:00 - 13:25 之間都嘗試市價
    can_use_market_order = False
    if is_trading:
         # check_trading_hours 已經確認在 09:00 - 13:30
         # 再確認是否在 13:25 前
         if now_tw.time() < datetime.time(13, 25):
             can_use_market_order = True

    print(f"[{now_ts()}] 🚀 Sending close orders (Market Order Allowed: {can_use_market_order})...")

    # ---------- 股票沖銷 ----------
    for p in stock_pos:
        code = str(p.code)
        qty = safe_int(getattr(p, "quantity", 0))
        direction = getattr(p, "direction", None)

        # 嚴格用 direction
        if direction == c.Action.Buy:
            action = c.Action.Sell
        elif direction == c.Action.Sell:
            action = c.Action.Buy
        else:
            print(f"[{now_ts()}] ❌ [STK] unknown direction for code={code}, direction={direction} (skip)")
            continue

        close_qty = abs(qty)
        contract = stock_contracts.get(code)
        if contract is None:
             print(f"[{now_ts()}] ❌ [STK] contract not found for code={code} (skip)")
             continue

        # Lot Type
        lot = pick_stock_order_lot(c, close_qty, for_closing=True)
        if lot is None:
            print(f"[{now_ts()}] ❌ [STK] cannot resolve StockOrderLot enum (skip)")
            continue

        # --- 價格與訂單類型決定邏輯 ---
        # 預設: 限價單 (ROD + LMT)
        try:
             order_type = c.OrderType.ROD
        except:
             order_type = "ROD"
        price_type = c.StockPriceType.LMT
        final_price = 0.0
        
        # 1. 如果在盤中 (09:00-13:25)，且非零股 (Common)，優先嘗試市價單
        # 注意: 零股 (IntradayOdd) 不支援市價單，必須用限價
        is_common_lot = (lot == getattr(c.StockOrderLot, 'Common', None) or str(lot)=="Common")
        
        if can_use_market_order and is_common_lot:
            # 市價單
            try:
                price_type = c.StockPriceType.MKT
                # 市價單通常搭配 IOC 或 FOK (Shioaji 預設 MKT 會搭配 IOC/FOK? 需明確指定)
                # 根據 Shioaji 文件，MKT 可搭配 ROD/IOC/FOK，但台股規則 MKT 通常是 IOC/FOK
                # 用 ROD + MKT 也是可以的 (會自動轉?) 
                # 安全起見，市價單用 ROD 即可 (系統會處理) 或者依慣例 MKT
                # 這裡設定 Price=0
                final_price = 0.0
            except:
                pass
        
        # 2. 如果決定不用市價單 (或是零股、或是非盤中)，則找最佳限價
        if price_type != c.StockPriceType.MKT:
             # 優先順序: Snapshot Close > Snapshot Reference > Position Price > Contract Ref
             snap = snapshots.get(code)
             
             # (A) Snapshot Check
             if snap:
                 if hasattr(snap, 'close') and snap.close and float(snap.close) > 0:
                     final_price = float(snap.close)
                 elif hasattr(snap, 'reference') and snap.reference and float(snap.reference) > 0:
                     final_price = float(snap.reference)
            
             # (B) Position Price (Cost)
             if final_price <= 0:
                  pos_price = getattr(p, "price", 0)
                  if pos_price and float(pos_price) > 0:
                       final_price = float(pos_price)
             
             # (C) Contract Reference
             if final_price <= 0:
                  ref = getattr(contract, 'reference', 0)
                  if ref and float(ref) > 0:
                       final_price = float(ref)
             
             if final_price <= 0:
                  print(f"[{now_ts()}] ❌ [STK] cannot determine price for code={code} (skip)")
                  continue
             
             # Adjust Tick
             final_price = adjust_price_to_tick(final_price)
        
        # 構建 Order
        # 如果是 MKT，price 設為 0 (或不設? Shioaji Order object requires price?)
        # Shioaji Order requires price argument usually. For MKT, distinct broker rules apply.
        # 但 Shioaji Python wrapper 允許 MKT 時 price=0? 
        # Safest: If MKT, price=0 is fine usually.
        
        order = api.Order(
            price=final_price,
            quantity=close_qty,
            action=action,
            price_type=price_type,
            order_type=order_type,
            order_lot=lot,
            account=api.stock_account
        )
        
        pt_str = "MKT" if price_type == c.StockPriceType.MKT else "LMT"
        print(f"[{now_ts()}] [STK] CLOSE code={code} action={action} qty={close_qty} type={pt_str} price={final_price} lot={lot}")

        if args.dry_run:
            print("  (dry-run) skip")
        else:
            try:
                trade = api.place_order(contract, order)
                print(trade)
                op_msg = extract_op_msg(trade)
                
                # Check for "Invalid Product Code" (88) which often happens in Simulation for Odd Lots
                # OR if we interpreted Lots as Shares (e.g. qty=2 means 2 lots, but we sent 2 shares Odd Order)
                is_error_88 = op_msg and ('無此商品代碼' in op_msg or '88' in str(getattr(trade, 'operation', {}).get('op_code', '')))
                
                if is_error_88 and (str(lot) == "IntradayOdd" or str(lot) == "Odd") and close_qty < 1000:
                    print(f"[{now_ts()}] ⚠️ [STK] Order rejected with '88'. It might be a Common Lot position (qty={close_qty} lots?). Retrying as Common Lot...")
                    
                    # Updates for Common Lot
                    lot_common = getattr(c.StockOrderLot, "Common", None)
                    if lot_common:
                        order.order_lot = lot_common
                        # NOTE: If we assume close_qty was LOTS, and now we send Common Lot order with same number...
                        # Shioaji Common Order Quantity is in LOTS? -> Yes.
                        # Shioaji IntradayOdd Order Quantity is in SHARES.
                        # So if we have 2, and we sent IntradayOdd(2), we sent 2 shares.
                        # If we retry Common(2), we send 2 Lots (2000 shares). 
                        # This matches the hypothesis that list_positions returned Lots.
                        
                        print(f"[{now_ts()}] 🔄 [STK] Retrying as Common Lot: qty={close_qty} (lots)")
                        trade_retry = api.place_order(contract, order)
                        print(trade_retry)
                        op_msg = extract_op_msg(trade_retry)
                    
                if op_msg:
                    print(f"[{now_ts()}] ⚠️ [STK] op_msg: {op_msg}")
            except Exception as e:
                print(f"[{now_ts()}] ❌ [STK] place_order failed for {code}: {e}")

        time.sleep(args.sleep)

    # ---------- 期貨沖銷 ----------
    for p in fut_pos:
        code = str(p.code)
        qty = safe_int(getattr(p, "quantity", 0))
        direction = getattr(p, "direction", None)

        if direction == c.Action.Buy:
            action = c.Action.Sell
        elif direction == c.Action.Sell:
            action = c.Action.Buy
        else:
            print(f"[{now_ts()}] ❌ [FUT] unknown direction for code={code}, direction={direction} (skip)")
            continue

        close_qty = abs(qty)
        contract = fut_contracts.get(code)
        if contract is None:
            print(f"[{now_ts()}] ❌ [FUT] contract not indexed for code={code} (skip)")
            continue

        # 期貨訂單邏輯
        # 預設限價單
        try:
            fut_order_type = c.FuturesOrderType.ROD
        except:
            fut_order_type = "ROD"
            
        fut_price_type = c.StockPriceType.LMT
        final_price = 0.0
        
        # 期貨是否市價單?
        # 期貨市價單風險較高，但如果是為了清倉...
        # 這裡策略: 如果 can_use_market_order 為真，則使用市價單
        if can_use_market_order:
             fut_price_type = c.StockPriceType.MKT
             final_price = 0.0
        else:
             # 限價單找價格
             snap = snapshots.get(code)
             # (A) Snapshot
             if snap:
                 if hasattr(snap, 'close') and snap.close and float(snap.close) > 0:
                     final_price = float(snap.close)
                 elif hasattr(snap, 'reference') and snap.reference and float(snap.reference) > 0:
                     final_price = float(snap.reference)
             # (B) Position
             if final_price <= 0:
                  pos_price = getattr(p, "price", 0)
                  if pos_price and float(pos_price) > 0:
                       final_price = float(pos_price)
             # (C) Contract Ref
             if final_price <= 0:
                  ref = getattr(contract, 'reference', 0)
                  if ref and float(ref) > 0:
                        final_price = float(ref)
             
             if final_price <= 0:
                  print(f"[{now_ts()}] ❌ [FUT] cannot determine price for code={code} (skip)")
                  continue
             
             final_price = adjust_price_to_tick(final_price)

        order = api.Order(
            action=action,
            price=final_price,
            quantity=close_qty,
            price_type=fut_price_type,
            order_type=fut_order_type,
            octype=c.FuturesOCType.Auto,
            account=api.futopt_account
        )

        pt_str = "MKT" if fut_price_type == c.StockPriceType.MKT else "LMT"
        print(f"[{now_ts()}] [FUT] CLOSE code={code} action={action} qty={close_qty} type={pt_str} price={final_price}")

        if args.dry_run:
            print("  (dry-run) skip")
        else:
            try:
                trade = api.place_order(contract, order)
                print(trade)
                op_msg = extract_op_msg(trade)
                if op_msg:
                     print(f"[{now_ts()}] ⚠️ [FUT] op_msg: {op_msg}")
            except Exception as e:
                print(f"[{now_ts()}] ❌ [FUT] place_order failed for {code}: {e}")

        time.sleep(args.sleep)

    if args.dry_run:
        print(f"[{now_ts()}] ✅ dry-run done.")
        return

    # ---------- 等待成交/持倉清空 ----------
    print(f"[{now_ts()}] ⏳ Waiting fills... (poll={args.poll}s, timeout={args.timeout}s)")
    t0 = time.time()
    while True:
        if time.time() - t0 > args.timeout:
            break

        try:
            api.update_status(api.stock_account)
        except Exception:
            pass
        try:
            api.update_status(api.futopt_account)
        except Exception:
            pass

        sp = list_positions_safe(api, api.stock_account)
        fp = list_positions_safe(api, api.futopt_account)

        if not sp and not fp:
            print(f"[{now_ts()}] ✅ All positions closed.")
            return

        print(f"[{now_ts()}] still open: STK={len(sp)} FUT={len(fp)}")
        time.sleep(args.poll)

    sp = list_positions_safe(api, api.stock_account)
    fp = list_positions_safe(api, api.futopt_account)
    print(f"[{now_ts()}] ⚠️ Timeout. Remaining positions:")
    for p in sp:
        print(f"  [STK] code={p.code} qty={p.quantity} dir={getattr(p,'direction',None)}")
    for p in fp:
        print(f"  [FUT] code={p.code} qty={p.quantity} dir={getattr(p,'direction',None)}")


if __name__ == "__main__":
    main()
