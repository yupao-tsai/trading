#!/usr/bin/env python3
"""
查看交易日誌工具
顯示：1. 送出的訂單 2. 成交的訂單 3. 失敗後沖銷的訂單 4. 損益
"""

import json
import os
import sys
from datetime import datetime
from collections import defaultdict
from typing import Dict, List

ORDER_HISTORY_FILE = "order_history.jsonl"

def load_logs(filename: str) -> List[Dict]:
    """載入所有記錄"""
    records = []
    if not os.path.exists(filename):
        return records
    
    try:
        with open(filename, 'r', encoding='utf-8') as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    rec = json.loads(line)
                    records.append(rec)
                except Exception:
                    pass
    except Exception as e:
        print(f"讀取失敗: {e}")
    
    return records

def format_time(ts: float) -> str:
    """格式化時間"""
    try:
        return datetime.fromtimestamp(ts).strftime('%Y-%m-%d %H:%M:%S')
    except:
        return str(ts)

def print_orders_sent(records: List[Dict]):
    """顯示送出的訂單"""
    sent_orders = [r for r in records if r.get("event_type") == "ORDER_SENT"]
    
    print("\n" + "=" * 80)
    print("1. 送出的訂單 (ORDER_SENT)")
    print("=" * 80)
    
    if not sent_orders:
        print("  無記錄")
        return
    
    print(f"{'時間':<20} {'訂單ID':<12} {'股票':<8} {'期貨':<10} {'階段':<8} {'部位':<8} {'動作':<8} {'價格':<10} {'數量':<6}")
    print("-" * 80)
    
    for r in sorted(sent_orders, key=lambda x: x.get("ts", 0)):
        print(f"{format_time(r.get('ts', 0)):<20} "
              f"{r.get('order_id', ''):<12} "
              f"{r.get('stock_code', ''):<8} "
              f"{r.get('fut_code', ''):<10} "
              f"{r.get('phase', ''):<8} "
              f"{r.get('leg', ''):<8} "
              f"{str(r.get('action', '')):<8} "
              f"{r.get('price', 0):>10.2f} "
              f"{r.get('qty', 0):>6}")

def print_orders_filled(records: List[Dict]):
    """顯示成交的訂單"""
    filled_orders = [r for r in records if r.get("event_type") == "ORDER_FILLED"]
    
    print("\n" + "=" * 80)
    print("2. 成交的訂單 (ORDER_FILLED)")
    print("=" * 80)
    
    if not filled_orders:
        print("  無記錄")
        return
    
    print(f"{'時間':<20} {'訂單ID':<12} {'股票':<8} {'期貨':<10} {'階段':<8} {'部位':<8} {'動作':<8} {'成交價':<10} {'成交量':<6} {'狀態':<12}")
    print("-" * 80)
    
    for r in sorted(filled_orders, key=lambda x: x.get("ts", 0)):
        print(f"{format_time(r.get('ts', 0)):<20} "
              f"{r.get('order_id', ''):<12} "
              f"{r.get('stock_code', ''):<8} "
              f"{r.get('fut_code', ''):<10} "
              f"{r.get('phase', ''):<8} "
              f"{r.get('leg', ''):<8} "
              f"{str(r.get('action', '')):<8} "
              f"{r.get('deal_price', 0):>10.2f} "
              f"{r.get('deal_qty', 0):>6} "
              f"{r.get('status', ''):<12}")

def print_orders_cancelled(records: List[Dict]):
    """顯示取消/失敗的訂單"""
    cancelled_orders = [r for r in records if r.get("event_type") in ("ORDER_CANCELLED", "REPAIR_FAILED")]
    
    print("\n" + "=" * 80)
    print("3. 取消/失敗的訂單 (ORDER_CANCELLED / REPAIR_FAILED)")
    print("=" * 80)
    
    if not cancelled_orders:
        print("  無記錄")
        return
    
    print(f"{'時間':<20} {'訂單ID':<12} {'股票':<8} {'期貨':<10} {'階段':<8} {'部位':<8} {'原因':<30}")
    print("-" * 80)
    
    for r in sorted(cancelled_orders, key=lambda x: x.get("ts", 0)):
        reason = r.get("reason", r.get("event_type", ""))
        print(f"{format_time(r.get('ts', 0)):<20} "
              f"{r.get('order_id', ''):<12} "
              f"{r.get('stock_code', ''):<8} "
              f"{r.get('fut_code', ''):<10} "
              f"{r.get('phase', ''):<8} "
              f"{r.get('leg', ''):<8} "
              f"{reason[:30]:<30}")

def print_pnl(records: List[Dict]):
    """顯示損益"""
    pnl_records = [r for r in records if r.get("event_type") == "P&L"]
    
    print("\n" + "=" * 80)
    print("4. 損益 (P&L)")
    print("=" * 80)
    
    if not pnl_records:
        print("  無記錄")
        return
    
    total_pnl = 0.0
    print(f"{'時間':<20} {'股票':<8} {'開倉股票價':<12} {'開倉期貨價':<12} {'平倉股票價':<12} {'平倉期貨價':<12} {'股票損益':<12} {'期貨損益':<12} {'手續費':<10} {'總損益':<12}")
    print("-" * 80)
    
    for r in sorted(pnl_records, key=lambda x: x.get("ts", 0)):
        total_pnl += float(r.get("total_pnl", 0))
        print(f"{format_time(r.get('ts', 0)):<20} "
              f"{r.get('stock_code', ''):<8} "
              f"{r.get('open_stock_price', 0):>12.2f} "
              f"{r.get('open_future_price', 0):>12.2f} "
              f"{r.get('close_stock_price', 0):>12.2f} "
              f"{r.get('close_future_price', 0):>12.2f} "
              f"{r.get('stock_pnl', 0):>12.2f} "
              f"{r.get('future_pnl', 0):>12.2f} "
              f"{r.get('transaction_cost', 0):>10.2f} "
              f"{r.get('total_pnl', 0):>12.2f}")
    
    print("-" * 80)
    print(f"{'總計':<20} {'':<8} {'':<12} {'':<12} {'':<12} {'':<12} {'':<12} {'':<12} {'':<10} {total_pnl:>12.2f}")

def main():
    if len(sys.argv) > 1:
        filename = sys.argv[1]
    else:
        filename = ORDER_HISTORY_FILE
    
    print(f"讀取交易日誌: {filename}")
    records = load_logs(filename)
    
    if not records:
        print(f"未找到記錄檔案或檔案為空: {filename}")
        return
    
    print(f"總共 {len(records)} 筆記錄")
    
    # 只顯示今天的記錄
    today = datetime.now().strftime('%Y-%m-%d')
    today_records = [r for r in records if r.get("date") == today]
    
    if today_records:
        print(f"今天的記錄: {len(today_records)} 筆")
        records = today_records
    else:
        print("今天無記錄，顯示所有記錄")
    
    print_orders_sent(records)
    print_orders_filled(records)
    print_orders_cancelled(records)
    print_pnl(records)
    
    print("\n" + "=" * 80)
    print("查詢完成")
    print("=" * 80)

if __name__ == "__main__":
    main()
