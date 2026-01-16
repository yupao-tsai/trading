#!/usr/bin/env python3
"""
測試套利交易監控 - 檢查是否有交易信號產生
"""

import sys
import signal
import time
import threading

# 設定 60 秒測試時間
stop_flag = threading.Event()

def timeout_handler(signum, frame):
    print("\n[測試] 60 秒測試時間到，停止程式...")
    stop_flag.set()
    sys.exit(0)

signal.signal(signal.SIGALRM, timeout_handler)
signal.alarm(60)

try:
    from arbitrage_v9 import main
    
    print("=" * 60)
    print("測試套利交易監控")
    print("=" * 60)
    print("測試時間：60 秒")
    print("監控項目：")
    print("  - 策略引擎是否運作")
    print("  - 是否有 OPEN/CLOSE 交易信號")
    print("  - 是否有實際下單")
    print("=" * 60)
    print()
    
    main()
    
except KeyboardInterrupt:
    print("\n[測試] 使用者中斷")
except SystemExit:
    print("\n[測試] 程式結束")
except Exception as e:
    print(f"\n[測試] 發生錯誤: {e}")
    import traceback
    traceback.print_exc()
finally:
    signal.alarm(0)
