#!/usr/bin/env python3
"""
測試 arbitrage_v9.py 的初始化與基本功能
"""

import sys
import signal
import time
import threading

# 設定 30 秒後自動停止
stop_flag = threading.Event()

def timeout_handler(signum, frame):
    print("\n[測試] 30 秒測試時間到，停止程式...")
    stop_flag.set()
    sys.exit(0)

# 設定信號處理（macOS 支援）
signal.signal(signal.SIGALRM, timeout_handler)
signal.alarm(30)

# 導入並執行主程式
try:
    # 直接執行 arbitrage_v9 的 main
    from arbitrage_v9 import main
    
    print("=" * 60)
    print("開始測試 arbitrage_v9.py")
    print("=" * 60)
    print("測試時間：30 秒")
    print("=" * 60)
    print()
    
    # 執行主程式
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
    signal.alarm(0)  # 取消計時器
