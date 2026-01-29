import tkinter as tk
from tkinter import ttk
import threading
import time
from datetime import datetime

# Define a futuristic dark theme
DARK_BG = "#1e1e1e"
DARK_FG = "#ffffff"
PANEL_BG = "#2d2d2d"
HIGHLIGHT = "#007acc"
GREEN_TEXT = "#00ff00"
RED_TEXT = "#ff4444"
YELLOW_TEXT = "#ffff00"

class ArbitrageDashboard:
    def __init__(self, manager):
        self.mgr = manager
        self.root = tk.Tk()
        self.root.title("Arbitrage Bot Dashboard")
        self.root.geometry("1600x900") # Wider for more columns
        self.root.configure(bg=DARK_BG)

        # Style configuration
        self.style = ttk.Style()
        self.style.theme_use('default')
        
        # Configure Grid Colors (Treeview)
        self.style.configure("Treeview", 
                           background=PANEL_BG, 
                           foreground=DARK_FG, 
                           fieldbackground=PANEL_BG,
                           borderwidth=0,
                           rowheight=25)
        self.style.map('Treeview', background=[('selected', HIGHLIGHT)])
        self.style.configure("Treeview.Heading", 
                           background="#3e3e3e", 
                           foreground=DARK_FG, 
                           relief="flat")
        
        self.style.configure("TFrame", background=DARK_BG)
        self.style.configure("TLabel", background=DARK_BG, foreground=DARK_FG, font=("Consolas", 10))
        self.style.configure("Header.TLabel", font=("Consolas", 14, "bold"), foreground=HIGHLIGHT)
        self.style.configure("Status.TLabel", font=("Consolas", 10), foreground="#aaaaaa")

        self.create_widgets()
        
        # Tracking internal state for UI updates
        self.last_update_ts = 0.0
        
        # Hook window close
        self.root.protocol("WM_DELETE_WINDOW", self.on_close)

    def create_widgets(self):
        # === Top Control Bar ===
        control_frame = ttk.Frame(self.root, padding="10")
        control_frame.pack(fill=tk.X)

        title_lbl = ttk.Label(control_frame, text="⚡ Arbitrage Bot Monitor", style="Header.TLabel")
        title_lbl.pack(side=tk.LEFT)

        self.status_lbl = ttk.Label(control_frame, text="Status: INIT", style="Header.TLabel", foreground=YELLOW_TEXT)
        self.status_lbl.pack(side=tk.LEFT, padx=30)
        
        # Fund Label (New)
        self.fund_lbl = ttk.Label(control_frame, text="Cash: - | Est.Cost: -", style="Header.TLabel", foreground=HIGHLIGHT)
        self.fund_lbl.pack(side=tk.LEFT, padx=30)

        # Right-side buttons
        btn_frame = ttk.Frame(control_frame)
        btn_frame.pack(side=tk.RIGHT)

        btn_Sync = tk.Button(btn_frame, text="🔄 SYNC POS", bg="#0066cc", fg="white", 
                             command=self.force_sync, relief="flat", padx=10)
        btn_Sync.pack(side=tk.LEFT, padx=5)

        btn_Repair = tk.Button(btn_frame, text="🛠 FORCE REPAIR", bg="#cc0000", fg="white", 
                               command=self.force_repair, relief="flat", padx=10)
        btn_Repair.pack(side=tk.LEFT, padx=5)

        btn_Stop = tk.Button(btn_frame, text="⏹ STOP SYSTEM", bg="#444444", fg="white", 
                             command=self.on_close, relief="flat", padx=10)
        btn_Stop.pack(side=tk.LEFT, padx=5)
        
        # === Main Content Area (Split Panes) ===
        main_pane = ttk.PanedWindow(self.root, orient=tk.VERTICAL)
        main_pane.pack(fill=tk.BOTH, expand=True, padx=10, pady=5)

        # --- Top Half: Monitored Pairs ---
        pairs_frame = ttk.Frame(main_pane)
        main_pane.add(pairs_frame, weight=3)
        
        ttk.Label(pairs_frame, text="Monitored Pairs (S=Stock, F=Future)", style="Header.TLabel").pack(anchor="w", pady=5)
        
        # Columns for Pairs - DETAILED QUOTES
        # Stock: Last, Vol, Bid(Sz), Ask(Sz)
        # Future: Last, Vol, Bid(Sz), Ask(Sz)
        # Spread
        pair_cols = (
            "Stock", "S.Last", "S.Vol", "S.Bid", "S.BSz", "S.Ask", "S.ASz",
            "Future", "F.Last", "F.Vol", "F.Bid", "F.BSz", "F.Ask", "F.ASz",
            "Spread", "Z", "Signal"
        )
        self.pair_tree = ttk.Treeview(pairs_frame, columns=pair_cols, show="headings", height=12)
        
        # Compact Column Widths
        # Code=60, Prices=60, Vols=50, Sizes=40
        col_widths = {
            "Stock": 60, "S.Last": 60, "S.Vol": 50, "S.Bid": 60, "S.BSz": 40, "S.Ask": 60, "S.ASz": 40,
            "Future": 60, "F.Last": 60, "F.Vol": 50, "F.Bid": 60, "F.BSz": 40, "F.Ask": 60, "F.ASz": 40,
            "Spread": 50, "Z": 50, "Signal": 120
        }

        for col in pair_cols:
            self.pair_tree.heading(col, text=col)
            w = col_widths.get(col, 60)
            self.pair_tree.column(col, width=w, anchor="center")
        
        # Scrollbar for pairs
        pair_scroll = ttk.Scrollbar(pairs_frame, orient="vertical", command=self.pair_tree.yview)
        self.pair_tree.configure(yscrollcommand=pair_scroll.set)
        pair_scroll.pack(side=tk.RIGHT, fill=tk.Y)
        self.pair_tree.pack(fill=tk.BOTH, expand=True)

        # --- Bottom Half: Horizontal Split (Positions | Orders) ---
        bottom_pane = ttk.PanedWindow(main_pane, orient=tk.HORIZONTAL)
        main_pane.add(bottom_pane, weight=2)

        # Positions Section
        pos_frame = ttk.Frame(bottom_pane)
        bottom_pane.add(pos_frame, weight=1)
        
        ttk.Label(pos_frame, text="Current Holdings (All)", style="Header.TLabel").pack(anchor="w", pady=5)
        pos_cols = ("Code", "Product", "Qty", "Avg Price")
        self.pos_tree = ttk.Treeview(pos_frame, columns=pos_cols, show="headings", height=8)
        
        for col in pos_cols:
            self.pos_tree.heading(col, text=col)
            self.pos_tree.column(col, width=80, anchor="center")
        
        pos_scroll = ttk.Scrollbar(pos_frame, orient="vertical", command=self.pos_tree.yview)
        self.pos_tree.configure(yscrollcommand=pos_scroll.set)
        pos_scroll.pack(side=tk.RIGHT, fill=tk.Y)
        self.pos_tree.pack(fill=tk.BOTH, expand=True)

        # Orders Log Section
        log_frame = ttk.Frame(bottom_pane)
        bottom_pane.add(log_frame, weight=2)
        
        ttk.Label(log_frame, text="Active / Recent Orders", style="Header.TLabel").pack(anchor="w", pady=5)
        
        ord_cols = ("TxID", "State", "Stk Ord", "Stk Fill", "Fut Ord", "Fut Fill", "Time")
        self.ord_tree = ttk.Treeview(log_frame, columns=ord_cols, show="headings", height=8)
        
        col_widths_ord = [70, 90, 70, 70, 70, 70, 90]
        for col, w in zip(ord_cols, col_widths_ord):
            self.ord_tree.heading(col, text=col)
            self.ord_tree.column(col, width=w, anchor="center")
            
        ord_scroll = ttk.Scrollbar(log_frame, orient="vertical", command=self.ord_tree.yview)
        self.ord_tree.configure(yscrollcommand=ord_scroll.set)
        ord_scroll.pack(side=tk.RIGHT, fill=tk.Y)
        self.ord_tree.pack(fill=tk.BOTH, expand=True)

        # === Footer ===
        footer_frame = ttk.Frame(self.root, padding="5")
        footer_frame.pack(fill=tk.X, side=tk.BOTTOM)
        
        self.msg_lbl = ttk.Label(footer_frame, text="System Ready.", style="Status.TLabel")
        self.msg_lbl.pack(side=tk.LEFT)
        
        self.clock_lbl = ttk.Label(footer_frame, text="--:--:--", style="Status.TLabel")
        self.clock_lbl.pack(side=tk.RIGHT)

    def force_sync(self):
        self.msg_lbl.config(text="⏳ Syncing Positions...", foreground=YELLOW_TEXT)
        # Use a thread to avoid freezing UI
        def _sync():
            try:
                self.mgr._sync_held_positions()
                self.mgr.execution.update_ledger_positions(self.mgr.ledger)
                self.msg_lbl.config(text="✅ Sync Complete", foreground=GREEN_TEXT)
            except Exception as e:
                self.msg_lbl.config(text=f"❌ Sync Failed: {e}", foreground=RED_TEXT)
        threading.Thread(target=_sync, daemon=True).start()

    def force_repair(self):
        self.mgr.trigger_repair = True
        self.msg_lbl.config(text="⚠️ Auto-Repair Triggered via GUI!", foreground=RED_TEXT)

    def on_close(self):
        print("[GUI] Close requested.")
        if self.mgr.running:
            self.mgr.stop()
        self.root.destroy()

    def run(self):
        # Schedule the first update
        self.root.after(100, self.update_loop)
        try:
            self.root.mainloop()
        except KeyboardInterrupt:
            self.on_close()

    def update_loop(self):
        if not self.mgr.running:
            self.status_lbl.config(text="STOPPED", foreground=RED_TEXT)
            return

        # LOGIC IS NOW IN BACKGROUND THREAD (vb06.py). 
        # API calls moved there. Here we purely read state.

        # 2. Update GUI elements (read-only from self.mgr)
        now = time.time()
        # Refresh UI every 500ms
        if now - self.last_update_ts > 0.5:
            try:
                self._refresh_ui()
            except Exception as e:
                print(f"[GUI] Refresh Error: {e}")
            self.last_update_ts = now
            
        # Schedule next tick (50ms ~ 20Hz)
        self.root.after(50, self.update_loop)


    # Cache to store last known values for rows to prevent useless updates
    # Key: IID, Value: Tuple of values
    _row_cache = {}

    def _refresh_ui(self):
        # Status
        self.status_lbl.config(text="RUNNING", foreground=GREEN_TEXT)
        self.clock_lbl.config(text=datetime.now().strftime("%H:%M:%S"))
        
        # --- Update Pairs ---
        current_pairs = list(self.mgr.monitored_pairs)
        
        for s, f in current_pairs:
            unique_id = f"PAIR_{s}_{f}"
            
            # Gather data
            stk = self.mgr.market_data.get_stock(s)
            fut = self.mgr.market_data.get_future(f)
            
            s_bid = stk.bid if stk else 0.0
            s_ask = stk.ask if stk else 0.0
            s_last = stk.price if stk else 0.0
            s_vol = stk.volume if stk else 0
            s_bid_sz = int(stk.bid_size) if (stk and stk.bid_size) else 0
            s_ask_sz = int(stk.ask_size) if (stk and stk.ask_size) else 0
            
            f_bid = fut.bid if fut else 0.0
            f_ask = fut.ask if fut else 0.0
            f_last = fut.price if fut else 0.0
            f_vol = fut.volume if fut else 0
            f_bid_sz = int(fut.bid_size) if (fut and fut.bid_size) else 0
            f_ask_sz = int(fut.ask_size) if (fut and fut.ask_size) else 0
            
            # Calcs
            sprd_open = (f_bid - s_ask) if (f_bid > 0 and s_ask > 0) else 0.0
            
            # Stats z-score
            st_open = self.mgr.strategy.stats.get((s, f, "OPEN"))
            z_open_str = "-"
            if st_open and st_open.samples > 0 and st_open.std > 0:
                z = (sprd_open - st_open.mean) / max(st_open.std, 1e-6)
                z_open_str = f"{z:.2f}"
            
            signal_txt = ""
            tx = self.mgr.active_transactions.get(s)
            if tx:
                signal_txt = f"ACTIVE: {tx.state.name}"
            
            # Helper to format price
            def fmt(p): 
                if p == 0: return "-"
                return f"{p:.2f}"
            def fmt_i(i): return f"{i}" 
            
            # Detect Limit Up/Down hints
            s_ask_str = fmt(s_ask)
            if s_bid > 0 and s_ask == 0: s_ask_str = "▲UP"
            s_bid_str = fmt(s_bid)
            if s_ask > 0 and s_bid == 0: s_bid_str = "▼DN"

            f_ask_str = fmt(f_ask)
            if f_bid > 0 and f_ask == 0: f_ask_str = "▲UP"
            f_bid_str = fmt(f_bid)
            if f_ask > 0 and f_bid == 0: f_bid_str = "▼DN"

            vals = (
                s, fmt(s_last), f"{s_vol}", s_bid_str, fmt_i(s_bid_sz), s_ask_str, fmt_i(s_ask_sz),
                f, fmt(f_last), f"{f_vol}", f_bid_str, fmt_i(f_bid_sz), f_ask_str, fmt_i(f_ask_sz),
                f"{sprd_open:.1f}", z_open_str,
                signal_txt
            )
            
            # DIFFERENTIAL UPDATE
            last_vals = self._row_cache.get(unique_id)
            if last_vals != vals:
                if self.pair_tree.exists(unique_id):
                    self.pair_tree.item(unique_id, values=vals)
                else:
                    self.pair_tree.insert("", "end", iid=unique_id, values=vals)
                self._row_cache[unique_id] = vals
            
        # --- Update Positions & Funds ---
        try:
             safe_positions = list(self.mgr.ledger.positions.items())
        except RuntimeError:
             safe_positions = []
             
        pos_iids = set()
        
        # Fund Calc
        est_stock_cost = 0.0
        est_fut_margin = 0.0
        stock_mult = 1000 # Default stock multiplier
        fut_mult = self.mgr.ledger.fut_multiplier_default
        fut_m_ratio = self.mgr.ledger.fut_margin_ratio
        
        for code, pos in sorted(safe_positions):
            # Calculate funds from active positions
            px = pos.avg_price if pos.avg_price > 0 else pos.last_price
            if px > 0:
                if pos.product == "Stock" and pos.qty > 0:
                    est_stock_cost += px * pos.qty * stock_mult
                elif pos.product != "Stock":
                    est_fut_margin += px * abs(pos.qty) * fut_mult * fut_m_ratio

            if pos.qty == 0: continue
            pid = f"POS_{code}"
            pos_iids.add(pid)
            prod = pos.product 
            vals = (code, prod, pos.qty, f"{pos.avg_price:.2f}")
            
            last_pos_vals = self._row_cache.get(pid)
            if last_pos_vals != vals:
                if self.pos_tree.exists(pid):
                    self.pos_tree.item(pid, values=vals)
                else:
                    self.pos_tree.insert("", "end", iid=pid, values=vals)
                self._row_cache[pid] = vals
        
        # Update Fund Label
        cash = self.mgr.ledger.cash_total
        total_equity = cash + est_stock_cost + est_fut_margin # Very Rough Estimate (Equity = Cash + MkV - Margin) -> actually logic is simpler. 
        # Just show what user asked: Funds Status.
        # Cash = Available Cash ideally.
        # Cost = Tied up capital.
        self.fund_lbl.config(text=f"Cash: {cash:,.0f} | Stock Val: {est_stock_cost:,.0f} | Fut Margin: {est_fut_margin:,.0f}")

        # Clear stale positions
        for child in self.pos_tree.get_children():
             if child not in pos_iids:
                 self.pos_tree.delete(child)
                 if child in self._row_cache: del self._row_cache[child]
                 
        # --- Orders ---
        # No caching for Orders yet as they might update frequently in non-value ways? 
        # Actually Orders list is rebuilt every time. Caching is harder here unless we key by TxID
        # Let's just do the existing logic but safeguard it.
        # Orders table is small (25 rows), typically fine.

        # --- Update Orders (Active Transactions) ---
        # Orders are time-ordered list, reusing IIDs is tricky unless we key by TxID
        # For scroll stability, we might want to just keep full refresh or use TxID if unique.
        # But TxList changes size. Full refresh for Orders is usually OK (fewer items).
        # Let's try to optimize if user scrolls.
        
        with self.mgr._lock:
            all_txs = list(self.mgr.active_transactions.values()) + list(self.mgr.completed_transactions)[-25:]
            
        all_txs.sort(key=lambda x: x.updated_at, reverse=True)
        ord_iids = set()
        
        for tx in all_txs[:25]:
            oid_key = f"ORD_{tx.tx_id}"
            ord_iids.add(oid_key)
            
            t_str = datetime.fromtimestamp(tx.updated_at).strftime("%H:%M:%S")
            vals = (tx.tx_id, tx.state.name, 
                    f"{tx.stock_order.status}", f"{tx.stock_order.filled_qty}",
                    f"{tx.future_order.status}", f"{tx.future_order.filled_qty}",
                    t_str)
            
            if self.ord_tree.exists(oid_key):
                self.ord_tree.item(oid_key, values=vals)
            else:
                self.ord_tree.insert("", "end", iid=oid_key, values=vals)
                
        # Cleanup old log entries
        for child in self.ord_tree.get_children():
            if child not in ord_iids:
                 self.ord_tree.delete(child)

if __name__ == "__main__":
    print("This module is intended to be imported by arbitrage_vb06.py")
