import tkinter as tk
from tkinter import ttk
import time
import threading
import random

class StaticLagTest:
    def __init__(self):
        self.root = tk.Tk()
        self.root.title("Lag Test: Static Button")
        self.root.geometry("600x400")
        self.root.configure(bg="#1e1e1e")

        control_frame = ttk.Frame(self.root, padding="10")
        control_frame.pack(fill=tk.X)
        
        # STATIC Button (USER REQUEST: Do not change text/color)
        self.btn_icon = tk.Button(control_frame, text="🔄 SYNC POS", bg="#0066cc", fg="white", 
                                  command=self.on_click, relief="flat", padx=10, font=("Arial", 12))
        self.btn_icon.pack(pady=20)
        
        self.lbl_info = tk.Label(self.root, text="Frame: 0", fg="white", bg="#1e1e1e")
        self.lbl_info.pack()

        self.counter = 0
        self.running = True
        
        # Keep the background load to verify if it affects the static window
        threading.Thread(target=self.background_load, daemon=True).start()
        
    def background_load(self):
        while self.running:
            # Simulate CPU work
            _ = [x**2 for x in range(50000)]
            time.sleep(0.01)

    def on_click(self):
        # Click feedback only
        print("Button Clicked!")

    def update_loop(self):
        self.counter += 1
        
        # ONLY update the counter label (to prove app is alive)
        # Button remains STATIC
        if self.counter % 5 == 0:
            self.lbl_info.config(text=f"Update: {self.counter}")
            
        self.root.after(50, self.update_loop)

    def run(self):
        self.root.after(100, self.update_loop)
        try:
            self.root.mainloop()
        except KeyboardInterrupt:
            self.running = False
            self.root.destroy()

if __name__ == "__main__":
    app = StaticLagTest()
    app.run()
