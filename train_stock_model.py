import torch
import torch.nn as nn
import torch.optim as optim
from torch.utils.data import DataLoader, random_split
from stock_data_loader import download_data, StockDataset
from stock_model import StockProbabilityModel
import os
import subprocess

def get_sp500_tickers():
    if not os.path.exists("symbols.txt"):
        print("symbols.txt not found. Generating...")
        # Check if gen script exists
        if os.path.exists("gen_sp500_symbols.py"):
            try:
                subprocess.run(["python3", "gen_sp500_symbols.py"], check=True)
            except Exception as e:
                print(f"Failed to generate symbols: {e}")
                return []
        else:
             print("gen_sp500_symbols.py missing.")
             return []
             
    if os.path.exists("symbols.txt"):
        with open("symbols.txt", "r") as f:
            tickers = [line.strip() for line in f if line.strip()]
        return tickers
    return []

def train():
    # 1. Config
    BATCH_SIZE = 64 # Increased batch size for larger data
    EPOCHS = 5 # Fewer epochs since data is huge, just for demo
    LEARNING_RATE = 0.001
    DEVICE = torch.device('cuda' if torch.cuda.is_available() else 'cpu')
    print(f"Using device: {DEVICE}")
    
    # 2. Data
    print("Fetching S&P 500 List...")
    tickers = get_sp500_tickers()
    if not tickers:
        print("Fallback to manual list.")
        tickers = ["AAPL", "MSFT", "GOOG", "AMZN", "NVDA"] # Fallback
    else:
        print(f"Found {len(tickers)} tickers.")

    
    print("Downloading data (Full History)...")
    # Using start_date=None to trigger period="max" in updated loader
    # Force download=False (default) to use cache if we have it, 
    # but for "Full History" we might want to ensure we aren't using the old short cache.
    # However, since we just added logic to handle cache, let's assume if it's there it's what we want 
    # OR we can clear cache manually. 
    # For now, I will NOT force download to save time if run repeatedly, but since previous cache was short, 
    # I recommend the user deletes 'data_cache' or I pass force_download=True once.
    # I'll pass start_date=None.
    
    data_frames = download_data(tickers, start_date=None)
    
    print("Creating dataset...")
    full_dataset = StockDataset(data_frames)
    
    if len(full_dataset) == 0:
        print("Error: Dataset is empty.")
        return

    # Split Train/Val
    train_size = int(0.9 * len(full_dataset)) # 90/10 split for massive data
    val_size = len(full_dataset) - train_size
    train_dataset, val_dataset = random_split(full_dataset, [train_size, val_size])
    
    # Use num_workers? subset?
    train_loader = DataLoader(train_dataset, batch_size=BATCH_SIZE, shuffle=True, num_workers=0)
    val_loader = DataLoader(val_dataset, batch_size=BATCH_SIZE, shuffle=False, num_workers=0)
    
    print(f"Training samples: {len(train_dataset)}, Validation samples: {len(val_dataset)}")
    
    # 3. Model
    model = StockProbabilityModel().to(DEVICE)
    if os.path.exists("best_stock_model.pth"):
         print("Loading existing model...")
         try:
            model.load_state_dict(torch.load("best_stock_model.pth", map_location=DEVICE))
         except:
            print("Failed to load existing model, starting fresh.")

    # Weighted Loss to handle class imbalance
    # Sparsity estimation: Price covers ~5 bins out of 100? -> 5% positive.
    # Recommended pos_weight = (Number of Negatives) / (Number of Positives) ~= 95/5 = 19.
    pos_weight = torch.tensor([20.0]).to(DEVICE)
    criterion = nn.BCEWithLogitsLoss(pos_weight=pos_weight)
    optimizer = optim.Adam(model.parameters(), lr=LEARNING_RATE)
    
    # 4. Loop
    best_val_loss = float('inf')
    
    for epoch in range(EPOCHS):
        model.train()
        train_loss = 0.0
        batch_count = 0
        
        for batch_x, batch_y in train_loader:
             batch_x, batch_y = batch_x.to(DEVICE), batch_y.to(DEVICE)
             
             optimizer.zero_grad()
             outputs = model(batch_x)
             loss = criterion(outputs, batch_y)
             loss.backward()
             optimizer.step()
             
             train_loss += loss.item() * batch_x.size(0)
             batch_count += 1
             if batch_count % 100 == 0:
                 print(f"  Batch {batch_count}/{len(train_loader)} Loss: {loss.item():.4f}")
            
        train_loss /= len(train_loader.dataset)
        
        # Validation
        model.eval()
        val_loss = 0.0
        with torch.no_grad():
            for batch_x, batch_y in val_loader:
                batch_x, batch_y = batch_x.to(DEVICE), batch_y.to(DEVICE)
                outputs = model(batch_x)
                loss = criterion(outputs, batch_y)
                val_loss += loss.item() * batch_x.size(0)
                
        val_loss /= len(val_loader.dataset)
        
        print(f"Epoch {epoch+1}/{EPOCHS} | Train Loss: {train_loss:.4f} | Val Loss: {val_loss:.4f}")
        
        if val_loss < best_val_loss:
            best_val_loss = val_loss
            torch.save(model.state_dict(), "best_stock_model.pth")
            print("  Values improved, model saved.")
            
    print("Training Complete.")

if __name__ == "__main__":
    train()
