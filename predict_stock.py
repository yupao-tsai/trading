import torch
import yfinance as yf
import pandas as pd
import numpy as np
import argparse
from stock_model import StockProbabilityModel
import matplotlib.pyplot as plt
import os

def predict(ticker):
    DEVICE = torch.device('cuda' if torch.cuda.is_available() else 'cpu')
    model = StockProbabilityModel().to(DEVICE)
    
    if not os.path.exists("best_stock_model.pth"):
        print("Model file best_stock_model.pth not found. Please train first.")
        return
        
    model.load_state_dict(torch.load("best_stock_model.pth", map_location=DEVICE))
    model.eval()
    
    print(f"Fetching latest data for {ticker}...")
    # diverse fetch to ensure we have 60 days and some buffer
    df = yf.download(ticker, period="1y", progress=False)
    
    if len(df) < 60:
        print("Not enough data.")
        return
        
    # Standardize columns
    if 'Close' not in df.columns:
         if ('Close', ticker) in df.columns:
             df['Close'] = df[('Close', ticker)]
         elif 'Adj Close' in df.columns:
             df['Close'] = df['Adj Close']
    
    # Calculate MAs
    df['MA5'] = df['Close'].rolling(window=5).mean()
    df['MA20'] = df['Close'].rolling(window=20).mean()
    df['MA60'] = df['Close'].rolling(window=60).mean()
    
    df['VolMA5'] = df['Volume'].rolling(window=5).mean()
    df['VolMA20'] = df['Volume'].rolling(window=20).mean()
    df['VolMA60'] = df['Volume'].rolling(window=60).mean()
    
    df = df.dropna()
    
    if len(df) < 60:
        print("Not enough data after MA calc.")
        return

    # Calculate Features (12 channels)
    eps = 1e-8
    df['F_C_5'] = df['Close'] / (df['MA5'] + eps)
    df['F_H_5'] = df['High'] / (df['MA5'] + eps)
    df['F_L_5'] = df['Low'] / (df['MA5'] + eps)
    df['F_V_5'] = df['Volume'] / (df['VolMA5'] + eps)
    
    df['F_C_20'] = df['Close'] / (df['MA20'] + eps)
    df['F_H_20'] = df['High'] / (df['MA20'] + eps)
    df['F_L_20'] = df['Low'] / (df['MA20'] + eps)
    df['F_V_20'] = df['Volume'] / (df['VolMA20'] + eps)
    
    df['F_C_60'] = df['Close'] / (df['MA60'] + eps)
    df['F_H_60'] = df['High'] / (df['MA60'] + eps)
    df['F_L_60'] = df['Low'] / (df['MA60'] + eps)
    df['F_V_60'] = df['Volume'] / (df['VolMA60'] + eps)

    # Prepare Input (Last 60 days)
    feature_cols = [
        'F_C_5', 'F_H_5', 'F_L_5', 'F_V_5',
        'F_C_20', 'F_H_20', 'F_L_20', 'F_V_20',
        'F_C_60', 'F_H_60', 'F_L_60', 'F_V_60'
    ]
    
    last_60 = df.iloc[-60:]
    norm_input = last_60[feature_cols].values.astype(np.float32)
    
    # Base MA5 for simple info
    base_ma5 = df.iloc[-1]['MA5']
    
    tensor_input = torch.tensor(norm_input, dtype=torch.float32).unsqueeze(0).to(DEVICE) # (1, 60, 12)
    
    print("Running inference...")
    with torch.no_grad():
        logits = model(tensor_input)
        outputs = torch.sigmoid(logits) # Apply sigmoid manually since model returns logits now
        
    # Output shape: (1, 60, 100)
    probs_np = outputs.squeeze(0).cpu().numpy() # (60, 100)
    
    # Visualization
    print("Generating heatmap...")
    plt.figure(figsize=(12, 8))
    # Transpose to have Price on Y axis (0 at bottom, 100 at top)
    # We want index 0 (price 0) at result bottom. imshow origin='lower' handles indices increasing upwards.
    plt.imshow(probs_np.T, aspect='auto', origin='lower', cmap='viridis', extent=[1, 60, 0, 5])
    plt.colorbar(label='Cumulative Probability')
    plt.title(f"Predicted Price Probability for {ticker} (Next 60 Days)\nNormalized by Current MA5: {base_ma5:.2f}")
    plt.xlabel("Days into Future")
    plt.ylabel("Normalized Price (x Current MA5)")
    plt.grid(alpha=0.3)
    
    output_file = f"{ticker}_prediction.png"
    plt.savefig(output_file)
    print(f"Prediction saved to {output_file}")
    
if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--ticker", type=str, default="AAPL", help="Ticker symbol to predict")
    args = parser.parse_args()
    predict(args.ticker)
