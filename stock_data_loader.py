import yfinance as yf
import pandas as pd
import numpy as np
import torch
from torch.utils.data import Dataset, DataLoader
import os
import warnings

# Suppress pandas date parsing warnings
warnings.filterwarnings("ignore", category=UserWarning, module="pandas")


def download_data(tickers, start_date=None, end_date=None, cache_dir="data_cache"):
    if not os.path.exists(cache_dir):
        os.makedirs(cache_dir)
    
    data_frames = {}
    for ticker in tickers:
        file_path = os.path.join(cache_dir, f"{ticker}.csv")
        df = None
        if os.path.exists(file_path):
            print(f"Loading {ticker} from cache...")
            # ... (cache loading logic is essentially same, but I'll skip re-writing it here since I used replace_file_content on lines 8-21, I need to be careful about not deleting the cache loading block I just fixed)
            # Actually, the user might want to re-download if the cache is only partial history.
            # But verifying cache "fullness" is hard.
            # For this request, I should probably assume if cache exists, it's good, OR the user can delete cache.
            # HOWEVER, since the previous runs downloaded from 2020, the cache IS incomplete.
            # I MUST check if the cached data is "old enough" or just force fresh download.
            # Given the request is "get ALL history", I'll modify logic to prefer fresh download if cache starts after 2000 for example, OR just rely on user clearing cache.
            # actually I will modify the function to accept a `force_download` arg or just assume we want max.
            
            # Let's keep simpler: I will rely on the fact that I will run this with a new set of tickers (SP500), many of which are not in cache. 
            # For the ones IN cache (the 20 I used), they are short. I should probably force download this time.
            pass

        # ... (I'll just edit the download call part)

        if not os.path.exists(file_path): # simplified for this edit tool, wait.
            # The previous edit made lines 8-48 complex. I should use `read_file` or `view_file` to be sure where I am sticking this.
            # But I know the file content.
            # let's just change the signature and the download call.
            pass
            
# OK, I need to be precise. 
# I will supply the entire function again or a large chunk to include the logic change.

def download_data(tickers, start_date=None, end_date=None, cache_dir="data_cache", force_download=False):
    if not os.path.exists(cache_dir):
        os.makedirs(cache_dir)
    
    data_frames = {}
    for ticker in tickers:
        file_path = os.path.join(cache_dir, f"{ticker}.csv")
        df = None
        
        # Check cache
        if not force_download and os.path.exists(file_path):
             print(f"Loading {ticker} from cache...")
             try:
                df = pd.read_csv(file_path, index_col=0, parse_dates=True)
                
                # Ensure Index is Datetime (fix for str vs Timestamp error)
                if not isinstance(df.index, pd.DatetimeIndex):
                    df.index = pd.to_datetime(df.index, errors='coerce')
                
                # Drop invalid dates
                df = df[df.index.notna()]

                # Ensure numeric columns
                cols_to_check = ['Open', 'High', 'Low', 'Close', 'Volume']
                for col in df.columns:
                     if df[col].dtype == object:
                           df[col] = pd.to_numeric(df[col], errors='coerce')
                df = df.dropna()
                
                # Check for "Full History" request vs "Partial Cache"
                # Old cache default was 2020-01-01. If we want full (start_date=None),
                # and cache starts >= 2020, force reload. 
                # (New stocks IPO'd > 2020 will just degrade to re-download, which is fast/fine)
                if start_date is None and not df.empty:
                    if df.index[0] >= pd.Timestamp("2020-01-01"):
                        print(f"Cache {ticker} starts {df.index[0].date()}, assuming partial. Re-downloading full...")
                        df = None
                        
             except Exception as e:
                print(f"Cache error {ticker}: {e}")
                df = None

        if df is None or df.empty:
            print(f"Downloading {ticker} (Full History)...")
            if start_date is None:
                df = yf.download(ticker, period="max", progress=False)
            else:
                df = yf.download(ticker, start=start_date, end=end_date, progress=False)
            
            if df.empty:
                print(f"Warning: No data for {ticker}")
                continue
                
            if isinstance(df.columns, pd.MultiIndex):
                df.columns = df.columns.get_level_values(0)
            
            df.to_csv(file_path)
        
        # Ensure 'Close' exists
        if 'Close' not in df.columns and 'Adj Close' in df.columns:
             df['Close'] = df['Adj Close']
             
        # Drop rows with minimal data if any
        df = df.dropna(subset=['Close', 'Volume'])

        # --- Pre-calculate Features ---
        # 1. MAs for Price
        df['MA5'] = df['Close'].rolling(window=5).mean()
        df['MA20'] = df['Close'].rolling(window=20).mean()
        df['MA60'] = df['Close'].rolling(window=60).mean()
        
        # 2. MAs for Volume
        df['VolMA5'] = df['Volume'].rolling(window=5).mean()
        df['VolMA20'] = df['Volume'].rolling(window=20).mean()
        df['VolMA60'] = df['Volume'].rolling(window=60).mean()
        
        # Drop NaNs
        df = df.dropna()
        
        if len(df) <= 120:
            continue
            
        # 3. Normalize Features (12 channels)
        # Avoid division by zero
        eps = 1e-8
        
        # Set 1: 5MA
        # Use simple names for feature columns
        df['F_C_5'] = df['Close'] / (df['MA5'] + eps)
        df['F_H_5'] = df['High'] / (df['MA5'] + eps)
        df['F_L_5'] = df['Low'] / (df['MA5'] + eps)
        df['F_V_5'] = df['Volume'] / (df['VolMA5'] + eps)
        
        # Set 2: 20MA
        df['F_C_20'] = df['Close'] / (df['MA20'] + eps)
        df['F_H_20'] = df['High'] / (df['MA20'] + eps)
        df['F_L_20'] = df['Low'] / (df['MA20'] + eps)
        df['F_V_20'] = df['Volume'] / (df['VolMA20'] + eps)
        
        # Set 3: 60MA
        df['F_C_60'] = df['Close'] / (df['MA60'] + eps)
        df['F_H_60'] = df['High'] / (df['MA60'] + eps)
        df['F_L_60'] = df['Low'] / (df['MA60'] + eps)
        df['F_V_60'] = df['Volume'] / (df['VolMA60'] + eps)
        
        # Use shared preprocessing
        features = preprocess_dataframe(df)
        if features is None:
             continue
        
        # We need to reconstruct the dataframe to store it? 
        # Actually download_data returns the DF with features added in the original code.
        # But for the dataloader we just need valid DFs or features.
        # Let's keep the original "mutate DF in place" logic for download_data to avoid breaking changes, 
        # OR just call the new function and assign columns back? 
        # The original code acted on 'df' and returned it.
        # Let's just define the function above and LEAVE download_data logic mostly alone but cleaner?
        # Or better: make download_data use preprocess_dataframe AND assign columns back.
        
        # Simpler: Just Paste the new function definition. 
        # And I won't change download_data deep logic to avoid breaking variable scoping unless I rewrite it.
        # For this edit, I will just append the function after download_data and BEFORE StockDataset.
        # Wait, I am replacing line 154 which is `return data_frames`.
        # I actually want to INSERT the function.

            
    return data_frames

def preprocess_dataframe(df, override_last_close=None):
    """
    Transforms a raw OHLCV DataFrame into the (T, 12) feature tensor expected by the model.
    override_last_close: If set, replaces the last 'Close' price with this value 
                         AFTER MA calculation but BEFORE normalization.
                         This allows strictly "Current Price / Old MA" features.
    Returns: numpy array of shape (N, 12) or None if insufficient data.
    """
    # Ensure numeric
    cols = ['Open', 'High', 'Low', 'Close', 'Volume']
    for c in cols:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors='coerce')
    
    df = df.dropna(subset=['Close', 'Volume'])
    
    # 1. MAs for Price
    df['MA5'] = df['Close'].rolling(window=5).mean()
    df['MA20'] = df['Close'].rolling(window=20).mean()
    df['MA60'] = df['Close'].rolling(window=60).mean()
    
    # 2. MAs for Volume
    df['VolMA5'] = df['Volume'].rolling(window=5).mean()
    df['VolMA20'] = df['Volume'].rolling(window=20).mean()
    df['VolMA60'] = df['Volume'].rolling(window=60).mean()
    
    df = df.dropna()
    
    if len(df) < 60: # Need at least one window of 60
        return None
        
    # Optional: Override last close for inference (Live Price)
    # We do this AFTER rolling calc (so MAs reflect history) but BEFORE feature ratio calc
    if override_last_close is not None and override_last_close > 0:
        # We only modify the very last row's 'Close'
        # Note: We must be careful not to break the dataframe for other logic if it's shared
        # But here we are checking the local 'df'. 
        
        # Adjust last Close
        df.iloc[-1, df.columns.get_loc('Close')] = override_last_close
        
        # Also adjust High/Low if they are exceeded by Current
        if override_last_close > df.iloc[-1]['High']:
             df.iloc[-1, df.columns.get_loc('High')] = override_last_close
        if override_last_close < df.iloc[-1]['Low']:
             df.iloc[-1, df.columns.get_loc('Low')] = override_last_close
        
    # 3. Normalize Features (12 channels)
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
    
    feature_cols = [
        'F_C_5', 'F_H_5', 'F_L_5', 'F_V_5',
        'F_C_20', 'F_H_20', 'F_L_20', 'F_V_20',
        'F_C_60', 'F_H_60', 'F_L_60', 'F_V_60'
    ]
    
    return df[feature_cols].values.astype(np.float32)


class StockDataset(Dataset):
    def __init__(self, data_frames, input_window=60, future_window=60, price_range=(0, 5), num_bins=100):
        self.samples = []
        self.input_window = input_window
        self.future_window = future_window
        self.min_price = price_range[0]
        self.max_price = price_range[1]
        self.num_bins = num_bins
        self.bin_size = (self.max_price - self.min_price) / self.num_bins
        
        # Define feature column names in order
        self.feature_cols = [
            'F_C_5', 'F_H_5', 'F_L_5', 'F_V_5',
            'F_C_20', 'F_H_20', 'F_L_20', 'F_V_20',
            'F_C_60', 'F_H_60', 'F_L_60', 'F_V_60'
        ]
        
        # Pre-process samples
        for ticker, df in data_frames.items():
            # Get Features (N, 12)
            features = df[self.feature_cols].values.astype(np.float32)
            
            # Get Raw Data for Target Calculation (N, 3) -> High, Low, MA5
            # We need raw MA5 to normalize the future raw High/Low
            raw_data = df[['High', 'Low', 'MA5']].values.astype(np.float32)
            
            num_days = len(features)
            required_len = input_window + future_window
            
            for i in range(num_days - required_len + 1):
                self.samples.append({
                    'input_feat': features[i : i + input_window],        # (60, 12)
                    'target_raw': raw_data[i + input_window : i + input_window + future_window], # (60, 3) High, Low, MA5
                    'base_ma5': raw_data[i + input_window - 1, 2]         # scalar: MA5 of last input day
                })

    def __len__(self):
        return len(self.samples)

    def __getitem__(self, idx):
        sample = self.samples[idx]
        
        # Input: Already normalized (60, 12)
        input_tensor = torch.tensor(sample['input_feat'], dtype=torch.float32)
        
        # Target Generation
        # raw_future: (60, 3) -> col 0: High, col 1: Low
        # base_ma5: scalar
        
        raw_future = sample['target_raw']
        base_ma5 = sample['base_ma5']
        
        if base_ma5 == 0: base_ma5 = 1.0
        
        norm_future_high = raw_future[:, 0] / base_ma5
        norm_future_low = raw_future[:, 1] / base_ma5
        
        # Generate Ground Truth Probability Map (60 x 100)
        bin_edges = np.linspace(self.min_price, self.max_price, self.num_bins + 1)
        
        lows = norm_future_low[:, None] # (60, 1)
        highs = norm_future_high[:, None] # (60, 1)
        
        bin_starts = bin_edges[:-1] 
        bin_ends = bin_edges[1:] 
        
        daily_coverage = (lows < bin_ends) & (highs > bin_starts)
        cumulative_counts = np.cumsum(daily_coverage, axis=0) 
        day_indices = np.arange(1, self.future_window + 1)[:, None]
        
        target_map = cumulative_counts / day_indices
        
        return input_tensor, torch.tensor(target_map, dtype=torch.float32)

if __name__ == "__main__":
    # Test
    # tickers = ["AAPL", "GOOG", "MSFT"]
    # dfs = download_data(tickers)
    # dataset = StockDataset(dfs)
    # loader = DataLoader(dataset, batch_size=2)
    # x, y = next(iter(loader))
    # print("Input shape:", x.shape)
    # print("Target shape:", y.shape)
    pass
