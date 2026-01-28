#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
rl_sp500_train_test.py

A complete, production-grade, single-file RL training/testing script for
portfolio allocation over a stock universe loaded from ./data_cache/*.csv.

Key goals:
- Robust CSV loader:
  * Accepts both Yahoo Finance styles:
    - Date, Open, High, Low, Close, Adj Close, Volume
    - Date, Open, High, Low, Close, Volume
  * Automatically uses Adj Close if present; otherwise falls back to Close.
  * Handles column case, spaces, and common variants.
- Universe selection by min_history_days, with clear diagnostics.
- Feature engineering (returns, vol, momentum, moving averages, z-scores, etc.)
- A Gymnasium-compatible environment for portfolio allocation with:
  * Long-only or long-short
  * Optional leverage cap
  * Transaction cost and slippage
  * Reward as log-utility with optional risk penalties
- Training with Stable-Baselines3 PPO
- Deterministic evaluation on a test split with report and optional CSV export.

Usage example:
  python rl_sp500_train_test.py \
    --mode train \
    --data_dir ./data_cache \
    --total_timesteps 800000 \
    --model_path sp500_kelly_ppo.zip

  python rl_sp500_train_test.py \
    --mode test \
    --data_dir ./data_cache \
    --model_path sp500_kelly_ppo.zip

Requirements:
  pip install pandas numpy gymnasium stable-baselines3 torch matplotlib

Notes:
- This file is self-contained and intentionally verbose and explicit.
"""

from __future__ import annotations

import os
import sys
import json
import math
import time
import glob
import argparse
import warnings
from dataclasses import dataclass
from typing import Dict, List, Tuple, Optional, Any

import numpy as np
import pandas as pd

# Gymnasium (preferred) or gym fallback
try:
    import gymnasium as gym
    from gymnasium import spaces
except Exception:
    import gym
    from gym import spaces

# Stable-Baselines3
try:
    from stable_baselines3 import PPO
    from stable_baselines3.common.vec_env import DummyVecEnv
    from stable_baselines3.common.callbacks import BaseCallback
    from stable_baselines3.common.utils import set_random_seed
except Exception as e:
    raise RuntimeError(
        "Missing stable-baselines3. Install: pip install stable-baselines3"
    ) from e


# =========================
# Utilities
# =========================

def _now_str() -> str:
    return time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())

def _log(msg: str) -> None:
    print(msg, flush=True)

def _warn(msg: str) -> None:
    _log(f"[WARN] {msg}")

def _info(msg: str) -> None:
    _log(f"[INFO] {msg}")

def _load_json_if_exists(path: str) -> Optional[dict]:
    if not path or not os.path.exists(path):
        return None
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return None

def _safe_mkdir(path: str) -> None:
    if path and not os.path.exists(path):
        os.makedirs(path, exist_ok=True)

def _as_float32(x: np.ndarray) -> np.ndarray:
    if x.dtype != np.float32:
        return x.astype(np.float32, copy=False)
    return x

def _clip_inf_nan(a: np.ndarray, nan_to: float = 0.0, posinf_to: float = 0.0, neginf_to: float = 0.0) -> np.ndarray:
    a = np.nan_to_num(a, nan=nan_to, posinf=posinf_to, neginf=neginf_to)
    return a

def _softmax(x: np.ndarray, axis: int = -1) -> np.ndarray:
    x = x - np.max(x, axis=axis, keepdims=True)
    e = np.exp(x)
    s = np.sum(e, axis=axis, keepdims=True)
    return e / np.maximum(s, 1e-12)

def _tanh_squash(x: np.ndarray) -> np.ndarray:
    return np.tanh(x)

def _normalize_abs_sum(x: np.ndarray, max_abs_sum: float) -> np.ndarray:
    """
    Scale vector so that sum(abs(x)) <= max_abs_sum.
    """
    s = np.sum(np.abs(x))
    if s <= 1e-12:
        return x
    if s <= max_abs_sum:
        return x
    return x * (max_abs_sum / s)

def _rolling_zscore(s: pd.Series, win: int) -> pd.Series:
    m = s.rolling(win, min_periods=max(3, win // 3)).mean()
    sd = s.rolling(win, min_periods=max(3, win // 3)).std(ddof=0)
    z = (s - m) / sd.replace(0, np.nan)
    return z

def _pct_change(s: pd.Series, periods: int = 1) -> pd.Series:
    return s.pct_change(periods=periods)

def _log_return(s: pd.Series) -> pd.Series:
    return np.log(s).diff()

def _safe_div(a: pd.Series, b: pd.Series) -> pd.Series:
    return a / b.replace(0, np.nan)

def _parse_date_col(df: pd.DataFrame) -> pd.DataFrame:
    """
    Ensure a datetime index named 'date' and sorted.
    """
    if "date" in df.columns:
        df["date"] = pd.to_datetime(df["date"], errors="coerce", utc=False)
        df = df.dropna(subset=["date"])
        df = df.set_index("date")
    else:
        # common variant 'Date'
        if "Date" in df.columns:
            df["Date"] = pd.to_datetime(df["Date"], errors="coerce", utc=False)
            df = df.dropna(subset=["Date"])
            df = df.set_index("Date")
            df.index.name = "date"
        else:
            # sometimes first column is date-like
            first = df.columns[0]
            df[first] = pd.to_datetime(df[first], errors="coerce", utc=False)
            df = df.dropna(subset=[first])
            df = df.set_index(first)
            df.index.name = "date"
    df = df.sort_index()
    return df


# =========================
# CSV Loader & Universe Builder
# =========================

OHLCV_CANON = ["open", "high", "low", "close", "volume"]
ADJ_CLOSE_KEYS = ["adj_close", "adjclose", "adj close", "adjusted_close", "adjusted close", "adjustedclose"]

def canonicalize_columns(cols: List[str]) -> List[str]:
    """
    Normalize column names:
    - strip spaces
    - lower
    - replace multiple spaces with one
    - replace '.' and '-' with '_'
    """
    out = []
    for c in cols:
        c2 = str(c).strip()
        c2 = c2.replace(".", "_").replace("-", "_")
        c2 = " ".join(c2.split())
        c2 = c2.lower()
        out.append(c2)
    return out

def load_single_csv(path: str) -> Optional[pd.DataFrame]:
    """
    Load one CSV and return canonical OHLCV dataframe indexed by date.

    Output columns:
      - open, high, low, close, volume, adj_close (always present; fallback to close)
    """
    try:
        df = pd.read_csv(path)
    except Exception as e:
        _warn(f"skip {path}: read_csv failed: {e}")
        return None

    if df.shape[0] < 10:
        _warn(f"skip {path}: too few rows: {df.shape[0]}")
        return None

    # Preserve original columns for error message
    orig_cols = list(df.columns)

    # Canonicalize
    df.columns = canonicalize_columns(list(df.columns))

    # Ensure date index
    df = _parse_date_col(df)

    # Identify required OHLCV
    present = set(df.columns)

    # Some CSV may store Volume as 'volume' already, good.
    # We require open/high/low/close/volume. Adj close optional.
    missing = [c for c in OHLCV_CANON if c not in present]
    if missing:
        _warn(
            f"skip {path}: missing OHLCV columns {missing}. Found(original): {orig_cols}"
        )
        return None

    # Determine adj_close
    adj_col = None
    for k in ADJ_CLOSE_KEYS:
        if k in present:
            adj_col = k
            break

    if adj_col is None:
        # fall back to close
        df["adj_close"] = df["close"].astype(float)
    else:
        df["adj_close"] = df[adj_col].astype(float)

    # Coerce numeric types
    for c in ["open", "high", "low", "close", "volume", "adj_close"]:
        df[c] = pd.to_numeric(df[c], errors="coerce")

    df = df.dropna(subset=["open", "high", "low", "close", "adj_close", "volume"])
    df = df.sort_index()

    # Remove duplicates
    df = df[~df.index.duplicated(keep="last")]

    if df.shape[0] < 10:
        _warn(f"skip {path}: too few valid rows after cleaning: {df.shape[0]}")
        return None

    return df


@dataclass
class UniverseConfig:
    min_history_days: int = 252 * 5      # default 5 years of daily data
    align_mode: str = "intersection"     # intersection | union_ffill
    max_tickers: int = 0                 # 0 means no cap
    dropna_after_align: bool = True
    verbose: bool = True

def load_universe_from_data_cache(
    data_dir: str,
    min_history_days: int,
    align_mode: str = "intersection",
    max_tickers: int = 0,
    dropna_after_align: bool = True,
    verbose: bool = True,
    # NEW knobs (safe defaults)
    auto_relax_min_history: bool = True,
    auto_fallback_union: bool = False,
    intersection_min_coverage_ratio: float = 0.995,  # allow tiny shortfall
) -> Tuple[Dict[str, pd.DataFrame], List[str]]:
    """
    Read all CSV in data_dir. Each CSV file name is ticker.csv
    Returns:
      big: dict[ticker] -> df with canonical columns and date index
      tickers: list of tickers used

    align_mode:
      - "intersection": align all tickers to common date intersection (no NaNs after align)
      - "union_ffill": align to union of dates and forward-fill price columns per ticker

    NEW behavior:
      - If align_mode='intersection' and the intersection length is slightly shorter than min_history_days,
        we can (1) relax min_history_days down to intersection length (auto_relax_min_history),
        or (2) fallback to union_ffill (auto_fallback_union).
      - intersection_min_coverage_ratio controls how tolerant we are.
        Example: 0.995 means if intersection >= min_history_days*0.995, we accept by relaxing.
    """
    if not os.path.isdir(data_dir):
        raise RuntimeError(f"data_dir not found: {data_dir}")

    paths = sorted(glob.glob(os.path.join(data_dir, "*.csv")))
    if not paths:
        raise RuntimeError(f"No CSV files found in: {data_dir}")

    if verbose:
        _info(f"[LOAD] reading universe from: {data_dir}, csv_count={len(paths)}")

    raw: Dict[str, pd.DataFrame] = {}
    skipped: List[Tuple[str, str]] = []

    for p in paths:
        ticker = os.path.splitext(os.path.basename(p))[0].strip()
        if not ticker:
            skipped.append((p, "empty ticker name"))
            continue

        df = load_single_csv(p)
        if df is None:
            skipped.append((p, "invalid csv"))
            continue

        if df.shape[0] < min_history_days:
            skipped.append((p, f"history days {df.shape[0]} < min_history_days {min_history_days}"))
            continue

        raw[ticker] = df

        if max_tickers and len(raw) >= max_tickers:
            break

    if verbose:
        _info(f"[LOAD] kept={len(raw)}, skipped={len(skipped)}, min_history_days={min_history_days}")
        if len(raw) == 0:
            reason_count: Dict[str, int] = {}
            for _, r in skipped:
                reason_count[r] = reason_count.get(r, 0) + 1
            _warn(f"[LOAD] zero kept tickers. Skip reasons summary: {reason_count}")

    if len(raw) == 0:
        raise RuntimeError("沒有任何 ticker 符合 min_history_days 條件，請降低門檻或確認資料。")

    tickers = sorted(raw.keys())

    if align_mode not in ("intersection", "union_ffill"):
        raise ValueError(f"Unknown align_mode: {align_mode}")

    # ---------- intersection ----------
    if align_mode == "intersection":
        common_idx = None
        for t in tickers:
            idx = raw[t].index
            common_idx = idx if common_idx is None else common_idx.intersection(idx)
        common_idx = common_idx.sort_values()
        inter_len = len(common_idx)

        if verbose:
            _info(f"[ALIGN] intersection_len={inter_len}, requested_min_history_days={min_history_days}")

        if inter_len < min_history_days:
            # tolerance check
            tolerant_threshold = int(math.floor(min_history_days * float(intersection_min_coverage_ratio)))
            if inter_len >= tolerant_threshold and auto_relax_min_history:
                if verbose:
                    _warn(
                        f"[ALIGN] intersection too short by {min_history_days - inter_len} days, "
                        f"but within coverage_ratio={intersection_min_coverage_ratio}. "
                        f"Auto-relax min_history_days: {min_history_days} -> {inter_len}"
                    )
                min_history_days = inter_len
            else:
                if auto_fallback_union:
                    if verbose:
                        _warn(
                            f"[ALIGN] intersection_len={inter_len} < min_history_days={min_history_days}. "
                            f"Auto fallback to union_ffill."
                        )
                    align_mode = "union_ffill"
                else:
                    _warn(
                        f"[ALIGN] intersection dates too short: {inter_len} (min_history_days={min_history_days}). "
                        f"Consider --align_mode union_ffill or lower min_history_days."
                    )
                    # keep going, will likely raise after filtering if none pass

        if align_mode == "intersection":
            big: Dict[str, pd.DataFrame] = {}
            for t in tickers:
                big[t] = raw[t].reindex(common_idx).copy()
            if dropna_after_align:
                for t in tickers:
                    big[t] = big[t].dropna()

            final = {}
            final_tickers = []
            for t in tickers:
                if big[t].shape[0] >= min_history_days:
                    final[t] = big[t]
                    final_tickers.append(t)

            if len(final_tickers) == 0:
                raise RuntimeError(
                    "對齊(intersection)後沒有任何 ticker 符合 min_history_days。"
                    "請改用 --align_mode union_ffill 或降低 min_history_days。"
                )
            return final, final_tickers

    # ---------- union_ffill ----------
    # union_ffill
    union_idx = None
    for t in tickers:
        idx = raw[t].index
        union_idx = idx if union_idx is None else union_idx.union(idx)
    union_idx = union_idx.sort_values()

    big = {}
    for t in tickers:
        df = raw[t].reindex(union_idx).copy()
        for c in ["open", "high", "low", "close", "adj_close"]:
            df[c] = df[c].ffill()
        df["volume"] = df["volume"].fillna(0.0)
        if dropna_after_align:
            df = df.dropna(subset=["adj_close"])
        big[t] = df

    final = {}
    final_tickers = []
    for t in tickers:
        if big[t].shape[0] >= min_history_days:
            final[t] = big[t]
            final_tickers.append(t)

    if len(final_tickers) == 0:
        raise RuntimeError(
            "對齊(union_ffill)後沒有任何 ticker 符合 min_history_days。請降低 min_history_days 或確認資料完整性。"
        )
    if verbose:
        _info(f"[ALIGN] union_ffill_len={len(union_idx)}, final_tickers={len(final_tickers)}")
    return final, final_tickers


# =========================
# Feature Engineering
# =========================

@dataclass
class FeatureConfig:
    # return features windows
    ret_lags: Tuple[int, ...] = (1, 2, 5, 10, 21, 63)
    vol_windows: Tuple[int, ...] = (5, 10, 21, 63)
    ma_windows: Tuple[int, ...] = (5, 10, 21, 50, 100, 200)
    z_windows: Tuple[int, ...] = (21, 63, 126)
    rsi_windows: Tuple[int, ...] = (14, 21)
    include_ohlc_spreads: bool = True
    include_volume_feats: bool = True
    include_intraday_range: bool = True
    clip_abs: float = 25.0


def compute_rsi(close: pd.Series, win: int) -> pd.Series:
    delta = close.diff()
    up = delta.clip(lower=0)
    down = (-delta).clip(lower=0)
    roll_up = up.rolling(win, min_periods=max(3, win // 3)).mean()
    roll_down = down.rolling(win, min_periods=max(3, win // 3)).mean()
    rs = roll_up / roll_down.replace(0, np.nan)
    rsi = 100.0 - (100.0 / (1.0 + rs))
    return rsi


def build_features_for_ticker(df: pd.DataFrame, cfg: FeatureConfig) -> pd.DataFrame:
    """
    Input df: columns open/high/low/close/adj_close/volume, index date
    Output: dataframe with many features + "ret_1" as primary.
    """
    out = pd.DataFrame(index=df.index)

    px = df["adj_close"].astype(float)
    close = df["close"].astype(float)
    open_ = df["open"].astype(float)
    high = df["high"].astype(float)
    low = df["low"].astype(float)
    vol = df["volume"].astype(float)

    # Base returns
    out["logret_1"] = _log_return(px)
    out["ret_1"] = _pct_change(px, 1)
    for k in cfg.ret_lags:
        out[f"ret_{k}"] = _pct_change(px, k)
        out[f"logret_{k}"] = np.log(px).diff(k)

    # Volatility (std of daily returns)
    for w in cfg.vol_windows:
        out[f"vol_{w}"] = out["ret_1"].rolling(w, min_periods=max(3, w // 3)).std(ddof=0)

    # Moving averages and ratios
    for w in cfg.ma_windows:
        ma = px.rolling(w, min_periods=max(3, w // 3)).mean()
        out[f"ma_{w}"] = ma
        out[f"px_over_ma_{w}"] = _safe_div(px, ma) - 1.0

    # Z-scores of price
    for w in cfg.z_windows:
        out[f"z_px_{w}"] = _rolling_zscore(px, w)
        out[f"z_ret_{w}"] = _rolling_zscore(out["ret_1"], w)

    # RSI
    for w in cfg.rsi_windows:
        out[f"rsi_{w}"] = compute_rsi(px, w) / 100.0  # normalize 0-1

    # Intraday range / spreads
    if cfg.include_intraday_range:
        out["hl_range"] = _safe_div(high - low, px)
        out["oc_change"] = _safe_div(close - open_, px)
        out["co_change"] = _safe_div(open_ - close.shift(1), px)

    if cfg.include_ohlc_spreads:
        out["close_to_high"] = _safe_div(px - high, px)
        out["close_to_low"] = _safe_div(px - low, px)
        out["open_to_close"] = _safe_div(close - open_, px)

    # Volume features
    if cfg.include_volume_feats:
        out["vol_chg_1"] = _pct_change(vol, 1)
        out["vol_log"] = np.log1p(vol)
        for w in (5, 10, 21, 63):
            vma = vol.rolling(w, min_periods=max(3, w // 3)).mean()
            out[f"vol_over_vma_{w}"] = _safe_div(vol, vma) - 1.0
            out[f"z_vol_{w}"] = _rolling_zscore(vol, w)

    # Clean
    out = out.replace([np.inf, -np.inf], np.nan)
    out = out.fillna(0.0)

    # Clip extreme values
    if cfg.clip_abs and cfg.clip_abs > 0:
        out = out.clip(lower=-cfg.clip_abs, upper=cfg.clip_abs)

    return out


def build_panel_features(
    big: Dict[str, pd.DataFrame],
    tickers: List[str],
    feat_cfg: FeatureConfig,
) -> Tuple[pd.DatetimeIndex, np.ndarray, np.ndarray, List[str]]:
    """
    Build a panel tensor:
      features: [T, N, F]
      returns:  [T, N] daily returns (ret_1)

    Also returns feature_names list length F
    """
    # Assume all tickers aligned by loader to comparable index length.
    idx = None
    per_ticker_feat: Dict[str, pd.DataFrame] = {}
    for t in tickers:
        df = big[t]
        if idx is None:
            idx = df.index
        else:
            # safeguard: align
            if not df.index.equals(idx):
                df = df.reindex(idx).copy()
                big[t] = df

        feat = build_features_for_ticker(df, feat_cfg)
        per_ticker_feat[t] = feat

    assert idx is not None
    feature_names = list(per_ticker_feat[tickers[0]].columns)
    F = len(feature_names)
    N = len(tickers)
    T = len(idx)

    feat_tensor = np.zeros((T, N, F), dtype=np.float32)
    ret_mat = np.zeros((T, N), dtype=np.float32)

    for j, t in enumerate(tickers):
        feat_df = per_ticker_feat[t].reindex(idx).copy()
        feat_tensor[:, j, :] = feat_df.values.astype(np.float32, copy=False)

        # primary return: use ret_1 in features (already filled)
        # But we should compute true daily return from adj_close for environment step.
        px = big[t]["adj_close"].astype(float).reindex(idx)
        r1 = px.pct_change(1).replace([np.inf, -np.inf], np.nan).fillna(0.0)
        ret_mat[:, j] = r1.values.astype(np.float32, copy=False)

    return idx, feat_tensor, ret_mat, feature_names


# =========================
# Portfolio Environment
# =========================

@dataclass
class EnvConfig:
    window_size: int = 60
    initial_cash: float = 1.0
    transaction_cost: float = 0.0005      # 5 bps
    slippage: float = 0.0002              # 2 bps
    long_short: bool = True
    max_leverage: float = 1.0             # sum(abs(weights)) <= max_leverage
    include_cash_asset: bool = True       # action includes cash implicitly or explicitly
    reward_mode: str = "log_growth"       # log_growth | sharpe_like
    risk_aversion: float = 0.0            # penalty on portfolio variance (approx)
    drawdown_penalty: float = 0.0         # penalty on drawdown
    turnover_penalty: float = 0.0         # penalty on turnover (in addition to cost)
    normalize_obs: bool = True
    obs_clip: float = 10.0
    seed: int = 42


class PortfolioEnv(gym.Env):
    """
    Observation: a window of features and recent returns
      - shape: [window_size, N, F] flattened to 1D or kept as 2D? We'll flatten.
    Action:
      - If long_short:
          raw action in R^N (or R^(N+1) if cash) -> tanh -> normalized abs sum <= max_leverage
        Cash weight = 1 - sum(weights) if include_cash_asset and long-only, but for long-short we keep cash implicit:
          We interpret weights on risky assets; remainder goes to cash (may be <0 if leveraged),
          but we cap leverage via abs-sum.
      - If long-only:
          raw action -> softmax -> weights sum to 1 across assets (and optional cash)

    Step:
      portfolio_return = sum(w * asset_return) - costs
      portfolio_value *= (1 + portfolio_return)
      reward = log( portfolio_value_{t+1} / portfolio_value_t ) - penalties
    """
    metadata = {"render_modes": []}

    def __init__(
        self,
        dates: pd.DatetimeIndex,
        feat_tensor: np.ndarray,   # [T, N, F]
        ret_mat: np.ndarray,       # [T, N]
        tickers: List[str],
        feature_names: List[str],
        cfg: EnvConfig,
        train: bool = True,
        start_index: int = 0,
        end_index: Optional[int] = None,
    ):
        super().__init__()
        self.dates = dates
        self.X = _as_float32(feat_tensor)
        self.R = _as_float32(ret_mat)
        self.tickers = tickers
        self.feature_names = feature_names
        self.cfg = cfg
        self.train = train

        self.T, self.N, self.F = self.X.shape

        self.start_index = int(start_index)
        self.end_index = int(end_index) if end_index is not None else (self.T - 1)
        self.end_index = min(self.end_index, self.T - 1)

        if self.end_index - self.start_index < (self.cfg.window_size + 5):
            raise RuntimeError(
                f"Env range too short: [{self.start_index}, {self.end_index}] "
                f"with window_size={self.cfg.window_size}"
            )

        # Observation space: flatten window_size * N * F
        obs_dim = self.cfg.window_size * self.N * self.F
        self.observation_space = spaces.Box(
            low=-np.inf, high=np.inf, shape=(obs_dim,), dtype=np.float32
        )

        # Action space
        act_dim = self.N
        self.action_space = spaces.Box(
            low=-1.0, high=1.0, shape=(act_dim,), dtype=np.float32
        )

        # State
        self._rng = np.random.default_rng(self.cfg.seed)
        self._t = None
        self._pv = None
        self._w = None
        self._peak = None
        self._last_reward = None

    def seed(self, seed: Optional[int] = None):
        if seed is not None:
            self.cfg.seed = int(seed)
        self._rng = np.random.default_rng(self.cfg.seed)

    def _get_obs(self) -> np.ndarray:
        t0 = self._t - self.cfg.window_size
        t1 = self._t
        obs = self.X[t0:t1, :, :]  # [W, N, F]

        if self.cfg.normalize_obs:
            # Normalize per-feature across window and assets
            m = obs.mean(axis=(0, 1), keepdims=True)
            s = obs.std(axis=(0, 1), keepdims=True)
            s = np.where(s < 1e-6, 1.0, s)
            obs = (obs - m) / s

        if self.cfg.obs_clip and self.cfg.obs_clip > 0:
            obs = np.clip(obs, -self.cfg.obs_clip, self.cfg.obs_clip)

        flat = obs.reshape(-1).astype(np.float32, copy=False)
        flat = _clip_inf_nan(flat, nan_to=0.0, posinf_to=0.0, neginf_to=0.0).astype(np.float32, copy=False)
        return flat

    def _action_to_weights(self, action: np.ndarray) -> np.ndarray:
        """
        Convert raw action to risky-asset weights of length N.
        """
        a = np.asarray(action, dtype=np.float32).reshape(-1)
        if a.shape[0] != self.N:
            raise ValueError(f"action dim mismatch: got {a.shape[0]} expected {self.N}")

        if self.cfg.long_short:
            # [-1,1] -> tanh, then cap abs-sum
            w = _tanh_squash(a)
            w = _normalize_abs_sum(w, self.cfg.max_leverage)
            return w.astype(np.float32, copy=False)
        else:
            # long-only: softmax => sum=1
            w = _softmax(a, axis=0)
            return w.astype(np.float32, copy=False)

    def reset(self, *, seed: Optional[int] = None, options: Optional[dict] = None):
        if seed is not None:
            self.seed(seed)

        # Choose start t
        if self.train:
            # random start for training
            min_t = self.start_index + self.cfg.window_size
            max_t = self.end_index - 1
            self._t = int(self._rng.integers(min_t, max_t))
        else:
            # deterministic for testing
            self._t = int(self.start_index + self.cfg.window_size)

        self._pv = float(self.cfg.initial_cash)
        self._w = np.zeros((self.N,), dtype=np.float32)  # start from cash
        self._peak = float(self._pv)
        self._last_reward = 0.0

        obs = self._get_obs()
        info = {
            "date": str(self.dates[self._t]),
            "pv": self._pv,
            "weights": self._w.copy(),
        }
        return obs, info

    def step(self, action: np.ndarray):
        # Current date t corresponds to returns at t -> t+1
        w_new = self._action_to_weights(action)
        w_old = self._w

        # transaction cost: proportional to turnover
        turnover = float(np.sum(np.abs(w_new - w_old)))
        cost = turnover * (self.cfg.transaction_cost + self.cfg.slippage)

        # portfolio risky return
        r_vec = self.R[self._t + 1, :]  # next day's return
        port_risky = float(np.dot(w_new, r_vec))

        # cash weight
        # For long_short: cash is residual (1 - sum(w)), can be >1 or <0 in theory,
        # but leverage is already capped by abs sum. We'll model cash return as 0.
        # For long_only: sum(w)=1 => cash=0 by design.
        cash_w = 1.0 - float(np.sum(w_new)) if self.cfg.include_cash_asset else 0.0
        cash_r = 0.0

        port_ret = port_risky + cash_w * cash_r
        port_ret_after_cost = port_ret - cost

        # Update PV
        pv_old = self._pv
        self._pv = self._pv * (1.0 + port_ret_after_cost)

        # Reward
        # log-growth utility:
        if self.cfg.reward_mode == "log_growth":
            # guard against negative pv
            if self._pv <= 0.0:
                reward = -100.0
            else:
                reward = math.log(max(self._pv, 1e-12) / max(pv_old, 1e-12))
        elif self.cfg.reward_mode == "sharpe_like":
            # approximate: reward = port_ret - 0.5 * risk_aversion * port_ret^2
            reward = port_ret_after_cost - 0.5 * self.cfg.risk_aversion * (port_ret_after_cost ** 2)
        else:
            reward = port_ret_after_cost

        # Risk penalty (approx) using cross-sectional variance of returns weighted by abs weights
        if self.cfg.risk_aversion and self.cfg.risk_aversion > 0 and self.cfg.reward_mode == "log_growth":
            # proxy risk: weighted variance of r_vec
            rw = np.abs(w_new)
            if float(np.sum(rw)) > 1e-12:
                rw = rw / float(np.sum(rw))
            var = float(np.sum(rw * (r_vec - float(np.dot(rw, r_vec))) ** 2))
            reward -= self.cfg.risk_aversion * var

        # Drawdown penalty
        if self.cfg.drawdown_penalty and self.cfg.drawdown_penalty > 0:
            self._peak = max(self._peak, self._pv)
            dd = 1.0 - (self._pv / max(self._peak, 1e-12))
            reward -= self.cfg.drawdown_penalty * dd

        # Turnover penalty (extra)
        if self.cfg.turnover_penalty and self.cfg.turnover_penalty > 0:
            reward -= self.cfg.turnover_penalty * turnover

        self._last_reward = float(reward)
        self._w = w_new

        # Advance time
        self._t += 1
        terminated = False
        truncated = False

        if self._t >= (self.end_index - 1):
            truncated = True

        obs = self._get_obs()
        info = {
            "date": str(self.dates[self._t]),
            "pv": float(self._pv),
            "pv_old": float(pv_old),
            "port_ret": float(port_ret),
            "port_ret_after_cost": float(port_ret_after_cost),
            "turnover": float(turnover),
            "cost": float(cost),
            "weights": self._w.copy(),
            "cash_w": float(cash_w),
            "reward": float(reward),
        }
        return obs, float(reward), terminated, truncated, info


# =========================
# Training / Testing
# =========================

class PrintCallback(BaseCallback):
    def __init__(self, print_freq: int = 2000):
        super().__init__()
        self.print_freq = int(print_freq)
        self._last_print = 0

    def _on_step(self) -> bool:
        n = self.num_timesteps
        if n - self._last_print >= self.print_freq:
            self._last_print = n
            _info(f"[TRAIN] timesteps={n}")
        return True


def make_env_fn(
    dates: pd.DatetimeIndex,
    feat_tensor: np.ndarray,
    ret_mat: np.ndarray,
    tickers: List[str],
    feature_names: List[str],
    cfg: EnvConfig,
    train: bool,
    start_index: int,
    end_index: int,
):
    def _fn():
        return PortfolioEnv(
            dates=dates,
            feat_tensor=feat_tensor,
            ret_mat=ret_mat,
            tickers=tickers,
            feature_names=feature_names,
            cfg=cfg,
            train=train,
            start_index=start_index,
            end_index=end_index,
        )
    return _fn


def run_train(
    dates: pd.DatetimeIndex,
    feat_tensor: np.ndarray,
    ret_mat: np.ndarray,
    tickers: List[str],
    feature_names: List[str],
    args: argparse.Namespace,
) -> None:
    _info(f"[MODE] train, start={_now_str()}")

    # Split
    T = len(dates)
    split_ratio = float(args.train_ratio)
    split_idx = int(T * split_ratio)
    split_idx = min(max(split_idx, args.window_size + 10), T - 10)

    train_start = 0
    train_end = split_idx - 1
    test_start = split_idx
    test_end = T - 1

    _info(f"[SPLIT] T={T}, train=[{train_start},{train_end}], test=[{test_start},{test_end}]")
    _info(f"[SPLIT] train_start_date={dates[train_start]}, train_end_date={dates[train_end]}")
    _info(f"[SPLIT] test_start_date={dates[test_start]}, test_end_date={dates[test_end]}")

    env_cfg = EnvConfig(
        window_size=int(args.window_size),
        initial_cash=1.0,
        transaction_cost=float(args.transaction_cost),
        slippage=float(args.slippage),
        long_short=bool(args.long_short),
        max_leverage=float(args.max_leverage),
        include_cash_asset=True,
        reward_mode=str(args.reward_mode),
        risk_aversion=float(args.risk_aversion),
        drawdown_penalty=float(args.drawdown_penalty),
        turnover_penalty=float(args.turnover_penalty),
        normalize_obs=True,
        obs_clip=float(args.obs_clip),
        seed=int(args.seed),
    )

    set_random_seed(int(args.seed))

    train_env = DummyVecEnv([make_env_fn(
        dates, feat_tensor, ret_mat, tickers, feature_names,
        env_cfg, True, train_start, train_end
    )])

    policy_kwargs = dict(
        net_arch=[dict(pi=[256, 256, 128], vf=[256, 256, 128])]
    )

    model = PPO(
        policy="MlpPolicy",
        env=train_env,
        learning_rate=float(args.learning_rate),
        n_steps=int(args.n_steps),
        batch_size=int(args.batch_size),
        n_epochs=int(args.n_epochs),
        gamma=float(args.gamma),
        gae_lambda=float(args.gae_lambda),
        clip_range=float(args.clip_range),
        ent_coef=float(args.ent_coef),
        vf_coef=float(args.vf_coef),
        max_grad_norm=float(args.max_grad_norm),
        tensorboard_log=args.tensorboard_log if args.tensorboard_log else None,
        verbose=1,
        seed=int(args.seed),
        policy_kwargs=policy_kwargs,
    )

    callback = PrintCallback(print_freq=int(args.print_freq))

    _info(f"[TRAIN] total_timesteps={args.total_timesteps}")
    model.learn(total_timesteps=int(args.total_timesteps), callback=callback)

    # Save model + meta
    model_path = args.model_path
    model.save(model_path)
    _info(f"[SAVE] model saved: {model_path}")

    meta = {
        "tickers": tickers,
        "feature_names": feature_names,
        "train_ratio": split_ratio,
        "split_idx": split_idx,
        "dates_range": [str(dates[0]), str(dates[-1])],
        "env_cfg": env_cfg.__dict__,
        "args": vars(args),
    }
    meta_path = os.path.splitext(model_path)[0] + ".meta.json"
    with open(meta_path, "w", encoding="utf-8") as f:
        json.dump(meta, f, ensure_ascii=False, indent=2)
    _info(f"[SAVE] meta saved: {meta_path}")

    # Optional quick evaluation after training
    if args.eval_after_train:
        _info("[EVAL] running quick eval on test split ...")
        run_test(dates, feat_tensor, ret_mat, tickers, feature_names, args, split_idx=split_idx)


def run_test(
    dates: pd.DatetimeIndex,
    feat_tensor: np.ndarray,
    ret_mat: np.ndarray,
    tickers: List[str],
    feature_names: List[str],
    args: argparse.Namespace,
    split_idx: Optional[int] = None,
) -> None:
    _info(f"[MODE] test, start={_now_str()}")

    model_path = args.model_path
    if not os.path.exists(model_path):
        raise RuntimeError(f"model_path not found: {model_path}")

    # Load meta if exists
    meta_path = os.path.splitext(model_path)[0] + ".meta.json"
    meta = _load_json_if_exists(meta_path)

    T = len(dates)
    if split_idx is None:
        if meta and "split_idx" in meta:
            split_idx = int(meta["split_idx"])
        else:
            split_ratio = float(args.train_ratio)
            split_idx = int(T * split_ratio)

    split_idx = min(max(split_idx, args.window_size + 10), T - 10)

    test_start = split_idx
    test_end = T - 1

    _info(f"[SPLIT] T={T}, test=[{test_start},{test_end}]")
    _info(f"[SPLIT] test_start_date={dates[test_start]}, test_end_date={dates[test_end]}")

    env_cfg = EnvConfig(
        window_size=int(args.window_size),
        initial_cash=1.0,
        transaction_cost=float(args.transaction_cost),
        slippage=float(args.slippage),
        long_short=bool(args.long_short),
        max_leverage=float(args.max_leverage),
        include_cash_asset=True,
        reward_mode=str(args.reward_mode),
        risk_aversion=float(args.risk_aversion),
        drawdown_penalty=float(args.drawdown_penalty),
        turnover_penalty=float(args.turnover_penalty),
        normalize_obs=True,
        obs_clip=float(args.obs_clip),
        seed=int(args.seed),
    )

    test_env = PortfolioEnv(
        dates=dates,
        feat_tensor=feat_tensor,
        ret_mat=ret_mat,
        tickers=tickers,
        feature_names=feature_names,
        cfg=env_cfg,
        train=False,
        start_index=test_start,
        end_index=test_end,
    )

    model = PPO.load(model_path)

    obs, info = test_env.reset(seed=int(args.seed))
    done = False

    rows = []
    pv0 = info["pv"]
    peak = pv0

    while True:
        action, _ = model.predict(obs, deterministic=True)
        obs, reward, terminated, truncated, info = test_env.step(action)

        pv = info["pv"]
        peak = max(peak, pv)
        dd = 1.0 - (pv / max(peak, 1e-12))

        rows.append({
            "date": info["date"],
            "pv": pv,
            "port_ret": info["port_ret"],
            "port_ret_after_cost": info["port_ret_after_cost"],
            "turnover": info["turnover"],
            "cost": info["cost"],
            "cash_w": info["cash_w"],
            "reward": info["reward"],
            "drawdown": dd,
            "weights": json.dumps(info["weights"].tolist()),
        })

        if truncated or terminated:
            break

    df = pd.DataFrame(rows)
    if df.shape[0] == 0:
        _warn("[TEST] no rows produced.")
        return

    # Summary metrics
    pv_end = float(df["pv"].iloc[-1])
    total_return = (pv_end / max(pv0, 1e-12)) - 1.0

    daily_ret = df["port_ret_after_cost"].astype(float).values
    mean_r = float(np.mean(daily_ret))
    std_r = float(np.std(daily_ret))
    sharpe = (mean_r / max(std_r, 1e-12)) * math.sqrt(252.0)

    max_dd = float(df["drawdown"].max())
    avg_turnover = float(df["turnover"].mean())
    avg_cost = float(df["cost"].mean())

    _info(f"[TEST] pv0={pv0:.6f} pv_end={pv_end:.6f} total_return={total_return*100:.2f}%")
    _info(f"[TEST] mean_daily_ret={mean_r:.6f} std_daily_ret={std_r:.6f} sharpe~={sharpe:.3f}")
    _info(f"[TEST] max_drawdown={max_dd*100:.2f}% avg_turnover={avg_turnover:.6f} avg_cost={avg_cost:.8f}")

    # Export
    if args.test_report_path:
        out_path = args.test_report_path
    else:
        out_path = os.path.splitext(model_path)[0] + ".test_report.csv"

    df.to_csv(out_path, index=False)
    _info(f"[SAVE] test report saved: {out_path}")


# =========================
# Main
# =========================

def build_arg_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser()

    p.add_argument("--mode", type=str, choices=["train", "test"], required=True)

    # Data
    p.add_argument("--data_dir", type=str, default="./data_cache")
    p.add_argument("--min_history_days", type=int, default=252 * 5)
    p.add_argument("--align_mode", type=str, default="intersection", choices=["intersection", "union_ffill"])
    p.add_argument("--max_tickers", type=int, default=0)

    # Model
    p.add_argument("--model_path", type=str, default="sp500_kelly_ppo.zip")
    p.add_argument("--total_timesteps", type=int, default=800000)

    # Split / window
    p.add_argument("--train_ratio", type=float, default=0.8)
    p.add_argument("--window_size", type=int, default=60)

    # PPO hyperparams
    p.add_argument("--learning_rate", type=float, default=3e-4)
    p.add_argument("--n_steps", type=int, default=2048)
    p.add_argument("--batch_size", type=int, default=256)
    p.add_argument("--n_epochs", type=int, default=10)
    p.add_argument("--gamma", type=float, default=0.99)
    p.add_argument("--gae_lambda", type=float, default=0.95)
    p.add_argument("--clip_range", type=float, default=0.2)
    p.add_argument("--ent_coef", type=float, default=0.0)
    p.add_argument("--vf_coef", type=float, default=0.5)
    p.add_argument("--max_grad_norm", type=float, default=0.5)

    # Env / reward
    p.add_argument("--transaction_cost", type=float, default=0.0005)
    p.add_argument("--slippage", type=float, default=0.0002)
    p.add_argument("--long_short", action="store_true", default=True)
    p.add_argument("--max_leverage", type=float, default=1.0)

    p.add_argument("--reward_mode", type=str, default="log_growth", choices=["log_growth", "sharpe_like", "raw"])
    p.add_argument("--risk_aversion", type=float, default=0.0)
    p.add_argument("--drawdown_penalty", type=float, default=0.0)
    p.add_argument("--turnover_penalty", type=float, default=0.0)

    # Obs
    p.add_argument("--obs_clip", type=float, default=10.0)

    # Misc
    p.add_argument("--seed", type=int, default=42)
    p.add_argument("--print_freq", type=int, default=2000)
    p.add_argument("--tensorboard_log", type=str, default="")
    p.add_argument("--eval_after_train", action="store_true", default=False)

    # Test outputs
    p.add_argument("--test_report_path", type=str, default="")

    return p


def main():
    args = build_arg_parser().parse_args()

    _info(f"[ARGS] {json.dumps(vars(args), ensure_ascii=False)}")

    # Load universe
    big, tickers = load_universe_from_data_cache(
        data_dir=args.data_dir,
        min_history_days=int(args.min_history_days),
        align_mode=str(args.align_mode),
        max_tickers=int(args.max_tickers),
        dropna_after_align=True,
        verbose=True,
    )

    _info(f"[UNIVERSE] tickers_used={len(tickers)}")
    _info(f"[UNIVERSE] sample={tickers[:10]}")

    # Build features
    feat_cfg = FeatureConfig()
    dates, feat_tensor, ret_mat, feature_names = build_panel_features(
        big=big,
        tickers=tickers,
        feat_cfg=feat_cfg,
    )

    _info(f"[FEATURE] dates={len(dates)} tickers={len(tickers)} features={len(feature_names)}")
    _info(f"[FEATURE] first_date={dates[0]} last_date={dates[-1]}")
    _info(f"[FEATURE] feature_names={feature_names}")

    # Run
    if args.mode == "train":
        run_train(dates, feat_tensor, ret_mat, tickers, feature_names, args)
    else:
        run_test(dates, feat_tensor, ret_mat, tickers, feature_names, args)

    _info(f"[DONE] end={_now_str()}")


if __name__ == "__main__":
    main()
