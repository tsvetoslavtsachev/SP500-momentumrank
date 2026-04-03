#!/usr/bin/env python3
"""
MomentumRank — fetch_data.py
Изтегля S&P 500 цени от Yahoo Finance, изчислява momentum scores, записва data.json
"""

import json
import math
import time
import warnings
from datetime import datetime, timedelta

import numpy as np
import pandas as pd
import yfinance as yf

warnings.filterwarnings("ignore")

# ─── 1. Вземи S&P 500 тикъри от Wikipedia ─────────────────────────────────────
def get_sp500_tickers():
    url = "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies"
    tables = pd.read_html(url)
    df = tables[0]
    df.columns = [c.strip() for c in df.columns]
    # Normalize column names
    sym_col = [c for c in df.columns if "Symbol" in c or "Ticker" in c][0]
    name_col = [c for c in df.columns if "Security" in c or "Name" in c][0]
    sect_col = [c for c in df.columns if "GICS Sector" in c or "Sector" in c][0]
    result = []
    for _, row in df.iterrows():
        ticker = str(row[sym_col]).strip().replace(".", "-")
        result.append({
            "symbol": ticker,
            "name": str(row[name_col]).strip(),
            "sector": str(row[sect_col]).strip(),
        })
    return result

# ─── 2. Изчисли momentum метрики ──────────────────────────────────────────────
def calc_return(prices, days):
    if len(prices) < 2:
        return None
    end = prices.iloc[-1]
    # Find closest index ~days ago
    target = prices.index[-1] - timedelta(days=days)
    past = prices[prices.index <= target]
    if past.empty:
        return None
    start = past.iloc[-1]
    if start == 0:
        return None
    return (end / start - 1) * 100

def percentile_ranks(scores):
    n = len(scores)
    sorted_idx = sorted(range(n), key=lambda i: scores[i] if scores[i] is not None else -1e9)
    ranks = [0.0] * n
    for rank, idx in enumerate(sorted_idx):
        ranks[idx] = round((rank / (n - 1)) * 100, 1) if n > 1 else 50.0
    return ranks

# ─── 3. Main ──────────────────────────────────────────────────────────────────
def main():
    print("Fetching S&P 500 list from Wikipedia...")
    sp500 = get_sp500_tickers()
    tickers = [s["symbol"] for s in sp500]
    meta = {s["symbol"]: s for s in sp500}
    print(f"Found {len(tickers)} tickers")

    # Download 13 months of price data in batches
    end_date = datetime.today()
    start_date = end_date - timedelta(days=400)
    all_close = {}

    batch_size = 50
    for i in range(0, len(tickers), batch_size):
        batch = tickers[i:i+batch_size]
        print(f"  Downloading batch {i//batch_size + 1}/{math.ceil(len(tickers)/batch_size)}...")
        try:
            raw = yf.download(
                batch,
                start=start_date.strftime("%Y-%m-%d"),
                end=end_date.strftime("%Y-%m-%d"),
                auto_adjust=True,
                progress=False,
                threads=True,
            )
            if isinstance(raw.columns, pd.MultiIndex):
                close = raw["Close"]
            else:
                close = raw[["Close"]]
            for t in batch:
                if t in close.columns:
                    series = close[t].dropna()
                    if len(series) > 20:
                        all_close[t] = series
        except Exception as e:
            print(f"  Batch error: {e}")
        time.sleep(1)

    print(f"Got price data for {len(all_close)} tickers")

    # Calculate metrics
    results = []
    raw_scores = []

    for t, prices in all_close.items():
        try:
            r12 = calc_return(prices, 365)
            r6  = calc_return(prices, 182)
            r3  = calc_return(prices, 91)
            r1  = calc_return(prices, 30)

            if r12 is None or r6 is None:
                continue

            # Daily returns for volatility / Sharpe
            daily = prices.pct_change().dropna()
            vol = daily.std() * math.sqrt(252) * 100  # annualized %
            mean_ret = daily.mean() * 252 * 100
            sharpe = (mean_ret / vol) if vol > 0 else 0

            if r3 is None: r3 = 0.0
            if r1 is None: r1 = 0.0

            weighted = 0.40*r12 + 0.30*r6 + 0.20*r3 + 0.10*r1
            raw_score = (weighted / vol + 0.3 * sharpe) if vol > 0 else 0

            price = round(float(prices.iloc[-1]), 2)
            info = meta.get(t, {"name": t, "sector": "Unknown"})

            results.append({
                "symbol": t,
                "name": info["name"],
                "sector": info["sector"],
                "price": price,
                "return12m": round(r12, 2),
                "return6m":  round(r6, 2),
                "return3m":  round(r3, 2),
                "return1m":  round(r1, 2),
                "volatility": round(vol, 2),
                "sharpe": round(sharpe, 2),
                "_raw_score": raw_score,
            })
            raw_scores.append(raw_score)
        except Exception as e:
            pass

    # Percentile ranks → momentumScore
    ranks = percentile_ranks(raw_scores)
    for i, row in enumerate(results):
        row["momentumScore"] = ranks[i]
        del row["_raw_score"]

    # Sort by momentumScore descending
    results.sort(key=lambda x: x["momentumScore"], reverse=True)

    # Assign rank
    for i, row in enumerate(results):
        row["rank"] = i + 1

    output = {
        "updated": datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ"),
        "count": len(results),
        "data": results,
    }

    with open("data.json", "w", encoding="utf-8") as f:
        json.dump(output, f, ensure_ascii=False, separators=(",", ":"))

    print(f"\n✅ data.json written with {len(results)} stocks")
    print(f"   Updated: {output['updated']}")

if __name__ == "__main__":
    main()
