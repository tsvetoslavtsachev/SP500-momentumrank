#!/usr/bin/env python3
"""
MomentumRank — fetch_data.py
Outputs data.json as a plain JSON array compatible with index.html
"""

import io, json, math, time, warnings
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta

import numpy as np
import pandas as pd
import requests
import yfinance as yf

warnings.filterwarnings("ignore")

# ── 1. S&P 500 list from Wikipedia ─────────────────────────────────────────
def get_sp500_tickers():
    url = "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies"
    headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"}
    resp = requests.get(url, headers=headers, timeout=30)
    resp.raise_for_status()
    tables = pd.read_html(io.StringIO(resp.text), attrs={"id": "constituents"})
    df = tables[0]
    df.columns = [c.strip() for c in df.columns]
    sym_col  = [c for c in df.columns if "Symbol"   in c or "Ticker"   in c][0]
    name_col = [c for c in df.columns if "Security" in c or "Name"     in c][0]
    sect_col = [c for c in df.columns if "GICS Sector" in c or "Sector" in c][0]
    result = []
    for _, row in df.iterrows():
        ticker = str(row[sym_col]).strip().replace(".", "-")
        result.append({
            "symbol": ticker,
            "name":   str(row[name_col]).strip(),
            "sector": str(row[sect_col]).strip(),
        })
    return result

# ── 2. Batch price download ─────────────────────────────────────────────────
def download_prices(tickers, start, end):
    prices = {}
    batch_size = 100
    for i in range(0, len(tickers), batch_size):
        batch = tickers[i:i + batch_size]
        print(f"  Price batch {i // batch_size + 1}/{math.ceil(len(tickers) / batch_size)}…", flush=True)
        try:
            raw = yf.download(batch, start=start, end=end,
                              auto_adjust=True, progress=False)
            if isinstance(raw.columns, pd.MultiIndex):
                lvl0 = raw.columns.get_level_values(0).unique()
                close = raw["Close"] if "Close" in lvl0 else raw.xs("Close", axis=1, level=1)
            else:
                t = batch[0]
                close = raw[["Close"]].rename(columns={"Close": t})
            for t in batch:
                if t in close.columns:
                    s = close[t].dropna()
                    if len(s) > 30:
                        prices[t] = s
        except Exception as e:
            print(f"  Batch error: {e}")
        time.sleep(0.3)
    return prices

# ── 3. Metric helpers ───────────────────────────────────────────────────────
def calc_return(prices, days):
    cutoff = prices.index[-1] - timedelta(days=days)
    past = prices[prices.index <= cutoff]
    if past.empty:
        return 0.0
    s, e = past.iloc[-1], prices.iloc[-1]
    return round((e / s - 1) * 100, 2) if s > 0 else 0.0

def calc_drawdown(prices_1y):
    if len(prices_1y) < 2:
        return 0.0
    peak = prices_1y.max()
    if peak <= 0:
        return 0.0
    return round((prices_1y.iloc[-1] / peak - 1) * 100, 2)

def calc_beta(ticker_ret, spy_ret):
    df = pd.concat([ticker_ret, spy_ret], axis=1, join="inner")
    df.columns = ["t", "m"]
    if len(df) < 30:
        return 1.0
    var_m = df["m"].var()
    return round(df.cov().loc["t", "m"] / var_m, 2) if var_m > 0 else 1.0

# ── 4. Per-ticker metadata ──────────────────────────────────────────────────
def fetch_meta(symbol):
    try:
        tkr = yf.Ticker(symbol)
        fi  = tkr.fast_info
        def _f(attr):
            try:    return getattr(fi, attr, None)
            except: return None
        mc       = float(_f("market_cap")                 or 0)
        avg_vol  = float(_f("three_month_average_volume") or _f("last_volume") or 0)
        high52   = float(_f("year_high")                  or 0)
        low52    = float(_f("year_low")                   or 0)
        prev_c   = float(_f("regular_market_previous_close") or _f("previous_close") or 0)
        last     = float(_f("last_price")                 or 0)
        day_chg  = round((last / prev_c - 1) * 100, 2) if prev_c > 0 and last > 0 else 0.0
        try:
            info     = tkr.info
            industry = info.get("industry", "") or ""
        except:
            industry = ""
        return {"marketCap": mc, "avgVolume": int(avg_vol),
                "high52w": high52, "low52w": low52,
                "dayChange": day_chg, "industry": industry}
    except:
        return {"marketCap": 0, "avgVolume": 0, "high52w": 0,
                "low52w": 0, "dayChange": 0.0, "industry": ""}

# ── 5. Percentile rank [0..100] ─────────────────────────────────────────────
def percentile_ranks(values):
    n = len(values)
    if n <= 1:
        return [50.0] * n
    order = sorted(range(n), key=lambda i: values[i])
    ranks = [0.0] * n
    for pos, idx in enumerate(order):
        ranks[idx] = round(pos / (n - 1) * 100, 1)
    return ranks

# ── MAIN ─────────────────────────────────────────────────────────────────────
def main():
    print("1/5  Fetching S&P 500 list from Wikipedia…", flush=True)
    sp500    = get_sp500_tickers()
    wiki_meta = {s["symbol"]: s for s in sp500}
    tickers  = [s["symbol"] for s in sp500]
    print(f"     {len(tickers)} tickers", flush=True)

    today = datetime.today()
    start = (today - timedelta(days=400)).strftime("%Y-%m-%d")
    end   = today.strftime("%Y-%m-%d")

    print("2/5  Downloading SPY (beta reference)…", flush=True)
    spy_raw = yf.download("SPY", start=start, end=end,
                          auto_adjust=True, progress=False)
    if isinstance(spy_raw.columns, pd.MultiIndex):
        spy = spy_raw["Close"].iloc[:, 0].dropna()
    else:
        spy = spy_raw["Close"].dropna()
    spy_ret = spy.pct_change().dropna()

    print("3/5  Downloading price history…", flush=True)
    all_prices = download_prices(tickers, start, end)
    valid = list(all_prices.keys())
    print(f"     {len(valid)} tickers with data", flush=True)

    print("4/5  Fetching metadata (parallel, ~3-5 min)…", flush=True)
    all_meta = {}
    with ThreadPoolExecutor(max_workers=10) as ex:
        futures = {ex.submit(fetch_meta, t): t for t in valid}
        done = 0
        for fut in as_completed(futures):
            sym = futures[fut]
            try:    all_meta[sym] = fut.result()
            except: all_meta[sym] = {}
            done += 1
            if done % 100 == 0:
                print(f"     {done}/{len(valid)} metadata done", flush=True)

    print("5/5  Computing momentum scores…", flush=True)
    records, raw_scores = [], []

    for t in valid:
        try:
            prices = all_prices[t]
            w = wiki_meta.get(t, {"name": t, "sector": "Unknown"})
            m = all_meta.get(t, {})

            r12 = calc_return(prices, 365)
            r6  = calc_return(prices, 182)
            r3  = calc_return(prices, 91)
            r1  = calc_return(prices, 30)

            daily   = prices.pct_change().dropna()
            ann_vol = daily.std() * math.sqrt(252) * 100
            ann_ret = daily.mean() * 252 * 100
            sharpe  = round(ann_ret / ann_vol, 2) if ann_vol > 0 else 0.0

            weighted  = 0.40*r12 + 0.30*r6 + 0.20*r3 + 0.10*r1
            raw_score = (weighted / ann_vol + 0.3 * sharpe) if ann_vol > 0 else 0.0

            prices_1y = prices[prices.index >= (prices.index[-1] - timedelta(days=365))]
            high52 = m.get("high52w") or round(float(prices_1y.max()), 2)
            low52  = m.get("low52w")  or round(float(prices_1y.min()), 2)
            dd     = calc_drawdown(prices_1y)
            beta   = calc_beta(daily, spy_ret)

            records.append({
                "symbol":        t,
                "name":          w["name"],
                "sector":        w["sector"],
                "industry":      m.get("industry", ""),
                "price":         round(float(prices.iloc[-1]), 2),
                "marketCap":     float(m.get("marketCap") or 0),
                "return12m":     r12,
                "return6m":      r6,
                "return3m":      r3,
                "return1m":      r1,
                "volatility":    round(ann_vol, 2),
                "avgVolume":     int(m.get("avgVolume") or 0),
                "dayChange":     float(m.get("dayChange") or 0),
                "high52w":       round(float(high52), 2),
                "low52w":        round(float(low52), 2),
                "drawdown":      dd,
                "beta":          beta,
                "sharpe":        sharpe,
                "momentumScore": 0.0,
                "weight":        0.0,
                "_raw":          raw_score,
            })
            raw_scores.append(raw_score)
        except Exception as e:
            print(f"  Skip {t}: {e}")

    ranks = percentile_ranks(raw_scores)
    for i, rec in enumerate(records):
        rec["momentumScore"] = ranks[i]
        del rec["_raw"]

    total_cap = sum(r["marketCap"] for r in records if r["marketCap"] > 0)
    for rec in records:
        rec["weight"] = round(rec["marketCap"] / total_cap * 100, 3) if total_cap > 0 and rec["marketCap"] > 0 else 0.0

    records.sort(key=lambda x: x["momentumScore"], reverse=True)

    with open("data.json", "w", encoding="utf-8") as f:
        json.dump(records, f, ensure_ascii=False, separators=(",", ":"))

    print(f"\n✅  data.json — {len(records)} stocks")
    print(f"    Top: {records[0]['symbol']} (score {records[0]['momentumScore']:.1f})")

if __name__ == "__main__":
    main()
