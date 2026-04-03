fetch_data_v6 = '''"""
MomentumRank S&P 500 - fetch_data.py v6
S&P 500 list: GitHub CSV (no Wikipedia, no HTML parsing)
"""

import json, time, random, math
from datetime import datetime, timedelta
from io import StringIO
import pandas as pd
import numpy as np
import requests

OUTPUT_FILE   = "data.json"
RATE_SLEEP    = (0.35, 0.75)
LOOKBACK_DAYS = 400

SECTOR_MAP = {
    "Information Technology":  "Technology",
    "Health Care":             "Healthcare",
    "Financials":              "Financial Services",
    "Consumer Discretionary":  "Consumer Cyclical",
    "Consumer Staples":        "Consumer Defensive",
    "Communication Services":  "Communication Services",
    "Materials":               "Basic Materials",
    "Industrials":             "Industrials",
    "Real Estate":             "Real Estate",
    "Energy":                  "Energy",
    "Utilities":               "Utilities",
}


def get_yahoo_session():
    session = requests.Session()
    session.headers.update({
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/122.0.0.0 Safari/537.36"
        ),
        "Accept":          "application/json",
        "Accept-Language": "en-US,en;q=0.9",
    })
    session.get("https://finance.yahoo.com", timeout=12)
    crumb = None
    try:
        r = session.get(
            "https://query1.finance.yahoo.com/v1/test/getcrumb", timeout=10
        )
        if r.status_code == 200 and r.text.strip():
            crumb = r.text.strip()
    except Exception:
        pass
    return session, crumb


def get_sp500_tickers():
    """
    Loads S&P 500 tickers from a GitHub-hosted CSV.
    No HTML parsing, no Wikipedia, never returns 403.
    """
    url  = "https://raw.githubusercontent.com/datasets/s-and-p-500-companies/main/data/constituents.csv"
    resp = requests.get(url, timeout=15)
    resp.raise_for_status()
    df   = pd.read_csv(StringIO(resp.text))
    df.columns = [c.strip() for c in df.columns]

    records = []
    for _, row in df.iterrows():
        sym         = str(row.get("Symbol", row.iloc[0])).strip().replace(".", "-")
        name        = str(row.get("Name",   row.iloc[1])).strip()
        wiki_sector = str(row.get("Sector", row.iloc[2])).strip()
        sector      = SECTOR_MAP.get(wiki_sector, wiki_sector)
        records.append({"symbol": sym, "name": name, "sector": sector})

    return records


def fetch_prices_and_volume(session, crumb, symbol, start_dt, end_dt):
    """Fetches daily prices AND avg volume from Yahoo v8 Chart API in one call."""
    url    = f"https://query1.finance.yahoo.com/v8/finance/chart/{symbol}"
    params = {
        "interval":             "1d",
        "period1":              int(start_dt.timestamp()),
        "period2":              int(end_dt.timestamp()),
        "events":               "div,splits",
        "includeAdjustedClose": "true",
    }
    if crumb:
        params["crumb"] = crumb

    try:
        r = session.get(url, params=params, timeout=20)
        r.raise_for_status()
        d = r.json()["chart"]["result"][0]
    except Exception:
        return None, 0

    timestamps = d.get("timestamp", [])
    if not timestamps:
        return None, 0

    try:
        closes = d["indicators"]["adjclose"][0]["adjclose"]
    except (KeyError, IndexError):
        closes = d["indicators"]["quote"][0]["close"]

    volumes = d["indicators"]["quote"][0].get("volume") or []

    idx     = pd.to_datetime([datetime.utcfromtimestamp(t) for t in timestamps])
    prices  = pd.Series(closes, index=idx, dtype=float).dropna()
    vol_s   = pd.Series([v if v else 0 for v in volumes], index=idx)
    avg_vol = int(vol_s.iloc[-63:].mean()) if len(vol_s) >= 5 else 0

    return prices, avg_vol


def fetch_market_cap(session, crumb, symbol):
    """Fetches market cap via Yahoo v10 quoteSummary (more reliable than fast_info)."""
    url    = f"https://query1.finance.yahoo.com/v10/finance/quoteSummary/{symbol}"
    params = {"modules": "price"}
    if crumb:
        params["crumb"] = crumb

    try:
        r      = session.get(url, params=params, timeout=15)
        r.raise_for_status()
        result = r.json()["quoteSummary"]["result"]
        if not result:
            return 0
        mc = result[0].get("price", {}).get("marketCap", {})
        return mc.get("raw", 0) if isinstance(mc, dict) else 0
    except Exception:
        return 0


def calc_return(prices, days):
    if prices is None or len(prices) < days + 1:
        return 0.0
    return round((prices.iloc[-1] / prices.iloc[-days] - 1) * 100, 2)

def calc_volatility(prices, days=252):
    if prices is None or len(prices) < 22:
        return 0.0
    rets = np.log(prices / prices.shift(1)).dropna().iloc[-days:]
    return round(float(rets.std() * math.sqrt(252) * 100), 2)

def calc_sharpe(prices, days=252, rf=0.05):
    if prices is None or len(prices) < 22:
        return 0.0
    rets    = np.log(prices / prices.shift(1)).dropna().iloc[-days:]
    ann_ret = float(rets.mean() * 252)
    ann_vol = float(rets.std() * math.sqrt(252))
    return round((ann_ret - rf) / ann_vol, 2) if ann_vol > 0 else 0.0

def calc_drawdown(prices):
    if prices is None or len(prices) < 2:
        return 0.0
    roll_max = prices.cummax()
    return round(float(((prices - roll_max) / roll_max * 100).min()), 2)

def calc_momentum_score(r1m, r3m, r6m, r12m, vol, sharpe, market_cap):
    def sig(x, scale):
        return 100.0 / (1.0 + math.exp(-x / scale))

    s12   = sig(r12m, 30)
    s6    = sig(r6m,  20)
    s3    = sig(r3m,  15)
    s1    = sig(r1m,  10)
    s_sh  = sig(sharpe, 1.0)
    s_vol = 100.0 / (1.0 + math.exp((vol - 25) / 10))

    if   market_cap >= 200e9: s_cap = 100
    elif market_cap >=  50e9: s_cap = 75
    elif market_cap >=  10e9: s_cap = 50
    elif market_cap >      0: s_cap = 25
    else:                     s_cap = 50

    return round(
        s12 * 0.30 + s6 * 0.25 + s3 * 0.20 + s1 * 0.10 +
        s_sh * 0.10 + s_vol * 0.03 + s_cap * 0.02,
        1
    )


def process_ticker(info, session, crumb, start_dt, end_dt):
    sym    = info["symbol"]
    name   = info["name"]
    sector = info["sector"]

    time.sleep(random.uniform(*RATE_SLEEP))

    prices, avg_vol = fetch_prices_and_volume(session, crumb, sym, start_dt, end_dt)
    if prices is None or len(prices) < 60:
        print(f"  SKIP {sym}: not enough data")
        return None

    time.sleep(random.uniform(0.1, 0.3))
    market_cap = fetch_market_cap(session, crumb, sym)

    r1m    = calc_return(prices, 21)
    r3m    = calc_return(prices, 63)
    r6m    = calc_return(prices, 126)
    r12m   = calc_return(prices, 252)
    vol    = calc_volatility(prices)
    sharpe = calc_sharpe(prices)
    dd     = calc_drawdown(prices)
    price  = round(float(prices.iloc[-1]), 2)

    p52    = prices.iloc[-252:] if len(prices) >= 252 else prices
    high52 = round(float(p52.max()), 2)
    low52  = round(float(p52.min()), 2)

    day_chg = round((prices.iloc[-1] / prices.iloc[-2] - 1) * 100, 2) if len(prices) >= 2 else 0.0
    score   = calc_momentum_score(r1m, r3m, r6m, r12m, vol, sharpe, market_cap)
    weight  = round(market_cap / 1e12, 3) if market_cap > 0 else 0.0

    return {
        "symbol":        sym,
        "name":          name,
        "sector":        sector,
        "price":         price,
        "marketCap":     market_cap,
        "return12m":     r12m,
        "return6m":      r6m,
        "return3m":      r3m,
        "return1m":      r1m,
        "volatility":    vol,
        "avgVolume":     avg_vol,
        "sharpe":        sharpe,
        "drawdown":      dd,
        "high52w":       high52,
        "low52w":        low52,
        "dayChange":     day_chg,
        "momentumScore": score,
        "weight":        weight,
    }


def main():
    print("MomentumRank - fetch_data.py v6")
    print("=" * 52)

    end_dt   = datetime.utcnow()
    start_dt = end_dt - timedelta(days=LOOKBACK_DAYS)

    print("Loading S&P 500 list from GitHub CSV...")
    tickers = get_sp500_tickers()
    print(f"  {len(tickers)} tickers loaded")
    print(f"  Sample : {[t['symbol'] for t in tickers[:5]]}")
    print(f"  Sectors: {sorted(set(t['sector'] for t in tickers))}")

    print("Initialising Yahoo Finance session...")
    session, crumb = get_yahoo_session()
    print(f"  Crumb: {'OK' if crumb else 'MISSING - retrying'}")
    if not crumb:
        time.sleep(3)
        session, crumb = get_yahoo_session()
        print(f"  Retry: {'OK' if crumb else 'proceeding without'}")

    results = []
    total   = len(tickers)
    print(f"\\nProcessing {total} tickers...\\n")

    for i, t in enumerate(tickers, 1):
        print(f"  [{i:3d}/{total}] {t['symbol']:<8}", end="", flush=True)
        rec = process_ticker(t, session, crumb, start_dt, end_dt)
        if rec:
            results.append(rec)
            print(
                f"  score={rec['momentumScore']:5.1f}"
                f"  vol={rec['avgVolume']:>12,}"
                f"  cap=${rec['marketCap']/1e9:>8.1f}B"
            )
        else:
            print("  SKIPPED")

        if i % 100 == 0:
            print(f"\\n  Refreshing Yahoo session...\\n")
            session, crumb = get_yahoo_session()
            time.sleep(2)

    results.sort(key=lambda x: x["momentumScore"], reverse=True)
    with open(OUTPUT_FILE, "w") as f:
        json.dump(results, f, indent=2)

    vol_ok = sum(1 for r in results if r["avgVolume"] > 0)
    cap_ok = sum(1 for r in results if r["marketCap"] > 0)
    print(f"\\nDone: {len(results)} records -> {OUTPUT_FILE}")
    print(f"avgVolume OK : {vol_ok}/{len(results)}")
    print(f"marketCap OK : {cap_ok}/{len(results)}")
    print(f"Top 5        : {[r['symbol'] for r in results[:5]]}")


if __name__ == "__main__":
    main()
'''

with open("output/fetch_data.py", "w") as f:
    f.write(fetch_data_v6)

print(f"OK — {len(fetch_data_v6.splitlines())} реда")
print("Първи 3 реда:")
for line in fetch_data_v6.splitlines()[:3]:
    print(f"  {line}")
