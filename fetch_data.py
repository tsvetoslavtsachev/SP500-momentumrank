"""
MomentumRank S&P 500 — fetch_data.py v5
Fixes:
  1. Sector names: Wikipedia GICS → Yahoo Finance style (matching HTML SECTORS array)
  2. avgVolume: extracted from Yahoo v8 Chart API (same call as prices — zero extra requests)
  3. marketCap: fetched via Yahoo v10 quoteSummary/price module (reliable)
"""

import json, time, random, math
from datetime import datetime, timedelta
import pandas as pd
import numpy as np
import requests

# ── CONFIG ──────────────────────────────────────────────────────────────────────
OUTPUT_FILE   = "data.json"
RATE_SLEEP    = (0.35, 0.75)   # random sleep between requests (seconds)
LOOKBACK_DAYS = 400            # ~252 trading days + buffer

# ── SECTOR MAP: Wikipedia GICS → Yahoo Finance (must match HTML SECTORS array) ──
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

# ── YAHOO SESSION + CRUMB ────────────────────────────────────────────────────────
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


# ── SP500 TICKERS FROM WIKIPEDIA ────────────────────────────────────────────────
def get_sp500_tickers():
    """Returns list of {symbol, name, sector} with Yahoo-style sector names."""
    url     = "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies"
    headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"}
    resp    = requests.get(url, headers=headers, timeout=15)
    resp.raise_for_status()
    tables  = pd.read_html(resp.text)   # pass HTML string, not URL
    df     = tables[0]
    df.columns = [c.strip() for c in df.columns]

    ticker_col = next(c for c in df.columns if "ticker" in c.lower() or "symbol" in c.lower())
    sector_col = next(
        c for c in df.columns
        if "gics sector" in c.lower() or ("sector" in c.lower() and "sub" not in c.lower())
    )
    name_col   = next(
        c for c in df.columns
        if "security" in c.lower() or "name" in c.lower() or "company" in c.lower()
    )

    records = []
    for _, row in df.iterrows():
        sym          = str(row[ticker_col]).strip().replace(".", "-")
        name         = str(row[name_col]).strip()
        wiki_sector  = str(row[sector_col]).strip()
        sector       = SECTOR_MAP.get(wiki_sector, wiki_sector)   # ← map here
        records.append({"symbol": sym, "name": name, "sector": sector})

    return records


# ── YAHOO v8 CHART API: prices + volume in ONE call ─────────────────────────────
def fetch_prices_and_volume(session, crumb, symbol, start_dt, end_dt):
    """
    Returns (pd.Series of daily adj-closes, avg_volume_63d_int).
    Volume lives in the same JSON response as prices — no extra round-trip.
    """
    url    = f"https://query1.finance.yahoo.com/v8/finance/chart/{symbol}"
    params = {
        "interval":              "1d",
        "period1":               int(start_dt.timestamp()),
        "period2":               int(end_dt.timestamp()),
        "events":                "div,splits",
        "includeAdjustedClose":  "true",
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

    # Adjusted close preferred; fall back to regular close
    try:
        closes = d["indicators"]["adjclose"][0]["adjclose"]
    except (KeyError, IndexError):
        closes = d["indicators"]["quote"][0]["close"]

    # --- VOLUME (already here, no extra call needed) ---
    volumes = d["indicators"]["quote"][0].get("volume") or []

    idx     = pd.to_datetime([datetime.utcfromtimestamp(t) for t in timestamps])
    prices  = pd.Series(closes,  index=idx, dtype=float).dropna()
    vol_s   = pd.Series([v if v else 0 for v in volumes], index=idx)

    # Average daily volume over last ~3 months (63 trading days)
    avg_vol = int(vol_s.iloc[-63:].mean()) if len(vol_s) >= 5 else 0

    return prices, avg_vol


# ── YAHOO v10 QUOTESUMMARY: market cap ──────────────────────────────────────────
def fetch_market_cap(session, crumb, symbol):
    """
    Returns raw market cap (USD) from Yahoo v10 quoteSummary/price module.
    More reliable than fast_info for batch runs.
    """
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


# ── METRICS ──────────────────────────────────────────────────────────────────────
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
    ann_vol = float(rets.std()  * math.sqrt(252))
    return round((ann_ret - rf) / ann_vol, 2) if ann_vol > 0 else 0.0

def calc_drawdown(prices):
    if prices is None or len(prices) < 2:
        return 0.0
    roll_max = prices.cummax()
    return round(float(((prices - roll_max) / roll_max * 100).min()), 2)

def calc_momentum_score(r1m, r3m, r6m, r12m, vol, sharpe, market_cap):
    """
    Composite score 0-100.
    Weights: 12M 30% | 6M 25% | 3M 20% | 1M 10% | Sharpe 10% | Vol 3% | Cap 2%
    Each sub-component normalised via sigmoid to 0-100.
    """
    def sig(x, scale):
        return 100.0 / (1.0 + math.exp(-x / scale))

    s12    = sig(r12m, 30)
    s6     = sig(r6m,  20)
    s3     = sig(r3m,  15)
    s1     = sig(r1m,  10)
    s_sh   = sig(sharpe, 1.0)
    s_vol  = 100.0 / (1.0 + math.exp((vol - 25) / 10))   # lower vol = higher score

    # Market-cap tier bonus
    if   market_cap >= 200e9: s_cap = 100
    elif market_cap >=  50e9: s_cap = 75
    elif market_cap >=  10e9: s_cap = 50
    elif market_cap >      0: s_cap = 25
    else:                     s_cap = 50   # unknown → neutral

    return round(
        s12 * 0.30 + s6 * 0.25 + s3 * 0.20 + s1 * 0.10 +
        s_sh * 0.10 + s_vol * 0.03 + s_cap * 0.02,
        1
    )


# ── PROCESS ONE TICKER ────────────────────────────────────────────────────────────
def process_ticker(info, session, crumb, start_dt, end_dt):
    sym    = info["symbol"]
    name   = info["name"]
    sector = info["sector"]

    time.sleep(random.uniform(*RATE_SLEEP))

    # 1. Prices + avgVolume — single API call
    prices, avg_vol = fetch_prices_and_volume(session, crumb, sym, start_dt, end_dt)
    if prices is None or len(prices) < 60:
        print(f"  SKIP {sym}: insufficient price data ({len(prices) if prices is not None else 0} days)")
        return None

    # 2. Market cap — separate call, short delay
    time.sleep(random.uniform(0.1, 0.3))
    market_cap = fetch_market_cap(session, crumb, sym)

    # 3. Compute metrics
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


# ── MAIN ──────────────────────────────────────────────────────────────────────────
def main():
    print("MomentumRank — fetch_data.py v5")
    print("=" * 52)

    end_dt   = datetime.utcnow()
    start_dt = end_dt - timedelta(days=LOOKBACK_DAYS)

    # Get tickers
    print("Fetching S&P 500 list from Wikipedia...")
    tickers = get_sp500_tickers()
    print(f"  {len(tickers)} tickers found")

    # Yahoo session
    print("Initialising Yahoo Finance session...")
    session, crumb = get_yahoo_session()
    print(f"  Crumb: {'OK' if crumb else 'MISSING — retrying'}")
    if not crumb:
        time.sleep(3)
        session, crumb = get_yahoo_session()
        print(f"  Retry crumb: {'OK' if crumb else 'proceeding without'}")

    # Process
    results = []
    total   = len(tickers)
    print(f"\nProcessing {total} tickers (~10-15 min on GitHub Actions)\n")

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

        # Refresh session every 100 tickers
        if i % 100 == 0:
            print(f"\n  Refreshing session at {i}...\n")
            session, crumb = get_yahoo_session()
            time.sleep(2)

    # Sort & save
    results.sort(key=lambda x: x["momentumScore"], reverse=True)
    with open(OUTPUT_FILE, "w") as f:
        json.dump(results, f, indent=2)

    vol_ok = sum(1 for r in results if r["avgVolume"] > 0)
    cap_ok = sum(1 for r in results if r["marketCap"] > 0)
    print(f"\n✓  {len(results)} records → {OUTPUT_FILE}")
    print(f"   avgVolume populated : {vol_ok}/{len(results)}")
    print(f"   marketCap populated : {cap_ok}/{len(results)}")
    print(f"   Top 5 : {[r['symbol'] for r in results[:5]]}")


if __name__ == "__main__":
    main()
