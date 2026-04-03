"""
MomentumRank S&P 500 - fetch_data.py v6
S&P 500 list: GitHub CSV (no HTML parsing)
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
    url  = "https://raw.githubusercontent.com/datasets/s-and-p-500-companies/main/data/constituents.csv"
    resp = requests.get(url, timeout=15)
    resp.raise_for_status()
    df   = pd.read_csv(StringIO(resp.text))
    df.columns = [c.strip() for c in df.columns]
    records = []
    for _, row in df.iterrows():
        sym    = str(row.get("Symbol", row.iloc[0])).strip().replace(".", "-")
        name   = str(row.get("Name",   row.iloc[1])).strip()
        sector = SECTOR_MAP.get(str(row.get("Sector", row.iloc[2])).strip(),
                                str(row.get("Sector", row.iloc[2])).strip())
        records.append({"symbol": sym, "name": name, "sector": sector})
    return records


def fetch_prices_and_volume(session, crumb, symbol, start_dt, end_dt):
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
    if prices is None or len(pr{}).get("marketCap", {})
        return mc.get("raw", 0) if isinstance(mc, dict) else 0
    ex:]
    return round(float(rets.s{}).get("marketCap", {})
        return mc.get("raw", 0) if isinstance(mc, dict) else 0
    exelse 0
    ex(prices) < 22:
        return 0.0
    rets    = np.log(prices / prices.shift(1)).dropna().iloc[-days:]
    ann_ret = float(rets.mean{}).get("marketCap", {})
        return mc.get("raw", 0) if isinstance(mc, dict) else 0
    exelse 0
    ex ann_vol > 0 else 0.0


def calc_drawdown(prices):
    if prices is None or len(pr{}).get("marketCap", {})
        return mc.get("raw", 0) if isinstance(mc, dict) else 0
    ex roll_max) / {}).get("marketCap", {})
        return mc.get("raw", 0) if isinstance(mc, dict) else 0
    excap):
    def sig(x, scale):
        return 100.0 / (1.0 + math.exp(-x / scale))
    s12{}).get("marketCap", {})
        return mc.get("raw", 0) if isinstance(mc, dict) else 0
    ex    return mc.get("raw", 0) if isinstance(mc, dict) else 0
    ex     return mc.get("raw", 0) if isinstance(mc, dict) else 0
    ex      return mc.get("raw", 0) if isinstance(mc, dict) else 0
    ex s_cap = 50
    elif market_c{}).get("marketCap", {})
        return mc.get("raw", 0) if isinstance(mc, dict) else 0
    ex * 0.30 + s6 * 0.25 + {}).get("marketCap", {})
        return mc.get("raw", 0) if isinstance(mc, dict) else 0
    ex 0) if isinstance(mc, dict) else 0
    ex end_dt):
    sym    = info["symbol"]
    {}).get("marketCap", {})
        return mc.get("raw", 0) if isinstance(mc, dict) else 0
    ex isinstance(mc, dict) else 0
    exand_volume(session, crumb, sym, start_dt, end_dt)
    if prices is None or len(prices) {}).get("marketCap", {})
        return mc.get("raw", 0) if isinstance(mc, dict) else 0
    exm.uniform({}).get("marketCap", {})
        return mc.get("raw", 0) if isinstance(mc, dict) else 0
    ex"marketCap", {})
        return mc.get("raw", 0) if isinstance(mc, dict) else 0
    ex  r12m   = calc_return(prices, 252)
    vol    = calc_volatility(prices)
    {}).get("marketCap", {})
        return mc.get("raw", 0) if isinstance(mc, dict) else 0
    exs.iloc[-1]), 2)
   {}).get("marketCap", {})
        return mc.get("raw", 0) if isinstance(mc, dict) else 0
    exx()), 2)
    low52  = round(float(p52.min{}).get("marketCap", {})
        return mc.get("raw", 0) if isinstance(mc, dict) else 0
    ex= 2 else 0.0
    s{}).get("marketCap", {})
        return mc.get("raw", 0) if isinstance(mc, dict) else 0
    exict) else 0
    ex, 3) if market_cap > 0 else 0.0
    return {
        "sy{}).get("marketCap", {})
        return mc.get("raw", 0) if isinstance(mc, dict) else 0
    ex       "return12m": r12m, "return6m": r6m, "return3m": r3m, "return1m": r1m,
        "volatility": vol, "avgVolume": avg_vol, "sharpe": sharpe,
        "drawdown": dd, "high52w": high52, "low52w": low52,
        "dayChange": day_chg, "momentumScore": score, "weight": weight,
    }


def main():
    print("MomentumRank - fetch_{}).get("marketCap", {})
        return mc.get("raw", 0) if isinstance(mc, dict) else 0
    exdelta(days=LOOKBACK_DAYS)

    print("Loading S&P 500 {}).get("marketCap", {})
        return mc.get("raw", 0) if isinstance(mc, dict) else 0
    exrs loaded")
    print(f"  Sectors: {sorted(set(t['sector'] for t in {}).get("marketCap", {})
        return mc.get("raw", 0) if isinstance(mc, dict) else 0
    exo_session()
    print(f"  Crumb: {'OK' if crumb else 'MISSING - r{}).get("marketCap", {})
        return mc.get("raw", 0) if isinstance(mc, dict) else 0
    ex()

    results = []
    total{}).get("marketCap", {})
        return mc.get("raw", 0) if isinstance(mc, dict) else 0
    exickers, 1):
        print(f"  [{i:3d}/{total}] {t['symbol']:<8}", end="", flush=True)
       {}).get("marketCap", {})
        return mc.get("raw", 0) if isinstance(mc, dict) else 0
    ex.append({}).get("marketCap", {})
        return mc.get("raw", 0) if isinstance(mc, dict) else 0
    exance(mc, dict) else 0
    exf}B")
        else:
            print("  SKIPPED")
        if i % 10{}).get("marketCap", {})
        return mc.get("raw", 0) if isinstance(mc, dict) else 0
    exet_yahoo_session()
            time.sleep(2)

    results.sort(key=lambda x: x["momentumScore"], reverse=True)
    with open(OUTPUT_FIL{}).get("marketCap", {})
        return mc.get("raw", 0) if isinstance(mc, dict) else 0
    ex, {})
        return mc.get("raw", 0) if isinstance(mc, dict) else 0
    ex 0)
    print(f"\nDone: {len(results)} records -> {OUTPUT_FILE}")
    print(f"avgVolum{}).get("marketCap", {})
        return mc.get("raw", 0) if isinstance(mc, dict) else 0
    ext(f"T{}).get("marketCap", {})
        return mc.get("raw", 0) if isinstance(mc, dict) else 0
    ex``

## Проверка преди run

Файлът е пр{}).get("marketCap", {})
        return mc.get("raw", 0) if isinstance(mc, dict) else 0
    exurn mc.get("raw", 0) if isinstance(mc, dict) else 0
    ex**Run work{}).get("marketCap", {})
        return mc.get("raw", 0) if isinstance(mc, dict) else 0
    ex
