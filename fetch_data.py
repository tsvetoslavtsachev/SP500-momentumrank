#!/usr/bin/env python3
"""
MomentumRank — fetch_data.py  v4
Uses direct Yahoo Finance v8 API (cookie/crumb auth) + stooq fallback
→ Bypasses the GitHub Actions IP rate-limit issue in yfinance
"""

import io, json, math, time, warnings
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta

import numpy as np
import pandas as pd
import requests
import yfinance as yf

warnings.filterwarnings("ignore")

# ── Browser headers ─────────────────────────────────────────────────────────
HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/122.0.0.0 Safari/537.36"
    ),
    "Accept":          "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "Accept-Encoding": "gzip, deflate, br",
}

# ── Yahoo Finance session with cookie/crumb auth ────────────────────────────
def make_yahoo_session():
    """Authenticate with Yahoo Finance to get cookies + crumb."""
    s = requests.Session()
    s.headers.update(HEADERS)
    crumb = None
    try:
        s.get("https://fc.yahoo.com", timeout=10, allow_redirects=True)
        time.sleep(1)
        r = s.get("https://query2.finance.yahoo.com/v1/test/getcrumb", timeout=10)
        raw = r.text.strip()
        crumb = raw if raw and "<" not in raw else None
        print(f"  Yahoo auth: {'OK (crumb={})'.format(crumb[:6]) if crumb else 'no crumb'}", flush=True)
    except Exception as e:
        print(f"  Yahoo auth error: {e}", flush=True)
    return s, crumb

def yahoo_prices(session, crumb, symbol, start_dt, end_dt):
    """Direct Yahoo v8 API — returns pd.Series of adjusted close prices."""
    params = {
        "period1": int(start_dt.timestamp()),
        "period2": int(end_dt.timestamp()),
        "interval": "1d",
        "events": "adjsplit",
    }
    if crumb:
        params["crumb"] = crumb
    url = f"https://query2.finance.yahoo.com/v8/finance/chart/{symbol}"
    r = session.get(url, params=params, timeout=20)
    r.raise_for_status()
    d = r.json()["chart"]["result"][0]
    ts     = d["timestamp"]
    adj    = d["indicators"].get("adjclose", [{}])[0].get("adjclose") \
             or d["indicators"]["quote"][0]["close"]
    idx    = pd.to_datetime([datetime.fromtimestamp(t) for t in ts])
    return pd.Series(adj, index=idx, dtype=float).dropna()

def stooq_prices(symbol, start_dt, end_dt):
    """stooq.com fallback — free, no rate limits on GitHub Actions."""
    sym = symbol.replace("-", ".") + ".US"
    url = (
        f"https://stooq.com/q/d/l/"
        f"?s={sym.lower()}"
        f"&d1={start_dt.strftime('%Y%m%d')}"
        f"&d2={end_dt.strftime('%Y%m%d')}"
        f"&i=d"
    )
    s = requests.Session()
    s.headers.update(HEADERS)
    r = s.get(url, timeout=20)
    df = pd.read_csv(io.StringIO(r.text), parse_dates=["Date"], index_col="Date")
    if "Close" in df.columns and not df.empty:
        return df["Close"].sort_index().dropna()
    return pd.Series(dtype=float)

# ── 1. S&P 500 list ─────────────────────────────────────────────────────────
def get_sp500_tickers():
    url  = "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies"
    resp = requests.get(url, headers=HEADERS, timeout=30)
    resp.raise_for_status()
    tables = pd.read_html(io.StringIO(resp.text), attrs={"id": "constituents"})
    df = tables[0]
    df.columns = [c.strip() for c in df.columns]
    sym_col  = [c for c in df.columns if "Symbol"      in c][0]
    name_col = [c for c in df.columns if "Security"    in c or "Name" in c][0]
    sect_col = [c for c in df.columns if "GICS Sector" in c][0]
    out = []
    for _, row in df.iterrows():
        out.append({
            "symbol": str(row[sym_col]).strip().replace(".", "-"),
            "name":   str(row[name_col]).strip(),
            "sector": str(row[sect_col]).strip(),
        })
    return out

# ── 2. Download all prices ──────────────────────────────────────────────────
def download_all_prices(tickers, start_dt, end_dt):
    """Try Yahoo v8 first; fall back to stooq for each failed ticker."""
    print("  Getting Yahoo auth session…", flush=True)
    session, crumb = make_yahoo_session()
    prices = {}
    failed = []

    # --- Yahoo v8 API, individual ticker, 8 parallel workers ---------------
    def fetch_yahoo(sym):
        for attempt in range(3):
            try:
                s = yahoo_prices(session, crumb, sym, start_dt, end_dt)
                if len(s) > 30:
                    return sym, s
                return sym, None
            except Exception as e:
                if "429" in str(e) or "Too Many" in str(e):
                    time.sleep(10 * (attempt + 1))
                else:
                    time.sleep(2)
        return sym, None

    print(f"  Yahoo v8: fetching {len(tickers)} tickers…", flush=True)
    with ThreadPoolExecutor(max_workers=8) as ex:
        futs = {ex.submit(fetch_yahoo, t): t for t in tickers}
        done = 0
        for fut in as_completed(futs):
            sym, series = fut.result()
            if series is not None:
                prices[sym] = series
            else:
                failed.append(sym)
            done += 1
            if done % 100 == 0:
                print(f"  Yahoo: {done}/{len(tickers)} done ({len(failed)} failed)", flush=True)

    # --- stooq fallback for failed tickers -----------------------------------
    if failed:
        print(f"  stooq fallback: {len(failed)} tickers…", flush=True)
        def fetch_stooq(sym):
            try:
                s = stooq_prices(sym, start_dt, end_dt)
                time.sleep(0.3)
                return sym, s if len(s) > 30 else None
            except:
                return sym, None

        with ThreadPoolExecutor(max_workers=4) as ex:
            futs = {ex.submit(fetch_stooq, t): t for t in failed}
            stooq_ok = 0
            for fut in as_completed(futs):
                sym, series = fut.result()
                if series is not None:
                    prices[sym] = series
                    stooq_ok += 1
        print(f"  stooq recovered {stooq_ok}/{len(failed)}", flush=True)

    return prices

# ── 3. Metric helpers ───────────────────────────────────────────────────────
def calc_return(prices, days):
    cutoff = prices.index[-1] - timedelta(days=days)
    past   = prices[prices.index <= cutoff]
    if past.empty: return 0.0
    s, e = past.iloc[-1], prices.iloc[-1]
    return round((e / s - 1) * 100, 2) if s > 0 else 0.0

def calc_drawdown(prices_1y):
    if len(prices_1y) < 2: return 0.0
    peak = prices_1y.max()
    return round((prices_1y.iloc[-1] / peak - 1) * 100, 2) if peak > 0 else 0.0

def calc_beta(t_ret, m_ret):
    df = pd.concat([t_ret, m_ret], axis=1, join="inner")
    df.columns = ["t", "m"]
    if len(df) < 30: return 1.0
    var_m = df["m"].var()
    return round(df.cov().loc["t", "m"] / var_m, 2) if var_m > 0 else 1.0

# ── 4. Metadata ─────────────────────────────────────────────────────────────
def fetch_meta(symbol):
    try:
        tkr = yf.Ticker(symbol)
        fi  = tkr.fast_info
        def _g(a):
            try: return getattr(fi, a, None)
            except: return None
        mc      = float(_g("market_cap")                  or 0)
        avg_vol = float(_g("three_month_average_volume")  or _g("last_volume") or 0)
        high52  = float(_g("year_high")                   or 0)
        low52   = float(_g("year_low")                    or 0)
        prev_c  = float(_g("regular_market_previous_close") or _g("previous_close") or 0)
        last    = float(_g("last_price")                  or 0)
        day_chg = round((last / prev_c - 1) * 100, 2) if prev_c > 0 and last > 0 else 0.0
        try:
            industry = (tkr.info or {}).get("industry", "") or ""
        except:
            industry = ""
        return {"marketCap": mc, "avgVolume": int(avg_vol),
                "high52w": high52, "low52w": low52,
                "dayChange": day_chg, "industry": industry}
    except:
        return {"marketCap": 0, "avgVolume": 0, "high52w": 0,
                "low52w": 0, "dayChange": 0.0, "industry": ""}

# ── 5. Percentile rank ───────────────────────────────────────────────────────
def pct_rank(values):
    n = len(values)
    if n <= 1: return [50.0] * n
    order = sorted(range(n), key=lambda i: values[i])
    ranks = [0.0] * n
    for pos, idx in enumerate(order):
        ranks[idx] = round(pos / (n - 1) * 100, 1)
    return ranks

# ── MAIN ─────────────────────────────────────────────────────────────────────
def main():
    print("1/5  S&P 500 list from Wikipedia…", flush=True)
    sp500     = get_sp500_tickers()
    wiki_meta = {s["symbol"]: s for s in sp500}
    tickers   = [s["symbol"] for s in sp500]
    print(f"     {len(tickers)} tickers", flush=True)

    today    = datetime.today()
    start_dt = today - timedelta(days=400)
    end_dt   = today

    print("2/5  Downloading SPY for beta…", flush=True)
    spy_session, spy_crumb = make_yahoo_session()
    spy_ret = pd.Series(dtype=float)
    for src in ["yahoo", "stooq"]:
        try:
            if src == "yahoo":
                spy_s = yahoo_prices(spy_session, spy_crumb, "SPY", start_dt, end_dt)
            else:
                spy_s = stooq_prices("SPY", start_dt, end_dt)
            if len(spy_s) > 30:
                spy_ret = spy_s.pct_change().dropna()
                print(f"     SPY via {src}: {len(spy_s)} days", flush=True)
                break
        except Exception as e:
            print(f"     SPY {src} failed: {e}", flush=True)

    print("3/5  Downloading price history (Yahoo v8 + stooq fallback)…", flush=True)
    all_prices = download_all_prices(tickers, start_dt, end_dt)
    valid = list(all_prices.keys())
    print(f"     {len(valid)} tickers with data", flush=True)

    if not valid:
        print("ERROR: 0 tickers — both Yahoo and stooq failed. Try again later.", flush=True)
        raise SystemExit(1)

    print("4/5  Fetching metadata (yfinance fast_info)…", flush=True)
    all_meta = {}
    with ThreadPoolExecutor(max_workers=8) as ex:
        futs = {ex.submit(fetch_meta, t): t for t in valid}
        done = 0
        for fut in as_completed(futs):
            sym = futs[fut]
            try:    all_meta[sym] = fut.result()
            except: all_meta[sym] = {}
            done += 1
            if done % 100 == 0:
                print(f"     {done}/{len(valid)} meta done", flush=True)

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

            p1y   = prices[prices.index >= (prices.index[-1] - timedelta(days=365))]
            high52 = m.get("high52w") or round(float(p1y.max()), 2)
            low52  = m.get("low52w")  or round(float(p1y.min()), 2)
            dd     = calc_drawdown(p1y)
            beta   = calc_beta(daily, spy_ret) if len(spy_ret) > 0 else 1.0

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

    if not records:
        print("ERROR: 0 records computed.", flush=True)
        raise SystemExit(1)

    ranks = pct_rank(raw_scores)
    for i, rec in enumerate(records):
        rec["momentumScore"] = ranks[i]
        del rec["_raw"]

    total_cap = sum(r["marketCap"] for r in records if r["marketCap"] > 0)
    for rec in records:
        rec["weight"] = round(rec["marketCap"] / total_cap * 100, 3) \
                        if total_cap > 0 and rec["marketCap"] > 0 else 0.0

    records.sort(key=lambda x: x["momentumScore"], reverse=True)

    with open("data.json", "w", encoding="utf-8") as f:
        json.dump(records, f, ensure_ascii=False, separators=(",", ":"))

    print(f"\n✅  data.json — {len(records)} stocks")
    print(f"    Top: {records[0]['symbol']} (score {records[0]['momentumScore']:.1f})")
    print(f"    Updated: {datetime.utcnow().strftime('%Y-%m-%dT%H:%M UTC')}")

if __name__ == "__main__":
    main()
