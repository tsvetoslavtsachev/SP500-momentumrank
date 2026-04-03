# MomentumRank S&P 500 - fetch_data.py v8
# Fixes:
#   - marketCap + avgVolume via yfinance .info (надеждно в GitHub Actions)
#   - avgVolume fallback от OHLCV volume колоната
#   - dayChange изчислен от adjusted close (не от raw price)
#   - calc_return използва точен брой търговски дни, не просто iloc[-N]
#   - weight = % от S&P 500 по market cap (не абсолютна стойност)
#   - retry логика при мрежови грешки

import json, time, random, math, sys
from datetime import datetime, timedelta
from io import StringIO

import pandas as pd
import numpy as np
import requests
import yfinance as yf

OUTPUT_FILE   = "data.json"
LOOKBACK_DAYS = 400        # ~16 месеца история за изчисления
RATE_SLEEP    = (0.3, 0.7) # пауза между тикери
MAX_RETRIES   = 3

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


# ── Списък на S&P 500 ─────────────────────────────────────────────────────────

def get_sp500_tickers():
    url  = (
        "https://raw.githubusercontent.com/datasets/s-and-p-500-companies"
        "/main/data/constituents.csv"
    )
    resp = requests.get(url, timeout=15)
    resp.raise_for_status()
    df   = pd.read_csv(StringIO(resp.text))
    df.columns = [c.strip() for c in df.columns]
    records = []
    for _, row in df.iterrows():
        sym    = str(row.get("Symbol", row.iloc[0])).strip().replace(".", "-")
        name   = str(row.get("Name",   row.iloc[1])).strip()
        raw_s  = str(row.get("Sector", row.iloc[2])).strip()
        sector = SECTOR_MAP.get(raw_s, raw_s)
        records.append({"symbol": sym, "name": name, "sector": sector})
    return records


# ── Данни от yfinance ─────────────────────────────────────────────────────────

def fetch_ticker_data(symbol, start_dt, end_dt):
    """
    Връща (prices_series, volume_series, market_cap, avg_volume_3m).
    prices_series  — adjusted close, DatetimeIndex, dropna
    volume_series  — daily volume, DatetimeIndex
    market_cap     — int (USD)
    avg_volume_3m  — int (среден дневен обем за 3 месеца)
    """
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            ticker = yf.Ticker(symbol)

            # ── OHLCV история ─────────────────────────────────────────────────
            hist = ticker.history(
                start=start_dt.strftime("%Y-%m-%d"),
                end=end_dt.strftime("%Y-%m-%d"),
                auto_adjust=True,   # Close = adjusted close
                actions=False,
            )

            if hist.empty or len(hist) < 60:
                return None, None, 0, 0

            prices = hist["Close"].dropna().astype(float)
            volumes = hist["Volume"].fillna(0).astype(float)

            # ── Метаданни (marketCap, avgVolume) ─────────────────────────────
            # .fast_info е по-надеждно от .info в GitHub Actions среда
            market_cap   = 0
            avg_volume   = 0

            try:
                fi = ticker.fast_info
                market_cap = int(getattr(fi, "market_cap", 0) or 0)
                avg_volume = int(getattr(fi, "three_month_average_volume", 0) or 0)
            except Exception:
                pass

            # Fallback: изчисли avg volume от историческите данни ако липсва
            if avg_volume == 0 and len(volumes) > 0:
                lookback_vol = volumes.iloc[-63:] if len(volumes) >= 63 else volumes
                avg_volume   = int(lookback_vol[lookback_vol > 0].mean()) if (lookback_vol > 0).any() else 0

            # Fallback за market_cap: price * shares (ако fast_info не дава)
            if market_cap == 0:
                try:
                    info       = ticker.info
                    market_cap = int(info.get("marketCap", 0) or 0)
                    if avg_volume == 0:
                        avg_volume = int(
                            info.get("averageDailyVolume3Month", 0)
                            or info.get("averageVolume", 0)
                            or 0
                        )
                except Exception:
                    pass

            return prices, volumes, market_cap, avg_volume

        except Exception as e:
            if attempt < MAX_RETRIES:
                wait = 2 ** attempt + random.uniform(0, 1)
                time.sleep(wait)
            else:
                print(f"  ERROR {symbol}: {e}")
                return None, None, 0, 0


# ── Изчисления ────────────────────────────────────────────────────────────────

def calc_return(prices, trading_days):
    """
    Връщане за N търговски дни назад.
    Използва iloc[-1] / iloc[-(trading_days+1)] за да вземе точно N свещи.
    """
    if prices is None or len(prices) < trading_days + 1:
        return 0.0
    return round((prices.iloc[-1] / prices.iloc[-(trading_days + 1)] - 1) * 100, 2)


def calc_volatility(prices, trading_days=252):
    """Анюализирана volatility от log-returns."""
    if prices is None or len(prices) < 22:
        return 0.0
    rets = np.log(prices / prices.shift(1)).dropna()
    if len(rets) > trading_days:
        rets = rets.iloc[-trading_days:]
    return round(float(rets.std() * math.sqrt(252) * 100), 2)


def calc_sharpe(prices, trading_days=252, rf=0.045):
    """
    Sharpe ratio: (ann_return - rf) / ann_vol
    rf = 4.5% (приблизителен 10-годишен US Treasury)
    """
    if prices is None or len(prices) < 22:
        return 0.0
    rets = np.log(prices / prices.shift(1)).dropna()
    if len(rets) > trading_days:
        rets = rets.iloc[-trading_days:]
    ann_ret = float(rets.mean() * 252)
    ann_vol = float(rets.std() * math.sqrt(252))
    if ann_vol <= 0:
        return 0.0
    return round((ann_ret - rf) / ann_vol, 2)


def calc_drawdown(prices):
    """Максимален drawdown за целия период (не само 52 седмици)."""
    if prices is None or len(prices) < 2:
        return 0.0
    roll_max = prices.expanding().max()
    dd_series = (prices - roll_max) / roll_max * 100
    return round(float(dd_series.min()), 2)


def calc_momentum_score(r1m, r3m, r6m, r12m, vol, sharpe, market_cap):
    """
    Composite momentum score 0-100.
    Тегла: 12M=30%, 6M=25%, 3M=20%, 1M=10%, Sharpe=10%, Vol=3%, Cap=2%
    """
    def sig(x, scale):
        # Ограничаваме exponent за да избегнем overflow
        exp_arg = max(-50, min(50, -x / scale))
        return 100.0 / (1.0 + math.exp(exp_arg))

    s12  = sig(r12m, 30)
    s6   = sig(r6m,  20)
    s3   = sig(r3m,  15)
    s1   = sig(r1m,  10)
    s_sh = sig(sharpe, 1.0)
    s_vol = 100.0 / (1.0 + math.exp(max(-50, min(50, (vol - 25) / 10))))

    if   market_cap >= 200e9: s_cap = 100
    elif market_cap >=  50e9: s_cap = 75
    elif market_cap >=  10e9: s_cap = 50
    elif market_cap >       0: s_cap = 25
    else:                      s_cap = 50  # неизвестен cap → неутрален

    return round(
        s12  * 0.30 + s6   * 0.25 + s3    * 0.20 + s1    * 0.10 +
        s_sh * 0.10 + s_vol * 0.03 + s_cap * 0.02,
        1,
    )


# ── Обработка на един тикер ───────────────────────────────────────────────────

def process_ticker(info, start_dt, end_dt):
    sym    = info["symbol"]
    name   = info["name"]
    sector = info["sector"]

    time.sleep(random.uniform(*RATE_SLEEP))

    prices, volumes, market_cap, avg_volume = fetch_ticker_data(sym, start_dt, end_dt)

    if prices is None or len(prices) < 60:
        return None

    # Изчисления
    r1m  = calc_return(prices, 21)
    r3m  = calc_return(prices, 63)
    r6m  = calc_return(prices, 126)
    r12m = calc_return(prices, 252)
    vol  = calc_volatility(prices)
    shp  = calc_sharpe(prices)
    dd   = calc_drawdown(prices)

    price   = round(float(prices.iloc[-1]), 2)
    day_chg = round((prices.iloc[-1] / prices.iloc[-2] - 1) * 100, 2) if len(prices) >= 2 else 0.0

    # 52-седмично high/low
    p52   = prices.iloc[-252:] if len(prices) >= 252 else prices
    high52 = round(float(p52.max()), 2)
    low52  = round(float(p52.min()), 2)

    # Drawdown from 52w high (за dashboard-а)
    dd52 = round((price - high52) / high52 * 100, 2) if high52 > 0 else 0.0

    score = calc_momentum_score(r1m, r3m, r6m, r12m, vol, shp, market_cap)

    # weight: marketCap в млрд. USD (ще се нормализира след събирането на всички)
    # Пазим raw стойността — нормализацията е в main()
    return {
        "symbol":        sym,
        "name":          name,
        "sector":        sector,
        "price":         price,
        "marketCap":     market_cap,          # USD absolute
        "return12m":     r12m,
        "return6m":      r6m,
        "return3m":      r3m,
        "return1m":      r1m,
        "volatility":    vol,
        "avgVolume":     avg_volume,
        "dayChange":     day_chg,
        "sharpe":        shp,
        "drawdown":      dd,
        "high52w":       high52,
        "low52w":        low52,
        "drawdown52w":   dd52,
        "momentumScore": score,
        "weight":        0.0,                 # попълва се в main() след нормализация
    }


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    print("MomentumRank - fetch_data.py v8")
    print("=" * 52)

    end_dt   = datetime.utcnow()
    start_dt = end_dt - timedelta(days=LOOKBACK_DAYS)

    print(f"Period : {start_dt.date()} → {end_dt.date()}")
    print("Loading S&P 500 list...")
    tickers = get_sp500_tickers()
    print(f"  {len(tickers)} tickers loaded")
    print()

    results = []
    total   = len(tickers)

    for i, t in enumerate(tickers, 1):
        sym = t["symbol"]
        print(f"  [{i:3d}/{total}] {sym:<8}", end="", flush=True)

        rec = process_ticker(t, start_dt, end_dt)

        if rec:
            results.append(rec)
            cap_b  = rec["marketCap"] / 1e9 if rec["marketCap"] else 0
            vol_m  = rec["avgVolume"] / 1e6 if rec["avgVolume"] else 0
            print(
                f"  score={rec['momentumScore']:.1f}"
                f"  r12m={rec['return12m']:+.1f}%"
                f"  vol={vol_m:.1f}M"
                f"  cap=${cap_b:.1f}B"
            )
        else:
            print("  SKIPPED")

        # Обнови сесията на всеки 50 тикера (yfinance handles this internally,
        # но пауза помага при rate-limiting)
        if i % 50 == 0:
            print(f"  --- checkpoint {i}/{total} ---")
            time.sleep(3)

    # ── Нормализирай weight = % от общ market cap ───────────────────────────
    total_cap = sum(r["marketCap"] for r in results if r["marketCap"] > 0)
    for r in results:
        if total_cap > 0 and r["marketCap"] > 0:
            r["weight"] = round(r["marketCap"] / total_cap * 100, 4)
        else:
            r["weight"] = 0.0

    # Сортирай по momentum score (descending)
    results.sort(key=lambda x: x["momentumScore"], reverse=True)

    # Добави rank поле
    for rank, r in enumerate(results, 1):
        r["rank"] = rank

    # Запази
    meta = {
        "updated":    datetime.utcnow().strftime("%Y-%m-%d %H:%M UTC"),
        "count":      len(results),
        "vol_ok":     sum(1 for r in results if r["avgVolume"] > 0),
        "cap_ok":     sum(1 for r in results if r["marketCap"] > 0),
    }

    # index.html очаква директен масив — записваме само results
    with open(OUTPUT_FILE, "w") as f:
        json.dump(results, f, separators=(",", ":"))

    # Отделен meta файл за debugging (не се използва от frontend)
    with open("data_meta.json", "w") as f:
        json.dump(meta, f, indent=2)

    print()
    print(f"Done          : {len(results)} records → {OUTPUT_FILE}")
    print(f"avgVolume OK  : {meta['vol_ok']}/{len(results)}")
    print(f"marketCap OK  : {meta['cap_ok']}/{len(results)}")
    print(f"Top 5         : {[r['symbol'] for r in results[:5]]}")
    print(f"Updated       : {meta['updated']}")

    # Изход с грешка ако повече от 20% нямат volume/cap (за GitHub Actions)
    if meta["vol_ok"] < len(results) * 0.8:
        print("WARNING: >20% missing avgVolume — проверете rate limiting!")
        sys.exit(1)


if __name__ == "__main__":
    main()
