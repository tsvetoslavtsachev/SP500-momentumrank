"""МЕТ1 (22.11.2026): една компания с няколко класа акции влиза в теглото веднъж.

Yahoo дава на GOOGL и на GOOG капитализацията на цялата Alphabet, затова
старото нормиране по marketCap броеше компанията два пъти (4,92 + 4,90).
"""

from __future__ import annotations

from fetch_data import compute_weights, share_class_groups

TICKERS = [
    {"symbol": "GOOGL", "name": "Alphabet Inc. (Class A)"},
    {"symbol": "GOOG", "name": "Alphabet Inc. (Class C)"},
    {"symbol": "NWSA", "name": "News Corp (Class A)"},
    {"symbol": "NWS", "name": "News Corp (Class B)"},
    {"symbol": "FOXA", "name": "Fox Corporation (Class A)"},
    {"symbol": "FOX", "name": "Fox Corporation (Class B)"},
    {"symbol": "BRK-B", "name": "Berkshire Hathaway"},
    {"symbol": "AAPL", "name": "Apple Inc."},
]


def _rec(sym, mcap, price=100.0, shares=0, stale=False):
    r = {"symbol": sym, "marketCap": mcap, "price": price, "weight": 0.0, "stale": stale}
    if shares:
        r["classShares"] = shares
    return r


def test_groups_are_found_by_class_name():
    g = share_class_groups(TICKERS)
    assert set(g) == {"GOOGL", "GOOG", "NWSA", "NWS", "FOXA", "FOX"}
    assert g["GOOGL"][1] == 2
    assert "BRK-B" not in g and "AAPL" not in g


def test_single_class_name_is_not_a_group():
    g = share_class_groups([{"symbol": "X", "name": "Solo Corp (Class A)"}])
    assert g == {}


def test_company_counts_once_with_class_shares():
    # Alphabet цяла = 3000; всеки клас има своите акции (по 15 при цена 100 → 1500).
    groups = share_class_groups(TICKERS[:2] + TICKERS[-1:])
    res = [
        _rec("GOOGL", 3000, shares=15),
        _rec("GOOG", 3000, shares=15),
        _rec("AAPL", 3000),
    ]
    compute_weights(res, groups)
    w = {r["symbol"]: r["weight"] for r in res}
    assert abs(sum(w.values()) - 100) < 1e-3
    assert abs(w["GOOGL"] + w["GOOG"] - 50) < 1e-3  # Alphabet = Apple, не двойно
    assert res[0]["weightBasis"] == "classShares"
    assert "weightBasis" not in res[2]


def test_fallback_without_class_shares_still_counts_once():
    groups = share_class_groups(TICKERS[:2] + TICKERS[-1:])
    res = [_rec("GOOGL", 3000), _rec("GOOG", 3000), _rec("AAPL", 3000)]
    compute_weights(res, groups)
    w = {r["symbol"]: r["weight"] for r in res}
    assert abs(w["GOOGL"] + w["GOOG"] - 50) < 1e-3
    assert res[0]["weightBasis"] == "marketCap/classes"


def test_marketcap_field_is_not_touched():
    # marketCap храни size score и колоната на страницата: методът не се пипа.
    groups = share_class_groups(TICKERS)
    res = [_rec("GOOGL", 3000, shares=15), _rec("GOOG", 3000, shares=15)]
    compute_weights(res, groups)
    assert [r["marketCap"] for r in res] == [3000, 3000]


def test_zero_cap_row_gets_zero_weight():
    res = [_rec("AAPL", 3000), _rec("ZZZ", 0)]
    compute_weights(res, {})
    assert res[1]["weight"] == 0.0 and res[0]["weight"] == 100.0
