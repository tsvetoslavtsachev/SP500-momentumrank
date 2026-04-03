# MomentumRank — S&P 500 Momentum Dashboard

Автоматично обновяван всеки делник чрез **GitHub Actions** + хостван безплатно на **GitHub Pages**.  
Не е нужен собствен сървър или ключ за API.

---

## Как работи

```
[GitHub Actions — всеки делник 07:00 UTC]
         │
         ▼
   fetch_data.py          ← Yahoo Finance (безплатно)
   изтегля цени за ~500 акции
   изчислява returns, volatility, Sharpe, momentum score
         │
         ▼
   data.json (commit → main)
         │
         ▼
   GitHub Pages → index.html + data.json → браузър
```

---

## Настройка (5 минути)

### 1 — Качи файловете в GitHub

Създай нов репо и качи:

```
my-momentumrank/
├── index.html
├── data.json
├── fetch_data.py
├── README.md
└── .github/
    └── workflows/
        └── update_data.yml
```

### 2 — Включи GitHub Pages

`Settings → Pages → Source: Deploy from branch → main → / (root) → Save`

Сайтът ще е на: `https://YOUR-USERNAME.github.io/my-momentumrank`

### 3 — Провери Actions Permissions

`Settings → Actions → General → Allow all actions → Save`
`Settings → Actions → General → Workflow permissions → Read and write permissions → Save`

### 4 — Първо ръчно стартиране (веднага)

`Actions → Update MomentumRank Data → Run workflow`

---

## Локално стартиране (тест)

```bash
pip install yfinance pandas numpy lxml html5lib
python fetch_data.py          # генерира data.json (~2-3 мин)
# отвори index.html в браузър
```

---

## Momentum Score формула

```
weighted_return = 0.40×r12m + 0.30×r6m + 0.20×r3m + 0.10×r1m
raw_score       = weighted_return / volatility + 0.3 × sharpe
momentumScore   = percentile_rank(raw_score) × 100   → [0..100]
```
