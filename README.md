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
├── momentum_core.py      ← споделено ядро (vendored, идентично с EU близнака)
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

`momentum_core.momentum_blend` — sigmoid blend, не percentile. Всеки компонент
минава през сигмоида `sig(x) = 100 / (1 + exp(-x / scale))`, после се претегля:

```
component         тегло   scale
12M return         0.30     30
6M return          0.25     20
3M return          0.20     15
1M return          0.10     10
Sharpe             0.10     1.0
Volatility         0.03     инвертиран (център 25 → висока vol = нисък скор)
Size (marketCap)   0.02     bracket score 0–100, без сигмоида

momentumScore = Σ(тегло × компонент) / Σ(тегла)   → [0..100]
```

Липсващ компонент (напр. акция с < 12м история → NaN return) се изхвърля и теглото
му се преразпределя върху наличните — частична история не дърпа скора към 50.
