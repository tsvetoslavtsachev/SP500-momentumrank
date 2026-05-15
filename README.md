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

Текущата версия използва sigmoid-нормализация на отделните фактори и weighted composite score:

```
sigmoid(x, scale) = 100 / (1 + exp(-x / scale))

s12    = sigmoid(return12m, 30)
s6     = sigmoid(return6m, 20)
s3     = sigmoid(return3m, 15)
s1     = sigmoid(return1m, 10)
sSharpe = sigmoid(sharpe, 1.0)
sVol   = 100 / (1 + exp((volatility - 25) / 10))

sCap = 100  ако marketCap >= 200B
       75   ако marketCap >= 50B
       50   ако marketCap >= 10B или marketCap липсва
       25   ако marketCap > 0 и < 10B

momentumScore =
  0.30×s12 + 0.25×s6 + 0.20×s3 + 0.10×s1
+ 0.10×sSharpe + 0.03×sVol + 0.02×sCap
```

Резултатът се закръгля до 1 знак след десетичната точка и се използва за сортиране/ранкиране на компаниите.
