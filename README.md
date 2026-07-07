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
component              тегло   scale
12M return (12-1)       0.30     30    ← пропуска последния месец (skip-month)
6M return               0.25     20
3M return               0.20     15
Sharpe                  0.10     1.0
Size (marketCap)        0.02     bracket score 0–100, без сигмоида

momentumScore = Σ(тегло × компонент) / Σ(тегла)   → [0..100]   (Σтегла = 0.87)
```

12-месечният прозорец е **12-1**: `close[t-21] / close[t-252] − 1` — стига до преди
~месец, не до днес. Последният месец се пропуска, защото на къс хоризонт доминира
краткосрочен reversal (Jegadeesh 1990, Lehmann 1990). Отделният 1-месечен терм и
самостоятелният volatility терм са **премахнати** (одит 2026-07-07, П2б).

Липсващ компонент (напр. акция с < 12м история → NaN return) се изхвърля и теглото
му се преразпределя върху наличните — частична история не дърпа скора към 50.

### Какво Е и какво НЕ Е този скор

- **Собствен многопрозоречен бленд, подравнен с momentum канона.** Главният
  12-месечен прозорец е каноничният **12-1** (пропуска последния месец заради
  краткосрочния reversal, както академичната и MSCI конвенцията); отделният
  1-месечен reversal терм е премахнат. Това пак **не е** чист академичен 12-1 или
  MSCI ранг — блендът добавя 6М/3М прозорци, Sharpe и size.
- **Абсолютна скала, не percentile.** Нивото на скора расте в бичи пазар и пада в
  мечи — праг като "Min Score 70" избира различна дълбочина от индекса в различни
  режими, а в силен бик топ имената се струпват над 90.
- **Не е чист momentum — но вече без двойно броене на риска.** Sharpe (10%) добавя
  risk-adjusted (low-vol) наклон към ранга; самостоятелният инвертиран volatility
  терм е премахнат (волатилността вече се брои веднъж — в знаменателя на Sharpe).
- **Без crash-защита.** Momentum стратегиите претърпяват резки сривове точно при
  режимни обрати — когато пропаднал пазар рязко се обръща нагоре от дъното. Рангът
  гледа само назад, затова е най-уверен в досегашните лидери точно преди обрата,
  когато водачеството се сменя (Daniel-Moskowitz 2016). Голият фактор е сред
  най-тежко пропадащите документирани: −91.59% за два месеца в най-лошия епизод
  (Barroso–Santa-Clara 2015). И собствената ни 12-1 емпирика повтаря профила —
  носеше в тренда, страдаше при обръщането. Дашбордът е информационен, не
  инвестиционен съвет.
