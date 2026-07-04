# Bubble Detector

Live bubble/froth indicators for the markets I actually hold — US indices
(large/mid/small), India (Nifty large/mid/small), US tech (AAPL), and FnO
positioning. Inspired by [levels.io/bubble-detector](https://levels.io/bubble-detector),
rebuilt for a US + India + derivatives book.

**Live page:** https://mayankkapoor.github.io/stock-market-valuations/

## How it works

- `scripts/fetch.py` (Python stdlib only, no API keys) pulls every indicator
  from free public sources and writes `data/data.json`.
- A GitHub Actions cron (`.github/workflows/update-data.yml`) runs it 4×/day
  and commits the refreshed JSON.
- GitHub Pages serves `index.html`, which renders the JSON client-side.
- If a source is unreachable, the last good value is kept and marked `STALE` —
  one broken API never breaks the page.

## Panels & sources

| Panel | Indicators | Sources |
|---|---|---|
| US Market | Shiller CAPE, Buffett Indicator, Tobin's Q, 10y−2y curve, S&P 500 ÷ M2, VIX, HY credit spread | multpl, Yahoo, FRED, US Treasury, CBOE |
| India | Nifty 50 P/E & P/B, Midcap 150 & Smallcap 250 P/E premium, Nifty dividend yield | NSE indices archive (`ind_close_all_*.csv`) |
| US Tech & Concentration | S&P top-10 weight, Nasdaq 100 ÷ S&P 500, AAPL trailing P/E | SlickCharts, Yahoo |
| Derivatives & Positioning | India VIX, NIFTY put/call OI ratio, VIX ÷ VIX3M, CBOE SKEW | NSE, CBOE |

## Scoring

Where a gauge has usable history it is scored as a **percentile of its own
past** (≥90th = stretched, 75–90th = caution). Fixed thresholds or regime
rules are used where history is short; each card states its rule. There is
deliberately **no composite bubble score**.

## Known quirks

- FRED (`fredgraph.csv`) is geo-blocked on some non-US networks; the Actions
  runner (US IP) fetches it fine. Locally those gauges may show STALE.
- NSE blocks many datacenter/cloud IPs. Current India values come from the
  open `nsearchives.nseindia.com` daily CSV, which is far more reliable than
  the nseindia.com APIs; the option-chain PCR is best-effort.
- Yahoo Finance rate-limits bursts; the fetcher paces and retries.

## Local run

```
python3 scripts/fetch.py              # refresh data/data.json
python3 scripts/fetch.py --bootstrap  # also seed ~10y of monthly India history
python3 -m http.server                # open http://localhost:8000
```

Not investment advice.
