#!/usr/bin/env python3
"""Bubble detector data pipeline.

Fetches every indicator from free, keyless public sources and writes
data/data.json for the static dashboard. Each indicator fetches
independently; on failure the last known value from the previous
data.json is kept and marked stale, so one broken source never breaks
the page.

Sources and their network quirks:
- FRED fredgraph CSV: keyless, but geo-blocked outside the US for some
  networks. Works from GitHub Actions (US IPs).
- Yahoo Finance chart/quote API: keyless but rate-limits bursts; we
  pace requests and retry with backoff.
- NSE archives (nsearchives.nseindia.com): open CSV per trading day,
  much less protected than nseindia.com APIs.
- CBOE CDN, US Treasury XML, multpl.com, SlickCharts: open.

Usage:
  python3 scripts/fetch.py              # normal run
  python3 scripts/fetch.py --bootstrap  # also seed 10y India history
"""
import csv
import http.cookiejar
import io
import json
import os
import re
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
import zipfile
from collections.abc import Callable, Iterable
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

# A time series of (iso_date, value), ascending by date.
Series = list[tuple[str, float]]
# One rendered indicator card / shared fetch context. Both are JSON-shaped
# and intentionally loose: the frontend consumes them as-is.
Indicator = dict[str, Any]
Ctx = dict[str, Any]
Hist = dict[str, Series]

ROOT = Path(__file__).resolve().parent.parent
DATA_DIR = ROOT / "data"
HISTORY_DIR = DATA_DIR / "history"
DATA_FILE = DATA_DIR / "data.json"
ACCUM_FILE = HISTORY_DIR / "accumulated.csv"

UA = ("Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
      "(KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36")

SPARK_POINTS = 120
SPARK_YEARS = 2


def log(msg: str) -> None:
    print(msg, flush=True)


def http_get(url: str, headers: dict[str, str] | None = None, timeout: int = 30,
             retries: int = 2, backoff: int = 5) -> str:
    last: Exception | None = None
    for attempt in range(retries + 1):
        try:
            req = urllib.request.Request(url, headers={"User-Agent": UA, **(headers or {})})
            with urllib.request.urlopen(req, timeout=timeout) as r:
                return str(r.read().decode("utf-8", "replace"))
        except Exception as e:  # noqa: BLE001 - each indicator is best-effort
            last = e
            if attempt < retries:
                time.sleep(backoff * (attempt + 1))
    assert last is not None
    raise last


def http_get_bytes(url: str, timeout: int = 30) -> bytes:
    req = urllib.request.Request(url, headers={"User-Agent": UA})
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return bytes(r.read())


# ---------------------------------------------------------------------------
# Series helpers
# ---------------------------------------------------------------------------

def clean_pe(series: Iterable[tuple[str, float | None]],
             lo: float = 1.0, hi: float = 100.0) -> Series:
    """Drop junk P/E observations (earnings-collapse periods print absurd
    ratios — smallcap 250 P/E hit five digits in 2020)."""
    return [(d, v) for d, v in series if v is not None and lo < v <= hi]


def percentile_rank(history: Iterable[float | None], value: float) -> int | None:
    """Percent of historical observations at or below value (0-100)."""
    vals = sorted(v for v in history if v is not None)
    if len(vals) < 20:
        return None
    below = sum(1 for v in vals if v <= value)
    return round(100.0 * below / len(vals))


def sparkify(series: Series) -> list[list[str | float]]:
    """series: list of (iso_date, value) ascending. Downsample last N years."""
    if not series:
        return []
    cutoff = (datetime.now(UTC) - timedelta(days=365 * SPARK_YEARS)).strftime("%Y-%m-%d")
    recent = [p for p in series if p[0] >= cutoff] or series
    step = max(1, len(recent) // SPARK_POINTS)
    out = recent[::step]
    if out[-1] != recent[-1]:
        out.append(recent[-1])
    return [[d, round(v, 4)] for d, v in out]


def pct_status(pct: int | None, hi_is_froth: bool = True,
               red: int = 90, amber: int = 75) -> str:
    """Map a percentile to red/amber/green. hi_is_froth=False inverts."""
    if pct is None:
        return "green"
    p = pct if hi_is_froth else 100 - pct
    if p >= red:
        return "red"
    if p >= amber:
        return "amber"
    return "green"


def ordinal(n: int) -> str:
    if 10 <= n % 100 <= 20:
        suffix = "th"
    else:
        suffix = {1: "st", 2: "nd", 3: "rd"}.get(n % 10, "th")
    return f"{n}{suffix}"


# ---------------------------------------------------------------------------
# Source fetchers
# ---------------------------------------------------------------------------

def fred_csv(series_id: str) -> Series:
    """FRED series -> list of (iso_date, float).

    Prefers the official API when a free FRED_API_KEY is set (reliable from
    datacenter IPs). Falls back to the keyless fredgraph CSV, which Akamai
    sometimes blocks/slows for cloud and non-US networks.
    """
    key = os.environ.get("FRED_API_KEY", "").strip()
    if key:
        body = http_get("https://api.stlouisfed.org/fred/series/observations"
                        f"?series_id={series_id}&api_key={key}&file_type=json",
                        timeout=40, retries=2)
        obs = json.loads(body).get("observations", [])
        out: Series = [(o["date"], float(o["value"]))
                       for o in obs if o.get("value") not in (".", "", None)]
        if not out:
            raise ValueError(f"FRED API {series_id}: no observations")
        return out
    body = http_get(f"https://fred.stlouisfed.org/graph/fredgraph.csv?id={series_id}",
                    headers={"Accept": "text/csv,*/*;q=0.8",
                             "Accept-Language": "en-US,en;q=0.9",
                             "Referer": f"https://fred.stlouisfed.org/series/{series_id}"},
                    timeout=35, retries=1, backoff=5)
    rows: Series = []
    for row in csv.reader(io.StringIO(body)):
        if len(row) != 2 or row[0] in ("DATE", "observation_date"):
            continue
        try:
            rows.append((row[0], float(row[1])))
        except ValueError:
            continue
    if not rows:
        raise ValueError(f"FRED {series_id}: no rows parsed")
    return rows


def spx_series() -> Series:
    """S&P 500 daily closes: Yahoo primary, FRED SP500 fallback."""
    try:
        return yahoo_chart("^GSPC", range_="10y")
    except Exception:  # noqa: BLE001
        return fred_csv("SP500")


_yahoo_last_call = [0.0]


def yahoo_chart(symbol: str, range_: str = "10y", interval: str = "1d") -> Series:
    """Yahoo chart API -> list of (iso_date, close). Paced + host fallback."""
    wait = 3.0 - (time.time() - _yahoo_last_call[0])
    if wait > 0:
        time.sleep(wait)
    err: Exception | None = None
    for host in ("query2", "query1"):
        for _attempt in range(2):
            try:
                url = (f"https://{host}.finance.yahoo.com/v8/finance/chart/"
                       f"{urllib.parse.quote(symbol)}?range={range_}&interval={interval}")
                body = http_get(url, timeout=25, retries=0)
                _yahoo_last_call[0] = time.time()
                j = json.loads(body)
                res = j["chart"]["result"][0]
                ts = res.get("timestamp") or []
                closes = res["indicators"]["quote"][0].get("close") or []
                out: Series = []
                for t, c in zip(ts, closes, strict=False):
                    if c is not None:
                        out.append((datetime.fromtimestamp(t, UTC).strftime("%Y-%m-%d"),
                                    float(c)))
                if not out:
                    raise ValueError("empty series")
                return out
            except Exception as e:  # noqa: BLE001
                err = e
                _yahoo_last_call[0] = time.time()
                time.sleep(6)
    assert err is not None
    raise err


def yahoo_quote_fields(symbol: str, fields: list[str]) -> dict[str, Any]:
    """Yahoo v7 quote (needs cookie+crumb) -> dict of requested fields."""
    cj = http.cookiejar.CookieJar()
    op = urllib.request.build_opener(urllib.request.HTTPCookieProcessor(cj))
    op.addheaders = [("User-Agent", UA)]
    try:
        op.open("https://fc.yahoo.com", timeout=15).read()
    except Exception:  # noqa: BLE001 - 404 expected, we only want cookies
        pass
    crumb = op.open("https://query2.finance.yahoo.com/v1/test/getcrumb", timeout=15).read().decode()
    url = (f"https://query2.finance.yahoo.com/v7/finance/quote?symbols={symbol}"
           f"&crumb={urllib.parse.quote(crumb)}")
    body = op.open(url, timeout=15).read().decode()
    q = json.loads(body)["quoteResponse"]["result"][0]
    return {f: q.get(f) for f in fields}


def cboe_history(name: str) -> Series:
    """CBOE index history CSV (VIX, VIX3M, SKEW) -> list of (iso_date, close)."""
    body = http_get(f"https://cdn.cboe.com/api/global/us_indices/daily_prices/{name}_History.csv",
                    timeout=30)
    out: Series = []
    for row in csv.reader(io.StringIO(body)):
        if not row or row[0] in ("DATE", "Date") or "/" not in row[0]:
            continue
        try:
            d = datetime.strptime(row[0], "%m/%d/%Y").strftime("%Y-%m-%d")
            out.append((d, float(row[-1])))
        except ValueError:
            continue
    if not out:
        raise ValueError(f"CBOE {name}: no rows")
    return out


def treasury_yield_curve(years: Iterable[int]) -> Series:
    """US Treasury daily yield curve XML -> list of (iso_date, y10 - y2)."""
    out: Series = []
    for year in years:
        url = ("https://home.treasury.gov/resource-center/data-chart-center/interest-rates/"
               f"pages/xml?data=daily_treasury_yield_curve&field_tdr_date_value={year}")
        body = http_get(url, timeout=45, retries=1)
        entries = re.findall(
            r"<d:NEW_DATE[^>]*>([\d-]+)T.*?<d:BC_2YEAR[^>]*>([\d.]+)</d:BC_2YEAR>.*?"
            r"<d:BC_10YEAR[^>]*>([\d.]+)</d:BC_10YEAR>",
            body, re.S)
        for d, y2, y10 in entries:
            out.append((d, round(float(y10) - float(y2), 2)))
    out.sort()
    if not out:
        raise ValueError("Treasury: no rows")
    return out


def multpl_current(slug: str) -> float:
    body = http_get(f"https://www.multpl.com/{slug}", timeout=25)
    m = re.search(r"Current [^:<]+[:\s]+([0-9]+\.[0-9]+)", body)
    if not m:
        raise ValueError(f"multpl {slug}: current value not found")
    return float(m.group(1))


def slickcharts_top10() -> float:
    body = http_get("https://www.slickcharts.com/sp500", timeout=25)
    rows = re.findall(r"<tr>(.*?)</tr>", body, re.S)
    weights = []
    for r in rows:
        cells = re.findall(r"<td[^>]*>(.*?)</td>", r, re.S)
        if len(cells) >= 4:
            m = re.search(r"([0-9]+\.[0-9]+)%", cells[3])
            if m:
                weights.append(float(m.group(1)))
    if len(weights) < 10:
        raise ValueError(f"SlickCharts: only {len(weights)} weights parsed")
    return round(sum(weights[:10]), 2)


NSE_INDICES = {
    "Nifty 50": "nifty50",
    "Nifty Midcap 150": "midcap150",
    "Nifty Smallcap 250": "smallcap250",
    "India VIX": "indiavix",
}


def nse_close_all(date: datetime) -> dict[str, dict[str, float | None]]:
    """Parse one NSE ind_close_all file -> {alias: {pe, pb, dy, close}}."""
    fn = f"ind_close_all_{date.strftime('%d%m%Y')}.csv"
    body = None
    for host in ("nsearchives.nseindia.com", "archives.nseindia.com"):
        try:
            body = http_get(f"https://{host}/content/indices/{fn}", timeout=25, retries=0)
            break
        except Exception:  # noqa: BLE001 - try the sibling archive host
            continue
    if body is None:
        raise ValueError(f"NSE archives: {fn} unavailable on both hosts")
    reader = csv.reader(io.StringIO(body))
    header = next(reader)
    idx = {h.strip().lower(): i for i, h in enumerate(header)}

    def col(row: list[str], *names: str) -> float | None:
        for n in names:
            i = idx.get(n)
            if i is not None and i < len(row):
                v = row[i].strip()
                if v and v != "-":
                    try:
                        return float(v)
                    except ValueError:
                        return None
        return None

    out: dict[str, dict[str, float | None]] = {}
    for row in reader:
        if not row:
            continue
        name = row[0].strip()
        if name in NSE_INDICES:
            out[NSE_INDICES[name]] = {
                "pe": col(row, "p/e"),
                "pb": col(row, "p/b"),
                "dy": col(row, "div yield"),
                "close": col(row, "closing index value"),
            }
    if "nifty50" not in out:
        raise ValueError("NSE close_all: Nifty 50 row missing")
    return out


def nse_latest() -> tuple[str, dict[str, dict[str, float | None]]]:
    """Walk back from today (IST) to the last published trading day file."""
    now_ist = datetime.now(UTC) + timedelta(hours=5, minutes=30)
    for back in range(0, 8):
        d = now_ist - timedelta(days=back)
        try:
            data = nse_close_all(d)
            return d.strftime("%Y-%m-%d"), data
        except Exception:  # noqa: BLE001 - holiday/weekend/not-yet-published
            continue
    raise ValueError("NSE archives: no file found in last 8 days")


def parse_fo_pcr(csv_body: str) -> float:
    """NIFTY put/call OI ratio (all expiries) from a UDiFF F&O bhavcopy CSV."""
    pe = ce = 0.0
    for row in csv.DictReader(io.StringIO(csv_body)):
        if row.get("TckrSymb") == "NIFTY" and row.get("FinInstrmTp") == "IDO":
            oi = float(row.get("OpnIntrst") or 0)
            if row.get("OptnTp") == "PE":
                pe += oi
            elif row.get("OptnTp") == "CE":
                ce += oi
    if not ce:
        raise ValueError("FO bhavcopy: no NIFTY call OI parsed")
    return round(pe / ce, 3)


def nse_fo_pcr() -> tuple[str, float]:
    """Latest end-of-day NIFTY PCR from the open NSE F&O bhavcopy archive."""
    now_ist = datetime.now(UTC) + timedelta(hours=5, minutes=30)
    err: Exception | None = None
    for back in range(0, 8):
        d = now_ist - timedelta(days=back)
        url = ("https://nsearchives.nseindia.com/content/fo/"
               f"BhavCopy_NSE_FO_0_0_0_{d.strftime('%Y%m%d')}_F_0000.csv.zip")
        try:
            blob = http_get_bytes(url, timeout=40)
            with zipfile.ZipFile(io.BytesIO(blob)) as z:
                body = z.read(z.namelist()[0]).decode("utf-8", "replace")
            return d.strftime("%Y-%m-%d"), parse_fo_pcr(body)
        except Exception as e:  # noqa: BLE001 - holiday/weekend/not-yet-published
            err = e
            continue
    raise ValueError(f"FO bhavcopy: none found in last 8 days (last error: {err})")


def stockanalysis_pe(symbol: str) -> float:
    """Trailing P/E scraped from stockanalysis.com's overview table."""
    body = http_get(f"https://stockanalysis.com/stocks/{symbol.lower()}/", timeout=25)
    m = re.search(r"PE Ratio</td><td[^>]*>([0-9.]+)", body)
    if not m:
        raise ValueError(f"stockanalysis {symbol}: PE Ratio not found")
    return float(m.group(1))


# ---------------------------------------------------------------------------
# Accumulated history (for sources that only expose current values)
# ---------------------------------------------------------------------------

def load_accumulated() -> Hist:
    hist: Hist = {}
    if ACCUM_FILE.exists():
        for row in csv.reader(ACCUM_FILE.open()):
            if len(row) == 3:
                hist.setdefault(row[1], []).append((row[0], float(row[2])))
    for v in hist.values():
        v.sort()
    return hist


def append_accumulated(hist: Hist, key: str, date: str, value: float | None) -> None:
    if value is None:
        return
    series = hist.setdefault(key, [])
    if any(d == date for d, _ in series):
        return
    series.append((date, float(value)))
    series.sort()


def save_accumulated(hist: Hist) -> None:
    HISTORY_DIR.mkdir(parents=True, exist_ok=True)
    with ACCUM_FILE.open("w", newline="") as f:
        w = csv.writer(f)
        for key in sorted(hist):
            for d, v in hist[key]:
                w.writerow([d, key, v])


def bootstrap_india_history(hist: Hist) -> None:
    """Seed ~10y of monthly NSE index samples (one-time, run locally)."""
    log("Bootstrapping India history (monthly samples, ~10y)...")
    now = datetime.now(UTC)
    month_starts = []
    y, m = now.year - 10, now.month
    while (y, m) <= (now.year, now.month):
        month_starts.append(datetime(y, m, 1, tzinfo=UTC))
        m += 1
        if m > 12:
            m, y = 1, y + 1
    fetched = 0
    for start in month_starts:
        got = None
        for off in range(0, 6):
            d = start + timedelta(days=off)
            iso = d.strftime("%Y-%m-%d")
            if any(k.startswith("nifty50") and any(dd == iso for dd, _ in v)
                   for k, v in hist.items()):
                got = "cached"
                break
            try:
                data = nse_close_all(d)
            except Exception:  # noqa: BLE001
                time.sleep(0.3)
                continue
            for alias, vals in data.items():
                if alias == "indiavix":
                    append_accumulated(hist, "indiavix_close", iso, vals["close"])
                else:
                    append_accumulated(hist, f"{alias}_pe", iso, vals["pe"])
                    append_accumulated(hist, f"{alias}_pb", iso, vals["pb"])
                    append_accumulated(hist, f"{alias}_dy", iso, vals["dy"])
            got = iso
            fetched += 1
            time.sleep(0.4)
            break
        log(f"  {start.strftime('%Y-%m')}: {got or 'not found'}")
    log(f"Bootstrap done: {fetched} new monthly samples.")


# ---------------------------------------------------------------------------
# Indicator definitions
# ---------------------------------------------------------------------------

def build_us_panel(ctx: Ctx) -> dict[str, Any]:
    ind = []

    def cape() -> Indicator:
        v = multpl_current("shiller-pe")
        append_accumulated(ctx["accum"], "cape", ctx["today"], v)
        if v > 32:
            st = "red"
        elif v > 25:
            st = "amber"
        else:
            st = "green"
        return dict(value=v, unit="×", status=st,
                    context=f"{v}× vs ~17 long-term average",
                    spark=sparkify(ctx["accum"].get("cape", [])))

    ind.append(make("cape", "Shiller CAPE ratio", cape,
                    "Price ÷ 10-year average inflation-adjusted earnings. The best long-run "
                    "return predictor there is; readings above ~30 have historically preceded "
                    "weak decade-ahead returns.", "multpl.com", ctx))

    def buffett() -> Indicator:
        mcap = fred_csv("BOGZ1LM893064105Q")  # Z.1 corporate equities, quarterly
        gdp = fred_csv("GDP")                 # quarterly SAAR
        series = normalize_ratio(ratio_series(mcap, gdp, lambda a, b: a / b),
                                 expect_low=0.2, expect_high=6.0)
        series = [(d, v * 100) for d, v in series]
        v = round(series[-1][1], 0)
        pct = percentile_rank([x for _, x in series], v)
        return dict(value=v, unit="% GDP", status=pct_status(pct),
                    context=(f"{v:.0f}% of GDP — {ordinal(pct)} percentile since the 1950s"
                             if pct is not None else f"{v:.0f}% of GDP"),
                    percentile=pct, spark=sparkify(series))

    ind.append(make("buffett", "Buffett Indicator (Z.1 basis)", buffett,
                    "All US corporate equities (Fed Z.1, includes unlisted companies) ÷ GDP, "
                    "so it prints higher than the famous Wilshire-based 150–200% quote — "
                    "compare the percentile, not the headline number.",
                    "FRED Z.1 (quarterly)", ctx))

    def tobin() -> Indicator:
        eq = fred_csv("NCBEILQ027S")     # corporate equities, quarterly
        nw = fred_csv("TNWMVBSNNCB")     # net worth, quarterly
        nw_map = dict(nw)
        series = [(d, v / nw_map[d]) for d, v in eq if d in nw_map and nw_map[d]]
        series = normalize_ratio(series, expect_low=0.15, expect_high=4.0)
        v = round(series[-1][1], 2)
        if v > 1.2:
            st = "red"
        elif v > 0.9:
            st = "amber"
        else:
            st = "green"
        return dict(value=v, unit="", status=st,
                    context=f"{v} vs ~0.75 long-run mean",
                    spark=sparkify(series))

    ind.append(make("tobinq", "Tobin's Q", tobin,
                    "Market value of nonfinancial companies ÷ replacement cost of their "
                    "assets. Long-run mean is ~0.75; above 1 the market prices companies "
                    "above their tangible worth.", "FRED Z.1 (quarterly)", ctx))

    def curve() -> Indicator:
        series = treasury_yield_curve([datetime.now().year - 2,
                                       datetime.now().year - 1,
                                       datetime.now().year])
        v = series[-1][1]
        last_inv = None
        for d, s in series:
            if s < 0:
                last_inv = d
        if v < 0:
            st, note = "amber", "curve is inverted — classic recession lead signal"
        elif last_inv:
            months = round((datetime.strptime(series[-1][0], "%Y-%m-%d")
                            - datetime.strptime(last_inv, "%Y-%m-%d")).days / 30)
            if months <= 24:
                st = "amber"
                note = (f"un-inverted ~{months} months ago; recessions historically strike "
                        "6–24 months AFTER un-inversion")
            else:
                st, note = "green", "normal upward slope"
        else:
            st, note = "green", "normal upward slope"
        return dict(value=v, unit="pp", status=st,
                    context=f"{v:+.2f}pp spread — {note}",
                    spark=sparkify(series))

    ind.append(make("yieldcurve", "Yield curve (10y−2y)", curve,
                    "10-year minus 2-year Treasury yield. Inversion is the most reliable "
                    "recession lead indicator — and the hit usually comes after the curve "
                    "un-inverts, not during the inversion.", "US Treasury", ctx))

    def spx_m2() -> Indicator:
        spx = spx_series()
        m2 = fred_csv("M2SL")  # $B, monthly
        series = ratio_series(spx, m2, lambda a, b: a / b)
        vals = [v for _, v in series]
        v = round(series[-1][1], 3)
        pct = percentile_rank(vals, v)
        return dict(value=v, unit="", status=pct_status(pct),
                    context=f"{ordinal(pct)} percentile of its 10-year range" if pct is not None
                    else "insufficient history",
                    percentile=pct, spark=sparkify(series))

    ind.append(make("spxm2", "S&P 500 ÷ M2", spx_m2,
                    "The index divided by the money supply — strips monetary inflation out "
                    "of 'record highs'. High percentile = stocks rich even after adjusting "
                    "for all the new money.", "Yahoo + FRED M2", ctx))

    def vix() -> Indicator:
        series = cboe_history("VIX")
        v = round(series[-1][1], 1)
        if v < 13:
            st, note = "red", "extreme complacency — classic late-bubble tell"
        elif v < 18:
            st, note = "amber", "low volatility, market pricing in calm"
        elif v <= 28:
            st, note = "green", "normal volatility regime"
        elif v <= 38:
            st, note = "amber", "elevated fear"
        else:
            st, note = "red", "panic regime"
        return dict(value=v, unit="", status=st, context=f"{v} — {note}",
                    spark=sparkify(series))

    ind.append(make("vix", "VIX (volatility)", vix,
                    "The 'fear index'. A very LOW VIX means investors are complacent — a "
                    "classic late-bubble tell. Spikes mean fear has arrived.", "CBOE", ctx))

    def hy() -> Indicator:
        series = fred_csv("BAMLH0A0HYM2")
        v = round(series[-1][1], 2)
        if v < 3.0:
            st, note = "red", "spreads extremely tight — lenders pricing in near-zero risk"
        elif v < 4.0:
            st, note = "amber", "tight spreads, credit froth building"
        elif v <= 6.5:
            st, note = "green", "normal risk premium"
        else:
            st, note = "amber", "spreads widening — credit stress"
        return dict(value=v, unit="pp", status=st,
                    context=f"{v}pp over Treasuries — {note}",
                    spark=sparkify(series))

    ind.append(make("hyspread", "High-yield credit spread", hy,
                    "Extra yield junk bonds pay over Treasuries. Very tight = investors "
                    "pricing in almost no risk (froth); widening = stress building.",
                    "FRED / ICE BofA", ctx))

    return panel("us", "US Market", ind,
                 "Your large/mid/small-cap US index exposure — classic valuation, "
                 "complacency and macro bubble measures.")


def build_india_panel(ctx: Ctx) -> dict[str, Any]:
    ind = []
    nse_date, nse = ctx.get("nse_date"), ctx.get("nse")
    hist: Hist = ctx["accum"]

    def hist_vals(key: str) -> list[float]:
        return [v for _, v in hist.get(key, [])]

    def spark_of(key: str) -> list[list[str | float]]:
        return sparkify(hist.get(key, []))

    def nifty_pe_fn() -> Indicator:
        if not nse:
            raise ValueError("NSE data unavailable")
        v = nse["nifty50"]["pe"]
        if v is None:
            raise ValueError("Nifty 50 P/E missing")
        # NSE switched to consolidated EPS in Apr 2021; earlier P/E prints
        # run ~3-5 points higher and aren't comparable.
        post2021 = [(d, x) for d, x in clean_pe(hist.get("nifty50_pe", []))
                    if d >= "2021-04-01"]
        pct = percentile_rank([x for _, x in post2021], v)
        ctx_line = (f"{v}× — {ordinal(pct)} percentile since the 2021 EPS-methodology change"
                    if pct is not None else f"{v}× (history still accumulating)")
        return dict(value=v, unit="×", status=pct_status(pct),
                    context=ctx_line, percentile=pct, spark=spark_of("nifty50_pe"))

    ind.append(make("nifty_pe", "Nifty 50 P/E", nifty_pe_fn,
                    "Price ÷ consolidated trailing earnings for India's benchmark 50. NSE "
                    "switched to consolidated EPS in 2021, so compare against the "
                    "post-2021 range more than the raw long-term average.",
                    "NSE indices archive", ctx))

    def nifty_pb() -> Indicator:
        if not nse:
            raise ValueError("NSE data unavailable")
        v = nse["nifty50"]["pb"]
        pct = percentile_rank(hist_vals("nifty50_pb"), v)
        return dict(value=v, unit="×", status=pct_status(pct),
                    context=f"{v}× — {ordinal(pct)} percentile of 10-year range" if pct is not None
                    else f"{v}×", percentile=pct, spark=spark_of("nifty50_pb"))

    ind.append(make("nifty_pb", "Nifty 50 P/B", nifty_pb,
                    "Price-to-book for the Nifty 50. Immune to the 2021 earnings-methodology "
                    "change, so it's the cleaner long-run valuation gauge for Indian "
                    "large caps.", "NSE indices archive", ctx))

    def premium(alias: str, key: str, label: str) -> Callable[[], Indicator]:
        def fn() -> Indicator:
            if not nse:
                raise ValueError("NSE data unavailable")
            pe, base = nse[alias]["pe"], nse["nifty50"]["pe"]
            if not pe or not base:
                raise ValueError("P/E missing")
            v = round((pe / base - 1) * 100, 1)
            base_hist = dict(clean_pe(hist.get("nifty50_pe", [])))
            prem_hist = [(d, (p / base_hist[d] - 1) * 100)
                         for d, p in clean_pe(hist.get(key, [])) if base_hist.get(d)]
            pct = percentile_rank([x for _, x in prem_hist], v)
            ctx_line = f"{label} P/E {pe}× vs Nifty {base}× ({v:+.0f}%)"
            if pct is not None:
                ctx_line += f" — {ordinal(pct)} percentile"
            return dict(value=v, unit="%", status=pct_status(pct),
                        context=ctx_line, percentile=pct,
                        spark=sparkify(prem_hist))
        return fn

    ind.append(make("midcap_prem", "Midcap 150 P/E premium",
                    premium("midcap150", "midcap150_pe", "Midcap"),
                    "How much more expensive midcaps are than the Nifty 50. This premium "
                    "blowing out is THE India froth signal — when it spiked in early 2024, "
                    "SEBI forced AMCs into stress-test disclosures.",
                    "NSE indices archive", ctx))

    ind.append(make("smallcap_prem", "Smallcap 250 P/E premium",
                    premium("smallcap250", "smallcap250_pe", "Smallcap"),
                    "Smallcap P/E versus the Nifty 50. Small caps carry higher risk and "
                    "historically trade at a discount — a fat premium means retail euphoria.",
                    "NSE indices archive", ctx))

    def div_yield() -> Indicator:
        if not nse:
            raise ValueError("NSE data unavailable")
        v = nse["nifty50"]["dy"]
        pct = percentile_rank(hist_vals("nifty50_dy"), v)
        return dict(value=v, unit="%", status=pct_status(pct, hi_is_froth=False),
                    context=(f"{v}% — {ordinal(pct)} percentile (low yield = expensive)"
                             if pct is not None else f"{v}%"),
                    percentile=pct, spark=spark_of("nifty50_dy"))

    ind.append(make("nifty_dy", "Nifty 50 dividend yield", div_yield,
                    "Dividends ÷ price, inverted valuation: the LOWER the yield, the more "
                    "expensive the market. Bottom-decile yield has marked every Indian "
                    "market top.", "NSE indices archive", ctx))

    subtitle = "Nifty large/mid/small valuation vs their own 10-year history."
    if nse_date:
        subtitle += f" NSE data as of {nse_date}."
    return panel("india", "India", ind, subtitle)


def build_tech_panel(ctx: Ctx) -> dict[str, Any]:
    ind = []
    hist: Hist = ctx["accum"]

    def top10() -> Indicator:
        v = slickcharts_top10()
        append_accumulated(hist, "sp500_top10", ctx["today"], v)
        if v > 35:
            st = "red"
        elif v > 30:
            st = "amber"
        else:
            st = "green"
        return dict(value=v, unit="%", status=st,
                    context=f"{v}% of the index in 10 stocks — dot-com peak was ~27%",
                    spark=sparkify(hist.get("sp500_top10", [])))

    ind.append(make("top10", "S&P 500 top-10 concentration", top10,
                    "Weight of the 10 biggest stocks in the S&P 500. Your 'index' exposure "
                    "is increasingly a bet on a handful of mega-cap tech names — this is "
                    "how much.", "SlickCharts", ctx))

    def ndx_spx() -> Indicator:
        try:
            ndx = yahoo_chart("^NDX", range_="10y")
            spx = yahoo_chart("^GSPC", range_="10y")
        except Exception:  # noqa: BLE001 - Yahoo throttled; FRED mirrors both
            ndx = fred_csv("NASDAQ100")
            spx = fred_csv("SP500")
        series = ratio_series(ndx, spx, lambda a, b: a / b)
        v = round(series[-1][1], 3)
        pct = percentile_rank([x for _, x in series], v)
        return dict(value=v, unit="", status=pct_status(pct),
                    context=f"{ordinal(pct)} percentile of 10-year range" if pct is not None
                    else "", percentile=pct, spark=sparkify(series))

    ind.append(make("ndxspx", "Nasdaq 100 ÷ S&P 500", ndx_spx,
                    "Tech's valuation premium over the broad market. At the dot-com peak "
                    "this ratio collapsed 60% peak-to-trough — it is your AAPL/tech "
                    "overweight's main risk.", "Yahoo Finance", ctx))

    def aapl_pe() -> Indicator:
        fwd = None
        try:
            q = yahoo_quote_fields("AAPL", ["trailingPE", "forwardPE"])
            raw = q.get("trailingPE")
            if not raw:
                raise ValueError("no trailingPE")
            v = round(float(raw), 1)
            fwd = q.get("forwardPE")
        except Exception:  # noqa: BLE001 - Yahoo throttles cloud IPs
            v = round(stockanalysis_pe("AAPL"), 1)
        append_accumulated(hist, "aapl_pe", ctx["today"], v)
        if v > 33:
            st = "red"
        elif v > 25:
            st = "amber"
        else:
            st = "green"
        ctx_line = f"{v}× trailing (10y median ~25×)"
        if fwd:
            ctx_line += f", {round(fwd, 1)}× forward"
        return dict(value=v, unit="×", status=st, context=ctx_line,
                    spark=sparkify(hist.get("aapl_pe", [])))

    ind.append(make("aapl_pe", "AAPL trailing P/E", aapl_pe,
                    "Apple's price ÷ trailing earnings. Apple spent 2010–2019 in the "
                    "12–20× range; the re-rating above 25× is multiple expansion, not "
                    "earnings growth.", "Yahoo / stockanalysis.com", ctx))

    return panel("tech", "US Tech & Concentration",
                 ind, "Your AAPL and tech-heavy exposure: concentration and relative "
                      "valuation measures.")


def build_fno_panel(ctx: Ctx) -> dict[str, Any]:
    ind = []
    hist: Hist = ctx["accum"]
    nse = ctx.get("nse")

    def india_vix() -> Indicator:
        v = nse["indiavix"]["close"] if nse and nse.get("indiavix") else None
        if v is None:
            raise ValueError("India VIX unavailable")
        v = round(v, 1)
        pct = percentile_rank([x for _, x in hist.get("indiavix_close", [])], v)
        if (pct is not None and pct <= 10) or v < 11:
            st, note = "red", "extreme complacency — option premium historically cheap"
        elif (pct is not None and pct <= 25) or v < 13:
            st, note = "amber", "low — market pricing in calm"
        elif v > 25:
            st, note = "amber", "elevated fear"
        else:
            st, note = "green", "normal regime"
        ctx_line = f"{v} — {note}"
        if pct is not None:
            ctx_line += f" ({ordinal(pct)} percentile of 10y)"
        return dict(value=v, unit="", status=st, context=ctx_line,
                    percentile=pct, spark=sparkify(hist.get("indiavix_close", [])))

    ind.append(make("indiavix", "India VIX", india_vix,
                    "Implied volatility of Nifty options. For an FnO trader this is the "
                    "price of optionality itself: very low VIX = cheap hedges, expensive "
                    "complacency, and violent unwinds when the regime flips.",
                    "NSE indices archive", ctx))

    def pcr() -> Indicator:
        pcr_date, v = nse_fo_pcr()
        append_accumulated(hist, "nifty_pcr", pcr_date, v)
        if v < 0.8:
            st, note = "red", "call-heavy — euphoric positioning"
        elif v < 1.0:
            st, note = "amber", "leaning bullish"
        elif v <= 1.4:
            st, note = "green", "balanced"
        else:
            st, note = "amber", "put-heavy — crowded hedging/fear"
        return dict(value=v, unit="", status=st,
                    context=f"{v} — {note} (EOD {pcr_date})",
                    spark=sparkify(hist.get("nifty_pcr", [])))

    ind.append(make("pcr", "NIFTY put/call ratio (OI)", pcr,
                    "Open-interest puts ÷ calls across all NIFTY option expiries, from "
                    "the end-of-day F&O bhavcopy. Very low = everyone is long calls "
                    "(euphoria); very high = crowded hedging.",
                    "NSE F&O bhavcopy", ctx))

    def term_structure() -> Indicator:
        vix = dict(cboe_history("VIX"))
        vix3m = cboe_history("VIX3M")
        series = [(d, round(vix[d] / v3, 3)) for d, v3 in vix3m if vix.get(d) and v3]
        v = series[-1][1]
        if v < 0.82:
            st, note = "red", "steep contango — extreme complacency"
        elif v < 0.90:
            st, note = "amber", "complacent contango"
        elif v <= 1.02:
            st, note = "green", "normal term structure"
        else:
            st, note = "red", "backwardation — stress regime is ON"
        return dict(value=v, unit="", status=st, context=f"{v} — {note}",
                    spark=sparkify(series))

    ind.append(make("vixterm", "VIX ÷ VIX3M term structure", term_structure,
                    "Spot VIX versus 3-month VIX. Deep contango (<0.85) = markets paying "
                    "nothing for near-term risk; a flip above 1.0 (backwardation) is the "
                    "single cleanest 'regime changed' alarm for option sellers.",
                    "CBOE", ctx))

    def skew() -> Indicator:
        series = cboe_history("SKEW")
        v = round(series[-1][1], 1)
        if v > 155:
            st, note = "red", "extreme tail-risk pricing — institutions buying crash protection"
        elif v > 140:
            st, note = "amber", "elevated demand for crash hedges"
        elif v >= 115:
            st, note = "green", "normal tail pricing"
        else:
            st, note = "amber", "unusually cheap tails — complacency"
        return dict(value=v, unit="", status=st, context=f"{v} — {note}",
                    spark=sparkify(series))

    ind.append(make("skew", "CBOE SKEW", skew,
                    "Prices of far out-of-the-money S&P puts vs at-the-money. High SKEW "
                    "with low VIX = smart money quietly paying up for crash insurance "
                    "while the surface looks calm.", "CBOE", ctx))

    return panel("fno", "Derivatives & Positioning", ind,
                 "What the options market is pricing — complacency, positioning and "
                 "regime signals for FnO.")


# ---------------------------------------------------------------------------
# Framework
# ---------------------------------------------------------------------------

STATUS_LABELS = {"red": "STRETCHED", "amber": "CAUTION", "green": "NORMAL"}


def make(id_: str, name: str, fn: Callable[[], Indicator],
         explainer: str, source: str, ctx: Ctx) -> Indicator:
    prev = ctx["previous"].get(id_)
    try:
        d = fn()
        d.update(id=id_, name=name, explainer=explainer, source=source,
                 stale=False, statusLabel=STATUS_LABELS[d["status"]],
                 updated=ctx["now_iso"])
        log(f"  ok    {id_}: {d['value']}{d['unit']} [{d['status']}]")
        return d
    except Exception as e:  # noqa: BLE001
        log(f"  FAIL  {id_}: {type(e).__name__}: {e}")
        if prev:
            prev["stale"] = True
            return dict(prev)
        return dict(id=id_, name=name, explainer=explainer, source=source,
                    value=None, unit="", status="na", statusLabel="NO DATA",
                    context="source unavailable — will fill on a future update",
                    stale=True, spark=[], updated=None)


def normalize_ratio(series: Series, expect_low: float, expect_high: float) -> Series:
    """Rescale a ratio series by 1000x if a unit mismatch (FRED returns some
    Z.1 series in millions via one endpoint and billions via another) pushed
    it outside the plausible range."""
    latest = series[-1][1]
    for scale in (1, 1000, 1 / 1000):
        if expect_low <= latest * scale <= expect_high:
            return [(d, v * scale) for d, v in series]
    raise ValueError(f"ratio {latest} not plausible at any unit scale")


def ratio_series(num: Series, den: Series, fn: Callable[[float, float], float]) -> Series:
    """Align two (date, value) series; den is forward-filled onto num dates."""
    den_sorted = sorted(den)
    out: Series = []
    di: int = 0
    dval: float | None = None
    for d, v in sorted(num):
        while di < len(den_sorted) and den_sorted[di][0] <= d:
            dval = den_sorted[di][1]
            di += 1
        if dval:
            out.append((d, fn(v, dval)))
    if not out:
        raise ValueError("ratio_series: no overlap")
    return out


def panel(id_: str, title: str, indicators: list[Indicator], subtitle: str) -> dict[str, Any]:
    counts = {"red": 0, "amber": 0, "green": 0, "na": 0}
    for i in indicators:
        counts[i["status"]] += 1
    scored = len(indicators) - counts["na"]
    if scored == 0:
        verdict = "No data yet"
    elif counts["red"] / scored >= 0.5:
        verdict = "Elevated risk: measures stretched"
    elif (counts["red"] + counts["amber"]) / scored >= 0.5:
        verdict = "Mixed signals: froth building, watch closely"
    else:
        verdict = "No broad froth by these measures"
    if counts["na"]:
        verdict += f" ({counts['na']} gauge{'s' if counts['na'] > 1 else ''} pending)"
    return dict(id=id_, title=title, subtitle=subtitle,
                summary=dict(**counts, verdict=verdict),
                indicators=indicators)


def main() -> None:
    bootstrap = "--bootstrap" in sys.argv
    now = datetime.now(UTC)
    ctx: Ctx = {
        "now_iso": now.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "today": now.strftime("%Y-%m-%d"),
        "previous": {},
        "accum": load_accumulated(),
    }

    if DATA_FILE.exists():
        try:
            old = json.loads(DATA_FILE.read_text())
            for p in old.get("panels", []):
                for i in p.get("indicators", []):
                    ctx["previous"][i["id"]] = i
        except Exception as e:  # noqa: BLE001
            log(f"warn: could not read previous data.json: {e}")

    if bootstrap:
        bootstrap_india_history(ctx["accum"])

    log("Fetching NSE latest...")
    try:
        ctx["nse_date"], ctx["nse"] = nse_latest()
        log(f"  ok    NSE close_all for {ctx['nse_date']}")
        for alias, vals in ctx["nse"].items():
            if alias == "indiavix":
                append_accumulated(ctx["accum"], "indiavix_close", ctx["nse_date"], vals["close"])
            else:
                for f in ("pe", "pb", "dy"):
                    append_accumulated(ctx["accum"], f"{alias}_{f}", ctx["nse_date"], vals[f])
    except Exception as e:  # noqa: BLE001
        log(f"  FAIL  NSE close_all: {e}")
        ctx["nse_date"], ctx["nse"] = None, None

    log("US panel:")
    us = build_us_panel(ctx)
    log("India panel:")
    india = build_india_panel(ctx)
    log("Tech panel:")
    tech = build_tech_panel(ctx)
    log("FnO panel:")
    fno = build_fno_panel(ctx)

    out = dict(
        generated_at=ctx["now_iso"],
        panels=[us, india, tech, fno],
    )
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    DATA_FILE.write_text(json.dumps(out, indent=1))
    save_accumulated(ctx["accum"])
    total = sum(len(p["indicators"]) for p in out["panels"])
    fresh = sum(1 for p in out["panels"] for i in p["indicators"] if not i["stale"])
    log(f"Wrote {DATA_FILE.relative_to(ROOT)} — {fresh}/{total} indicators fresh.")


if __name__ == "__main__":
    main()
