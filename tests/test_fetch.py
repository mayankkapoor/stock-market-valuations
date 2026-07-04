"""Unit tests for scripts/fetch.py — pure logic and parsers, no network.

Network-touching parsers are tested by monkeypatching fetch.http_get with
realistic fixture payloads captured from the real sources.
"""
from datetime import UTC, datetime, timedelta

import fetch
import pytest

# ---------------------------------------------------------------------------
# Scoring helpers
# ---------------------------------------------------------------------------


class TestPercentileRank:
    def test_needs_20_observations(self):
        assert fetch.percentile_rank([1.0] * 19, 1.0) is None

    def test_rank_of_max_is_100(self):
        assert fetch.percentile_rank([float(i) for i in range(100)], 99.0) == 100

    def test_rank_of_below_min_is_0(self):
        assert fetch.percentile_rank([float(i) for i in range(100)], -1.0) == 0

    def test_median(self):
        hist = [float(i) for i in range(1, 101)]
        assert fetch.percentile_rank(hist, 50.0) == 50

    def test_ignores_none(self):
        hist = [None] * 50 + [float(i) for i in range(30)]
        assert fetch.percentile_rank(hist, 29.0) == 100


class TestPctStatus:
    def test_none_is_green(self):
        assert fetch.pct_status(None) == "green"

    @pytest.mark.parametrize("pct,expected", [(95, "red"), (90, "red"), (80, "amber"),
                                              (75, "amber"), (74, "green"), (0, "green")])
    def test_thresholds(self, pct, expected):
        assert fetch.pct_status(pct) == expected

    def test_inverted_direction(self):
        # Low dividend yield = expensive = froth.
        assert fetch.pct_status(5, hi_is_froth=False) == "red"
        assert fetch.pct_status(80, hi_is_froth=False) == "green"


class TestOrdinal:
    @pytest.mark.parametrize("n,s", [(1, "1st"), (2, "2nd"), (3, "3rd"), (4, "4th"),
                                     (11, "11th"), (12, "12th"), (13, "13th"),
                                     (21, "21st"), (78, "78th"), (99, "99th")])
    def test_suffixes(self, n, s):
        assert fetch.ordinal(n) == s


class TestCleanPe:
    def test_drops_junk(self):
        series = [("2020-01-01", 25.0), ("2020-06-01", 44633.7),
                  ("2020-07-01", None), ("2020-08-01", 0.0), ("2021-01-01", 30.0)]
        assert fetch.clean_pe(series) == [("2020-01-01", 25.0), ("2021-01-01", 30.0)]


class TestSparkify:
    def test_empty(self):
        assert fetch.sparkify([]) == []

    def test_downsamples_to_budget(self):
        today = datetime.now(UTC)
        series = [((today - timedelta(days=i)).strftime("%Y-%m-%d"), float(i))
                  for i in range(600, 0, -1)]
        out = fetch.sparkify(series)
        assert len(out) <= fetch.SPARK_POINTS + 1
        assert out[-1] == [series[-1][0], series[-1][1]]  # last point kept

    def test_old_series_kept_when_nothing_recent(self):
        series = [("2010-01-01", 1.0), ("2010-02-01", 2.0)]
        assert fetch.sparkify(series) == [["2010-01-01", 1.0], ["2010-02-01", 2.0]]


class TestNormalizeRatio:
    def test_no_scaling_needed(self):
        s = [("2026-01-01", 1.8)]
        assert fetch.normalize_ratio(s, 0.15, 4.0) == [("2026-01-01", 1.8)]

    def test_scales_up_1000x(self):
        s = [("2026-01-01", 0.0018)]
        out = fetch.normalize_ratio(s, 0.15, 4.0)
        assert out[0][1] == pytest.approx(1.8)

    def test_scales_down_1000x(self):
        s = [("2026-01-01", 1800.0)]
        out = fetch.normalize_ratio(s, 0.15, 4.0)
        assert out[0][1] == pytest.approx(1.8)

    def test_implausible_raises(self):
        with pytest.raises(ValueError):
            fetch.normalize_ratio([("2026-01-01", 5e9)], 0.15, 4.0)


class TestRatioSeries:
    def test_forward_fills_denominator(self):
        num = [("2026-01-01", 10.0), ("2026-02-01", 20.0), ("2026-03-01", 30.0)]
        den = [("2026-01-01", 2.0), ("2026-03-01", 3.0)]
        out = fetch.ratio_series(num, den, lambda a, b: a / b)
        assert out == [("2026-01-01", 5.0), ("2026-02-01", 10.0), ("2026-03-01", 10.0)]

    def test_num_before_denominator_starts_is_skipped(self):
        num = [("2025-12-01", 1.0), ("2026-01-15", 10.0)]
        den = [("2026-01-01", 2.0)]
        out = fetch.ratio_series(num, den, lambda a, b: a / b)
        assert out == [("2026-01-15", 5.0)]

    def test_no_overlap_raises(self):
        with pytest.raises(ValueError):
            fetch.ratio_series([("2020-01-01", 1.0)], [("2021-01-01", 1.0)], lambda a, b: a / b)


# ---------------------------------------------------------------------------
# Panel verdicts
# ---------------------------------------------------------------------------


def ind(status):
    return {"status": status}


class TestPanelVerdict:
    def test_majority_red_is_elevated(self):
        p = fetch.panel("x", "X", [ind("red"), ind("red"), ind("green")], "s")
        assert p["summary"]["verdict"].startswith("Elevated risk")

    def test_single_scored_red_is_elevated_with_pending_note(self):
        p = fetch.panel("x", "X", [ind("red"), ind("na"), ind("na")], "s")
        assert p["summary"]["verdict"].startswith("Elevated risk")
        assert "(2 gauges pending)" in p["summary"]["verdict"]

    def test_ambers_are_mixed(self):
        p = fetch.panel("x", "X", [ind("amber"), ind("amber"), ind("green")], "s")
        assert p["summary"]["verdict"].startswith("Mixed signals")

    def test_all_green_is_calm(self):
        p = fetch.panel("x", "X", [ind("green"), ind("green")], "s")
        assert p["summary"]["verdict"].startswith("No broad froth")

    def test_all_na(self):
        p = fetch.panel("x", "X", [ind("na")], "s")
        assert p["summary"]["verdict"].startswith("No data yet")

    def test_counts(self):
        p = fetch.panel("x", "X", [ind("red"), ind("amber"), ind("green"), ind("na")], "s")
        assert (p["summary"]["red"], p["summary"]["amber"],
                p["summary"]["green"], p["summary"]["na"]) == (1, 1, 1, 1)


# ---------------------------------------------------------------------------
# make(): stale fallback behavior
# ---------------------------------------------------------------------------


def ctx_with(previous=None):
    return {"previous": previous or {}, "now_iso": "2026-07-04T00:00:00Z",
            "today": "2026-07-04", "accum": {}}


class TestMake:
    def test_success_decorates_indicator(self):
        d = fetch.make("t", "T", lambda: {"value": 1.0, "unit": "", "status": "red",
                                          "context": "c", "spark": []},
                       "expl", "src", ctx_with())
        assert d["statusLabel"] == "STRETCHED"
        assert d["stale"] is False
        assert d["id"] == "t"

    def test_failure_keeps_previous_marked_stale(self):
        prev = {"id": "t", "value": 9.9, "status": "amber", "stale": False}
        d = fetch.make("t", "T", lambda: 1 / 0, "expl", "src",
                       ctx_with(previous={"t": prev}))
        assert d["value"] == 9.9
        assert d["stale"] is True

    def test_failure_without_previous_yields_no_data_card(self):
        d = fetch.make("t", "T", lambda: 1 / 0, "expl", "src", ctx_with())
        assert d["status"] == "na"
        assert d["statusLabel"] == "NO DATA"
        assert d["stale"] is True


# ---------------------------------------------------------------------------
# Parsers against fixtures (http_get monkeypatched)
# ---------------------------------------------------------------------------

NSE_CSV = """\
Index Name,Index Date,Open Index Value,High Index Value,Low Index Value,Closing Index Value,Points Change,Change(%),Volume,Turnover (Rs. Cr.),P/E,P/B,Div Yield
Nifty 50,03-07-2026,24375.65,24378.15,24252.35,24270.85,95.15,.39,326634751,25955.73,20.92,3.17,1.25
India VIX,03-07-2026,12.2875,12.2875,11.65,11.8,-0.49,-3.99,-,-,-,-,-
Nifty Smallcap 250,03-07-2026,18073.05,18085.9,17979,17996.25,12.9,.07,749682138,23103.94,36.07,3.79,.65
Nifty Midcap 150,03-07-2026,23031.5,23031.55,22866.8,22884.35,-50.55,-.22,978343249,32932.34,29.32,4.8,.67
"""

FRED_CSV = """\
observation_date,T10Y2Y
2026-06-30,0.32
2026-07-01,.
2026-07-02,0.35
"""

FRED_API_JSON = ('{"observations": ['
                 '{"date": "2026-07-01", "value": "."},'
                 '{"date": "2026-07-02", "value": "0.35"}]}')

CBOE_CSV = """\
DATE,OPEN,HIGH,LOW,CLOSE
07/01/2026,17.110000,17.300000,15.970000,16.590000
07/02/2026,17.050000,17.210000,15.790000,16.150000
"""

TREASURY_XML = """
<entry><content><m:properties>
<d:NEW_DATE m:type="Edm.DateTime">2026-07-02T00:00:00</d:NEW_DATE>
<d:BC_1MONTH m:type="Edm.Double">3.70</d:BC_1MONTH>
<d:BC_2YEAR m:type="Edm.Double">4.14</d:BC_2YEAR>
<d:BC_3YEAR m:type="Edm.Double">4.16</d:BC_3YEAR>
<d:BC_10YEAR m:type="Edm.Double">4.49</d:BC_10YEAR>
</m:properties></content></entry>
"""

MULTPL_HTML = '<meta name="description" content="Current Shiller PE Ratio is 41.60, a change">'

SLICK_ROW = ("<tr><td>{rank}</td><td><a href=\"/x\">Company {rank}</a></td>"
             "<td>SYM{rank}</td><td>{w}%</td><td>$100</td></tr>")
SLICK_HTML = "<table>" + "".join(
    SLICK_ROW.format(rank=i, w=round(5.0 - i * 0.3, 2)) for i in range(1, 15)) + "</table>"


class TestParsers:
    def test_nse_close_all(self, monkeypatch):
        monkeypatch.setattr(fetch, "http_get", lambda url, **kw: NSE_CSV)
        out = fetch.nse_close_all(datetime(2026, 7, 3))
        assert out["nifty50"]["pe"] == 20.92
        assert out["nifty50"]["pb"] == 3.17
        assert out["nifty50"]["dy"] == 1.25
        assert out["smallcap250"]["pe"] == 36.07
        assert out["indiavix"]["close"] == 11.8
        assert out["indiavix"]["pe"] is None  # '-' parses to None

    def test_nse_close_all_missing_nifty_raises(self, monkeypatch):
        header = NSE_CSV.splitlines()[0]
        monkeypatch.setattr(fetch, "http_get", lambda url, **kw: header + "\n")
        with pytest.raises(ValueError):
            fetch.nse_close_all(datetime(2026, 7, 3))

    def test_nse_close_all_tries_second_host(self, monkeypatch):
        calls = []

        def fake(url, **kw):
            calls.append(url)
            if "nsearchives" in url:
                raise OSError("blocked")
            return NSE_CSV

        monkeypatch.setattr(fetch, "http_get", fake)
        out = fetch.nse_close_all(datetime(2026, 7, 3))
        assert out["nifty50"]["pe"] == 20.92
        assert len(calls) == 2

    def test_fred_csv_keyless(self, monkeypatch):
        monkeypatch.delenv("FRED_API_KEY", raising=False)
        monkeypatch.setattr(fetch, "http_get", lambda url, **kw: FRED_CSV)
        out = fetch.fred_csv("T10Y2Y")
        assert out == [("2026-06-30", 0.32), ("2026-07-02", 0.35)]  # '.' rows skipped

    def test_fred_api_with_key(self, monkeypatch):
        seen = {}

        def fake(url, **kw):
            seen["url"] = url
            return FRED_API_JSON

        monkeypatch.setenv("FRED_API_KEY", "testkey")
        monkeypatch.setattr(fetch, "http_get", fake)
        out = fetch.fred_csv("T10Y2Y")
        assert out == [("2026-07-02", 0.35)]
        assert "api.stlouisfed.org" in seen["url"]
        assert "testkey" in seen["url"]

    def test_cboe_history(self, monkeypatch):
        monkeypatch.setattr(fetch, "http_get", lambda url, **kw: CBOE_CSV)
        out = fetch.cboe_history("VIX")
        assert out == [("2026-07-01", 16.59), ("2026-07-02", 16.15)]

    def test_treasury_yield_curve(self, monkeypatch):
        monkeypatch.setattr(fetch, "http_get", lambda url, **kw: TREASURY_XML)
        out = fetch.treasury_yield_curve([2026])
        assert out == [("2026-07-02", 0.35)]  # 4.49 - 4.14

    def test_multpl_current(self, monkeypatch):
        monkeypatch.setattr(fetch, "http_get", lambda url, **kw: MULTPL_HTML)
        assert fetch.multpl_current("shiller-pe") == 41.60

    def test_multpl_current_not_found_raises(self, monkeypatch):
        monkeypatch.setattr(fetch, "http_get", lambda url, **kw: "<html>nope</html>")
        with pytest.raises(ValueError):
            fetch.multpl_current("shiller-pe")

    def test_slickcharts_top10_sums_first_ten(self, monkeypatch):
        monkeypatch.setattr(fetch, "http_get", lambda url, **kw: SLICK_HTML)
        expected = round(sum(round(5.0 - i * 0.3, 2) for i in range(1, 11)), 2)
        assert fetch.slickcharts_top10() == expected

    def test_slickcharts_too_few_rows_raises(self, monkeypatch):
        monkeypatch.setattr(fetch, "http_get", lambda url, **kw: "<table></table>")
        with pytest.raises(ValueError):
            fetch.slickcharts_top10()


# ---------------------------------------------------------------------------
# Accumulated history round-trip
# ---------------------------------------------------------------------------


class TestAccumulated:
    def test_append_dedupes_by_date_and_sorts(self):
        hist = {}
        fetch.append_accumulated(hist, "k", "2026-07-02", 2.0)
        fetch.append_accumulated(hist, "k", "2026-07-01", 1.0)
        fetch.append_accumulated(hist, "k", "2026-07-02", 99.0)  # dupe date ignored
        fetch.append_accumulated(hist, "k", "2026-07-03", None)  # None ignored
        assert hist["k"] == [("2026-07-01", 1.0), ("2026-07-02", 2.0)]

    def test_save_load_roundtrip(self, tmp_path, monkeypatch):
        monkeypatch.setattr(fetch, "HISTORY_DIR", tmp_path)
        monkeypatch.setattr(fetch, "ACCUM_FILE", tmp_path / "accumulated.csv")
        hist = {"a": [("2026-01-01", 1.5)], "b": [("2026-01-02", 2.5)]}
        fetch.save_accumulated(hist)
        assert fetch.load_accumulated() == hist

    def test_load_missing_file_is_empty(self, tmp_path, monkeypatch):
        monkeypatch.setattr(fetch, "ACCUM_FILE", tmp_path / "nope.csv")
        assert fetch.load_accumulated() == {}
