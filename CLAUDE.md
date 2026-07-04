# stock-market-valuations

Live "bubble detector" dashboard (inspired by levels.io/bubble-detector) for the markets I invest in: US indices (large/mid/small), US tech (AAPL), India indices (Nifty large/mid/small), and FnO. Static page + scheduled data pipeline; see the build plan in session history.

## Development checks (run these — CI gates on them)

One-time setup: `make setup` (creates `.venv` with pytest/ruff/mypy from `requirements-dev.txt`).

- `make check` — everything CI runs: lint + types + tests. Run before every commit/push.
- `make lint` — `ruff check scripts tests` + `node --check app.js`
- `make type` — `mypy --strict` over `scripts/` (config in `pyproject.toml`)
- `make test` — `pytest` (`tests/`, no network — parsers are tested against fixtures with `fetch.http_get` monkeypatched)
- `make fetch` — refresh `data/data.json` from live sources (network; don't commit a locally-degraded data.json — FRED/Yahoo are blocked from this network, CI regenerates it)

`.github/workflows/ci.yml` runs the same gate on every PR and on pushes to main (data-only bot commits are excluded via `paths-ignore`). The `checks` job is a required status check for PR merges.

Constraints to preserve:
- `scripts/fetch.py` must stay **stdlib-only** at runtime — the Actions cron runs it with zero installs. Dev tools live in `requirements-dev.txt` only.
- Indicator/context payloads are deliberately `dict[str, Any]` (JSON-shaped, consumed verbatim by `app.js`); don't force TypedDicts on them.
- New indicator logic needs a unit test (pure logic) or a fixture-based parser test; broad `except Exception` in fetchers is intentional (stale-tolerance) and carries `# noqa: BLE001`.

## Skill routing

When the user's request matches an available skill, invoke it via the Skill tool. When in doubt, invoke the skill.

Key routing rules:
- Product ideas/brainstorming → invoke /office-hours
- Strategy/scope → invoke /plan-ceo-review
- Architecture → invoke /plan-eng-review
- Design system/plan review → invoke /design-consultation or /plan-design-review
- Full review pipeline → invoke /autoplan
- Bugs/errors → invoke /investigate
- QA/testing site behavior → invoke /qa or /qa-only
- Code review/diff check → invoke /review
- Visual polish → invoke /design-review
- Ship/deploy/PR → invoke /ship or /land-and-deploy
- Save progress → invoke /context-save
- Resume context → invoke /context-restore
