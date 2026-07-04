# Local quality gates — same commands CI runs. `make check` before pushing.
# Uses .venv if present (make setup creates it), system python3 otherwise.

VENV_BIN := $(shell [ -d .venv ] && echo .venv/bin/ || echo "")
PY       := $(VENV_BIN)python3

.PHONY: setup lint type test check fetch

setup:            ## one-time: create .venv with dev tools
	python3 -m venv .venv
	.venv/bin/pip install --quiet -r requirements-dev.txt

lint:             ## ruff lint + import order
	$(VENV_BIN)ruff check scripts tests
	node --check app.js

type:             ## mypy --strict over scripts/
	$(VENV_BIN)mypy

test:             ## unit tests (no network)
	$(PY) -m pytest

check: lint type test  ## everything CI gates on

fetch:            ## refresh data/data.json from live sources
	$(PY) scripts/fetch.py
