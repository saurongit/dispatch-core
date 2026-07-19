PYTHON ?= python3

.PHONY: demo test coverage lint check

demo:
	PYTHONPATH=src $(PYTHON) -m dispatch_core

test:
	PYTHONPATH=src $(PYTHON) -m pytest

coverage:
	PYTHONPATH=src $(PYTHON) -m coverage erase
	PYTHONPATH=src $(PYTHON) -m coverage run -m pytest
	PYTHONPATH=src $(PYTHON) -m coverage report

lint:
	$(PYTHON) -m ruff check src tests

check: lint
	PYTHONPATH=src $(PYTHON) -m compileall -q src tests
	PYTHONPATH=src $(PYTHON) -m pytest
