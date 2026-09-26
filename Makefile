# CairnDB Development Makefile

.PHONY: help install test coverage mutation mutation-results azure-integration lint format type-check clean demo docs docs-serve

help:
	@echo "CairnDB Development Commands"
	@echo "=============================="
	@echo ""
	@echo "Building & Testing:"
	@echo "  make install      Install package in development mode"
	@echo "  make test         Run all tests"
	@echo "  make coverage     Run tests with coverage report"
	@echo "  make mutation     Run mutation testing (mutmut)"
	@echo "  make mutation-results  Show surviving mutants from last run"
	@echo "  make azure-integration Run the live Azure suite (needs CAIRNDB_AZURE_* env)"
	@echo "  make lint         Run ruff linter"
	@echo "  make format       Format code with black"
	@echo "  make type-check   Run mypy type checker"
	@echo "  make all          Run format, lint, type-check, and test"
	@echo ""
	@echo "Documentation:"
	@echo "  make docs         Build the HTML docs into docs/_build/html (warnings fail)"
	@echo "  make docs-serve   Serve the docs with live reload on http://127.0.0.1:8000"
	@echo ""
	@echo "Running:"
	@echo "  make demo         Run the committer + reader demo (no server)"
	@echo ""
	@echo "Cleanup:"
	@echo "  make clean        Remove build artifacts"
	@echo ""

install:
	pip install -e ".[dev]"

test:
	pytest tests/ -v

coverage:
	pytest --cov=cairndb --cov-report=term-missing --cov-report=html tests/

mutation:
	mutmut run

mutation-results:
	mutmut results

# Requires an existing container and credentials, either exported:
#   export CAIRNDB_AZURE_CONTAINER=<container>
#   export CAIRNDB_AZURE_CONNECTION_STRING='...'  # or CAIRNDB_AZURE_ACCOUNT_URL
# or kept in a plain VAR=value env file and passed via AZURE_ENV:
#   make azure-integration AZURE_ENV=env
azure-integration:
	@f='$(AZURE_ENV)'; \
	if [ -n "$$f" ]; then \
		case "$$f" in */*) ;; *) f="./$$f";; esac; \
		set -a; . "$$f"; set +a; \
	fi; \
	CAIRNDB_AZURE_INTEGRATION=1 pytest tests/integration/test_azure_live.py -v

lint:
	ruff check src/ tests/

format:
	black src/ tests/ examples/

type-check:
	mypy src/

clean:
	find . -type d -name __pycache__ -exec rm -rf {} + 2>/dev/null || true
	find . -type f -name "*.pyc" -delete
	find . -type f -name "*.pyo" -delete
	find . -type d -name "*.egg-info" -exec rm -rf {} + 2>/dev/null || true
	rm -rf build/ dist/ .coverage htmlcov/ .pytest_cache/ .mypy_cache/ docs/_build/

demo:
	CAIRNDB_STORAGE_TYPE=filesystem \
	CAIRNDB_STORAGE_PATH=./demo-data \
	CAIRNDB_DB_PATH=./demo-projection.db \
	CAIRNDB_POLL_INTERVAL=1 \
	python -m cairndb.client.run

docs:
	sphinx-build -W --keep-going -b html docs docs/_build/html

docs-serve:
	sphinx-autobuild docs docs/_build/html --watch src

all: format lint type-check test
	@echo ""
	@echo "All checks passed! ✓"
