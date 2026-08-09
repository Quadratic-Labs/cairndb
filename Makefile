# CairnDB Development Makefile

.PHONY: help install test coverage lint format type-check clean demo

help:
	@echo "CairnDB Development Commands"
	@echo "=============================="
	@echo ""
	@echo "Building & Testing:"
	@echo "  make install      Install package in development mode"
	@echo "  make test         Run all tests"
	@echo "  make coverage     Run tests with coverage report"
	@echo "  make lint         Run ruff linter"
	@echo "  make format       Format code with black"
	@echo "  make type-check   Run mypy type checker"
	@echo "  make all          Run format, lint, type-check, and test"
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
	rm -rf build/ dist/ .coverage htmlcov/ .pytest_cache/ .mypy_cache/

demo:
	CAIRNDB_STORAGE_TYPE=filesystem \
	CAIRNDB_STORAGE_PATH=./demo-data \
	CAIRNDB_DB_PATH=./demo-projection.db \
	CAIRNDB_POLL_INTERVAL=1 \
	python -m cairndb.client.run

all: format lint type-check test
	@echo ""
	@echo "All checks passed! ✓"
