format:
	uv run ruff format lazylines tests

lint:
	uv run ruff check lazylines tests

lint-fix:
	uv run ruff check --fix lazylines tests

test:
	uv run pytest

interrogate:
	uv run interrogate -vv --ignore-nested-functions --ignore-semiprivate --ignore-private --ignore-magic --ignore-module --ignore-init-method --fail-under 100 tests
	uv run interrogate -vv --ignore-nested-functions --ignore-semiprivate --ignore-private --ignore-magic --ignore-module --ignore-init-method --fail-under 100 lazylines

check: format lint test interrogate

setup:
	@echo "Setting up development environment..."
	@command -v uv >/dev/null 2>&1 || { echo "Installing uv..."; curl -LsSf https://astral.sh/uv/install.sh | sh; }
	@if [ -d .venv ]; then \
		echo "Removing existing .venv..."; \
		rm -rf .venv; \
	fi
	@echo "Creating virtual environment..."
	uv venv
	@echo "Installing dependencies..."
	uv sync --all-extras
	@echo "Installing pre-commit hooks..."
	uv run pre-commit install
	@echo "Setup complete! Activate the virtualenv with: source .venv/bin/activate"

install:
	uv sync --all-extras

pypi:
	uv build
	uv publish

clean:
	rm -rf **/.ipynb_checkpoints **/.pytest_cache **/__pycache__ **/**/__pycache__ .ipynb_checkpoints .pytest_cache
	uv run ruff format lazylines tests
	uv run ruff check --fix lazylines tests
