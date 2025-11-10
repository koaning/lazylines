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

install:
	uv sync --all-extras

pypi:
	uv build
	uv publish

clean:
	rm -rf **/.ipynb_checkpoints **/.pytest_cache **/__pycache__ **/**/__pycache__ .ipynb_checkpoints .pytest_cache
	uv run ruff format lazylines tests
	uv run ruff check --fix lazylines tests
