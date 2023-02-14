black:
	black lazylines tests setup.py --check

flake:
	flake8 lazylines tests setup.py

test:
	pytest

interrogate:
	interrogate -vv --ignore-nested-functions --ignore-semiprivate --ignore-private --ignore-magic --ignore-module --ignore-init-method --fail-under 100 tests
	interrogate -vv --ignore-nested-functions --ignore-semiprivate --ignore-private --ignore-magic --ignore-module --ignore-init-method --fail-under 100 lazylines

check: clean black flake test interrogate

install:
	python -m pip install -e ".[dev]"

pypi:
	python setup.py sdist
	python setup.py bdist_wheel --universal
	twine upload dist/*

clean:
	rm -rf **/.ipynb_checkpoints **/.pytest_cache **/__pycache__ **/**/__pycache__ .ipynb_checkpoints .pytest_cache
	black lazylines tests setup.py
	isort lazylines tests setup.py
	autoflake lazylines/*.py  --in-place
	autoflake tests/*.py  --in-place
