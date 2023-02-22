import os

from setuptools import find_packages, setup

base_packages = ["srsly>=2.0.8", "tqdm>=4.64.1"]

test_packages = [
    "pytest>=5.4.3",
    "black>=19.10b0",
    "flake8>=3.8.3",
    "mktestdocs>=0.1.0",
    "interrogate>=1.2.0",
    "isort==5.12.0",
    "autoflake==2.0.1",
]

docs_packages = [
    "mkdocs>=1.1",
    "mkdocs-material>=4.6.3",
    "mkdocstrings>=0.8.0",
]

dev_packages = test_packages + docs_packages


def read(fname):
    return open(os.path.join(os.path.dirname(__file__), fname)).read()


setup(
    name="lazylines",
    version="0.0.1",
    author="Vincent D. Warmerdam",
    long_description=read("README.md"),
    packages=find_packages(include=["lazylines", "lazylines.*"]),
    install_requires=base_packages,
    extras_require={"dev": dev_packages},
)
