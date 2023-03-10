import pytest
from mktestdocs import check_docstring, get_codeblock_members

from lazylines import LazyLines


# Note the use of `__qualname__`, makes for pretty output
@pytest.mark.parametrize(
    "obj", get_codeblock_members(LazyLines), ids=lambda d: d.__qualname__
)
def test_member(obj):
    """Checks the docstrings as a unit test."""
    check_docstring(obj)
