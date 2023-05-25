from pprint import pprint
from lazylines import LazyLines
import pytest 

@pytest.fixture
def data():
    return ({"a": i, "b": i+1, "c": i % 2, "d": i % 3} for i in range(100))


def test_rename(data):
    items = LazyLines(data).rename(c="b").collect()
    for item in items:
        assert item["a"] + 1 == item["c"]


def test_select(data):
    items = LazyLines(data).select("a", "b").collect()
    for item in items:
        assert "a" in item
        assert "b" in item
        assert "c" not in item
        assert "d" not in item


def test_drop(data):
    items = LazyLines(data).drop("a", "b").collect()
    for item in items:
        assert "a" not in item
        assert "b" not in item
        assert "c" in item
        assert "d" in item


def test_nest(data):
    items = LazyLines(data).nest_by("c", "d").collect()
    pprint(items)
    assert len(items) == 6
    for item in items:
        assert "a" not in item
        assert "b" not in item
        assert "c" in item
        assert "d" in item

        
