from pathlib import Path

from lazylines import read_csv, read_jsonl


def test_read_csv_local(tmp_path):
    """Test reading local CSV files with various options."""
    # Create test CSV
    csv_file = tmp_path / "test.csv"
    csv_file.write_text("name,age,city\nAlice,30,New York\nBob,25,LA\n")

    # Basic read
    result = read_csv(csv_file).collect()
    assert len(result) == 2
    assert result[0] == {"name": "Alice", "age": "30", "city": "New York"}

    # With head() for row limit
    result = read_csv(csv_file).head(1).collect()
    assert len(result) == 1

    # Custom delimiter
    tsv_file = tmp_path / "test.tsv"
    tsv_file.write_text("name\tage\nAlice\t30\n")
    result = read_csv(tsv_file, delimiter="\t").collect()
    assert result[0] == {"name": "Alice", "age": "30"}

    # Custom fieldnames (first row becomes data)
    result = read_csv(csv_file, fieldnames=["col1", "col2", "col3"]).collect()
    assert len(result) == 3
    assert result[0] == {"col1": "name", "col2": "age", "col3": "city"}

    # Method chaining
    result = read_csv(csv_file).mutate(age_plus_10=lambda d: str(int(d["age"]) + 10)).select("name", "age_plus_10").collect()
    assert result[0] == {"name": "Alice", "age_plus_10": "40"}


def test_read_jsonl_local():
    """Test reading local JSONL file."""
    jsonl_path = Path(__file__).parent / "pokemon.jsonl"
    result = read_jsonl(jsonl_path).head(3).collect()
    assert len(result) == 3
