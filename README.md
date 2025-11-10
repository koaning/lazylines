<img src="icon.png" width="125" height="125" align="right" />

### lazylines

This project contains a minimal method-chained API to deal with `.jsonl` and `.csv` files.
The API is inspired by [dplyr](https://dplyr.tidyverse.org/) and is designed to be lazy.
Everything is a generator until you call `.collect()`.

## Features

- Read `.jsonl` files from local paths or URLs
- Read `.csv` files from local paths or URLs
- Lazy evaluation - data is only loaded as needed
- Method chaining for data transformation
- Support for custom delimiters and field names

## Quick Examples

```python
from lazylines import read_jsonl, read_csv

# Read JSONL from URL
pokemon = read_jsonl("https://calmcode.io/static/data/pokemon.jsonl")
for p in pokemon.head(5):
    print(p['name'])

# Read CSV from local file
data = read_csv("data.csv")

# Read CSV from URL with head() for row limit
iris = read_csv("https://raw.githubusercontent.com/mwaskom/seaborn-data/master/iris.csv").head(100)

# Chain operations
results = (
    read_csv("data.csv")
    .mutate(new_col=lambda d: d["age"] * 2)
    .keep(lambda d: d["age"] > 25)
    .select("name", "new_col")
    .collect()
)
```

The best way to explore is to check the docs on GitHub pages. 
