<img src="icon.png" width="125" height="125" align="right" />

Lazylines is a small Python library that makes it easier to work with
lists of dictionaries as well as JSONL files. It was originally designed
to help handle some boilerplate for some of my custom [Prodigy](https://prodi.gy)
recipes.

## Quickstart 

To give a demo of what the base useage might look like, let's download a .jsonl file. 

```
wget https://calmcode.io/datasets/pokemon.jsonl
```

This file has data that looks like this: 

```json
{"attack": 49, "hp": 45, "name": "Bulbasaur", "total": 318, "type": ["Grass", "Poison"]}
{"attack": 62, "hp": 60, "name": "Ivysaur", "total": 405, "type": ["Grass", "Poison"]}
```


```python
from lazylines import read_jsonl

(
    read_jsonl("tests/pokemon.jsonl")
      .unnest("type")
      .nest_by("type")
      .mutate(count=lambda d: len(d['subset']))
      .drop("subset")
      .collect()
)
```

Let's go through everything happening line-by-line. 

1. First we read the file with `read_jsonl`. Internally, it's stored as a generator, so no data is actually being read just yet. 
2. Next, we unnest the list in the `"type"` key. This turns an item with a nested example into two examples each containing one of the nested items.  
3. Next, we nest by the type again. This allows us to have an item for each `"type"` together with a `"subset"` key that contains all relevant examples that belong to the `"type"`. 
4. Next, we add a key called `"count"` by calculating the length of the subset. 
5. Next, we drop the `"subset"` key. 
6. Finally, we `collect()` the data. All the previous steps were "lazy", only when we call `collect()` do we actually perform the operations in the chain. 

This is what the output looks like. 

```python
[{'type': 'Grass', 'count': 95},
 {'type': 'Poison', 'count': 62},
 {'type': 'Fire', 'count': 64},
 {'type': 'Flying', 'count': 101},
 {'type': 'Dragon', 'count': 50},
 {'type': 'Water', 'count': 126},
 {'type': 'Bug', 'count': 72},
 {'type': 'Normal', 'count': 102},
 {'type': 'Electric', 'count': 50},
 {'type': 'Ground', 'count': 67},
 {'type': 'Fairy', 'count': 40},
 {'type': 'Fighting', 'count': 53},
 {'type': 'Psychic', 'count': 90},
 {'type': 'Rock', 'count': 58},
 {'type': 'Steel', 'count': 49},
 {'type': 'Ice', 'count': 38},
 {'type': 'Ghost', 'count': 46},
 {'type': 'Dark', 'count': 51}]
```

There's a _lot_ that you can do just by nesting and unnesting and this library tries to make it easy to perform common data wrangling tasks without having to resort to heavy dependencies.

## Fun Features 

There are a few fun features to mention out loud. 

### Progress bars 

If you have long running tasks you can keep track of the progress via the `.progress()` method. 

```python
import time 
from lazylines import read_jsonl

(
    read_jsonl("tests/pokemon.jsonl")
      .progress()
      .foreach(lambda d: time.sleep(0.02))
      .collect()
)
```

The `foreach` method runs a function for each item and then returns
the original item. The `progress` method adds the progress bar. 

### Intermediate results 

It can help to "see" what is happening in each step in a pipeline. 
For these use-cases you can use the `.show()` method. 


```python
from lazylines import read_jsonl

(
    read_jsonl("tests/pokemon.jsonl")
      .show(2)
      .unnest("type")
      .show(2)
)
```

The first call to `.show(2)` will render: 

```python
{'attack': 49,
 'hp': 45,
 'name': 'Bulbasaur',
 'total': 318,
 'type': ['Grass', 'Poison']}
{'attack': 62,
 'hp': 60,
 'name': 'Ivysaur',
 'total': 405,
 'type': ['Grass', 'Poison']}
```

The second call to `.show(2)` will render: 

```python
{'attack': 49, 'hp': 45, 'name': 'Bulbasaur', 'total': 318, 'type': 'Grass'}
{'attack': 49, 'hp': 45, 'name': 'Bulbasaur', 'total': 318, 'type': 'Poison'}
```


## Learn More 

To learn more, check the [API guide](/api/). 