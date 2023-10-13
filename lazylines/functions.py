from lazylines import LazyLines 
import datetime as dt 
import itertools as it


def round_timestamp(ts:int, to="day"):
    mapping = {
        "day": "%Y-%m-%d",
        "week": "%Y-%W",
        "month": "%Y-%m"
    }
    if to not in mapping:
        raise ValueError(f"`to` must be in {mapping.keys}. got {to}.")
    return str(dt.datetime.fromtimestamp(ts).strftime(mapping[to]))


def to_nested_pairs(lines: LazyLines , subset_key: str="subset"):
    for ex in lines:
        for c1, c2 in it.combinations(ex[subset_key], r=2):
            yield {**ex, "subset": [c1, c2]}


def pluck_from_subset(key: LazyLines , subset_key: str="subset"):
    def func(item):
        arr = item[subset_key]
        return [ex[key] for ex in arr if key in ex]
    return func


def text_to_spacy(lines: LazyLines, nlp):
    """
    Adds a json-compatible doc key filled with spaCy data.

    Assumes a `text` key in the lines, will use it to add doc properties.
    
    Arguments:
        - lines: `LazyLines` object 
        - nlp: `spacy.Language` object, like `spacy.load("en_core_web_md")`

    **Usage**

    ```python
    import spacy 
    from lazylines import LazyLines 

    lines = LazyLines([{"text": "hello"}])
    
    lines.pipe(text_to_spacy, nlp=spacy.load("en_core_web_sm")).collect()
    ```
    """
    orig, new = lines.tee()
    texts = (ex['text'] for ex in new)
    for doc, orig in nlp.pipe(zip(texts, orig), as_tuples=True):
        yield {**orig, "doc": doc.to_json()}
