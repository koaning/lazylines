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


def pluck_from_subset(key:str, subset_key:str="subset"):
    def func(item):
        arr = item[subset_key]
        return [ex[key] for ex in arr if key in ex]
    return func


class _MeanAccumulator:
    def __init__(self, name):
        self.name = name
        self.accum = 0
        self.n = 0
    
    def __call__(self, ex):
        self.accum += ex[self.name]
        self.n += 1
        return self.accum

class _CountAccumulator:
    def __init__(self, name:str=None):
        self.name = name
        self.accum = 0
    
    def __call__(self, ex):
        if self.name is None:
            self.accum += 1
        else:
            self.accum += 1 if self.name in ex else 0
        return self.accum

def calc_mean(col:str):
    """Can be used to calculate the mean of a key in a LazyLines collection"""
    return f"mean_{col}", _MeanAccumulator(col)

def count(col:str=None):
    """Can be used to count the number of items in a LazyLines collection"""
    name = f"count_{col}"
    if col is None:
        name = "count"
    return name, _CountAccumulator(col)

def calc_agreement(lines: LazyLines, label: str):
    return (lines
         .nest_by("text")
         .mutate(annot=pluck_from_subset("love"))
         .drop("subset")
         .keep(lambda d: len(d['annot']) >= 3)
         .mutate(agreement = lambda d: len(set(d['annot'])) == 1)
         .nest_by("agreement")
         .mutate(n=lambda d: len(d['subset']))
         .drop("subset")
         .show(2)
    )
