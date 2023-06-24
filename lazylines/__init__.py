from __future__ import annotations

import itertools as it
import pprint
from pathlib import Path
from typing import Tuple, Union, Dict, Callable, Optional

import srsly
import tqdm


def read_jsonl(path: Union[str, Path]) -> LazyLines:
    """Read .jsonl file and turn it into a LazyLines object."""
    return LazyLines(srsly.read_jsonl(path))


class LazyLines:
    """
    An object that can wrangle iterables of dictionaries (similar to JSONL).

    ```python
    from lazylines import LazyLines
    ```
    """

    def __init__(self, g):
        self.g = g
        self.groups = set()

    def cache(self) -> LazyLines:
        """
        Cache the result internally by turning it into a list.

        It's recommended to store the result into another variable.

        ```python
        from lazylines import LazyLines

        items = ({"a": i} for i in range(100))

        # The intermediate representation is now a list.
        cached = (LazyLines(items).cache())
        ```
        """
        return LazyLines(g=list(self.g))

    def mutate(self, **kwargs: Dict[str, Callable]) -> LazyLines:
        """
        Adds/overwrites keys in the dictionary based on lambda.

        Arguments:
            kwargs: str/callable pairs that represent keys and a function to calculate it's value

        **Usage**:

        ```python
        from lazylines import LazyLines

        items = [{"a": 2}, {"a": 3}]
        results = (LazyLines(items).mutate(b=lambda d: d["a"] * 2))
        expected = [{"a": 2, "b": 4}, {"a": 3, "b": 6}]
        assert results.collect() == expected
        ```
        """

        def new_gen():
            for item in self.g:
                for k, v in kwargs.items():
                    item[k] = v(item)
                yield item

        return LazyLines(g=new_gen())

    def keep(self, *args: Callable) -> LazyLines:
        """
        Only keep a subset of the items in the generator based on lambda.

        Arguments:
            args: functions that can be used to filter the data, if it outputs `True` it will be kept around

        **Usage**:

        ```python
        from lazylines import LazyLines

        items = [{"a": 2}, {"a": 3}]
        results = (LazyLines(items).keep(lambda d: d["a"] % 2 == 0))
        expected = [{"a": 2}]
        assert results.collect() == expected
        ```
        """

        def new_gen():
            for item in self.g:
                allowed = True
                for func in args:
                    if not func(item):
                        allowed = False
                if allowed:
                    yield item

        return LazyLines(g=new_gen())

    def unnest(self, key: str="subset") -> LazyLines:
        """
        Explodes a key, effectively un-nesting it.

        Arguments:
            key: the key to un-nest

        **Usage**:

        ```python
        from lazylines import LazyLines

        data = [{'annotator': 'a',
                 'subset': [{'accept': True, 'text': 'foo'},
                            {'accept': True, 'text': 'foobar'}]},
                {'annotator': 'b',
                 'subset': [{'accept': True, 'text': 'foo'},
                            {'accept': True, 'text': 'foobar'}]}]

        expected = [
            {'accept': True, 'annotator': 'a', 'text': 'foo'},
            {'accept': True, 'annotator': 'a', 'text': 'foobar'},
            {'accept': True, 'annotator': 'b', 'text': 'foo'},
            {'accept': True, 'annotator': 'b', 'text': 'foobar'}
        ]

        result = LazyLines(data).unnest("subset").collect()
        assert result == expected
        ```
        """

        def new_gen():
            for item in self.g:
                for value in item[key]:
                    orig = {k: v for k, v in item.items() if k != key}
                    d = {**value, **orig}
                    yield d

        return LazyLines(g=new_gen())

    def head(self, n=5) -> LazyLines:
        """
        Make a subset and only return the top `n` items.

        Arguments:
            n: the number of examples to take
        """
        if isinstance(self.g, list):
            return LazyLines(g=(i for i in self.g[:5]))

        def new_gen():
            for _ in range(n):
                yield next(self.g)

        return LazyLines(g=new_gen())

    def show(self, n: int=5) -> LazyLines:
        """
        Give a preview of the first `n` examples. 

        Arguments:
            n: the number of examples to preview
        """
        stream_orig, stream_copy = it.tee(self.g)
        for _ in range(n):
            pprint.pprint(next(stream_copy))
        return LazyLines(g=stream_orig)

    def map(self, func: Callable) -> LazyLines:
        """
        Apply a function to each item before yielding it back.
        
        Arguments:
            func: the function to call on each item
        """

        def new_gen():
            for item in self.g:
                yield func(item)

        return LazyLines(g=new_gen())

    def tee(self, n:int=2) -> Tuple[LazyLines]:
        """
        Copies the lazylines.
        
        Arguments:
            n: how often to `tee` the stream

        Usage:

        ```python
        from lazylines import LazyLines

        data = [
            {'accept': True, 'annotator': 'a', 'text': 'foo'},
            {'accept': True, 'annotator': 'a', 'text': 'foobar'},
            {'accept': True, 'annotator': 'b', 'text': 'foo'},
            {'accept': True, 'annotator': 'b', 'text': 'foobar'}
        ]

        lines1, lines2 = LazyLines(data).tee(n=2)
        lines1, lines2, lines3 = LazyLines(data).tee(n=3)
        ```
        """
        return tuple(LazyLines(g=gen) for gen in it.tee(self.g, n))

    def __iter__(self):
        return iter(self.g)

    def sort_by(self, *keys: str) -> LazyLines:
        """
        Sort the items based on a subset of the keys.
        
        Arguments:
            keys: the keys to use for sorting
        """
        return LazyLines(g=sorted(self.g, key=lambda d: tuple([d[c] for c in keys])))

    def rename(self, **kwargs: Dict[str, str]) -> LazyLines:
        """
        Rename a few keys in each item.
        
        Arguments:
            kwargs: str/str pairs that resemble the new name and old name of a key

        Usage:

        ```python
        from lazylines import LazyLines

        data = [
            {'labeller': 'a', 'text': 'foo'},
            {'labeller': 'a', 'text': 'foobar'},
            {'labeller': 'b', 'text': 'foo'},
            {'labeller': 'b', 'text': 'foobar'}
        ]

        expected = [
            {'annotator': 'a', 'text': 'foo'},
            {'annotator': 'a', 'text': 'foobar'},
            {'annotator': 'b', 'text': 'foo'},
            {'annotator': 'b', 'text': 'foobar'}
        ]

        result = (LazyLines(data).rename(annotator="labeller").collect())
        assert result == expected
        ```
        """

        def new_gen():
            for item in self.g:
                old = {k: v for k, v in item.items() if k not in kwargs.values()}
                new = {k: item[v] for k, v in kwargs.items()}
                yield {**old, **new}

        return LazyLines(g=new_gen())

    def nest_by(self, *keys: str) -> LazyLines:
        """
        Group by keys and return nested collections.

        The opposite of `.unnest()`

        Arguments:
            keys: the keys to nest by

        **Usage**:

        ```python
        from lazylines import LazyLines

        data = [
            {'accept': True, 'annotator': 'a', 'text': 'foo'},
            {'accept': True, 'annotator': 'a', 'text': 'foobar'},
            {'accept': True, 'annotator': 'b', 'text': 'foo'},
            {'accept': True, 'annotator': 'b', 'text': 'foobar'}
        ]

        expected = [
                {'annotator': 'a',
                 'subset': [{'accept': True, 'text': 'foo'},
                            {'accept': True, 'text': 'foobar'}]},
                {'annotator': 'b',
                 'subset': [{'accept': True, 'text': 'foo'},
                            {'accept': True, 'text': 'foobar'}]}
        ]

        result = LazyLines(data).nest_by("annotator")
        assert result.collect() == expected
        ```
        """
        groups = {}
        for example in self.g:
            key = tuple(example.get(arg, None) for arg in keys)
            if key not in groups:
                groups[key] = []
            for arg in keys:
                del example[arg]
            groups[key].append(example)
        result = []
        for key, values in groups.items():
            result.append({**{k: v for k, v in zip(keys, key)}, "subset": values})
        return LazyLines(result)

    def progress(self, desc:Optional[str]=None) -> LazyLines:
        """Adds a progress bar. Meant to be used early."""
        stream_orig, stream_copy = it.tee(self.g)
        total = sum(1 for _ in stream_copy)

        def new_gen():
            for ex in tqdm.tqdm(stream_orig, total=total, desc=desc):
                yield ex

        return LazyLines(g=new_gen())

    def collect(self) -> LazyLines:
        """
        Turns the (final) sequence into a list.
        
        Note that, as a consequence, this will also empty the lazyline object.
        """
        return [ex for ex in self.g]

    def write_jsonl(
        self, path, append: bool = False, append_new_line: bool = True
    ) -> LazyLines:
        """
        Write everything into a jsonl file.
        
        Note that, as a consequence, this will also empty the lazyline object.
        """
        srsly.write_jsonl(path, self.g, append=append, append_new_line=append_new_line)

    def select(self, *keys: str) -> LazyLines:
        """
        Only select specific keys from each dictionary.
        
        Arguments:
            keys: the names of the keys to be kept around

        Usage:

        ```python
        from lazylines import LazyLines

        data = [
            {'accept': True, 'annotator': 'a', 'text': 'foo'},
            {'accept': True, 'annotator': 'a', 'text': 'foobar'},
            {'accept': True, 'annotator': 'b', 'text': 'foo'},
            {'accept': True, 'annotator': 'b', 'text': 'foobar'}
        ]

        expected = [
            {'annotator': 'a', 'text': 'foo'},
            {'annotator': 'a', 'text': 'foobar'},
            {'annotator': 'b', 'text': 'foo'},
            {'annotator': 'b', 'text': 'foobar'}
        ]

        result = LazyLines(data).select("annotator", "text")
        assert result.collect() == expected
        ```
        """

        def new_gen():
            for ex in self.g:
                yield {k: v for k, v in ex.items() if k in keys}

        return LazyLines(g=new_gen())

    def drop(self, *args) -> LazyLines:
        """
        Drop specific keys from each dictionary.
        
        Arguments:
            keys: the names of the keys to be kept around
        
        Usage:

        ```python
        from lazylines import LazyLines

        data = [
            {'accept': True, 'annotator': 'a', 'text': 'foo'},
            {'accept': True, 'annotator': 'a', 'text': 'foobar'},
            {'accept': True, 'annotator': 'b', 'text': 'foo'},
            {'accept': True, 'annotator': 'b', 'text': 'foobar'}
        ]

        expected = [
            {'annotator': 'a', 'text': 'foo'},
            {'annotator': 'a', 'text': 'foobar'},
            {'annotator': 'b', 'text': 'foo'},
            {'annotator': 'b', 'text': 'foobar'}
        ]

        result = LazyLines(data).drop("accept")
        assert result.collect() == expected
        ```
        """

        def new_gen():
            for ex in self.g:
                yield {k: v for k, v in ex.items() if k not in args}

        return LazyLines(g=new_gen())

    def pipe(self, func, *args, **kwargs) -> LazyLines:
        """Call a function over the entire generator."""
        return LazyLines(g=func(self, *args, **kwargs))

    def foreach(self, func, *args, **kwargs) -> LazyLines:
        """Just call a function on each dictionary, but pass the original forward."""

        def new_gen():
            for ex in self.g:
                func(ex, *args, **kwargs)
                yield ex

        return LazyLines(g=new_gen())
    
    def agg(self, **kwargs: Dict[str, Callable]):
        data = [ex for ex in self.g]
        return {
            k: func(data) for k, func in kwargs.items()
        }
