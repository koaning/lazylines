from __future__ import annotations

import itertools as it
import pprint
from pathlib import Path
from typing import Tuple, Union

import srsly
import tqdm


def read_jsonl(path: Union[str, Path]) -> LazyLines:
    """Read .jsonl file and turn it into a LazyLines object."""
    return LazyLines(srsly.read_jsonl(path))


class LazyLines:
    """
    An object that can wrangle .jsonl-like files.

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

        It's recommended to store this into another variable to enjoy
        the speedup that comes at the cost of memory.

        ```python
        from lazylines import LazyLines

        items = ({"a": i} for i in range(100))

        # The intermediate representation is now a list.
        cached = (LazyLines(items).cache())
        ```
        """
        return LazyLines(g=list(self.g))

    def mutate(self, **kwargs) -> LazyLines:
        """
        Adds/overwrites keys in the dictionary based on lambda.

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

    def keep(self, *args) -> LazyLines:
        """
        Only keep a subset of the items in the generator based on lambda.

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
                for func in args:
                    if func(item):
                        yield item

        return LazyLines(g=new_gen())

    def unnest(self, key: str) -> LazyLines:
        """
        Explodes a key, effectively un-nesting it.

        Arguments:
            key: the key to use while nesting

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

        result = LazyLines(data).unnest("subset")
        assert result.collect() == expected
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
        """
        if isinstance(self.g, list):
            return LazyLines(g=(i for i in self.g[:5]))

        def new_gen():
            for _ in range(n):
                yield next(self.g)

        return LazyLines(g=new_gen())

    def show(self, n=5) -> LazyLines:
        """
        Give a preview of the first `n` examples.
        """
        stream_orig, stream_copy = it.tee(self.g)
        for _ in range(n):
            pprint.pprint(next(stream_copy))
        return LazyLines(g=stream_orig)

    def map(self, func) -> LazyLines:
        """Apply a function to each item before yielding it back."""

        def new_gen():
            for item in self.g:
                yield func(item)

        return LazyLines(g=new_gen())

    def tee(self, n=2) -> Tuple[LazyLines]:
        """Copies the lazylines."""
        return tuple(LazyLines(g=gen) for gen in it.tee(self.g, n))

    def __iter__(self):
        return self.g

    def sort_by(self, *cols) -> LazyLines:
        """Sort the items."""
        return LazyLines(g=sorted(self.g, key=lambda d: tuple([d[c] for c in cols])))

    def rename(self, **kwargs) -> LazyLines:
        """Rename a few keys in each item."""

        def new_gen():
            for item in self.g:
                new_items = {k: item[v] for k, v in kwargs.items()}
                yield {**item, **new_items}

        return LazyLines(g=new_gen())

    def nest_by(self, *args) -> LazyLines:
        """
        Group by keys and return nested collections.

        The opposite of `.unnest()`

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
            key = tuple(example.get(arg, None) for arg in args)
            if key not in groups:
                groups[key] = []
            for arg in args:
                del example[arg]
            groups[key].append(example)
        result = []
        for key, values in groups.items():
            result.append({**{k: v for k, v in zip(args, key)}, "subset": values})
        return LazyLines(result)

    def progress(self) -> LazyLines:
        """Adds a progress bar. Meant to be used early."""
        stream_orig, stream_copy = it.tee(self.g)
        total = sum(1 for _ in stream_copy)
        return LazyLines(g=tqdm.tqdm(stream_orig, total=total))

    def collect(self) -> LazyLines:
        """Turns the collection into a list."""
        return [ex for ex in self.g]

    def write_jsonl(
        self, path, append: bool = False, append_new_line: bool = True
    ) -> LazyLines:
        """Write everything into a jsonl file again."""
        srsly.write_jsonl(path, self.g, append=append, append_new_line=append_new_line)

    def select(self, *args) -> LazyLines:
        """Only select specific keys from each dictionary."""

        def new_gen():
            for ex in self.g:
                yield {k: v for k, v in ex.items() if k in args}

        return LazyLines(g=new_gen())

    def drop(self, *args) -> LazyLines:
        """Drop specific keys from each dictionary."""

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
    
    def agg(self, **kwargs):
        data = [ex for ex in self.g]
        return {
            k: func(data) for k, func in kwargs.items()
        }
