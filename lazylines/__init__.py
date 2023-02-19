from typing import List 
import itertools as it
import pprint

import srsly
import tqdm


def read_jsonl(path):
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

    def cache(self):
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


    def mutate(self, **kwargs):
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

    def keep(self, *args):
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

    def explode(self, key):
        def new_gen():
            for item in self.g:
                for value in item[key]:
                    yield {**item, key: value}

        return LazyLines(g=new_gen())

    def unpack(self, key):
        def new_gen():
            for item in self.g:
                addition = {**item[key]}
                del item[key]
                yield {**item, **addition}

        return LazyLines(g=new_gen())

    def head(self, n=5):
        """
        Make a subset and only return the top `n` items.
        """
        if isinstance(self.g, list):
            return LazyLines(g=(i for i in self.g[:5]))

        def new_gen():
            for _ in range(n):
                yield next(self.g)

        return LazyLines(g=new_gen())

    def show(self, n=5):
        """
        Give a preview of the first `n` examples.
        """
        stream_orig, stream_copy = it.tee(self.g)
        for _ in range(n):
            pprint.pprint(next(stream_copy))
        return LazyLines(g=stream_orig)

    def map(self, func):
        """Apply a function to each item before yielding it back."""

        def new_gen():
            for item in self.g:
                yield func(item)

        return LazyLines(g=new_gen())

    def tee(self, n=2):
        """Copies the lazylines."""
        return tuple(LazyLines(g=gen) for gen in it.tee(self.g, n))

    def __iter__(self):
        return self.g

    def nest_by(self, *args):
        """
        Group by keys and return nested collections.

        The opposite of `.explode()`
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

    def progress(self):
        """Adds a progress bar. Meant to be used early."""
        stream_orig, stream_copy = it.tee(self.g)
        total = sum(1 for _ in stream_copy)
        return LazyLines(g=tqdm.tqdm(stream_orig, total=total))

    def collect(self):
        """Turns the collection into a list."""
        return [ex for ex in self.g]

    def write_jsonl(self, path):
        """Write everything into a jsonl file again."""
        srsly.write_jsonl(path, self.g)

    def select(self, *args):
        """Only select specific keys from each dictionary."""

        def new_gen():
            for ex in self.g:
                yield {k: v for k, v in ex.items() if v in args}

        return LazyLines(g=new_gen())

    def drop(self, *args):
        """Drop specific keys from each dictionary."""

        def new_gen():
            for ex in self.g:
                yield {k: v for k, v in ex.items() if v not in args}

        return LazyLines(g=new_gen())

    def pipe(self, func, *args, **kwargs):
        """Call a function over the entire generator."""
        return LazyLines(g=func(self.g, *args, **kwargs))

    def foreach(self, func, *args, **kwargs):
        """Just call a function on each dictionary, but pass the original forward."""
        stream_orig, stream_copy = self.tee()
        for item in stream_copy:
            func(item, *args, **kwargs)
        return LazyLines(g=stream_orig)
