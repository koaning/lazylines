import tqdm 
import time 
import pprint
import srsly
import itertools as it

def read_jsonl(path):
    return LazyLines(srsly.read_jsonl(path))


class LazyLines:
    def __init__(self, g):
        self.g = g
        self.groups = set()
    
    def cache(self):
        return LazyLines(g=list(self.g))

    def mutate(self, **kwargs):
        def new_gen():
            for item in self.g:
                for k, v in kwargs.items():
                    item[k] = v(item)
                    yield item
        return LazyLines(g=new_gen())
    
    def keep(self, *args):
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
        if isinstance(self.g, list):
            return LazyLines(g = (i for i in self.g[:5]))
        def new_gen():
            for _ in range(n):
                yield next(self.g)
        return LazyLines(g=new_gen())
    
    def show(self, n=5):
        stream_orig, stream_copy = it.tee(self.g)
        for _ in range(n):
            pprint.pprint(next(stream_copy))
        return LazyLines(g=stream_orig)
    
    def map(self, func):
        def new_gen():
            for item in self.g:
                yield func(item)
        return LazyLines(g=new_gen())
    
    def tee(self, n=2):
        return tuple(LazyLines(g=gen) for gen in it.tee(self.g, n))
    
    def __iter__(self):
        return self.g

    def group_by(self, *args):
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
            result.append({
                **{k: v for k, v in zip(args, key)},
                "subset": values
            })
        return LazyLines(result)
                 
    def progress(self):
        stream_orig, stream_copy = it.tee(self.g)
        total = sum(1 for _ in stream_copy)
        return LazyLines(g=tqdm.tqdm(stream_orig, total=total))

    def collect(self):
        return [ex for ex in self.g]
    
    def write_jsonl(self, path):
        srsly.write_jsonl(path, self.g)
    
    def select(self, *args):
        def new_gen():
            for ex in self.g:
                yield ex

# print(list(read_jsonl("examples.jsonl").collect()))

import random
examples = ({"number": i, "group_a": random.random() < 0.5, "group_b": random.random() < 0.5} for i in range(10_000))
srsly.write_jsonl("examples.jsonl", examples)

def sleep_pass(item):
    time.sleep(0.0001)
    return item

import pprint
read_jsonl("examples.jsonl").progress().map(sleep_pass).group_by("group_a", "group_b").show(4)

# tic = time.time()
# c1 = read_jsonl("examples.jsonl").map(key("number")).collect()
# toc = time.time()
# print(toc - tic)

# tic = time.time()
# c2 = read_jsonl("examples.jsonl").map(lambda d: d["number"]).collect()
# toc = time.time()
# print(toc - tic)


# tic = time.time()
# Clumper.read_jsonl("examples.jsonl").mutate(text2 = lambda d: d['text'] * 2, text3 = lambda d: d['text'] * 3).head(100).collect()
# toc = time.time()

# print(toc - tic)