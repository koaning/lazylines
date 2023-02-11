import tqdm 
import time 
import pprint
import srsly
from clumper import Clumper
import itertools as it

def read_jsonl(path):
    return LazyClumper(srsly.read_jsonl(path))


class LazyClumper:
    def __init__(self, g):
        self.g = g
        self.groups = set()
    
    def cache(self):
        return LazyClumper(g=list(self.g))

    def mutate(self, **kwargs):
        def new_gen():
            for item in self.g:
                for k, v in kwargs.items():
                    item[k] = v(item)
                    yield item
        return LazyClumper(g=new_gen())
    
    def keep(self, *args):
        def new_gen():
            for item in self.g:
                for func in args:
                    if func(item):
                        yield item
        return LazyClumper(g=new_gen())

    def explode(self, key):
        def new_gen():
            for item in self.g:
                for value in item[key]:
                        yield {**item, key: value}
        return LazyClumper(g=new_gen())
    
    def unpack(self, key):
        def new_gen():
            for item in self.g:
                addition = {**item[key]}
                del item[key]
                yield {**item, **addition}
        return LazyClumper(g=new_gen())

    def head(self, n=5):
        if isinstance(self.g, list):
            return LazyClumper(g = (i for i in self.g[:5]))
        def new_gen():
            for _ in range(n):
                yield next(self.g)
        return LazyClumper(g=new_gen())
    
    def show(self, n=5):
        stream_orig, stream_copy = it.tee(self.g)
        for _ in range(n):
            pprint.pprint(next(stream_copy))
        return LazyClumper(g=stream_orig)
    
    def map(self, func):
        def new_gen():
            for item in self.g:
                yield func(item)
        return LazyClumper(g=new_gen())
    
    def tee(self, n=2):
        return tuple(LazyClumper(g=gen) for gen in it.tee(self.g, n=n))

    def agg(self, *args, **kwargs):
        """The args represent the groups, the kwargs hold the operations.
        
        ```python
        clump.agg("a", "b", 
                  n=len, 
                  mean=lambda d: sum([_['col'] for _ in d])/len(d))
        ```
        """
        stream_orig, stream_copy = self.tee()
        for arg in args:
            stream_orig, stream_copy = stream_copy.tee()
            values = set(stream_copy.map(lambda d: d[arg]))
            for val in values:
                subset = self.keep(lambda d: d[arg] == val)
                # yield {}
                
    def progress(self):
        stream_orig, stream_copy = it.tee(self.g)
        total = sum(1 for _ in stream_copy)
        return LazyClumper(g=tqdm.tqdm(stream_orig, total=total))

    def collect(self):
        return [ex for ex in self.g]
    
    def write_jsonl(self, path):
        srsly.write_jsonl(path, self.g)

# print(list(read_jsonl("examples.jsonl").collect()))

# examples = ({"text": f"example {i}"} for i in range(10_000))
# srsly.write_jsonl("examples.jsonl", examples)

def sleep_pass(item):
    time.sleep(0.1)
    return item

tic = time.time()
c = read_jsonl("examples.jsonl").map(sleep_pass).mutate(text2 = lambda d: d['text'] * 2, text3 = lambda d: d['text'] * 3).head(1).collect()
toc = time.time()
print(toc - tic)



tic = time.time()
Clumper.read_jsonl("examples.jsonl").mutate(text2 = lambda d: d['text'] * 2, text3 = lambda d: d['text'] * 3).head(100).collect()
toc = time.time()

print(toc - tic)