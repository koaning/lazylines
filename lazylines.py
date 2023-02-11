import time 
import srsly
from clumper import Clumper

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

    def collect(self):
        return [ex for ex in self.g]

# print(list(read_jsonl("examples.jsonl").collect()))
tic = time.time()
c = read_jsonl("examples.jsonl").mutate(text2 = lambda d: d['text'] * 2, text3 = lambda d: d['text'] * 3).head(100).collect()
toc = time.time()

print(toc - tic)



tic = time.time()
Clumper.read_jsonl("examples.jsonl").mutate(text2 = lambda d: d['text'] * 2, text3 = lambda d: d['text'] * 3).head(100).collect()
toc = time.time()

print(toc - tic)