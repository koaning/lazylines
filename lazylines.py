import time 
import srsly
from clumper import Clumper

def read_jsonl(path):
    return LazyClumper(srsly.read_jsonl(path))


class LazyClumper:
    def __init__(self, g):
        self.g = g
        self.groups = set()

    def assign(self, **kwargs):
        def new_gen():
            for item in self.g:
                for k, v in kwargs.items():
                    item[k] = v(item)
                    yield item
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
c = read_jsonl("examples.jsonl").assign(text2 = lambda d: d['text'] * 2, text3 = lambda d: d['text'] * 3).head(100).collect()
toc = time.time()

print(toc - tic)



tic = time.time()
Clumper.read_jsonl("examples.jsonl").mutate(text2 = lambda d: d['text'] * 2, text3 = lambda d: d['text'] * 3).head(100).collect()
toc = time.time()

print(toc - tic)