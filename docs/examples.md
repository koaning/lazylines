In this section we will show a few examples that use this library. As a motivating example, we will 
dive into a subset of [the Google Emotions dataset](https://github.com/google-research/google-research/tree/master/goemotions) 
to dive into annotator agreement.

You can download this dataset yourself here. Or you can fetch it via: 

```
wget https://raw.githubusercontent.com/koaning/lazylines/main/go_emotions_subset.jsonl
```

You can have a look at the first example via:

```python
from lazylines import read_jsonl

read_jsonl("go_emotions_subset.jsonl").show(1)
```

Which will show you: 

```python
{
    'annotations': {'excitement': 0, 'love': 0},
    'rater_id': 1,
    'text': 'That game hurt.',
    'timestamp': 1548381039
}
```

Each example here contains two annotations for a piece of text indicating if an
annotator believed that the text contained "excitement" or "love". The "text" 
key shows the text in question and the timestamp should be the timestamp of the 
annotation.

## Agreement per item.

As a first exercise, let's explore how often annotators agree on the examples. 
This will involve many steps, but the first step would be to nest by the text key. 

```python
from lazylines import read_jsonl

read_jsonl("go_emotions_subset.jsonl").nest_by("text").show(1)
```

The data will now be reshaped to include a `subset` key that's grouped by
the `text` key. 

```python
{
    'subset': [{'annotations': {'excitement': 0, 'love': 0},
                'rater_id': 1,
                'timestamp': 1548381039},
                {'annotations': {'excitement': 0, 'love': 0},
                'rater_id': 72,
                'timestamp': 1548381039},
                {'annotations': {'excitement': 0, 'love': 0},
                'rater_id': 52,
                'timestamp': 1548381039}],
    'text': 'That game hurt.'
}
```

This is nice first step. But there's a lot of information in the `subset` that
we don't need for now. We won't need the timestamp and we're typically only 
interested in a single label at a time. So let's include this knowledge by 
selecting only a few columns before we nest. 

```python
(
    read_jsonl("go_emotions_subset.jsonl")                # point to a file on disk
     .mutate(annot=lambda d: d['annotations']['love'])    # just focus on `love` annotation
     .select("text", "annot", "rater_id")                 # only care about these keys
     .show(2)                                             # show the first two items
)
```

Here's what the first two examples look like. 

```python
{'annot': 0, 'rater_id': 1, 'text': 'That game hurt.'}
{'annot': 1, 'rater_id': 18, 'text': 'Man I love reddit.'}
```

When we now nest ...

```python
(
    read_jsonl("go_emotions_subset.jsonl")
     .mutate(annot=lambda d: d['annotations']['love'])    # just focus on `love` annotation
     .select("text", "annot", "rater_id")                 # only care about these keys
     .nest_by("text")                                     # now do the nesting
     .show(2)                                             # show the first two items
)
```

... it looks a bunch cleaner. 

```python
{
    'subset': [{'annot': 0, 'rater_id': 1},
               {'annot': 0, 'rater_id': 72},
               {'annot': 0, 'rater_id': 52}],
    'text': 'That game hurt.'
}
```

### Using `.pipe`

This might be a great time to turn out logic into a function
and to re-use that in a `.pipe()` method. 

```python
from lazylines import LazyLines

def nest_towards_label(lines: LazyLines, label: str):
    return (lines
         .mutate(annot=lambda d: d['annotations'][label])
         .select("text", "annot", "rater_id")
         .nest_by("text")
    )

(
    read_jsonl("go_emotions_subset.jsonl")
        .pipe(nest_towards_label, label="love")
        .show(1)
)
```

The output is exactly the same, but this improves the readability
and re-usability of our code. Next up, let's "pluck" out the values
that we really care about.

### Utility functions 

This might be a good time to start using a utility function to "pluck" 
the values that we're interested in from the `"subset"` key. When you
nest, you keep the keys around. But at this point we might only care about
the values of the `"love"` key. 

```python
(
    read_jsonl("go_emotions_subset.jsonl")
        .pipe(nest_towards_label, label="love")
        .mutate(annot=pluck_from_subset("annot"))
        .show(1)
)
```

This shows: 

```python
{
    'annot': [0, 0, 0],
    'subset': [{'annot': 0, 'rater_id': 1},
               {'annot': 0, 'rater_id': 72},
               {'annot': 0, 'rater_id': 52}],
    'text': 'That game hurt.'
}
```

### Making relevant subsets

We now have a "annot" list of values attached to each text, which
can use for statistics. But let's only consider looking at items
that have at least three annotators. 

```python
(
    read_jsonl("go_emotions_subset.jsonl")
        .pipe(nest_towards_label, label="love")
        .keep(lambda d: len(d['subset']) >= 3)
        .mutate(annot=pluck_from_subset("annot"))
        .show(1)
)
```

Next, we will add a column that indictes if all annotators agree. 
Once we have those two statistics, we won't need the `"subset"` 
key around anymore.

```python
(
    read_jsonl("go_emotions_subset.jsonl")
        .pipe(nest_towards_label, label="love")
        .keep(lambda d: len(d['subset']) >= 3)
        .mutate(annot=pluck_from_subset("annot"),
                agreement = lambda d: len(set(d['annot'])) == 1)
        .drop("subset")
        .show(1)
)
```

This shows:

```python
{'agreement': True, 'annot': [0, 0, 0], 'text': 'That game hurt.'}
```

Time for one more `nest_by`. Let's now nest everything by the `"agreement"`
key and use this result to count how many items had full agreement.

```python
(
    read_jsonl("go_emotions_subset.jsonl")
        .pipe(nest_towards_label, label="love")
        .keep(lambda d: len(d['subset']) >= 3)
        .mutate(annot=pluck_from_subset("annot"),
                agreement = lambda d: len(set(d['annot'])) == 1)
        .drop("subset")
        .nest_by("agreement")
        .mutate(n=lambda d: len(d['subset']))
        .drop("subset")
        .show(2)
)
```

This results in: 

```python
{'agreement': True, 'n': 5373}
{'agreement': False, 'n': 307}
```

So it seems, at least on this dataset on this label, that about %5 of all
annotators disagree. That's pretty interesting, but let's clean up our code
one more time. 

### Final cleanup 

```python
from lazylines import LazyLines

def nest_towards_label(lines: LazyLines, label: str):
    return (lines
         .mutate(annot=lambda d: d['annotations'][label])
         .select("text", "annot", "rater_id")
         .nest_by("text")
    )

def calculate_agreement(lines: LazyLines, min_annotators:int = 3):
    return (lines
            .keep(lambda d: len(d['subset']) >= min_annotators)
            .mutate(annot=pluck_from_subset("annot"),
                    agreement = lambda d: len(set(d['annot'])) == 1)
            .drop("subset")
            .nest_by("agreement")
            .mutate(n=lambda d: len(d['subset']))
            .drop("subset")
    )

(
    read_jsonl("go_emotions_subset.jsonl")
        .pipe(nest_towards_label, label = "love")
        .pipe(calculate_agreement, min_annotators = 3)
        .show(2)
)
```

This is a pretty nice, and flexible, pipeline! You can change the minimum 
number of annotators that you expect or switch the label pretty easily. 

