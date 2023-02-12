This project contains a minimal method-chained API to deal with `.jsonl` files. 
It's made as a personal companion for the `db-out` output from Prodigy. The API
is inspired by [dplyr](https://dplyr.tidyverse.org/) and is designed to be lazy.

At the moment this repo is still a work in progress. I'm still uncertain about the name.

### Stuff I'd like to do. 

- [ ] decide if I like `nest_by` better than `group_by` 
- [ ] think about how to do aggregations

Methods I still gotta add:

- [ ] `.select()`
- [ ] `.foreach()`
- [ ] `.drop()`
- [ ] `.sort()`
- [ ] `.pipe()`

Things that might be nice:

- [ ] some spaCy helpers that can attach specific information in the right format
- [ ] something that can automate the docs for me
- [ ] a column selector that's faster than using lambdas all the time 
- [ ] maybe something clever with pydantic? I think `foreach` should have that covered though
- [ ] a sublibrary/command line interface for some common Prodigy tasks 
