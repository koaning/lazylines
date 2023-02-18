<img src="icon.png" width="125" height="125" align="right" />

# lazylines 

This project contains a minimal method-chained API to deal with `.jsonl` files. 
It's made as a personal companion for the `db-out` output from Prodigy. The API
is inspired by [dplyr](https://dplyr.tidyverse.org/) and is designed to be lazy.

At the moment this repo is still a work in progress. 

### Stuff I'd like to do. 

- [ ] add a submodule with easy aggregation functions
  - [ ] count()
  - [ ] mean() 
  - [ ] unique()
  - [ ] n_unique()

Methods I still gotta add:

- [ ] `.sort()`
- [ ] `.rename()`
- [ ] `.dedup()`

Things that might be nice:

- [ ] some spaCy helpers that can attach specific information in the right format
- [ ] something that can automate the docs for me
