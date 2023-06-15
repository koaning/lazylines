import datetime as dt 

def round_timestamp(ts, to="day"):
    mapping = {
        "day": "%Y-%m-%d",
        "week": "%Y-%W",
        "month": "%Y-%m"
    }
    if to not in mapping:
        raise ValueError(f"`to` must be in {mapping.keys}. got {to}.")
    return str(dt.datetime.fromtimestamp(ts).strftime(mapping[to]))


def to_nested_pairs(lines, subset_key="subset"):
    for ex in lines:
        for c1, c2 in it.combinations(ex[subset_key], r=2):
            yield {**ex, "subset": [c1, c2]}
