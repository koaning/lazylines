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
