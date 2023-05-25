from lazylines.functions import round_timestamp

def test_round_timestamp():
    assert round_timestamp(1545730073, to="day") == "2018-12-25"
    assert round_timestamp(1545730073, to="week") == "2018-52"
    assert round_timestamp(1545730073, to="month") == "2018-12"
