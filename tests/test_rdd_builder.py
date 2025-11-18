from src.streaming.rdd_builder import parse_categories


def test_parse_categories_handles_blank_values():
    assert parse_categories(None) == tuple()
    assert parse_categories("") == tuple()
    assert parse_categories("Mexican,  Food , Nightlife") == ("Mexican", "Food", "Nightlife")

