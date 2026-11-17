import pytest

def test_advisory_map_is_open_gap():
    from src.changelogfeed import advisory_map
    with pytest.raises(NotImplementedError):
        advisory_map.process({"id": "x"})

def test_subscribe_is_open_gap():
    from src.changelogfeed import subscribe
    with pytest.raises(NotImplementedError):
        subscribe.process({"id": "x"})
