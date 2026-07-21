from kvstore.store import Store


def test_set_get_delete():
    s = Store()
    s.apply({"op": "set", "key": "x", "value": "1"})
    assert s.get("x") == "1"
    s.apply({"op": "set", "key": "x", "value": "2"})
    assert s.get("x") == "2"
    s.apply({"op": "del", "key": "x"})
    assert s.get("x") is None
    assert s.size() == 0


def test_delete_missing_is_noop():
    s = Store()
    s.apply({"op": "del", "key": "ghost"})
    assert s.size() == 0
