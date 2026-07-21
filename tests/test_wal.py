from kvstore.wal import WAL


def test_append_and_replay(tmp_path):
    wal = WAL(str(tmp_path / "wal.log"))
    wal.append({"index": 1, "op": "set", "key": "a", "value": "1"})
    wal.append({"index": 2, "op": "set", "key": "b", "value": "2"})
    wal.close()

    entries = list(WAL(str(tmp_path / "wal.log")).replay())
    assert [e["key"] for e in entries] == ["a", "b"]


def test_entries_since(tmp_path):
    wal = WAL(str(tmp_path / "wal.log"))
    for i in range(1, 6):
        wal.append({"index": i, "op": "set", "key": f"k{i}", "value": str(i)})
    assert [e["index"] for e in wal.entries_since(3)] == [4, 5]


def test_torn_line_is_skipped(tmp_path):
    path = tmp_path / "wal.log"
    wal = WAL(str(path))
    wal.append({"index": 1, "op": "set", "key": "a", "value": "1"})
    wal.close()
    with open(path, "a") as f:
        f.write('{"index": 2, "op": "se')  # simulate a crash mid-write

    entries = list(WAL(str(path)).replay())
    assert len(entries) == 1 and entries[0]["key"] == "a"
