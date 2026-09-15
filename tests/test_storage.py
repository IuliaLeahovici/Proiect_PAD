import storage
def test_queue_persistence_roundtrip(tmp_path, monkeypatch):
    monkeypatch.setattr(storage, "STORAGE_DIR", str(tmp_path))
    messages = [
        {
            "messageId": "m1",
            "topic": "notificari",
            "payload": "Salut"
        },
        {
            "messageId": "m2",
            "topic": "notificari",
            "payload": "Salut 2"
        },
    ]
    storage.save_queue_to_disk("sub-test", messages)
    loaded = storage.load_queue_from_disk("sub-test")
    assert loaded == messages
def test_queue_can_be_cleared_after_ack(tmp_path, monkeypatch):
    monkeypatch.setattr(storage, "STORAGE_DIR", str(tmp_path))
    messages = [{"messageId": "m1", "payload": "Salut"}]
    storage.save_queue_to_disk("sub-test", messages)
    storage.save_queue_to_disk("sub-test", [])
    assert storage.load_queue_from_disk("sub-test") == []
def test_dlq_persists_message_and_reason(tmp_path, monkeypatch):
    monkeypatch.setattr(storage, "STORAGE_DIR", str(tmp_path))
    monkeypatch.setattr(storage, "DLQ_FILE", str(tmp_path / "dlq.json"))
    message = {
        "messageId": "dead-1",
        "topic": "notificari",
        "payload": "nu poate fi procesat"
    }
    storage.save_to_dlq(message, "Exceeded max delivery retries without ACK")
    data = storage.load_queue_from_disk("nonexistent")
    assert data == []
    import json
    with open(storage.DLQ_FILE, "r", encoding="utf-8") as f:
        dlq = json.load(f)
    assert len(dlq) == 1
    assert dlq[0]["messageId"] == "dead-1"
    assert dlq[0]["dlqReason"] == "Exceeded max delivery retries without ACK"