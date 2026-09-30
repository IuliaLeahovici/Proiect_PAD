import time

from broker import MessageBroker

class FakeSocket:
    def __init__(self):
        self.sent = []
    def sendall(self, data):
        self.sent.append(data)
def make_message(message_id="m1"):
    return {
        "action": "publish",
        "topic": "notificari",
        "clientId": "publisher-1",
        "messageId": message_id,
        "correlationId": "corr-1",
        "messageType": "notification",
        "schemaVersion": 1,
        "occurredAt": "2026-09-13T15:29:10+00:00",
        "payload": "Salut"
    }

def test_publish_routes_message_to_subscriber():
    broker = MessageBroker()
    sock = FakeSocket()
    broker.subscriptions["notificari"] = {"sub-1"}
    broker.active_sockets["sub-1"] = sock
    broker.client_queues["sub-1"] = []
    broker.delivery_meta["sub-1"] = {}
    broker.process_publish(make_message("m1"))
    assert len(broker.client_queues["sub-1"]) == 1
    assert broker.client_queues["sub-1"][0]["messageId"] == "m1"
    assert len(sock.sent) == 1
def test_ack_removes_message_from_queue():
    broker = MessageBroker()
    broker.client_queues["sub-1"] = [make_message("m1")]
    broker.delivery_meta["sub-1"] = {
        "m1": {
            "retries": 1,
            "lastSent": time.time()
        }
    }
    broker.process_ack("sub-1", "m1")
    assert broker.client_queues["sub-1"] == []
    assert "m1" not in broker.delivery_meta["sub-1"]

def test_retry_happens_after_ack_timeout():
    broker = MessageBroker()
    sock = FakeSocket()
    broker.active_sockets["sub-1"] = sock
    broker.client_queues["sub-1"] = [make_message("m1")]
    broker.delivery_meta["sub-1"] = {
        "m1": {
            "retries": 1,
            "lastSent": time.time() - 6
        }
    }
    with broker.lock:
        broker._flush_queue_locked("sub-1")
    assert len(sock.sent) == 1
    assert broker.delivery_meta["sub-1"]["m1"]["retries"] == 2

def test_message_goes_to_dlq_after_max_attempts():
    broker = MessageBroker()
    sock = FakeSocket()
    broker.active_sockets["sub-1"] = sock
    broker.client_queues["sub-1"] = [make_message("dead-1")]
    broker.delivery_meta["sub-1"] = {
        "dead-1": {
            "retries": 3,
            "lastSent": time.time()
        }
    }
    with broker.lock:
        broker._flush_queue_locked("sub-1")
    assert broker.client_queues["sub-1"] == []
    assert "dead-1" not in broker.delivery_meta["sub-1"]
    assert len(broker.dlq) == 1
    assert broker.dlq[0]["messageId"] == "dead-1"