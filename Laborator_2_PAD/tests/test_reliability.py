from concurrent import futures

import broker_grpc
import messaging_pb2 as pb
from helpers import make_request, subscribe, wait_disconnected, wait_until

def test_message_without_ack_is_retried_then_goes_to_dlq(env, monkeypatch):
    broker, stub = env
    monkeypatch.setattr(broker_grpc, "ACK_TIMEOUT", 0.2)  

    stream = subscribe(broker, stub, "stud3", "pad")
    stub.Publish(make_request(topic="pad", payload="test dlq"), timeout=5)

    received = [next(stream) for _ in range(broker_grpc.MAX_RETRIES)]
    assert len({m.messageId for m in received}) == 1     

    assert wait_until(lambda: len(broker.dlq) == 1)
    entry = broker.dlq[0]
    assert entry["subscriberId"] == "stud3"
    assert entry["payload"] == "test dlq"
    assert entry["reason"] == "ACK timeout"
    assert broker.client_queues["stud3"] == []

    stream.cancel()

def test_acknowledged_message_never_reaches_dlq(env, monkeypatch):
    broker, stub = env
    monkeypatch.setattr(broker_grpc, "ACK_TIMEOUT", 0.2)
    stream = subscribe(broker, stub, "stud1", "bd")
    stub.Publish(make_request(topic="bd"), timeout=5)
    message = next(stream)
    stub.Acknowledge(
        pb.AckRequest(subscriberId="stud1", messageId=message.messageId), timeout=5
    )

    assert wait_until(lambda: broker.client_queues["stud1"] == [])
    assert broker.dlq == []

    stream.cancel()

def test_unacked_message_survives_disconnect_and_is_redelivered(env):
    broker, stub = env
    first = subscribe(broker, stub, "stud2", "pam")
    stub.Publish(make_request(topic="pam", payload="persistenta"), timeout=5)
    message = next(first)                      

    first.cancel()
    wait_disconnected(broker, "stud2")
    assert len(broker.client_queues["stud2"]) == 1          

    second = subscribe(broker, stub, "stud2", "pam")        
    redelivered = next(second)
    assert redelivered.messageId == message.messageId

    second.cancel()

def test_message_published_while_offline_is_delivered_on_reconnect(env):
    broker, stub = env
    first = subscribe(broker, stub, "stud2", "pam")
    first.cancel()
    wait_disconnected(broker, "stud2")

    published = stub.Publish(make_request(topic="pam", payload="cat timp erai offline"), timeout=5)
    assert len(broker.client_queues["stud2"]) == 1

    second = subscribe(broker, stub, "stud2", "pam")
    assert next(second).messageId == published.messageId

    second.cancel()

def test_concurrent_publishes_are_all_stored(env):
    broker, stub = env
    stream = subscribe(broker, stub, "stud1", "bd")

    def publish(i):
        return stub.Publish(make_request(topic="bd", payload=f"mesaj-{i}"), timeout=10)

    with futures.ThreadPoolExecutor(max_workers=20) as pool:
        responses = list(pool.map(publish, range(40)))

    assert all(r.status == "ACCEPTED" for r in responses)
    assert len({r.messageId for r in responses}) == 40
    assert len(broker.client_queues["stud1"]) == 40

    stream.cancel()

def test_concurrent_publish_with_same_key_is_processed_once(env):
    broker, stub = env
    stream = subscribe(broker, stub, "stud1", "bd")

    def publish(_):
        return stub.Publish(make_request(topic="bd", key="aceeasi-cheie"), timeout=10)

    with futures.ThreadPoolExecutor(max_workers=10) as pool:
        responses = list(pool.map(publish, range(10)))

    statuses = [r.status for r in responses]
    assert statuses.count("ACCEPTED") == 1
    assert statuses.count("ALREADY_PROCESSED") == 9
    assert len(broker.client_queues["stud1"]) == 1

    stream.cancel()