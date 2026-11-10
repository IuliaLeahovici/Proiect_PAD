import grpc
import pytest

import messaging_pb2 as pb
from helpers import make_request, subscribe, wait_disconnected

def test_message_is_routed_only_to_matching_topic(env):
    broker, stub = env
    s1 = subscribe(broker, stub, "stud1", "bd")
    s2 = subscribe(broker, stub, "stud2", "pam")

    published = stub.Publish(make_request(topic="bd", payload="test1"), timeout=5)

    message = next(s1)
    assert message.payload == "test1"
    assert message.messageId == published.messageId
    assert broker.client_queues["stud2"] == []   

    s1.cancel()
    s2.cancel()

def test_fan_out_to_all_subscribers_of_same_topic(env):
    broker, stub = env
    s1 = subscribe(broker, stub, "stud1", "bd")
    s2 = subscribe(broker, stub, "stud3", "bd")
    published = stub.Publish(make_request(topic="bd", payload="catre toti"), timeout=5)
    assert next(s1).messageId == published.messageId
    assert next(s2).messageId == published.messageId
    s1.cancel()
    s2.cancel()

def test_ack_removes_message_from_queue(env):
    broker, stub = env
    stream = subscribe(broker, stub, "stud1", "bd")
    stub.Publish(make_request(topic="bd"), timeout=5)
    message = next(stream)

    ack = stub.Acknowledge(
        pb.AckRequest(subscriberId="stud1", messageId=message.messageId), timeout=5
    )
    assert ack.success is True
    assert broker.client_queues["stud1"] == []

    again = stub.Acknowledge(
        pb.AckRequest(subscriberId="stud1", messageId=message.messageId), timeout=5
    )
    assert again.success is False

    stream.cancel()

def test_ack_for_unknown_message_returns_false(env):
    broker, stub = env
    ack = stub.Acknowledge(
        pb.AckRequest(subscriberId="nimeni", messageId="id-inexistent"), timeout=5
    )
    assert ack.success is False

def test_broker_fills_default_fields(env):
    broker, stub = env
    stream = subscribe(broker, stub, "stud1", "bd")
    request = pb.PublishRequest(
        message=pb.Message(schemaVersion=1, topic="bd", payload="x"),
        idempotencyKey="k-default",
    )
    stub.Publish(request, timeout=5)

    message = next(stream)
    assert message.messageId != ""
    assert message.correlationId != ""
    assert message.occurredAt != ""
    assert message.messageType == "notification"

    stream.cancel()

def test_duplicate_subscriber_name_is_rejected(env):
    broker, stub = env
    first = subscribe(broker, stub, "stud1", "bd")

    second = stub.Subscribe(pb.SubscribeRequest(subscriberId="stud1", topic="bd"), timeout=5)
    with pytest.raises(grpc.RpcError) as error:
        next(second)
    assert error.value.code() == grpc.StatusCode.ALREADY_EXISTS

    first.cancel()

def test_subscriber_cannot_switch_to_another_topic(env):
    broker, stub = env
    first = subscribe(broker, stub, "stud1", "bd")
    first.cancel()
    wait_disconnected(broker, "stud1")

    second = stub.Subscribe(pb.SubscribeRequest(subscriberId="stud1", topic="pam"), timeout=5)
    with pytest.raises(grpc.RpcError) as error:
        next(second)
    assert error.value.code() == grpc.StatusCode.FAILED_PRECONDITION

@pytest.mark.parametrize("subscriber_id,topic", [("", "bd"), ("stud1", ""), ("  ", "bd")])
def test_subscribe_with_empty_fields_is_rejected(env, subscriber_id, topic):
    broker, stub = env
    stream = stub.Subscribe(
        pb.SubscribeRequest(subscriberId=subscriber_id, topic=topic), timeout=5
    )
    with pytest.raises(grpc.RpcError) as error:
        next(stream)
    assert error.value.code() == grpc.StatusCode.INVALID_ARGUMENT