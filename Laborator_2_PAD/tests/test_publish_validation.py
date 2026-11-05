import grpc
import pytest

import messaging_pb2 as pb
from helpers import make_request

@pytest.mark.parametrize(
    "request_kwargs",
    [
        {"payload": ""},     
        {"topic": ""},       
        {"version": 2},      
        {"key": ""},         
    ],
)
def test_invalid_publish_is_rejected(env, request_kwargs):
    broker, stub = env
    with pytest.raises(grpc.RpcError) as error:
        stub.Publish(make_request(**request_kwargs), timeout=5)
    assert error.value.code() == grpc.StatusCode.INVALID_ARGUMENT
    assert len(broker.operations) == 0

def test_valid_publish_is_accepted(env):
    broker, stub = env
    response = stub.Publish(make_request(), timeout=5)
    assert response.success is True
    assert response.status == "ACCEPTED"
    assert response.messageId != ""

def test_publish_without_subscribers_is_accepted_but_not_stored(env):
    broker, stub = env
    response = stub.Publish(make_request(topic="fara-abonati"), timeout=5)
    assert response.status == "ACCEPTED"
    assert broker.client_queues == {}

def test_same_key_same_content_is_idempotent(env):
    broker, stub = env
    first = stub.Publish(make_request(key="k1"), timeout=5)
    second = stub.Publish(make_request(key="k1"), timeout=5)
    assert first.status == "ACCEPTED"
    assert second.status == "ALREADY_PROCESSED"
    assert second.messageId == first.messageId

def test_same_key_different_content_is_rejected(env):
    broker, stub = env
    stub.Publish(make_request(key="k2", payload="a"), timeout=5)
    with pytest.raises(grpc.RpcError) as error:
        stub.Publish(make_request(key="k2", payload="b"), timeout=5)
    assert error.value.code() == grpc.StatusCode.ALREADY_EXISTS

def test_get_publish_status_found_and_not_found(env):
    broker, stub = env
    missing = stub.GetPublishStatus(pb.StatusRequest(idempotencyKey="nu-exista"), timeout=5)
    assert missing.found is False
    assert missing.status == "NOT_FOUND"

    published = stub.Publish(make_request(key="k3"), timeout=5)
    found = stub.GetPublishStatus(pb.StatusRequest(idempotencyKey="k3"), timeout=5)
    assert found.found is True
    assert found.messageId == published.messageId