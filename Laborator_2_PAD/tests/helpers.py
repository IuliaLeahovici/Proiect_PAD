import time
import uuid

import messaging_pb2 as pb

def make_request(topic="bd", payload="salut", key=None, version=1):
    message = pb.Message(
        messageType="notification",
        schemaVersion=version,
        topic=topic,
        payload=payload,
    )
    key = str(uuid.uuid4()) if key is None else key
    return pb.PublishRequest(message=message, idempotencyKey=key)


def wait_until(condition, timeout=5.0):
    end = time.time() + timeout
    while time.time() < end:
        if condition():
            return True
        time.sleep(0.05)
    return False


def subscribe(broker, stub, subscriber_id, topic):
    stream = stub.Subscribe(
        pb.SubscribeRequest(subscriberId=subscriber_id, topic=topic), timeout=20
    )
    assert wait_until(lambda: subscriber_id in broker.active_clients)
    return stream


def wait_disconnected(broker, subscriber_id):
    assert wait_until(lambda: subscriber_id not in broker.active_clients)