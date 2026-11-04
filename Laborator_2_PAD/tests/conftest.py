import os
import sys
from concurrent import futures

import grpc
import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import broker_grpc
import messaging_pb2_grpc as rpc


@pytest.fixture
def env():
    broker = broker_grpc.MessageBroker()
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=30))
    rpc.add_MessageBrokerServicer_to_server(broker, server)
    port = server.add_insecure_port("127.0.0.1:0")
    server.start()
    channel = grpc.insecure_channel(f"127.0.0.1:{port}")
    stub = rpc.MessageBrokerStub(channel)
    yield broker, stub
    channel.close()
    server.stop(0)