import json
from protocol import send_json, receive_json
class FakeSocket:
    def __init__(self, incoming=b""):
        self.sent = []
        self.incoming = incoming
    def sendall(self, data):
        self.sent.append(data)
    def recv(self, size):
        if not self.incoming:
            return b""
        chunk = self.incoming[:size]
        self.incoming = self.incoming[size:]
        return chunk
def test_send_json_adds_newline_and_sends_valid_json():
    sock = FakeSocket()
    message = {"action": "publish", "payload": "Salut"}
    send_json(sock, message)
    assert len(sock.sent) == 1
    raw = sock.sent[0].decode("utf-8")
    assert raw.endswith("\n")
    assert json.loads(raw) == message
def test_receive_json_reads_one_message_and_preserves_remaining_buffer():
    first = {"action": "ack", "messageId": "m1"}
    second = {"action": "ack", "messageId": "m2"}
    raw = (
        json.dumps(first) + "\n" +
        json.dumps(second) + "\n"
    ).encode("utf-8")
    sock = FakeSocket(raw)
    msg1, buffer = receive_json(sock, "")
    msg2, buffer = receive_json(sock, buffer)
    assert msg1 == first
    assert msg2 == second
    assert buffer == ""