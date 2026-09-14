import json
def send_json(sock, data):
    payload = json.dumps(data) + "\n"
    sock.sendall(payload.encode('utf-8'))
def receive_json(sock, buffer):
    while "\n" not in buffer:
        chunk = sock.recv(1024).decode('utf-8')
        if not chunk:
            return None, buffer
        buffer += chunk
    line, buffer = buffer.split("\n", 1)
    return json.loads(line), buffer