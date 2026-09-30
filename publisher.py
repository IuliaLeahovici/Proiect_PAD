import socket
import uuid
from datetime import datetime, timezone

from protocol import send_json
from netutils import bind_random_local_port

def run_publisher():

    client_id = f"publisher-{uuid.uuid4().hex[:6]}"
    sock = socket.socket(
        socket.AF_INET,
        socket.SOCK_STREAM
    )
    sock.setsockopt(
        socket.SOL_SOCKET,
        socket.SO_REUSEADDR,
        1
    )
    local_ip, local_port = bind_random_local_port(sock)
    sock.connect(
        ("127.0.0.1", 5000)
    )
    print()
    print(f"ID            : {client_id}")
    print(f"Adresa locala : {local_ip}:{local_port}")
    print()
    print("Conectat la Broker.")
    print()
    while True:
        try:
            topic = input(
                "Introdu topicul: "
            ).strip()
            if not topic:
                print(" Topic-ul nu poate fi gol.")
                print()
                continue
            message_text = input(
                "Introdu mesajul: "
            ).strip()
            if not message_text:
                print("Mesajul nu poate fi gol.")
                print()
                continue
            msg = {
                "action": "publish",
                "topic": topic,
                "clientId": client_id,
                "messageId": str(
                    uuid.uuid4()
                )[:8],
                "correlationId": str(
                    uuid.uuid4()
                )[:8],
                "messageType": "notification",
                "schemaVersion": 1,
                "occurredAt": datetime.now(
                    timezone.utc
                ).isoformat(),
                "payload": message_text
            }
            send_json(
                sock,
                msg
            )
            print()
            print("Mesaj trimis.")
            print(f"   Topic      : {topic}")
            print(f"   Message ID : {msg['messageId']}")
            print()
        except KeyboardInterrupt:
            print()
            print("Inchidere voluntara")
            break
        except (ConnectionResetError, BrokenPipeError):
            print()
            print("Conexiunea cu Brokerul a fost inchisa.")
            break
        except Exception as error:
            print(f"Eroare: {error}")
            break
    try:
        sock.close()
    except Exception:
        pass
    print(
        f"{client_id} oprit."
    )
if __name__ == "__main__":
    run_publisher()