import socket

from protocol import send_json, receive_json
from netutils import bind_random_local_port

def run_subscriber():
    client_id = input(
        "Introdu numele: "
    ).strip()
    if not client_id:
        print("Numele nu poate fi gol.")
        return
    topic = input(
        "Introdu topicul: "
    ).strip()
    if not topic:
        print("Topic-ul nu poate fi gol.")
        return
    ack_answer = input(
        "Trimite ACK? (d/n): "
    ).strip().lower()
    no_ack = ack_answer in ("n", "nu")
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
    print(f"Nume          : {client_id}")
    print(f"Topic         : {topic}")
    print(f"Adresa locala : {local_ip}:{local_port}")
    print(f"ACK activ     : {not no_ack}")
    print()
    subscription = {
        "action": "subscribe",
        "topic": topic,
        "clientId": client_id
    }
    send_json(
        sock,
        subscription
    )
    
    if no_ack:

        print(
            "Mesajele vor ajunge in DLQ."
        )
    else:
        print()
    print("Astept mesaje...")
    print()
    buffer = ""
    while True:
        try:
            msg, buffer = receive_json(
                sock,
                buffer
            )
            if msg is None:
                print(
                    "Brokerul a inchis conexiunea."
                )
                break
            message_id = (
                msg.get("messageId")
                or msg.get("message_id")
            )
            message_topic = msg.get(
                "topic"
            )
            payload = msg.get(
                "payload"
            )
            print()
            print("MESAJ PRIMIT")
            print(
                f"Topic      : {message_topic}"
            )
            print(
                f"Message ID : {message_id}"
            )
            print(
                f"Mesaj      : {payload}"
            )
            if no_ack:
                print(
                    f"NU se trimite ACK pentru "
                    f"messageId={message_id}"
                )
                print(
                    "Brokerul va reincerca livrarea."
                )
            else:
                ack = {
                    "action": "ack",
                    "clientId": client_id,
                    "messageId": message_id
                }
                send_json(
                    sock,
                    ack
                )
                print(
                    f"Confirmare trimisa pentru "
                    f"messageId={message_id}"
                )
        except KeyboardInterrupt:
            print()
            print(
                "Inchidere voluntara..."
            )
            break
        except ConnectionResetError:
            print(
                "Conexiunea cu brokerul a fost inchisa."
            )
            break
        except BrokenPipeError:
            print(
                "Conexiunea cu brokerul s-a inchis."
            )
            break
        except Exception as error:
            print(
                f"Eroare: {error}"
            )
            break
    try:
        sock.close()
    except Exception:
        pass

    print(
        f"{client_id} oprit."
    )
if __name__ == "__main__":
    run_subscriber()