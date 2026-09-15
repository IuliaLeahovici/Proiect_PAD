import socket
import sys
from protocol import send_json, receive_json
CLIENT_ID = sys.argv[1] if len(sys.argv) > 1 else "student-1"
NO_ACK = "--no-ack" in sys.argv
COURSES = [
    arg for arg in sys.argv[2:]
    if arg != "--no-ack"
]
if not COURSES:
    COURSES = ["python"]
def run_subscriber():
    processed_ids = set()
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.connect(("127.0.0.1", 5000))
    print()
    print("         SUBSCRIBER CONNECTAT")
    print(f"Client ID : {CLIENT_ID}")
    print(f"Cursuri   : {COURSES}")
    print(f"No ACK    : {NO_ACK}")
    print()
    send_json(
        sock,
        {
            "action": "subscribe",
            "topic": "all:students",
            "clientId": CLIENT_ID
        }
    )
    for course in COURSES:
        send_json(
            sock,
            {
                "action": "subscribe",
                "topic": f"course:{course}",
                "clientId": CLIENT_ID
            }
        )
    send_json(
        sock,
        {
            "action": "subscribe",
            "topic": f"student:{CLIENT_ID}",
            "clientId": CLIENT_ID
        }
    )
    print(f"[{CLIENT_ID}] Abonamente activate:")
    print("    all:students")
    for course in COURSES:
        print(f"    course:{course}")
    print(f"    student:{CLIENT_ID}")
    print()
    print(f"[{CLIENT_ID}] Astept mesaje...")
    print()
    buffer = ""
    while True:
        try:
            msg, buffer = receive_json(sock, buffer)
            if msg is None:
                print(
                    f"[{CLIENT_ID}] Brokerul a inchis conexiunea."
                )
                break
            msg_id = (
                msg.get("messageId")
                or msg.get("message_id")
            )
            topic = msg.get("topic")
            payload = msg.get("payload")
            if msg_id in processed_ids:
                print(
                    f"[{CLIENT_ID}] DUPLICAT ignorat "
                    f"pentru messageId={msg_id}"
                )
            else:
                processed_ids.add(msg_id)
                print(f"[{CLIENT_ID}] MESAJ PRIMIT")
                print(f"   topic     : {topic}")
                print(f"   messageId : {msg_id}")
                print(f"   payload   : {payload}")
                print()
            if NO_ACK:
                print(
                    f"[{CLIENT_ID}] "
                    f" NU trimit ACK "
                    f"pentru messageId={msg_id}"
                )
                print()
                continue
            send_json(
                sock,
                {
                    "action": "ack",
                    "clientId": CLIENT_ID,
                    "messageId": msg_id
                }
            )
            print(
                f"[{CLIENT_ID}] ACK trimis "
                f"pentru messageId={msg_id}"
            )
            print()
        except KeyboardInterrupt:
            print()
            print(
                f"[{CLIENT_ID}] "
                f"Inchidere voluntara..."
            )
            break
        except ConnectionResetError:
            print(
                f"[{CLIENT_ID}] "
                f"Conexiunea cu brokerul a fost resetata."
            )
            break
        except BrokenPipeError:
            print(
                f"[{CLIENT_ID}] "
                f"Conexiunea cu brokerul s-a inchis."
            )
            break
        except Exception as e:
            print(
                f"[{CLIENT_ID}] Eroare: {e}"
            )
            break
    try:
        sock.close()
    except Exception:
        pass
    print(
        f"[{CLIENT_ID}] Subscriber oprit."
    )
if name == "__main__":
    run_subscriber()