import socket
import sys
import uuid
from datetime import datetime, timezone
from protocol import send_json
CLIENT_ID = sys.argv[1] if len(sys.argv) > 1 else "publisher-1"
def run_publisher(client_id):
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.connect(("127.0.0.1", 5000))
    print(f"[{client_id}] Conectat la Broker")
    while True:
        print("       MENIU PUBLISHER")
        print("1 - Trimite tuturor studentilor")
        print("2 - Trimite unui curs")
        print("3 - Trimite unui singur student")
        print("4 - Iesire")
        choice = input("Alege optiunea: ")
        if choice == "4":
            break
        if choice == "1":
            topic = "all:students"
        elif choice == "2":
            course = input(
                "Introdu cursul: "
            ).strip()
            topic = f"course:{course}"
        elif choice == "3":
            student_id = input(
                "Introdu ID-ul studentului: "
            ).strip()
            topic = f"student:{student_id}"
        else:
            print("Optiune invalida.")
            continue
        text = input(
            "Introdu mesajul: "
        ).strip()
        if not text:
            print("Mesaj gol.")
            continue
        msg = {
            "action": "publish",
            "topic": topic,
            "clientId": client_id,
            "messageId": str(uuid.uuid4())[:8],
            "correlationId": str(uuid.uuid4())[:8],
            "messageType": "notification",
            "schemaVersion": 1,
            "occurredAt": datetime.now(
                timezone.utc
            ).isoformat(),
            "payload": text
        }
        send_json(sock, msg)
        print(
            f"\n[PUBLISHER] Mesaj trimis"
        )
        print(
            f"   publisher = {client_id}"
        )
        print(
            f"   topic     = {topic}"
        )
        print(
            f"   messageId = {msg['messageId']}"
        )
    sock.close()
if name == "__main__":
    run_publisher(CLIENT_ID)