import socket
import threading
import json
import time
import uuid
from datetime import datetime, timezone

from protocol import send_json, receive_json

MAX_RETRIES = 3
ACK_TIMEOUT = 5

class MessageBroker:
    def __init__(self, host="127.0.0.1", port=5000):
        self.host = host
        self.port = port
        self.subscriptions = {}
        self.client_queues = {}
        self.delivery_meta = {}
        self.active_sockets = {}
        self.dlq = []
        self.lock = threading.Lock()

    def start(self):
        server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        server.bind((self.host, self.port))
        server.listen(5)
        print(f"[BROKER] Serverul ruleaza pe {self.host}:{self.port}")
        print("[BROKER] Mesajele si DLQ sunt pastrate in RAM.")
        print()
        threading.Thread(
            target=self.retry_worker,
            daemon=True
        ).start()
        threading.Thread(
            target=self.cron_job_worker,
            daemon=True
        ).start()
        while True:
            client_sock, addr = server.accept()
            print(
                f"[CONEXIUNE NOUA] {addr[0]}:{addr[1]}"
            )
            threading.Thread(
                target=self.handle_client,
                args=(client_sock, addr),
                daemon=True
            ).start()

    def retry_worker(self):
        while True:
            time.sleep(1)
            with self.lock:
                for client_id in list(self.client_queues.keys()):
                    if client_id in self.active_sockets:
                        self._flush_queue_locked(client_id)

    def cron_job_worker(self):
        while True:
            time.sleep(30)
            with self.lock:
                total_pending = sum(
                    len(queue)
                    for queue in self.client_queues.values()
                )
                print(
                    f"[VERIFICARE] "
                    f"Clienti conectati: {len(self.active_sockets)} | "
                    f"Mesaje in RAM: {total_pending} | "
                    f"DLQ in RAM: {len(self.dlq)}"
                )

    def handle_client(self, sock, addr):
        buffer = ""
        current_client_id = None
        try:
            while True:
                try:
                    data, buffer = receive_json(sock, buffer)
                except (json.JSONDecodeError, ValueError) as err:
                    print(
                        f"[VALIDATION ERROR] "
                        f"Mesaj invalid de la {addr[0]}:{addr[1]}"
                    )
                    # DLQ doar in RAM
                    with self.lock:
                        self.dlq.append({
                            "raw": "corrupted_payload",
                            "reason": f"Invalid JSON: {err}",
                            "occurredAt": datetime.now(
                                timezone.utc
                            ).isoformat()
                        })
                        print(
                            f"Mesaj invalid salvat in DLQ. "
                            f"Total DLQ: {len(self.dlq)}"
                        )
                    continue
                if data is None:
                    break
                action = data.get("action")
                if data.get("clientId"):
                    current_client_id = data.get("clientId")
                elif data.get("client_id"):
                    current_client_id = data.get("client_id")
                if action == "subscribe":
                    self.process_subscribe(
                        current_client_id,
                        data.get("topic"),
                        sock
                    )
                elif action == "publish":
                    self.process_publish(data)
                elif action == "ack":
                    message_id = (
                        data.get("messageId")
                        or data.get("message_id")
                    )
                    self.process_ack(
                        current_client_id,
                        message_id
                    )
        except (ConnectionResetError, BrokenPipeError):
            print(
                f"[AVERTIZARE] Clientul "
                f"{current_client_id or addr} "
                f"s-a deconectat subit!"
            )
        finally:
            self.disconnect_client(
                current_client_id,
                sock
            )

    def process_subscribe(
        self,
        client_id,
        topic,
        sock
    ):
        with self.lock:
            if not client_id or not topic:
                print(
                    "[ERROR] Subscriberul trebuie "
                    "sa aiba nume si topic."
                )
                return
            if (
                client_id in self.active_sockets
                and self.active_sockets[client_id] != sock
            ):
                print(
                    f"[RESPINS] Clientul '{client_id}' "
                    f"este deja conectat."
                )
                sock.close()
                return
            self.subscriptions.setdefault(
                topic,
                set()
            ).add(client_id)
            self.active_sockets[client_id] = sock
            if client_id not in self.client_queues:
                self.client_queues[client_id] = []
            self.delivery_meta.setdefault(
                client_id,
                {}
            )
            print()
            print(
                f"[ABONARE] "
                f"'{client_id}' s-a abonat la topicul "
                f"'{topic}'"
            )
            pending = len(
                self.client_queues[client_id]
            )
            if pending > 0:
                print(
                    f"Pentru '{client_id}' exista "
                    f"{pending} mesaj(e) in RAM."
                )
                print(
                    "Mesajele vor fi livrate acum."
                )
            self._flush_queue_locked(client_id)

    def process_publish(self, msg):
        msg.setdefault(
            "messageId",
            str(uuid.uuid4())[:8]
        )
        msg.setdefault(
            "correlationId",
            str(uuid.uuid4())[:8]
        )
        msg.setdefault(
            "messageType",
            "notification"
        )
        msg.setdefault(
            "schemaVersion",
            1
        )
        msg.setdefault(
            "occurredAt",
            datetime.now(timezone.utc).isoformat()
        )
        topic = msg.get("topic")
        print()
        print(
            f"[PUBLICARE] "
            f"topic='{topic}' "
            f"messageId={msg['messageId']}"
        )
        print(
            f"[PUBLICARE] Mesaj: {msg.get('payload')}"
        )
        with self.lock:
            subscribers = self.subscriptions.get(
                topic,
                set()
            )
            if not subscribers:
                print(
                    f"[BROKER] Nu exista subscriber conectat "
                    f"pentru topicul '{topic}'."
                )
                print(
                    "[BROKER] Mesajul nu poate fi livrat "
                    "deoarece nu exista un destinatar."
                )
                return
            for client_id in subscribers:
                self.client_queues.setdefault(
                    client_id,
                    []
                )
                self.delivery_meta.setdefault(
                    client_id,
                    {}
                )
                self.client_queues[client_id].append(msg)
                self.delivery_meta[client_id][
                    msg["messageId"]
                ] = {
                    "retries": 0,
                    "lastSent": None
                }
                print(
                    f"[RAM QUEUE] "
                    f"Mesajul {msg['messageId']} "
                    f"a fost salvat in RAM pentru "
                    f"'{client_id}'."
                )
                if client_id in self.active_sockets:
                    self._flush_queue_locked(
                        client_id
                    )
                else:
                    print(
                        f"[RAM QUEUE] "
                        f"'{client_id}' este OFFLINE."
                    )
                    print(
                        f"[RAM QUEUE] "
                        f"Mesajul ramane in RAM "
                        f"pana la reconectare."
                    )

    def _flush_queue_locked(self, client_id):
        sock = self.active_sockets.get(
            client_id
        )
        if not sock:
            return
        queue = self.client_queues.get(
            client_id,
            []
        )
        meta_map = self.delivery_meta.setdefault(
            client_id,
            {}
        )
        to_remove = []
        now = time.time()
        for msg in list(queue):
            msg_id = (
                msg.get("messageId")
                or msg.get("message_id")
            )
            meta = meta_map.setdefault(
                msg_id,
                {
                    "retries": 0,
                    "lastSent": None
                }
            )
            if meta["retries"] >= MAX_RETRIES:
                print()
                print(
                    f"[DLQ RAM] "
                    f"messageId={msg_id} "
                    f"a depasit {MAX_RETRIES} incercari."
                )
                print(
                    "[DLQ RAM] "
                    "Mesajul a fost salvat in DLQ RAM."
                )
                dlq_message = dict(msg)
                dlq_message["dlqReason"] = (
                    "Exceeded max delivery retries "
                    "without ACK"
                )
                dlq_message["dlqTime"] = (
                    datetime.now(
                        timezone.utc
                    ).isoformat()
                )
                self.dlq.append(
                    dlq_message
                )
                print(
                    f"[DLQ RAM] "
                    f"Total mesaje in DLQ: "
                    f"{len(self.dlq)}"
                )
                to_remove.append(msg)
                meta_map.pop(
                    msg_id,
                    None
                )
                continue
            never_sent = (
                meta["lastSent"] is None
            )
            timed_out = (
                not never_sent
                and
                now - meta["lastSent"] > ACK_TIMEOUT
            )
            if never_sent or timed_out:
                try:
                    send_json(
                        sock,
                        msg
                    )
                    meta["retries"] += 1
                    meta["lastSent"] = now
                    if never_sent:
                        print(
                            f"[LIVRARE] "
                            f"-> {client_id} "
                            f"messageId={msg_id} "
                            f"(prima trimitere)"
                        )
                    else:
                        print(
                            f"[RETRY] "
                            f"-> {client_id} "
                            f"messageId={msg_id} "
                            f"(retry #{meta['retries']})"
                        )
                except Exception:
                    print(
                        f"[AVERTIZARE] "
                        f"Esec la trimiterea catre "
                        f"{client_id}."
                    )
                    break
        for msg in to_remove:
            if msg in queue:
                queue.remove(msg)

    def process_ack(
        self,
        client_id,
        message_id
    ):
        with self.lock:
            queue = self.client_queues.get(
                client_id,
                []
            )
            self.client_queues[client_id] = [
                message
                for message in queue
                if (
                    message.get("messageId")
                    or message.get("message_id")
                ) != message_id
            ]
            self.delivery_meta.get(
                client_id,
                {}
            ).pop(
                message_id,
                None
            )
            print(
                f"[ACK] "
                f"'{client_id}' a confirmat "
                f"messageId={message_id}"
            )
            print(
                f"[RAM] "
                f"Mesajul {message_id} "
                f"a fost eliminat din coada RAM."
            )

    def disconnect_client(
        self,
        client_id,
        sock
    ):
        with self.lock:
            if (
                client_id
                and
                self.active_sockets.get(client_id) is sock
            ):
                del self.active_sockets[
                    client_id
                ]
                pending = self.client_queues.get(
                    client_id,
                    []
                )
                print()
                print(
                    f"[DECONECTARE] "
                    f"'{client_id}' s-a deconectat."
                )
                if pending:
                    print(
                        f"[RAM QUEUE] "
                        f"{len(pending)} mesaj(e) "
                        f"raman in RAM pentru "
                        f"'{client_id}'."
                    )
                    print(
                        "[RAM QUEUE] "
                        "Mesajele vor fi livrate "
                        "la reconectare."
                    )
                else:
                    print(
                        "[RAM QUEUE] "
                        "Nu exista mesaje in asteptare."
                    )
            try:
                sock.close()
            except Exception:
                pass
if __name__ == "__main__":
    broker = MessageBroker()
    broker.start()