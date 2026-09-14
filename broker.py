import socket
import threading
import json
import time
import uuid
from datetime import datetime, timezone
from protocol import send_json, receive_json
import storage
MAX_RETRIES = 3
ACK_TIMEOUT = 5 
class MessageBroker:
    def __init__(self, host='127.0.0.1', port=5000):
        self.host = host
        self.port = port
        self.subscriptions = {}        
        self.client_queues = {}          
        self.delivery_meta = {}          
        self.active_sockets = {}         
        self.lock = threading.Lock()
    def start(self):
        server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        server.bind((self.host, self.port))
        server.listen(5)
        print(f"[BROKER] Serverul ruleaza pe {self.host}:{self.port}")
        threading.Thread(target=self.retry_worker, daemon=True).start()
        threading.Thread(target=self.cron_job_worker, daemon=True).start()
        while True:
            client_sock, addr = server.accept()
            threading.Thread(target=self.handle_client, args=(client_sock, addr), daemon=True).start()
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
                total_pending = sum(len(q) for q in self.client_queues.values())
                print(f"[CRON WORKER] {len(self.active_sockets)} clienti conectati, {total_pending} mesaje in asteptare.")
    def handle_client(self, sock, addr):
        buffer = ""
        current_client_id = None
        try:
            while True:
                try:
                    data, buffer = receive_json(sock, buffer)
                except (json.JSONDecodeError, ValueError) as err:
                    print(f"[VALIDATION ERROR] Mesaj invalid de la {addr}! Trimis in DLQ.")
                    storage.save_to_dlq({"raw": "corrupted_payload"}, f"Invalid JSON: {err}")
                    continue
                if data is None:
                    break
                action = data.get("action")
                current_client_id = data.get("clientId") or data.get("client_id")
                if action == "publish" and not all(k in data for k in ["topic", "payload"]):
                    print(f"[VALIDATION ERROR] Schema invalida de la {current_client_id}!")
                    storage.save_to_dlq(data, "Schema Validation Failed: Missing topic or payload")
                    continue
                if action == "subscribe":
                    self.process_subscribe(current_client_id, data.get("topic"), sock)
                elif action == "publish":
                    self.process_publish(data)
                elif action == "ack":
                    msg_id = data.get("messageId") or data.get("message_id")
                    self.process_ack(current_client_id, msg_id)
        except (ConnectionResetError, BrokenPipeError):
            print(f"[WARN] Clientul {current_client_id or addr} s-a deconectat subit!")
        finally:
            self.disconnect_client(current_client_id, sock)
    def process_subscribe(self, client_id, topic, sock):
        with self.lock:
            self.subscriptions.setdefault(topic, set()).add(client_id)
            self.active_sockets[client_id] = sock
            if client_id not in self.client_queues:
                self.client_queues[client_id] = storage.load_queue_from_disk(client_id)
            self.delivery_meta.setdefault(client_id, {})
            print(f"[SUBSCRIPTION] '{client_id}' s-a abonat la '{topic}' "
                  f"({len(self.client_queues[client_id])} mesaje in asteptare din persistenta)")
            self._flush_queue_locked(client_id)
    def process_publish(self, msg):
        msg.setdefault("messageId", str(uuid.uuid4())[:8])
        msg.setdefault("correlationId", str(uuid.uuid4())[:8])
        msg.setdefault("messageType", "notification")
        msg.setdefault("schemaVersion", 1)
        msg.setdefault("occurredAt", datetime.now(timezone.utc).isoformat())
        topic = msg.get("topic")
        print(f"[PUBLISH] topic='{topic}' messageId={msg['messageId']} correlationId={msg['correlationId']}: {msg.get('payload')}")
        with self.lock:
            for client_id in self.subscriptions.get(topic, set()):
                self.client_queues.setdefault(client_id, [])
                self.delivery_meta.setdefault(client_id, {})
                self.client_queues[client_id].append(msg)
                self.delivery_meta[client_id][msg["messageId"]] = {"retries": 0, "lastSent": None}
                storage.save_queue_to_disk(client_id, self.client_queues[client_id])  # persistenta imediata
                if client_id in self.active_sockets:
                    self._flush_queue_locked(client_id)
    def _flush_queue_locked(self, client_id):
        sock = self.active_sockets.get(client_id)
        if not sock:
            return
        queue = self.client_queues.get(client_id, [])
        meta_map = self.delivery_meta.setdefault(client_id, {})
        to_remove = []
        now = time.time()
        for msg in list(queue):
            msg_id = msg["messageId"] if "messageId" in msg else msg.get("message_id")
            meta = meta_map.setdefault(msg_id, {"retries": 0, "lastSent": None})
            if meta["retries"] >= MAX_RETRIES:
                print(f"[DLQ] messageId={msg_id} a depasit {MAX_RETRIES} incercari fara ACK. Trimis in DLQ.")
                storage.save_to_dlq(msg, "Exceeded max delivery retries without ACK")
                to_remove.append(msg)
                meta_map.pop(msg_id, None)
                continue
            never_sent = meta["lastSent"] is None
            timed_out = (not never_sent) and (now - meta["lastSent"] > ACK_TIMEOUT)
            if never_sent or timed_out:
                try:
                    send_json(sock, msg)
                    meta["retries"] += 1
                    meta["lastSent"] = now
                    tag = "prima trimitere" if never_sent else f"retry #{meta['retries']}"
                    print(f"[DELIVER] -> {client_id} messageId={msg_id} ({tag})")
                except Exception:
                    print(f"[WARN] Esec la trimiterea catre {client_id}.")
                    break
        if to_remove:
            for msg in to_remove:
                queue.remove(msg)
            storage.save_queue_to_disk(client_id, queue)
    def process_ack(self, client_id, message_id):
        with self.lock:
            queue = self.client_queues.get(client_id, [])
            self.client_queues[client_id] = [
                m for m in queue if (m.get("messageId") or m.get("message_id")) != message_id
            ]
            self.delivery_meta.get(client_id, {}).pop(message_id, None)
            storage.save_queue_to_disk(client_id, self.client_queues[client_id])
            print(f"[ACK] '{client_id}' a confirmat messageId={message_id}")
    def disconnect_client(self, client_id, sock):
        with self.lock:
            if client_id and self.active_sockets.get(client_id) is sock:
                del self.active_sockets[client_id]
                pending = self.client_queues.get(client_id, [])
                storage.save_queue_to_disk(client_id, pending)
                print(f"[PERSISTENCE] '{client_id}' offline, {len(pending)} mesaje raman salvate pe disc.")
            try:
                sock.close()
            except Exception:
                pass
if __name__ == "__main__":
    broker = MessageBroker()
    broker.start()