import time
import uuid
import threading
from concurrent import futures
from datetime import datetime, timezone

import grpc
import messaging_pb2 as pb
import messaging_pb2_grpc as rpc

MAX_RETRIES = 3
ACK_TIMEOUT = 5

def short_id(value):
    return value[:8]

class MessageBroker(rpc.MessageBrokerServicer):
    def __init__(self):
        self.subscriptions = {}
        self.client_queues = {}
        self.delivery_meta = {}
        self.active_clients = {}
        self.dlq = []
        self.operations = {}
        self.seen_publishers = set()
        self.lock = threading.RLock()

    def stats(self):
        pending = sum(len(q) for q in self.client_queues.values())
        print(f"[VERIFICARE] Clienti conectati: {len(self.active_clients)} | Mesaje in RAM: {pending} | DLQ: {len(self.dlq)}")

    def Publish(self, request, context):
        msg = request.message
        key = request.idempotencyKey
        peer = context.peer()
        with self.lock:
            if peer not in self.seen_publishers:
                self.seen_publishers.add(peer)
                print(f"\n[CONEXIUNE NOUA] {peer} (publisher)")
        with self.lock:
            if not key:
                context.abort(grpc.StatusCode.INVALID_ARGUMENT, "idempotencyKey este obligatoriu")
            if not msg.topic or not msg.payload:
                context.abort(grpc.StatusCode.INVALID_ARGUMENT, "Topic si payload obligatorii")
            if msg.schemaVersion != 1:
                context.abort(grpc.StatusCode.INVALID_ARGUMENT, "Versiune incompatibila")
            fingerprint = (msg.topic, msg.payload, msg.messageType)
            if key in self.operations:
                old = self.operations[key]
                if old['fingerprint'] != fingerprint:
                    context.abort(grpc.StatusCode.ALREADY_EXISTS, "Cheie reutilizata cu alt continut")
                print(f"[IDEMPOTENCY] Publicare deja procesata: {short_id(old['messageId'])}")
                return pb.PublishResponse(success=True, messageId=old['messageId'], status='ALREADY_PROCESSED')
            message = pb.Message()
            message.CopyFrom(msg)
            if not message.messageId:
                message.messageId = str(uuid.uuid4())
            if not message.correlationId:
                message.correlationId = str(uuid.uuid4())
            if not message.occurredAt:
                message.occurredAt = datetime.now(timezone.utc).isoformat()
            if not message.messageType:
                message.messageType = 'notification'
            subscribers = sorted(self.subscriptions.get(message.topic, set()))
            print(f"\n[PUBLICARE] topic='{message.topic}' messageId={short_id(message.messageId)}")
            print(f"[PUBLICARE] Mesaj: {message.payload}")
            for client_id in subscribers:
                copy = pb.Message()
                copy.CopyFrom(message)
                self.client_queues.setdefault(client_id, []).append(copy)
                self.delivery_meta.setdefault(client_id, {})[copy.messageId] = {'retries': 0, 'lastSent': None}
                print(f"[RAM QUEUE] Mesaj salvat pentru '{client_id}'.")
            if not subscribers:
                print(f"[BROKER] Nu exista abonati pentru topicul '{message.topic}'.")
            self.operations[key] = {'messageId': message.messageId, 'fingerprint': fingerprint}
            self.stats()
            return pb.PublishResponse(success=True, messageId=message.messageId, status='ACCEPTED')

    def Subscribe(self, request, context):
        client_id = request.subscriberId.strip()
        topic = request.topic.strip()
        if not client_id or not topic:
            context.abort(grpc.StatusCode.INVALID_ARGUMENT, 'Nume si topic obligatorii')
        with self.lock:
            if client_id in self.active_clients:
                context.abort(grpc.StatusCode.ALREADY_EXISTS, 'Subscriber deja conectat')
            old_topics = [t for t, ids in self.subscriptions.items() if client_id in ids]
            if old_topics and topic not in old_topics:
                context.abort(grpc.StatusCode.FAILED_PRECONDITION, f"'{client_id}' este deja abonat la '{old_topics[0]}'")
            self.subscriptions.setdefault(topic, set()).add(client_id)
            self.client_queues.setdefault(client_id, [])
            self.delivery_meta.setdefault(client_id, {})
            token = object()
            self.active_clients[client_id] = token
            print(f"\n[CONEXIUNE NOUA] {context.peer()}")
            print(f"[ABONARE] '{client_id}' s-a abonat la topicul '{topic}'")
            self.stats()
        try:
            while context.is_active():
                outgoing = []
                now = time.monotonic()
                with self.lock:
                    queue = self.client_queues[client_id]
                    meta_map = self.delivery_meta[client_id]
                    for message in list(queue):
                        mid = message.messageId
                        meta = meta_map.get(mid)
                        if meta is None:
                            continue
                        last = meta['lastSent']
                        if last is not None and now - last < ACK_TIMEOUT:
                            continue
                        if meta['retries'] >= MAX_RETRIES:
                            self.dlq.append({'messageId': mid, 'subscriberId': client_id, 'topic': message.topic, 'payload': message.payload, 'reason': 'ACK timeout', 'time': datetime.now(timezone.utc).isoformat()})
                            queue.remove(message)
                            meta_map.pop(mid, None)
                            print(f"[DLQ RAM] '{client_id}' messageId={short_id(mid)} (fara ACK)")
                            self.stats()
                            continue
                        meta['retries'] += 1
                        meta['lastSent'] = now
                        outgoing.append(message)
                        print(f"[LIVRARE] -> {client_id} messageId={short_id(mid)} (incercarea {meta['retries']})")
                for message in outgoing:
                    if not context.is_active():
                        break
                    yield message
                time.sleep(0.2)
        finally:
            with self.lock:
                if self.active_clients.get(client_id) is token:
                    self.active_clients.pop(client_id, None)
                    for meta in self.delivery_meta.get(client_id, {}).values():
                        meta['lastSent'] = None
                    print(f"[DECONECTARE] '{client_id}' - mesajele neconfirmate raman in RAM")
                    self.stats()

    def Acknowledge(self, request, context):
        client_id = request.subscriberId
        mid = request.messageId
        with self.lock:
            queue = self.client_queues.get(client_id, [])
            found = any(m.messageId == mid for m in queue)
            if found:
                self.client_queues[client_id] = [m for m in queue if m.messageId != mid]
                self.delivery_meta.get(client_id, {}).pop(mid, None)
                print(f"[ACK] '{client_id}' a confirmat messageId={short_id(mid)}")
                print(f"[RAM] Mesajul {short_id(mid)} a fost eliminat din coada.")
                self.stats()
            return pb.AckResponse(success=found)

    def GetPublishStatus(self, request, context):
        with self.lock:
            operation = self.operations.get(request.idempotencyKey)
            if operation is None:
                return pb.StatusResponse(found=False, status='NOT_FOUND')
            return pb.StatusResponse(found=True, messageId=operation['messageId'], status='ACCEPTED')


def serve():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=20))
    rpc.add_MessageBrokerServicer_to_server(MessageBroker(), server)
    port = server.add_insecure_port('127.0.0.1:50051')
    if port == 0:
        raise RuntimeError('Nu se poate deschide portul 50051')
    server.start()
    print('[BROKER] Serverul gRPC ruleaza pe 127.0.0.1:50051')
    print('[BROKER] Mesajele si DLQ sunt pastrate in RAM.')
    print('[BROKER] Astept conexiuni...')
    try:
        server.wait_for_termination()
    except KeyboardInterrupt:
        print('\n[BROKER] Oprire')
        server.stop(grace=2).wait()


if __name__ == '__main__':
    serve()
