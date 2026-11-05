import uuid
from datetime import datetime, timezone
import grpc
import messaging_pb2 as pb
import messaging_pb2_grpc as rpc

BROKER_ADDRESS = '127.0.0.1:50051'
RPC_TIMEOUT = 5
MAX_RPC_ATTEMPTS = 3

def run_publisher():
    publisher_id = f'publisher-{uuid.uuid4().hex[:8]}'
    channel = grpc.insecure_channel(BROKER_ADDRESS)
    stub = rpc.MessageBrokerStub(channel)
    print(f'\nPublisher ID : {publisher_id}')
    print(f'Broker       : {BROKER_ADDRESS}')
    try:
        grpc.channel_ready_future(channel).result(timeout=5)
        print('[CONECTAT] Conectat la Broker prin gRPC.\n')
    except grpc.FutureTimeoutError:
        print('[ERROR] Brokerul nu este disponibil.')
        channel.close()
        return
    try:
        while True:
            topic = input('Introdu topicul: ').strip()
            if not topic:
                print('Topicul nu poate fi gol.')
                continue
            payload = input('Introdu mesajul: ').strip()
            if not payload:
                print('Mesajul nu poate fi gol.')
                continue
            message = pb.Message(messageId=str(uuid.uuid4()), correlationId=str(uuid.uuid4()), messageType='notification', schemaVersion=1, occurredAt=datetime.now(timezone.utc).isoformat(), topic=topic, payload=payload)
            key = str(uuid.uuid4())
            request = pb.PublishRequest(message=message, idempotencyKey=key)
            for attempt in range(1, MAX_RPC_ATTEMPTS + 1):
                try:
                    response = stub.Publish(request, timeout=RPC_TIMEOUT)
                    print('\nMesaj trimis.')
                    print(f'    Topic      : {topic}')
                    print(f'    Message ID : {response.messageId[:8]}')
                    print(f'    Status     : {response.status}\n')
                    break
                except grpc.RpcError as error:
                    print(f'[RPC ERROR] Incercarea {attempt}: {error.code().name}')
                    if error.code() not in (grpc.StatusCode.DEADLINE_EXCEEDED, grpc.StatusCode.UNAVAILABLE):
                        break
                    try:
                        status = stub.GetPublishStatus(pb.StatusRequest(idempotencyKey=key), timeout=RPC_TIMEOUT)
                        if status.found:
                            print(f'[FALLBACK] Operatie deja executata. Message ID: {status.messageId[:8]}')
                            break
                    except grpc.RpcError:
                        print('[FALLBACK] Status indisponibil.')
                    if attempt == MAX_RPC_ATTEMPTS:
                        print('[UNKNOWN] Rezultat necunoscut. Pastreaza cheia pentru verificare:')
                        print(key)
    except (KeyboardInterrupt, EOFError):
        print('\n[PUBLISHER] Inchidere voluntara.')
    finally:
        channel.close()
        print(f'[PUBLISHER] {publisher_id} oprit.')


if __name__ == '__main__':
    run_publisher()
