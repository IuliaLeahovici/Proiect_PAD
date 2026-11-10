import time
import grpc
import messaging_pb2 as pb
import messaging_pb2_grpc as rpc

BROKER_ADDRESS = '127.0.0.1:50051'
RECONNECT_DELAY = 3

def run_subscriber():
    subscriber_id = input('Introdu numele: ').strip()
    if not subscriber_id:
        print('[ERROR] Numele nu poate fi gol.')
        return
    topic = input('Introdu topicul: ').strip()
    if not topic:
        print('[ERROR] Topicul nu poate fi gol.')
        return
    answer = input('Trimite ACK? (d/n): ').strip().lower()
    no_ack = answer in ('n', 'nu')
    received_ids = set()
    print(f'\nNume       : {subscriber_id}')
    print(f'Topic      : {topic}')
    print(f'Broker     : {BROKER_ADDRESS}')
    print(f'ACK activ  : {not no_ack}\n')
    try:
        while True:
            channel = grpc.insecure_channel(BROKER_ADDRESS)
            stub = rpc.MessageBrokerStub(channel)
            try:
                grpc.channel_ready_future(channel).result(timeout=5)
                print('[CONECTAT] Conectat la Broker prin gRPC.')
                stream = stub.Subscribe(pb.SubscribeRequest(subscriberId=subscriber_id, topic=topic))
                print('[SUBSCRIBER] Astept mesaje...\n')
                for message in stream:
                    mid = message.messageId
                    short = mid[:8]
                    if mid in received_ids:
                        print(f'[DUPLICATE] Mesajul {short} a fost deja primit.')
                    else:
                        received_ids.add(mid)
                        print('\nMESAJ PRIMIT')
                        print(f'Topic      : {message.topic}')
                        print(f'Message ID : {short}')
                        print(f'Mesaj      : {message.payload}')
                    if no_ack:
                        print(f'[NO ACK] Nu confirm mesajul {short}')
                    else:
                        try:
                            response = stub.Acknowledge(pb.AckRequest(subscriberId=subscriber_id, messageId=mid), timeout=5)
                            print(f"[ACK] {'Confirmat' if response.success else 'Mesajul nu mai este in coada'}: {short}")
                        except grpc.RpcError as error:
                            print(f'[ACK ERROR] {error.code().name}')
            except grpc.FutureTimeoutError:
                print('[ERROR] Brokerul nu este disponibil.')
            except grpc.RpcError as error:
                print(f'[RPC ERROR] {error.code().name}: {error.details()}')
                if error.code() in (grpc.StatusCode.ALREADY_EXISTS, grpc.StatusCode.FAILED_PRECONDITION, grpc.StatusCode.INVALID_ARGUMENT):
                    return
            finally:
                channel.close()
            print(f'[RECONNECT] Reincercare in {RECONNECT_DELAY} secunde...')
            time.sleep(RECONNECT_DELAY)
    except (KeyboardInterrupt, EOFError):
        print('\n[SUBSCRIBER] Inchidere voluntara.')
    finally:
        print(f"[SUBSCRIBER] '{subscriber_id}' oprit.")


if __name__ == '__main__':
    run_subscriber()
