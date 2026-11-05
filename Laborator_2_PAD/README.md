# Laboratorul 2 PAD – Sistem de mesagerie Publish/Subscribe prin gRPC

## Descriere

Proiectul implementează în Python un sistem de mesagerie de tip **Publish/Subscribe**, cu un **Broker gRPC** central, **Publisheri** care publică mesaje și **Subscriberi** care primesc mesaje doar pentru topicurile la care sunt abonați.

Brokerul gestionează cozi individuale pentru fiecare Subscriber, confirmări **ACK**, retrimiteri în caz de lipsă a confirmării, o **Dead Letter Queue (DLQ)** în memorie și controlul publicărilor duplicate prin **idempotencyKey**.

## Tehnologii

- Python 3 (testat cu Python 3.14.2)
- gRPC (`grpcio`)
- Protocol Buffers (`protobuf`)
- Pytest pentru testarea automată

## Structura proiectului

```text
Laborator_2_PAD/
├── broker_grpc.py                 # Serverul Broker gRPC
├── publisher_grpc.py              # Clientul care publică mesaje
├── subscriber_grpc.py             # Clientul care primește mesaje
├── messaging.proto                # Definițiile mesajelor și serviciului gRPC
├── messaging_pb2.py               # Cod generat din fișierul .proto
├── messaging_pb2_grpc.py          # Stub-uri și clase gRPC generate
├── requirements.txt               # Dependențe Python
└── tests/
    ├── conftest.py                # Fixture pentru Brokerul de test
    ├── helpers.py                 # Funcții auxiliare
    ├── test_publish_validation.py # Validare și idempotency
    ├── test_reliability.py        # ACK, retry, DLQ și reconectare
    └── test_subscribe_delivery.py # Abonare, topicuri și livrare
```

Fișierele `messaging_pb2.py` și `messaging_pb2_grpc.py` sunt generate automat și nu trebuie editate manual.

## Instalare

Deschideți un terminal în directorul proiectului și executați:

```
python -m pip install -r requirements.txt
```

Pentru regenerarea fișierelor gRPC din `messaging.proto`, instalați și `grpcio-tools`, apoi executați:

```
python -m pip install grpcio-tools
python -m grpc_tools.protoc -I. --python_out=. --grpc_python_out=. messaging.proto
```

## Rulare

Deschideți terminale separate în directorul proiectului, în următoarea ordine.

**1. Pornirea Brokerului**

```
python broker_grpc.py
```

Brokerul ascultă pe `127.0.0.1:50051` și afișează conexiunile, publicările, livrările, ACK-urile și starea cozilor RAM/DLQ.

**2. Pornirea unui Subscriber**

```
python subscriber_grpc.py
```

Introduceți, de exemplu:

```text
Introdu numele: stud1
Introdu topicul: bd
Trimite ACK? (d/n): d
```

Puteți porni alți Subscriberi în terminale suplimentare, cu nume diferite și topicuri identice sau diferite.

**3. Pornirea Publisherului**

```
python publisher_grpc.py
```

Introduceți, de exemplu:

```text
Introdu topicul: bd
Introdu mesajul: test1
```

Subscriberii abonați la `bd` primesc mesajul. Cei abonați la alte topicuri nu îl primesc.

Pentru oprirea aplicațiilor, apăsați `Ctrl+C` în terminalele respective.

## Funcționalități

- **Rutare după topic:** un mesaj este livrat numai Subscriberilor abonați la topicul respectiv.
- **Fan-out:** fiecare Subscriber abonat la același topic primește propria copie în coadă.
- **ACK:** confirmarea validă elimină mesajul din coada Subscriberului.
- **Retry:** în lipsa ACK, Brokerul retrimite mesajul după `ACK_TIMEOUT = 5` secunde, până la `MAX_RETRIES = 3` încercări totale de livrare.
- **DLQ:** după epuizarea încercărilor fără ACK, mesajul este eliminat din coada Subscriberului și înregistrat în DLQ din RAM.
- **Reconectare:** mesajele neconfirmate rămân în RAM și pot fi relivrate la reconectarea aceluiași Subscriber.
- **Idempotency:** aceeași cheie și același conținut returnează `ALREADY_PROCESSED`; reutilizarea cheii cu alt conținut este respinsă.
- **Verificarea publicării:** `GetPublishStatus` permite interogarea unei operații după `idempotencyKey`.
- **Validare:** sunt respinse publicările fără topic/payload/cheie sau cu `schemaVersion` diferită de `1`.
- **Concurență:** accesul la structurile comune este sincronizat cu `threading.RLock`.

## Servicii gRPC

 `Publish` - Publică un mesaj și returnează identificatorul și starea operației. 
 `Subscribe` - Deschide un flux de mesaje pentru un Subscriber și un topic. 
 `Acknowledge` - Confirmă primirea unui mesaj de către Subscriber. 
 `GetPublishStatus` - Verifică dacă o publicare identificată prin cheie a fost procesată. 

## Scenarii de demonstrare

**Livrare normală:** porniți Brokerul, un Subscriber `stud1` pe topicul `bd` cu ACK activ și Publisherul; publicați un mesaj pe `bd`. Brokerul afișează livrarea și ACK-ul, apoi eliminarea din coadă.

**Fan-out și filtrare:** porniți `stud1` și `stud3` pe `bd`, respectiv `stud2` pe `pam`; un mesaj publicat pe `bd` ajunge numai la `stud1` și `stud3`.

**Reconectare:** porniți `stud2` pe `pam`, deconectați-l, publicați un mesaj pe `pam` și reconectați-l cu același nume și topic. Mesajul stocat în RAM este livrat după reconectare.

**DLQ:** porniți `stud3` pe `bd` și răspundeți `n` la întrebarea despre ACK. Publicați un mesaj pe `bd`. După trei livrări fără confirmare, Brokerul îl mută în DLQ din RAM.

## Testare automată

Rulați din directorul principal:

```
python -m pytest tests -v
```

Testele verifică validarea publicărilor, idempotency, livrarea după topic, fan-out, ACK, retry, DLQ, reconectarea și publicările concurente.

Testele creează un Broker separat pentru fiecare caz și folosesc un **port liber alocat automat** (`127.0.0.1:0`), fără a necesita pornirea manuală a Brokerului pe portul `50051`.

## Porturi și conexiuni

- Brokerul aplicației folosește portul fix **50051** pe `127.0.0.1`.
- Sistemul de operare atribuie automat porturi sursă temporare conexiunilor clienților.
- Brokerul afișează aceste porturi în jurnal prin `context.peer()`; nu este necesară configurarea lor manuală în Publisher sau Subscriber.
- Aplicația este configurată pentru utilizare locală, nu pentru acces din rețea.


## Concluzie

În cadrul lucrării de laborator a fost realizată și testată o aplicație de mesagerie distribuită, utilizând Python, gRPC și arhitectura Publisher–Broker–Subscriber. Au fost implementate mecanisme de publicare și distribuire a mesajelor în funcție de topic, confirmare prin ACK, retransmitere automată și gestionare a mesajelor neconfirmate prin Dead Letter Queue (DLQ). De asemenea, au fost utilizate mecanisme de idempotentă și păstrare a mesajelor în memoria RAM pentru a asigura fiabilitatea comunicării. Funcționalitatea aplicației a fost verificată prin 25 de teste automate Pytest, toate executate cu succes. Rezultatele obținute au demonstrat funcționarea corectă a sistemului și au contribuit la consolidarea cunoștințelor privind comunicarea și gestionarea mesajelor în sistemele distribuite.