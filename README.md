Laboratorul 1

# 1. Descriere
Proiectul implementează un message broker orientat pe evenimente, dezvoltat în Python, care asigură comunicarea asincronă și fiabilă între componente distribuite prin socket-uri TCP.

# 2. Cerințe acoperite

- Comunicare de rețea TCP (`SOCK_STREAM`) cu mesaje structurate în format JSON.
- Arhitectură concurentă cu fir de execuție separat (`threading`) pentru fiecare client conectat.
- Model de Comunicație Publish / Subscribe cu rutare dinamică pe topicuri:
- `all:students` (multicast către toți studenții)
- `course:<nume_curs>` (notificări pentru un curs specific)
- `student:<id_student>` (mesaje directe / private)
- Cozi de mesaje dedicate salvate pe disc pentru fiecare client.
- Mecanism de confirmare la nivel de aplicație (ACK).
- Re-transmisie automată (Retry) după un interval de timeout (`ACK_TIMEOUT = 5s`) în lipsa unui ACK, limitată la maximum 3 încercări (`MAX_RETRIES = 3`).
- Persistența datelor pe disc (în directorul `broker_storage/`) pentru mesajele nelivrate când clienții sunt offline.
- Dead Letter Queue (DLQ) pentru interceptarea mesajelor corupte sau care au eșuat în mod repetat la livrare (`dlq.json`).
- Metadate avansate atașate mesajelor: `messageId`, `correlationId`, `messageType`, `schemaVersion` și `occurredAt` (în format UTC).
- Deduplicare automată la nivelul subscriberului pe baza `messageId`.

#  3. Pornirea Sistemului (Pași pentru rulare)

Deschideți mai multe terminale în VS Code pentru a rula componentele separat:

## Terminal 1 - Pornirea Brokerului
python broker.py

## Terminal 2 - Pornirea unui Subscriber (Student)
Puteți specifica un ID de student și cursurile la care doriți să vă abonați:
python subscriber.py stud_1 PAD BD

## Terminal 3 — Pornirea altui Subscriber
python subscriber.py stud_2 PAM

## Terminal 4 — Pornirea Publisher-ului
python publisher.py publisher-1

Meniul interactiv vă va permite să alegeți dacă trimiteți un mesaj către toți studenții (`all:students`), către un curs anume (`course:<nume>`) sau către un student direct (`student:<id>`).

# 4. Testarea funcționalităților principale

## A. Demo Multicast / Publish-Subscribe
1. Conectați doi subscriberi (de exemplu, `stud_1` și `stud_2`) abonați la același curs (ex. `python`).
2. Trimiteți un mesaj din publisher pentru cursul respectiv.
3. Ambele instanțe de subscriber vor recepționa și afișa payload-ul în mod concurent.

## B. Demo Persistență (Offline Messaging)
1. Porniți brokerul și un subscriber (ex. `stud_1`), apoi închideți subscriberul (prin `Ctrl+C`).
2. Trimiteți câteva mesaje din publisher destinate acestuia.
3. Verificați directorul `broker_storage/` — veți observa că mesajele au fost salvate în fișierul `stud_1.json`.
4. Reporniți subscriberul (`python subscriber.py stud_1 python`); acesta va descărca automat mesajele restante și va trimite ACK.

## C. Demo Retry & Dead Letter Queue (DLQ)
1. Porniți un subscriber în modul fără confirmare (`--no-ack`):
python subscriber.py stud_1 BD --no-ack
2. Trimiteți un mesaj din publisher. Deoarece subscriberul refuză să trimită ACK, brokerul va încerca retransmisia de maximum 3 ori (`MAX_RETRIES`).
3. După epuizarea încercărilor, mesajul va fi mutat automat în coșul de erori `broker_storage/dlq.json` cu motivul specificat (`dlqReason`).

# 5. Rularea Testelor Automate

Proiectul conține o suită completă de teste unitare și de integrare folosind `pytest`.

## Pentru a rula toate testele:
python -m pytest -q

## Pentru a rula un test specific:
python -m pytest tests/test_broker.py -q
python -m pytest tests/test_protocol.py -q
python -m pytest tests/test_storage.py -q

## Ce verifică fiecare test automat (pytest)
test_protocol.py: Serializarea JSON, delimitarea corectă a mesajelor prin \n și prezența metadatelor obligatorii (messageId, schemaVersion etc.).

test_storage.py: Salvarea pe disc a mesajelor pentru clienții offline, ștergerea lor după ACK și scrierea corectă în DLQ (dlq.json).

test_broker.py: Corectitudinea rutării pe topicuri, declanșarea retransmisiei la timeout și oprirea după maxim 3 încercări.

# 6. Structura
Repository-ului

lab-pad-v3/
│
├── broker.py          # Logica centrală a serverului TCP, thread-uri, cron jobs, retry & ACK
├── protocol.py        # Gestionarea fluxului JSON și delimitarea prin newline (\n)
├── publisher.py       # Interfața clientului pentru trimiterea mesajelor pe topicuri
├── subscriber.py      # Clientul care recepționează mesaje, trimite ACK și filtrează duplicatele
├── storage.py         # Persistența cozilor pe disc și gestionarea fișierului DLQ
├── requirements.txt   # Dependențele proiectului (pytest)
├── .gitignore         # Fișiere excluse din controlul versiunilor
│
├── broker_storage/    # Directorul de stocare locală
│   ├── dlq.json       # Mesajele eșuate
│   └── stud_*.json    # Cozile persistente per client
│
└── tests/             # Suita de teste automate
    ├── init.py
    ├── test_broker.py
    ├── test_protocol.py
    └── test_storage.py