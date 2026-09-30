import random
import socket

def bind_random_local_port(
    sock,
    host="127.0.0.1",
    port_range=(20000, 60000),
    max_attempts=30
):
    for _ in range(max_attempts):
        port = random.randint(
            *port_range
        )
        try:
            sock.bind(
                (host, port)
            )
            return host, port
        except OSError:
            continue
    raise RuntimeError(
        "Nu s-a gasit niciun port local liber."
    )