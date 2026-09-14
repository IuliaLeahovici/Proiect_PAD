import json
import os
import threading
STORAGE_DIR = "broker_storage"
DLQ_FILE = os.path.join(STORAGE_DIR, "dlq.json")
_file_lock = threading.Lock()
if not os.path.exists(STORAGE_DIR):
    os.makedirs(STORAGE_DIR)
def _queue_path(client_id):
    return os.path.join(STORAGE_DIR, f"{client_id}.json")
def save_queue_to_disk(client_id, messages):
    with _file_lock:
        with open(_queue_path(client_id), "w") as f:
            json.dump(messages, f, indent=2)
def load_queue_from_disk(client_id):
    filepath = _queue_path(client_id)
    if os.path.exists(filepath):
        with open(filepath, "r") as f:
            try:
                return json.load(f)
            except Exception:
                return []
    return []
def save_to_dlq(message, reason):
    with _file_lock:
        dlq_data = []
        if os.path.exists(DLQ_FILE):
            with open(DLQ_FILE, "r") as f:
                try:
                    dlq_data = json.load(f)
                except Exception:
                    dlq_data = []
        message = dict(message)
        message["dlqReason"] = reason
        dlq_data.append(message)
        with open(DLQ_FILE, "w") as f:
            json.dump(dlq_data, f, indent=2)