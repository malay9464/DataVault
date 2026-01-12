import csv
import os
from datetime import datetime

LOG_DIR = "logs"
LOG_FILE = os.path.join(LOG_DIR, "upload_log.csv")

HEADERS = [
    "uploaded_at",
    "filename",
    "total_records",
    "duplicate_records",
    "failed_records",
    "status"
]

def log_to_csv(filename, total, duplicate, failed, status):
    os.makedirs(LOG_DIR, exist_ok=True)
    file_exists = os.path.isfile(LOG_FILE)

    with open(LOG_FILE, "a", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        if not file_exists:
            writer.writerow(HEADERS)

        writer.writerow([
            datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            filename,
            total,
            duplicate,
            failed,
            status
        ])
