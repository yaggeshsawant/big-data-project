# -*- coding: utf-8 -*-
"""
producer_drive_stream_to_kafka.py
Python 2.7.17 compatible. Streams CSV rows directly from Google Drive
into Kafka without saving locally.
Dependencies:
    pip install kafka-python==1.4.7 pydrive==1.3.1 oauth2client==4.1.3
"""

import csv
import json
import io
from kafka import KafkaProducer
from pydrive.auth import GoogleAuth
from pydrive.drive import GoogleDrive
from oauth2client.service_account import ServiceAccountCredentials

# -------------------------------
# Google Drive Authentication
# -------------------------------
scope = ['https://www.googleapis.com/auth/drive']

credentials = ServiceAccountCredentials.from_json_keyfile_name(
    '/home/talentum/Desktop/Big_data_Project/service_account.json', scope
)

gauth = GoogleAuth()
gauth.credentials = credentials
drive = GoogleDrive(gauth)

print("[drive] authenticated with Google Drive")

# -------------------------------
# Kafka Configuration
# -------------------------------
KAFKA_BOOTSTRAP = "localhost:9092"
KAFKA_TOPIC = "fmcg-batch-load"
BATCH_SIZE = 1000

def get_producer():
    return KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8"),
        linger_ms=50,
        retries=3,
        acks="all"
    )

def stream_rows_from_drive(file_id):
    # Get file content as string
    file_obj = drive.CreateFile({'id': file_id})
    content = file_obj.GetContentString()   # read file directly into memory
    text_stream = io.StringIO(content)
    reader = csv.DictReader(text_stream)
    for row in reader:
        yield row

def main():
    producer = get_producer()
    batch, batch_num, total_rows = [], 0, 0

    for row in stream_rows_from_drive("1W_lJSB4eiV_db8gTtU7Y-pmnHKuXqbo4"):
        batch.append(row)
        total_rows += 1
        if len(batch) >= BATCH_SIZE:
            batch_num += 1
            key = "batch-%05d" % batch_num
            future = producer.send(KAFKA_TOPIC, key=key,
                                   value={"batch_id": batch_num, "rows": batch})
            future.get(timeout=30)
            print("[producer] sent %s with %d rows (total so far: %d)" %
                  (key, len(batch), total_rows))
            batch = []

    if batch:
        batch_num += 1
        key = "batch-%05d" % batch_num
        future = producer.send(KAFKA_TOPIC, key=key,
                               value={"batch_id": batch_num, "rows": batch})
        future.get(timeout=30)
        print("[producer] sent final %s with %d rows" % (key, len(batch)))

    producer.flush()
    producer.close()
    print("[producer] done. total rows: %d" % total_rows)

if __name__ == "__main__":
    main()
