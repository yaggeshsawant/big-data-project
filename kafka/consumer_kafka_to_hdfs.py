#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
consumer_kafka_to_hdfs.py
Python 2.7.17 compatible. Consumes JSON batch messages from Kafka and
appends them as line-delimited JSON files into the HDFS landed zone.
Dependencies:
    pip install kafka-python==1.4.7 hdfs==2.5.8
"""

import json
from kafka import KafkaConsumer
from hdfs import InsecureClient

HDFS_WEBHDFS_URL = "http://localhost:50070"
HDFS_USER = "talentum"
HDFS_LANDED_DIR = "/fmcg/raw/landed"
KAFKA_BOOTSTRAP = "localhost:9092"
KAFKA_TOPIC = "fmcg-batch-load"
GROUP_ID = "fmcg-landed-writer"
IDLE_TIMEOUT_MS = 120000  # stop consuming if no new message for 120s

def main():
    client = InsecureClient(HDFS_WEBHDFS_URL, user=HDFS_USER)
    consumer = KafkaConsumer(
        bootstrap_servers=KAFKA_BOOTSTRAP,
        group_id=GROUP_ID,
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        key_deserializer=lambda k: k.decode("utf-8") if k else None,
        auto_offset_reset="earliest",
        enable_auto_commit=False,
        consumer_timeout_ms=IDLE_TIMEOUT_MS
    )
    consumer.subscribe([KAFKA_TOPIC])
    print("[consumer] connecting to Kafka at %s, topic=%s, group=%s" % (KAFKA_BOOTSTRAP, KAFKA_TOPIC, GROUP_ID), flush=True)

    # Make sure the consumer starts from the beginning of the topic for this run.
    while not consumer.assignment():
        consumer.poll(timeout_ms=1000)
    print("[consumer] assigned partitions: %s" % consumer.assignment(), flush=True)
    print("[consumer] starting from the current consumer-group offset", flush=True)

    batches_written = 0
    rows_written = 0

    for message in consumer:
        batch_id = message.value.get("batch_id")
        rows = message.value.get("rows", [])
        out_path = "%s/batch_%05d.json" % (HDFS_LANDED_DIR, batch_id)

        # line-delimited JSON for PySpark
        lines = "\n".join(json.dumps(r) for r in rows)
        with client.write(out_path, encoding="utf-8", overwrite=True) as writer:
            writer.write(lines)

        batches_written += 1
        rows_written += len(rows)
        consumer.commit()
        print("[consumer] wrote %s (%d rows) -> %d batches, %d rows so far"
              % (out_path, len(rows), batches_written, rows_written), flush=True)

    consumer.close()
    print("[consumer] idle timeout reached. total batches: %d, total rows: %d"
          % (batches_written, rows_written), flush=True)

if __name__ == "__main__":
    main()

