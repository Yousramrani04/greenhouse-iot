"""Groupe "grafana-writer" : Kafka -> PostgreSQL (table live_measurements).

Clé primaire (sensor_id, event_time) + ON CONFLICT DO NOTHING : écriture idempotente.
Les mesures invalides sont gardées avec un statut ; Grafana filtrera sur status = 'ok'.

Usage : python -m src.streaming.grafana_writer
"""
from __future__ import annotations

import json
import os
import time

import psycopg2
from confluent_kafka import Consumer
from dotenv import load_dotenv
from psycopg2.extras import execute_values

from src.streaming.classify import classify

GROUP = "grafana-writer"
TOPIC = "capteurs"

INSERT = """
INSERT INTO live_measurements
  (sensor_id, greenhouse_id, metric, event_time, raw_value, raw_unit, value, status)
VALUES %s ON CONFLICT (sensor_id, event_time) DO NOTHING"""


def main() -> None:
    load_dotenv()
    db = psycopg2.connect(host="localhost", port=os.environ["POSTGRES_PORT"],
                          user=os.environ["POSTGRES_USER"], password=os.environ["POSTGRES_PASSWORD"],
                          dbname=os.environ["POSTGRES_DB"])
    consumer = Consumer({"bootstrap.servers": os.environ["KAFKA_BOOTSTRAP_SERVERS"],
                         "group.id": GROUP, "enable.auto.commit": False,
                         "auto.offset.reset": "earliest"})
    consumer.subscribe([TOPIC])
    print(f"{GROUP} démarré (Ctrl-C pour arrêter)")
    total = 0
    try:
        while True:
            msgs = [m for m in consumer.consume(num_messages=200, timeout=1.0) if not m.error()]
            if not msgs:
                continue
            batch = []
            for m in msgs:
                p = json.loads(m.value())
                value, status = classify(p["metric"], p["value"], p["unit"])
                batch.append((p["sensor_id"], p["greenhouse_id"], p["metric"], p["event_time"],
                              p["value"], p["unit"], value, status))
            with db, db.cursor() as cur:
                execute_values(cur, INSERT, batch)
            consumer.commit(asynchronous=False)                  # APRÈS le commit Postgres
            total += len(batch)
            print(f"{time.strftime('%H:%M:%S')} Postgres : +{len(batch)} (total {total})")
    except KeyboardInterrupt:
        print("\narrêt")
    finally:
        consumer.close()
        db.close()


if __name__ == "__main__":
    main()
