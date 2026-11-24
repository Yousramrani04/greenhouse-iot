"""Groupe "bronze-writer" : Kafka -> MinIO (Bronze, Parquet brut).

Fichiers : s3://bronze/live/capteurs/event_date=AAAA-MM-JJ/p<partition>-<offset min>-<offset max>.parquet
Les offsets ne sont validés QU'APRÈS l'écriture (au moins une fois ; les doublons sont éliminés en Silver).

Usage : python -m src.streaming.bronze_writer
"""
from __future__ import annotations

import io
import json
import os
import time

import pandas as pd
from confluent_kafka import Consumer
from dotenv import load_dotenv

from src.generator.ingest_capteurs import s3_client

GROUP = "bronze-writer"
TOPIC = "capteurs"
MAX_ROWS = 2000
FLUSH_SECONDS = 30


def flush(rows: list, s3, consumer) -> None:
    if not rows:
        return
    df = pd.DataFrame([r["payload"] for r in rows])
    df["partition"] = [r["partition"] for r in rows]
    df["offset"] = [r["offset"] for r in rows]
    df["value"] = pd.to_numeric(df["value"], errors="coerce")            # None -> NaN
    df["event_time"] = pd.to_datetime(df["event_time"], utc=True).astype("datetime64[us, UTC]")
    df["_ingested_at"] = pd.Timestamp.now(tz="UTC").floor("us")
    df["_ingested_at"] = pd.to_datetime(df["_ingested_at"], utc=True).astype("datetime64[us, UTC]")
    df["_batch_id"] = f"live-{GROUP}"
    df["event_date"] = df["event_time"].dt.strftime("%Y-%m-%d")
    cols = ["sensor_id", "greenhouse_id", "metric", "value", "unit",
            "event_time", "_ingested_at", "_batch_id"]
    for (part, day), g in df.groupby(["partition", "event_date"]):
        buf = io.BytesIO()
        g[cols].to_parquet(buf, index=False, compression="zstd", coerce_timestamps="us")
        key = (f"live/capteurs/event_date={day}/"
               f"p{part}-{g['offset'].min()}-{g['offset'].max()}.parquet")
        s3.put_object(Bucket="bronze", Key=key, Body=buf.getvalue())
    consumer.commit(asynchronous=False)                                  # APRÈS l'écriture
    print(f"{time.strftime('%H:%M:%S')} Bronze : {len(df)} messages écrits")


def main() -> None:
    load_dotenv()
    s3 = s3_client()
    consumer = Consumer({"bootstrap.servers": os.environ["KAFKA_BOOTSTRAP_SERVERS"],
                         "group.id": GROUP, "enable.auto.commit": False,
                         "auto.offset.reset": "earliest"})
    consumer.subscribe([TOPIC])
    rows: list = []
    last_flush = time.time()
    print(f"{GROUP} démarré (Ctrl-C pour arrêter)")
    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is not None and not msg.error():
                rows.append({"payload": json.loads(msg.value()),
                             "partition": msg.partition(), "offset": msg.offset()})
            elif msg is not None:
                print("erreur Kafka :", msg.error())
            if len(rows) >= MAX_ROWS or (rows and time.time() - last_flush >= FLUSH_SECONDS):
                flush(rows, s3, consumer)
                rows, last_flush = [], time.time()
    except KeyboardInterrupt:
        print("\narrêt : écriture du dernier lot")
        flush(rows, s3, consumer)
    finally:
        consumer.close()


if __name__ == "__main__":
    main()
