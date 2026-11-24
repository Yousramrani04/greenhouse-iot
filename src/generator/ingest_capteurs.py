"""IOT-01 : 7 jours d'historique de 60 capteurs, 6 défauts, écriture en Bronze.

Usage (depuis la racine du projet) :
    python -m src.generator.ingest_capteurs --end 2026-11-23T00:00:00
"""
from __future__ import annotations

import argparse
import io
import os

import boto3
import numpy as np
import pandas as pd
from botocore.config import Config
from dotenv import load_dotenv

from src.common.defects import (
    LATE_SECONDS, N_SENTINEL_OUTAGES, N_SILENT_OUTAGES, NORMAL_LATENCY_SECONDS,
    OUTAGE_HOURS, RATES, SENTINEL, outlier_value,
)
from src.common.sensors import (
    HISTORY_DAYS, SAMPLE_SECONDS, Sensor, build_registry, clean_value, to_emitted_unit,
)


def plan_outages(registry: list[Sensor], start, end, rng) -> dict:
    """Choisit quels capteurs tombent en panne, quand, et de quel type."""
    n = N_SENTINEL_OUTAGES + N_SILENT_OUTAGES
    chosen = rng.choice(len(registry), size=n, replace=False)
    span_s = int((end - start) / np.timedelta64(1, "s"))
    plan = {}
    for i, idx in enumerate(chosen):
        dur = int(rng.uniform(*OUTAGE_HOURS) * 3600)
        offset = int(rng.integers(3600, span_s - dur - 3600))   # loin des bords
        a = start + np.timedelta64(offset, "s")
        kind = "sentinel" if i < N_SENTINEL_OUTAGES else "silence"
        plan[registry[idx].sensor_id] = (kind, a, a + np.timedelta64(dur, "s"))
    return plan


def generate_sensor(sensor: Sensor, ts: np.ndarray, rng, outage):
    """Données d'un capteur avec défauts. Renvoie (messages, vérité terrain)."""
    n = len(ts)
    value = clean_value(sensor, ts, rng)

    # 1. aberrantes (en unité canonique, avant conversion éventuelle en °F)
    m_outlier = rng.random(n) < RATES["outlier"]
    value[m_outlier] = outlier_value(sensor.metric, int(m_outlier.sum()), rng)

    # 2. unité émise par le capteur
    value = to_emitted_unit(sensor, value)

    # 3. manquantes
    m_missing = rng.random(n) < RATES["missing"]
    value[m_missing] = np.nan

    # 4. pannes : une panne prime sur les autres défauts de la même mesure
    m_sentinel = np.zeros(n, dtype=bool)
    m_silent = np.zeros(n, dtype=bool)
    if outage:
        kind, a, b = outage
        window = (ts >= a) & (ts < b)
        if kind == "sentinel":
            m_sentinel = window
            value[m_sentinel] = SENTINEL
        else:
            m_silent = window
    broken = m_sentinel | m_silent
    m_outlier &= ~broken
    m_missing &= ~broken

    # 5. retards : on décale l'heure d'ARRIVÉE, jamais l'heure de mesure
    delay = rng.uniform(*NORMAL_LATENCY_SECONDS, n)
    m_late = (rng.random(n) < RATES["late"]) & ~m_silent
    delay[m_late] = rng.uniform(*LATE_SECONDS, int(m_late.sum()))

    event_time = pd.to_datetime(ts, utc=True)
    df = pd.DataFrame({
        "sensor_id": sensor.sensor_id,
        "greenhouse_id": sensor.greenhouse_id,
        "metric": sensor.metric,
        "value": value,
        "unit": sensor.unit,
        "event_time": event_time,
        "_ingested_at": event_time + pd.to_timedelta(delay, unit="s"),
    })

    truth = [
        pd.DataFrame({"sensor_id": sensor.sensor_id, "event_time": event_time[mask], "defect": name})
        for name, mask in [("outlier", m_outlier), ("missing", m_missing),
                           ("sentinel_outage", m_sentinel), ("silence", m_silent),
                           ("late", m_late)]
        if mask.any()
    ]

    df = df[~m_silent]            # un capteur muet n'envoie aucun message

    # 6. doublons : même contenu, reçu une 2e fois quelques secondes après
    dups = df[rng.random(len(df)) < RATES["duplicate"]].copy()
    dups["_ingested_at"] += pd.to_timedelta(rng.uniform(0.5, 5, len(dups)), unit="s")
    truth.append(dups[["sensor_id", "event_time"]].assign(defect="duplicate"))

    return pd.concat([df, dups]), pd.concat(truth)


def s3_client():
    return boto3.client(
        "s3",
        endpoint_url=os.environ["S3_ENDPOINT_URL"],
        aws_access_key_id=os.environ["MINIO_ROOT_USER"],
        aws_secret_access_key=os.environ["MINIO_ROOT_PASSWORD"],
        region_name="us-east-1",
        config=Config(s3={"addressing_style": "path"}),
    )


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--end", help="fin de l'historique, UTC (défaut : maintenant)")
    parser.add_argument("--seed", type=int, default=42)
    args = parser.parse_args()
    load_dotenv()

    end_ts = pd.Timestamp(args.end) if args.end else pd.Timestamp.now(tz="UTC").tz_localize(None)
    end = end_ts.floor(f"{SAMPLE_SECONDS}s").to_datetime64().astype("datetime64[s]")
    start = end - np.timedelta64(HISTORY_DAYS, "D")
    ts = np.arange(start, end, np.timedelta64(SAMPLE_SECONDS, "s"))
    print(f"fenêtre : {start} -> {end} UTC ({len(ts)} mesures par capteur)")

    rng = np.random.default_rng(args.seed)
    registry = build_registry(args.seed)
    outages = plan_outages(registry, start, end, rng)

    frames, truths = [], []
    for sensor in registry:
        df, truth = generate_sensor(sensor, ts, rng, outages.get(sensor.sensor_id))
        frames.append(df)
        truths.append(truth)

    data = pd.concat(frames, ignore_index=True)
    # Piège Spark : les timestamps en nanosecondes ne sont pas lisibles -> microsecondes UTC
    for col in ("event_time", "_ingested_at"):
        data[col] = data[col].astype("datetime64[us, UTC]")
    data["_batch_id"] = f"hist-{args.seed}"
    data = data.sort_values("_ingested_at")       # Bronze = ordre d'arrivée
    data["event_date"] = data["event_time"].dt.strftime("%Y-%m-%d")

    # Vérité terrain : locale, jamais dans Bronze (qui reste brut)
    os.makedirs("data/ground_truth", exist_ok=True)
    pd.concat(truths).to_parquet("data/ground_truth/history_defects.parquet", index=False)
    pd.DataFrame(
        [(sid, k, a, b) for sid, (k, a, b) in outages.items()],
        columns=["sensor_id", "kind", "start", "end"],
    ).to_csv("data/ground_truth/outages.csv", index=False)

    s3 = s3_client()
    for day, part in data.groupby("event_date"):
        buf = io.BytesIO()
        part.drop(columns="event_date").to_parquet(
            buf, index=False, compression="zstd", coerce_timestamps="us")
        # Nom FIXE -> relancer écrase au lieu de dupliquer (écriture idempotente)
        key = f"historique/capteurs/event_date={day}/part-0.parquet"
        s3.put_object(Bucket="bronze", Key=key, Body=buf.getvalue())
        print(f"{key} : {len(part):>8} lignes, {buf.tell() / 1e6:5.1f} Mo")

    print("\npannes planifiées :")
    print(pd.read_csv("data/ground_truth/outages.csv").to_string(index=False))


if __name__ == "__main__":
    main()
