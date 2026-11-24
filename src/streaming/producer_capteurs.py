"""IOT-01 (live) : simule les 60 capteurs en direct vers Kafka (topic "capteurs", clé = sensor_id).

Usage (depuis la racine du projet) :
  python -m src.streaming.producer_capteurs                      # un tick toutes les 30 s
  python -m src.streaming.producer_capteurs --interval 5         # démo plus rapide
  python -m src.streaming.producer_capteurs --heat G05           # force une canicule dans G05 (alerte)
  python -m src.streaming.producer_capteurs --dry-run --ticks 2  # affiche sans Kafka
"""
from __future__ import annotations

import argparse
import heapq
import itertools
import json
import os
import time
from datetime import datetime, timezone

import numpy as np
from dotenv import load_dotenv

from src.common.defects import LATE_SECONDS, RATES, SENTINEL, outlier_value
from src.common.sensors import build_registry, clean_value, to_emitted_unit

TOPIC = "capteurs"


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--interval", type=int, default=30, help="secondes entre deux mesures")
    ap.add_argument("--ticks", type=int, default=0, help="0 = sans fin")
    ap.add_argument("--heat", nargs="*", default=[], help="serres en canicule, ex. G05")
    ap.add_argument("--outage-prob", type=float, default=0.02, help="proba de panne par tick")
    ap.add_argument("--seed", type=int, default=7)
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()
    load_dotenv()

    rng = np.random.default_rng(args.seed)
    registry = build_registry(42)          # même parc de capteurs que l'historique
    producer = None
    if not args.dry_run:
        from confluent_kafka import Producer
        producer = Producer({"bootstrap.servers": os.environ["KAFKA_BOOTSTRAP_SERVERS"],
                             "linger.ms": 50, "enable.idempotence": True})

    pending: list = []                     # messages en retard : (heure d'envoi, n°, clé, charge)
    counter = itertools.count()
    outages: dict[str, list] = {}          # sensor_id -> [type, ticks restants]
    stats = {"sent": 0, "late": 0, "dup": 0, "missing": 0, "outlier": 0, "silent": 0}

    def send(key: str, payload: dict) -> None:
        stats["sent"] += 1
        if producer is None:
            print(json.dumps(payload))
            return
        producer.produce(TOPIC, key=key, value=json.dumps(payload))
        producer.poll(0)

    tick = 0
    try:
        while args.ticks == 0 or tick < args.ticks:
            t0 = time.time()
            now = datetime.fromtimestamp(t0, tz=timezone.utc).replace(microsecond=0)
            ts = np.array([np.datetime64(now.replace(tzinfo=None), "s")])

            # pannes : on en démarre parfois une, on fait avancer les autres
            if rng.random() < args.outage_prob:
                s = registry[int(rng.integers(len(registry)))]
                if s.sensor_id not in outages:
                    outages[s.sensor_id] = [str(rng.choice(["sentinel", "silence"])), int(rng.integers(6, 20))]
                    print(f"PANNE {outages[s.sensor_id][0]} sur {s.sensor_id} ({outages[s.sensor_id][1]} ticks)")

            for sensor in registry:
                outage = outages.get(sensor.sensor_id)
                if outage and outage[0] == "silence":
                    stats["silent"] += 1
                    continue                                   # capteur muet : aucun message
                value = float(clean_value(sensor, ts, rng)[0])
                if sensor.metric == "temperature" and sensor.greenhouse_id in args.heat:
                    value = max(value + 12, 36.0)              # canicule forcée (démo d'alerte)
                if rng.random() < RATES["outlier"]:
                    value = float(outlier_value(sensor.metric, 1, rng)[0]); stats["outlier"] += 1
                if sensor.unit == "F":
                    value = float(to_emitted_unit(sensor, np.array([value]))[0])
                out_value: float | None = value
                if rng.random() < RATES["missing"]:
                    out_value = None; stats["missing"] += 1
                if outage and outage[0] == "sentinel":
                    out_value = SENTINEL

                payload = {"sensor_id": sensor.sensor_id, "greenhouse_id": sensor.greenhouse_id,
                           "metric": sensor.metric, "value": out_value, "unit": sensor.unit,
                           "event_time": now.isoformat()}
                copies = 2 if rng.random() < RATES["duplicate"] else 1   # doublon : même contenu
                stats["dup"] += copies - 1
                for _ in range(copies):
                    if rng.random() < RATES["late"]:           # retardataire : mesure ancienne, envoi différé
                        delay = float(rng.uniform(*LATE_SECONDS)); stats["late"] += 1
                        heapq.heappush(pending, (t0 + delay, next(counter), sensor.sensor_id, payload))
                    else:
                        send(sensor.sensor_id, payload)

            for key in list(outages):                          # fin des pannes
                outages[key][1] -= 1
                if outages[key][1] <= 0:
                    del outages[key]

            tick += 1
            # sleep jusqu'au prochain tick, en libérant les retardataires échus
            next_tick = t0 + args.interval
            while time.time() < next_tick:
                while pending and pending[0][0] <= time.time():
                    _, _, key, payload = heapq.heappop(pending)
                    send(key, payload)
                time.sleep(0.2)
            print(f"tick {tick} | {stats} | en attente (retard) : {len(pending)}")
    except KeyboardInterrupt:
        print("\narrêt demandé")
    finally:
        if producer is not None:
            producer.flush(10)
        print(f"{len(pending)} retardataires jamais envoyés (perdus à l'arrêt)")


if __name__ == "__main__":
    main()
