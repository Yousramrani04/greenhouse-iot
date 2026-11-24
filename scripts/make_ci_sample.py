"""Petit échantillon Parquet au format de l'export Silver, pour 'dbt build' en CI (sans Spark ni Docker).

60 capteurs x 3 h, quelques minutes manquantes, une canicule de 20 min en G05 (-> une période d'alerte),
quelques rejets. Écrit dans ci_sample/ (ignoré par git).
"""
import sys
from pathlib import Path

import numpy as np
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))   # lancé comme script : racine du projet
from src.common.sensors import build_registry, clean_value  # noqa: E402

OUT = Path("ci_sample")
rng = np.random.default_rng(42)
minutes = np.arange(np.datetime64("2026-11-20T10:00:00"), np.datetime64("2026-11-20T13:00:00"), np.timedelta64(1, "m"))

rows = []
for s in build_registry(42):
    v = clean_value(s, minutes, rng)
    df = pd.DataFrame({"sensor_id": s.sensor_id, "greenhouse_id": s.greenhouse_id, "metric": s.metric,
                       "minute_ts": minutes, "value": v})
    if s.sensor_id == "G05-TEMP":
        df.loc[60:79, "value"] = 38.0                      # 20 minutes au-dessus de 35 °C
    df["unit"] = {"temperature": "C", "air_humidity": "%", "soil_humidity": "%", "co2": "ppm"}[s.metric]
    rows.append(df[rng.random(len(df)) > 0.04])            # ~4 % de minutes absentes
silver = pd.concat(rows, ignore_index=True)
silver["n_readings"] = rng.integers(1, 3, len(silver)).astype("int32")
silver["minute_ts"] = silver["minute_ts"].astype("datetime64[us]")
silver["last_ingested_at"] = silver["minute_ts"] + pd.Timedelta(seconds=45)

sensors = silver[["sensor_id", "greenhouse_id", "metric"]].drop_duplicates().sample(20, random_state=1)
rej = sensors.assign(event_time=pd.Timestamp("2026-11-20 11:00:00"), raw_value=-999.0, raw_unit="C",
                     reason=rng.choice(["null", "sentinel", "out_of_range"], len(sensors)),
                     ingested_at=pd.Timestamp("2026-11-20 11:00:30"))
rej["event_time"] = rej["event_time"].astype("datetime64[us]")
rej["ingested_at"] = rej["ingested_at"].astype("datetime64[us]")

for name, df in (("measurements_1min", silver), ("measurements_rejected", rej)):
    d = OUT / name
    d.mkdir(parents=True, exist_ok=True)
    df.to_parquet(d / "part-0.parquet", index=False)
    print(f"{d}/part-0.parquet : {len(df)} lignes")
