"""IOT-09 : les seuils vectorisés doivent reproduire classify.py ; l'Isolation Forest doit voir une anomalie contextuelle."""
import numpy as np
import pandas as pd

from src.common.defects import SENTINEL
from src.ml.isolation_forest import experiment_b, threshold_flags
from src.streaming.classify import classify


def _frame(rows):
    df = pd.DataFrame(rows, columns=["metric", "unit", "value"])
    df["value_c"] = np.where((df["unit"] == "F") & (df["value"] != SENTINEL), (df["value"] - 32) * 5 / 9, df["value"])
    return df


def test_seuils_vectorises_egaux_a_classify():
    rows = [("temperature", "C", v) for v in (-11, -10, 24, 60, 61, SENTINEL)]
    rows += [("temperature", "F", v) for v in (75.2, 200.0, SENTINEL)]
    rows += [("air_humidity", "%", v) for v in (-1, 0, 100, 130)]
    rows += [("co2", "ppm", v) for v in (0, 650, 3000, 5000)]
    df = _frame(rows)
    expected = [classify(m, v, u)[1] != "ok" for m, u, v in rows]
    assert threshold_flags(df).tolist() == expected


def test_isolation_forest_detecte_les_anomalies_contextuelles():
    rng = np.random.default_rng(0)
    times = pd.date_range("2026-11-16", periods=6 * 288, freq="5min", tz="UTC")   # 6 jours
    frames = []
    for i in range(20):
        hour = (times.hour + times.minute / 60).to_numpy()
        value = 24 + 6 * np.sin(2 * np.pi * (hour - 9) / 24) + rng.normal(0, 0.3, len(times))
        frames.append(pd.DataFrame({"sensor_id": f"S{i:02d}", "metric": "temperature", "unit": "C",
                                    "value": value, "value_c": value, "event_time": times, "y_real": 0}))
    df = pd.concat(frames, ignore_index=True)
    res, n_pos, _ = experiment_b(df, contamination=0.01, seed=42)
    assert n_pos > 10
    assert res["Seuils fixes"]["recall"] == 0.0           # valeurs plausibles : les seuils ne voient rien
    assert res["Isolation Forest"]["recall"] > 0.8
