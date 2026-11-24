"""IOT-09 : détection d'anomalies par Isolation Forest, comparée aux seuils fixes.

Deux expériences, mesurées avec précision, rappel, F1 et ROC-AUC :
  A. Défauts RÉELS du générateur (vérité terrain data/ground_truth/) : valeurs aberrantes et
     sentinelle -999. Détecteur seuils = règles de classify.py ; Isolation Forest sur les valeurs brutes.
  B. Anomalies CONTEXTUELLES injectées (valeurs plausibles mais anormales à cette heure) dans le flux
     déjà nettoyé : les seuils fixes ne peuvent pas les voir, l'Isolation Forest si.
     Attention : ces anomalies sont injectées par ce script, pas par la plateforme.

Isolation Forest : non supervisé, entraîné sur les 5 premiers jours, évalué sur les 2 derniers.
Variables : valeur en °C/%/ppm, heure du jour (sin, cos), écart avec la mesure précédente du capteur.

Usage (racine du projet) :
    python -m src.ml.isolation_forest                    # lit Bronze historique dans MinIO
    python -m src.ml.isolation_forest --source generator # régénère en mémoire (graine 42), sans MinIO
"""
from __future__ import annotations

import argparse
import io
import os
from pathlib import Path

import numpy as np
import pandas as pd
from sklearn.ensemble import IsolationForest
from sklearn.metrics import f1_score, precision_score, recall_score, roc_auc_score

from src.common.defects import SENTINEL
from src.streaming.classify import PLAUSIBLE

ANOMALY_DEFECTS = {"outlier", "sentinel_outage"}
TRAIN_DAYS = 5
FEATURES = ["value_c", "h_sin", "h_cos", "delta"]
# Décalage des anomalies contextuelles injectées (restent dans la plage plausible)
SHIFTS = {"temperature": 8.0, "air_humidity": -25.0, "soil_humidity": -20.0, "co2": 900.0}


# ---------- données ----------
def load_minio() -> tuple[pd.DataFrame, pd.DataFrame]:
    import boto3
    from botocore.config import Config
    from dotenv import load_dotenv
    load_dotenv()
    s3 = boto3.client("s3", endpoint_url=os.environ["S3_ENDPOINT_URL"],
                      aws_access_key_id=os.environ["MINIO_ROOT_USER"],
                      aws_secret_access_key=os.environ["MINIO_ROOT_PASSWORD"],
                      region_name="us-east-1", config=Config(s3={"addressing_style": "path"}))
    keys = [o["Key"] for o in s3.list_objects_v2(Bucket="bronze", Prefix="historique/capteurs/")["Contents"]
            if o["Key"].endswith(".parquet")]
    frames = [pd.read_parquet(io.BytesIO(s3.get_object(Bucket="bronze", Key=k)["Body"].read())) for k in keys]
    truth = pd.read_parquet("data/ground_truth/history_defects.parquet")
    return pd.concat(frames, ignore_index=True), truth


def load_generator(end: str = "2026-11-23T00:00:00", seed: int = 42) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Reproduit src/generator/ingest_capteurs.py en mémoire (mêmes appels, même graine)."""
    from src.common.sensors import HISTORY_DAYS, SAMPLE_SECONDS, build_registry
    from src.generator.ingest_capteurs import generate_sensor, plan_outages
    end_np = pd.Timestamp(end).floor(f"{SAMPLE_SECONDS}s").to_datetime64().astype("datetime64[s]")
    start = end_np - np.timedelta64(HISTORY_DAYS, "D")
    ts = np.arange(start, end_np, np.timedelta64(SAMPLE_SECONDS, "s"))
    rng = np.random.default_rng(seed)
    registry = build_registry(seed)
    outages = plan_outages(registry, start, end_np, rng)
    frames, truths = [], []
    for sensor in registry:
        df, truth = generate_sensor(sensor, ts, rng, outages.get(sensor.sensor_id))
        frames.append(df)
        truths.append(truth)
    data = pd.concat(frames, ignore_index=True)
    for col in ("event_time", "_ingested_at"):
        data[col] = data[col].astype("datetime64[us, UTC]")
    return data, pd.concat(truths, ignore_index=True)


# ---------- préparation ----------
def prepare(raw: pd.DataFrame, truth: pd.DataFrame) -> pd.DataFrame:
    """Dédoublonne (premier message reçu), retire les valeurs manquantes, ajoute value_c et l'étiquette réelle."""
    df = (raw.sort_values("_ingested_at")
             .drop_duplicates(["sensor_id", "event_time"], keep="first")
             .dropna(subset=["value"]).copy())
    # La sentinelle est testée AVANT la conversion °F -> °C (même règle que Silver)
    conv = np.where((df["unit"] == "F") & (df["value"] != SENTINEL), (df["value"] - 32) * 5 / 9, df["value"])
    df["value_c"] = conv
    bad = truth[truth["defect"].isin(ANOMALY_DEFECTS)][["sensor_id", "event_time"]].drop_duplicates()
    bad["y_real"] = 1
    df = df.merge(bad, on=["sensor_id", "event_time"], how="left")
    df["y_real"] = df["y_real"].fillna(0).astype(int)
    return df.sort_values(["sensor_id", "event_time"]).reset_index(drop=True)


def threshold_flags(df: pd.DataFrame) -> np.ndarray:
    """Seuils fixes : sentinelle ou valeur hors plage plausible (mêmes règles que src/streaming/classify.py)."""
    flag = (df["value"] == SENTINEL).to_numpy().copy()
    for metric, (lo, hi) in PLAUSIBLE.items():
        m = (df["metric"] == metric).to_numpy()
        v = df["value_c"].to_numpy()
        flag |= m & ((v < lo) | (v > hi))
    return flag


def add_features(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    t = df["event_time"].dt
    hour = t.hour + t.minute / 60 + t.second / 3600
    df["h_sin"] = np.sin(2 * np.pi * hour / 24)
    df["h_cos"] = np.cos(2 * np.pi * hour / 24)
    df["delta"] = df.groupby("sensor_id")["value_c"].diff().fillna(0.0)
    return df


def inject_subtle(df: pd.DataFrame, rng: np.random.Generator, rate: float = 0.005) -> pd.DataFrame:
    """Décale la valeur de `rate` des mesures, en restant dans la plage plausible. Ajoute y_sub."""
    df = df.copy()
    pick = rng.random(len(df)) < rate
    for metric, shift in SHIFTS.items():
        m = pick & (df["metric"] == metric).to_numpy()
        lo, hi = PLAUSIBLE[metric]
        df.loc[m, "value_c"] = (df.loc[m, "value_c"] + shift).clip(lo + 1e-6, hi - 1e-6)
    df["y_sub"] = pick.astype(int)
    return df


# ---------- détecteurs et métriques ----------
def fit_score(df: pd.DataFrame, train_mask: np.ndarray, contamination: float, seed: int) -> np.ndarray:
    """Un modèle par type de mesure, entraîné sur la période d'entraînement. Renvoie un score d'anomalie (haut = anormal)."""
    score = np.zeros(len(df))
    for metric in df["metric"].unique():
        m = (df["metric"] == metric).to_numpy()
        model = IsolationForest(n_estimators=100, contamination=contamination, random_state=seed, n_jobs=-1)
        model.fit(df.loc[m & train_mask, FEATURES])
        score[m] = -model.score_samples(df.loc[m, FEATURES])
        # seuil de décision : contamination appliquée sur l'entraînement
        thr = np.quantile(-model.score_samples(df.loc[m & train_mask, FEATURES]), 1 - contamination)
        score[m] = score[m] - thr          # > 0 : anormal
    return score


def metrics(y, pred, score=None) -> dict:
    out = {"precision": precision_score(y, pred, zero_division=0),
           "recall": recall_score(y, pred, zero_division=0),
           "f1": f1_score(y, pred, zero_division=0),
           "auc": roc_auc_score(y, score) if score is not None and 0 < y.sum() < len(y) else float("nan")}
    out["tp"], out["fp"], out["fn"] = int(((pred == 1) & (y == 1)).sum()), int(((pred == 1) & (y == 0)).sum()), int(((pred == 0) & (y == 1)).sum())
    return out


def split_mask(df: pd.DataFrame) -> tuple[np.ndarray, np.ndarray]:
    t0 = df["event_time"].min().floor("D")
    train = (df["event_time"] < t0 + pd.Timedelta(days=TRAIN_DAYS)).to_numpy()
    return train, ~train


def experiment_a(df, contamination, seed):
    train, test = split_mask(df)
    df = add_features(df)
    score = fit_score(df, train, contamination, seed)
    y = df.loc[test, "y_real"].to_numpy()
    return {"Seuils fixes": metrics(y, threshold_flags(df)[test].astype(int)),
            "Isolation Forest": metrics(y, (score[test] > 0).astype(int), score[test])}, int(y.sum()), int(test.sum())


def experiment_b(df, contamination, seed):
    clean = df[(df["y_real"] == 0) & ~threshold_flags(df)].reset_index(drop=True)
    train, test = split_mask(clean)
    # Injection UNIQUEMENT sur la période de test : l'entraînement reste sur un flux propre
    injected = inject_subtle(clean, np.random.default_rng(seed + 1))
    injected.loc[train, "y_sub"] = 0
    injected.loc[train, "value_c"] = clean.loc[train, "value_c"]
    injected = add_features(injected)
    score = fit_score(injected, train, contamination, seed)
    y = injected.loc[test, "y_sub"].to_numpy()
    return {"Seuils fixes": metrics(y, threshold_flags(injected.assign(value=injected["value_c"]))[test].astype(int)),
            "Isolation Forest": metrics(y, (score[test] > 0).astype(int), score[test])}, int(y.sum()), int(test.sum())


def table(res: dict) -> str:
    lines = ["| Méthode | Précision | Rappel | F1 | ROC-AUC | VP | FP | FN |", "|---|---|---|---|---|---|---|---|"]
    for name, m in res.items():
        auc = "n/a" if np.isnan(m["auc"]) else f"{m['auc']:.3f}"
        lines.append(f"| {name} | {m['precision']:.3f} | {m['recall']:.3f} | {m['f1']:.3f} | {auc} | {m['tp']} | {m['fp']} | {m['fn']} |")
    return "\n".join(lines)


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--source", choices=["minio", "generator"], default="minio")
    ap.add_argument("--contamination", type=float, default=0.01)
    ap.add_argument("--seed", type=int, default=42)
    ap.add_argument("--out", default="docs/iot09_isolation_forest.md")
    args = ap.parse_args()

    raw, truth = load_minio() if args.source == "minio" else load_generator(seed=args.seed)
    df = prepare(raw, truth)
    print(f"{len(df)} mesures valides (non nulles, dédoublonnées) ; anomalies réelles : {int(df['y_real'].sum())}")

    a, a_pos, a_n = experiment_a(df, args.contamination, args.seed)
    b, b_pos, b_n = experiment_b(df, args.contamination, args.seed)
    md = f"""# IOT-09 : Isolation Forest comparé aux seuils fixes

Source : {args.source} ; graine {args.seed} ; contamination {args.contamination} ; entraînement : {TRAIN_DAYS} premiers jours ; évaluation : le reste.

## A. Défauts réels du générateur (aberrants + sentinelle -999)
Évalué sur {a_n} mesures dont {a_pos} anomalies.

{table(a)}

## B. Anomalies contextuelles injectées (valeurs plausibles, anormales à l'heure considérée)
Évalué sur {b_n} mesures dont {b_pos} anomalies injectées (décalages : {SHIFTS}).

{table(b)}
"""
    print(md)
    Path(args.out).parent.mkdir(parents=True, exist_ok=True)
    Path(args.out).write_text(md, encoding="utf-8")
    print("écrit :", args.out)


if __name__ == "__main__":
    main()
