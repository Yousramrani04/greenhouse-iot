"""Paramètres et fonctions de défauts, partagés par l'historique et le live."""
import numpy as np

RATES = {"missing": 0.02, "outlier": 0.005, "duplicate": 0.01, "late": 0.03}

LATE_SECONDS = (60, 1800)             # message "en retard" : 1 à 30 min
NORMAL_LATENCY_SECONDS = (0.2, 3.0)   # latence réseau normale
SENTINEL = -999.0                     # valeur émise par un capteur en panne
OUTAGE_HOURS = (1, 3)
N_SENTINEL_OUTAGES = 4                # capteurs bloqués sur -999
N_SILENT_OUTAGES = 4                  # capteurs qui se taisent


def outlier_value(metric: str, n: int, rng: np.random.Generator) -> np.ndarray:
    """Valeurs physiquement impossibles, en unité canonique (°C, %, ppm)."""
    if metric == "temperature":
        return rng.uniform(80, 150, n)
    if metric in ("air_humidity", "soil_humidity"):
        return rng.uniform(130, 300, n)       # une humidité ne dépasse pas 100 %
    if metric == "co2":
        return rng.uniform(5000, 20000, n)
    raise ValueError(f"mesure inconnue : {metric}")
