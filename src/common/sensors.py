"""Modèle commun des capteurs de serres.

Utilisé par ingest_capteurs.py (historique) ET producer_capteurs.py (live),
pour que les deux circuits produisent exactement la même physique.

Convention : tous les horodatages sont en UTC.
"""
from __future__ import annotations

from dataclasses import dataclass

import numpy as np

N_GREENHOUSES = 15
SAMPLE_SECONDS = 30          # une mesure brute toutes les 30 s par capteur
HISTORY_DAYS = 7

# code court -> (nom de la mesure, unité canonique)
METRICS = {
    "TEMP": ("temperature", "C"),
    "HAIR": ("air_humidity", "%"),
    "SOIL": ("soil_humidity", "%"),
    "CO2": ("co2", "ppm"),
}

# Serres dont le capteur de température émet en Fahrenheit (défaut "unités")
FAHRENHEIT_GREENHOUSES = {4, 9, 13}


@dataclass(frozen=True)
class Sensor:
    sensor_id: str        # ex. "G07-TEMP" : sert de clé Kafka
    greenhouse_id: str    # ex. "G07"
    metric: str           # temperature | air_humidity | soil_humidity | co2
    unit: str             # unité EMISE par le capteur (C, F, %, ppm)
    gh_offset: float      # biais thermique propre à la serre (°C)


def build_registry(seed: int = 42) -> list[Sensor]:
    """Construit les 60 capteurs. Déterministe grâce à la graine."""
    rng = np.random.default_rng(seed)
    sensors: list[Sensor] = []
    for g in range(1, N_GREENHOUSES + 1):
        gh = f"G{g:02d}"
        gh_offset = float(rng.uniform(-1.5, 1.5))  # une serre un peu plus chaude/froide
        for code, (metric, unit) in METRICS.items():
            if code == "TEMP" and g in FAHRENHEIT_GREENHOUSES:
                unit = "F"
            sensors.append(Sensor(f"{gh}-{code}", gh, metric, unit, gh_offset))
    return sensors


def _hour_of_day(ts: np.ndarray) -> np.ndarray:
    """Heure décimale UTC (0.0 à 24.0) pour un tableau datetime64."""
    day_start = ts.astype("datetime64[D]").astype(ts.dtype)
    return (ts - day_start) / np.timedelta64(1, "h")


def _temp_base(h: np.ndarray, gh_offset: float) -> np.ndarray:
    # sin atteint son max quand (h-9)/24 = 0.25, soit h = 15 ; min à h = 3
    return 24.0 + gh_offset + 6.0 * np.sin(2 * np.pi * (h - 9) / 24)


def clean_value(sensor: Sensor, ts: np.ndarray, rng: np.random.Generator) -> np.ndarray:
    """Valeur 'saine' (sans défaut) dans l'unité CANONIQUE (°C, %, ppm)."""
    h = _hour_of_day(ts)
    n = len(ts)
    if sensor.metric == "temperature":
        return _temp_base(h, sensor.gh_offset) + rng.normal(0, 0.3, n)
    if sensor.metric == "air_humidity":
        v = 65 - 1.8 * (_temp_base(h, sensor.gh_offset) - 24) + rng.normal(0, 1.5, n)
        return np.clip(v, 20, 100)
    if sensor.metric == "soil_humidity":
        since_irrigation = (h - 6) % 12          # arrosage à 06h et 18h
        return 70 - 1.6 * since_irrigation + rng.normal(0, 0.4, n)
    if sensor.metric == "co2":
        photosynthesis = np.maximum(0, np.sin(2 * np.pi * (h - 6) / 24))
        return 650 - 250 * photosynthesis + rng.normal(0, 15, n)
    raise ValueError(f"mesure inconnue : {sensor.metric}")


def to_emitted_unit(sensor: Sensor, canonical: np.ndarray) -> np.ndarray:
    """Convertit en unité émise par le capteur (°C -> °F si besoin)."""
    return canonical * 9 / 5 + 32 if sensor.unit == "F" else canonical


if __name__ == "__main__":
    # Auto-vérification : python -m src.common.sensors
    reg = build_registry()
    print(len(reg), "capteurs")
    print("capteurs en F :", [s.sensor_id for s in reg if s.unit == "F"])
    rng = np.random.default_rng(0)
    day = np.arange(
        np.datetime64("2026-11-16T00:00:00"),
        np.datetime64("2026-11-17T00:00:00"),
        np.timedelta64(SAMPLE_SECONDS, "s"),
    )
    for s in reg[:4]:
        v = clean_value(s, day, rng)
        print(f"{s.sensor_id:10s} {s.metric:14s} min={v.min():7.1f} max={v.max():7.1f} n={len(v)}")
