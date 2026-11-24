"""Classement d'une mesure live : même règles que le job Silver (spark/silver_job.py)."""
import math

SENTINEL = -999.0
PLAUSIBLE = {
    "temperature": (-10.0, 60.0),
    "air_humidity": (0.0, 100.0),
    "soil_humidity": (0.0, 100.0),
    "co2": (0.0, 3000.0),
}


def classify(metric: str, value, unit: str):
    """Renvoie (valeur en unité canonique ou None, statut).

    Ordre : null -> sentinelle -> conversion °F -> plage.
    (La sentinelle est testée AVANT la conversion : sinon -999 °F deviendrait -573 °C.)
    """
    if value is None or (isinstance(value, float) and math.isnan(value)):
        return None, "null"
    if value == SENTINEL:
        return None, "sentinel"
    v = (value - 32) * 5 / 9 if unit == "F" else value
    lo, hi = PLAUSIBLE[metric]
    if not (lo <= v <= hi):
        return None, "out_of_range"
    return v, "ok"
