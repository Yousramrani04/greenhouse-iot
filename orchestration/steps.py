"""Briques d'exécution de la chaîne par lots (sans dépendance à Dagster, donc testables seules).

Chaque étape lance la même commande que scripts/refresh_chain.sh ; Dagster ne fait
qu'orchestrer (planification, historique, échec géré). Journal complet : logs/<étape>.log
"""
import os
import subprocess
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
BIN = ROOT / ".venv" / "bin"          # dbt et soda restent dans le venv principal (évite les conflits de versions)
NOISE = ("INFO", "WARN", "SLF4J")


def load_env() -> dict:
    """Environnement du processus + variables de .env (dbt et Soda ne lisent pas .env tout seuls)."""
    env = os.environ.copy()
    f = ROOT / ".env"
    if f.exists():
        for line in f.read_text(encoding="utf-8").splitlines():
            line = line.strip()
            if line and not line.startswith("#") and "=" in line:
                k, v = line.split("=", 1)
                env[k.strip()] = v.strip().strip('"').strip("'")
    return env


def run(name: str, cmd: list, cwd: Path = ROOT):
    """Lance cmd, écrit le journal complet dans logs/<name>.log, renvoie (code, dernières lignes utiles)."""
    log = ROOT / "logs" / f"{name}.log"
    log.parent.mkdir(exist_ok=True)
    with open(log, "w", encoding="utf-8") as f:
        p = subprocess.run(cmd, cwd=cwd, env=load_env(), stdout=f, stderr=subprocess.STDOUT)
    lines = [l for l in log.read_text(encoding="utf-8", errors="replace").splitlines()
             if l.strip() and not any(n in l for n in NOISE)]
    return p.returncode, lines[-15:]


def spark_cmd(script: str) -> list:
    # -T : pas de TTY (Dagster n'en a pas)
    return ["docker", "compose", "run", "--rm", "-T", "spark", "/opt/spark/bin/spark-submit", f"/opt/jobs/{script}"]


def dbt_cmd() -> list:
    return [str(BIN / "dbt"), "build", "--profiles-dir", "."]


def soda_cmd(source: str, checks: str) -> list:
    return [str(BIN / "soda"), "scan", "-d", source, "-c", "soda/configuration.yml", checks]


def soda_verdict(code: int) -> str:
    """0 = ok ; 1 = avertissement (la chaîne continue) ; 2 = échec ; 3 = erreur (la chaîne s'arrête)."""
    if code == 0:
        return "ok"
    if code == 1:
        return "warn"
    return "fail"
