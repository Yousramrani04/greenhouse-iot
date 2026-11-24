"""Orchestration Dagster du chemin par lots : Silver -> export -> dbt -> Soda.

Planification horaire (UTC). Chaque étape est idempotente (Silver incrémental avec MERGE,
dbt delete+insert), donc une relance ne duplique rien.
Échec géré : Soda Silver code 2 ou 3 => la chaîne s'arrête ; code 1 => avertissement, on continue.
Soda live (fraîcheur) est non bloquant : un producteur arrêté ne doit pas casser la chaîne.
"""
from dagster import (Config, DefaultScheduleStatus, Definitions, Failure, In, Nothing,
                     OpExecutionContext, RetryPolicy, ScheduleDefinition, job, op)

from . import steps

SPARK_RETRY = RetryPolicy(max_retries=1, delay=20)   # une relance en cas d'incident transitoire (Docker, S3)


def _spark(context: OpExecutionContext, script: str) -> None:
    code, tail = steps.run(script, steps.spark_cmd(script))
    context.log.info("\n".join(tail))
    if code != 0:
        raise Failure(description=f"{script} a échoué (code {code}) ; voir logs/{script}.log",
                      metadata={"dernieres_lignes": "\n".join(tail)})


@op(retry_policy=SPARK_RETRY)
def silver(context: OpExecutionContext) -> None:
    """Bronze (historique + live) -> Iceberg lake.silver.* (MERGE incrémental)."""
    _spark(context, "silver_job.py")


@op(ins={"start": In(Nothing)}, retry_policy=SPARK_RETRY)
def export_silver(context: OpExecutionContext) -> None:
    """Silver Iceberg -> Parquet lisible par DuckDB."""
    _spark(context, "export_silver.py")


@op(ins={"start": In(Nothing)})
def dbt_build(context: OpExecutionContext) -> None:
    """Gold : modèles dbt + tests de données."""
    code, tail = steps.run("dbt_build", steps.dbt_cmd(), cwd=steps.ROOT / "dbt")
    context.log.info("\n".join(tail))
    if code != 0:
        raise Failure(description=f"dbt build a échoué (code {code}) ; voir logs/dbt_build.log",
                      metadata={"dernieres_lignes": "\n".join(tail)})


class SodaConfig(Config):
    # Démonstration d'échec : mettre 2 dans le Launchpad pour simuler un contrôle en échec (-1 = désactivé)
    simulate_exit_code: int = -1


@op(ins={"start": In(Nothing)})
def soda_silver(context: OpExecutionContext, config: SodaConfig) -> None:
    """Contrôles Silver / Gold (IOT-05). Bloquant à partir du code 2."""
    if config.simulate_exit_code >= 0:
        code, tail = config.simulate_exit_code, ["(échec simulé pour la démonstration)"]
    else:
        code, tail = steps.run("soda_silver", steps.soda_cmd("silver", "soda/checks_silver.yml"))
    verdict = steps.soda_verdict(code)
    context.log.info("\n".join(tail))
    if verdict == "warn":
        context.log.warning(f"Soda Silver : avertissement (code {code}) ; la chaîne continue")
    elif verdict == "fail":
        raise Failure(description=f"Soda Silver bloquant (code {code}) : chaîne arrêtée",
                      metadata={"code_soda": code, "dernieres_lignes": "\n".join(tail)})


@op(ins={"start": In(Nothing)})
def soda_live(context: OpExecutionContext) -> None:
    """Fraîcheur du chemin temps réel (< 10 min). Non bloquant : simple avertissement."""
    code, tail = steps.run("soda_live", steps.soda_cmd("live", "soda/checks_live.yml"))
    context.log.info("\n".join(tail))
    if steps.soda_verdict(code) != "ok":
        context.log.warning(f"Soda live : code {code} (producteur arrêté ou capteurs silencieux ?) ; non bloquant")


@job
def chaine_donnees():
    soda_live(start=soda_silver(start=dbt_build(start=export_silver(start=silver()))))


planification_horaire = ScheduleDefinition(
    name="chaine_horaire",
    job=chaine_donnees,
    cron_schedule="0 * * * *",            # toutes les heures pile, en UTC
    execution_timezone="UTC",
    default_status=DefaultScheduleStatus.RUNNING,
)

defs = Definitions(jobs=[chaine_donnees], schedules=[planification_horaire])
