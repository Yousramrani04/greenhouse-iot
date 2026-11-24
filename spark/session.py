"""Session Spark configurée pour Iceberg (catalogue JDBC/Postgres) et Silo (S3).

Le catalogue s'appelle "lake" : toute table s'écrit lake.<namespace>.<table>.
Deux chemins vers Silo :
  - S3FileIO (Iceberg)  -> tables Iceberg
  - S3A (Hadoop)        -> fichiers bruts du Bronze (s3a://bronze/...)
"""
import os

from pyspark.sql import SparkSession

CATALOG = "lake"


def get_spark(app_name: str) -> SparkSession:
    c = f"spark.sql.catalog.{CATALOG}"
    endpoint = os.environ["S3_ENDPOINT_INTERNAL"]
    spark = (
        SparkSession.builder.appName(app_name)
        # SQL Iceberg : MERGE INTO, CALL procédures, ALTER ... PARTITION FIELD
        .config("spark.sql.extensions",
                "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .config(c, "org.apache.iceberg.spark.SparkCatalog")
        # Catalogue : un simple jeu de tables SQL dans Postgres
        .config(f"{c}.catalog-impl", "org.apache.iceberg.jdbc.JdbcCatalog")
        .config(f"{c}.uri",
                f"jdbc:postgresql://{os.environ['POSTGRES_HOST']}:5432/{os.environ['ICEBERG_CATALOG_DB']}")
        .config(f"{c}.jdbc.user", os.environ["POSTGRES_USER"])
        .config(f"{c}.jdbc.password", os.environ["POSTGRES_PASSWORD"])
        # Tables Iceberg : fichiers dans Silo via S3FileIO
        .config(f"{c}.warehouse", "s3://silver/warehouse")
        .config(f"{c}.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
        .config(f"{c}.s3.endpoint", endpoint)
        .config(f"{c}.s3.path-style-access", "true")
        .config(f"{c}.s3.access-key-id", os.environ["MINIO_ROOT_USER"])
        .config(f"{c}.s3.secret-access-key", os.environ["MINIO_ROOT_PASSWORD"])
        .config(f"{c}.client.region", "us-east-1")
        # Fichiers bruts (Bronze) : Hadoop S3A
        .config("spark.hadoop.fs.s3a.endpoint", endpoint)
        .config("spark.hadoop.fs.s3a.endpoint.region", "us-east-1")
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .config("spark.hadoop.fs.s3a.access.key", os.environ["MINIO_ROOT_USER"])
        .config("spark.hadoop.fs.s3a.secret.key", os.environ["MINIO_ROOT_PASSWORD"])
        # Event time : tout en UTC, jamais dans le fuseau de la machine
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark
