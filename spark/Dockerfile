FROM apache/spark:4.0.5-scala2.13-java17-python3-ubuntu

USER root
ARG ICEBERG_VERSION=1.10.1
ARG PG_JDBC_VERSION=42.7.4
ARG HADOOP_AWS_VERSION=3.4.1
ARG AWS_SDK_BUNDLE_VERSION=2.24.6
ARG MAVEN=https://repo1.maven.org/maven2

# 1. moteur Iceberg pour Spark 4.0 / Scala 2.13
ADD ${MAVEN}/org/apache/iceberg/iceberg-spark-runtime-4.0_2.13/${ICEBERG_VERSION}/iceberg-spark-runtime-4.0_2.13-${ICEBERG_VERSION}.jar /opt/spark/jars/
# 2. client S3 d'Iceberg (tables Iceberg <-> Silo)
ADD ${MAVEN}/org/apache/iceberg/iceberg-aws-bundle/${ICEBERG_VERSION}/iceberg-aws-bundle-${ICEBERG_VERSION}.jar /opt/spark/jars/
# 3. driver JDBC pour le catalogue stocké dans Postgres
ADD ${MAVEN}/org/postgresql/postgresql/${PG_JDBC_VERSION}/postgresql-${PG_JDBC_VERSION}.jar /opt/spark/jars/
# 4. lecture des fichiers bruts du Bronze via s3a:// (doit correspondre au Hadoop de Spark)
ADD ${MAVEN}/org/apache/hadoop/hadoop-aws/${HADOOP_AWS_VERSION}/hadoop-aws-${HADOOP_AWS_VERSION}.jar /opt/spark/jars/
ADD ${MAVEN}/software/amazon/awssdk/bundle/${AWS_SDK_BUNDLE_VERSION}/bundle-${AWS_SDK_BUNDLE_VERSION}.jar /opt/spark/jars/

RUN chmod 644 /opt/spark/jars/*.jar
USER spark
