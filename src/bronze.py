from pyspark.sql import SparkSession
from pyspark.sql.functions import input_file_name, lit, current_timestamp
from pyspark.sql import DataFrame
import os

from .config import DATA_RAW_DIR, DATA_BRONZE_DIR


def _read_csv_pattern(spark: SparkSession, pattern: str) -> DataFrame:
    path = os.path.join(DATA_RAW_DIR, pattern)
    print(f"Leyendo archivos desde: {path}")
    df = (
        spark.read
        .option("header", "true")
        .option("inferSchema", "true")
        .csv(path)
        .withColumn("ingestion_timestamp", current_timestamp())
        .withColumn("source_file", input_file_name())
    )
    return df


def ingest_customers(spark: SparkSession) -> None:
    df = _read_csv_pattern(spark, "customers*.csv")
    target_path = os.path.join(DATA_BRONZE_DIR, "customers")
    print(f"Escribiendo Bronze customers en: {target_path}")
    df.write.mode("overwrite").parquet(target_path)


def ingest_products(spark: SparkSession) -> None:
    df = _read_csv_pattern(spark, "products*.csv")
    target_path = os.path.join(DATA_BRONZE_DIR, "products")
    print(f"Escribiendo Bronze products en: {target_path}")
    df.write.mode("overwrite").parquet(target_path)


def ingest_orders_header(spark: SparkSession) -> None:
    df = _read_csv_pattern(spark, "orders_header*.csv")
    target_path = os.path.join(DATA_BRONZE_DIR, "orders_header")
    print(f"Escribiendo Bronze orders_header en: {target_path}")
    df.write.mode("overwrite").parquet(target_path)


def ingest_orders_detail(spark: SparkSession) -> None:
    df = _read_csv_pattern(spark, "orders_detail*.csv")
    target_path = os.path.join(DATA_BRONZE_DIR, "orders_detail")
    print(f"Escribiendo Bronze orders_detail en: {target_path}")
    df.write.mode("overwrite").parquet(target_path)


def run_bronze(spark: SparkSession) -> None:
    print("Ejecutando capa BRONZE...")
    ingest_customers(spark)
    ingest_products(spark)
    ingest_orders_header(spark)
    ingest_orders_detail(spark)
    print("Capa BRONZE finalizada.")
