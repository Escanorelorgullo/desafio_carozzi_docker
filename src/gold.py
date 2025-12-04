from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col,
    sum as F_sum,
    max as F_max,
    lit,
)
import os
from datetime import timedelta

from .config import DATA_SILVER_DIR, DATA_GOLD_DIR


def read_parquet(spark: SparkSession, folder: str) -> DataFrame:
    path = os.path.join(DATA_SILVER_DIR, folder)
    print(f"Leyendo Silver {folder} desde: {path}")
    return spark.read.parquet(path)


def write_parquet(df: DataFrame, folder: str) -> None:
    path = os.path.join(DATA_GOLD_DIR, folder)
    print(f"Escribiendo Gold {folder} en: {path}")
    (
        df
        .coalesce(1)  # GOLD suele ser dimensiones/agregados pequeños
        .write
        .mode("overwrite")
        .parquet(path)
    )


def build_fact_last_3m(fact: DataFrame) -> DataFrame:
    """
    Filtra fact_order_line para quedarse solo con los últimos 3 meses
    en base a la última fecha disponible en order_date.
    """
    max_order_date = fact.agg(F_max("order_date")).collect()[0][0]
    print(f"Última fecha disponible en fact_order_line: {max_order_date}")

    if max_order_date is None:
        print("⚠️ fact_order_line está vacío; devolviendo DF vacío.")
        return fact.limit(0)

    # Restamos ~3 meses (90 días) a la última fecha
    three_months_ago = max_order_date - timedelta(days=90)
    print(f"Filtrando registros desde: {three_months_ago}")

    # Comparación directa con un literal de tipo datetime
    fact_3m = fact.filter(col("order_date") >= lit(three_months_ago))

    return fact_3m


def build_gold_sales_by_customer(fact_3m: DataFrame, dim_customer: DataFrame) -> DataFrame:
    """
    Agrega las ventas de los últimos 3 meses por cliente y enriquece
    con los datos de la dimensión de clientes.
    """
    agg = (
        fact_3m
        .groupBy("customer_id")
        .agg(
            F_sum("order_net_amount").alias("total_order_net_amount_3m"),
            F_sum("line_net_amount").alias("total_line_net_amount_3m"),
            F_sum("qty_units").alias("total_qty_units_3m"),
        )
    )

    # Enriquecemos con los datos del cliente
    result = agg.join(dim_customer, on="customer_id", how="left")

    return result


def build_dim_features(gold_sales_3m: DataFrame) -> DataFrame:
    """
    Construye una tabla de features por cliente a partir de la facturación
    de los últimos 3 meses.

    Esta será la tabla final esperada por el desafío:
    data/gold/dim_features
    """
    dim = (
        gold_sales_3m.select(
            col("customer_id"),
            col("total_order_net_amount_3m").alias("monto_total_3m"),
            col("total_line_net_amount_3m").alias("monto_linea_total_3m"),
            col("total_qty_units_3m").alias("unidades_totales_3m"),
        )
    )

    return dim


def write_dim_features(df: DataFrame, base_path: str) -> None:
    output_path = os.path.join(base_path, "dim_features")
    print(f"Escribiendo tabla GOLD dim_features en: {output_path}")
    (
        df
        .coalesce(1)  # misma idea: dejar un solo archivo parquet de salida
        .write
        .mode("overwrite")
        .parquet(output_path)
    )


def run_gold(spark: SparkSession) -> None:
    print("Ejecutando capa GOLD...")

    # 1) Leer insumos desde SILVER
    fact = read_parquet(spark, "fact_order_line")
    dim_customer = read_parquet(spark, "dim_customer")

    # 2) Filtrar últimos 3 meses
    fact_3m = build_fact_last_3m(fact)

    # 3) Agregado por cliente (tabla gold_sales_customer_3m)
    gold_sales_customer_3m = build_gold_sales_by_customer(fact_3m, dim_customer)

    # 4) Escribir resultado GOLD principal
    write_parquet(gold_sales_customer_3m, "gold_sales_customer_3m")

    # 5) Construir tabla final dim_features a partir de la tabla GOLD ya generada
    dim_features = build_dim_features(gold_sales_customer_3m)
    write_dim_features(dim_features, DATA_GOLD_DIR)

    print("Capa GOLD finalizada.")


