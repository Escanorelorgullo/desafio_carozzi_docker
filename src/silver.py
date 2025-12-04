from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col,
    year,
    month,
    dayofmonth,
    dayofweek,
    date_format,
)
import os

from .config import DATA_BRONZE_DIR, DATA_SILVER_DIR


def read_parquet_bronze(spark: SparkSession, folder: str) -> DataFrame:
    """
    Lee un dataset desde la capa Bronze.
    """
    path = os.path.join(DATA_BRONZE_DIR, folder)
    print(f"Leyendo Bronze {folder} desde: {path}")
    return spark.read.parquet(path)


def write_parquet_silver(df: DataFrame, folder: str) -> None:
    """
    Escribe un dataset en la capa Silver.
    """
    path = os.path.join(DATA_SILVER_DIR, folder)
    print(f"Escribiendo Silver {folder} en: {path}")
    df.write.mode("overwrite").parquet(path)


# ==========================
#  CONSTRUCCIÓN DE DIMENSIONES
# ==========================

def build_dim_customer(customers: DataFrame) -> DataFrame:
    """
    Dimensión de clientes.
    - Elimina duplicados por customer_id.
    """
    return customers.dropDuplicates(["customer_id"])


def build_dim_product(products: DataFrame) -> DataFrame:
    """
    Dimensión de productos.
    - Elimina duplicados por product_id.
    """
    return products.dropDuplicates(["product_id"])


def build_dim_date(fact: DataFrame) -> DataFrame:
    """
    Dimensión de fechas (dim_date), a partir de las fechas de orden.
    - Se generan columnas típicas de calendario.
    """
    dim_date = (
        fact
        .select(col("order_date").cast("date").alias("date"))
        .distinct()
        .withColumn("year", year(col("date")))
        .withColumn("month", month(col("date")))
        .withColumn("day", dayofmonth(col("date")))
        .withColumn("day_of_week", dayofweek(col("date")))
        # Clave de fecha en formato YYYYMMDD
        .withColumn("date_key", date_format(col("date"), "yyyyMMdd"))
    )

    # Ordenamos columnas para dejar más limpio
    dim_date = dim_date.select(
        "date_key",
        "date",
        "year",
        "month",
        "day",
        "day_of_week",
    )

    return dim_date


# ==========================
#  TABLA DE HECHOS
# ==========================

def build_fact_order_line(
    orders_header: DataFrame,
    orders_detail: DataFrame,
) -> DataFrame:
    """
    Construye la tabla de hechos fact_order_line.
    Requisitos según enunciado:
    - Remover duplicados en orders_header por order_id.
    - Remover duplicados en orders_detail por (order_id, line_id).
    - Excluir pedidos con order_net_amount <= 0.
    - Excluir líneas con qty_units <= 0.
    - Left join entre orders_header y orders_detail por order_id.
    - Evitar columnas duplicadas en el resultado.
    """

    # 1) Eliminar duplicados en origen (criterios de negocio)
    oh_dedup = orders_header.dropDuplicates(["order_id"])
    od_dedup = orders_detail.dropDuplicates(["order_id", "line_id"])

    # 2) Aplicar filtros de calidad
    oh_clean = oh_dedup.filter(col("order_net_amount") > 0)
    od_clean_filtered = od_dedup.filter(col("qty_units") > 0)

    # 3) Evitar columnas duplicadas en el join
    common_cols = set(oh_clean.columns).intersection(set(od_clean_filtered.columns))

    # Dejamos order_id como llave, removemos el resto de comunes del detalle
    if "order_id" in common_cols:
        common_cols.remove("order_id")

    cols_od_keep = [c for c in od_clean_filtered.columns if c not in common_cols]
    od_clean = od_clean_filtered.select(cols_od_keep)

    # 4) Join LEFT entre header y detail
    fact = oh_clean.join(od_clean, on="order_id", how="left")

    # 5) Como capa de seguridad, eliminamos posibles duplicados en fact
    if "line_id" in fact.columns:
        fact = fact.dropDuplicates(["order_id", "line_id"])
    else:
        fact = fact.dropDuplicates(["order_id"])

    return fact


# ==========================
#  ORQUESTADOR DE LA CAPA SILVER
# ==========================

def run_silver(spark: SparkSession) -> None:
    print("Ejecutando capa SILVER...")

    # 1) Lectura desde Bronze
    customers_bz = read_parquet_bronze(spark, "customers")
    products_bz = read_parquet_bronze(spark, "products")
    orders_header_bz = read_parquet_bronze(spark, "orders_header")
    orders_detail_bz = read_parquet_bronze(spark, "orders_detail")

    # 2) Construcción de la tabla de hechos
    fact_order_line = build_fact_order_line(orders_header_bz, orders_detail_bz)

    # 3) Construcción de dimensiones
    dim_customer = build_dim_customer(customers_bz)
    dim_product = build_dim_product(products_bz)
    dim_date = build_dim_date(fact_order_line)

    # 4) Persistencia en ./data/silver
    write_parquet_silver(dim_customer, "dim_customer")
    write_parquet_silver(dim_product, "dim_product")
    write_parquet_silver(dim_date, "dim_date")
    write_parquet_silver(fact_order_line, "fact_order_line")

    print("Capa SILVER finalizada.")
