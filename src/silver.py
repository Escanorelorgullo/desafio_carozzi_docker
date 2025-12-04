from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql import DataFrame
import os

from .config import DATA_BRONZE_DIR, DATA_SILVER_DIR


def read_parquet(spark: SparkSession, folder: str) -> DataFrame:
    path = os.path.join(DATA_BRONZE_DIR, folder)
    print(f"Leyendo Bronze {folder} desde: {path}")
    return spark.read.parquet(path)


def write_parquet(df: DataFrame, folder: str, num_partitions: int = 4) -> None:
    """
    Escribe un DataFrame en Silver, controlando el número de particiones
    para reducir la presión de memoria y desactivando el diccionario Parquet.
    """
    path = os.path.join(DATA_SILVER_DIR, folder)
    print(f"Escribiendo Silver {folder} en: {path}")

    # Reparte a pocas particiones para que no haya demasiados writers en paralelo
    df_to_write = df.repartition(num_partitions)

    (
        df_to_write
        .write
        .mode("overwrite")
        # Esto reduce el uso de memoria en columnas numéricas grandes
        .option("parquet.enable.dictionary", "false")
        .parquet(path)
    )


def build_dim_customer(customers: DataFrame) -> DataFrame:
    # Eliminar duplicados por customer_id
    return customers.dropDuplicates(["customer_id"])


def build_dim_product(products: DataFrame) -> DataFrame:
    # Eliminar duplicados por product_id
    return products.dropDuplicates(["product_id"])


def build_fact_order_line(orders_header: DataFrame, orders_detail: DataFrame) -> DataFrame:
    """
    Construye la tabla de hechos de detalle de pedido.
    Evita columnas duplicadas en el join (ingestion_timestamp, source_file, etc.).
    """

    # 1) Aplicar filtros básicos de calidad (ejemplo)
    oh = orders_header.filter(col("order_net_amount") > 0)
    od = orders_detail.filter(col("qty_units") > 0)

    # 2) Detectar columnas en común (aparte de la llave)
    common_cols = set(oh.columns).intersection(set(od.columns))

    # Nos quedamos con 'order_id' como llave y removemos el resto de comunes del detalle
    if "order_id" in common_cols:
        common_cols.remove("order_id")

    # 3) Quitamos del detalle las columnas duplicadas
    cols_od_keep = [c for c in od.columns if c not in common_cols]
    od_clean = od.select(cols_od_keep)

    # 4) Join sin columnas duplicadas
    fact = oh.join(od_clean, on="order_id", how="left")

    return fact


def run_silver(spark: SparkSession) -> None:
    print("Ejecutando capa SILVER...")

    customers_bz = read_parquet(spark, "customers")
    products_bz = read_parquet(spark, "products")
    orders_header_bz = read_parquet(spark, "orders_header")
    orders_detail_bz = read_parquet(spark, "orders_detail")

    dim_customer = build_dim_customer(customers_bz)
    dim_product = build_dim_product(products_bz)
    fact_order_line = build_fact_order_line(orders_header_bz, orders_detail_bz)

    # Dimensiones generalmente pequeñas → puedes incluso coalesce(1) si quisieras 1 archivo
    write_parquet(dim_customer, "dim_customer", num_partitions=1)
    write_parquet(dim_product, "dim_product", num_partitions=1)

    # Tabla de hechos más grande → pocas particiones pero más de 1
    write_parquet(fact_order_line, "fact_order_line", num_partitions=4)

    print("Capa SILVER finalizada.")
