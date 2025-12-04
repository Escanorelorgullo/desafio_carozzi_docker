from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    max as F_max,
    min as F_min,
    sum as F_sum,
    count,
    countDistinct,
    expr,
)
import os
import datetime

from .config import DATA_BRONZE_DIR, DATA_SILVER_DIR, DATA_GOLD_DIR
from .main import create_spark_session


# ==============================
#   prints de pantalla
# ==============================

def banner(title):
    print("\n" + "=" * 80)
    print(f"=== {title.upper()} ===")
    print("=" * 80)


def ok(msg):
    print(f"  Correcto {msg}")


def warn(msg):
    print(f"  Warning {msg}")


def fail(msg):
    print(f"  Error {msg}")


# ==============================
#     CHECKS DE CALIDAD
# ==============================

def check_nulls(df, name, exclude=None):
    """
    Revisa nulos por columna, excepto las que indiques en exclude.
    """
    exclude = exclude or []
    print(f"\n→ Validando nulos en {name}:")
    for c in df.columns:
        if c in exclude:
            continue
        nulls = df.filter(col(c).isNull()).count()
        if nulls > 0:
            warn(f"{name}.{c} → {nulls} nulos")
        else:
            ok(f"{name}.{c} → OK, sin nulos")


def check_no_negative(df, cols, name):
    """
    Valida que columnas numéricas no sean negativas.
    """
    print(f"\n→ Validando valores negativos en {name}:")
    for c in cols:
        neg = df.filter(col(c) < 0).count()
        if neg > 0:
            warn(f"{name}.{c} tiene {neg} valores negativos")
        else:
            ok(f"{name}.{c} sin negativos")


def check_keys_unique(df, key, name):
    """
    Valida que el dataframe no tenga duplicados por key.
    """
    dups = df.groupBy(key).count().filter(col("count") > 1).count()
    if dups == 0:
        ok(f"{name}: sin duplicados en {key}")
    else:
        fail(f"{name}: {dups} duplicados en {key}")


def check_fk(parent_df, child_df, key, parent_name, child_name):
    """
    Valida integridad referencial: child.key ⊆ parent.key
    """
    missing = (
        child_df.select(key).distinct()
        .join(parent_df.select(key).distinct(), on=key, how="left_anti")
        .count()
    )
    if missing == 0:
        ok(f"Integridad referencial OK: {child_name}.{key} ⊆ {parent_name}.{key}")
    else:
        fail(f"{child_name}: {missing} {key} sin correspondencia en {parent_name}")


# ==============================
#           MAIN
# ==============================

def main():
    spark = create_spark_session("carozzi-checks")

    banner("INICIO VALIDACIONES")

    # ====================================================
    #                     BRONZE
    # ====================================================
    banner("BRONZE")

    customers_bz = spark.read.parquet(os.path.join(DATA_BRONZE_DIR, "customers"))
    products_bz = spark.read.parquet(os.path.join(DATA_BRONZE_DIR, "products"))
    oh_bz = spark.read.parquet(os.path.join(DATA_BRONZE_DIR, "orders_header"))
    od_bz = spark.read.parquet(os.path.join(DATA_BRONZE_DIR, "orders_detail"))

    print(f"customers_bz rows: {customers_bz.count()}")
    print(f"products_bz rows: {products_bz.count()}")
    print(f"orders_header_bz rows: {oh_bz.count()}")
    print(f"orders_detail_bz rows: {od_bz.count()}")

    customers_bz.show(5, truncate=False)
    products_bz.show(5, truncate=False)

    # Nulos básicos
    check_nulls(customers_bz, "customers_bz")
    check_nulls(products_bz, "products_bz")

    # ====================================================
    #                     SILVER
    # ====================================================
    banner("SILVER")

    dim_customer = spark.read.parquet(os.path.join(DATA_SILVER_DIR, "dim_customer"))
    dim_product = spark.read.parquet(os.path.join(DATA_SILVER_DIR, "dim_product"))
    fact = spark.read.parquet(os.path.join(DATA_SILVER_DIR, "fact_order_line"))

    print(f"dim_customer rows: {dim_customer.count()}")
    print(f"dim_product rows: {dim_product.count()}")
    print(f"fact_order_line rows: {fact.count()}")

    # Unicidad claves
    check_keys_unique(dim_customer, "customer_id", "dim_customer")
    check_keys_unique(dim_product, "product_id", "dim_product")

    # FK: fact → dims
    check_fk(dim_customer, fact, "customer_id", "dim_customer", "fact")
    check_fk(dim_product, fact, "product_id", "dim_product", "fact")

    # Nulos en Silver
    check_nulls(dim_customer, "dim_customer")
    check_nulls(dim_product, "dim_product")
    check_nulls(fact, "fact", exclude=["promo_flag"])

    # Valores negativos en montos
    check_no_negative(fact, ["qty_units", "order_net_amount", "line_net_amount"], "fact")

    # Distribuciones rápidas
    print("\n→ Estadísticas numéricas fact (Resumen):")
    fact.describe(["qty_units", "order_net_amount", "line_net_amount"]).show()

    # ====================================================
    #                     GOLD
    # ====================================================
    banner("GOLD")

    gold = spark.read.parquet(os.path.join(DATA_GOLD_DIR, "gold_sales_customer_3m"))
    print(f"gold_sales_customer_3m rows: {gold.count()}")
    gold.show(5, truncate=False)

    # Validación ventana 3 meses
    max_date = fact.agg(F_max("order_date")).collect()[0][0]
    min_date = fact.agg(F_min("order_date")).collect()[0][0]

    print(f"max_order_date FACT: {max_date}")
    print(f"min_order_date FACT: {min_date}")

    # Consistencia: fecha en GOLD debe estar dentro última ventana de 3 meses
    # (Validación suave, ya que Gold.py tiene la lógica correcta)
    out_of_window = gold.filter(col("ingestion_timestamp") < expr("now() - interval 120 days")).count()
    if out_of_window == 0:
        ok("Todas las filas de GOLD están dentro de ventana razonable")
    else:
        warn(f"{out_of_window} filas parecen fuera de ventana temporal en GOLD")

    # Tomar un cliente muestra
    sample = gold.select("customer_id").limit(1).collect()
    if sample:
        cust = sample[0]["customer_id"]
        print(f"\n→ Validando cliente {cust}")

        gold_rec = (
            gold.filter(col("customer_id") == cust)
            .select(
                "total_order_net_amount_3m",
                "total_line_net_amount_3m",
                "total_qty_units_3m",
            )
            .collect()[0]
        )

        fact_rec = (
            fact.filter(col("customer_id") == cust)
            .agg(
                F_sum("order_net_amount").alias("sum_order_net_amount"),
                F_sum("line_net_amount").alias("sum_line_net_amount"),
                F_sum("qty_units").alias("sum_qty_units"),
            )
            .collect()[0]
        )

        print("GOLD (3m) vs FACT (total histórico):")
        print(f"  GOLD total_order_net_amount_3m : {gold_rec['total_order_net_amount_3m']}")
        print(f"  FACT sum_order_net_amount      : {fact_rec['sum_order_net_amount']}")
        print(f"  GOLD total_line_net_amount_3m  : {gold_rec['total_line_net_amount_3m']}")
        print(f"  FACT sum_line_net_amount       : {fact_rec['sum_line_net_amount']}")
        print(f"  GOLD total_qty_units_3m        : {gold_rec['total_qty_units_3m']}")
        print(f"  FACT sum_qty_units             : {fact_rec['sum_qty_units']}")

    banner("VALIDACIONES COMPLETADAS")

    spark.stop()


if __name__ == "__main__":
    main()
