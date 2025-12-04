# src/check_data.py

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    max as F_max,
    min as F_min,
    sum as F_sum,
    count,
    countDistinct,
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
    print(f"  Warning  {msg}")


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
            ok(f"{name}.{c} → sin nulos")


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


def check_between_0_1(df, cols, name):
    """
    Valida que columnas estén en el rango [0, 1].
    """
    print(f"\n→ Validando que columnas estén en [0, 1] en {name}:")
    for c in cols:
        below0 = df.filter(col(c) < 0).count()
        above1 = df.filter(col(c) > 1).count()
        if below0 == 0 and above1 == 0:
            ok(f"{name}.{c} en rango [0,1]")
        else:
            warn(f"{name}.{c} fuera de rango: {below0} <0, {above1} >1")


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
    products_bz  = spark.read.parquet(os.path.join(DATA_BRONZE_DIR, "products"))
    oh_bz        = spark.read.parquet(os.path.join(DATA_BRONZE_DIR, "orders_header"))
    od_bz        = spark.read.parquet(os.path.join(DATA_BRONZE_DIR, "orders_detail"))

    print(f"customers_bz rows: {customers_bz.count()}")
    print(f"products_bz rows: {products_bz.count()}")
    print(f"orders_header_bz rows: {oh_bz.count()}")
    print(f"orders_detail_bz rows: {od_bz.count()}")

    customers_bz.show(5, truncate=False)
    products_bz.show(5, truncate=False)

    # Nulos básicos
    check_nulls(customers_bz, "customers_bz")
    check_nulls(products_bz,  "products_bz")

    # ====================================================
    #                     SILVER
    # ====================================================
    banner("SILVER")

    dim_customer = spark.read.parquet(os.path.join(DATA_SILVER_DIR, "dim_customer"))
    dim_product  = spark.read.parquet(os.path.join(DATA_SILVER_DIR, "dim_product"))
    fact         = spark.read.parquet(os.path.join(DATA_SILVER_DIR, "fact_order_line"))

    print(f"dim_customer rows: {dim_customer.count()}")
    print(f"dim_product rows: {dim_product.count()}")
    print(f"fact_order_line rows: {fact.count()}")

    # Unicidad claves
    check_keys_unique(dim_customer, "customer_id", "dim_customer")
    check_keys_unique(dim_product,  "product_id",  "dim_product")

    # FK: fact → dims
    check_fk(dim_customer, fact, "customer_id", "dim_customer", "fact")
    check_fk(dim_product,  fact, "product_id",  "dim_product",  "fact")

    # Nulos en Silver
    check_nulls(dim_customer, "dim_customer")
    check_nulls(dim_product,  "dim_product")
    check_nulls(fact,         "fact", exclude=["promo_flag"])

    # Valores negativos en montos
    check_no_negative(
        fact,
        ["qty_units", "order_net_amount", "line_net_amount"],
        "fact"
    )

    print("\n→ Estadísticas numéricas fact (Resumen):")
    fact.describe(["qty_units", "order_net_amount", "line_net_amount"]).show()

    # ====================================================
    #                     GOLD
    # ====================================================
    banner("GOLD")

    gold_sales = spark.read.parquet(
        os.path.join(DATA_GOLD_DIR, "gold_sales_customer_3m")
    )
    dim_features = spark.read.parquet(
        os.path.join(DATA_GOLD_DIR, "dim_features")
    )

    print(f"gold_sales_customer_3m rows: {gold_sales.count()}")
    gold_sales.show(5, truncate=False)

    print(f"dim_features rows: {dim_features.count()}")
    dim_features.show(5, truncate=False)

    # --- Validar columnas esperadas en dim_features ---
    expected_cols = {
        "customer_id",
        "ventas_total_3m",
        "recencia_dias",
        "frecuencia_pedidos_3m",
        "ticket_promedio_3m",
        "dias_promedio_entre_pedidos",
        "variedad_categorias_3m",
        "porcentaje_pedidos_promo_3m",
        "mix_credito_vs_contado_3m",
    }
    actual_cols = set(dim_features.columns)

    missing_cols = expected_cols - actual_cols
    extra_cols   = actual_cols - expected_cols

    if not missing_cols and not extra_cols:
        ok("dim_features tiene exactamente las columnas esperadas")
    else:
        if missing_cols:
            fail(f"dim_features: faltan columnas {missing_cols}")
        if extra_cols:
            warn(f"dim_features: columnas extra {extra_cols}")

    # --- Unicidad por cliente en dim_features ---
    check_keys_unique(dim_features, "customer_id", "dim_features")

    # --- Nulos y rangos en features ---
    # Algunas pueden ser nulas si hay pocos pedidos (ticket_promedio, dias_promedio)
    check_nulls(
        dim_features,
        "dim_features",
        exclude=["ticket_promedio_3m", "dias_promedio_entre_pedidos"]
    )

    # No negativos
    check_no_negative(
        dim_features,
        [
            "ventas_total_3m",
            "recencia_dias",
            "frecuencia_pedidos_3m",
            "ticket_promedio_3m",
            "dias_promedio_entre_pedidos",
            "variedad_categorias_3m",
        ],
        "dim_features",
    )

    # Porcentajes en [0,1]
    check_between_0_1(
        dim_features,
        ["porcentaje_pedidos_promo_3m", "mix_credito_vs_contado_3m"],
        "dim_features",
    )

    # --- Regla de negocio: solo clientes activos, canal tradicional ---
    active_trad = (
        dim_customer
        .filter((col("canal") == "tradicional") & (col("estado") == "activo"))
        .select("customer_id")
        .distinct()
    )

    df_customers = dim_features.select("customer_id").distinct()

    missing_in_active = (
        df_customers.join(active_trad, on="customer_id", how="left_anti").count()
    )

    if missing_in_active == 0:
        ok("Todos los customers de dim_features son clientes activos de canal tradicional")
    else:
        warn(
            f"{missing_in_active} customer_id en dim_features no cumplen "
            "con (canal='tradicional' y estado='activo')"
        )

    # --- Consistencia básica con gold_sales_customer_3m ---
    gold_customers = gold_sales.select("customer_id").distinct()
    missing_in_gold = (
        df_customers.join(gold_customers, on="customer_id", how="left_anti").count()
    )

    if missing_in_gold == 0:
        ok("Todos los customer_id de dim_features existen en gold_sales_customer_3m")
    else:
        warn(
            f"{missing_in_gold} customer_id en dim_features no tienen registro en gold_sales_customer_3m"
        )

    # Cliente de ejemplo para comparar ventas_total_3m vs gold_sales
    sample = dim_features.select("customer_id").limit(1).collect()
    if sample:
        cust = sample[0]["customer_id"]
        print(f"\n→ Validando cliente de ejemplo en GOLD y dim_features: {cust}")

        gold_row = (
            gold_sales
            .filter(col("customer_id") == cust)
            .select(
                "total_order_net_amount_3m",
                "total_line_net_amount_3m",
                "total_qty_units_3m",
            )
            .collect()[0]
        )

        feat_row = (
            dim_features
            .filter(col("customer_id") == cust)
            .select(
                "ventas_total_3m",
                "frecuencia_pedidos_3m",
                "variedad_categorias_3m",
            )
            .collect()[0]
        )

        print("gold_sales_customer_3m vs dim_features:")
        print(f"  GOLD  total_order_net_amount_3m : {gold_row['total_order_net_amount_3m']}")
        print(f"  FEAT  ventas_total_3m           : {feat_row['ventas_total_3m']}")
        print(f"  GOLD  total_qty_units_3m        : {gold_row['total_qty_units_3m']}")
        print(f"  FEAT  frecuencia_pedidos_3m     : {feat_row['frecuencia_pedidos_3m']}")
        print(f"  FEAT  variedad_categorias_3m    : {feat_row['variedad_categorias_3m']}")

    banner("VALIDACIONES COMPLETADAS")

    spark.stop()


if __name__ == "__main__":
    main()
