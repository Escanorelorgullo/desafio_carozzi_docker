from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col,
    sum as F_sum,
    max as F_max,
    min as F_min,
    avg as F_avg,
    first as F_first,
    countDistinct,
    when,
    datediff,
    lit,
    lag as F_lag,
)
from pyspark.sql.window import Window
import os
from datetime import timedelta

from .config import DATA_SILVER_DIR, DATA_GOLD_DIR


# -------------------------------------------------------------------
# Helpers genéricos de lectura / escritura
# -------------------------------------------------------------------
def read_parquet(spark: SparkSession, folder: str) -> DataFrame:
    path = os.path.join(DATA_SILVER_DIR, folder)
    print(f"Leyendo Silver {folder} desde: {path}")
    return spark.read.parquet(path)


def write_parquet(df: DataFrame, folder: str) -> None:
    path = os.path.join(DATA_GOLD_DIR, folder)
    print(f"Escribiendo Gold {folder} en: {path}")
    (
        df.coalesce(1)  # GOLD suele ser datasets pequeños
          .write
          .mode("overwrite")
          .parquet(path)
    )


# -------------------------------------------------------------------
# 1) Ventana de últimos 3 meses en fact_order_line
# -------------------------------------------------------------------
def build_fact_last_3m(fact: DataFrame) -> DataFrame:
    """
    Filtra fact_order_line para quedarse solo con los últimos ~3 meses
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

    fact_3m = fact.filter(col("order_date") >= lit(three_months_ago))
    return fact_3m


# -------------------------------------------------------------------
# 2) Tabla agregada GOLD auxiliar (no es el entregable final)
# -------------------------------------------------------------------
def build_gold_sales_by_customer(fact_3m: DataFrame, dim_customer: DataFrame) -> DataFrame:
    """
    Agrega ventas de los últimos 3 meses por cliente y enriquece con datos
    de cliente. Es una tabla GOLD auxiliar (no el entregable final).
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

    result = (
        agg.join(dim_customer, on="customer_id", how="left")
    )

    return result


# -------------------------------------------------------------------
# 3) dim_features: 8 features de comportamiento en 3 meses
# -------------------------------------------------------------------
def build_dim_features(fact_3m: DataFrame, dim_customer: DataFrame) -> DataFrame:
    """
    Construye la tabla dim_features siguiendo el enunciado:

    - Granularidad: cliente (customer_id)
    - Solo clientes ACTIVOS del canal TRADICIONAL
      con al menos 1 pedido en los últimos 3 meses.
    - 8 features calculadas usando exclusivamente el intervalo de 3 meses.
    """

    # Fecha de referencia = última fecha disponible en la ventana 3m
    ref_date = fact_3m.agg(F_max("order_date")).collect()[0][0]
    print(f"Fecha de referencia (última order_date en 3m): {ref_date}")

    # ---- Nivel pedido (order_id) por cliente ----
    # Consolidamos por pedido para contar pedidos, fechas, promo, medio de pago, etc.
    orders = (
        fact_3m
        .groupBy("customer_id", "order_id")
        .agg(
            F_min("order_date").alias("order_date"),
            F_sum("line_net_amount").alias("order_amount"),
            F_max("promo_flag").alias("has_promo"),
            F_first("tipo_pago").alias("tipo_pago"),
        )
    )

    # ---- 1) ventas_total_3m, 2) frecuencia_pedidos_3m, 3) recencia_dias,
    #      7) porcentaje_pedidos_promo_3m, 8) mix_credito_vs_contado_3m ----
    agg_orders = (
        orders
        .groupBy("customer_id")
        .agg(
            F_sum("order_amount").alias("ventas_total_3m"),
            countDistinct("order_id").alias("frecuencia_pedidos_3m"),
            F_max("order_date").alias("last_order_date"),
            # pedidos con al menos una línea en promo
            F_sum(when(col("has_promo") == 1, 1).otherwise(0)).alias("pedidos_con_promo"),
            # ventas en crédito
            F_sum(
                when(col("tipo_pago") == "credito", col("order_amount")).otherwise(0.0)
            ).alias("ventas_credito_3m"),
        )
    )

    agg_orders = (
        agg_orders
        .withColumn(
            "recencia_dias",
            datediff(lit(ref_date), col("last_order_date"))
        )
        .withColumn(
            "ticket_promedio_3m",
            when(col("frecuencia_pedidos_3m") > 0,
                 col("ventas_total_3m") / col("frecuencia_pedidos_3m"))
            .otherwise(lit(None))
        )
        .withColumn(
            # porcentaje de pedidos con promo (valor entre 0 y 1)
            "porcentaje_pedidos_promo_3m",
            when(col("frecuencia_pedidos_3m") > 0,
                 col("pedidos_con_promo") / col("frecuencia_pedidos_3m"))
            .otherwise(lit(0.0))
        )
        .withColumn(
            # porcentaje de ventas en crédito sobre total ventas (0 a 1)
            "mix_credito_vs_contado_3m",
            when(col("ventas_total_3m") > 0,
                 col("ventas_credito_3m") / col("ventas_total_3m"))
            .otherwise(lit(0.0))
        )
    )

    # ---- 5) dias_promedio_entre_pedidos ----
    w = Window.partitionBy("customer_id").orderBy(col("order_date"))
    orders_with_lag = orders.withColumn(
        "prev_order_date", F_lag("order_date").over(w)
    )
    orders_with_diff = orders_with_lag.withColumn(
        "diff_days",
        datediff(col("order_date"), col("prev_order_date"))
    )

    dias_promedio = (
        orders_with_diff
        .filter(col("prev_order_date").isNotNull())  # solo si hay al menos 2 pedidos
        .groupBy("customer_id")
        .agg(
            F_avg("diff_days").alias("dias_promedio_entre_pedidos")
        )
    )

    # ---- 6) variedad_categorias_3m ----
    variedad_categorias = (
        fact_3m
        .groupBy("customer_id")
        .agg(
            countDistinct("product_category").alias("variedad_categorias_3m")
        )
    )

    # ---- Join de todas las features por cliente ----
    features = (
        agg_orders
        .join(dias_promedio, on="customer_id", how="left")
        .join(variedad_categorias, on="customer_id", how="left")
    )

    # ---- Filtro de negocio: clientes activos, canal tradicional ----
    clientes_activos_trad = dim_customer.select(
        "customer_id", "canal", "estado"
    )

    features = (
        features
        .join(clientes_activos_trad, on="customer_id", how="inner")
        .filter(
            (col("canal") == "tradicional")
            & (col("estado") == "activo")
            & (col("frecuencia_pedidos_3m") >= 1)
        )
    )

    # Dejamos solo las columnas mínimas pedidas
    dim_features = features.select(
        "customer_id",
        "ventas_total_3m",
        "recencia_dias",
        "frecuencia_pedidos_3m",
        "ticket_promedio_3m",
        "dias_promedio_entre_pedidos",
        "variedad_categorias_3m",
        "porcentaje_pedidos_promo_3m",
        "mix_credito_vs_contado_3m",
    )

    return dim_features


# -------------------------------------------------------------------
# 4) Orquestador de la capa GOLD
# -------------------------------------------------------------------
def run_gold(spark: SparkSession) -> None:
    print("Ejecutando capa GOLD...")

    fact = read_parquet(spark, "fact_order_line")
    dim_customer = read_parquet(spark, "dim_customer")

    # 1) Ventana últimos 3 meses
    fact_3m = build_fact_last_3m(fact)

    # 2) Tabla GOLD auxiliar (no es requerida, pero es útil tenerla)
    gold_sales_customer_3m = build_gold_sales_by_customer(fact_3m, dim_customer)
    write_parquet(gold_sales_customer_3m, "gold_sales_customer_3m")

    # 3) dim_features final con las 8 features
    dim_features = build_dim_features(fact_3m, dim_customer)
    write_parquet(dim_features, "dim_features")

    print("Capa GOLD finalizada.")
