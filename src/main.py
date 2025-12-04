from pyspark.sql import SparkSession

from .bronze import run_bronze
from .silver import run_silver
from .gold import run_gold


def create_spark_session(app_name: str = "carozzi-medallion") -> SparkSession:
    spark = (
        SparkSession.builder
        .appName(app_name)
        # No uses todos los cores, limita a 2 para no reventar el heap
        .master("local[2]")
        # Zona horaria
        .config("spark.sql.session.timeZone", "UTC")
        # Más memoria para el proceso Spark (ajusta si tu máquina tiene menos RAM)
        .config("spark.driver.memory", "2g")
        .config("spark.executor.memory", "2g")
        # Menos particiones en los shuffles → menos writers concurrentes
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.default.parallelism", "4")
        .getOrCreate()
    )

    # Reduce un poco el logging
    spark.sparkContext.setLogLevel("WARN")

    # Imprimir config útil para debug
    print("=== Spark config ===")
    print("master:", spark.sparkContext.master)
    print("spark.driver.memory:", spark.conf.get("spark.driver.memory"))
    print("spark.executor.memory:", spark.conf.get("spark.executor.memory"))
    print("spark.sql.shuffle.partitions:", spark.conf.get("spark.sql.shuffle.partitions"))
    print("defaultParallelism:", spark.sparkContext.defaultParallelism)
    print("====================")

    return spark


def main():
    spark = create_spark_session()

    try:
        run_bronze(spark)
        run_silver(spark)
        run_gold(spark)
    finally:
        # Por si el JVM ya murió, evita que reviente al hacer stop
        try:
            spark.stop()
        except Exception as e:
            print(f"⚠️ No se pudo detener Spark limpiamente: {e}")


if __name__ == "__main__":
    main()

