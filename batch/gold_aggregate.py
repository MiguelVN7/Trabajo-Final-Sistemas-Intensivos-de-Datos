"""
batch/gold_aggregate.py
Gold Aggregations — Metro de Medellín.

Lee rest.silver.trips y produce tres tablas Gold (full refresh) en el REST Catalog:

  gold.estaciones_volumen     PA1 — Estaciones con mayor demanda de pasajeros
                                    por franja horaria y día de la semana.
                                    Granularidad: (station_id, hour_block, day_of_week)
                                    "Estación" = station_id físico; se miden boardings
                                    (viajes con esa estación como ORIGEN) para evitar
                                    doble conteo origen+destino.

  gold.retraso_por_linea_mes  PA2 — Evolución del retraso promedio por línea mes a mes.
                                    Granularidad: (line, year, month)
                                    Incluye desglose por hora pico/valle y festivo/normal,
                                    más flag is_rainy_month para el efecto climático.
                                    Meses lluviosos Medellín: {4, 5, 9, 10, 11}.

  gold.demanda_rutas          PA3 — Líneas con mayor proporción de viajes de alta
                                    demanda por franja horaria.
                                    Granularidad: (line, tipo_servicio, hour_block, time_of_day)
                                    "Alta demanda" = passenger_count > p90 de la distribución
                                    de la propia línea (umbral derivado de los datos, no
                                    absoluto). Métrica central: pct_high_demand.
                                    NOTA: passenger_count mide pasajeros de un viaje
                                    individual (μ≈180 en pico), NO ocupación instantánea
                                    del vehículo (que vive en el stream). PA3 mide demanda
                                    relativa, no congestión de capacidad.

Variables de entorno requeridas:
    MINIO_USER        Credencial MinIO.
    MINIO_PASS        Credencial MinIO.
    MINIO_BUCKET      Nombre del bucket (default: lakehouse).
    ICEBERG_REST_URI  URL del REST Catalog (default: http://iceberg-rest:8181).

Uso (dentro del contenedor spark-master):
    spark-submit --jars $ICEBERG_JAR,$ICEBERG_AWS_JAR batch/gold_aggregate.py
"""
import os

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F

# ── Configuración ─────────────────────────────────────────────────────────────
MINIO_USER       = os.environ["MINIO_USER"]
MINIO_PASS       = os.environ["MINIO_PASS"]
MINIO_BUCKET     = os.environ.get("MINIO_BUCKET",     "lakehouse")
ICEBERG_REST_URI = os.environ.get("ICEBERG_REST_URI", "http://iceberg-rest:8181")

CATALOG      = "rest"
SOURCE_TABLE = f"{CATALOG}.silver.trips"
T_ESTACIONES = f"{CATALOG}.gold.estaciones_volumen"
T_RETRASO    = f"{CATALOG}.gold.retraso_por_linea_mes"
T_DEMANDA    = f"{CATALOG}.gold.demanda_rutas"


# ── Spark Session ─────────────────────────────────────────────────────────────
def build_spark() -> SparkSession:
    return (
        SparkSession.builder
        .appName("gold-aggregate")
        .config("spark.sql.extensions",
                "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .config("spark.sql.catalog.rest",               "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.rest.type",          "rest")
        .config("spark.sql.catalog.rest.uri",           ICEBERG_REST_URI)
        .config("spark.sql.catalog.rest.io-impl",       "org.apache.iceberg.aws.s3.S3FileIO")
        .config("spark.sql.catalog.rest.s3.endpoint",   "http://minio:9000")
        .config("spark.sql.catalog.rest.s3.path-style-access", "true")
        .config("spark.sql.catalog.rest.s3.access-key-id",     MINIO_USER)
        .config("spark.sql.catalog.rest.s3.secret-access-key", MINIO_PASS)
        .config("spark.sql.catalog.rest.s3.region",            "us-east-1")
        .config("spark.sql.catalog.rest.warehouse",     f"s3://{MINIO_BUCKET}/")
        .config("spark.driver.extraJavaOptions",        "-Duser.home=/tmp")
        .config("spark.executor.extraJavaOptions",      "-Duser.home=/tmp")
        .getOrCreate()
    )


# ── Helpers de expresiones (funciones, no módulo-nivel, para evitar AssertionError JVM) ──

def month_name_col(col):
    """Número de mes → nombre en español."""
    return (
        F.when(col == 1,  "Enero")    .when(col == 2,  "Febrero")
         .when(col == 3,  "Marzo")    .when(col == 4,  "Abril")
         .when(col == 5,  "Mayo")     .when(col == 6,  "Junio")
         .when(col == 7,  "Julio")    .when(col == 8,  "Agosto")
         .when(col == 9,  "Septiembre").when(col == 10, "Octubre")
         .when(col == 11, "Noviembre").when(col == 12, "Diciembre")
    )


def day_name_col(spark_dow):
    """Spark dayofweek (1=Dom … 7=Sáb) → nombre en español."""
    return (
        F.when(spark_dow == 1, "Domingo")
         .when(spark_dow == 2, "Lunes")
         .when(spark_dow == 3, "Martes")
         .when(spark_dow == 4, "Miércoles")
         .when(spark_dow == 5, "Jueves")
         .when(spark_dow == 6, "Viernes")
         .when(spark_dow == 7, "Sábado")
    )


def iso_dow_num(spark_dow):
    """Spark DOW (1=Dom) → ISO (1=Lun, 7=Dom): (spark_dow + 5) % 7 + 1."""
    return (spark_dow + 5) % 7 + 1


# ── PA1: gold.estaciones_volumen ──────────────────────────────────────────────

def build_estaciones_volumen(silver: DataFrame) -> DataFrame:
    """
    Agrega boardings (origin = embarco) por estación física, franja horaria y
    día de la semana. Cada fila responde: "en la estación X, en la franja Y,
    los [día], ¿cuántos pasajeros embarcan de media?"
    """
    df = (
        silver
        .withColumn("_dow", F.dayofweek("trip_date"))
        .groupBy(
            F.col("station_id_origen").alias("station_id"),
            F.col("nombre_origen").alias("nombre_estacion"),
            F.col("municipio_origen").alias("municipio"),
            "hour_block",
            "time_of_day",
            "_dow",
        )
        .agg(
            F.count("*")                              .alias("total_trips"),
            F.sum("passenger_count")                  .alias("total_passengers"),
            F.round(F.avg("passenger_count"), 2)      .alias("avg_passengers"),
            F.max("passenger_count")                  .alias("max_passengers"),
            F.round(F.avg("delay_seconds"),   2)      .alias("avg_delay_seconds"),
            F.current_timestamp()                     .alias("gold_ingestion_ts"),
        )
    )

    return (
        df
        .withColumn("day_of_week_num", iso_dow_num(F.col("_dow")))
        .withColumn("day_name",        day_name_col(F.col("_dow")))
        .withColumn("is_weekend",      F.col("_dow").isin(1, 7))
        .drop("_dow")
        .select(
            "station_id", "nombre_estacion", "municipio",
            "hour_block", "time_of_day",
            "day_of_week_num", "day_name", "is_weekend",
            "total_trips", "total_passengers",
            "avg_passengers", "max_passengers",
            "avg_delay_seconds",
            "gold_ingestion_ts",
        )
    )


# ── PA2: gold.retraso_por_linea_mes ──────────────────────────────────────────

def build_retraso_por_linea_mes(silver: DataFrame) -> DataFrame:
    """
    Evolución del retraso por línea, mes a mes, con desglose
    pico/valle y festivo/normal. is_rainy_month permite aislar
    el efecto de la temporada de lluvias de Medellín.
    """
    rainy = F.col("month").isin(4, 5, 9, 10, 11)

    df = (
        silver
        .withColumn("year",  F.year("trip_date"))
        .withColumn("month", F.month("trip_date"))
        .groupBy("line", "tipo_servicio", "year", "month")
        .agg(
            F.count("*")
             .alias("total_trips"),
            F.sum(F.col("is_holiday").cast("int"))
             .alias("total_holiday_trips"),
            F.round(F.avg("delay_seconds"), 2)
             .alias("avg_delay_seconds"),
            # Pico vs. valle
            F.round(F.avg(F.when( F.col("is_peak_hour"),  F.col("delay_seconds"))), 2)
             .alias("avg_delay_peak"),
            F.round(F.avg(F.when(~F.col("is_peak_hour"),  F.col("delay_seconds"))), 2)
             .alias("avg_delay_offpeak"),
            # Festivo vs. día normal
            F.round(F.avg(F.when( F.col("is_holiday"),    F.col("delay_seconds"))), 2)
             .alias("avg_delay_holiday"),
            F.round(F.avg(F.when(~F.col("is_holiday"),    F.col("delay_seconds"))), 2)
             .alias("avg_delay_nonholiday"),
            F.current_timestamp()
             .alias("gold_ingestion_ts"),
        )
    )

    return (
        df
        .withColumn("is_rainy_month", rainy)
        .withColumn("month_name",     month_name_col(F.col("month")))
        .select(
            "line", "tipo_servicio",
            "year", "month", "month_name", "is_rainy_month",
            "total_trips", "total_holiday_trips",
            "avg_delay_seconds",
            "avg_delay_peak",    "avg_delay_offpeak",
            "avg_delay_holiday", "avg_delay_nonholiday",
            "gold_ingestion_ts",
        )
        .orderBy("line", "year", "month")
    )


# ── PA3: gold.demanda_rutas ───────────────────────────────────────────────────

def build_demanda_rutas(silver: DataFrame) -> DataFrame:
    """
    Proporción de viajes de "alta demanda" por línea y franja horaria.

    "Alta demanda" se define como passenger_count > p90 de la distribución de la
    propia línea — umbral derivado de los datos, no absoluto. Esto hace la métrica
    coherente entre líneas de distinta capacidad (metro vs. cable).

    p90_line_threshold se incluye en la tabla para que la métrica sea auditable:
    el analista puede ver exactamente qué umbral se aplicó a cada línea.
    """
    # Paso 1 — p90 por línea (una fila por línea, 9 en total)
    p90 = (
        silver
        .groupBy("line")
        .agg(
            F.percentile_approx("passenger_count", 0.90)
             .alias("p90_threshold"),
        )
    )

    # Paso 2 — flag de alta demanda por viaje
    flagged = (
        silver
        .join(p90, "line")
        .withColumn("is_high_demand",
                    F.col("passenger_count") > F.col("p90_threshold"))
    )

    # Paso 3 — agregación por (línea, franja)
    return (
        flagged
        .groupBy("line", "tipo_servicio", "hour_block", "time_of_day")
        .agg(
            F.count("*")
             .alias("total_trips"),
            F.sum(F.col("is_high_demand").cast("int"))
             .alias("high_demand_trips"),
            F.round(
                F.sum(F.col("is_high_demand").cast("int")) / F.count("*") * 100, 2
            ).alias("pct_high_demand"),
            F.round(F.avg("passenger_count"), 2)
             .alias("avg_passengers"),
            # p90 es constante para la misma línea; first() es correcto aquí
            F.first("p90_threshold", ignorenulls=True)
             .alias("p90_line_threshold"),
            F.max("passenger_count")
             .alias("max_passengers"),
            F.current_timestamp()
             .alias("gold_ingestion_ts"),
        )
        .select(
            "line", "tipo_servicio", "hour_block", "time_of_day",
            "total_trips", "high_demand_trips", "pct_high_demand",
            "avg_passengers", "p90_line_threshold", "max_passengers",
            "gold_ingestion_ts",
        )
        .orderBy(F.col("pct_high_demand").desc())
    )


# ── Escritura Iceberg (createOrReplace = full refresh) ────────────────────────

def write_gold(df: DataFrame, table: str, spark: SparkSession) -> int:
    (
        df.writeTo(table)
        .using("iceberg")
        .tableProperty("write.format.default",                 "parquet")
        .tableProperty("write.parquet.compression-codec",      "snappy")
        .tableProperty("history.expire.min-snapshots-to-keep", "3")
        .createOrReplace()
    )
    return spark.table(table).count()


# ── Lógica principal ──────────────────────────────────────────────────────────

def main() -> None:
    print()
    print("=" * 60)
    print("  Gold Aggregations — Metro Medellín")
    print(f"  Fuente : {SOURCE_TABLE}")
    print(f"  Destino: rest.gold.*")
    print(f"  Bucket : s3://{MINIO_BUCKET}/")
    print("=" * 60)

    spark = build_spark()
    spark.sparkContext.setLogLevel("ERROR")

    # ── Leer Silver ──────────────────────────────────────────────────────────
    print("\n[0] Leyendo silver.trips…")
    silver = spark.table(SOURCE_TABLE)
    n_silver = silver.count()
    print(f"  {n_silver:,} filas en silver.trips")

    # ── Crear namespace gold ─────────────────────────────────────────────────
    spark.sql("CREATE NAMESPACE IF NOT EXISTS rest.gold")

    # ── PA1: estaciones_volumen ───────────────────────────────────────────────
    print("\n[1/3] Construyendo gold.estaciones_volumen (PA1)…")
    df_est = build_estaciones_volumen(silver)
    n_est  = write_gold(df_est, T_ESTACIONES, spark)
    print(f"  Escrita OK — {n_est:,} filas (hasta 49 estaciones × 7 franjas × 7 días)")

    # ── PA2: retraso_por_linea_mes ────────────────────────────────────────────
    print("\n[2/3] Construyendo gold.retraso_por_linea_mes (PA2)…")
    df_ret = build_retraso_por_linea_mes(silver)
    n_ret  = write_gold(df_ret, T_RETRASO, spark)
    print(f"  Escrita OK — {n_ret:,} filas (hasta 9 líneas × 12 meses = 108)")

    # ── PA3: demanda_rutas ────────────────────────────────────────────────────
    print("\n[3/3] Construyendo gold.demanda_rutas (PA3)…")
    df_dem = build_demanda_rutas(silver)
    n_dem  = write_gold(df_dem, T_DEMANDA, spark)
    print(f"  Escrita OK — {n_dem:,} filas (hasta 9 líneas × 7 franjas = 63)")

    # ── Resumen ───────────────────────────────────────────────────────────────
    _show_summary(spark)
    spark.stop()


def _show_summary(spark: SparkSession) -> None:
    sep = "─" * 60

    # ── PA1 ─────────────────────────────────────────────────────────────────
    print(f"\n{sep}")
    print("  PA1 — gold.estaciones_volumen")
    print(f"{sep}")
    print("  Schema:")
    spark.sql(f"DESCRIBE {T_ESTACIONES}").show(20, truncate=False)

    print("  Top 10 estaciones por avg_passengers en hora pico (06-09 y 15-18):")
    spark.sql(f"""
        SELECT nombre_estacion, municipio, hour_block, day_name,
               total_trips, avg_passengers, avg_delay_seconds
        FROM {T_ESTACIONES}
        WHERE hour_block IN ('06-09','15-18')
          AND is_weekend = false
        ORDER BY avg_passengers DESC
        LIMIT 10
    """).show(truncate=False)

    # ── PA2 ─────────────────────────────────────────────────────────────────
    print(f"\n{sep}")
    print("  PA2 — gold.retraso_por_linea_mes")
    print(f"{sep}")
    print("  Schema:")
    spark.sql(f"DESCRIBE {T_RETRASO}").show(20, truncate=False)

    print("  Línea A — evolución mensual (lluvioso vs. seco):")
    spark.sql(f"""
        SELECT month_name, is_rainy_month,
               total_trips, total_holiday_trips,
               avg_delay_seconds,
               avg_delay_peak, avg_delay_offpeak,
               avg_delay_holiday, avg_delay_nonholiday
        FROM {T_RETRASO}
        WHERE line = 'A'
        ORDER BY month
    """).show(12, truncate=False)

    print("  Comparativa metro vs. cable en meses lluviosos:")
    spark.sql(f"""
        SELECT tipo_servicio, is_rainy_month,
               ROUND(AVG(avg_delay_seconds), 2) AS avg_delay,
               ROUND(AVG(avg_delay_peak),    2) AS avg_delay_peak,
               SUM(total_trips)                 AS total_trips
        FROM {T_RETRASO}
        GROUP BY tipo_servicio, is_rainy_month
        ORDER BY tipo_servicio, is_rainy_month
    """).show(truncate=False)

    # ── PA3 ─────────────────────────────────────────────────────────────────
    print(f"\n{sep}")
    print("  PA3 — gold.demanda_rutas")
    print(f"{sep}")
    print("  Schema:")
    spark.sql(f"DESCRIBE {T_DEMANDA}").show(15, truncate=False)

    print("  Top 10 combinaciones línea/franja por pct_high_demand:")
    spark.sql(f"""
        SELECT line, tipo_servicio, hour_block, time_of_day,
               total_trips, high_demand_trips, pct_high_demand,
               avg_passengers, p90_line_threshold
        FROM {T_DEMANDA}
        ORDER BY pct_high_demand DESC
        LIMIT 10
    """).show(truncate=False)

    print("  Umbral p90 por línea (referencia):")
    spark.sql(f"""
        SELECT line, tipo_servicio,
               MAX(p90_line_threshold) AS p90_threshold,
               ROUND(AVG(avg_passengers), 1) AS avg_passengers_all_slots
        FROM {T_DEMANDA}
        GROUP BY line, tipo_servicio
        ORDER BY tipo_servicio, line
    """).show(truncate=False)

    print(sep)


if __name__ == "__main__":
    main()
