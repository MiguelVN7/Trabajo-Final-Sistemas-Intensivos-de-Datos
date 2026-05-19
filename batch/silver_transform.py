"""
batch/silver_transform.py
Silver Transform — Metro de Medellín.

Lee rest.bronze.trips, aplica limpieza, enriquecimiento dimensional (JOIN con
paradas.csv y estaciones.csv) y añade columnas calculadas. Escribe el resultado
a rest.silver.trips mediante createOrReplace (full refresh de la capa Silver).

Idempotencia: el job es idempotente por diseño. Re-ejecutarlo produce el mismo
resultado dado el mismo estado de Bronze. Silver es siempre una vista
consistente y limpia de Bronze en su estado actual.

Política de descartes:
  Pre-JOIN  (reglas de negocio):
    - Nulos en trip_id, stop_id_origen, stop_id_destino o line → descartar
    - passenger_count fuera de [1, 400]                        → descartar
    - delay_seconds fuera de [0, 1200]                         → descartar
    - stop_id_origen == stop_id_destino (circular)             → descartar
      * Se espera 0 filas: el generador garantiza origen ≠ destino.
      * Si dispara → ALERTA UPSTREAM (bug en generador, no en Silver).
  Post-JOIN (integridad referencial):
    - stop_id_origen sin match en catálogo (orphan)            → descartar
    - stop_id_destino sin match en catálogo (orphan)           → descartar
      * Se esperan 0 filas: FK garantizada en validate_catalogo.py.
      * Si disparan → ALERTA UPSTREAM (bug en generador, no en Silver).

Mapeo hour_block → time_of_day (sin solapamiento):
    "05-06"              → madrugada
    "06-09" | "09-12"   → mañana
    "12-15" | "15-18"   → tarde
    "18-21" | "21-23"   → noche

Variables de entorno requeridas:
    MINIO_USER        Credencial MinIO.
    MINIO_PASS        Credencial MinIO.
    MINIO_BUCKET      Nombre del bucket (lakehouse).
    ICEBERG_REST_URI  URL del REST Catalog (http://iceberg-rest:8181).
    DATA_DIR          Directorio con paradas.csv y estaciones.csv.

Uso (dentro del contenedor spark-master):
    spark-submit --jars $ICEBERG_JAR,$ICEBERG_AWS_JAR batch/silver_transform.py
"""
import os

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField,
    StringType, IntegerType, DoubleType, BooleanType,
)

# ── Configuración por entorno ─────────────────────────────────────────────────
MINIO_USER       = os.environ["MINIO_USER"]
MINIO_PASS       = os.environ["MINIO_PASS"]
MINIO_BUCKET     = os.environ.get("MINIO_BUCKET",     "lakehouse")
ICEBERG_REST_URI = os.environ.get("ICEBERG_REST_URI", "http://iceberg-rest:8181")
DATA_DIR         = os.environ.get("DATA_DIR",         "/tmp/silver_data")

CATALOG       = "rest"
SOURCE_TABLE  = f"{CATALOG}.bronze.trips"
TARGET_TABLE  = f"{CATALOG}.silver.trips"

PARADAS_PATH    = f"{DATA_DIR}/paradas.csv"
ESTACIONES_PATH = f"{DATA_DIR}/estaciones.csv"

# ── Schemas dimensionales (evita inferencia de tipos) ─────────────────────────
PARADAS_SCHEMA = StructType([
    StructField("stop_id",       StringType(),  nullable=False),
    StructField("station_id",    StringType(),  nullable=False),
    StructField("linea",         StringType(),  nullable=False),
    StructField("orden",         IntegerType(), nullable=False),
    StructField("tipo_servicio", StringType(),  nullable=False),
])

ESTACIONES_SCHEMA = StructType([
    StructField("station_id",    StringType(),  nullable=False),
    StructField("nombre",        StringType(),  nullable=False),
    StructField("latitud",       DoubleType(),  nullable=False),
    StructField("longitud",      DoubleType(),  nullable=False),
    StructField("municipio",     StringType(),  nullable=False),
    StructField("es_transbordo", BooleanType(), nullable=False),
])

# ── Expresiones de columnas calculadas ────────────────────────────────────────
# Definidas como funciones para evitar evaluar F.col() antes de que el JVM
# (SparkSession) esté activo — si se definen a nivel de módulo Spark 3.5 lanza
# AssertionError al importar.

def time_of_day_col():
    """
    Mapeo explícito hour_block → time_of_day sin solapamiento (ver docstring):
      "05-06"            → madrugada
      "06-09" | "09-12"  → mañana
      "12-15" | "15-18"  → tarde
      "18-21" | "21-23"  → noche
    """
    return (
        F.when(F.col("hour_block") == "05-06",                  "madrugada")
         .when(F.col("hour_block").isin("06-09", "09-12"),      "mañana")
         .when(F.col("hour_block").isin("12-15", "15-18"),      "tarde")
         .when(F.col("hour_block").isin("18-21", "21-23"),      "noche")
         .otherwise("desconocido")   # defensivo; no debe ocurrir con datos válidos
    )


def delay_category_col():
    """
    "puntual" (0-30 s): retraso menor, no ausencia de retraso.
    """
    return (
        F.when(F.col("delay_seconds") <= 30,  "puntual")
         .when(F.col("delay_seconds") <= 120, "leve")
         .when(F.col("delay_seconds") <= 300, "moderado")
         .otherwise("severo")
    )


# ── Spark Session ─────────────────────────────────────────────────────────────
def build_spark() -> SparkSession:
    return (
        SparkSession.builder
        .appName("silver-transform")
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


# ── Dimensión combinada ───────────────────────────────────────────────────────
def load_dimension(spark: SparkSession):
    """
    Une paradas.csv con estaciones.csv (INNER join en station_id).
    Resultado: 57 filas con stop_id, station_id, nombre, coordenadas,
    municipio, es_transbordo, orden y tipo_servicio.
    """
    paradas = (
        spark.read
        .option("header", "true")
        .schema(PARADAS_SCHEMA)
        .csv(PARADAS_PATH)
    )
    estaciones = (
        spark.read
        .option("header", "true")
        .schema(ESTACIONES_SCHEMA)
        .csv(ESTACIONES_PATH)
    )
    return paradas.join(estaciones, "station_id")


# ── Lógica principal ──────────────────────────────────────────────────────────
def main() -> None:
    print()
    print("=" * 60)
    print("  Silver Transform — Metro Medellín")
    print(f"  Fuente : {SOURCE_TABLE}")
    print(f"  Destino: {TARGET_TABLE}")
    print(f"  Bucket : s3://{MINIO_BUCKET}/")
    print("=" * 60)

    spark = build_spark()
    spark.sparkContext.setLogLevel("ERROR")

    # ── [1] Leer Bronze ──────────────────────────────────────────────────────
    print("\n[1/7] Leyendo bronze.trips desde el REST Catalog…")
    df_bronze = spark.table(SOURCE_TABLE)
    n_bronze  = df_bronze.count()
    print(f"  {n_bronze:,} filas en bronze.trips")

    # ── [2] Limpieza pre-JOIN ────────────────────────────────────────────────
    print("\n[2/7] Limpieza y validaciones pre-JOIN…")
    df = df_bronze
    discards = {}

    # Nulos en campos clave de identidad
    mask_nulls = (
        F.col("trip_id").isNull()
        | F.col("stop_id_origen").isNull()
        | F.col("stop_id_destino").isNull()
        | F.col("line").isNull()
    )
    n = df.filter(mask_nulls).count()
    discards["nulos_campos_clave"]       = n
    df = df.filter(~mask_nulls)
    print(f"  nulos_campos_clave                : {n:>6,}")

    # passenger_count fuera del rango físico del Metro [1, 400]
    mask_pax = (F.col("passenger_count") < 1) | (F.col("passenger_count") > 400)
    n = df.filter(mask_pax).count()
    discards["passenger_count_invalido"] = n
    df = df.filter(~mask_pax)
    print(f"  passenger_count_invalido [1-400]  : {n:>6,}")

    # delay_seconds fuera del rango del generador [0, 1200]
    mask_delay = (F.col("delay_seconds") < 0) | (F.col("delay_seconds") > 1200)
    n = df.filter(mask_delay).count()
    discards["delay_seconds_invalido"]   = n
    df = df.filter(~mask_delay)
    print(f"  delay_seconds_invalido [0-1200]   : {n:>6,}")

    # Viajes circulares — DIAGNÓSTICO: FK upstream garantiza 0
    mask_circ = F.col("stop_id_origen") == F.col("stop_id_destino")
    n = df.filter(mask_circ).count()
    discards["viajes_circulares"]        = n
    if n > 0:
        print(f"  *** ALERTA UPSTREAM: {n:,} viajes circulares "
              f"(stop_id_origen == stop_id_destino).")
        print(f"      Indica un bug en el generador batch. Se descartan.")
    else:
        print(f"  viajes_circulares (esperado=0)    :      0  [OK]")
    df = df.filter(~mask_circ)

    n_post_clean = df.count()
    total_pre = n_bronze - n_post_clean
    print(f"  ──")
    print(f"  Filas tras limpieza pre-JOIN: {n_post_clean:,}  "
          f"(descartadas: {total_pre:,})")

    # ── [3] Cargar dimensión ─────────────────────────────────────────────────
    print("\n[3/7] Cargando catálogo dimensional (paradas.csv + estaciones.csv)…")
    dim = load_dimension(spark)
    print(f"  {dim.count()} paradas en el catálogo dimensional")

    # Proyección ORIGEN — todas las columnas renombradas antes del join
    dim_orig = dim.select(
        F.col("stop_id"),
        F.col("station_id").alias("station_id_origen"),
        F.col("nombre").alias("nombre_origen"),
        F.col("municipio").alias("municipio_origen"),
        F.col("latitud").alias("latitud_origen"),
        F.col("longitud").alias("longitud_origen"),
        F.col("orden").alias("orden_origen"),
        F.col("es_transbordo").alias("es_transbordo_origen"),
        F.col("tipo_servicio"),   # idéntico en origen y destino (misma línea)
    )

    # Proyección DESTINO — ídem, sin tipo_servicio (ya tomado de origen)
    dim_dest = dim.select(
        F.col("stop_id"),
        F.col("station_id").alias("station_id_destino"),
        F.col("nombre").alias("nombre_destino"),
        F.col("municipio").alias("municipio_destino"),
        F.col("latitud").alias("latitud_destino"),
        F.col("longitud").alias("longitud_destino"),
        F.col("orden").alias("orden_destino"),
        F.col("es_transbordo").alias("es_transbordo_destino"),
    )

    # ── [4] JOIN dimensional ─────────────────────────────────────────────────
    print("\n[4/7] Aplicando JOIN dimensional (LEFT para auditar orphans)…")
    df_joined = (
        df
        .join(dim_orig, df["stop_id_origen"] == dim_orig["stop_id"], "left")
        .drop(dim_orig["stop_id"])
        .join(dim_dest, df["stop_id_destino"] == dim_dest["stop_id"], "left")
        .drop(dim_dest["stop_id"])
    )

    # Orphans — DIAGNÓSTICO: FK upstream garantiza 0
    n_oo = df_joined.filter(F.col("nombre_origen").isNull()).count()
    n_od = df_joined.filter(F.col("nombre_destino").isNull()).count()
    discards["orphan_stop_id_origen"]  = n_oo
    discards["orphan_stop_id_destino"] = n_od

    if n_oo > 0:
        print(f"  *** ALERTA UPSTREAM: {n_oo:,} stop_id_origen sin match en catálogo.")
        print(f"      stop_id desconocidos en paradas.csv — investigar generador.")
    else:
        print(f"  orphan_stop_id_origen  (esperado=0): {n_oo:>6,}  [OK]")

    if n_od > 0:
        print(f"  *** ALERTA UPSTREAM: {n_od:,} stop_id_destino sin match en catálogo.")
        print(f"      stop_id desconocidos en paradas.csv — investigar generador.")
    else:
        print(f"  orphan_stop_id_destino (esperado=0): {n_od:>6,}  [OK]")

    df_joined = df_joined.filter(
        F.col("nombre_origen").isNotNull() & F.col("nombre_destino").isNotNull()
    )

    # ── [5] Columnas calculadas ──────────────────────────────────────────────
    print("\n[5/7] Calculando columnas derivadas…")
    df_silver = (
        df_joined
        .withColumn("n_stops",
                    F.abs(F.col("orden_destino") - F.col("orden_origen")))
        .withColumn("trip_direction",
                    F.when(F.col("orden_destino") > F.col("orden_origen"), "forward")
                     .otherwise("reverse"))
        .withColumn("time_of_day",      time_of_day_col())
        .withColumn("is_peak_hour",
                    F.col("hour_block").isin("06-09", "15-18"))
        .withColumn("is_transfer_trip",
                    F.col("es_transbordo_origen") | F.col("es_transbordo_destino"))
        .withColumn("delay_category",   delay_category_col())
        .withColumn("silver_ingestion_ts", F.current_timestamp())
    )

    # Orden canónico de columnas en Silver
    df_silver = df_silver.select(
        # ── Herencia Bronze ──────────────────────────────────────────────────
        "trip_id", "stop_id_origen", "stop_id_destino", "line",
        "passenger_count", "delay_seconds", "trip_date", "hour_block",
        "is_holiday", "source_file", "ingestion_ts", "bronze_ingestion_ts",
        # ── Enriquecimiento ORIGEN ───────────────────────────────────────────
        "station_id_origen", "nombre_origen", "municipio_origen",
        "latitud_origen", "longitud_origen", "orden_origen", "es_transbordo_origen",
        # ── Enriquecimiento DESTINO ──────────────────────────────────────────
        "station_id_destino", "nombre_destino", "municipio_destino",
        "latitud_destino", "longitud_destino", "orden_destino", "es_transbordo_destino",
        # ── Dimensión compartida ─────────────────────────────────────────────
        "tipo_servicio",
        # ── Calculadas ───────────────────────────────────────────────────────
        "n_stops", "trip_direction", "time_of_day",
        "is_peak_hour", "is_transfer_trip", "delay_category",
        # ── Auditoría ────────────────────────────────────────────────────────
        "silver_ingestion_ts",
    )

    n_silver = df_silver.count()
    total_discarded = n_bronze - n_silver
    print(f"  6 columnas calculadas añadidas.")
    print(f"  Filas resultantes: {n_silver:,}  (total descartadas: {total_discarded:,})")

    # ── [6] Crear namespace y escribir Silver ─────────────────────────────────
    print(f"\n[6/7] Escribiendo {TARGET_TABLE} (createOrReplace)…")
    spark.sql("CREATE NAMESPACE IF NOT EXISTS rest.silver")

    (
        df_silver.writeTo(TARGET_TABLE)
        .using("iceberg")
        .partitionedBy("line")
        .tableProperty("write.format.default",                  "parquet")
        .tableProperty("write.parquet.compression-codec",       "snappy")
        .tableProperty("history.expire.min-snapshots-to-keep",  "5")
        .createOrReplace()
    )
    print(f"  Escrita OK.")

    # ── [7] Resumen ───────────────────────────────────────────────────────────
    _show_summary(spark, n_bronze, n_silver, total_discarded, discards)
    spark.stop()


def _show_summary(
    spark: SparkSession,
    n_bronze: int,
    n_silver: int,
    total_discarded: int,
    discards: dict,
) -> None:
    print()
    print("─" * 60)
    print(f"  Bronze leídas     : {n_bronze:,}")
    print(f"  Silver escritas   : {n_silver:,}")
    print(f"  Total descartadas : {total_discarded:,}")
    print()
    print("  Desglose de descartes:")

    UPSTREAM_ALERTS = {"viajes_circulares", "orphan_stop_id_origen", "orphan_stop_id_destino"}
    for causa, n in discards.items():
        alerta = "  *** ALERTA UPSTREAM — investigar" if (n > 0 and causa in UPSTREAM_ALERTS) else ""
        print(f"    {causa:<38}: {n:>6,}{alerta}")

    print()
    print("  Conteo en silver.trips por lote:")
    spark.sql(f"""
        SELECT source_file,
               COUNT(*)              AS filas,
               MIN(trip_date)        AS fecha_min,
               MAX(trip_date)        AS fecha_max,
               COUNT(DISTINCT line)  AS lineas
        FROM {TARGET_TABLE}
        GROUP BY source_file
        ORDER BY source_file
    """).show(truncate=False)

    print("  Snapshots Iceberg de silver.trips:")
    spark.sql(f"""
        SELECT snapshot_id,
               committed_at,
               operation,
               summary['added-records']  AS added_records,
               summary['total-records']  AS total_records
        FROM {TARGET_TABLE}.snapshots
        ORDER BY committed_at
    """).show(truncate=False)

    print("  Schema de silver.trips:")
    spark.sql(f"DESCRIBE {TARGET_TABLE}").show(50, truncate=False)

    print("  Sample 10 filas (columnas analíticas):")
    spark.sql(f"""
        SELECT trip_id,
               line, tipo_servicio,
               nombre_origen, nombre_destino,
               n_stops, trip_direction,
               time_of_day, is_peak_hour,
               is_transfer_trip, delay_category,
               passenger_count, delay_seconds
        FROM {TARGET_TABLE}
        LIMIT 10
    """).show(truncate=60)

    print("─" * 60)


if __name__ == "__main__":
    main()
