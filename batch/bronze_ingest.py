"""
batch/bronze_ingest.py
Ingesta Bronze — Metro de Medellín.

Lee un CSV de viajes y lo agrega (APPEND) a la tabla Iceberg rest.bronze.trips
vía el REST Catalog, con MinIO como warehouse.

Idempotencia por lote: si el campo source_file del CSV ya existe en la tabla,
el job termina sin escribir (exit 0). Así, re-ejecutar el mismo lote no duplica
datos ni crea snapshots adicionales.

Variables de entorno requeridas:
  SOURCE_PATH          Ruta completa al CSV dentro del contenedor.
  MINIO_USER           Credencial MinIO (MINIO_ROOT_USER).
  MINIO_PASS           Credencial MinIO (MINIO_ROOT_PASSWORD).
  MINIO_BUCKET         Nombre del bucket (lakehouse).
  ICEBERG_REST_URI     URL del REST Catalog  (http://iceberg-rest:8181).
  ICEBERG_JAR          Ruta al JAR iceberg-spark-runtime.
  ICEBERG_AWS_JAR      Ruta al JAR iceberg-aws-bundle.

Uso (dentro del contenedor spark-master):
  spark-submit --jars $ICEBERG_JAR,$ICEBERG_AWS_JAR batch/bronze_ingest.py
"""
import os
import sys
from pathlib import Path
from datetime import timezone

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField,
    StringType, IntegerType, BooleanType, DateType, TimestampType
)

# ── Configuración por entorno ─────────────────────────────────────────────────
SOURCE_PATH      = os.environ["SOURCE_PATH"]
MINIO_USER       = os.environ["MINIO_USER"]
MINIO_PASS       = os.environ["MINIO_PASS"]
MINIO_BUCKET     = os.environ.get("MINIO_BUCKET",    "lakehouse")
ICEBERG_REST_URI = os.environ.get("ICEBERG_REST_URI", "http://iceberg-rest:8181")

CATALOG   = "rest"
NAMESPACE = "bronze"
TABLE     = "trips"
FULL_TABLE = f"{CATALOG}.{NAMESPACE}.{TABLE}"

# ── Schema explícito del CSV (evita inferencia errónea de tipos) ──────────────
CSV_SCHEMA = StructType([
    StructField("trip_id",          StringType(),  nullable=False),
    StructField("stop_id_origen",   StringType(),  nullable=False),
    StructField("stop_id_destino",  StringType(),  nullable=False),
    StructField("line",             StringType(),  nullable=False),
    StructField("passenger_count",  IntegerType(), nullable=False),
    StructField("delay_seconds",    IntegerType(), nullable=False),
    StructField("trip_date",        DateType(),    nullable=False),
    StructField("hour_block",       StringType(),  nullable=False),
    StructField("is_holiday",       BooleanType(), nullable=False),
    StructField("source_file",      StringType(),  nullable=False),
    StructField("ingestion_ts",     TimestampType(), nullable=False),
])

# ── Spark Session ────────────────────────────────────────────────────────────
def build_spark() -> SparkSession:
    return (
        SparkSession.builder
        .appName(f"bronze-ingest-{Path(SOURCE_PATH).name}")
        .config("spark.sql.extensions",
                "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        # REST Catalog
        .config("spark.sql.catalog.rest",               "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.rest.type",          "rest")
        .config("spark.sql.catalog.rest.uri",           ICEBERG_REST_URI)
        # S3FileIO → MinIO
        .config("spark.sql.catalog.rest.io-impl",       "org.apache.iceberg.aws.s3.S3FileIO")
        .config("spark.sql.catalog.rest.s3.endpoint",   f"http://minio:9000")
        .config("spark.sql.catalog.rest.s3.path-style-access", "true")
        .config("spark.sql.catalog.rest.s3.access-key-id",     MINIO_USER)
        .config("spark.sql.catalog.rest.s3.secret-access-key", MINIO_PASS)
        .config("spark.sql.catalog.rest.s3.region",            "us-east-1")
        .config("spark.sql.catalog.rest.warehouse",     f"s3://{MINIO_BUCKET}/")
        # Evitar problemas con user.home en el contenedor Spark
        .config("spark.driver.extraJavaOptions",        "-Duser.home=/tmp")
        .config("spark.executor.extraJavaOptions",      "-Duser.home=/tmp")
        .getOrCreate()
    )


# ── DDL idempotente ──────────────────────────────────────────────────────────
DDL_CREATE_NAMESPACE = f"CREATE NAMESPACE IF NOT EXISTS {CATALOG}.{NAMESPACE}"

DDL_CREATE_TABLE = f"""
    CREATE TABLE IF NOT EXISTS {FULL_TABLE} (
        trip_id             STRING       COMMENT 'ID único del viaje',
        stop_id_origen      STRING       COMMENT 'Parada de embarco (FK paradas.csv)',
        stop_id_destino     STRING       COMMENT 'Parada de desembarco (FK paradas.csv)',
        line                STRING       COMMENT 'Código de línea (A, B, K, …, TA)',
        passenger_count     INT          COMMENT 'Pasajeros en el viaje',
        delay_seconds       INT          COMMENT 'Retraso reportado en segundos',
        trip_date           DATE         COMMENT 'Fecha del viaje (YYYY-MM-DD)',
        hour_block          STRING       COMMENT 'Franja horaria (05-06, 06-09, …)',
        is_holiday          BOOLEAN      COMMENT 'True si es festivo colombiano 2024',
        source_file         STRING       COMMENT 'Archivo CSV de origen (del generador)',
        ingestion_ts        TIMESTAMP    COMMENT 'Timestamp de generación del CSV',
        bronze_ingestion_ts TIMESTAMP    COMMENT 'Timestamp del append Bronze (este job)'
    )
    USING iceberg
    PARTITIONED BY (line)
    TBLPROPERTIES (
        'write.format.default'     = 'parquet',
        'write.parquet.compression-codec' = 'snappy',
        'history.expire.min-snapshots-to-keep' = '10'
    )
"""


def table_exists(spark: SparkSession) -> bool:
    try:
        spark.sql(f"SELECT 1 FROM {FULL_TABLE} LIMIT 0")
        return True
    except Exception:
        return False


def already_ingested(spark: SparkSession, batch_key: str) -> bool:
    """True si ya hay filas con source_file = batch_key en la tabla."""
    count = (
        spark.sql(
            f"SELECT COUNT(1) AS n FROM {FULL_TABLE} "
            f"WHERE source_file = '{batch_key}'"
        )
        .collect()[0]["n"]
    )
    return count > 0


# ── Lógica principal ─────────────────────────────────────────────────────────
def main() -> None:
    source_path = Path(SOURCE_PATH)
    batch_key   = source_path.name     # "metro_trips_2024_S1.csv"

    print()
    print("=" * 60)
    print(f"  Bronze Ingest — {batch_key}")
    print(f"  Tabla : {FULL_TABLE}")
    print(f"  Bucket: s3://{MINIO_BUCKET}/")
    print("=" * 60)

    spark = build_spark()
    spark.sparkContext.setLogLevel("ERROR")

    # ── DDL idempotente ──────────────────────────────────────────────────────
    print("\n[1/5] Asegurando namespace y tabla…")
    spark.sql(DDL_CREATE_NAMESPACE)
    spark.sql(DDL_CREATE_TABLE)
    print(f"  {FULL_TABLE} lista (creada o ya existía)")

    # ── Check idempotencia por batch ─────────────────────────────────────────
    print(f"\n[2/5] Verificando idempotencia para batch '{batch_key}'…")
    if table_exists(spark) and already_ingested(spark, batch_key):
        print(f"  [SKIP] El batch '{batch_key}' ya está en Bronze.")
        print(f"         Re-ejecutar el mismo lote no crea snapshots duplicados.")
        _show_summary(spark)
        spark.stop()
        sys.exit(0)
    print(f"  Batch '{batch_key}' no encontrado → procede la ingesta.")

    # ── Leer CSV ─────────────────────────────────────────────────────────────
    print(f"\n[3/5] Leyendo {source_path}…")
    df = (
        spark.read
        .option("header",   "true")
        .option("encoding", "UTF-8")
        .schema(CSV_SCHEMA)
        .csv(str(source_path))
    )
    n_raw = df.count()
    print(f"  {n_raw:,} filas leídas del CSV")

    # ── Añadir metadatos de auditoría Bronze ─────────────────────────────────
    print("\n[4/5] Añadiendo bronze_ingestion_ts…")
    df_bronze = df.withColumn(
        "bronze_ingestion_ts",
        F.current_timestamp()   # UTC real del momento del append
    )
    # Verificar que no haya trip_id nulos (integridad mínima)
    n_nulls = df_bronze.filter(F.col("trip_id").isNull()).count()
    if n_nulls > 0:
        print(f"  WARN: {n_nulls} filas con trip_id nulo — se ingestarán igual")
    print(f"  Schema final: {len(df_bronze.columns)} columnas")

    # ── APPEND a Iceberg ─────────────────────────────────────────────────────
    print(f"\n[5/5] Escribiendo a {FULL_TABLE} (mode=append)…")
    df_bronze.writeTo(FULL_TABLE).append()
    print(f"  Append completado.")

    _show_summary(spark)
    spark.stop()


def _show_summary(spark: SparkSession) -> None:
    """Muestra conteo total, distribución por source_file y snapshots."""
    print()
    print("─" * 60)

    # Conteo total y por lote
    print("  Conteo en bronze.trips:")
    spark.sql(f"""
        SELECT source_file,
               COUNT(*)         AS filas,
               MIN(trip_date)   AS fecha_min,
               MAX(trip_date)   AS fecha_max,
               MIN(bronze_ingestion_ts) AS ingesta
        FROM {FULL_TABLE}
        GROUP BY source_file
        ORDER BY source_file
    """).show(truncate=False)

    # Historial de snapshots Iceberg
    print("  Snapshots Iceberg (historial de appends):")
    spark.sql(f"""
        SELECT snapshot_id,
               committed_at,
               operation,
               summary['added-records']     AS added_records,
               summary['total-records']     AS total_records,
               summary['added-data-files']  AS added_files
        FROM {FULL_TABLE}.snapshots
        ORDER BY committed_at
    """).show(truncate=False)

    print("─" * 60)


if __name__ == "__main__":
    main()
