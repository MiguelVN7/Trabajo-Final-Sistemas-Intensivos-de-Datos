"""
batch/iceberg_features.py
Demostración de features Iceberg — Metro de Medellín.
Rúbrica 6.4: ACID, Time Travel, Schema Evolution.

══════════════════════════════════════════════════════════════
SECCIÓN 1 — ACID / Historial de Snapshots
  Evidencia de que cada append a bronze.trips es atómico e
  independiente: dos snapshots distintos, operation=append,
  total_records escalando de 50 K a 100 K.

SECCIÓN 2 — Time Travel
  Consulta de bronze.trips en el snapshot ANTERIOR al segundo
  append (→ 50 K filas), usando:
    a) VERSION AS OF <snapshot_id>
    b) TIMESTAMP AS OF '<committed_at del snapshot 1>'
  Contraste con el estado actual (→ 100 K filas).

SECCIÓN 3 — Schema Evolution
  ADD COLUMN weather_condition a bronze.trips sin reescribir
  datos. Verificaciones:
    • Las 100 K filas existentes siguen consultables (NULL en la col nueva).
    • Un append posterior puede poblar la columna → filas nuevas con valor.
  Tabla elegida: bronze.trips.
    Razón: demostrar en datos reales (100 K filas) es más
    convincente; Silver lee silver.trips —no bronze— por lo que
    el ALTER no rompe ningún job existente.

Variables de entorno requeridas:
    MINIO_USER  MINIO_PASS  MINIO_BUCKET  ICEBERG_REST_URI
"""
import os

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# ── Configuración ─────────────────────────────────────────────────────────────
MINIO_USER       = os.environ["MINIO_USER"]
MINIO_PASS       = os.environ["MINIO_PASS"]
MINIO_BUCKET     = os.environ.get("MINIO_BUCKET",     "lakehouse")
ICEBERG_REST_URI = os.environ.get("ICEBERG_REST_URI", "http://iceberg-rest:8181")

BRONZE_TABLE  = "rest.bronze.trips"
SEP  = "═" * 62
SEP2 = "─" * 62


def build_spark() -> SparkSession:
    return (
        SparkSession.builder
        .appName("iceberg-features-demo")
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


# ══════════════════════════════════════════════════════════════
# SECCIÓN 1 — ACID / Historial de Snapshots
# ══════════════════════════════════════════════════════════════
def seccion_acid(spark: SparkSession) -> list:
    """
    Lista el historial de snapshots de bronze.trips y confirma que
    los dos appends (S1 y S2) son transacciones atómicas e independientes.
    Retorna la lista de snapshots para uso en las secciones siguientes.
    """
    print(f"\n{SEP}")
    print("  SECCIÓN 1 — ACID / Historial de Snapshots")
    print(f"{SEP}")

    snapshots_df = spark.sql(f"""
        SELECT
            snapshot_id,
            committed_at,
            operation,
            CAST(summary['added-records']  AS LONG) AS added_records,
            CAST(summary['total-records']  AS LONG) AS total_records,
            CAST(summary['added-data-files'] AS INT) AS added_files
        FROM {BRONZE_TABLE}.snapshots
        ORDER BY committed_at
    """)

    print(f"\n  Historial de snapshots de {BRONZE_TABLE}:")
    snapshots_df.show(truncate=False)

    snapshots = snapshots_df.collect()
    n_snaps = len(snapshots)

    print(f"  Número de snapshots : {n_snaps}")
    for i, s in enumerate(snapshots, 1):
        print(f"  Snapshot {i}: id={s['snapshot_id']}  "
              f"op={s['operation']}  "
              f"added={s['added_records']:,}  "
              f"total={s['total_records']:,}  "
              f"ts={s['committed_at']}")

    # Verificar que el total escaló de 50K a 100K
    assert snapshots[0]['total_records'] == 50_000, \
        f"Snapshot 1 debería tener 50,000 filas, tiene {snapshots[0]['total_records']}"
    assert snapshots[1]['total_records'] == 100_000, \
        f"Snapshot 2 debería tener 100,000 filas, tiene {snapshots[1]['total_records']}"

    print(f"\n  [OK] Snapshot 1 → 50,000 filas (append S1 atómico)")
    print(f"  [OK] Snapshot 2 → 100,000 filas (append S2 atómico, no sobreescribió S1)")
    print(f"  [OK] Cada lote es un snapshot independiente con operation='append'")

    return snapshots


# ══════════════════════════════════════════════════════════════
# SECCIÓN 2 — Time Travel
# ══════════════════════════════════════════════════════════════
def seccion_time_travel(spark: SparkSession, snapshots: list) -> None:
    """
    Consulta bronze.trips en el snapshot ANTERIOR al segundo append
    (debe devolver 50,000 filas) y en el estado actual (100,000).
    Demuestra ambas formas: VERSION AS OF y TIMESTAMP AS OF.
    """
    print(f"\n{SEP}")
    print("  SECCIÓN 2 — Time Travel")
    print(f"{SEP}")

    snap1_id = snapshots[0]['snapshot_id']
    snap2_id = snapshots[1]['snapshot_id']
    snap1_ts = snapshots[0]['committed_at']

    # Formatear timestamp para SQL con precisión de milisegundos.
    # strftime('%H:%M:%S') trunca a segundos: si committed_at=14:32:59.954,
    # el string '14:32:59' es ANTERIOR al snapshot y Spark no lo encuentra.
    ms = snap1_ts.microsecond // 1000
    snap1_ts_str = snap1_ts.strftime(f'%Y-%m-%d %H:%M:%S.{ms:03d}')

    print(f"\n  Referencia de snapshots:")
    print(f"    Snapshot 1 (S1): id={snap1_id}  ts={snap1_ts_str}")
    print(f"    Snapshot 2 (S2): id={snap2_id}  (estado actual)")

    # ── 2a) VERSION AS OF snapshot_id ────────────────────────────────────────
    print(f"\n{SEP2}")
    print("  2a) VERSION AS OF <snapshot_id>")
    print(f"{SEP2}")

    n_v1 = spark.sql(f"""
        SELECT COUNT(*) AS n
        FROM {BRONZE_TABLE} VERSION AS OF {snap1_id}
    """).collect()[0]['n']

    n_v2 = spark.sql(f"""
        SELECT COUNT(*) AS n
        FROM {BRONZE_TABLE} VERSION AS OF {snap2_id}
    """).collect()[0]['n']

    print(f"\n  VERSION AS OF {snap1_id}  →  {n_v1:,} filas  (solo lote S1)")
    print(f"  VERSION AS OF {snap2_id}  →  {n_v2:,} filas  (S1 + S2)")

    # Verificar source_file en el snapshot 1 — debe tener solo S1
    print(f"\n  source_file en VERSION AS OF snapshot 1:")
    spark.sql(f"""
        SELECT source_file, COUNT(*) AS filas
        FROM {BRONZE_TABLE} VERSION AS OF {snap1_id}
        GROUP BY source_file
    """).show(truncate=False)

    # ── 2b) TIMESTAMP AS OF committed_at ─────────────────────────────────────
    print(f"{SEP2}")
    print("  2b) TIMESTAMP AS OF '<committed_at del snapshot 1>'")
    print(f"{SEP2}")

    n_t1 = spark.sql(f"""
        SELECT COUNT(*) AS n
        FROM {BRONZE_TABLE} TIMESTAMP AS OF '{snap1_ts_str}'
    """).collect()[0]['n']

    print(f"\n  TIMESTAMP AS OF '{snap1_ts_str}'  →  {n_t1:,} filas")

    # Estado actual (sin TIME TRAVEL)
    n_current = spark.sql(f"SELECT COUNT(*) AS n FROM {BRONZE_TABLE}").collect()[0]['n']
    print(f"  Estado actual (sin VERSION/TIMESTAMP)        →  {n_current:,} filas")

    # ── Contraste visual ──────────────────────────────────────────────────────
    print(f"\n  Contraste Time Travel:")
    print(f"  ┌─────────────────────────────────────────────────────┐")
    print(f"  │  Consulta                          │  Filas         │")
    print(f"  ├─────────────────────────────────────────────────────┤")
    print(f"  │  VERSION AS OF snapshot_1          │  {n_v1:>10,}  │")
    print(f"  │  TIMESTAMP AS OF ts_snapshot_1     │  {n_t1:>10,}  │")
    print(f"  │  VERSION AS OF snapshot_2          │  {n_v2:>10,}  │")
    print(f"  │  Estado actual                     │  {n_current:>10,}  │")
    print(f"  └─────────────────────────────────────────────────────┘")

    assert n_v1 == 50_000,   f"Time travel snapshot 1 debe dar 50K, dio {n_v1}"
    assert n_t1 == 50_000,   f"Time travel timestamp 1 debe dar 50K, dio {n_t1}"
    assert n_v2 == 100_000,  f"Time travel snapshot 2 debe dar 100K, dio {n_v2}"
    assert n_current == n_v2, "Estado actual debe coincidir con snapshot 2"
    print(f"\n  [OK] Todos los conteos verificados.")


# ══════════════════════════════════════════════════════════════
# SECCIÓN 3 — Schema Evolution
# ══════════════════════════════════════════════════════════════
def seccion_schema_evolution(spark: SparkSession) -> None:
    """
    Agrega la columna weather_condition a bronze.trips con ALTER TABLE.
    Verifica que:
      1. Las 100K filas existentes siguen consultables (weather_condition = NULL).
      2. Un append posterior puede poblar la columna.
      3. El schema cambia sin reescritura de datos.
    """
    print(f"\n{SEP}")
    print("  SECCIÓN 3 — Schema Evolution")
    print(f"  Tabla: {BRONZE_TABLE}")
    print(f"  Columna nueva: weather_condition STRING")
    print(f"{SEP}")

    # ── 3a) Schema ANTES ─────────────────────────────────────────────────────
    print(f"\n{SEP2}")
    print("  3a) Schema ANTES del ALTER TABLE")
    print(f"{SEP2}")
    spark.sql(f"DESCRIBE {BRONZE_TABLE}").show(20, truncate=False)
    cols_before = [r['col_name'] for r in spark.sql(f"DESCRIBE {BRONZE_TABLE}").collect()
                   if not r['col_name'].startswith('#')]
    print(f"  Columnas antes: {len(cols_before)}  → {cols_before}")

    # ── 3b) ALTER TABLE ADD COLUMN ────────────────────────────────────────────
    print(f"\n{SEP2}")
    print("  3b) ALTER TABLE ADD COLUMN weather_condition")
    print(f"{SEP2}")
    spark.sql(f"""
        ALTER TABLE {BRONZE_TABLE}
        ADD COLUMN weather_condition STRING
        COMMENT 'Condición climática en el momento del viaje.
                 NULL en datos históricos; disponible para ingestas
                 futuras que incluyan API climática.'
    """)
    print(f"  ALTER TABLE ejecutado.")

    # ── 3c) Schema DESPUÉS ────────────────────────────────────────────────────
    print(f"\n{SEP2}")
    print("  3c) Schema DESPUÉS del ALTER TABLE")
    print(f"{SEP2}")
    spark.sql(f"DESCRIBE {BRONZE_TABLE}").show(20, truncate=False)
    cols_after = [r['col_name'] for r in spark.sql(f"DESCRIBE {BRONZE_TABLE}").collect()
                  if not r['col_name'].startswith('#')]
    print(f"  Columnas después: {len(cols_after)}  → {cols_after}")
    assert "weather_condition" in cols_after, "weather_condition no aparece en DESCRIBE"
    print(f"  [OK] weather_condition presente en el schema.")

    # ── 3d) Verificar filas existentes: weather_condition = NULL ─────────────
    print(f"\n{SEP2}")
    print("  3d) Filas existentes → weather_condition = NULL (sin reescritura)")
    print(f"{SEP2}")

    print(f"\n  Sample de 5 filas (incluye weather_condition):")
    spark.sql(f"""
        SELECT trip_id, line, trip_date, passenger_count, weather_condition
        FROM {BRONZE_TABLE}
        LIMIT 5
    """).show(truncate=False)

    n_nulls = spark.sql(f"""
        SELECT COUNT(*) AS n FROM {BRONZE_TABLE}
        WHERE weather_condition IS NULL
    """).collect()[0]['n']
    n_total_before_demo = spark.sql(f"SELECT COUNT(*) AS n FROM {BRONZE_TABLE}").collect()[0]['n']

    print(f"  Total filas en tabla                 : {n_total_before_demo:,}")
    print(f"  Filas con weather_condition = NULL   : {n_nulls:,}")
    assert n_nulls == n_total_before_demo, \
        f"Todas las filas existentes deben tener NULL, pero {n_nulls} != {n_total_before_demo}"
    print(f"  [OK] Ningún dato existente fue reescrito — todo NULL como se esperaba.")

    # ── 3e) Append con weather_condition poblada ──────────────────────────────
    print(f"\n{SEP2}")
    print("  3e) Append de filas NUEVAS con weather_condition poblada")
    print(f"{SEP2}")

    # Tomamos 3 filas existentes, les cambiamos trip_id y les asignamos weather
    CONDITIONS_MAP = {"A": "Lluvioso", "K": "Tormenta", "B": "Nublado"}
    demo_df = (
        spark.table(BRONZE_TABLE)
        .filter(F.col("line").isin("A", "K", "B"))
        .limit(3)
        .withColumn("trip_id",
                    F.concat(F.lit("DEMO-WX-"), F.col("trip_id")))
        .withColumn("source_file",
                    F.lit("demo_weather_patch.csv"))
        .withColumn("bronze_ingestion_ts",
                    F.current_timestamp())
        .withColumn("weather_condition",
                    F.when(F.col("line") == "A", "Lluvioso")
                     .when(F.col("line") == "K", "Tormenta")
                     .when(F.col("line") == "B", "Nublado")
                     .otherwise("Soleado"))
    )

    print(f"  Filas de demo a insertar (con weather_condition):")
    demo_df.select("trip_id", "line", "trip_date", "passenger_count",
                   "weather_condition").show(truncate=False)

    demo_df.writeTo(BRONZE_TABLE).append()
    print(f"  Append completado.")

    # ── 3f) Estado final ───────────────────────────────────────────────────────
    print(f"\n{SEP2}")
    print("  3f) Estado final")
    print(f"{SEP2}")

    n_final    = spark.sql(f"SELECT COUNT(*) AS n FROM {BRONZE_TABLE}").collect()[0]['n']
    n_with_wx  = spark.sql(f"""
        SELECT COUNT(*) AS n FROM {BRONZE_TABLE}
        WHERE weather_condition IS NOT NULL
    """).collect()[0]['n']
    n_null_wx  = n_final - n_with_wx

    print(f"\n  Total filas                          : {n_final:,}")
    print(f"  weather_condition IS NOT NULL        : {n_with_wx:,}  ← filas DEMO nuevas")
    print(f"  weather_condition IS NULL            : {n_null_wx:,}  ← datos históricos")

    print(f"\n  Filas DEMO con weather_condition poblada:")
    spark.sql(f"""
        SELECT trip_id, line, trip_date, passenger_count, weather_condition
        FROM {BRONZE_TABLE}
        WHERE source_file = 'demo_weather_patch.csv'
    """).show(truncate=False)

    # Historial de snapshots actualizado (ahora debe haber 3)
    print(f"\n  Historial de snapshots FINAL (incluye el append DEMO):")
    spark.sql(f"""
        SELECT snapshot_id, committed_at, operation,
               CAST(summary['added-records'] AS LONG) AS added_records,
               CAST(summary['total-records'] AS LONG) AS total_records
        FROM {BRONZE_TABLE}.snapshots
        ORDER BY committed_at
    """).show(truncate=False)

    assert n_with_wx == 3, f"Deben ser 3 filas con weather_condition, son {n_with_wx}"
    print(f"  [OK] Schema Evolution verificada:")
    print(f"       • ALTER TABLE ADD COLUMN no reescribió datos existentes.")
    print(f"       • 100K filas históricas: weather_condition = NULL.")
    print(f"       • 3 filas nuevas: weather_condition poblada correctamente.")


# ══════════════════════════════════════════════════════════════
# Main
# ══════════════════════════════════════════════════════════════
def main() -> None:
    print()
    print(SEP)
    print("  Iceberg Features Demo — Metro Medellín")
    print(f"  Tabla principal: {BRONZE_TABLE}")
    print(f"  Bucket         : s3://{MINIO_BUCKET}/")
    print(SEP)

    spark = build_spark()
    spark.sparkContext.setLogLevel("ERROR")

    # Sección 1: ACID
    snapshots = seccion_acid(spark)

    # Sección 2: Time Travel (usa los snapshots de la sección 1)
    seccion_time_travel(spark, snapshots)

    # Sección 3: Schema Evolution
    seccion_schema_evolution(spark)

    print(f"\n{SEP}")
    print("  Demo completada — 3/3 features Iceberg verificados.")
    print(f"  1. ACID          : 2 snapshots independientes, 50K + 50K = 100K atómico")
    print(f"  2. Time Travel   : VERSION AS OF y TIMESTAMP AS OF funcionando")
    print(f"  3. Schema Evol.  : ADD COLUMN sin reescritura, NULL en históricos")
    print(SEP)

    spark.stop()


if __name__ == "__main__":
    main()
