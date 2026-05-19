"""
integration/batch_streaming_compare.py
ST1630 — Integración Batch × Streaming (Rúbrica 7.1)

Cruza dos caminos de datos para responder: ¿está una estación inusualmente
cargada HOY respecto a su patrón histórico relativo?

FUENTES (escalas incompatibles — NO se comparan valores absolutos):

  BATCH  (Iceberg gold.estaciones_volumen, histórico semanas)
    avg_passengers : pasajeros de un viaje individual (generador batch).
    Orden de magnitud: μ ≈ 70–180 según franja horaria.

  STREAMING  (MongoDB station_occupancy, ventana actual de 1 min)
    avg_vehicle_occupancy : ocupación instantánea del vehículo (generador
    streaming). Orden de magnitud: 0–1080.

  Son dos modelos de generación distintos. Restar sus valores absolutos
  produce un sesgo sistemático (el batch siempre parece "más bajo").

MÉTODO — z-score cruzado (comparación adimensional):

  Para cada lado se normaliza la distribución completa de estaciones dentro
  de la misma franja [hour_block, day_of_week_num]:

    z_hist(s)   = (avg_passengers(s)        − μ_batch)  / σ_batch
    z_stream(s) = (avg_vehicle_occupancy(s) − μ_stream) / σ_stream

  Δz(s) = z_stream(s) − z_hist(s)

  Interpretación de Δz:
    Δz >> 0  → la estación subió en el ranking relativo: más cargada hoy
               que lo que su posición histórica entre pares predice.
    Δz ≈  0  → mantiene su posición relativa, sea cual sea el absoluto.
    Δz << 0  → bajó en el ranking: menos cargada de lo esperado.

PROPIEDAD IMPORTANTE DEL MÉTODO:
  Si TODA la red se desplaza uniformemente (p.ej. toda la línea A sube por
  igual), los Δz de todas las estaciones quedan ≈ 0 porque la media de
  referencia (μ_stream) también sube. Δz detecta cambios en la POSICIÓN
  RELATIVA de cada estación respecto a sus pares, no el nivel absoluto de
  la red. Para detectar un desplazamiento global se reportan por separado
  μ_stream y μ_batch como indicadores de contexto (en sus propias escalas,
  sin restarlos).

CAVEATS:
  1. n_stream ≈ 10–25 eventos por ventana de 1 min → z_stream es más ruidoso
     que z_hist (que promedia semanas). Se muestra n_stream como advertencia.
  2. Estaciones sin histórico para [hour_block, dow] quedan excluidas.
  3. Si σ_batch ≈ 0 (distribución batch plana), z_hist degenera a 0; se
     informa y el ranking de esas filas no es significativo.

DEPENDENCIAS:
  - Pipeline Flink corriendo (station_occupancy con datos de los últimos 5 min)
  - spark-master con JARs en /tmp/iceberg_jars/
  - gold.estaciones_volumen poblada (batch/run_gold.sh ejecutado)
  - pymongo (pip install pymongo)

Ejecución:
  docker exec -e MONGO_URI=... -e AWS_REGION=us-east-1 spark-master \\
    python3 /tmp/batch_streaming_compare.py
"""
import os
import sys
from datetime import datetime, timezone, timedelta

# ── 0. Configuración ──────────────────────────────────────────────────────────
MONGO_URI    = os.environ.get("MONGO_URI",   "mongodb://root:rootpassword@mongodb:27017/")
MONGO_DB     = os.environ.get("MONGO_DB",    "metro_medellin_ops")
MINIO_ENDPOINT = os.environ.get("MINIO_ENDPOINT", "http://minio:9000")
MINIO_KEY    = os.environ.get("MINIO_KEY",   "admin")
MINIO_SECRET = os.environ.get("MINIO_SECRET","password123")
ICEBERG_URI  = os.environ.get("ICEBERG_URI", "http://iceberg-rest:8181")
JAR_DIR      = os.environ.get("JAR_DIR",     "/tmp/iceberg_jars")

ICEBERG_JAR  = f"{JAR_DIR}/iceberg-spark-runtime-3.5_2.12-1.10.1.jar"
AWS_JAR      = f"{JAR_DIR}/iceberg-aws-bundle-1.10.1.jar"

TOP_N        = 10    # estaciones a mostrar en el ranking
MIN_HIST_TRIPS = 5   # filtro: ignorar celdas con pocas observaciones históricas

SEP  = "=" * 66
SEP2 = "-" * 66


# ── 1. Helpers ────────────────────────────────────────────────────────────────
def hour_to_block(h: int) -> str:
    """Misma función que generate_batch.py y silver_transform.py."""
    if h < 6:  return "05-06"
    if h < 9:  return "06-09"
    if h < 12: return "09-12"
    if h < 15: return "12-15"
    if h < 18: return "15-18"
    if h < 21: return "18-21"
    return "21-23"


def iso_dow(dt: datetime) -> int:
    """ISO día de semana: 1=lunes … 7=domingo (igual que gold_aggregate.py)."""
    return dt.isoweekday()


def fmt_pct(v: float) -> str:
    sign = "+" if v >= 0 else ""
    return f"{sign}{v:.1f}%"


# ── 2. Leer streaming: MongoDB station_occupancy ──────────────────────────────
def load_streaming(now: datetime) -> dict:
    """
    Devuelve {station_id: doc} con la última ventana cerrada por cada estación.
    Se considera 'reciente' si window_end está dentro de los últimos 5 min
    (permite hasta 4 ventanas de retraso en el watermark).
    """
    from pymongo import MongoClient
    cutoff = now - timedelta(minutes=5)
    client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=5_000)
    docs = list(
        client[MONGO_DB]["station_occupancy"].find(
            {"window_end": {"$gte": cutoff}},
            {"_id": 1, "station_id": 1, "stop_name": 1,
             "avg_vehicle_occupancy": 1, "max_vehicle_occupancy": 1,
             "event_count": 1, "window_end": 1, "lines_serving": 1}
        )
    )
    client.close()
    return {d["station_id"]: d for d in docs}


# ── 3. Leer batch: Iceberg gold.estaciones_volumen ────────────────────────────
def load_historical(hour_block: str, dow: int) -> dict:
    """
    Lee el subconjunto de gold.estaciones_volumen para la franja horaria y
    día de semana actuales. Devuelve {station_id: avg_passengers}.

    PySpark se inicia aquí (heavy import) para no bloquear si Spark no está
    disponible. El catalog 'iceberg' apunta al REST catalog → MinIO.
    """
    from pyspark.sql import SparkSession

    spark = (
        SparkSession.builder
        .appName("ST1630-BatchStreamingCompare")
        .master("local[2]")          # local: el script corre en el contenedor,
                                     # no necesita taskmanager Spark dedicado
        .config("spark.jars",        f"{ICEBERG_JAR},{AWS_JAR}")
        .config("spark.sql.extensions",
                "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .config("spark.sql.catalog.iceberg",
                "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.iceberg.type",     "rest")
        .config("spark.sql.catalog.iceberg.uri",      ICEBERG_URI)
        .config("spark.sql.catalog.iceberg.s3.endpoint",           MINIO_ENDPOINT)
        .config("spark.sql.catalog.iceberg.s3.access-key-id",      MINIO_KEY)
        .config("spark.sql.catalog.iceberg.s3.secret-access-key",  MINIO_SECRET)
        .config("spark.sql.catalog.iceberg.s3.path-style-access",  "true")
        .config("spark.sql.catalog.iceberg.s3.region",            "us-east-1")
        .config("spark.hadoop.fs.s3a.endpoint",       MINIO_ENDPOINT)
        .config("spark.hadoop.fs.s3a.access.key",     MINIO_KEY)
        .config("spark.hadoop.fs.s3a.secret.key",     MINIO_SECRET)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl",
                "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.driver.memory",  "1g")
        .config("spark.executor.memory","1g")
        .config("spark.ui.enabled",     "false")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")

    df = spark.sql(f"""
        SELECT station_id,
               nombre_estacion,
               avg_passengers,
               total_trips,
               avg_delay_seconds
        FROM   iceberg.gold.estaciones_volumen
        WHERE  hour_block      = '{hour_block}'
          AND  day_of_week_num = {dow}
          AND  total_trips     >= {MIN_HIST_TRIPS}
    """)

    rows = df.collect()
    spark.stop()
    return {r["station_id"]: r for r in rows}


# ── 4. Comparación por z-score cruzado ───────────────────────────────────────
def _stats(values: list) -> tuple:
    """Devuelve (mean, std) de una lista de floats. std=0 si n<2."""
    n = len(values)
    if n == 0:
        return 0.0, 0.0
    mu = sum(values) / n
    variance = sum((v - mu) ** 2 for v in values) / n
    return mu, variance ** 0.5


def compare(streaming: dict, historical: dict) -> tuple:
    """
    Calcula Δz = z_stream − z_hist para cada estación con datos en ambos lados.

    Los z-scores se computan sobre el MISMO conjunto de estaciones cruzadas,
    de modo que ambas distribuciones tienen el mismo soporte (mismas estaciones).

    Retorna (rows, context) donde:
      rows    — lista de dicts ordenada por Δz descendente
      context — dict con indicadores agregados de cada distribución
    """
    # Estaciones presentes en AMBAS fuentes
    common = {sid for sid in streaming if sid in historical}

    stream_vals = [streaming[sid]["avg_vehicle_occupancy"] for sid in common]
    batch_vals  = [historical[sid]["avg_passengers"]       for sid in common]

    mu_s, sigma_s = _stats(stream_vals)
    mu_b, sigma_b = _stats(batch_vals)

    context = {
        "n_common":     len(common),
        "n_only_stream": len(streaming) - len(common),
        "n_only_hist":   len(historical) - len(common),
        # Nivel absoluto de cada distribución en su propia escala
        # (NO se restan: son magnitudes distintas)
        "mu_stream":    round(mu_s, 1),
        "sigma_stream": round(sigma_s, 1),
        "mu_batch":     round(mu_b, 1),
        "sigma_batch":  round(sigma_b, 1),
        "sigma_b_zero": sigma_b < 1e-6,
    }

    rows = []
    for sid in common:
        s_doc    = streaming[sid]
        h_row    = historical[sid]
        now_val  = s_doc["avg_vehicle_occupancy"]
        hist_val = h_row["avg_passengers"]

        z_stream = (now_val  - mu_s) / sigma_s if sigma_s > 1e-6 else 0.0
        z_hist   = (hist_val - mu_b) / sigma_b if sigma_b > 1e-6 else 0.0
        delta_z  = z_stream - z_hist

        lines_list = s_doc.get("lines_serving", [])

        # ESTACIONES MULTIMODALES (ej. Acevedo = A + K + P, San Antonio = A + B + TA)
        # avg_vehicle_occupancy en streaming agrega eventos de TODOS los vehículos
        # que pasan por la estación, sin distinguir línea: un metro de la A (capacidad
        # ~1080) y una cabina del K (capacidad ~50) contribuyen con el mismo peso al
        # promedio. El Δz sigue siendo válido como señal relativa (la estación se compara
        # consigo misma a lo largo del tiempo), pero la interpretación física es más
        # compleja: un Δz alto puede reflejar un aumento en la mezcla de vehículos grandes
        # (metro) en vez de más pasajeros. En la defensa, si se pregunta por Acevedo,
        # señalar que es nodo de transbordo metro+cable y que el análisis por station_id
        # es correcto para monitoreo operacional pero no distingue capacidades por línea.
        is_multimodal = len(set(lines_list)) > 1

        rows.append({
            "station_id":    sid,
            "stop_name":     s_doc.get("stop_name", sid),
            "lines":         ",".join(lines_list),
            "multimodal":    is_multimodal,
            "now_val":       round(now_val, 1),
            "hist_val":      round(hist_val, 1),
            "z_stream":      round(z_stream, 2),
            "z_hist":        round(z_hist, 2),
            "delta_z":       round(delta_z, 2),
            "n_stream":      s_doc["event_count"],
            "n_hist_trips":  h_row["total_trips"],
            "window_end":    s_doc["window_end"].strftime("%H:%M:%SZ")
                             if hasattr(s_doc["window_end"], "strftime")
                             else str(s_doc["window_end"]),
        })

    # Ordenar por Δz desc: estaciones que más subieron en ranking relativo primero
    return sorted(rows, key=lambda r: r["delta_z"], reverse=True), context


# ── 5. Main ───────────────────────────────────────────────────────────────────
def main():
    now        = datetime.now(tz=timezone.utc)
    hour_block = hour_to_block(now.hour)
    dow        = iso_dow(now)

    print(f"\n{SEP}")
    print(f"  ST1630 — Batch × Streaming: Ocupación vs Histórico")
    print(f"  Hora UTC    : {now.strftime('%Y-%m-%dT%H:%M:%SZ')}")
    print(f"  Franja      : {hour_block}  |  day_of_week_num={dow} "
          f"({now.strftime('%A')})")
    print(SEP)

    # ── Paso 1: cargar datos de streaming ─────────────────────────────────────
    print("\n[1/3] Leyendo station_occupancy desde MongoDB...")
    try:
        streaming = load_streaming(now)
    except Exception as e:
        print(f"  ERROR MongoDB: {e}")
        sys.exit(1)
    print(f"  {len(streaming)} estaciones con datos en tiempo real")
    if not streaming:
        print("  Sin datos recientes en station_occupancy.")
        print("  ¿Está el job de Flink corriendo y ha cerrado al menos una ventana?")
        sys.exit(0)

    # ── Paso 2: cargar histórico desde Iceberg ────────────────────────────────
    print(f"\n[2/3] Leyendo gold.estaciones_volumen desde Iceberg "
          f"(hora={hour_block}, dow={dow})...")
    try:
        historical = load_historical(hour_block, dow)
    except Exception as e:
        print(f"  ERROR Spark/Iceberg: {e}")
        sys.exit(1)
    print(f"  {len(historical)} registros históricos para esta franja")

    # ── Paso 3: z-score cruzado ───────────────────────────────────────────────
    print("\n[3/3] Calculando z-scores cruzados (Δz = z_stream − z_hist)...")
    rows, ctx = compare(streaming, historical)

    print(f"  Estaciones cruzadas    : {ctx['n_common']}")
    print(f"  Excluidas (sin hist.)  : {ctx['n_only_stream']}  "
          f"→ sin histórico para hour_block='{hour_block}' dow={dow}")
    if ctx["sigma_b_zero"]:
        print("  ⚠ σ_batch ≈ 0: distribución histórica plana; z_hist = 0 para todas.")

    if not rows:
        print("\n  Sin cruce posible — revisa gold.estaciones_volumen.")
        sys.exit(0)

    # ── Contexto de red: nivel absoluto de cada distribución ──────────────────
    # Se reportan en sus propias escalas SIN restarlos: son magnitudes distintas.
    # Sirven para detectar si TODA la red se desplazó (caso que Δz no captura).
    print(f"\n{SEP2}")
    print("  CONTEXTO DE RED (escalas independientes, NO comparables entre sí)")
    print(f"  Streaming  μ={ctx['mu_stream']:>6.1f}  σ={ctx['sigma_stream']:.1f}"
          f"  [pax/vehículo instantáneo]")
    print(f"  Batch      μ={ctx['mu_batch']:>6.1f}  σ={ctx['sigma_batch']:.1f}"
          f"  [pax/viaje individual histórico]")
    print("  (Si la red entera sube o baja uniformemente, Δz ≈ 0 en todas las")
    print("   estaciones — ese desplazamiento global sólo es visible en μ_stream.)")

    # ── Ranking por Δz ────────────────────────────────────────────────────────
    print(f"\n{SEP2}")
    print(f"  RANKING POR Δz — cambio en posición relativa respecto a pares")
    print(f"  (hora={hour_block}  |  day_of_week_num={dow})")
    print(f"  Δz > 0 → subió en ranking entre pares (inusualmente cargada hoy)")
    print(f"  Δz < 0 → bajó en ranking entre pares (menos cargada de lo esperado)")
    print(f"  Δz ≈ 0 → mantiene posición relativa histórica")
    print(f"  (M) = estación multimodal: ver nota en compare()")
    print(SEP2)
    hdr = (f"  {'Estación':<22} {'Líneas':<8} "
           f"{'z_stream':>9} {'z_hist':>7} {'Δz':>7}  "
           f"{'n_rt':>5} {'n_hist':>6}  ventana")
    print(hdr)
    print("  " + "-" * 70)

    for r in rows[:TOP_N]:
        flag = ""
        if   r["delta_z"] >  1.0: flag = " ▲▲"
        elif r["delta_z"] >  0.5: flag = " ▲"
        elif r["delta_z"] < -1.0: flag = " ▼▼"
        elif r["delta_z"] < -0.5: flag = " ▼"
        # (M) marca las estaciones multimodales en el output para alertar al lector
        # que su Δz mezcla capacidades de vehículos heterogéneos (ver nota en compare())
        mm = " (M)" if r["multimodal"] else "    "
        print(
            f"  {r['stop_name']:<22} {r['lines']:<8} "
            f"{r['z_stream']:>+9.2f} {r['z_hist']:>+7.2f} {r['delta_z']:>+7.2f}{flag}{mm}"
            f"  {r['n_stream']:>4}ev  {r['n_hist_trips']:>5}hist"
            f"  {r['window_end']}"
        )

    # ── Cola inferior (más por debajo de su norma) ────────────────────────────
    bottom = [r for r in rows if r["delta_z"] < 0]
    if bottom:
        print(f"\n  --- Cola inferior (bajaron de posición) ---")
        for r in bottom[-min(5, len(bottom)):]:
            mm = " (M)" if r["multimodal"] else ""
            print(
                f"  {r['stop_name']:<22} {r['lines']:<8} "
                f"{r['z_stream']:>+9.2f} {r['z_hist']:>+7.2f} {r['delta_z']:>+7.2f}{mm}"
                f"  {r['n_stream']:>4}ev  {r['n_hist_trips']:>5}hist"
            )

    # ── Resumen ───────────────────────────────────────────────────────────────
    above = sum(1 for r in rows if r["delta_z"] > 0)
    below = len(rows) - above
    print(SEP2)
    print(f"  {above} estaciones subieron de posición relativa, "
          f"{below} bajaron o mantuvieron")
    print(f"  Excluidas sin histórico: {ctx['n_only_stream']}")
    print(SEP)


if __name__ == "__main__":
    main()
