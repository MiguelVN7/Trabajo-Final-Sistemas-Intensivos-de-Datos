#!/usr/bin/env python3
"""
ST1630 — Generador de capturas para demo de respaldo
Corre cada paso del sistema, captura el output y renderiza
imágenes PNG 1920×1080 de estilo terminal + slideshow HTML.

Uso (desde la raíz del proyecto):
    python3 scripts/make_demo.py

Salida: demo_capturas/
    01_healthcheck.png
    02_bronze_time_travel.png
    03_gold_pa1.png
    04_mongodb_queries.png
    05_integracion_zscore.png
    index.html
"""
import subprocess, re, os, sys, textwrap, shutil
from pathlib import Path
from PIL import Image, ImageDraw, ImageFont

# ── Configuración ────────────────────────────────────────────────────────────
OUT_DIR    = Path("demo_capturas")
W, H       = 1920, 1080
FONT_PATH  = "/System/Library/Fonts/Menlo.ttc"
FONT_SIZE  = 15
LINE_H     = 22          # px entre líneas
PAD_X      = 70          # margen lateral
TOP_BAR    = 110         # altura barra de título
BOT_BAR    = 55          # altura pie de página
MAX_LINES  = (H - TOP_BAR - BOT_BAR - 20) // LINE_H   # líneas que caben

# Paleta GitHub Dark
BG        = "#0d1117"
FG        = "#c9d1d9"
MUTED     = "#6e7681"
BLUE      = "#79c0ff"
GREEN     = "#3fb950"
RED       = "#f85149"
YELLOW    = "#e3b341"
PURPLE    = "#d2a8ff"
TEAL      = "#39d353"
BAR_BG    = "#161b22"
ACCENT    = "#58a6ff"

# ── Iceberg / Spark / Mongo env ──────────────────────────────────────────────
SPARK_ENV = {
    "MINIO_ROOT_USER":     "admin",
    "MINIO_ROOT_PASSWORD": "password123",
    "MINIO_BUCKET":        "lakehouse",
    "AWS_REGION":          "us-east-1",
    "PYSPARK_PYTHON":      "/usr/bin/python3",
    "MONGO_ROOT_USER":     "root",
    "MONGO_ROOT_PASSWORD": "rootpassword",
}
ICEBERG_JAR     = "/tmp/iceberg-spark-runtime-3.5_2.12-1.10.1.jar"
ICEBERG_AWS_JAR = "/tmp/iceberg-aws-bundle-1.10.1.jar"
ICEBERG_CONFS   = [
    "--conf", "spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
    "--conf", "spark.sql.catalog.iceberg=org.apache.iceberg.spark.SparkCatalog",
    "--conf", "spark.sql.catalog.iceberg.type=rest",
    "--conf", "spark.sql.catalog.iceberg.uri=http://iceberg-rest:8181",
    "--conf", "spark.sql.catalog.iceberg.io-impl=org.apache.iceberg.aws.s3.S3FileIO",
    "--conf", "spark.sql.catalog.iceberg.s3.endpoint=http://minio:9000",
    "--conf", "spark.sql.catalog.iceberg.s3.path-style-access=true",
    "--conf", "spark.sql.catalog.iceberg.s3.access-key-id=admin",
    "--conf", "spark.sql.catalog.iceberg.s3.secret-access-key=password123",
    "--conf", "spark.sql.catalog.iceberg.s3.region=us-east-1",
    "--conf", "spark.sql.catalog.iceberg.warehouse=s3://lakehouse/",
]

# ── Helpers de ejecución ─────────────────────────────────────────────────────
def strip_ansi(text: str) -> str:
    return re.sub(r"\x1b\[[0-9;]*[mABCDEFGHJKLMnpqrsuhl]", "", text)

# Patrones de log de Spark/Java que ensucian el output útil
_SPARK_LOG_RE = re.compile(
    r"^\d{2}/\d{2}/\d{2} \d{2}:\d{2}:\d{2} (INFO|WARN|DEBUG|ERROR)\s+\S+"
    r"|^SLF4J:|^log4j:|^Using Spark|^Setting default|^Ivy Default Cache"
    r"|^The jars for|^:: loading settings|^:: resolving|^:: resolution report"
    r"|^download|^:: modules in use|^\s*\|?\s*(org\.apache|com\.amazonaws)"
)

def filter_spark_noise(text: str) -> str:
    """Elimina líneas de log de Spark que no aportan información al demo."""
    lines = text.splitlines()
    clean = [l for l in lines if not _SPARK_LOG_RE.match(l)]
    return "\n".join(clean)

def run(cmd, *, timeout=300, env_extra=None, stderr=False) -> str:
    """stderr=False: sólo captura stdout (descarta logs de Spark en stderr)."""
    env = os.environ.copy()
    if env_extra:
        env.update(env_extra)
    try:
        r = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout, env=env)
        raw = (r.stdout + r.stderr).strip() if stderr else r.stdout.strip()
        return strip_ansi(raw)
    except subprocess.TimeoutExpired:
        return "(timeout)"

def spark_submit_script(name: str, code: str) -> str:
    """Escribe un script Python, lo copia al contenedor y lo ejecuta con spark-submit.
    Solo captura stdout; stderr (logs Spark/Java) se descarta."""
    local_tmp = f"/tmp/{name}.py"
    Path(local_tmp).write_text(code)
    subprocess.run(["docker", "cp", local_tmp, f"spark-master:/tmp/{name}.py"],
                   check=True, capture_output=True)
    cmd = [
        "docker", "exec",
        *sum([["-e", f"{k}={v}"] for k, v in SPARK_ENV.items()], []),
        "spark-master",
        "/opt/spark/bin/spark-submit",
        "--jars", f"{ICEBERG_JAR},{ICEBERG_AWS_JAR}",
        *ICEBERG_CONFS,
        f"/tmp/{name}.py",
    ]
    out = run(cmd, timeout=300, stderr=False)   # stderr=False → descarta logs Java
    return filter_spark_noise(out)

# ── Renderizado PNG ──────────────────────────────────────────────────────────
def hex2rgb(h: str):
    h = h.lstrip("#")
    return tuple(int(h[i:i+2], 16) for i in (0, 2, 4))

def make_image(title: str, step: str, command: str, output: str) -> Image.Image:
    img  = Image.new("RGB", (W, H), hex2rgb(BG))
    draw = ImageDraw.Draw(img)

    try:
        font_title = ImageFont.truetype(FONT_PATH, 22)
        font_step  = ImageFont.truetype(FONT_PATH, 14)
        font_cmd   = ImageFont.truetype(FONT_PATH, FONT_SIZE)
        font_out   = ImageFont.truetype(FONT_PATH, FONT_SIZE)
        font_small = ImageFont.truetype(FONT_PATH, 12)
    except Exception:
        font_title = font_step = font_cmd = font_out = font_small = ImageFont.load_default()

    # ── Barra de título ──
    draw.rectangle([0, 0, W, TOP_BAR], fill=hex2rgb(BAR_BG))
    draw.line([(0, TOP_BAR), (W, TOP_BAR)], fill=hex2rgb(MUTED), width=1)

    # Dot decorations (macOS-style)
    for i, color in enumerate(["#ff5f57", "#febc2e", "#28c840"]):
        cx = 30 + i * 22
        draw.ellipse([cx-7, 47, cx+7, 61], fill=hex2rgb(color))

    # Step badge
    badge_x = 85
    draw.rounded_rectangle([badge_x, 38, badge_x + 85, 68], radius=6, fill=hex2rgb(ACCENT))
    draw.text((badge_x + 10, 46), step, font=font_step, fill=hex2rgb(BG))

    # Title
    draw.text((badge_x + 100, 42), title, font=font_title, fill=hex2rgb(FG))

    # Project label (right)
    draw.text((W - 340, 46), "ST1630 · Metro Medellín · EAFIT 2026-1",
              font=font_small, fill=hex2rgb(MUTED))

    # ── Cuerpo del contenido ──
    y = TOP_BAR + 12

    # Command prompt line
    if command:
        prompt = "$ "
        draw.text((PAD_X, y), prompt, font=font_cmd, fill=hex2rgb(TEAL))
        cmd_x = PAD_X + int(draw.textlength(prompt, font=font_cmd))
        draw.text((cmd_x, y), command, font=font_cmd, fill=hex2rgb(BLUE))
        y += LINE_H + 4

    # Output lines
    lines = output.splitlines()
    if len(lines) > MAX_LINES:
        lines = lines[:MAX_LINES - 1] + [f"  … ({len(lines) - MAX_LINES + 1} líneas más)"]

    for line in lines:
        # Colorizar según contenido
        stripped = line.strip()
        if re.search(r"\[OK\]", stripped, re.I):
            color = hex2rgb(GREEN)
        elif re.search(r"\[(FALLO|FAIL)\]|error|exception", stripped, re.I):
            color = hex2rgb(RED)
        elif re.search(r"^(──|══|---|\+--)", stripped):
            color = hex2rgb(MUTED)
        elif re.search(r"^\s*[▲▼]\s", stripped) or re.search(r"delta_z|z_stream|z_hist", stripped, re.I):
            color = hex2rgb(YELLOW)
        elif re.search(r"^\d+/5|^paso|^=== ", stripped, re.I):
            color = hex2rgb(PURPLE)
        elif re.search(r"RUNNING|healthy|OK|✓", stripped):
            color = hex2rgb(GREEN)
        elif re.search(r"snapshot_id|time.travel|version as of", stripped, re.I):
            color = hex2rgb(TEAL)
        else:
            color = hex2rgb(FG)

        draw.text((PAD_X, y), line[:220], font=font_out, fill=color)
        y += LINE_H

    # ── Pie de página ──
    bot_y = H - BOT_BAR
    draw.line([(0, bot_y), (W, bot_y)], fill=hex2rgb(MUTED), width=1)
    draw.rectangle([0, bot_y, W, H], fill=hex2rgb(BAR_BG))
    draw.text((PAD_X, bot_y + 16),
              "Sistemas Intensivos en Datos  ·  Esteban Molina · Miguel Villegas · Sebastián Rodríguez",
              font=font_small, fill=hex2rgb(MUTED))
    draw.text((W - 200, bot_y + 16), "github.com/eafit-st1630",
              font=font_small, fill=hex2rgb(MUTED))

    return img

# ── HTML slideshow ───────────────────────────────────────────────────────────
def make_html(images: list[tuple[str, str]]) -> str:
    imgs_js = "[" + ",".join(f'"{p}"' for _, p in images) + "]"
    titles_js = "[" + ",".join(f'"{t}"' for t, _ in images) + "]"
    return f"""<!DOCTYPE html>
<html lang="es">
<head>
<meta charset="UTF-8">
<title>ST1630 — Demo de respaldo · Metro Medellín</title>
<style>
  * {{ box-sizing: border-box; margin: 0; padding: 0; }}
  body {{ background: #000; color: #fff; font-family: system-ui, sans-serif;
          display: flex; flex-direction: column; height: 100vh; overflow: hidden; }}
  #slide {{ flex: 1; display: flex; align-items: center; justify-content: center; }}
  #slide img {{ max-width: 100%; max-height: 100%; object-fit: contain; }}
  #bar {{ background: #161b22; border-top: 1px solid #30363d;
          display: flex; align-items: center; gap: 16px; padding: 8px 24px;
          font-size: 13px; color: #8b949e; }}
  #bar button {{ background: #21262d; border: 1px solid #30363d; color: #c9d1d9;
                 padding: 4px 14px; border-radius: 6px; cursor: pointer; font-size: 13px; }}
  #bar button:hover {{ background: #30363d; }}
  #title {{ flex: 1; color: #79c0ff; font-weight: 600; }}
  #counter {{ min-width: 60px; text-align: right; }}
</style>
</head>
<body>
<div id="slide"><img id="img" src=""></div>
<div id="bar">
  <button onclick="prev()">◀  Anterior</button>
  <span id="title"></span>
  <button onclick="next()">Siguiente  ▶</button>
  <span id="counter"></span>
</div>
<script>
const imgs   = {imgs_js};
const titles = {titles_js};
let cur = 0;
function show(i) {{
  cur = (i + imgs.length) % imgs.length;
  document.getElementById("img").src     = imgs[cur];
  document.getElementById("title").textContent  = titles[cur];
  document.getElementById("counter").textContent = (cur+1) + " / " + imgs.length;
}}
function next() {{ show(cur+1); }}
function prev() {{ show(cur-1); }}
document.addEventListener("keydown", e => {{
  if (e.key === "ArrowRight" || e.key === " ") next();
  if (e.key === "ArrowLeft")  prev();
}});
show(0);
</script>
</body>
</html>"""

# ── Scripts Spark ────────────────────────────────────────────────────────────
BRONZE_SCRIPT = """\
import os
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("demo-bronze-timetravel").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

print("=== Snapshots de iceberg.bronze.trips ===")
snaps = spark.sql(
    "SELECT snapshot_id, committed_at, operation "
    "FROM iceberg.bronze.trips.snapshots ORDER BY committed_at"
)
snaps.show(truncate=False)

ids = [r.snapshot_id for r in snaps.collect()]
if len(ids) >= 2:
    first_id = ids[0]
    n_first = spark.sql(
        f"SELECT count(*) as n FROM iceberg.bronze.trips VERSION AS OF {first_id}"
    ).collect()[0].n
    n_now   = spark.sql("SELECT count(*) as n FROM iceberg.bronze.trips").collect()[0].n

    print(f"=== Time Travel: VERSION AS OF {first_id} ===")
    print(f"  Filas en snapshot inicial : {n_first:,}")
    print(f"  Filas en snapshot actual  : {n_now:,}")
    print(f"  Delta                     : +{n_now - n_first:,} filas agregadas")
    print()
    print("=== Distribución por fecha (snapshot inicial) ===")
    spark.sql(
        f"SELECT trip_date, count(*) as n_viajes "
        f"FROM iceberg.bronze.trips VERSION AS OF {first_id} "
        f"GROUP BY trip_date ORDER BY trip_date"
    ).show(15, truncate=False)
else:
    print("(solo un snapshot disponible)")
    snaps.show(truncate=False)
"""

GOLD_PA1_SCRIPT = """\
import os
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("demo-gold-pa1").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

print("=== PA1 — Estaciones con mayor demanda por franja horaria ===")
print("    Tabla: iceberg.gold.estaciones_volumen")
print()
df = spark.table("iceberg.gold.estaciones_volumen")
print(f"Total de filas en Gold: {df.count():,}")
print()

print("--- Top 10 estaciones por pasajeros promedio (hora pico 06-09) ---")
spark.sql('''
    SELECT station_id, nombre_estacion, hour_block, day_name,
           ROUND(avg_passengers, 1) AS avg_pax,
           total_trips
    FROM iceberg.gold.estaciones_volumen
    WHERE hour_block = "06-09"
    ORDER BY avg_passengers DESC
    LIMIT 10
''').show(truncate=False)

print("--- Top 10 estaciones globales (todos los horarios) ---")
spark.sql('''
    SELECT station_id, nombre_estacion,
           ROUND(AVG(avg_passengers), 1) AS avg_pax_global,
           SUM(total_trips)              AS total_trips_global
    FROM iceberg.gold.estaciones_volumen
    GROUP BY station_id, nombre_estacion
    ORDER BY avg_pax_global DESC
    LIMIT 10
''').show(truncate=False)
"""

ZSCORE_PREP = """\
import os, sys
sys.path.insert(0, "/opt/spark/python")
"""

# ── Paso 4: queries_operacionales via spark-master (tiene pymongo instalado) ──
def run_mongo_queries() -> str:
    subprocess.run(
        ["docker", "cp", "streaming/queries_operacionales.py",
         "spark-master:/tmp/queries_operacionales.py"],
        check=True, capture_output=True,
    )
    cmd = [
        "docker", "exec",
        "-e", "MONGO_URI=mongodb://root:rootpassword@mongodb:27017/",
        "spark-master",
        "python3", "/tmp/queries_operacionales.py",
    ]
    return run(cmd, timeout=90, stderr=True)   # stderr=True: captura errores de pymongo

# ── Paso 5: integración (ya copiado a spark-master) ──────────────────────────
def run_integration() -> str:
    cmd = [
        "docker", "exec",
        "-e", f"MONGO_URI=mongodb://root:rootpassword@mongodb:27017/",
        "-e", f"MINIO_ENDPOINT=http://minio:9000",
        "-e", f"MINIO_KEY=admin",
        "-e", f"MINIO_SECRET=password123",
        "-e", f"ICEBERG_URI=http://iceberg-rest:8181",
        "-e", f"AWS_REGION=us-east-1",
        "-e", f"PYTHONPATH=/opt/spark/python:/opt/spark/python/lib/py4j-0.10.9.7-src.zip",
        "spark-master",
        "python3", "/tmp/batch_streaming_compare.py",
    ]
    return run(cmd, timeout=300)

# ── Main ─────────────────────────────────────────────────────────────────────
def main():
    OUT_DIR.mkdir(exist_ok=True)

    steps = []  # [(title, filename)]

    # ── 1/5  Healthcheck ──────────────────────────────────────────────────────
    print("\n[1/5] Corriendo healthcheck…")
    hc_out = run(["bash", "scripts/healthcheck.sh"])
    img = make_image(
        title   = "Pre-demo Healthcheck — 17 OK / 0 FALLO",
        step    = "PASO 1/5",
        command = "bash scripts/healthcheck.sh",
        output  = hc_out,
    )
    path = "01_healthcheck.png"
    img.save(OUT_DIR / path)
    steps.append(("1/5 · Healthcheck — 17 OK", path))
    print(f"    → {path}")

    # ── 2/5  Bronze Time Travel ───────────────────────────────────────────────
    print("\n[2/5] Corriendo Bronze Time Travel (Spark)…")
    bronze_out = spark_submit_script("demo_bronze_tt", BRONZE_SCRIPT)
    img = make_image(
        title   = "Lakehouse Bronze — Snapshots Iceberg + Time Travel",
        step    = "PASO 2/5",
        command = "spark.sql('SELECT … FROM bronze.trips.snapshots')  +  VERSION AS OF <snapshot_id>",
        output  = bronze_out,
    )
    path = "02_bronze_time_travel.png"
    img.save(OUT_DIR / path)
    steps.append(("2/5 · Bronze · Time Travel Iceberg", path))
    print(f"    → {path}")

    # ── 3/5  Gold PA1 ─────────────────────────────────────────────────────────
    print("\n[3/5] Corriendo Gold PA1 (Spark)…")
    gold_out = spark_submit_script("demo_gold_pa1", GOLD_PA1_SCRIPT)
    img = make_image(
        title   = "Lakehouse Gold — PA1: Estaciones con mayor demanda por franja horaria",
        step    = "PASO 3/5",
        command = "spark.table('iceberg.gold.estaciones_volumen').orderBy('avg_passengers', desc)",
        output  = gold_out,
    )
    path = "03_gold_pa1.png"
    img.save(OUT_DIR / path)
    steps.append(("3/5 · Gold PA1 · Demanda por estación", path))
    print(f"    → {path}")

    # ── 4/5  MongoDB queries ──────────────────────────────────────────────────
    print("\n[4/5] Corriendo queries operacionales MongoDB…")
    mongo_out = run_mongo_queries()
    img = make_image(
        title   = "MongoDB — Queries operacionales PO1 / PO2 / PO3",
        step    = "PASO 4/5",
        command = "python3 streaming/queries_operacionales.py",
        output  = mongo_out,
    )
    path = "04_mongodb_queries.png"
    img.save(OUT_DIR / path)
    steps.append(("4/5 · MongoDB · PO1 / PO2 / PO3", path))
    print(f"    → {path}")

    # ── 5/5  Integración z-score ──────────────────────────────────────────────
    print("\n[5/5] Corriendo integración batch × streaming (z-score)…")
    int_out = run_integration()
    img = make_image(
        title   = "Integración Batch × Streaming — Ranking Δz (z-score cruzado)",
        step    = "PASO 5/5",
        command = "docker exec spark-master python3 /tmp/batch_streaming_compare.py",
        output  = int_out,
    )
    path = "05_integracion_zscore.png"
    img.save(OUT_DIR / path)
    steps.append(("5/5 · Integración · Ranking Δz", path))
    print(f"    → {path}")

    # ── HTML slideshow ────────────────────────────────────────────────────────
    html = make_html(steps)
    (OUT_DIR / "index.html").write_text(html, encoding="utf-8")

    print(f"\n✓  Demo lista en {OUT_DIR}/")
    print(f"   Abre en el navegador:  open {OUT_DIR}/index.html")
    print(f"   Imágenes: {len(steps)} capturas PNG 1920×1080")

if __name__ == "__main__":
    os.chdir(Path(__file__).parent.parent)   # asegurar raíz del proyecto
    main()
