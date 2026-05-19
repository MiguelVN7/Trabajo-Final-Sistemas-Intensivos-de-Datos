"""
validate_catalogo.py
Validación del catálogo de estaciones Metro Medellín con PySpark.
Ejecutar vía scripts/validate_catalogo.sh (que maneja el docker-cp).

Checks:
  1. Carga + schema inferido correcto (tipos y conteos)
  2. Integridad referencial paradas → estaciones (sin huérfanos)
  3. Tildes y caracteres especiales (encoding UTF-8)
  4. es_transbordo=true exactamente en 6 estaciones
  5. Columna orden: sin saltos ni duplicados por línea
"""
import sys
import os
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StringType, DoubleType, BooleanType, IntegerType, LongType
)

spark = (
    SparkSession.builder
    .appName("validate-catalogo")
    .master("local[*]")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("ERROR")

results = {"pass": 0, "fail": 0}
failures = []

def ok(msg):
    print(f"  [OK]   {msg}")
    results["pass"] += 1

def fallo(msg, detail=None):
    print(f"  [FAIL] {msg}")
    if detail:
        for line in detail:
            print(f"         {line}")
    results["fail"] += 1
    failures.append(msg)

DATA_DIR = os.environ.get("DATA_DIR", "/tmp/data")

print()
print("=" * 62)
print("  Validación Catálogo Metro Medellín")
print(f"  Datos: {DATA_DIR}")
print("=" * 62)

# =============================================================
# 1. CARGA Y SCHEMAS
# =============================================================
print()
print("--- [1/5] Carga y schemas ---")

estaciones = (
    spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .option("encoding", "UTF-8")
    .csv(f"{DATA_DIR}/estaciones.csv")
)
paradas = (
    spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .option("encoding", "UTF-8")
    .csv(f"{DATA_DIR}/paradas.csv")
)

# Imprimir schemas
print()
print("  estaciones.csv  →  schema inferido:")
for field in estaciones.schema.fields:
    print(f"    {field.name:<25} {field.dataType}")
print()
print("  paradas.csv  →  schema inferido:")
for field in paradas.schema.fields:
    print(f"    {field.name:<25} {field.dataType}")
print()

# Validar tipos esperados
schema_est = {f.name: f.dataType for f in estaciones.schema.fields}
schema_par = {f.name: f.dataType for f in paradas.schema.fields}

tipos_ok = True

for col, expected in [("latitud", DoubleType()), ("longitud", DoubleType())]:
    if not isinstance(schema_est.get(col), DoubleType):
        fallo(f"estaciones.{col}: esperado DoubleType, obtenido {schema_est.get(col)}")
        tipos_ok = False

if not isinstance(schema_est.get("es_transbordo"), BooleanType):
    fallo(
        "estaciones.es_transbordo: esperado BooleanType, "
        f"obtenido {schema_est.get('es_transbordo')}"
    )
    tipos_ok = False

if not isinstance(schema_par.get("orden"), (IntegerType, LongType)):
    fallo(
        "paradas.orden: esperado IntegerType/LongType, "
        f"obtenido {schema_par.get('orden')}"
    )
    tipos_ok = False

if tipos_ok:
    ok("Tipos inferidos correctos (DoubleType, BooleanType, IntegerType)")

# Conteos esperados
n_est = estaciones.count()
n_par = paradas.count()

if n_est == 49:
    ok(f"estaciones.csv: {n_est} filas (esperadas 49)")
else:
    fallo(f"estaciones.csv: {n_est} filas (esperadas 49)")

if n_par == 57:
    ok(f"paradas.csv: {n_par} filas (esperadas 57)")
else:
    fallo(f"paradas.csv: {n_par} filas (esperadas 57)")

# =============================================================
# 2. INTEGRIDAD REFERENCIAL
# =============================================================
print()
print("--- [2/5] Integridad referencial paradas → estaciones ---")

huerfanos = paradas.join(estaciones, on="station_id", how="left_anti")
n_huerfanos = huerfanos.count()

if n_huerfanos == 0:
    ok("Sin huérfanos: todos los station_id de paradas existen en estaciones")
else:
    rows = huerfanos.select("stop_id", "station_id").collect()
    detail = [f"{r.stop_id}  →  station_id '{r.station_id}' no existe" for r in rows]
    fallo(f"{n_huerfanos} parada(s) huérfana(s)", detail)

# Verificar también la dirección inversa: estaciones sin paradas
sin_parada = estaciones.join(paradas, on="station_id", how="left_anti")
n_sin_parada = sin_parada.count()
if n_sin_parada == 0:
    ok("Toda estación física tiene al menos una parada en paradas.csv")
else:
    rows = sin_parada.select("station_id", "nombre").collect()
    detail = [f"{r.station_id}  '{r.nombre}'" for r in rows]
    fallo(f"{n_sin_parada} estación(es) sin parada asociada", detail)

# =============================================================
# 3. TILDES Y CARACTERES ESPECIALES
# =============================================================
print()
print("--- [3/5] Encoding UTF-8 / caracteres especiales ---")

probe_nombres = [
    "Niquía",
    "Itagüí",
    "Alejandro Echavarría",
    "Trece de Noviembre",
    "Sena (Picacho)",
    "Sena (Ayacucho)",
    "Doce de Octubre",
]

nombres_en_csv = {r.nombre for r in estaciones.select("nombre").collect()}

for nombre in probe_nombres:
    if nombre in nombres_en_csv:
        ok(f"UTF-8 OK: '{nombre}'")
    else:
        fallo(f"No encontrado o encoding incorrecto: '{nombre}'")

# =============================================================
# 4. es_transbordo=true EXACTAMENTE EN 6 ESTACIONES
# =============================================================
print()
print("--- [4/5] Estaciones de transbordo ---")

transbordos = estaciones.filter(F.col("es_transbordo") == True).orderBy("station_id")
n_transbordo = transbordos.count()

if n_transbordo == 6:
    ok("es_transbordo=true en exactamente 6 estaciones")
else:
    fallo(f"es_transbordo=true en {n_transbordo} estaciones (esperadas 6)")

print("  Detalle:")
for r in transbordos.select("station_id", "nombre").collect():
    lineas_row = paradas.filter(F.col("station_id") == r.station_id)
    lineas_list = sorted(x.linea for x in lineas_row.select("linea").collect())
    print(f"    {r.station_id}  {r.nombre:<30}  líneas: {', '.join(lineas_list)}")

# =============================================================
# 5. ORDEN POR LÍNEA: SIN SALTOS NI DUPLICADOS
# =============================================================
print()
print("--- [5/5] Columna orden por línea ---")

lineas = sorted(r.linea for r in paradas.select("linea").distinct().collect())

for linea in lineas:
    df_linea = paradas.filter(F.col("linea") == linea).orderBy("orden")
    ordenes = [r.orden for r in df_linea.select("orden").collect()]
    n = len(ordenes)
    esperado = list(range(1, n + 1))

    # Duplicados
    if len(ordenes) != len(set(ordenes)):
        dupes = [o for o in ordenes if ordenes.count(o) > 1]
        fallo(f"Línea {linea}: orden duplicado en valores {set(dupes)}")
        continue

    # Saltos
    if ordenes != esperado:
        fallo(f"Línea {linea}: orden con saltos — obtenido {ordenes}")
        continue

    ok(f"Línea {linea:<4}  {n} paradas  orden 1..{n}  sin saltos ni duplicados")

# =============================================================
# RESULTADO FINAL
# =============================================================
print()
print("=" * 62)
print(f"  Resultado: {results['pass']} OK  /  {results['fail']} FALLO")
print("=" * 62)

if failures:
    print()
    print("  Checks fallidos:")
    for f in failures:
        print(f"    ✗ {f}")
    print()

spark.stop()
sys.exit(0 if results["fail"] == 0 else 1)
