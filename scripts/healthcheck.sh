#!/usr/bin/env bash
# =============================================================================
# ST1630 — Pre-demo healthcheck
# Verifica: (1) contenedores  (2) Flink job  (3) MongoDB / stream activo
#           (4) tablas Gold Iceberg con filas  (5) tópico Kafka
# Uso: bash scripts/healthcheck.sh
# Salida: exit 0 si todo OK, exit 1 si algo falla.
# =============================================================================
set -uo pipefail

# Cargar .env (debe ejecutarse desde la raíz del proyecto)
if [[ -f .env ]]; then
  set -a; source .env; set +a
else
  echo "ERROR: .env no encontrado. Ejecuta desde la raíz del proyecto." >&2
  exit 1
fi

# ── Colores y helpers ────────────────────────────────────────────────────────
GREEN='\033[0;32m'; RED='\033[0;31m'; CYAN='\033[0;36m'
BOLD='\033[1m'; NC='\033[0m'

PASS=0; FAIL=0
declare -a FAILURES=()

ok()      { printf "  ${GREEN}[OK]   ${NC}%s\n" "$*"; PASS=$((PASS+1)); }
fail()    { printf "  ${RED}[FALLO]${NC} %s\n" "$*"; FAIL=$((FAIL+1)); FAILURES+=("$*"); }
section() { printf "\n${BOLD}${CYAN}── %s${NC}\n" "$*"; }

# =============================================================================
# 1. CONTENEDORES DOCKER COMPOSE
# =============================================================================
section "1/5  Contenedores Docker Compose"

# Daemons: deben estar running + healthy
DAEMON_SERVICES=(minio iceberg-rest kafka flink-jobmanager flink-taskmanager spark-master spark-worker mongodb)

for svc in "${DAEMON_SERVICES[@]}"; do
  CONTAINER=$(docker compose ps -q "${svc}" 2>/dev/null | head -1)
  if [[ -z "${CONTAINER}" ]]; then
    fail "contenedor '${svc}' no encontrado (¿stack levantado?)"
    continue
  fi
  STATUS=$(docker inspect --format '{{.State.Status}}' "${CONTAINER}" 2>/dev/null)
  HEALTH=$(docker inspect --format '{{if .State.Health}}{{.State.Health.Status}}{{else}}no-healthcheck{{end}}' "${CONTAINER}" 2>/dev/null)

  if [[ "${STATUS}" == "running" && ("${HEALTH}" == "healthy" || "${HEALTH}" == "no-healthcheck") ]]; then
    ok "${svc} — running / ${HEALTH}"
  else
    fail "${svc} — status=${STATUS} health=${HEALTH}"
  fi
done

# minio-init es one-shot: usa --all para ver contenedores exited
INIT_CONTAINER=$(docker compose ps --all -q minio-init 2>/dev/null | head -1)
if [[ -n "${INIT_CONTAINER}" ]]; then
  INIT_STATUS=$(docker inspect --format '{{.State.Status}}' "${INIT_CONTAINER}" 2>/dev/null)
  INIT_EXIT=$(docker inspect --format '{{.State.ExitCode}}' "${INIT_CONTAINER}" 2>/dev/null)
  if [[ "${INIT_STATUS}" == "exited" && "${INIT_EXIT}" == "0" ]]; then
    ok "minio-init — exited(0) ✓ bucket creado"
  else
    fail "minio-init — status=${INIT_STATUS} exit=${INIT_EXIT}"
  fi
else
  fail "minio-init — contenedor no encontrado"
fi

# =============================================================================
# 2. FLINK — job en estado RUNNING
# =============================================================================
section "2/5  Flink — job RUNNING"

FLINK_JOBS=$(curl -sf http://localhost:8081/jobs 2>/dev/null)
if [[ -z "${FLINK_JOBS}" ]]; then
  fail "Flink REST API no responde en localhost:8081"
else
  RUNNING_IDS=$(echo "${FLINK_JOBS}" | python3 -c "
import sys, json
jobs = json.load(sys.stdin).get('jobs', [])
running = [j['id'] for j in jobs if j['status'] == 'RUNNING']
print('\n'.join(running))
")
  if [[ -z "${RUNNING_IDS}" ]]; then
    fail "Flink: no hay ningún job en estado RUNNING"
  else
    while IFS= read -r jid; do
      JOB_NAME=$(curl -sf "http://localhost:8081/jobs/${jid}" 2>/dev/null \
                 | python3 -c "import sys,json; d=json.load(sys.stdin); print(d.get('name','?'))")
      ok "Flink job RUNNING — ${JOB_NAME} (${jid:0:12}…)"
    done <<< "${RUNNING_IDS}"
  fi
fi

# =============================================================================
# 3. MONGODB — colecciones existen y stream está escribiendo
# =============================================================================
section "3/5  MongoDB — colecciones y actividad del stream (< 5 min)"

MONGO_URI="mongodb://${MONGO_ROOT_USER}:${MONGO_ROOT_PASSWORD}@localhost:27017/"

MONGO_CHECK=$(docker exec mongodb mongosh "${MONGO_URI}" --quiet --eval '
var DB    = "metro_medellin_ops";
var now   = new Date();
var ago5m = new Date(now - 5 * 60 * 1000);
var db2   = db.getSiblingDB(DB);

// Colecciones con upsert por ventana: last_updated debe ser reciente (< 5 min)
function checkFresh(coll, tsField) {
  var colls = db2.getCollectionNames();
  if (!colls.includes(coll)) { print("MISSING:" + coll); return; }
  var doc = db2[coll].findOne({}, {[tsField]: 1}, {sort: {[tsField]: -1}});
  if (!doc) { print("EMPTY:" + coll); return; }
  var ts = doc[tsField];
  if (!ts) { print("NO_TS:" + coll); return; }
  var d = (ts instanceof Date) ? ts : new Date(ts);
  if (d >= ago5m) {
    print("OK:" + coll + ":" + d.toISOString());
  } else {
    print("STALE:" + coll + ":" + d.toISOString());
  }
}

// station_alerts: insert condicional (solo cuando hay alertas activas).
// TTL = 60 min: si hay docs, el stream escribió en la última hora.
function checkExists(coll) {
  var colls = db2.getCollectionNames();
  if (!colls.includes(coll)) { print("MISSING:" + coll); return; }
  var n = db2[coll].countDocuments();
  if (n > 0) {
    var last = db2[coll].findOne({}, {"window_end": 1}, {sort: {"window_end": -1}});
    var ts = last ? last["window_end"] : null;
    var label = ts ? (((ts instanceof Date) ? ts : new Date(ts)).toISOString()) : "?";
    print("OK_EXISTS:" + coll + ":" + n + " docs (última alerta: " + label + ")");
  } else {
    print("EMPTY:" + coll);
  }
}

checkFresh("station_occupancy", "last_updated");
checkFresh("line_delays",       "last_updated");
checkExists("station_alerts");
' 2>/dev/null)

while IFS= read -r line; do
  case "${line}" in
    OK:*)
      coll=$(echo "${line}" | cut -d: -f2)
      ts=$(echo "${line}" | cut -d: -f3-)
      ok "MongoDB.${coll} — último doc: ${ts}"
      ;;
    OK_EXISTS:*)
      coll=$(echo "${line}" | cut -d: -f2)
      detail=$(echo "${line}" | cut -d: -f3-)
      ok "MongoDB.${coll} — ${detail}"
      ;;
    STALE:*)
      coll=$(echo "${line}" | cut -d: -f2)
      ts=$(echo "${line}" | cut -d: -f3-)
      fail "MongoDB.${coll} — último doc DESACTUALIZADO (${ts}) — ¿Flink/productor corriendo?"
      ;;
    MISSING:*)
      coll=$(echo "${line}" | cut -d: -f2)
      fail "MongoDB.${coll} — colección no existe"
      ;;
    EMPTY:*)
      coll=$(echo "${line}" | cut -d: -f2)
      fail "MongoDB.${coll} — colección vacía"
      ;;
    NO_TS:*)
      coll=$(echo "${line}" | cut -d: -f2)
      fail "MongoDB.${coll} — campo timestamp no encontrado en el documento"
      ;;
  esac
done <<< "${MONGO_CHECK}"

# =============================================================================
# 4. ICEBERG — tablas Gold existen y tienen filas
# =============================================================================
section "4/5  Iceberg — tablas Gold con filas"

# Verificar existencia vía REST catalog (rápido, sin Spark)
GOLD_TABLES=(estaciones_volumen retraso_por_linea_mes demanda_rutas)
REST_OK=true
for TABLE in "${GOLD_TABLES[@]}"; do
  HTTP_CODE=$(curl -s -o /dev/null -w "%{http_code}" \
    "http://localhost:8181/v1/namespaces/gold/tables/${TABLE}" 2>/dev/null)
  if [[ "${HTTP_CODE}" == "200" ]]; then
    ok "Iceberg REST: gold.${TABLE} existe"
  else
    fail "Iceberg REST: gold.${TABLE} — HTTP ${HTTP_CODE} (¿Gold poblado?)"
    REST_OK=false
  fi
done

# Verificar row counts con Spark solo si las tablas existen
ICEBERG_JAR="/tmp/iceberg-spark-runtime-3.5_2.12-1.10.1.jar"
ICEBERG_AWS_JAR="/tmp/iceberg-aws-bundle-1.10.1.jar"

if [[ "${REST_OK}" == "false" ]]; then
  printf "  (skipping Spark row count — tabla(s) no encontradas en REST catalog)\n"
elif ! docker exec spark-master test -f "${ICEBERG_JAR}" 2>/dev/null || \
     ! docker exec spark-master test -f "${ICEBERG_AWS_JAR}" 2>/dev/null; then
  fail "Iceberg JARs no encontrados en spark-master (ejecuta primero batch/run_bronze.sh)"
else
  # Escribir el script PySpark en el host y copiarlo al contenedor
  # (no usar heredoc dentro de docker exec: el shell del host lo consume antes de llegar al contenedor)
  PYSPARK_TMP=$(mktemp /tmp/healthcheck_gold_XXXXXX.py)
  cat > "${PYSPARK_TMP}" << 'PYEOF'
import os, sys
from pyspark.sql import SparkSession

spark = (SparkSession.builder
  .appName("healthcheck-gold")
  .master("local[2]")
  .config("spark.sql.extensions",
          "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
  .config("spark.sql.catalog.iceberg",
          "org.apache.iceberg.spark.SparkCatalog")
  .config("spark.sql.catalog.iceberg.type",              "rest")
  .config("spark.sql.catalog.iceberg.uri",               "http://iceberg-rest:8181")
  .config("spark.sql.catalog.iceberg.io-impl",
          "org.apache.iceberg.aws.s3.S3FileIO")
  .config("spark.sql.catalog.iceberg.s3.endpoint",       "http://minio:9000")
  .config("spark.sql.catalog.iceberg.s3.path-style-access", "true")
  .config("spark.sql.catalog.iceberg.s3.access-key-id",
          os.environ["MINIO_ROOT_USER"])
  .config("spark.sql.catalog.iceberg.s3.secret-access-key",
          os.environ["MINIO_ROOT_PASSWORD"])
  .config("spark.sql.catalog.iceberg.s3.region",         "us-east-1")
  .config("spark.sql.catalog.iceberg.warehouse",
          f"s3://{os.environ['MINIO_BUCKET']}/")
  .getOrCreate())

spark.sparkContext.setLogLevel("ERROR")

for t in ["estaciones_volumen", "retraso_por_linea_mes", "demanda_rutas"]:
    try:
        n = spark.table(f"iceberg.gold.{t}").count()
        print(f"GOLD_COUNT:{t}:{n}", flush=True)
    except Exception as e:
        print(f"GOLD_ERROR:{t}:{e}", flush=True)
PYEOF
  docker cp "${PYSPARK_TMP}" spark-master:/tmp/healthcheck_gold.py >/dev/null 2>&1
  rm -f "${PYSPARK_TMP}"

  SPARK_OUT=$(docker exec \
    -e MINIO_ROOT_USER="${MINIO_ROOT_USER}" \
    -e MINIO_ROOT_PASSWORD="${MINIO_ROOT_PASSWORD}" \
    -e MINIO_BUCKET="${MINIO_BUCKET}" \
    -e AWS_REGION="us-east-1" \
    -e PYSPARK_PYTHON=/usr/bin/python3 \
    spark-master \
    /opt/spark/bin/spark-submit \
      --master local[2] \
      --jars "${ICEBERG_JAR},${ICEBERG_AWS_JAR}" \
      /tmp/healthcheck_gold.py 2>/dev/null)

  while IFS= read -r line; do
    case "${line}" in
      GOLD_COUNT:*)
        tbl=$(echo "${line}" | cut -d: -f2)
        cnt=$(echo "${line}" | cut -d: -f3)
        if [[ "${cnt}" -gt 0 ]]; then
          ok "Spark: gold.${tbl} — ${cnt} filas"
        else
          fail "Spark: gold.${tbl} — 0 filas (¿Gold vacío?)"
        fi
        ;;
      GOLD_ERROR:*)
        tbl=$(echo "${line}" | cut -d: -f2)
        msg=$(echo "${line}" | cut -d: -f3-)
        fail "Spark: gold.${tbl} — ${msg}"
        ;;
    esac
  done <<< "${SPARK_OUT}"
fi

# =============================================================================
# 5. KAFKA — tópico transport-events existe
# =============================================================================
section "5/5  Kafka — tópico transport-events"

# Usar --list (una línea por tópico) en lugar de --describe; más robusto con pipefail
if docker exec kafka \
     /opt/kafka/bin/kafka-topics.sh \
       --bootstrap-server localhost:9092 --list \
     2>/dev/null | grep -qx "transport-events"; then
  PARTITIONS=$(docker exec kafka \
    /opt/kafka/bin/kafka-topics.sh \
      --bootstrap-server localhost:9092 \
      --describe --topic transport-events 2>/dev/null \
    | sed -n 's/.*PartitionCount: *\([0-9]*\).*/\1/p' | head -1)
  ok "Kafka: tópico 'transport-events' — ${PARTITIONS:-?} partición(es)"
else
  fail "Kafka: tópico 'transport-events' no existe (¿productor iniciado alguna vez?)"
fi

# =============================================================================
# RESULTADO FINAL
# =============================================================================
echo ""
printf "${BOLD}══════════════════════════════════════════${NC}\n"
printf "${BOLD}  Resultado: ${GREEN}%d OK${NC}${BOLD}  /  ${RED}%d FALLO${NC}\n" "${PASS}" "${FAIL}"
printf "${BOLD}══════════════════════════════════════════${NC}\n"

if [[ "${FAIL}" -gt 0 ]]; then
  echo ""
  printf "${RED}  Checks fallidos:${NC}\n"
  for e in "${FAILURES[@]}"; do
    printf "  ${RED}✗${NC} %s\n" "${e}"
  done
  echo ""
  exit 1
fi

echo ""
exit 0
