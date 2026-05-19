#!/usr/bin/env bash
# =============================================================================
# ST1630 — Smoke test de conectividad Fase 0
# Verifica: (1) Kafka  (2) MinIO  (3) Spark + Iceberg REST Catalog end-to-end
# Idempotente: se puede ejecutar varias veces sin efectos secundarios.
# Salida: exit 0 si todo OK, exit 1 si algún check falla.
# =============================================================================
set -uo pipefail

# --- Cargar .env ---
if [ -f .env ]; then
  set -a; source .env; set +a
fi

PASS=0; FAIL=0
declare -a FAILURES=()

ok()   { echo "  [OK]   $*"; PASS=$((PASS + 1)); }
fail() { echo "  [FAIL] $*"; FAIL=$((FAIL + 1)); FAILURES+=("$*"); }

# Ejecuta un comando; OK si exit 0, FAIL si exit != 0.
check() {
  local label="$1"; shift
  if "$@" >/dev/null 2>&1; then ok "$label"; else fail "$label"; fi
}

# Detectar nombre de red del proyecto (prefix generado por Compose)
NETWORK=$(docker network ls --format '{{.Name}}' | grep 'lakehouse-net' | head -1)
if [ -z "$NETWORK" ]; then
  echo "ERROR: red 'lakehouse-net' no encontrada. ¿Está el stack levantado?"
  exit 1
fi

echo ""
echo "============================================================"
echo "  ST1630 Smoke Test — Fase 0"
echo "  Red Docker: ${NETWORK}"
echo "============================================================"

# =============================================================================
# 1. APACHE KAFKA
# =============================================================================
echo ""
echo "--- [1/3] Apache Kafka ---"

# Crear topic (--if-not-exists lo hace idempotente)
if docker exec kafka \
     /opt/kafka/bin/kafka-topics.sh \
       --bootstrap-server localhost:9092 \
       --create --topic smoke-test \
       --partitions 1 --replication-factor 1 \
       --if-not-exists >/dev/null 2>&1; then
  ok "Kafka: topic 'smoke-test' creado (o ya existía)"
else
  fail "Kafka: no se pudo crear el topic 'smoke-test'"
fi

# Confirmar que el topic aparece en el listado del broker
check "Kafka: topic 'smoke-test' confirmado en el broker" \
  bash -c "docker exec kafka \
    /opt/kafka/bin/kafka-topics.sh \
      --bootstrap-server localhost:9092 --list \
    2>/dev/null | grep -qx 'smoke-test'"

# Describe el topic para verificar metadata completa
check "Kafka: topic 'smoke-test' con 1 partición y RF=1" \
  bash -c "docker exec kafka \
    /opt/kafka/bin/kafka-topics.sh \
      --bootstrap-server localhost:9092 \
      --describe --topic smoke-test \
    2>/dev/null | grep -q 'PartitionCount: *1'"

# =============================================================================
# 2. MINIO — Object Storage
# =============================================================================
echo ""
echo "--- [2/3] MinIO (Object Storage) ---"

check "MinIO: health API responde en localhost:9000" \
  curl -sf http://localhost:9000/minio/health/live

# Verificar bucket con mc (imagen ya cacheada localmente)
check "MinIO: bucket '${MINIO_BUCKET}' existe y es accesible" \
  docker run --rm --network "${NETWORK}" \
    --entrypoint /bin/sh \
    minio/mc:RELEASE.2025-08-13T08-35-41Z \
    -c "mc alias set local http://minio:9000 \
          ${MINIO_ROOT_USER} ${MINIO_ROOT_PASSWORD} --quiet \
        && mc ls local/${MINIO_BUCKET} >/dev/null 2>&1"

# =============================================================================
# 3. SPARK + ICEBERG REST CATALOG (end-to-end)
# =============================================================================
echo ""
echo "--- [3/3] Spark + Iceberg REST Catalog (end-to-end) ---"

# Descargar los JARs de Iceberg si no están cacheados en el contenedor
ICEBERG_JAR="/tmp/iceberg-spark-runtime-3.5_2.12-1.10.1.jar"
ICEBERG_AWS_JAR="/tmp/iceberg-aws-bundle-1.10.1.jar"
ICEBERG_URL="https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-3.5_2.12/1.10.1/iceberg-spark-runtime-3.5_2.12-1.10.1.jar"
ICEBERG_AWS_URL="https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-aws-bundle/1.10.1/iceberg-aws-bundle-1.10.1.jar"

if ! docker exec spark-master test -f "${ICEBERG_JAR}"; then
  echo "  Descargando iceberg-spark-runtime (~45 MB)..."
  docker exec spark-master wget -q -O "${ICEBERG_JAR}" "${ICEBERG_URL}" \
    || { fail "No se pudo descargar iceberg-spark-runtime"; }
fi
if ! docker exec spark-master test -f "${ICEBERG_AWS_JAR}"; then
  echo "  Descargando iceberg-aws-bundle (~60 MB, incluye AWS SDK v2)..."
  docker exec spark-master wget -q -O "${ICEBERG_AWS_JAR}" "${ICEBERG_AWS_URL}" \
    || { fail "No se pudo descargar iceberg-aws-bundle"; }
fi

# Escribir el script PySpark en un tmp del HOST y copiarlo al contenedor
# (heredoc a docker exec no pasa stdin correctamente desde dentro de un script)
PYSPARK_TMP=$(mktemp /tmp/smoke_iceberg_XXXXXX.py)
cat > "${PYSPARK_TMP}" << 'PYEOF'
import sys, os
from pyspark.sql import SparkSession

MINIO_USER   = os.environ["MINIO_USER"]
MINIO_PASS   = os.environ["MINIO_PASS"]
MINIO_BUCKET = os.environ["MINIO_BUCKET"]

spark = (SparkSession.builder
  .appName("smoke-test-iceberg")
  .config("spark.sql.extensions",
          "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
  .config("spark.sql.catalog.rest",
          "org.apache.iceberg.spark.SparkCatalog")
  .config("spark.sql.catalog.rest.type",                   "rest")
  .config("spark.sql.catalog.rest.uri",                    "http://iceberg-rest:8181")
  .config("spark.sql.catalog.rest.io-impl",
          "org.apache.iceberg.aws.s3.S3FileIO")
  .config("spark.sql.catalog.rest.s3.endpoint",            "http://minio:9000")
  .config("spark.sql.catalog.rest.s3.path-style-access",   "true")
  .config("spark.sql.catalog.rest.s3.access-key-id",       MINIO_USER)
  .config("spark.sql.catalog.rest.s3.secret-access-key",   MINIO_PASS)
  .config("spark.sql.catalog.rest.warehouse",              f"s3://{MINIO_BUCKET}/")
  .getOrCreate())

spark.sparkContext.setLogLevel("ERROR")

try:
    spark.sql("CREATE NAMESPACE IF NOT EXISTS rest.smoke")
    spark.sql("""
        CREATE TABLE IF NOT EXISTS rest.smoke.ping
        (id BIGINT, ts STRING)
        USING iceberg
    """)
    rows = spark.sql("SHOW TABLES IN rest.smoke").collect()
    assert any(r.tableName == "ping" for r in rows), \
        "La tabla 'ping' no aparece en el catálogo"
    spark.sql("DROP TABLE rest.smoke.ping")
    spark.sql("DROP NAMESPACE rest.smoke")
    print("SMOKE_OK")
    sys.exit(0)
except Exception as e:
    print(f"SMOKE_FAIL: {e}", file=sys.stderr)
    sys.exit(1)
PYEOF
docker cp "${PYSPARK_TMP}" spark-master:/tmp/smoke_iceberg.py
docker exec -u root spark-master chmod 644 /tmp/smoke_iceberg.py
rm -f "${PYSPARK_TMP}"

# Ejecutar spark-submit con el runtime de Iceberg
SPARK_OUT=$(docker exec \
  -e MINIO_USER="${MINIO_ROOT_USER}" \
  -e MINIO_PASS="${MINIO_ROOT_PASSWORD}" \
  -e MINIO_BUCKET="${MINIO_BUCKET}" \
  -e PYSPARK_PYTHON=/usr/bin/python3 \
  spark-master \
  bash -c "
    /opt/spark/bin/spark-submit \
      --jars ${ICEBERG_JAR},${ICEBERG_AWS_JAR} \
      /tmp/smoke_iceberg.py 2>&1
  " 2>&1)

if echo "${SPARK_OUT}" | grep -q "SMOKE_OK"; then
  ok "Spark: tabla Iceberg creada y eliminada vía REST Catalog ↔ MinIO"
else
  fail "Spark: tabla Iceberg vía REST Catalog ↔ MinIO"
  # Mostrar las últimas líneas de log para diagnóstico
  echo ""
  echo "  --- últimas líneas del log de Spark ---"
  echo "${SPARK_OUT}" | grep -v "^[[:space:]]*$" | tail -20 | sed 's/^/  | /'
  echo "  --- fin del log ---"
fi

# =============================================================================
# RESULTADO FINAL
# =============================================================================
echo ""
echo "============================================================"
printf "  Resultado: %d OK  /  %d FAIL\n" "${PASS}" "${FAIL}"
echo "============================================================"

if [ "${FAIL}" -gt 0 ]; then
  echo ""
  echo "  Checks fallidos:"
  for e in "${FAILURES[@]}"; do echo "    ✗ $e"; done
  echo ""
  exit 1
fi

echo ""
exit 0
