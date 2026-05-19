#!/usr/bin/env bash
# =============================================================================
# ST1630 — Agregaciones Gold (Metro Medellín)
# Copia gold_aggregate.py al contenedor spark-master y ejecuta spark-submit
# para generar las tres tablas Gold desde rest.silver.trips.
#
# Requisito previo: silver.trips debe existir (bash batch/run_silver.sh).
# Los JARs Iceberg deben existir en el contenedor (descargados por run_bronze.sh).
#
# Uso:
#   bash batch/run_gold.sh
# =============================================================================
set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"

# ── Cargar .env ───────────────────────────────────────────────────────────────
if [ -f "${PROJECT_ROOT}/.env" ]; then
  set -a; source "${PROJECT_ROOT}/.env"; set +a
fi

MINIO_USER="${MINIO_USER:-${MINIO_ROOT_USER:-admin}}"
MINIO_PASS="${MINIO_PASS:-${MINIO_ROOT_PASSWORD:-password123}}"
MINIO_BUCKET="${MINIO_BUCKET:-lakehouse}"
ICEBERG_REST_URI="${ICEBERG_REST_URI:-http://iceberg-rest:8181}"

CONTAINER_SCRIPT="/tmp/gold_aggregate.py"
CONTAINER_JAR_DIR="/tmp/iceberg_jars"
ICEBERG_JAR="${CONTAINER_JAR_DIR}/iceberg-spark-runtime-3.5_2.12-1.10.1.jar"
ICEBERG_AWS_JAR="${CONTAINER_JAR_DIR}/iceberg-aws-bundle-1.10.1.jar"

echo ""
echo "============================================================"
echo "  ST1630 — Gold Aggregations"
echo "  Fuente : rest.silver.trips"
echo "  Destino: rest.gold.{estaciones_volumen,"
echo "           retraso_por_linea_mes, demanda_rutas}"
echo "  Bucket : s3://${MINIO_BUCKET}/"
echo "============================================================"
echo ""

# ── Verificar contenedor ──────────────────────────────────────────────────────
if ! docker ps --format '{{.Names}}' | grep -q '^spark-master$'; then
  echo "ERROR: el contenedor spark-master no está corriendo."
  echo "       Ejecuta primero: docker compose up -d"
  exit 1
fi

# ── Verificar JARs ────────────────────────────────────────────────────────────
echo "[1/3] Verificando JARs Iceberg…"
for jar in "${ICEBERG_JAR}" "${ICEBERG_AWS_JAR}"; do
  if ! docker exec spark-master test -f "${jar}" 2>/dev/null; then
    echo "  ERROR: JAR no encontrado: ${jar}"
    echo "         Ejecuta primero: bash batch/run_bronze.sh data/raw/metro_trips_2024_S1.csv"
    exit 1
  fi
  echo "  [OK] $(basename "${jar}")"
done

# ── Copiar script ─────────────────────────────────────────────────────────────
echo ""
echo "[2/3] Copiando gold_aggregate.py al contenedor…"
docker cp "${SCRIPT_DIR}/gold_aggregate.py" "spark-master:${CONTAINER_SCRIPT}"
docker exec -u root spark-master chmod 644 "${CONTAINER_SCRIPT}"
echo "  Copiado: gold_aggregate.py"

# ── Ejecutar spark-submit ─────────────────────────────────────────────────────
echo ""
echo "[3/3] Ejecutando spark-submit…"
echo ""

docker exec \
  -e MINIO_USER="${MINIO_USER}" \
  -e MINIO_PASS="${MINIO_PASS}" \
  -e MINIO_BUCKET="${MINIO_BUCKET}" \
  -e ICEBERG_REST_URI="${ICEBERG_REST_URI}" \
  -e PYSPARK_PYTHON=/usr/bin/python3 \
  -e AWS_REGION=us-east-1 \
  spark-master \
  /opt/spark/bin/spark-submit \
    --master "local[*]" \
    --jars "${ICEBERG_JAR},${ICEBERG_AWS_JAR}" \
    "${CONTAINER_SCRIPT}" \
  2>/dev/null

EXIT_CODE=$?

echo ""
echo "Resultado: $([ "${EXIT_CODE}" -eq 0 ] && echo 'OK (exit 0)' || echo "FALLO (exit ${EXIT_CODE})")"
echo ""

exit "${EXIT_CODE}"
