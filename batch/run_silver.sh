#!/usr/bin/env bash
# =============================================================================
# ST1630 — Transformación Silver (Metro Medellín)
# Copia el catálogo dimensional y silver_transform.py al contenedor spark-master
# y ejecuta spark-submit para generar rest.silver.trips desde rest.bronze.trips.
#
# Requisito previo: bronze.trips debe tener datos (bash batch/run_bronze.sh).
# Los JARs Iceberg deben existir en el contenedor (descargados por run_bronze.sh).
#
# Uso:
#   bash batch/run_silver.sh
#
# Variables de entorno opcionales (sobreescriben el .env):
#   MINIO_USER     MINIO_PASS     MINIO_BUCKET     ICEBERG_REST_URI
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

# ── Rutas dentro del contenedor ───────────────────────────────────────────────
CONTAINER_DATA_DIR="/tmp/silver_data"
CONTAINER_SCRIPT="/tmp/silver_transform.py"
CONTAINER_JAR_DIR="/tmp/iceberg_jars"
ICEBERG_JAR="${CONTAINER_JAR_DIR}/iceberg-spark-runtime-3.5_2.12-1.10.1.jar"
ICEBERG_AWS_JAR="${CONTAINER_JAR_DIR}/iceberg-aws-bundle-1.10.1.jar"

echo ""
echo "============================================================"
echo "  ST1630 — Silver Transform"
echo "  Fuente : rest.bronze.trips"
echo "  Destino: rest.silver.trips"
echo "  Bucket : s3://${MINIO_BUCKET}/"
echo "============================================================"
echo ""

# ── Verificar contenedor ──────────────────────────────────────────────────────
if ! docker ps --format '{{.Names}}' | grep -q '^spark-master$'; then
  echo "ERROR: el contenedor spark-master no está corriendo."
  echo "       Ejecuta primero: docker compose up -d"
  exit 1
fi

# ── Verificar JARs (deben existir del paso Bronze) ────────────────────────────
echo "[1/4] Verificando JARs Iceberg…"
for jar in "${ICEBERG_JAR}" "${ICEBERG_AWS_JAR}"; do
  if ! docker exec spark-master test -f "${jar}" 2>/dev/null; then
    echo "  ERROR: JAR no encontrado en el contenedor: ${jar}"
    echo "         Ejecuta primero: bash batch/run_bronze.sh data/raw/metro_trips_2024_S1.csv"
    exit 1
  fi
  echo "  [OK] $(basename "${jar}")"
done

# ── Copiar archivos al contenedor ─────────────────────────────────────────────
echo ""
echo "[2/4] Copiando archivos al contenedor…"
docker exec -u root spark-master mkdir -p "${CONTAINER_DATA_DIR}"

docker cp "${PROJECT_ROOT}/data/paradas.csv"    "spark-master:${CONTAINER_DATA_DIR}/paradas.csv"
docker cp "${PROJECT_ROOT}/data/estaciones.csv" "spark-master:${CONTAINER_DATA_DIR}/estaciones.csv"
docker cp "${SCRIPT_DIR}/silver_transform.py"   "spark-master:${CONTAINER_SCRIPT}"

docker exec -u root spark-master chmod 644 \
  "${CONTAINER_DATA_DIR}/paradas.csv" \
  "${CONTAINER_DATA_DIR}/estaciones.csv" \
  "${CONTAINER_SCRIPT}"

echo "  Copiados: paradas.csv, estaciones.csv, silver_transform.py"

# ── Ejecutar spark-submit ─────────────────────────────────────────────────────
echo ""
echo "[3/4] Ejecutando spark-submit…"
echo ""

docker exec \
  -e MINIO_USER="${MINIO_USER}" \
  -e MINIO_PASS="${MINIO_PASS}" \
  -e MINIO_BUCKET="${MINIO_BUCKET}" \
  -e ICEBERG_REST_URI="${ICEBERG_REST_URI}" \
  -e DATA_DIR="${CONTAINER_DATA_DIR}" \
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
echo "[4/4] Resultado: $([ "${EXIT_CODE}" -eq 0 ] && echo 'OK (exit 0)' || echo "FALLO (exit ${EXIT_CODE})")"
echo ""

exit "${EXIT_CODE}"
