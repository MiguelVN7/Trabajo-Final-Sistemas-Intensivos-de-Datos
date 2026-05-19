#!/usr/bin/env bash
# =============================================================================
# ST1630 — Ingesta Bronze (Metro Medellín)
# Copia el job PySpark al contenedor spark-master, descarga los JARs Iceberg
# si no existen, y ejecuta spark-submit para ingestar un CSV a bronze.trips.
#
# Uso:
#   bash batch/run_bronze.sh data/raw/metro_trips_2024_S1.csv
#   bash batch/run_bronze.sh data/raw/metro_trips_2024_S2.csv
#
# Variables de entorno opcionales (sobreescriben el .env):
#   MINIO_USER     MINIO_PASS     MINIO_BUCKET     ICEBERG_REST_URI
# =============================================================================
set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"

# ── Argumento obligatorio ─────────────────────────────────────────────────────
if [ $# -lt 1 ]; then
  echo "Uso: bash batch/run_bronze.sh <ruta/al/archivo.csv>"
  exit 1
fi

HOST_CSV_PATH="$(realpath "${1}")"
CSV_FILENAME="$(basename "${HOST_CSV_PATH}")"

# ── Cargar .env ───────────────────────────────────────────────────────────────
if [ -f "${PROJECT_ROOT}/.env" ]; then
  set -a; source "${PROJECT_ROOT}/.env"; set +a
fi

MINIO_USER="${MINIO_USER:-${MINIO_ROOT_USER:-admin}}"
MINIO_PASS="${MINIO_PASS:-${MINIO_ROOT_PASSWORD:-password123}}"
MINIO_BUCKET="${MINIO_BUCKET:-lakehouse}"
ICEBERG_REST_URI="${ICEBERG_REST_URI:-http://iceberg-rest:8181}"

# ── Rutas dentro del contenedor ───────────────────────────────────────────────
CONTAINER_DATA_DIR="/tmp/bronze_data"
CONTAINER_CSV_PATH="${CONTAINER_DATA_DIR}/${CSV_FILENAME}"
CONTAINER_SCRIPT="/tmp/bronze_ingest.py"
CONTAINER_JAR_DIR="/tmp/iceberg_jars"
ICEBERG_JAR="${CONTAINER_JAR_DIR}/iceberg-spark-runtime-3.5_2.12-1.10.1.jar"
ICEBERG_AWS_JAR="${CONTAINER_JAR_DIR}/iceberg-aws-bundle-1.10.1.jar"

# ── Verificar contenedor spark-master ────────────────────────────────────────
echo ""
echo "============================================================"
echo "  ST1630 — Bronze Ingest"
echo "  Archivo : ${CSV_FILENAME}"
echo "  Tabla   : rest.bronze.trips"
echo "  Bucket  : s3://${MINIO_BUCKET}/"
echo "============================================================"
echo ""

if ! docker ps --format '{{.Names}}' | grep -q '^spark-master$'; then
  echo "ERROR: el contenedor spark-master no está corriendo."
  echo "       Ejecuta primero: docker compose up -d"
  exit 1
fi

# ── Verificar que el CSV existe en el host ────────────────────────────────────
if [ ! -f "${HOST_CSV_PATH}" ]; then
  echo "ERROR: archivo no encontrado: ${HOST_CSV_PATH}"
  exit 1
fi

# ── Preparar directorios en el contenedor ────────────────────────────────────
echo "[1/4] Preparando contenedor…"
docker exec -u root spark-master mkdir -p "${CONTAINER_DATA_DIR}" "${CONTAINER_JAR_DIR}"

# ── Descargar JARs si no existen ─────────────────────────────────────────────
MAVEN_BASE="https://repo1.maven.org/maven2/org/apache/iceberg"

check_and_download() {
  local jar_path="$1"
  local jar_url="$2"
  local jar_name
  jar_name="$(basename "${jar_path}")"

  if docker exec spark-master test -f "${jar_path}" 2>/dev/null; then
    echo "  [OK] ${jar_name} ya existe."
  else
    echo "  Descargando ${jar_name}…"
    docker exec -u root spark-master \
      bash -c "wget -q '${jar_url}' -O '${jar_path}' && echo '  Descargado OK'"
  fi
}

check_and_download \
  "${ICEBERG_JAR}" \
  "${MAVEN_BASE}/iceberg-spark-runtime-3.5_2.12/1.10.1/iceberg-spark-runtime-3.5_2.12-1.10.1.jar"

check_and_download \
  "${ICEBERG_AWS_JAR}" \
  "${MAVEN_BASE}/iceberg-aws-bundle/1.10.1/iceberg-aws-bundle-1.10.1.jar"

# ── Copiar archivos al contenedor ─────────────────────────────────────────────
echo ""
echo "[2/4] Copiando archivos al contenedor…"
docker cp "${HOST_CSV_PATH}"                   "spark-master:${CONTAINER_CSV_PATH}"
docker cp "${SCRIPT_DIR}/bronze_ingest.py"     "spark-master:${CONTAINER_SCRIPT}"
docker exec -u root spark-master chmod 644 \
  "${CONTAINER_CSV_PATH}" "${CONTAINER_SCRIPT}"
echo "  Copiados: ${CSV_FILENAME}, bronze_ingest.py"

# ── Ejecutar spark-submit ─────────────────────────────────────────────────────
echo ""
echo "[3/4] Ejecutando spark-submit…"
echo ""

docker exec \
  -e SOURCE_PATH="${CONTAINER_CSV_PATH}" \
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
echo "[4/4] Resultado: $([ "${EXIT_CODE}" -eq 0 ] && echo 'OK (exit 0)' || echo "FALLO (exit ${EXIT_CODE})")"
echo ""

exit "${EXIT_CODE}"
