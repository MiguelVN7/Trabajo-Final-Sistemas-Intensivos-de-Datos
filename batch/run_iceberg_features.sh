#!/usr/bin/env bash
# =============================================================================
# ST1630 — Demo Features Iceberg (ACID, Time Travel, Schema Evolution)
# =============================================================================
set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"

if [ -f "${PROJECT_ROOT}/.env" ]; then
  set -a; source "${PROJECT_ROOT}/.env"; set +a
fi

MINIO_USER="${MINIO_USER:-${MINIO_ROOT_USER:-admin}}"
MINIO_PASS="${MINIO_PASS:-${MINIO_ROOT_PASSWORD:-password123}}"
MINIO_BUCKET="${MINIO_BUCKET:-lakehouse}"
ICEBERG_REST_URI="${ICEBERG_REST_URI:-http://iceberg-rest:8181}"

CONTAINER_SCRIPT="/tmp/iceberg_features.py"
CONTAINER_JAR_DIR="/tmp/iceberg_jars"
ICEBERG_JAR="${CONTAINER_JAR_DIR}/iceberg-spark-runtime-3.5_2.12-1.10.1.jar"
ICEBERG_AWS_JAR="${CONTAINER_JAR_DIR}/iceberg-aws-bundle-1.10.1.jar"

if ! docker ps --format '{{.Names}}' | grep -q '^spark-master$'; then
  echo "ERROR: spark-master no está corriendo."; exit 1
fi

echo "[1/3] Verificando JARs…"
for jar in "${ICEBERG_JAR}" "${ICEBERG_AWS_JAR}"; do
  docker exec spark-master test -f "${jar}" 2>/dev/null \
    && echo "  [OK] $(basename "${jar}")" \
    || { echo "  ERROR: falta ${jar}"; exit 1; }
done

echo "[2/3] Copiando iceberg_features.py…"
docker cp "${SCRIPT_DIR}/iceberg_features.py" "spark-master:${CONTAINER_SCRIPT}"
docker exec -u root spark-master chmod 644 "${CONTAINER_SCRIPT}"

echo "[3/3] Ejecutando demo…"
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
exit "${EXIT_CODE}"
