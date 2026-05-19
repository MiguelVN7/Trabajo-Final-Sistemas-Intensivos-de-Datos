#!/usr/bin/env bash
# =============================================================================
# ST1630 — Validación del catálogo de estaciones Metro Medellín
# Copia data/estaciones.csv, data/paradas.csv y el script PySpark al
# contenedor spark-master y ejecuta la validación en modo local (sin cluster).
# Idempotente. Salida: exit 0 si todos los checks pasan, exit 1 si alguno falla.
# =============================================================================
set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"

# --- Cargar .env ---
if [ -f "${PROJECT_ROOT}/.env" ]; then
  set -a; source "${PROJECT_ROOT}/.env"; set +a
fi

echo ""
echo "============================================================"
echo "  ST1630 — Validación Catálogo Metro Medellín"
echo "============================================================"

# Verificar que spark-master esté corriendo
if ! docker ps --format '{{.Names}}' | grep -q '^spark-master$'; then
  echo ""
  echo "ERROR: el contenedor spark-master no está corriendo."
  echo "       Ejecuta primero: docker compose up -d"
  exit 1
fi

# Copiar los CSV y el script al contenedor
echo ""
echo "  Copiando archivos al contenedor spark-master..."

docker exec spark-master mkdir -p /tmp/data

docker cp "${PROJECT_ROOT}/data/estaciones.csv"         spark-master:/tmp/data/estaciones.csv
docker cp "${PROJECT_ROOT}/data/paradas.csv"            spark-master:/tmp/data/paradas.csv
docker cp "${SCRIPT_DIR}/validate_catalogo.py"          spark-master:/tmp/validate_catalogo.py

docker exec -u root spark-master chmod 644 \
  /tmp/data/estaciones.csv \
  /tmp/data/paradas.csv \
  /tmp/validate_catalogo.py

echo "  Archivos copiados."
echo ""

# Ejecutar validación — stderr (logs JVM/Spark) suprimido; stdout es el reporte
docker exec \
  -e DATA_DIR=/tmp/data \
  -e PYSPARK_PYTHON=/usr/bin/python3 \
  spark-master \
  bash -c "/opt/spark/bin/spark-submit /tmp/validate_catalogo.py 2>/dev/null"

EXIT_CODE=$?

echo ""
if [ "${EXIT_CODE}" -eq 0 ]; then
  echo "  Todos los checks pasaron."
else
  echo "  Hay fallos — revisa los [FAIL] arriba."
fi
echo ""

exit "${EXIT_CODE}"
