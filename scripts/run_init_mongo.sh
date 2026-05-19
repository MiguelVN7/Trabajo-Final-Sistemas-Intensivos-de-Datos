#!/usr/bin/env bash
# =============================================================================
# ST1630 — Inicialización MongoDB (metro_medellin_ops)
# Ejecuta init_mongo.py en un contenedor Python efímero dentro de la red
# Docker del stack, conectando a mongodb:27017.
# =============================================================================
set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"

if [ -f "${PROJECT_ROOT}/.env" ]; then
  set -a; source "${PROJECT_ROOT}/.env"; set +a
fi

MONGO_USER="${MONGO_ROOT_USER:-root}"
MONGO_PASS="${MONGO_ROOT_PASSWORD:-rootpassword}"

# Detectar la red del stack
NETWORK=$(docker network ls --format '{{.Name}}' | grep 'lakehouse-net' | head -1)
if [ -z "${NETWORK}" ]; then
  echo "ERROR: red 'lakehouse-net' no encontrada. Ejecuta: docker compose up -d"
  exit 1
fi

if ! docker ps --format '{{.Names}}' | grep -q '^mongodb$'; then
  echo "ERROR: el contenedor 'mongodb' no está corriendo. Ejecuta: docker compose up -d"
  exit 1
fi

echo ""
echo "============================================================"
echo "  ST1630 — init_mongo: metro_medellin_ops"
echo "  Red    : ${NETWORK}"
echo "  MongoDB: mongodb:27017"
echo "============================================================"
echo ""

docker run --rm \
  --name mongo-init \
  --network "${NETWORK}" \
  -e MONGO_HOST="mongodb" \
  -e MONGO_PORT="27017" \
  -e MONGO_USER="${MONGO_USER}" \
  -e MONGO_PASS="${MONGO_PASS}" \
  -v "${SCRIPT_DIR}/init_mongo.py:/init_mongo.py:ro" \
  python:3.11-slim \
  bash -c "pip install pymongo --quiet && python3 /init_mongo.py"
