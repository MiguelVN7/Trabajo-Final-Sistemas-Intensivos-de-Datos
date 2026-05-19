#!/usr/bin/env bash
# =============================================================================
# ST1630 — Lanzador del productor Kafka (Metro Medellín)
# Ejecuta streaming/producer.py en un contenedor Python dentro de la red
# Docker del stack, usando kafka:9092 como broker.
#
# Uso:
#   bash streaming/run_producer.sh                  # parámetros por defecto
#   TIME_SCALE=120 bash streaming/run_producer.sh   # 2× velocidad
#   LATE_EVENT_PROBABILITY=0.05 bash streaming/run_producer.sh
# =============================================================================
set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"

# Parámetros configurables por variable de entorno
TIME_SCALE="${TIME_SCALE:-60}"
LATE_EVENT_PROBABILITY="${LATE_EVENT_PROBABILITY:-0.0}"
RANDOM_SEED="${RANDOM_SEED:-42}"
KAFKA_TOPIC="${KAFKA_TOPIC:-transport-events}"

# Detectar la red del stack
NETWORK=$(docker network ls --format '{{.Name}}' | grep 'lakehouse-net' | head -1)
if [ -z "${NETWORK}" ]; then
  echo "ERROR: red 'lakehouse-net' no encontrada."
  echo "       Ejecuta primero: docker compose up -d"
  exit 1
fi

# Verificar que Kafka esté corriendo
if ! docker ps --format '{{.Names}}' | grep -q '^kafka$'; then
  echo "ERROR: el contenedor 'kafka' no está corriendo."
  exit 1
fi

echo ""
echo "============================================================"
echo "  ST1630 — Productor Kafka Metro Medellín"
echo "  Red Docker  : ${NETWORK}"
echo "  TIME_SCALE  : ${TIME_SCALE}×"
echo "  LATE_PROB   : ${LATE_EVENT_PROBABILITY}"
echo "============================================================"
echo ""

# Detener contenedor previo si existe (idempotente)
docker rm -f kafka-producer 2>/dev/null || true

docker run --rm \
  --name kafka-producer \
  --network "${NETWORK}" \
  -e KAFKA_BROKER="kafka:9092" \
  -e KAFKA_TOPIC="${KAFKA_TOPIC}" \
  -e TIME_SCALE="${TIME_SCALE}" \
  -e LATE_EVENT_PROBABILITY="${LATE_EVENT_PROBABILITY}" \
  -e DATA_DIR="/data" \
  -e RANDOM_SEED="${RANDOM_SEED}" \
  -v "${PROJECT_ROOT}/data:/data:ro" \
  -v "${SCRIPT_DIR}/producer.py:/producer.py:ro" \
  python:3.11-slim \
  bash -c "pip install kafka-python --quiet && python3 /producer.py"
