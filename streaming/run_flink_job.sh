#!/usr/bin/env bash
# =============================================================================
# ST1630 — Envío del job de Flink (Metro Medellín)
#
# Descarga el conector Kafka al JobManager, copia los catálogos CSV al
# TaskManager y envía flink_metro_job.py al cluster.
#
# Uso:
#   bash streaming/run_flink_job.sh
#   WINDOW_MINUTES=2 bash streaming/run_flink_job.sh
#
# Requiere:
#   - docker compose up -d  (flink-jobmanager y flink-taskmanager healthy)
#   - data/paradas.csv y data/estaciones.csv presentes
# =============================================================================
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"

# Parámetros configurables
WINDOW_MINUTES="${WINDOW_MINUTES:-1}"
WATERMARK_DELAY_SECONDS="${WATERMARK_DELAY_SECONDS:-120}"
KAFKA_BROKER="${KAFKA_BROKER:-kafka:9092}"
KAFKA_TOPIC="${KAFKA_TOPIC:-transport-events}"

# Conector Kafka para Flink 1.20
KAFKA_JAR="flink-sql-connector-kafka-3.3.0-1.20.jar"
KAFKA_JAR_URL="https://repo1.maven.org/maven2/org/apache/flink/flink-sql-connector-kafka/3.3.0-1.20/${KAFKA_JAR}"
JAR_REMOTE="/opt/flink/usrlib/${KAFKA_JAR}"

# ── Verificar contenedores ────────────────────────────────────────────────────
echo ""
echo "============================================================"
echo "  ST1630 — Flink Metro Job"
echo "  WINDOW     : ${WINDOW_MINUTES} min"
echo "  WATERMARK  : ${WATERMARK_DELAY_SECONDS}s"
echo "  BROKER     : ${KAFKA_BROKER}"
echo "============================================================"
echo ""

for CONTAINER in flink-jobmanager flink-taskmanager; do
  if ! docker ps --format '{{.Names}}' | grep -q "^${CONTAINER}$"; then
    echo "ERROR: contenedor '${CONTAINER}' no está corriendo."
    echo "       Ejecuta: docker compose up -d"
    exit 1
  fi
done

# ── Paso 1: Python 3 + apache-flink en ambos contenedores ─────────────────────
# apache/flink:1.20.4-java17 no incluye Python. Se instala una vez; las
# siguientes ejecuciones detectan el binario y saltan la instalación.
PYFLINK_VERSION="1.20.0"

install_python() {
  local CONTAINER=$1
  # Verificar si pyflink ya está disponible (instalación previa)
  if docker exec "${CONTAINER}" bash -c "python3 -c 'from pyflink.datastream import StreamExecutionEnvironment'" &>/dev/null; then
    echo "  ${CONTAINER}: PyFlink ya instalado, saltando."
    return
  fi
  echo "  ${CONTAINER}: instalando Python3 + apache-flink ${PYFLINK_VERSION}..."
  # apache/flink:1.20.4-java17 incluye JRE (sin headers de compilación).
  # apache-flink necesita jni.h → instalar JDK headless y copiar headers al JAVA_HOME del contenedor.
  docker exec "${CONTAINER}" bash -c "
    apt-get update -qq 2>/dev/null &&
    apt-get install -y -q python3 python3-pip openjdk-17-jdk-headless 2>/dev/null &&
    mkdir -p /opt/java/openjdk/include &&
    cp -rn /usr/lib/jvm/java-17-openjdk-\$(uname -m | sed 's/aarch64/arm64/;s/x86_64/amd64/')/include/. /opt/java/openjdk/include/ &&
    pip3 install --quiet apache-flink==${PYFLINK_VERSION} &&
    ln -sf /usr/bin/python3 /usr/bin/python &&
    echo 'PyFlink ready'
  "
  echo "  ${CONTAINER}: PyFlink instalado."
}

echo "[1/5] Verificando Python en JobManager y TaskManager..."
install_python flink-jobmanager
install_python flink-taskmanager

# ── Paso 2: Conector Kafka en JobManager y TaskManager ────────────────────────
# El JAR debe estar en /opt/flink/lib/ en AMBOS nodos para que el TaskManager
# pueda cargar el operador KafkaSource en tiempo de ejecución.
echo "[2/5] Verificando conector Kafka..."
docker exec flink-jobmanager mkdir -p /opt/flink/usrlib

if docker exec flink-jobmanager test -f "${JAR_REMOTE}" 2>/dev/null; then
  echo "  ${KAFKA_JAR}: ya existe en JobManager, saltando descarga."
else
  echo "  Descargando ${KAFKA_JAR} (~50 MB)..."
  docker exec flink-jobmanager \
    curl -fsSL "${KAFKA_JAR_URL}" -o "${JAR_REMOTE}"
  echo "  JAR descargado en JobManager: ${JAR_REMOTE}"
fi

# Copiar JAR al TaskManager (requerido para ejecución de operadores)
TM_LIB_JAR="/opt/flink/lib/${KAFKA_JAR}"
if docker exec flink-taskmanager test -f "${TM_LIB_JAR}" 2>/dev/null; then
  echo "  ${KAFKA_JAR}: ya existe en TaskManager, saltando copia."
else
  docker cp flink-jobmanager:"${JAR_REMOTE}" /tmp/"${KAFKA_JAR}"
  docker cp /tmp/"${KAFKA_JAR}" flink-taskmanager:"${TM_LIB_JAR}"
  echo "  JAR copiado a TaskManager: ${TM_LIB_JAR}"
fi

# ── Paso 3: Catálogos CSV en TaskManager ─────────────────────────────────────
# EnrichWithStationIdMap.open() se ejecuta en el TaskManager.
# Los archivos deben estar en el nodo que ejecuta los operadores.
echo "[3/5] Copiando catálogos CSV al TaskManager..."
docker exec flink-taskmanager mkdir -p /opt/flink/data

for CSV_FILE in paradas.csv estaciones.csv; do
  LOCAL_PATH="${PROJECT_ROOT}/data/${CSV_FILE}"
  if [ ! -f "${LOCAL_PATH}" ]; then
    echo "ERROR: no se encontró ${LOCAL_PATH}"
    echo "       Ejecuta primero: python3 data/generate_batch.py"
    exit 1
  fi
  docker cp "${LOCAL_PATH}" "flink-taskmanager:/opt/flink/data/${CSV_FILE}"
  echo "  Copiado: ${CSV_FILE} → flink-taskmanager:/opt/flink/data/"
done

# ── Paso 4: Job Python en JobManager ─────────────────────────────────────────
echo "[4/5] Copiando flink_metro_job.py al JobManager..."
docker cp "${SCRIPT_DIR}/flink_metro_job.py" \
  flink-jobmanager:/opt/flink/usrlib/flink_metro_job.py
echo "  Copiado: flink_metro_job.py → flink-jobmanager:/opt/flink/usrlib/"

# ── Paso 5: Envío del job ─────────────────────────────────────────────────────
echo "[5/5] Enviando job al cluster Flink..."
echo ""
echo "  Las ventanas de ${WINDOW_MINUTES} min cierran con EVENT TIME."
echo "  Inicia el productor en otra terminal para generar eventos:"
echo "    bash streaming/run_producer.sh"
echo ""

# env.add_jars() en el código Python registra el JAR dinámicamente (job level).
# No se necesita --jars aquí.
docker exec \
  -e KAFKA_BROKER="${KAFKA_BROKER}" \
  -e KAFKA_TOPIC="${KAFKA_TOPIC}" \
  -e KAFKA_GROUP_ID="flink-metro-consumer" \
  -e WINDOW_MINUTES="${WINDOW_MINUTES}" \
  -e WATERMARK_DELAY_SECONDS="${WATERMARK_DELAY_SECONDS}" \
  -e PARADAS_CSV_PATH="/opt/flink/data/paradas.csv" \
  -e ESTACIONES_CSV_PATH="/opt/flink/data/estaciones.csv" \
  -e KAFKA_JAR_PATH="/opt/flink/lib/${KAFKA_JAR}" \
  flink-jobmanager \
  flink run \
    --python /opt/flink/usrlib/flink_metro_job.py
