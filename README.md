# ST1630 — Sistema de Datos Híbrido · Metro Medellín

Plataforma de monitoreo del transporte público de Medellín (Metro + cables +
tranvía). Arquitectura híbrida con dos caminos de datos complementarios:

- **Camino Batch**: datos históricos → Lakehouse Bronze → Silver → Gold sobre
  Apache Iceberg + MinIO, procesados con PySpark.
- **Camino Streaming**: eventos en tiempo real → Kafka → Flink → MongoDB.
- **Integración**: cruce batch × streaming con z-score cruzado para detectar
  estaciones que se desvían de su patrón histórico relativo.

Curso: ST1630 Sistemas Intensivos en Datos — Universidad EAFIT, 2026-1.  
Integrantes: Esteban Molina, Miguel Villegas, Sebastián Rodríguez.

---

## Arquitectura

![Diagrama de arquitectura](arquitectura_sistema_hibrido.svg)

---

## Prerrequisitos

| Requisito | Versión mínima | Nota |
|-----------|---------------|------|
| Docker Desktop | 4.28 / Engine 25 | Incluye Compose v2.24+ |
| Docker Compose | 2.4 | Necesario para `condition: service_completed_successfully` |
| RAM disponible | 8 GB | Todos los servicios simultáneos |
| Espacio en disco | 10 GB | Imágenes (~5 GB) + volúmenes de datos |
| Acceso a Internet | — | Primera ejecución: pull de imágenes + JARs |

```bash
docker version          # Engine ≥ 25
docker compose version  # v2.24+
```

---

## Servicios y puertos

| Servicio | Imagen | Puerto host | UI |
|----------|--------|-------------|----|
| **MinIO** (S3 API) | `minio/minio:RELEASE.2025-09-07T16-13-09Z` | `9000` | — |
| **MinIO** (Console) | misma imagen | `9001` | http://localhost:9001 |
| **Iceberg REST Catalog** | `tabulario/iceberg-rest:1.6.0` | `8181` | http://localhost:8181/v1/config |
| **Apache Kafka** | `apache/kafka:4.2.1` | `9092` | — |
| **Flink JobManager** | `apache/flink:1.20.4-java17` | `8081` | http://localhost:8081 |
| **Flink TaskManager** | misma imagen | — | gestionado por JobManager |
| **Spark Master** | `apache/spark:3.5.8-scala2.12-java17-python3-r-ubuntu` | `8080` · `7077` | http://localhost:8080 |
| **Spark Worker** | misma imagen | `8082` | http://localhost:8082 |
| **MongoDB** | `mongo:8.0.23` | `27017` | — |

Credenciales en `.env` (ver `.env.example`).

---

## Tópicos de Kafka

| Tópico | Productor | Consumidor | Descripción |
|--------|-----------|------------|-------------|
| `transport-events` | `streaming/producer.py` | `streaming/flink_metro_job.py` | Eventos arrival/departure de vehículos en tiempo real |

---

## Orden de ejecución completo

### Fase 0 — Infraestructura

```bash
# 1. Configurar variables de entorno
cp .env.example .env
# Editar .env; generar KAFKA_CLUSTER_ID:
python3 -c "import base64, uuid; print(base64.urlsafe_b64encode(uuid.uuid4().bytes).decode().rstrip('='))"

# 2. Levantar todos los servicios
docker compose up -d

# Orden de arranque por depends_on + healthcheck:
#   Nivel 0 (paralelo): minio · kafka · spark-master · mongodb
#   Nivel 1:            minio-init       → minio (healthy)
#   Nivel 2:            iceberg-rest     → minio-init (completed_successfully)
#   Nivel 3:            flink-jobmanager → kafka + iceberg-rest (healthy)
#                       spark-worker     → spark-master (healthy)
#   Nivel 4:            flink-taskmanager → flink-jobmanager (healthy)

# 3. Verificar
docker compose ps                  # todos (healthy); minio-init → Exited (0)
bash scripts/smoke_test.sh         # Kafka · MinIO · Spark+Iceberg end-to-end

# Detener (sin borrar datos):
docker compose down
# Reset completo (borra volúmenes):
docker compose down -v
```

### Fase 1 — Datos batch

```bash
# Generar dataset histórico sintético
python3 data/generate_batch.py
# Salida: data/raw/trips_YYYY-MM-DD.csv (múltiples fechas)
```

### Fase 2 — Lakehouse batch (Bronze → Silver → Gold)

```bash
# Bronze: ingestar datos crudos a Iceberg/MinIO
bash batch/run_bronze.sh
# Primer uso: descarga JARs Iceberg (~105 MB) en spark-master

# Silver: limpiar, tipar y enriquecer
bash batch/run_silver.sh

# Gold: producir tablas analíticas
bash batch/run_gold.sh

# Iceberg features (opcional): time travel, schema evolution, partition evolution
bash batch/run_iceberg_features.sh
```

### Fase 3 — Streaming (Kafka → Flink → MongoDB)

```bash
# Paso 1: inicializar MongoDB (colecciones + índices + TTL)
bash scripts/run_init_mongo.sh

# Paso 2: lanzar job de Flink (instala PyFlink en contenedores en primer uso)
bash streaming/run_flink_job.sh
# Variables opcionales:
# WINDOW_MINUTES=1 WATERMARK_DELAY_SECONDS=120 bash streaming/run_flink_job.sh

# Paso 3: arrancar productor de eventos (en otra terminal)
bash streaming/run_producer.sh
# Ctrl+C para detener

# Paso 4: queries operacionales contra MongoDB
python3 streaming/queries_operacionales.py
```

### Fase 4 — Integración batch × streaming

```bash
# Copiar script al contenedor spark-master (una sola vez)
docker cp integration/batch_streaming_compare.py spark-master:/tmp/

# Ejecutar cruce z-score (requiere Flink corriendo + gold poblado)
docker exec \
  -e MONGO_URI="mongodb://root:rootpassword@mongodb:27017/" \
  -e MINIO_ENDPOINT="http://minio:9000" \
  -e MINIO_KEY="admin" -e MINIO_SECRET="password123" \
  -e ICEBERG_URI="http://iceberg-rest:8181" \
  -e PYTHONPATH="/opt/spark/python:/opt/spark/python/lib/py4j-0.10.9.7-src.zip" \
  -e AWS_REGION="us-east-1" \
  spark-master \
  python3 /tmp/batch_streaming_compare.py
```

---

## Lakehouse — capas Iceberg

### Bronze · `bronze.trips`

Datos crudos ingestados sin transformación desde los CSV del generador.
Particionado por `trip_date`. Preserva todos los campos originales.

### Silver · `silver.trips`

Datos limpios y tipados. Columnas derivadas: `hour_block` (franja de 3 h),
`time_of_day`, `is_peak_hour`, `delay_category`, `trip_direction`, `n_stops`.
Elimina registros con campos obligatorios nulos.

### Gold — tablas analíticas

| Tabla | Pregunta analítica | Granularidad | Métrica clave |
|-------|--------------------|--------------|---------------|
| `gold.estaciones_volumen` | PA1 — ¿Qué estaciones concentran más demanda por franja horaria y día? | `station_id × hour_block × day_of_week` | `avg_passengers`, `total_trips` |
| `gold.retraso_por_linea_mes` | PA2 — ¿Cómo evoluciona el retraso promedio por línea mes a mes? | `line × year × month` | `avg_delay_seconds`, `is_rainy_month` |
| `gold.demanda_rutas` | PA3 — ¿Qué líneas tienen mayor proporción de viajes de alta demanda? | `line × tipo_servicio × hour_block` | `pct_high_demand` (viajes > p90 propio) |

---

## MongoDB — colecciones operacionales

Base de datos: `metro_medellin_ops`

| Colección | Pregunta operacional | Escritura | TTL | Índice principal |
|-----------|---------------------|-----------|-----|-----------------|
| `station_occupancy` | PO1 — ocupación actual por estación | Upsert por `_id=station_id` | 10 min en `last_updated` | `_id` (IDHACK, O(1)) |
| `line_delays` | PO2 — líneas con retraso crítico (`high_delay=true`) | Upsert por `_id=line` | 15 min en `last_updated` | `idx_high_delay` (IXSCAN) |
| `station_alerts` | PO3 — alertas por estación en la última hora | Insert acumulativo | 60 min en `window_end` | `idx_station_window` (station_id + window_end) |

`high_delay = True` cuando `max_delay_seconds > 300` en la ventana actual
(señal basada en el peor vehículo, no en el promedio — el promedio diluye picos).

---

## Integración — método z-score cruzado

Las dos fuentes usan escalas incompatibles (`avg_passengers` batch ≈ 70–180;
`avg_vehicle_occupancy` streaming ≈ 0–1080). La comparación válida es:

```
z_hist(s)   = (avg_passengers(s)        − μ_batch)  / σ_batch
z_stream(s) = (avg_vehicle_occupancy(s) − μ_stream) / σ_stream
Δz(s)       = z_stream(s) − z_hist(s)
```

Δz mide el cambio en la **posición relativa** de una estación respecto a sus
pares. Un desplazamiento uniforme de toda la red produce Δz ≈ 0 en todas las
estaciones; ese nivel global se reporta por separado como contexto (μ_stream
y μ_batch en sus propias escalas, sin restarlos).

---

## Versiones fijadas

| Componente | Versión |
|-----------|---------|
| Apache Flink | `1.20.4-java17` |
| flink-sql-connector-kafka | `3.3.0-1.20` |
| PyFlink (pip) | `1.20.0` |
| Apache Kafka | `4.2.1` (KRaft, sin ZooKeeper) |
| Apache Spark | `3.5.8-scala2.12-java17-python3-r-ubuntu` |
| iceberg-spark-runtime | `3.5_2.12-1.10.1` |
| iceberg-aws-bundle | `1.10.1` |
| tabulario/iceberg-rest | `1.6.0` |
| MinIO | `RELEASE.2025-09-07T16-13-09Z` |
| MongoDB | `8.0.23` |
