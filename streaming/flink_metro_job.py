"""
streaming/flink_metro_job.py
Job de Flink — Metro de Medellín (ST1630).

Consume el tópico transport-events de Kafka, asigna event time con watermarks
tolerantes a desorden, enriquece stop_id → station_id con catálogo estático, y
produce tres flujos agregados con ventanas tumbling que se persisten en MongoDB.

── Flujo ─────────────────────────────────────────────────────────────────────

  Kafka Source (transport-events, SimpleStringSchema)
      │
      ▼  ParseEventMap  → Row[event_id, event_type, stop_id, line,
      │                        vehicle_id, passenger_count, delay_seconds, timestamp]
      │
      ▼  WatermarkStrategy — BoundedOutOfOrderness(WATERMARK_DELAY_SECONDS)
      │  TimestampAssigner: parsea campo `timestamp` ISO8601 → epoch ms
      │
      ├── branch A (raw) ────────────────────────────────────────
      │   keyBy(line) → TumblingEventTimeWindow(WINDOW_MINUTES)
      │   DelayAggFunction + DelayWindowFunction
      │   → MongoDB line_delays (upsert por _id=line)
      │   → PrintSink "[PO2]" (confirmación en log)
      │
      └── branch B (enriquecido) ─────────────────────────────────
          EnrichWithStationIdMap: open() carga paradas.csv + estaciones.csv
          Row + [station_id, stop_name]
          │
          ├── filter arrivals + departures
          │   keyBy(station_id) → TumblingEventTimeWindow(WINDOW_MINUTES)
          │   OccupancyAggFunction + OccupancyWindowFunction
          │   → MongoDB station_occupancy (upsert por _id=station_id)
          │   → PrintSink "[PO1]" (confirmación en log)
          │
          └── filter delay_alert + capacity_alert
              keyBy(station_id) → TumblingEventTimeWindow(WINDOW_MINUTES)
              AlertAggFunction + AlertWindowFunction
              → MongoDB station_alerts (insert, acumula historial 1h)
              → PrintSink "[PO3]" (confirmación en log)

── Semántica de tiempo ────────────────────────────────────────────────────────

  EVENT TIME sobre el campo `timestamp` del evento (UTC real de publicación).
  TIME_SCALE solo afecta cuántos eventos caen en la ventana, NO su duración.

── Watermark ─────────────────────────────────────────────────────────────────

  BoundedOutOfOrderness(WATERMARK_DELAY_SECONDS=120):
    • Con LATE_EVENT_PROBABILITY=0.0 (default): eventos en orden, watermarks
      avanzan sin retraso.
    • Con LATE_EVENT_PROBABILITY>0: backdating máximo de 120 s (productor),
      cubierto exactamente por la tolerancia.
    • Eventos que llegan más tarde del umbral: descartados (numLateRecordsDropped).
    • Confirmación: un evento con timestamp T cae en la ventana
      [floor(T/W)*W, floor(T/W)*W + W), sin importar cuándo llega (si T está
      dentro de la tolerancia). El processing time no influye en la asignación.

── MongoDB ───────────────────────────────────────────────────────────────────

  Base de datos : metro_medellin_ops
  Colecciones   :
    station_occupancy  PO1 — upsert por _id=station_id al cierre de ventana
    line_delays        PO2 — upsert por _id=line al cierre de ventana
    station_alerts     PO3 — insert por ventana, acumula historial 1h (TTL)

  La escritura se hace dentro de ProcessWindowFunction.process() usando pymongo.
  No se necesita un JAR de conector MongoDB adicional.

── Variables de entorno ──────────────────────────────────────────────────────

  KAFKA_BROKER              kafka:9092
  KAFKA_TOPIC               transport-events
  KAFKA_GROUP_ID            flink-metro-consumer
  WINDOW_MINUTES            1       (ventana tumbling en minutos reales)
  WATERMARK_DELAY_SECONDS   120     (tolerancia a desorden)
  PARADAS_CSV_PATH          /opt/flink/data/paradas.csv
  ESTACIONES_CSV_PATH       /opt/flink/data/estaciones.csv
  MONGO_URI                 mongodb://root:rootpassword@mongodb:27017/
  MONGO_DB                  metro_medellin_ops

── JARs requeridos ───────────────────────────────────────────────────────────

  flink-sql-connector-kafka-3.3.0-1.20.jar   (en /opt/flink/lib/)
"""
import csv
import json
import os
from datetime import datetime, timezone
from typing import Iterator

from pyflink.common import Duration, Row, Types, WatermarkStrategy
from pyflink.common.watermark_strategy import TimestampAssigner
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import (
    KafkaOffsetsInitializer,
    KafkaSource,
)
from pyflink.datastream.functions import (
    AggregateFunction,
    MapFunction,
    ProcessWindowFunction,
)
from pyflink.datastream.window import TumblingEventTimeWindows, Time
from pyflink.common.serialization import SimpleStringSchema

# ── Configuración por entorno ──────────────────────────────────────────────────
KAFKA_BROKER     = os.environ.get("KAFKA_BROKER",            "kafka:9092")
KAFKA_TOPIC      = os.environ.get("KAFKA_TOPIC",             "transport-events")
KAFKA_GROUP_ID   = os.environ.get("KAFKA_GROUP_ID",          "flink-metro-consumer")
WINDOW_MINUTES   = int(os.environ.get("WINDOW_MINUTES",      "1"))
WATERMARK_DELAY  = int(os.environ.get("WATERMARK_DELAY_SECONDS", "120"))
PARADAS_PATH     = os.environ.get("PARADAS_CSV_PATH",        "/opt/flink/data/paradas.csv")
ESTACIONES_PATH  = os.environ.get("ESTACIONES_CSV_PATH",     "/opt/flink/data/estaciones.csv")
MONGO_URI        = os.environ.get("MONGO_URI",               "mongodb://root:rootpassword@mongodb:27017/")
MONGO_DB         = os.environ.get("MONGO_DB",                "metro_medellin_ops")
KAFKA_JAR_PATH   = os.environ.get("KAFKA_JAR_PATH",          "/opt/flink/lib/flink-sql-connector-kafka-3.3.0-1.20.jar")

HIGH_DELAY_THRESHOLD = 300   # segundos

# ── Tipos Row ──────────────────────────────────────────────────────────────────
RAW_EVENT_TYPE = Types.ROW_NAMED(
    ["event_id", "event_type", "stop_id", "line",
     "vehicle_id", "passenger_count", "delay_seconds", "timestamp"],
    [Types.STRING(), Types.STRING(), Types.STRING(), Types.STRING(),
     Types.STRING(), Types.INT(), Types.INT(), Types.STRING()],
)

ENRICHED_EVENT_TYPE = Types.ROW_NAMED(
    ["event_id", "event_type", "stop_id", "line",
     "vehicle_id", "passenger_count", "delay_seconds", "timestamp",
     "station_id", "stop_name"],
    [Types.STRING(), Types.STRING(), Types.STRING(), Types.STRING(),
     Types.STRING(), Types.INT(), Types.INT(), Types.STRING(),
     Types.STRING(), Types.STRING()],
)


# ── Helpers ────────────────────────────────────────────────────────────────────
def _parse_window_dt(iso_str: str) -> datetime:
    """Convierte "2026-05-18T14:30:00Z" a datetime UTC para MongoDB BSON Date."""
    return datetime.strptime(iso_str, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)


# ══════════════════════════════════════════════════════════════
# Paso 1 — Parseo del JSON de Kafka
# ══════════════════════════════════════════════════════════════
class ParseEventMap(MapFunction):
    """Deserializa el JSON del productor a un Row tipado."""
    def map(self, value: str) -> Row:
        try:
            d = json.loads(value)
            return Row(
                event_id=d.get("event_id", ""),
                event_type=d.get("event_type", "_invalid"),
                stop_id=d.get("stop_id", ""),
                line=d.get("line", ""),
                vehicle_id=d.get("vehicle_id", ""),
                passenger_count=int(d.get("passenger_count", 0)),
                delay_seconds=int(d.get("delay_seconds", 0)),
                timestamp=d.get("timestamp", ""),
            )
        except Exception:
            return Row(
                event_id="", event_type="_invalid", stop_id="", line="",
                vehicle_id="", passenger_count=0, delay_seconds=0, timestamp="",
            )


# ══════════════════════════════════════════════════════════════
# Paso 2 — Asignación de event time
# ══════════════════════════════════════════════════════════════
class EventTimestampAssigner(TimestampAssigner):
    """Extrae el campo `timestamp` ISO8601 (UTC) → epoch ms.

    Formato del productor: "2026-05-18T14:32:59.954Z"

    Garantía sobre eventos tardíos:
      Un evento con timestamp T se asigna a la ventana cuyo rango cubre T,
      independientemente del processing time en que llega. El watermark =
      max_seen_ts - WATERMARK_DELAY_SECONDS protege contra desorden de hasta
      ese margen. Eventos más tardíos se descartan (late records).
    """
    def extract_timestamp(self, value: Row, record_timestamp: int) -> int:
        try:
            ts_str = value["timestamp"]
            dt = datetime.strptime(ts_str, "%Y-%m-%dT%H:%M:%S.%fZ")
            dt = dt.replace(tzinfo=timezone.utc)
            return int(dt.timestamp() * 1000)
        except Exception:
            return record_timestamp


# ══════════════════════════════════════════════════════════════
# Paso 3 — Enriquecimiento stop_id → station_id
# ══════════════════════════════════════════════════════════════
class EnrichWithStationIdMap(MapFunction):
    """Carga paradas.csv + estaciones.csv en open() y añade station_id/stop_name.
    Estrategia: dict in-memory de 57 entradas (catálogo estático, no broadcast).
    En PyFlink 1.20 MapFunction hereda Function.open() — no existe RichMapFunction.
    """
    def __init__(self, paradas_path: str, estaciones_path: str):
        self._paradas_path    = paradas_path
        self._estaciones_path = estaciones_path
        self._stop_map        = {}

    def open(self, runtime_context):
        station_names = {}
        with open(self._estaciones_path, encoding="utf-8") as f:
            for row in csv.DictReader(f):
                station_names[row["station_id"]] = row["nombre"]
        with open(self._paradas_path, encoding="utf-8") as f:
            for row in csv.DictReader(f):
                sid = row["station_id"]
                self._stop_map[row["stop_id"]] = {
                    "station_id": sid,
                    "stop_name":  station_names.get(sid, sid),
                }

    def map(self, value: Row) -> Row:
        info = self._stop_map.get(
            value["stop_id"],
            {"station_id": value["stop_id"], "stop_name": value["stop_id"]},
        )
        return Row(
            event_id=value["event_id"],
            event_type=value["event_type"],
            stop_id=value["stop_id"],
            line=value["line"],
            vehicle_id=value["vehicle_id"],
            passenger_count=value["passenger_count"],
            delay_seconds=value["delay_seconds"],
            timestamp=value["timestamp"],
            station_id=info["station_id"],
            stop_name=info["stop_name"],
        )


# ══════════════════════════════════════════════════════════════
# PO1 — Ocupación de vehículos por estación
# MongoDB: station_occupancy  (upsert por _id = station_id)
# ══════════════════════════════════════════════════════════════
class OccupancyAggFunction(AggregateFunction):
    def create_accumulator(self):
        # (max_occ, sum_occ, count, station_id, stop_name, lines_set_json)
        return (0, 0, 0, "", "", "[]")

    def add(self, value: Row, acc: tuple) -> tuple:
        pax   = value["passenger_count"]
        lines = set(json.loads(acc[5]))
        lines.add(value["line"])
        return (max(acc[0], pax), acc[1] + pax, acc[2] + 1,
                value["station_id"], value["stop_name"], json.dumps(sorted(lines)))

    def get_result(self, acc: tuple) -> tuple:
        return acc

    def merge(self, a: tuple, b: tuple) -> tuple:
        lines = set(json.loads(a[5])) | set(json.loads(b[5]))
        return (max(a[0], b[0]), a[1] + b[1], a[2] + b[2],
                a[3] or b[3], a[4] or b[4], json.dumps(sorted(lines)))


class OccupancyWindowFunction(ProcessWindowFunction):
    """Al cierre de cada ventana: escribe upsert en station_occupancy y emite JSON."""

    def __init__(self, mongo_uri: str, db_name: str):
        self._mongo_uri = mongo_uri
        self._db_name   = db_name
        self._coll      = None

    def open(self, runtime_context):
        from pymongo import MongoClient
        self._coll = MongoClient(self._mongo_uri)[self._db_name]["station_occupancy"]

    def process(self, key: str, context, elements: Iterator) -> Iterator[str]:
        for acc in elements:
            if acc[2] == 0:
                return
            now     = datetime.now(tz=timezone.utc)
            w_start = datetime.fromtimestamp(context.window().start / 1000, tz=timezone.utc)
            w_end   = datetime.fromtimestamp(context.window().end   / 1000, tz=timezone.utc)
            doc = {
                "_id":                   key,
                "station_id":            acc[3] or key,
                "stop_name":             acc[4],
                "lines_serving":         json.loads(acc[5]),
                "window_start":          w_start,
                "window_end":            w_end,
                "max_vehicle_occupancy": acc[0],
                "avg_vehicle_occupancy": round(acc[1] / acc[2], 2),
                "event_count":           acc[2],
                "last_updated":          now,
            }
            # Upsert: estado actual de la estación (reemplaza ventana anterior)
            self._coll.replace_one({"_id": key}, doc, upsert=True)
            # Emit JSON para log de confirmación
            log_doc = dict(doc)
            log_doc["window_start"] = w_start.strftime("%Y-%m-%dT%H:%M:%SZ")
            log_doc["window_end"]   = w_end.strftime("%Y-%m-%dT%H:%M:%SZ")
            log_doc["last_updated"] = now.strftime("%Y-%m-%dT%H:%M:%SZ")
            yield json.dumps(log_doc)


# ══════════════════════════════════════════════════════════════
# PO2 — Retraso acumulado por línea
# MongoDB: line_delays  (upsert por _id = line)
# ══════════════════════════════════════════════════════════════
class DelayAggFunction(AggregateFunction):
    def create_accumulator(self):
        # (sum_delay, max_delay, count, vehicles_delayed, line, tipo_servicio)
        return (0, 0, 0, 0, "", "")

    def add(self, value: Row, acc: tuple) -> tuple:
        d      = value["delay_seconds"]
        # vehicles_delayed: sólo vehículos con retraso operacionalmente significativo
        is_del = 1 if d > HIGH_DELAY_THRESHOLD else 0
        _CABLE = {"K", "J", "L", "H", "M", "P"}
        _TRANV = {"TA"}
        line   = value["line"]
        svc    = "cable" if line in _CABLE else ("tranvia" if line in _TRANV else "metro")
        return (acc[0] + d, max(acc[1], d), acc[2] + 1, acc[3] + is_del, line, svc)

    def get_result(self, acc: tuple) -> tuple:
        return acc

    def merge(self, a: tuple, b: tuple) -> tuple:
        return (a[0] + b[0], max(a[1], b[1]), a[2] + b[2], a[3] + b[3],
                a[4] or b[4], a[5] or b[5])


class DelayWindowFunction(ProcessWindowFunction):
    """Al cierre de cada ventana: escribe upsert en line_delays y emite JSON."""

    def __init__(self, mongo_uri: str, db_name: str):
        self._mongo_uri = mongo_uri
        self._db_name   = db_name
        self._coll      = None

    def open(self, runtime_context):
        from pymongo import MongoClient
        self._coll = MongoClient(self._mongo_uri)[self._db_name]["line_delays"]

    def process(self, key: str, context, elements: Iterator) -> Iterator[str]:
        for acc in elements:
            if acc[2] == 0:
                return
            avg_delay = round(acc[0] / acc[2], 2)
            now     = datetime.now(tz=timezone.utc)
            w_start = datetime.fromtimestamp(context.window().start / 1000, tz=timezone.utc)
            w_end   = datetime.fromtimestamp(context.window().end   / 1000, tz=timezone.utc)
            doc = {
                "_id":               key,
                "line":              acc[4] or key,
                "tipo_servicio":     acc[5],
                "window_start":      w_start,
                "window_end":        w_end,
                "avg_delay_seconds": avg_delay,
                "max_delay_seconds": acc[1],
                "vehicles_seen":     acc[2],
                "vehicles_delayed":  acc[3],
                # high_delay = True si al menos un vehículo superó 300s de retraso.
                # Usar max_delay (no avg): el avg diluye picos; una línea con 1
                # vehículo en 600s entre 50 normales ya es operacionalmente crítica.
                "high_delay":        acc[1] > HIGH_DELAY_THRESHOLD,
                "last_updated":      now,
            }
            self._coll.replace_one({"_id": key}, doc, upsert=True)
            log_doc = dict(doc)
            log_doc["window_start"] = w_start.strftime("%Y-%m-%dT%H:%M:%SZ")
            log_doc["window_end"]   = w_end.strftime("%Y-%m-%dT%H:%M:%SZ")
            log_doc["last_updated"] = now.strftime("%Y-%m-%dT%H:%M:%SZ")
            yield json.dumps(log_doc)


# ══════════════════════════════════════════════════════════════
# PO3 — Conteo de alertas por estación
# MongoDB: station_alerts  (insert — acumula historial 1h via TTL)
# ══════════════════════════════════════════════════════════════
class AlertAggFunction(AggregateFunction):
    def create_accumulator(self):
        # (delay_alerts, capacity_alerts, station_id, stop_name, line)
        return (0, 0, "", "", "")

    def add(self, value: Row, acc: tuple) -> tuple:
        is_delay    = 1 if value["event_type"] == "delay_alert"    else 0
        is_capacity = 1 if value["event_type"] == "capacity_alert" else 0
        return (acc[0] + is_delay, acc[1] + is_capacity,
                value["station_id"], value["stop_name"], value["line"])

    def get_result(self, acc: tuple) -> tuple:
        return acc

    def merge(self, a: tuple, b: tuple) -> tuple:
        return (a[0] + b[0], a[1] + b[1],
                a[2] or b[2], a[3] or b[3], a[4] or b[4])


class AlertWindowFunction(ProcessWindowFunction):
    """Al cierre de cada ventana: inserta documento en station_alerts y emite JSON.
    PO3 usa INSERT (no upsert) para acumular historial de la última hora.
    El TTL de 3600 s en window_end expira documentos automáticamente.
    """

    def __init__(self, mongo_uri: str, db_name: str):
        self._mongo_uri = mongo_uri
        self._db_name   = db_name
        self._coll      = None

    def open(self, runtime_context):
        from pymongo import MongoClient
        self._coll = MongoClient(self._mongo_uri)[self._db_name]["station_alerts"]

    def process(self, key: str, context, elements: Iterator) -> Iterator[str]:
        for acc in elements:
            total   = acc[0] + acc[1]
            w_start = datetime.fromtimestamp(context.window().start / 1000, tz=timezone.utc)
            w_end   = datetime.fromtimestamp(context.window().end   / 1000, tz=timezone.utc)
            doc = {
                "station_id":      acc[2] or key,
                "stop_name":       acc[3],
                "line":            acc[4],
                "window_start":    w_start,
                "window_end":      w_end,
                "delay_alerts":    acc[0],
                "capacity_alerts": acc[1],
                "total_alerts":    total,
            }
            # Insert: cada ventana es un documento nuevo (historial rolling).
            # Construir log_doc ANTES de insert_one: pymongo muta 'doc' añadiendo
            # _id: ObjectId(...) in-place, que no es serializable con json.dumps.
            log_doc = {
                "station_id":      doc["station_id"],
                "stop_name":       doc["stop_name"],
                "line":            doc["line"],
                "window_start":    w_start.strftime("%Y-%m-%dT%H:%M:%SZ"),
                "window_end":      w_end.strftime("%Y-%m-%dT%H:%M:%SZ"),
                "delay_alerts":    doc["delay_alerts"],
                "capacity_alerts": doc["capacity_alerts"],
                "total_alerts":    doc["total_alerts"],
            }
            self._coll.insert_one(doc)
            yield json.dumps(log_doc)


# ══════════════════════════════════════════════════════════════
# Main — wiring del DAG
# ══════════════════════════════════════════════════════════════
def main():
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)

    # Registrar el conector Kafka (necesario cuando el JAR se copió post-startup)
    env.add_jars(f"file://{KAFKA_JAR_PATH}")

    print(f"[config] KAFKA_BROKER={KAFKA_BROKER}  TOPIC={KAFKA_TOPIC}")
    print(f"[config] WINDOW={WINDOW_MINUTES} min  WATERMARK={WATERMARK_DELAY}s")
    print(f"[config] MONGO_URI={MONGO_URI}  DB={MONGO_DB}")

    # ── Kafka Source ─────────────────────────────────────────────────────────
    kafka_source = (
        KafkaSource.builder()
        .set_bootstrap_servers(KAFKA_BROKER)
        .set_topics(KAFKA_TOPIC)
        .set_group_id(KAFKA_GROUP_ID)
        .set_starting_offsets(KafkaOffsetsInitializer.latest())
        .set_value_only_deserializer(SimpleStringSchema())
        .build()
    )

    # WatermarkStrategy: BoundedOutOfOrderness sobre el timestamp del evento.
    # max_event_ts - WATERMARK_DELAY_SECONDS = watermark corriente.
    watermark_strategy = (
        WatermarkStrategy
        .for_bounded_out_of_orderness(Duration.of_seconds(WATERMARK_DELAY))
        .with_timestamp_assigner(EventTimestampAssigner())
    )

    raw_strings = env.from_source(
        kafka_source,
        WatermarkStrategy.no_watermarks(),
        "Kafka transport-events",
    )

    parsed = (
        raw_strings
        .map(ParseEventMap(), output_type=RAW_EVENT_TYPE)
        .filter(lambda e: e["event_type"] != "_invalid")
    )

    # Asignar event time + watermarks DESPUÉS del parseo
    timestamped = parsed.assign_timestamps_and_watermarks(watermark_strategy)

    # ── Branch A: PO2 — retraso por línea ───────────────────────────────────
    (
        timestamped
        .filter(lambda e: e["delay_seconds"] >= 0)
        .key_by(lambda e: e["line"], key_type=Types.STRING())
        .window(TumblingEventTimeWindows.of(Time.minutes(WINDOW_MINUTES)))
        .aggregate(
            DelayAggFunction(),
            window_function=DelayWindowFunction(MONGO_URI, MONGO_DB),
            accumulator_type=Types.TUPLE([
                Types.LONG(), Types.INT(), Types.INT(), Types.INT(),
                Types.STRING(), Types.STRING(),
            ]),
            output_type=Types.STRING(),
        )
        .print().name("[PO2] line_delays → MongoDB")
    )

    # ── Branch B: enriquecimiento stop_id → station_id ───────────────────────
    enriched = timestamped.map(
        EnrichWithStationIdMap(PARADAS_PATH, ESTACIONES_PATH),
        output_type=ENRICHED_EVENT_TYPE,
    )

    # ── Branch B1: PO1 — ocupación por estación ──────────────────────────────
    (
        enriched
        .filter(lambda e: e["event_type"] in ("arrival", "departure"))
        .key_by(lambda e: e["station_id"], key_type=Types.STRING())
        .window(TumblingEventTimeWindows.of(Time.minutes(WINDOW_MINUTES)))
        .aggregate(
            OccupancyAggFunction(),
            window_function=OccupancyWindowFunction(MONGO_URI, MONGO_DB),
            accumulator_type=Types.TUPLE([
                Types.INT(), Types.LONG(), Types.INT(),
                Types.STRING(), Types.STRING(), Types.STRING(),
            ]),
            output_type=Types.STRING(),
        )
        .print().name("[PO1] station_occupancy → MongoDB")
    )

    # ── Branch B2: PO3 — alertas por estación ────────────────────────────────
    (
        enriched
        .filter(lambda e: e["event_type"] in ("delay_alert", "capacity_alert"))
        .key_by(lambda e: e["station_id"], key_type=Types.STRING())
        .window(TumblingEventTimeWindows.of(Time.minutes(WINDOW_MINUTES)))
        .aggregate(
            AlertAggFunction(),
            window_function=AlertWindowFunction(MONGO_URI, MONGO_DB),
            accumulator_type=Types.TUPLE([
                Types.INT(), Types.INT(),
                Types.STRING(), Types.STRING(), Types.STRING(),
            ]),
            output_type=Types.STRING(),
        )
        .print().name("[PO3] station_alerts → MongoDB")
    )

    env.execute("flink-metro-job")


if __name__ == "__main__":
    main()
