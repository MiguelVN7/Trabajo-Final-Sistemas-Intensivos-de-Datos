"""
scripts/init_mongo.py
Inicialización de MongoDB — Metro de Medellín (ST1630).

Crea la base de datos operacional `metro_medellin_ops` con las tres
colecciones y sus índices optimizados para PO1, PO2 y PO3.

Base de datos: metro_medellin_ops
Colecciones:
  station_occupancy   PO1 — ocupación estimada de cada estación (estado actual)
  line_delays         PO2 — retraso acumulado por línea (estado actual)
  station_alerts      PO3 — historial de alertas por estación en última hora

Criterios de diseño:
  • PO1 y PO2 usan upsert por _id → documentos de estado actual, colección acotada.
  • PO3 usa insert acumulativo → ventanas de alertas, acotada con TTL.
  • Todos los lookups son O(1) por clave o range-scan con cobertura de índice.
  • TTL indexes garantizan bounded growth sin mantenimiento manual.

Variables de entorno:
  MONGO_URI    URI completa  (default: mongodb://root:rootpassword@localhost:27017/)
  MONGO_HOST   Solo host     (alternativa a URI completa; default: localhost)
  MONGO_PORT   Puerto        (default: 27017)
  MONGO_USER   Usuario       (default: root)
  MONGO_PASS   Contraseña    (default: rootpassword)

Uso:
  python3 scripts/init_mongo.py                      # contra localhost
  MONGO_HOST=mongodb python3 scripts/init_mongo.py   # contra contenedor
"""
import os
import sys
from datetime import datetime, timezone, timedelta

try:
    from pymongo import MongoClient, ASCENDING, DESCENDING
    from pymongo.errors import CollectionInvalid, OperationFailure
except ImportError:
    print("ERROR: pymongo no está instalado.")
    print("       pip install pymongo  o  pip3 install pymongo")
    sys.exit(1)

# ── Conexión ──────────────────────────────────────────────────────────────────
def build_uri() -> str:
    if "MONGO_URI" in os.environ:
        return os.environ["MONGO_URI"]
    host = os.environ.get("MONGO_HOST", "localhost")
    port = os.environ.get("MONGO_PORT", "27017")
    user = os.environ.get("MONGO_USER", "root")
    pwd  = os.environ.get("MONGO_PASS", "rootpassword")
    return f"mongodb://{user}:{pwd}@{host}:{port}/"

DB_NAME = "metro_medellin_ops"
SEP     = "─" * 58


# ── Definiciones de colecciones e índices ─────────────────────────────────────

COLLECTIONS = {

    # ── PO1: Ocupación estimada por estación ───────────────────────────────
    # "Ocupación" = carga media de los vehículos que pasan por la estación
    # en la última ventana. Proxy de congestión del servicio, no aforo físico.
    # Flink hace upsert ({_id: station_id}) al cierre de cada ventana tumbling.
    # TTL de 10 min: si Flink se detiene, los documentos expiran solos.
    #
    # Documento ejemplo:
    # {
    #   "_id": "EST-004",
    #   "station_id": "EST-004",
    #   "stop_name": "Acevedo",
    #   "lines_serving": ["A", "K"],
    #   "window_start": ISODate("2026-05-18T14:30:00Z"),
    #   "window_end":   ISODate("2026-05-18T14:35:00Z"),
    #   "max_vehicle_occupancy": 847,
    #   "avg_vehicle_occupancy": 612.4,   ← media de passengers_on_board de vehículos
    #   "event_count": 17,
    #   "last_updated": ISODate("2026-05-18T14:34:23Z")
    # }
    "station_occupancy": {
        "validator": {
            "$jsonSchema": {
                "bsonType": "object",
                "required": ["_id", "station_id", "window_end", "last_updated"],
                "properties": {
                    "_id":                    {"bsonType": "string"},
                    "station_id":             {"bsonType": "string"},
                    "stop_name":              {"bsonType": "string"},
                    "lines_serving":          {"bsonType": "array"},
                    "window_start":           {"bsonType": "date"},
                    "window_end":             {"bsonType": "date"},
                    "max_vehicle_occupancy":  {"bsonType": ["int", "double"]},
                    "avg_vehicle_occupancy":  {"bsonType": ["int", "double"]},
                    "event_count":            {"bsonType": ["int", "long"]},
                    "last_updated":           {"bsonType": "date"},
                },
            }
        },
        "indexes": [
            # TTL: expira 10 min tras la última actualización (2 ventanas de 5 min)
            {
                "keys":   [("last_updated", ASCENDING)],
                "opts":   {"name": "ttl_last_updated", "expireAfterSeconds": 600},
            },
        ],
    },

    # ── PO2: Retraso acumulado por línea ───────────────────────────────────
    # Estado actual de cada línea: avg_delay en la última ventana, flag high_delay.
    # Flink hace upsert ({_id: line}) al cierre de cada ventana tumbling.
    # TTL de 15 min: 3 ventanas de 5 min sin actualización → dato obsoleto.
    #
    # Documento ejemplo:
    # {
    #   "_id": "A",
    #   "line": "A",
    #   "tipo_servicio": "metro",
    #   "window_start": ISODate("2026-05-18T14:30:00Z"),
    #   "window_end":   ISODate("2026-05-18T14:35:00Z"),
    #   "avg_delay_seconds": 342,
    #   "max_delay_seconds": 890,
    #   "vehicles_seen": 8,
    #   "vehicles_delayed": 3,
    #   "high_delay": true,          ← avg_delay_seconds > 300
    #   "last_updated": ISODate("2026-05-18T14:34:23Z")
    # }
    "line_delays": {
        "validator": {
            "$jsonSchema": {
                "bsonType": "object",
                "required": ["_id", "line", "window_end", "last_updated"],
                "properties": {
                    "_id":               {"bsonType": "string"},
                    "line":              {"bsonType": "string"},
                    "tipo_servicio":     {"bsonType": "string"},
                    "window_start":      {"bsonType": "date"},
                    "window_end":        {"bsonType": "date"},
                    "avg_delay_seconds": {"bsonType": ["int", "double"]},
                    "max_delay_seconds": {"bsonType": ["int", "double"]},
                    "vehicles_seen":     {"bsonType": ["int", "long"]},
                    "vehicles_delayed":  {"bsonType": ["int", "long"]},
                    "high_delay":        {"bsonType": "bool"},
                    "last_updated":      {"bsonType": "date"},
                },
            }
        },
        "indexes": [
            # PO2 query: find({high_delay: true}) — evita scan en colección de 9 docs
            {
                "keys": [("high_delay", ASCENDING)],
                "opts": {"name": "idx_high_delay"},
            },
            # TTL: expira 15 min (3 ventanas) sin actualización
            {
                "keys": [("last_updated", ASCENDING)],
                "opts": {"name": "ttl_last_updated", "expireAfterSeconds": 900},
            },
        ],
    },

    # ── PO3: Historial de alertas por estación (última hora) ───────────────
    # Cada documento = una estación × una ventana de 5 min con sus alertas.
    # Flink hace INSERT (acumula); la colección nunca se sobreescribe.
    # TTL de 3600 s: los documentos expiran exactamente 1 hora tras su window_end.
    # En steady-state: ≤ 49 estaciones × 12 ventanas/hora = ≤ 588 documentos.
    #
    # Query PO3:
    #   db.station_alerts.aggregate([
    #     { $match: { station_id: "EST-004",
    #                 window_end: { $gte: new Date(Date.now()-3600000) } } },
    #     { $group: { _id: "$station_id",
    #                 delay_alerts:    { $sum: "$delay_alerts" },
    #                 capacity_alerts: { $sum: "$capacity_alerts" },
    #                 total_alerts:    { $sum: "$total_alerts" } } }
    #   ])
    #
    # Documento ejemplo:
    # {
    #   "_id": ObjectId("..."),
    #   "station_id": "EST-004",
    #   "stop_name": "Acevedo",
    #   "line": "A",
    #   "window_start": ISODate("2026-05-18T14:30:00Z"),
    #   "window_end":   ISODate("2026-05-18T14:35:00Z"),
    #   "delay_alerts": 2,
    #   "capacity_alerts": 1,
    #   "total_alerts": 3
    # }
    "station_alerts": {
        "validator": {
            "$jsonSchema": {
                "bsonType": "object",
                "required": ["station_id", "window_end"],
                "properties": {
                    "station_id":      {"bsonType": "string"},
                    "stop_name":       {"bsonType": "string"},
                    "line":            {"bsonType": "string"},
                    "window_start":    {"bsonType": "date"},
                    "window_end":      {"bsonType": "date"},
                    "delay_alerts":    {"bsonType": ["int", "long"]},
                    "capacity_alerts": {"bsonType": ["int", "long"]},
                    "total_alerts":    {"bsonType": ["int", "long"]},
                },
            }
        },
        "indexes": [
            # Cubre el $match de PO3: station_id (equality) + window_end (range)
            {
                "keys": [("station_id", ASCENDING), ("window_end", DESCENDING)],
                "opts": {"name": "idx_station_window"},
            },
            # TTL: documentos expiran 1 hora después de su window_end
            # NOTA: TTL index debe ser campo único (no compound).
            {
                "keys": [("window_end", ASCENDING)],
                "opts": {"name": "ttl_window_end", "expireAfterSeconds": 3600},
            },
        ],
    },
}


# ── Helpers ───────────────────────────────────────────────────────────────────

def ensure_collection(db, name: str, definition: dict) -> str:
    """Crea la colección con validación si no existe; si ya existe, no falla."""
    try:
        db.create_collection(
            name,
            validator=definition.get("validator"),
            validationLevel="moderate",   # warn but don't reject on existing docs
            validationAction="warn",
        )
        return "creada"
    except CollectionInvalid:
        return "ya existía"


def ensure_indexes(collection, index_defs: list) -> list:
    """Crea los índices definidos; ignora si ya existen con el mismo nombre."""
    results = []
    existing = set(collection.index_information().keys())
    for idx in index_defs:
        name = idx["opts"].get("name", "")
        if name in existing:
            results.append(f"    {name}: ya existía")
            continue
        try:
            collection.create_index(idx["keys"], **idx["opts"])
            ttl = idx["opts"].get("expireAfterSeconds")
            suffix = f" (TTL {ttl}s)" if ttl else ""
            results.append(f"    {name}{suffix}: creado")
        except OperationFailure as e:
            results.append(f"    {name}: ERROR — {e}")
    return results


def insert_sample_docs(db) -> None:
    """Inserta documentos de ejemplo para verificar que los schemas validan."""
    now = datetime.now(tz=timezone.utc)
    w_start = now.replace(second=0, microsecond=0) - timedelta(minutes=1)
    w_end   = w_start + timedelta(minutes=1)

    # station_occupancy — 2 estaciones de muestra
    for station_id, name, lines in [("EST-004", "Acevedo", ["A", "K"]),
                                     ("EST-011", "San Antonio", ["A", "B"])]:
        db.station_occupancy.replace_one(
            {"_id": station_id},
            {
                "_id":                   station_id,
                "station_id":            station_id,
                "stop_name":             name,
                "lines_serving":         lines,
                "window_start":          w_start,
                "window_end":            w_end,
                "max_vehicle_occupancy": 0,
                "avg_vehicle_occupancy": 0.0,
                "event_count":           0,
                "last_updated":          now,
            },
            upsert=True,
        )

    # line_delays — 2 líneas de muestra
    for line, svc in [("A", "metro"), ("K", "cable")]:
        db.line_delays.replace_one(
            {"_id": line},
            {
                "_id":               line,
                "line":              line,
                "tipo_servicio":     svc,
                "window_start":      w_start,
                "window_end":        w_end,
                "avg_delay_seconds": 0.0,
                "max_delay_seconds": 0,
                "vehicles_seen":     0,
                "vehicles_delayed":  0,
                "high_delay":        False,
                "last_updated":      now,
            },
            upsert=True,
        )

    # station_alerts — 1 ventana de muestra
    db.station_alerts.insert_one({
        "station_id":      "EST-004",
        "stop_name":       "Acevedo",
        "line":            "A",
        "window_start":    w_start,
        "window_end":      w_end,
        "delay_alerts":    0,
        "capacity_alerts": 0,
        "total_alerts":    0,
    })


# ── Main ──────────────────────────────────────────────────────────────────────

def main() -> None:
    uri = build_uri()
    # Ocultar credenciales en el log
    uri_log = uri.split("@")[-1] if "@" in uri else uri
    print(f"\n{'=' * 58}")
    print(f"  init_mongo.py — metro_medellin_ops")
    print(f"  Host: {uri_log}")
    print(f"{'=' * 58}\n")

    client = MongoClient(uri, serverSelectionTimeoutMS=5000)
    try:
        client.admin.command("ping")
        print("  [OK] Conexión a MongoDB establecida.")
    except Exception as e:
        print(f"  ERROR: No se pudo conectar a MongoDB: {e}")
        sys.exit(1)

    db = client[DB_NAME]
    print(f"  Base de datos: {DB_NAME}\n")

    for coll_name, definition in COLLECTIONS.items():
        print(f"{SEP}")
        status = ensure_collection(db, coll_name, definition)
        print(f"  Colección '{coll_name}': {status}")

        idx_results = ensure_indexes(db[coll_name], definition["indexes"])
        print(f"  Índices:")
        for r in idx_results:
            print(r)

    # Documentos de ejemplo (validación de schema)
    print(f"\n{SEP}")
    print("  Insertando documentos de muestra (validación)…")
    insert_sample_docs(db)
    print("  [OK] Documentos de muestra insertados.")

    # Verificación final
    print(f"\n{SEP}")
    print("  Estado final:")
    for coll_name in COLLECTIONS:
        coll = db[coll_name]
        n    = coll.count_documents({})
        idxs = list(coll.index_information().keys())
        print(f"  {coll_name:<25} docs={n:>4}  índices={idxs}")

    print(f"\n{'=' * 58}")
    print("  Inicialización completada.")
    print(f"  Consultas de verificación:")
    print(f"    PO1: db.station_occupancy.find({{}})")
    print(f"    PO2: db.line_delays.find({{high_delay: true}})")
    print(f"    PO3: db.station_alerts.find({{station_id: 'EST-004'}})")
    print(f"{'=' * 58}\n")

    client.close()


if __name__ == "__main__":
    main()
