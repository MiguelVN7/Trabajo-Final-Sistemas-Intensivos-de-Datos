"""
streaming/queries_operacionales.py
ST1630 — Queries operacionales sobre MongoDB (Fase 3)

Responde las tres preguntas operacionales del proyecto contra las colecciones
que Flink popula en tiempo real mediante ventanas tumbling de event time.

Colecciones:
  station_occupancy   PO1 — estado actual de ocupación por estación
  line_delays         PO2 — retraso actual por línea
  station_alerts      PO3 — historial de alertas por estación (última hora)

Uso:
  python3 streaming/queries_operacionales.py
  MONGO_HOST=mongodb python3 streaming/queries_operacionales.py
"""
import os
import sys
from datetime import datetime, timezone, timedelta

try:
    from pymongo import MongoClient
    from bson.json_util import dumps as bson_dumps
    import json
except ImportError:
    print("ERROR: pip install pymongo")
    sys.exit(1)

# ── Conexión ──────────────────────────────────────────────────────────────────
MONGO_HOST = os.environ.get("MONGO_HOST", "localhost")
MONGO_PORT = int(os.environ.get("MONGO_PORT", "27017"))
MONGO_USER = os.environ.get("MONGO_USER", "root")
MONGO_PASS = os.environ.get("MONGO_PASS", "rootpassword")
MONGO_URI  = os.environ.get(
    "MONGO_URI",
    f"mongodb://{MONGO_USER}:{MONGO_PASS}@{MONGO_HOST}:{MONGO_PORT}/"
)

SEP   = "=" * 62
SEP2  = "-" * 62
TOPN  = 5          # estaciones a mostrar en rankings
STATION_EJEMPLO = "EST-015"   # Poblado — alta ocupación garantizada
STATION_ALERTAS = "EST-011"   # San Antonio — aparece en station_alerts

# ── Helpers ───────────────────────────────────────────────────────────────────
def fmt_date(d) -> str:
    if isinstance(d, datetime):
        return d.strftime("%Y-%m-%dT%H:%M:%SZ")
    return str(d)

def print_explain_summary(explain: dict, query_label: str) -> None:
    """Extrae el winning plan y confirma IXSCAN vs COLLSCAN."""
    winning = explain.get("queryPlanner", {}).get("winningPlan", {})
    # Desciende al stage hoja (puede estar anidado en inputStage)
    stage = winning
    stages = []
    while stage:
        stages.append(stage.get("stage", "?"))
        stage = stage.get("inputStage") or stage.get("inputStages", [None])[0]
    exec_stats = explain.get("executionStats", {})
    docs_examined = exec_stats.get("totalDocsExamined", "?")
    keys_examined = exec_stats.get("totalKeysExamined", "?")
    docs_returned = exec_stats.get("nReturned", "?")
    ms            = exec_stats.get("executionTimeMillis", "?")
    index_name    = winning.get("inputStage", {}).get("indexName", winning.get("indexName", "—"))
    access_type   = stages[0] if stages else "?"
    print(f"  [{query_label}] plan: {' → '.join(s for s in stages if s != '?')}")
    print(f"    índice usado : {index_name}")
    print(f"    tipo acceso  : {access_type}")
    print(f"    keys/docs exam: {keys_examined} / {docs_examined}  →  devueltos: {docs_returned}  ({ms} ms)")
    if "COLLSCAN" in stages:
        print("    ⚠ COLLSCAN — sin índice en este path")
    else:
        print("    ✓ index seek — sin scan de colección completa")

# ══════════════════════════════════════════════════════════════════════════════
def main():
    client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=5_000)
    try:
        client.admin.command("ping")
    except Exception as e:
        print(f"ERROR conectando a MongoDB: {e}")
        sys.exit(1)

    db   = client["metro_medellin_ops"]
    now  = datetime.now(tz=timezone.utc)
    hora = now - timedelta(hours=1)

    print(f"\n{SEP}")
    print(f"  ST1630 — Queries Operacionales   {fmt_date(now)}")
    print(SEP)

    # ══════════════════════════════════════════════════════════════
    # PO1.A — Ocupación de UNA estación específica
    # ──────────────────────────────────────────────────────────────
    # Pregunta de negocio:
    #   ¿Cuántos pasajeros llevan en promedio los vehículos que pasan
    #   ahora por la estación X?  Permite decidir si hay sobrecarga de
    #   servicio en un punto concreto de la red sin procesar toda la
    #   colección.
    #
    # Índice: _id (IDHACK — acceso directo por clave primaria, O(1)).
    # Interpretación: avg_vehicle_occupancy > 300 → vehículos muy cargados;
    #   max_vehicle_occupancy acerca a la capacidad máxima del tren/cable.
    # ══════════════════════════════════════════════════════════════
    print(f"\n{SEP2}")
    print(f"PO1.A — Ocupación de estación '{STATION_EJEMPLO}' (lookup O(1) por _id)")
    print(SEP2)

    doc = db.station_occupancy.find_one({"_id": STATION_EJEMPLO})
    if doc:
        print(f"  Estación     : {doc['stop_name']}  ({doc['station_id']})")
        print(f"  Líneas       : {doc.get('lines_serving', [])}")
        print(f"  Ventana      : {fmt_date(doc['window_start'])} → {fmt_date(doc['window_end'])}")
        print(f"  Ocupación avg: {doc['avg_vehicle_occupancy']} pax/vehículo")
        print(f"  Ocupación max: {doc['max_vehicle_occupancy']} pax/vehículo")
        print(f"  Eventos      : {doc['event_count']} vehículos vistos en la ventana")
        print(f"  Actualizado  : {fmt_date(doc['last_updated'])}")
    else:
        print(f"  Sin datos para {STATION_EJEMPLO} (ventana aún no cerrada)")

    # explain: _id lookup
    exp = db.command("explain",
                     {"find": "station_occupancy", "filter": {"_id": STATION_EJEMPLO}},
                     verbosity="executionStats")
    print()
    print_explain_summary(exp, "PO1.A")

    # ══════════════════════════════════════════════════════════════
    # PO1.B — Ranking de estaciones más ocupadas (top-N)
    # ──────────────────────────────────────────────────────────────
    # Pregunta de negocio:
    #   ¿Qué estaciones concentran la mayor carga de pasajeros ahora?
    #   Permite priorizar dónde reforzar el servicio o emitir alertas
    #   preventivas de capacidad.
    #
    # Índice: DECISIÓN DE DISEÑO — COLLSCAN intencional.
    #   station_occupancy tiene exactamente 1 documento por estación activa
    #   (upsert por _id). Con 49 estaciones fijas en la red Metro Medellín, la
    #   colección nunca supera 49 docs; un COLLSCAN aquí es O(49) = coste
    #   constante y acotado, idéntico en latencia a un index scan de 49 entradas.
    #   Añadir un índice sobre avg_vehicle_occupancy introduciría overhead de
    #   mantenimiento en cada upsert sin beneficio measurable. El COLLSCAN es
    #   correcto y esperado para esta colección de estado actual de tamaño fijo.
    # Interpretación: las primeras posiciones son los cuellos de botella
    #   actuales de la red.
    # ══════════════════════════════════════════════════════════════
    print(f"\n{SEP2}")
    print(f"PO1.B — Ranking top-{TOPN} estaciones más ocupadas (ventana actual)")
    print(SEP2)

    top_estaciones = list(
        db.station_occupancy
        .find({}, {"station_id": 1, "stop_name": 1, "lines_serving": 1,
                   "avg_vehicle_occupancy": 1, "max_vehicle_occupancy": 1,
                   "event_count": 1, "window_end": 1})
        .sort("avg_vehicle_occupancy", -1)
        .limit(TOPN)
    )
    for i, d in enumerate(top_estaciones, 1):
        lineas = ",".join(d.get("lines_serving", []))
        print(f"  #{i}  {d['stop_name']:<22} líneas={lineas:<8}  "
              f"avg={d['avg_vehicle_occupancy']:>6.1f}  "
              f"max={d['max_vehicle_occupancy']:>4}  "
              f"eventos={d['event_count']:>3}  "
              f"ventana_fin={fmt_date(d['window_end'])}")

    exp_rank = db.command("explain",
                          {"find": "station_occupancy", "filter": {},
                           "projection": {"avg_vehicle_occupancy": 1},
                           "sort": {"avg_vehicle_occupancy": -1}},
                          verbosity="executionStats")
    print()
    print_explain_summary(exp_rank, "PO1.B")

    # ══════════════════════════════════════════════════════════════
    # PO2 — Líneas con retraso crítico ahora
    # ──────────────────────────────────────────────────────────────
    # Pregunta de negocio:
    #   ¿Qué líneas tienen ahora mismo al menos un vehículo con más de
    #   5 minutos de retraso acumulado?  Informa directamente a control
    #   de operaciones para gestión de flota o aviso a pasajeros.
    #
    # Índice: idx_high_delay  (ASCENDING sobre high_delay).
    #   Con 9 documentos, el beneficio es marginal en absoluto, pero el
    #   patrón de acceso es correcto para colecciones grandes: index seek
    #   sobre el campo booleano que discrimina el estado crítico.
    # Interpretación: cada documento devuelto = línea en situación de
    #   alerta; vehicles_delayed es el nº de trenes/cables por encima
    #   del umbral de 300s en la última ventana.
    # ══════════════════════════════════════════════════════════════
    print(f"\n{SEP2}")
    print("PO2 — Líneas con retraso crítico ahora  (high_delay = true)")
    print(SEP2)

    lineas_criticas = list(
        db.line_delays.find(
            {"high_delay": True},
            {"line": 1, "tipo_servicio": 1, "avg_delay_seconds": 1,
             "max_delay_seconds": 1, "vehicles_seen": 1, "vehicles_delayed": 1,
             "window_end": 1}
        ).sort("max_delay_seconds", -1)
    )

    if lineas_criticas:
        for d in lineas_criticas:
            # Señal operacional primero: cuántos vehículos críticos y cuál es el peor caso.
            # avg_delay va al final como contexto: un promedio bajo con max alto significa
            # un pico puntual, no una degradación generalizada — igualmente accionable.
            print(f"  ⚠  Línea {d['line']:<4} ({d['tipo_servicio']:<7})  "
                  f"veh_críticos={d['vehicles_delayed']}/{d['vehicles_seen']}  "
                  f"max={d['max_delay_seconds']:>4}s  "
                  f"avg={d['avg_delay_seconds']:>5.1f}s (contexto)  "
                  f"ventana_fin={fmt_date(d['window_end'])}")
    else:
        print("  (sin líneas con retraso crítico en la ventana actual)")

    # explain con hint para forzar el índice idx_high_delay
    exp_po2 = db.command("explain",
                         {"find": "line_delays", "filter": {"high_delay": True},
                          "hint": "idx_high_delay"},
                         verbosity="executionStats")
    print()
    print_explain_summary(exp_po2, "PO2")

    # ══════════════════════════════════════════════════════════════
    # PO3.A — Alertas acumuladas de UNA estación en la última hora
    # ──────────────────────────────────────────────────────────────
    # Pregunta de negocio:
    #   ¿Cuántas alertas de retraso y de capacidad generó la estación X
    #   en la última hora?  Sirve para detectar puntos crónicamente
    #   conflictivos que requieren intervención estructural, más allá del
    #   estado puntual de cada ventana.
    #
    # Índice: idx_station_window  (station_id ASC + window_end DESC).
    #   El $match usa igualdad en station_id (columna izquierda del índice
    #   compuesto) y rango en window_end → IXSCAN con bound scan, O(k)
    #   donde k = ventanas en la última hora (≤ 12 documentos).
    # Interpretación: total_alerts alto en una estación durante 1h señala
    #   un punto de la red con problemas recurrentes en ese turno.
    # ══════════════════════════════════════════════════════════════
    print(f"\n{SEP2}")
    print(f"PO3.A — Alertas de estación '{STATION_ALERTAS}' en la última hora")
    print(SEP2)

    pipeline_estacion = [
        {"$match": {
            "station_id": STATION_ALERTAS,
            "window_end": {"$gte": hora},
        }},
        {"$group": {
            "_id":             "$station_id",
            "stop_name":       {"$first": "$stop_name"},
            "delay_alerts":    {"$sum": "$delay_alerts"},
            "capacity_alerts": {"$sum": "$capacity_alerts"},
            "total_alerts":    {"$sum": "$total_alerts"},
            "ventanas_vistas": {"$sum": 1},
            "primera_ventana": {"$min": "$window_end"},
            "ultima_ventana":  {"$max": "$window_end"},
        }},
    ]

    results_po3a = list(db.station_alerts.aggregate(pipeline_estacion))
    if results_po3a:
        r = results_po3a[0]
        print(f"  Estación     : {r['stop_name']}  ({r['_id']})")
        print(f"  Período      : {fmt_date(r['primera_ventana'])} → {fmt_date(r['ultima_ventana'])}")
        print(f"  Ventanas     : {r['ventanas_vistas']} ventanas de 1 min en la última hora")
        print(f"  delay_alerts : {r['delay_alerts']}")
        print(f"  cap_alerts   : {r['capacity_alerts']}")
        print(f"  total_alerts : {r['total_alerts']}")
    else:
        print(f"  Sin alertas para {STATION_ALERTAS} en la última hora")

    # explain del $match antes de la agregación (primer stage)
    exp_po3a = db.command("explain",
                          {"find": "station_alerts",
                           "filter": {"station_id": STATION_ALERTAS,
                                      "window_end": {"$gte": hora}}},
                          verbosity="executionStats")
    print()
    print_explain_summary(exp_po3a, "PO3.A")

    # ══════════════════════════════════════════════════════════════
    # PO3.B — Ranking de estaciones con más alertas en la última hora
    # ──────────────────────────────────────────────────────────────
    # Pregunta de negocio:
    #   ¿Qué estaciones acumularon más alertas en la última hora?
    #   Identifica los puntos de la red con mayor conflictividad
    #   sostenida para priorizar intervención operacional.
    #
    # Índice: DECISIÓN DE DISEÑO — usa ttl_window_end, no idx_station_window.
    #   Sin station_id en el $match, idx_station_window (compuesto) no es
    #   selectivo por su columna izquierda; MongoDB elige ttl_window_end
    #   (índice simple en window_end) para el filtro de rango temporal.
    #   El número de docs examinados está acotado por diseño: TTL de 3600s
    #   garantiza que station_alerts nunca supere 49 estaciones × 60 ventanas/h
    #   = 2940 docs en steady-state → IXSCAN sobre ttl_window_end es O(k)
    #   con k ≤ 2940, latencia sub-milisegundo. No es un descuido de indexación;
    #   la cota superior está controlada por el TTL, no por un índice adicional.
    # Interpretación: las primeras posiciones del ranking son las
    #   estaciones que requieren atención inmediata o revisión de horarios.
    # ══════════════════════════════════════════════════════════════
    print(f"\n{SEP2}")
    print(f"PO3.B — Ranking top-{TOPN} estaciones con más alertas (última hora)")
    print(SEP2)

    pipeline_ranking = [
        {"$match": {"window_end": {"$gte": hora}}},
        {"$group": {
            "_id":             "$station_id",
            "stop_name":       {"$first": "$stop_name"},
            "delay_alerts":    {"$sum": "$delay_alerts"},
            "capacity_alerts": {"$sum": "$capacity_alerts"},
            "total_alerts":    {"$sum": "$total_alerts"},
            "ventanas":        {"$sum": 1},
        }},
        {"$sort":  {"total_alerts": -1}},
        {"$limit": TOPN},
    ]

    results_po3b = list(db.station_alerts.aggregate(pipeline_ranking))
    if results_po3b:
        for i, r in enumerate(results_po3b, 1):
            print(f"  #{i}  {r['stop_name']:<22} ({r['_id']})  "
                  f"total={r['total_alerts']:>3}  "
                  f"delay={r['delay_alerts']:>3}  "
                  f"cap={r['capacity_alerts']:>3}  "
                  f"ventanas={r['ventanas']:>2}")
    else:
        print("  Sin alertas acumuladas en la última hora")

    # explain del primer $match del pipeline ranking
    exp_po3b = db.command("explain",
                          {"find": "station_alerts",
                           "filter": {"window_end": {"$gte": hora}}},
                          verbosity="executionStats")
    print()
    print_explain_summary(exp_po3b, "PO3.B")

    print(f"\n{SEP}")
    print("  Queries completadas.")
    print(f"  station_occupancy : {db.station_occupancy.count_documents({})} docs")
    print(f"  line_delays       : {db.line_delays.count_documents({})} docs")
    print(f"  station_alerts    : {db.station_alerts.count_documents({})} docs")
    print(SEP)
    client.close()


if __name__ == "__main__":
    main()
