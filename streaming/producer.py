"""
streaming/producer.py
Productor Kafka con estado — Metro de Medellín.

Simula una flota de 39 vehículos recorriendo sus líneas en orden, con
evolución de pasajeros con estado, delay acumulado y emisión condicional
de delay_alert (edge-triggered a 300 s) y capacity_alert (90 % de cap).

Variables de entorno:
  KAFKA_BROKER              kafka:9092
  KAFKA_TOPIC               transport-events
  TIME_SCALE                60   (1 s real = 60 s simulados)
  LATE_EVENT_PROBABILITY    0.0  (fracción 0–1; activa backdating para Flink)
  DATA_DIR                  /data
  RANDOM_SEED               42
"""
import csv
import heapq
import json
import math
import os
import random
import sys
import time
import uuid
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Dict, FrozenSet, List, Optional, Set

try:
    from kafka import KafkaProducer
    from kafka.errors import NoBrokersAvailable
except ImportError:
    sys.exit("ERROR: pip install kafka-python")

# ── Entorno ──────────────────────────────────────────────────────────────────
KAFKA_BROKER    = os.getenv("KAFKA_BROKER",              "kafka:9092")
KAFKA_TOPIC     = os.getenv("KAFKA_TOPIC",               "transport-events")
TIME_SCALE      = float(os.getenv("TIME_SCALE",          "60"))
LATE_EVENT_PROB = float(os.getenv("LATE_EVENT_PROBABILITY", "0.0"))
DATA_DIR        = Path(os.getenv("DATA_DIR",             "/data"))
SEED            = int(os.getenv("RANDOM_SEED",           "42"))

# ── Parámetros de dominio ────────────────────────────────────────────────────
PEAK_HOURS      = frozenset({6, 7, 8, 15, 16, 17, 18})
CABLE_LINES     = frozenset({"K", "J", "L", "H", "M", "P"})
RAINY_MONTHS    = frozenset({4, 5, 9, 10, 11})
DELAY_THRESHOLD = 300  # segundos acumulados → dispara delay_alert (1 vez/recorrido)

# Flota por línea
FLEET: Dict[str, int] = {
    "A": 12, "B": 6, "TA": 4,
    "K": 3, "J": 3, "M": 3, "P": 3, "H": 3, "L": 2,
}

# Capacidades por tipo de servicio (modeladas como grupo de cabinas para cables)
CAPACITY = {"metro": 1200, "tranvia": 300, "cable": 50}

# Demanda base de embarco por parada (pasajeros / parada en condición estándar)
# metro subido a 100: estado estacionario pico = 100×1.8/0.15 = 1200 → umbral 1080 alcanzable
BASE_DEMAND = {"metro": 100, "tranvia": 30, "cable": 8}

# Tiempos de tránsito entre paradas (segundos simulados)
TRANSIT_MU = {"metro": 150, "tranvia": 200, "cable": 240}
TRANSIT_SD = {"metro":  20, "tranvia":  30, "cable":  40}

TURNAROUND_SIM = 300   # pausa extra en terminal (s simulados) antes del retorno


# ── Carga de catálogos ───────────────────────────────────────────────────────

def load_data(data_dir: Path):
    """
    Lee estaciones.csv y paradas.csv.
    Devuelve:
      by_line       : {linea: [{"stop_id", "station_id", "tipo_servicio"}, ...]}
                      ordenado por columna 'orden'.
      transfer_stops: conjunto de stop_ids en estaciones de transbordo
                      (derivado de es_transbordo en estaciones.csv).
    """
    transbordos: Set[str] = set()
    with open(data_dir / "estaciones.csv", encoding="utf-8") as f:
        for r in csv.DictReader(f):
            if r["es_transbordo"].lower() == "true":
                transbordos.add(r["station_id"])

    by_line: Dict[str, List[Dict]] = {}
    transfer_stops: Set[str] = set()
    with open(data_dir / "paradas.csv", encoding="utf-8") as f:
        for r in csv.DictReader(f):
            by_line.setdefault(r["linea"], []).append({
                "stop_id":       r["stop_id"],
                "station_id":    r["station_id"],
                "tipo_servicio": r["tipo_servicio"],
                "orden":         int(r["orden"]),
            })
            if r["station_id"] in transbordos:
                transfer_stops.add(r["stop_id"])

    for linea in by_line:
        by_line[linea].sort(key=lambda x: x["orden"])

    return by_line, transfer_stops


# ── Entidad Vehicle ──────────────────────────────────────────────────────────

@dataclass
class Vehicle:
    # Identidad (inmutable durante la vida del objeto)
    vehicle_id:   str
    line:         str
    stops:        List[Dict]  # paradas ordenadas {stop_id, station_id, tipo_servicio}
    capacity:     int
    type_service: str         # metro | tranvia | cable

    # Estado mutable
    stop_idx:         int    # índice actual en stops[] (0..N-1)
    direction:        int    # +1 → hacia stop[N-1], -1 → hacia stop[0]
    passengers:       int    # ocupación actual (0..capacity)
    cumulative_delay: float  # retraso acumulado en el recorrido actual (s simulados)
    delay_alerted:    bool   # True después de emitir el primer delay_alert del recorrido
    last_seg_delay:   float  # retraso del tramo recién transitado (para el próximo arrival)

    # Scheduler
    next_real_ts: float  # time.time() del próximo evento
    next_action:  str    # 'arrive' | 'depart'

    def __lt__(self, other: "Vehicle") -> bool:
        return self.next_real_ts < other.next_real_ts


# ── Funciones estadísticas y de dominio ──────────────────────────────────────

def binom_sample(rng: random.Random, n: int, p: float) -> int:
    """Binomial(n, p) con aproximación normal para n > 20."""
    if n <= 0 or p <= 0:
        return 0
    if p >= 1.0:
        return n
    if n > 20:
        mu = n * p
        sigma = math.sqrt(n * p * (1.0 - p))
        return max(0, min(n, int(rng.gauss(mu, sigma))))
    return sum(1 for _ in range(n) if rng.random() < p)


def hour_demand_factor(h: int) -> float:
    """Multiplicador de demanda según hora del día (coherente con batch generator)."""
    if h in PEAK_HOURS:          return 1.8
    if h in {9, 14, 19, 20}:    return 1.2
    if h <= 5 or h >= 21:       return 0.4
    return 1.0


def compute_seg_delay(
    rng: random.Random, h: int, month: int, line: str, is_sunday: bool
) -> float:
    """
    Retraso del segmento (s simulados). Exponencial con factores multiplicativos.
    Coherente con el modelo de delay del batch generator.
    """
    base = rng.expovariate(1.0 / 20.0)
    if is_sunday:
        base *= 0.7
    elif h in PEAK_HOURS:
        base *= 1.8
    if month in RAINY_MONTHS:
        base *= 1.3
        if line in CABLE_LINES:
            base *= 1.2     # cables más sensibles al viento/lluvia
    return base


def make_timestamp(rng: random.Random) -> str:
    """
    Timestamp REAL del momento de publicación (UTC).
    Si LATE_EVENT_PROB activo, retrocede el campo timestamp entre 10–120 s
    para simular out-of-order events (Flink watermark testing).
    El sim_clock y TIME_SCALE no influyen aquí.
    """
    now = datetime.now(timezone.utc)
    if LATE_EVENT_PROB > 0.0 and rng.random() < LATE_EVENT_PROB:
        now -= timedelta(seconds=rng.uniform(10, 120))
    # Resolución de milisegundos: garantiza que arrival y departure
    # del mismo vehículo en la misma parada tengan timestamps distintos
    # incluso con TIME_SCALE alto (el dwell es siempre > 0 ms en real time).
    return now.strftime("%Y-%m-%dT%H:%M:%S.") + f"{now.microsecond // 1000:03d}Z"


def build_event(
    etype: str,
    vehicle: Vehicle,
    stop_id: str,
    delay_s: int,
    ts: str,
) -> dict:
    return {
        "event_id":        str(uuid.uuid4()),
        "event_type":      etype,
        "stop_id":         stop_id,
        "line":            vehicle.line,
        "vehicle_id":      vehicle.vehicle_id,
        "passenger_count": vehicle.passengers,
        "delay_seconds":   delay_s,
        "timestamp":       ts,
    }


# ── Máquina de estados ───────────────────────────────────────────────────────

def do_arrive(
    v: Vehicle,
    rng: random.Random,
    transfer_stops: Set[str],
) -> List[dict]:
    """
    Procesa la llegada del vehículo a stops[stop_idx].
    Mutua el estado del vehículo y devuelve lista de eventos a publicar.

    Terminal:
      • Fuerza desembarco del 100 % de pasajeros.
      • Invierte direction y resetea cumulative_delay / delay_alerted.
      • Programa depart (el turnaround) antes de la siguiente salida.

    No-terminal:
      • Intercambio realista de pasajeros (desembarco binomial + embarco Normal).
      • Emite capacity_alert si occupancy > 90 % del aforo.
    """
    events: List[dict] = []
    n       = len(v.stops)
    stop    = v.stops[v.stop_idx]
    stop_id = stop["stop_id"]
    h       = datetime.now(timezone.utc).hour

    is_terminal = (
        (v.direction == +1 and v.stop_idx == n - 1) or
        (v.direction == -1 and v.stop_idx == 0)
    )

    # ── Intercambio de pasajeros ────────────────────────────────────────────
    if is_terminal:
        alight = v.passengers    # todos desembarcan
        board  = 0
    else:
        dist_to_term = (n - 1 - v.stop_idx) if v.direction == +1 else v.stop_idx
        p_base    = 0.70 if dist_to_term <= 1 else 0.15
        p_bonus   = 0.08 if stop_id in transfer_stops else 0.0   # reducido de 0.15
        p_alight  = min(0.80, p_base + p_bonus)

        alight = binom_sample(rng, v.passengers, p_alight)
        free   = v.capacity - (v.passengers - alight)

        # Demanda de embarco: factor hora × posición × transbordo
        base_d   = BASE_DEMAND[v.type_service]
        pos_w    = 1.0 + 0.5 * math.sin(math.pi * v.stop_idx / max(n - 1, 1))
        trans_w  = 1.3 if stop_id in transfer_stops else 1.0   # era 1.5; base_demand más alto lo compensa
        mu       = base_d * hour_demand_factor(h) * pos_w * trans_w
        demand   = max(0, int(rng.gauss(mu, max(1.0, mu * 0.30))))
        board    = min(demand, max(0, free))

    v.passengers = v.passengers - alight + board

    # ── Eventos ─────────────────────────────────────────────────────────────
    ts = make_timestamp(rng)
    events.append(build_event("arrival", v, stop_id, int(v.last_seg_delay), ts))

    if v.passengers > 0.90 * v.capacity:
        events.append(build_event("capacity_alert", v, stop_id, 0, make_timestamp(rng)))

    # ── Siguiente acción ─────────────────────────────────────────────────────
    if is_terminal:
        v.direction        *= -1
        v.cumulative_delay  = 0.0
        v.delay_alerted     = False
        # Turnaround: pausa extra antes de volver a salir
        dwell_sim = max(60.0, rng.gauss(TURNAROUND_SIM, 60))
    else:
        dwell_mu  = 75 if h in PEAK_HOURS else 45
        dwell_sim = max(15.0, rng.gauss(dwell_mu, 12))

    v.next_action  = "depart"
    v.next_real_ts = time.time() + dwell_sim / TIME_SCALE

    return events


def do_depart(
    v: Vehicle,
    rng: random.Random,
) -> List[dict]:
    """
    Procesa la salida del vehículo desde stops[stop_idx].
    Mutua el estado y devuelve lista de eventos a publicar.

    delay_alert: edge-triggered — se emite UNA sola vez por recorrido
    cuando cumulative_delay cruza DELAY_THRESHOLD (300 s simulados).
    Se resetea en el próximo terminal (do_arrive).
    """
    events: List[dict] = []
    stop_id = v.stops[v.stop_idx]["stop_id"]
    now_utc = datetime.now(timezone.utc)
    h       = now_utc.hour
    month   = now_utc.month
    is_sun  = now_utc.weekday() == 6

    # ── Cómputo de retraso del segmento ─────────────────────────────────────
    seg_d = compute_seg_delay(rng, h, month, v.line, is_sun)

    v.cumulative_delay += seg_d
    # Recovery parcial: conductor "acelera" para compensar retrasos previos
    if v.cumulative_delay > 60:
        recovery = rng.uniform(0.0, min(20.0, v.cumulative_delay * 0.15))
        v.cumulative_delay -= recovery

    v.last_seg_delay = seg_d   # el próximo do_arrive lo usa como delay_of_arrival

    # ── Eventos ─────────────────────────────────────────────────────────────
    ts = make_timestamp(rng)
    events.append(build_event("departure", v, stop_id, int(seg_d), ts))

    # delay_alert: edge-triggered — solo una vez por recorrido
    if v.cumulative_delay > DELAY_THRESHOLD and not v.delay_alerted:
        events.append(build_event(
            "delay_alert", v, stop_id, int(v.cumulative_delay), make_timestamp(rng)
        ))
        v.delay_alerted = True

    # ── Avance a la siguiente parada ────────────────────────────────────────
    v.stop_idx += v.direction

    transit_sim = max(30.0, rng.gauss(TRANSIT_MU[v.type_service], TRANSIT_SD[v.type_service]))
    v.next_action  = "arrive"
    v.next_real_ts = time.time() + transit_sim / TIME_SCALE

    return events


# ── Inicialización de la flota ────────────────────────────────────────────────

def init_fleet(
    by_line: Dict[str, List[Dict]],
    rng: random.Random,
) -> List[Vehicle]:
    """
    Crea los vehículos de todas las líneas con posiciones iniciales escalonadas
    para que desde t=0 haya vehículos distribuidos a lo largo de cada línea.
    """
    fleet: List[Vehicle] = []
    now = time.time()

    for line in sorted(by_line):
        stops   = by_line[line]
        n       = len(stops)
        tipo    = stops[0]["tipo_servicio"]
        cap     = CAPACITY[tipo]
        n_veh   = FLEET.get(line, 1)

        for i in range(n_veh):
            # Escalonar: vehículo i arranca en la parada (i × N) // F
            start_idx = (i * n) // n_veh
            direction = +1 if i % 2 == 0 else -1

            # Evitar arrancar en terminal con dirección equivocada
            if direction == +1 and start_idx == n - 1:
                direction = -1
            if direction == -1 and start_idx == 0:
                direction = +1

            # Ocupación inicial entre 30–55 % de la capacidad
            init_pax = int(rng.uniform(0.30, 0.55) * cap)

            fleet.append(Vehicle(
                vehicle_id        = f"{line}-V{i+1:02d}",
                line              = line,
                stops             = stops,
                capacity          = cap,
                type_service      = tipo,
                stop_idx          = start_idx,
                direction         = direction,
                passengers        = init_pax,
                cumulative_delay  = 0.0,
                delay_alerted     = False,
                last_seg_delay    = 0.0,
                next_real_ts      = now + rng.uniform(0.0, 3.0),  # stagger arranque
                next_action       = "arrive",
            ))

    return fleet


# ── Loop principal ────────────────────────────────────────────────────────────

def main() -> None:
    rng = random.Random(SEED)

    print("=" * 60)
    print("  ST1630 — Productor Kafka Metro Medellín")
    print("=" * 60)
    print(f"  KAFKA_BROKER    : {KAFKA_BROKER}")
    print(f"  KAFKA_TOPIC     : {KAFKA_TOPIC}")
    print(f"  TIME_SCALE      : {TIME_SCALE}×  (1 real s = {TIME_SCALE} sim s)")
    print(f"  LATE_EVENT_PROB : {LATE_EVENT_PROB}")
    print(f"  DATA_DIR        : {DATA_DIR}")

    print("\nCargando catálogo…")
    by_line, transfer_stops = load_data(DATA_DIR)
    total_veh = sum(FLEET.get(l, 1) for l in by_line)
    for line in sorted(by_line):
        tipo = by_line[line][0]["tipo_servicio"]
        print(f"  {line:<3} {len(by_line[line]):>2} paradas  "
              f"{FLEET.get(line,1):>2} vehículos  cap={CAPACITY[tipo]:>4}  [{tipo}]")
    print(f"  Total flota: {total_veh} vehículos")
    print(f"  Transfer stops: {len(transfer_stops)}")

    print(f"\nConectando a Kafka ({KAFKA_BROKER})…")
    try:
        producer = KafkaProducer(
            bootstrap_servers=KAFKA_BROKER,
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            acks="all",
            retries=5,
            request_timeout_ms=10_000,
        )
    except NoBrokersAvailable:
        sys.exit(f"ERROR: sin brokers en {KAFKA_BROKER}")

    print(f"Publicando en '{KAFKA_TOPIC}'… (Ctrl-C para detener)\n")

    fleet = init_fleet(by_line, rng)
    heap: List[Vehicle] = list(fleet)
    heapq.heapify(heap)

    counts = {"arrival": 0, "departure": 0, "delay_alert": 0, "capacity_alert": 0}
    total  = 0

    try:
        while True:
            v = heapq.heappop(heap)

            wait = v.next_real_ts - time.time()
            if wait > 0:
                time.sleep(wait)

            events = (
                do_arrive(v, rng, transfer_stops)
                if v.next_action == "arrive"
                else do_depart(v, rng)
            )

            for evt in events:
                producer.send(KAFKA_TOPIC, value=evt)
                counts[evt["event_type"]] += 1
                total += 1
                etype = evt["event_type"]
                print(
                    f"  [{etype:<16}] {evt['vehicle_id']:<8} "
                    f"stop={evt['stop_id']:<12} "
                    f"pax={evt['passenger_count']:>4} "
                    f"delay={evt['delay_seconds']:>5}s "
                    f"ts={evt['timestamp']}"
                )

            heapq.heappush(heap, v)

            if total % 100 == 0:
                print(
                    f"\n  ── {total} eventos ── "
                    + "  ".join(f"{k}:{c}" for k, c in counts.items())
                    + "\n"
                )

    except KeyboardInterrupt:
        pass
    finally:
        print(f"\n{'='*60}")
        print(f"  Detenido. {total} eventos publicados.")
        print("  " + "  ".join(f"{k}: {c}" for k, c in counts.items()))
        print(f"{'='*60}")
        producer.flush()
        producer.close()


if __name__ == "__main__":
    main()
