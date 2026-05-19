"""
data/generate_batch.py
Generador de datos históricos de viajes — Metro de Medellín.

Produce dos archivos CSV de ~50 000 filas cada uno:
  data/raw/metro_trips_2024_S1.csv   (enero–junio  2024)
  data/raw/metro_trips_2024_S2.csv   (julio–diciembre 2024)

Requiere únicamente la biblioteca estándar de Python 3.8+.
Lee data/paradas.csv para garantizar que todo stop_id es válido.
Uso: python3 data/generate_batch.py
"""

import csv
import os
import random
import sys
import uuid
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Dict, List, Optional, Tuple

# ─────────────────────────────────────────────────────────────
# Rutas
# ─────────────────────────────────────────────────────────────
PROJECT_ROOT = Path(__file__).resolve().parent.parent
PARADAS_CSV  = PROJECT_ROOT / "data" / "paradas.csv"
OUTPUT_DIR   = PROJECT_ROOT / "data" / "raw"

# ─────────────────────────────────────────────────────────────
# Configuración de lotes
# ─────────────────────────────────────────────────────────────
ROWS_PER_LOTE = 50_000

LOTES = [
    {
        "source_file": "metro_trips_2024_S1.csv",
        "start":       date(2024,  1,  1),
        "end":         date(2024,  6, 30),
        "trip_prefix": "S1",
        "seed":        202401,
    },
    {
        "source_file": "metro_trips_2024_S2.csv",
        "start":       date(2024,  7,  1),
        "end":         date(2024, 12, 31),
        "trip_prefix": "S2",
        "seed":        202402,
    },
]

# ─────────────────────────────────────────────────────────────
# Festivos Colombia 2024 (18 fechas oficiales)
# ─────────────────────────────────────────────────────────────
HOLIDAYS_2024 = {
    date(2024,  1,  1),   # Año Nuevo
    date(2024,  1,  8),   # Reyes Magos (trasladado de ene 6)
    date(2024,  3, 25),   # San José    (trasladado de mar 19)
    date(2024,  3, 28),   # Jueves Santo
    date(2024,  3, 29),   # Viernes Santo
    date(2024,  5,  1),   # Día del Trabajo
    date(2024,  5, 13),   # Ascensión   (trasladado de may 10)
    date(2024,  6,  3),   # Corpus Christi (trasladado de may 30)
    date(2024,  6, 10),   # Sagrado Corazón (trasladado de jun 7)
    date(2024,  7,  1),   # San Pedro y San Pablo (trasladado de jun 29)
    date(2024,  7, 20),   # Independencia
    date(2024,  8,  7),   # Batalla de Boyacá
    date(2024,  8, 19),   # Asunción    (trasladado de ago 15)
    date(2024, 10, 14),   # Día de la Raza (trasladado de oct 12)
    date(2024, 11,  4),   # Todos los Santos (trasladado de nov 1)
    date(2024, 11, 11),   # Independencia de Cartagena
    date(2024, 12,  8),   # Inmaculada Concepción
    date(2024, 12, 25),   # Navidad
}

# Meses con temporada de lluvias en el Área Metropolitana
RAINY_MONTHS = {4, 5, 9, 10, 11}

# ─────────────────────────────────────────────────────────────
# Pesos por línea (proporcionales a la demanda real)
# ─────────────────────────────────────────────────────────────
LINE_WEIGHTS: Dict[str, int] = {
    "A":  38,
    "B":  18,
    "TA": 12,
    "K":   9,
    "J":   6,
    "M":   5,
    "P":   5,
    "H":   5,
    "L":   2,
}
CABLE_LINES = {"K", "J", "L", "H", "M", "P"}

# ─────────────────────────────────────────────────────────────
# Distribución horaria (granularidad de hora; Metro 05-23 h)
# ─────────────────────────────────────────────────────────────
_HOUR_RAW: Dict[int, float] = {
     5: 0.5,
     6: 4.5,   # ┐ pico mañana
     7: 9.0,   # │
     8: 9.0,   # ┘
     9: 5.5,
    10: 3.0,
    11: 2.8,
    12: 2.8,
    13: 2.8,
    14: 3.0,
    15: 5.5,   # ┐ pico tarde
    16: 9.0,   # │
    17: 9.0,   # │
    18: 6.5,   # ┘
    19: 4.0,
    20: 2.5,
    21: 1.5,
    22: 0.5,
}
_HOURS      = sorted(_HOUR_RAW)
_HOUR_TOTAL = sum(_HOUR_RAW.values())
_HOUR_PROBS = [_HOUR_RAW[h] / _HOUR_TOTAL for h in _HOURS]

PEAK_HOURS = {6, 7, 8, 15, 16, 17, 18}

FIELDNAMES = [
    "trip_id", "stop_id_origen", "stop_id_destino", "line",
    "passenger_count", "delay_seconds", "trip_date", "hour_block",
    "is_holiday", "source_file", "ingestion_ts",
]


# ─────────────────────────────────────────────────────────────
# Funciones auxiliares
# ─────────────────────────────────────────────────────────────

def hour_to_block(h: int) -> str:
    """Mapea una hora (5..22) a su franja de operación."""
    if h < 6:   return "05-06"
    if h < 9:   return "06-09"
    if h < 12:  return "09-12"
    if h < 15:  return "12-15"
    if h < 18:  return "15-18"
    if h < 21:  return "18-21"
    return "21-23"


def load_paradas(path: Path) -> Dict[str, List[str]]:
    """
    Lee paradas.csv y devuelve {linea: [stop_id, ...]} ordenado por 'orden'.
    Garantía: los stop_id del generador siempre existirán en el catálogo.
    """
    by_line: Dict[str, List[Tuple[int, str]]] = {}
    with open(path, newline="", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            by_line.setdefault(row["linea"], []).append(
                (int(row["orden"]), row["stop_id"])
            )
    return {
        linea: [sid for _, sid in sorted(stops)]
        for linea, stops in by_line.items()
    }


def pick_trip(
    rng: random.Random, stops: List[str]
) -> Optional[Tuple[str, str]]:
    """
    Elige (stop_id_origen, stop_id_destino) de la misma línea.
    - Origen: distribución triangular hacia paradas centrales.
    - Dirección: 70 % sentido natural (j > i), 30 % inverso.
    - Distancia: geométrica con media ≈ 3 paradas.
    """
    n = len(stops)
    if n < 2:
        return None

    # Peso triangular: más probabilidad en paradas centrales
    weights = [float(min(i + 1, n - i)) for i in range(n)]
    total_w = sum(weights)
    probs   = [w / total_w for w in weights]
    i = rng.choices(range(n), weights=probs, k=1)[0]

    # Dirección preferida
    forward = rng.random() < 0.70
    if forward and i >= n - 1:
        forward = False
    if not forward and i == 0:
        forward = True

    max_dist = (n - 1 - i) if forward else i
    # Exponencial discretizada, media ≈ 3, mínimo 1
    dist = min(max(1, int(rng.expovariate(1.0 / 3.0))), max_dist)
    j = (i + dist) if forward else (i - dist)

    return stops[i], stops[j]


def gen_passenger_count(
    rng: random.Random,
    hour: int,
    is_weekend: bool,
    is_holiday: bool,
    line: str,
) -> int:
    """Normal truncada [1, 400]; μ y σ en función del tipo de día y hora."""
    if is_holiday or is_weekend:
        mu, sigma = 60, 25
    elif hour in PEAK_HOURS:
        mu, sigma = 180, 60
    else:
        mu, sigma = 95, 38

    if line in CABLE_LINES:          # cabinas de menor capacidad
        mu    = max(5,  int(mu    * 0.40))
        sigma = max(3,  int(sigma * 0.40))

    return max(1, min(int(rng.gauss(mu, sigma)), 400))


def gen_delay(
    rng: random.Random,
    hour: int,
    month: int,
    is_holiday: bool,
    line: str,
) -> int:
    """
    Exponencial (media base 45 s) multiplicada por factores acumulativos:
      × 1.8  si hora pico (y no festivo)
      × 0.70 si festivo   (servicio más holgado)
      × 1.30 si mes lluvioso
      × 1.20 adicional si cable en mes lluvioso
    Techo: 1 200 s (20 min).
    """
    base = rng.expovariate(1.0 / 45.0)

    if is_holiday:
        base *= 0.70
    elif hour in PEAK_HOURS:
        base *= 1.80

    if month in RAINY_MONTHS:
        base *= 1.30
        if line in CABLE_LINES:
            base *= 1.20

    return int(min(base, 1_200))


# ─────────────────────────────────────────────────────────────
# Generador de un lote completo
# ─────────────────────────────────────────────────────────────

def generate_lote(
    cfg: dict,
    paradas: Dict[str, List[str]],
    n_rows: int,
    ingestion_ts: str,
) -> List[dict]:
    rng    = random.Random(cfg["seed"])
    prefix = cfg["trip_prefix"]
    start  = cfg["start"]
    end    = cfg["end"]
    src    = cfg["source_file"]

    lines   = list(LINE_WEIGHTS.keys())
    weights = list(LINE_WEIGHTS.values())
    span    = (end - start).days

    rows:     List[dict] = []
    attempts: int        = 0

    while len(rows) < n_rows:
        attempts += 1
        if attempts > n_rows * 10:
            print(f"  WARN: abortando tras {attempts} intentos", file=sys.stderr)
            break

        line  = rng.choices(lines, weights=weights, k=1)[0]
        stops = paradas.get(line, [])
        trip  = pick_trip(rng, stops)
        if trip is None:
            continue

        orig, dest = trip
        trip_date  = start + timedelta(days=rng.randint(0, span))
        hour       = rng.choices(_HOURS, weights=_HOUR_PROBS, k=1)[0]
        is_holiday = trip_date in HOLIDAYS_2024
        is_weekend = trip_date.weekday() >= 5   # 5=sábado, 6=domingo

        rows.append({
            "trip_id":         f"{prefix}-{uuid.UUID(int=rng.getrandbits(128))}",
            "stop_id_origen":  orig,
            "stop_id_destino": dest,
            "line":            line,
            "passenger_count": gen_passenger_count(rng, hour, is_weekend, is_holiday, line),
            "delay_seconds":   gen_delay(rng, hour, trip_date.month, is_holiday, line),
            "trip_date":       trip_date.isoformat(),
            "hour_block":      hour_to_block(hour),
            "is_holiday":      str(is_holiday).lower(),
            "source_file":     src,
            "ingestion_ts":    ingestion_ts,
        })

    return rows


# ─────────────────────────────────────────────────────────────
# Main
# ─────────────────────────────────────────────────────────────

def main() -> None:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    print("Cargando paradas.csv…")
    paradas = load_paradas(PARADAS_CSV)
    for linea in sorted(paradas):
        print(f"  Línea {linea:2s}: {len(paradas[linea])} paradas")

    # Timestamp de generación — mismo para ambos lotes (misma ejecución)
    ingestion_ts = datetime.now(tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    print(f"\ningestion_ts: {ingestion_ts}")

    for cfg in LOTES:
        out_path = OUTPUT_DIR / cfg["source_file"]
        print(f"\nGenerando {cfg['source_file']}  "
              f"({cfg['start']} → {cfg['end']}, seed={cfg['seed']})…")
        rows = generate_lote(cfg, paradas, ROWS_PER_LOTE, ingestion_ts)
        with open(out_path, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=FIELDNAMES)
            writer.writeheader()
            writer.writerows(rows)
        print(f"  {len(rows):,} filas → {out_path.relative_to(PROJECT_ROOT)}")

    print("\nListo.")


if __name__ == "__main__":
    main()
