# Proyecto Final ST1630 — Sistema de Datos Híbrido

Plataforma de monitoreo del transporte público de Medellín (Metro + buses
alimentadores). Arquitectura híbrida con dos caminos complementarios:

- **Camino Batch**: datos históricos → Lakehouse (Bronze → Silver → Gold) sobre
  Iceberg + MinIO, procesados con PySpark.
- **Camino Streaming**: eventos en tiempo real → Kafka → Flink → MongoDB.

Curso: ST1630 Sistemas Intensivos en Datos — Universidad EAFIT, 2026-1.
Integrantes: Esteban Molina, Miguel Villegas, Sebastián Rodríguez.

---

## Fase actual: FASE 1 — Datos

**Fase 0 completada:** `docker-compose.yml` con 9 servicios `healthy`,
bucket de MinIO creado automáticamente, smoke test end-to-end pasando
(Kafka · MinIO · Spark + Iceberg REST Catalog).

**Objetivo de la Fase 1:** poblar el lakehouse con datos históricos del
transporte público de Medellín. Ingestar datasets en Bronze (raw), transformar
a Silver (limpio y tipado) y producir tablas Gold (agregaciones listas para
análisis), todo con PySpark sobre tablas Iceberg.

---

## Stack obligatorio (definido por el enunciado del curso)

| Componente            | Tecnología                       | Rol                                  |
|-----------------------|----------------------------------|--------------------------------------|
| Infraestructura       | Docker Compose                   | Reproducibilidad local               |
| Mensajería            | Apache Kafka                     | Broker de eventos                    |
| Streaming             | Apache Flink (JobManager + TaskManager) | Motor de streaming con estado  |
| Procesamiento batch   | Apache Spark (PySpark)           | Motor batch                          |
| Table format          | Apache Iceberg + REST Catalog    | ACID, Time Travel, Schema Evolution  |
| Object storage        | MinIO                            | Almacenamiento del Lakehouse (S3)    |
| Base de datos NoSQL   | MongoDB                          | Servicio de consultas operacionales  |

---

## Reglas de construcción (NO negociables)

- **Versiones FIJAS** en todas las imágenes. Nunca usar `latest`.
- **Verificar la matriz de compatibilidad** Flink / iceberg-flink-runtime /
  Kafka ANTES de fijar versiones. El `iceberg-flink-runtime` debe coincidir con
  la versión menor de Flink. El `iceberg-spark-runtime` debe coincidir con la
  versión de Spark y de Scala.
- **Imágenes oficiales** siempre que existan.
- **Nada de paths absolutos** del equipo del desarrollador. Toda la
  configuración va en un archivo `.env`.
- **Healthchecks** en cada servicio del compose.
- **`depends_on` con `condition: service_healthy`** para orden de arranque.
- **Bucket de MinIO creado automáticamente** al levantar (contenedor `mc` o
  equivalente), no manualmente.

---

## Estructura de carpetas esperada del proyecto

```
proyecto-st1630/
├── CLAUDE.md
├── .env
├── .env.example
├── docker-compose.yml
├── README.md
├── scripts/
│   └── smoke_test.sh
├── batch/          (Fase 2 — vacío por ahora)
├── streaming/      (Fase 3 — vacío por ahora)
└── data/           (Fase 1 — vacío por ahora)
```

---

## Workflow preferido

- Antes de escribir o modificar archivos, explicar el plan y esperar
  confirmación (usar modo plan).
- Cambios incrementales y verificables, no todo de una vez.
- Después de cada cambio relevante en el compose, levantar y verificar.
- Si un servicio queda `unhealthy`, leer sus logs y diagnosticar antes de
  cambiar otra cosa.
- Correcciones de código puntuales y dirigidas; no reescribir archivos
  completos si solo cambia una sección.
