"""P5 — validación de Q1–Q5 con un micro-dataset de resultado conocido.

Construye 10 eventos (uno duplicado por event_id), ejecuta las MISMAS funciones
del servicio de analítica (app.consultas) y compara con los valores calculados
a mano. Corre en el contenedor de analítica:

  docker compose -f proyecto2/docker-compose.yml run --rm -v "$(pwd)/proyecto2/pruebas:/pruebas:ro" \
    analitica python /pruebas/p5_analitica/validar_consultas.py | tee proyecto2/evidence/p5_validacion_consultas.txt
"""

import os
import platform
import sys
from datetime import datetime, timezone

sys.path.insert(0, "/app")
from pyspark.sql import SparkSession  # noqa: E402
from pyspark.sql import types as T  # noqa: E402

from app.consultas import CONSULTAS, ejecutar  # noqa: E402

resultados: list[bool] = []


def comprobar(nombre, condicion, detalle=""):
    resultados.append(bool(condicion))
    print(f"[{'OK' if condicion else 'FALLO'}] {nombre}" + (f" — {detalle}" if detalle else ""))


def ts(texto):
    return datetime.fromisoformat(texto).replace(tzinfo=timezone.utc)


ESQUEMA = T.StructType([
    T.StructField("event_id", T.StringType()), T.StructField("tipo_evento", T.StringType()),
    T.StructField("ocurrido_en", T.TimestampType()), T.StructField("tarea_id", T.StringType()),
    T.StructField("cliente_id", T.StringType()), T.StructField("proyecto_id", T.StringType()),
    T.StructField("equipo_id", T.StringType()), T.StructField("modalidad", T.StringType()),
    T.StructField("tipo_tarea", T.StringType()), T.StructField("duracion_ms", T.LongType()),
    T.StructField("tomada_en", T.TimestampType()), T.StructField("aprobada_en", T.TimestampType()),
    T.StructField("sla_horas", T.DoubleType()),
])

# event_id, tipo, ocurrido, tarea, cliente, proyecto, equipo, modalidad, tipo_tarea, duracion, tomada, aprobada, sla
EVENTOS = [
    ("e6", "tarea_anotada", "2026-10-10T08:30", "T1", "C1", "P1", "E1", "imagen", "bbox", 1000, "2026-10-10T08:00", None, 24.0),
    ("e7", "tarea_anotada", "2026-10-08T12:30", "T2", "C1", "P1", "E1", "lidar", "cuboide_3d", 4000, "2026-10-08T12:00", None, 24.0),
    ("e8", "tarea_anotada", "2026-10-11T08:30", "T3", "C1", "P2", "E2", "imagen", "bbox", 3000, "2026-10-11T08:00", None, 48.0),
    ("e9", "tarea_anotada", "2026-06-01T01:00", "T4", "C2", "P3", "E2", "texto", "span", 500, "2026-06-01T00:00", None, 72.0),
    ("e1", "revision_aprobada", "2026-10-10T10:00", "T1", "C1", "P1", "E1", "imagen", "bbox", None, "2026-10-10T08:00", "2026-10-10T10:00", 24.0),
    ("e1", "revision_aprobada", "2026-10-10T10:00", "T1", "C1", "P1", "E1", "imagen", "bbox", None, "2026-10-10T08:00", "2026-10-10T10:00", 24.0),
    ("e2", "revision_aprobada", "2026-10-10T12:00", "T2", "C1", "P1", "E1", "lidar", "cuboide_3d", None, "2026-10-08T12:00", "2026-10-10T12:00", 24.0),
    ("e3", "revision_rechazada", "2026-10-11T10:00", "T3", "C1", "P2", "E2", "imagen", "bbox", None, "2026-10-11T08:00", None, 48.0),
    ("e4", "revision_aprobada", "2026-10-12T09:00", "T3", "C1", "P2", "E2", "imagen", "bbox", None, "2026-10-12T08:00", "2026-10-12T09:00", 48.0),
    ("e5", "revision_aprobada", "2026-06-01T06:00", "T4", "C2", "P3", "E2", "texto", "span", None, "2026-06-01T00:00", "2026-06-01T06:00", 72.0),
]


def main():
    print(f"Inicio UTC: {datetime.now(timezone.utc).isoformat()}")
    print("Objetivo: validar Q1–Q5 con un micro-dataset de resultado conocido (10 filas, 1 duplicada)")
    print(f"Máquina: {platform.platform()}, CPU lógicos: {os.cpu_count()}")

    spark = (SparkSession.builder.master("local[1]").appName("validar-q1-q5")
             .config("spark.ui.enabled", "false").config("spark.sql.session.timeZone", "UTC")
             .getOrCreate())
    spark.sparkContext.setLogLevel("ERROR")
    filas = [(e, t, ts(o), ta, c, p, eq, m, tt, d, ts(tm), ts(ap) if ap else None, s)
             for e, t, o, ta, c, p, eq, m, tt, d, tm, ap, s in EVENTOS]
    df = spark.createDataFrame(filas, ESQUEMA).dropDuplicates(["event_id"])
    comprobar("deduplicación: 10 filas físicas -> 9 eventos únicos", df.count() == 9, str(df.count()))

    r = {q: ejecutar(fn(df)) for q, fn in CONSULTAS.items()}
    for q, filas_q in r.items():
        print(f"{q}: {filas_q}")

    # Q1: ventana de 90 días anclada en el último evento (2026-10-12 09:00): T4 (junio) queda fuera.
    q1 = {(f["dia"], f["modalidad"]): f["tareas_aprobadas"] for f in r["q1"]}
    comprobar("Q1: aprobadas por día y modalidad (el duplicado cuenta una vez, junio fuera de la ventana)",
              q1 == {("2026-10-10", "imagen"): 1, ("2026-10-10", "lidar"): 1, ("2026-10-12", "imagen"): 1},
              str(q1))

    q2 = {(f["equipo_id"], f["mes"]): (f["revisadas"], f["aprobadas"], f["proporcion_aprobadas"]) for f in r["q2"]}
    comprobar("Q2: aprobadas / revisadas por equipo y mes",
              q2 == {("E1", "2026-10"): (2, 2, 1.0), ("E2", "2026-06"): (1, 1, 1.0),
                     ("E2", "2026-10"): (2, 1, 0.5)}, str(q2))

    q3 = {(f["modalidad"], f["tipo_tarea"]): (f["muestras"], f["media_duracion_ms"], f["p95_duracion_ms"])
          for f in r["q3"]}
    comprobar("Q3: media y p95 por modalidad y tipo de tarea",
              q3 == {("imagen", "bbox"): (2, 2000.0, 3000), ("lidar", "cuboide_3d"): (1, 4000.0, 4000),
                     ("texto", "span"): (1, 500.0, 500)}, str(q3))

    q4 = {(f["cliente_id"], f["proyecto_id"]): (f["revisadas"], f["rechazadas"], f["proporcion_rechazo"])
          for f in r["q4"]}
    comprobar("Q4: rechazadas / revisadas por cliente y proyecto",
              q4 == {("C1", "P1"): (2, 0, 0.0), ("C1", "P2"): (2, 1, 0.5), ("C2", "P3"): (1, 0, 0.0)},
              str(q4))

    q5 = {f["cliente_id"]: (f["tareas_aprobadas"], f["p50_horas"], f["p95_horas"], f["pct_fuera_sla"])
          for f in r["q5"]}
    comprobar("Q5: toma -> aprobación por cliente y % fuera de SLA (T2: 48 h con SLA 24 h)",
              q5 == {"C1": (3, 2.0, 48.0, 33.33), "C2": (1, 6.0, 6.0, 0.0)}, str(q5))

    spark.stop()
    ok = sum(resultados)
    print(f"\nResultado: {ok}/{len(resultados)} comprobaciones correctas")
    print(f"Fin UTC: {datetime.now(timezone.utc).isoformat()}")
    sys.exit(0 if ok == len(resultados) else 1)


if __name__ == "__main__":
    main()
