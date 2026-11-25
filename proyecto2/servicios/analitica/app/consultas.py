"""Consultas analíticas Q1–Q5 sobre eventos Parquet deduplicados por event_id.

Cada función recibe los eventos limpios y devuelve un DataFrame (sin ejecutar),
para poder obtener su plan completo con explain() antes de recolectar filas.
"""

from __future__ import annotations

from datetime import timedelta
from pathlib import Path

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

VENTANA_Q1_DIAS = 90


def ruta_conjunto(ruta_datalake: str, conjunto: str) -> str:
    """principal: eventos/ ; grande: eventos_grande/ (opcional, junto a eventos/)."""
    base = Path(ruta_datalake.rstrip("/"))
    ruta = base if conjunto == "principal" else base.parent / "eventos_grande"
    if not ruta.exists():
        raise FileNotFoundError(f"El conjunto '{conjunto}' no está generado ({ruta})")
    return str(ruta)


def df_eventos_limpios(spark, ruta_datalake: str, conjunto: str = "principal") -> DataFrame:
    """Lee solo los Parquet publicados (mes=*/*.parquet; nunca .staging) y deduplica."""
    patron = f"{ruta_conjunto(ruta_datalake, conjunto)}/mes=*/*.parquet"
    return spark.read.parquet(patron).dropDuplicates(["event_id"])


def q1(df: DataFrame) -> DataFrame:
    """Q1: tareas aprobadas por día y modalidad en los últimos 90 días.

    La ventana se ancla en el evento más reciente del conjunto, para que la
    consulta sea reproducible sobre el dataset generado y también incluya los
    eventos que produce el sistema en funcionamiento.
    """
    ultimo = df.agg(F.max("ocurrido_en")).first()[0]
    cond = F.col("tipo_evento") == "revision_aprobada"
    if ultimo is not None:
        cond = cond & (F.col("ocurrido_en") > F.lit(ultimo - timedelta(days=VENTANA_Q1_DIAS)))
    return (
        df.filter(cond)
        .groupBy(F.to_date("ocurrido_en").alias("dia"), "modalidad")
        .agg(F.countDistinct("tarea_id").alias("tareas_aprobadas"))
        .orderBy("dia", "modalidad")
    )


def q2(df: DataFrame) -> DataFrame:
    """Q2: proporción de aprobadas respecto de las revisadas, por equipo y mes."""
    return (
        df.filter(F.col("tipo_evento").isin("revision_aprobada", "revision_rechazada"))
        .withColumn("mes", F.date_format("ocurrido_en", "yyyy-MM"))
        .groupBy("equipo_id", "mes")
        .agg(
            F.count("*").alias("revisadas"),
            F.sum(F.when(F.col("tipo_evento") == "revision_aprobada", 1).otherwise(0)).alias("aprobadas"),
        )
        .withColumn("proporcion_aprobadas", F.round(F.col("aprobadas") / F.col("revisadas"), 4))
        .orderBy("equipo_id", "mes")
    )


def q3(df: DataFrame) -> DataFrame:
    """Q3: duración media y percentil 95, por modalidad y tipo de tarea."""
    return (
        df.filter((F.col("tipo_evento") == "tarea_anotada") & F.col("duracion_ms").isNotNull())
        .groupBy("modalidad", "tipo_tarea")
        .agg(
            F.count("*").alias("muestras"),
            F.round(F.avg("duracion_ms"), 2).alias("media_duracion_ms"),
            F.expr("percentile_approx(duracion_ms, 0.95)").alias("p95_duracion_ms"),
        )
        .orderBy("modalidad", "tipo_tarea")
    )


def q4(df: DataFrame) -> DataFrame:
    """Q4: proporción de rechazadas respecto de las revisadas, por cliente y proyecto."""
    return (
        df.filter(F.col("tipo_evento").isin("revision_aprobada", "revision_rechazada"))
        .groupBy("cliente_id", "proyecto_id")
        .agg(
            F.count("*").alias("revisadas"),
            F.sum(F.when(F.col("tipo_evento") == "revision_rechazada", 1).otherwise(0)).alias("rechazadas"),
        )
        .withColumn("proporcion_rechazo", F.round(F.col("rechazadas") / F.col("revisadas"), 4))
        .orderBy("cliente_id", "proyecto_id")
    )


def q5(df: DataFrame) -> DataFrame:
    """Q5: horas entre la toma y la aprobación, p50 y p95 por cliente, y % fuera de SLA."""
    ap = (
        df.filter(
            (F.col("tipo_evento") == "revision_aprobada")
            & F.col("tomada_en").isNotNull()
            & F.col("aprobada_en").isNotNull()
        )
        .withColumn(
            "horas",
            (F.col("aprobada_en").cast("double") - F.col("tomada_en").cast("double")) / 3600.0,
        )
    )
    return (
        ap.groupBy("cliente_id")
        .agg(
            F.count("*").alias("tareas_aprobadas"),
            F.round(F.expr("percentile_approx(horas, 0.5)"), 2).alias("p50_horas"),
            F.round(F.expr("percentile_approx(horas, 0.95)"), 2).alias("p95_horas"),
            F.round(100.0 * F.avg(F.when(F.col("horas") > F.col("sla_horas"), 1.0).otherwise(0.0)), 2)
            .alias("pct_fuera_sla"),
        )
        .orderBy("cliente_id")
    )


CONSULTAS = {"q1": q1, "q2": q2, "q3": q3, "q4": q4, "q5": q5}


def ejecutar(res: DataFrame) -> list[dict]:
    """Recolecta las filas con tipos serializables a JSON."""
    filas = []
    for row in res.collect():
        d = row.asDict()
        filas.append({k: (v.isoformat() if hasattr(v, "isoformat") else v) for k, v in d.items()})
    return filas
