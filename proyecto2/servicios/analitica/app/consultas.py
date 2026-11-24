"""Consultas analíticas Q1-Q5 sobre eventos Parquet deduplicados por event_id."""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def df_eventos_limpios(spark, ruta_datalake: str, conjunto: str = "principal") -> DataFrame:
    """Lee Parquet publicados (excluyendo .staging) y deduplica por event_id."""
    patron = f"{ruta_datalake.rstrip('/')}/mes=*/*.parquet"
    df = spark.read.parquet(patron)

    if conjunto == "principal":
        df = df.filter(
            (F.col("ocurrido_en") >= F.lit("2025-01-01T00:00:00Z").cast("timestamp"))
            & (F.col("ocurrido_en") < F.lit("2026-01-01T00:00:00Z").cast("timestamp"))
        )

    return df.dropDuplicates(["event_id"])


def ejecutar_q1(df: DataFrame) -> list[dict]:
    """Q1: Conteo de eventos únicos por mes y tipo de evento."""
    res = (
        df.withColumn("mes_str", F.date_format(F.col("ocurrido_en"), "yyyy-MM"))
        .groupBy("mes_str", "tipo_evento")
        .agg(F.count("*").alias("total_eventos"))
        .orderBy("mes_str", "tipo_evento")
    )
    return [row.asDict() for row in res.collect()]


def ejecutar_q2(df: DataFrame) -> list[dict]:
    """Q2: Tareas únicas y eventos por modalidad y equipo."""
    res = (
        df.groupBy("modalidad", "equipo_id")
        .agg(
            F.approx_count_distinct("tarea_id").alias("tareas_aprox"),
            F.count("*").alias("total_eventos"),
        )
        .orderBy(F.desc("total_eventos"))
    )
    return [row.asDict() for row in res.collect()]


def ejecutar_q3(df: DataFrame) -> list[dict]:
    """Q3: Duración de anotación (duracion_ms) en eventos tarea_anotada."""
    res = (
        df.filter((F.col("tipo_evento") == "tarea_anotada") & F.col("duracion_ms").isNotNull())
        .groupBy("modalidad")
        .agg(
            F.count("*").alias("muestras"),
            F.avg("duracion_ms").alias("promedio_duracion_ms"),
            F.expr("percentile_approx(duracion_ms, 0.5)").alias("p50_duracion_ms"),
            F.expr("percentile_approx(duracion_ms, 0.95)").alias("p95_duracion_ms"),
        )
        .orderBy("modalidad")
    )
    return [row.asDict() for row in res.collect()]


def ejecutar_q4(df: DataFrame) -> list[dict]:
    """Q4: Proporción de rechazos sobre revisadas por cliente y proyecto."""
    rev = df.filter(F.col("tipo_evento").isin("revision_aprobada", "revision_rechazada"))
    res = (
        rev.groupBy("cliente_id", "proyecto_id")
        .agg(
            F.count("*").alias("total_revisadas"),
            F.sum(F.when(F.col("tipo_evento") == "revision_rechazada", 1).otherwise(0)).alias("rechazadas"),
        )
        .withColumn(
            "proporcion_rechazo",
            F.round(F.col("rechazadas") / F.col("total_revisadas"), 4),
        )
        .orderBy("cliente_id", "proyecto_id")
    )
    return [row.asDict() for row in res.collect()]


def ejecutar_q5(df: DataFrame) -> list[dict]:
    """Q5: p50, p95 de toma -> aprobación por cliente y % fuera de SLA."""
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
    res = (
        ap.groupBy("cliente_id")
        .agg(
            F.count("*").alias("tareas_aprobadas"),
            F.expr("percentile_approx(horas, 0.5)").alias("p50_horas"),
            F.expr("percentile_approx(horas, 0.95)").alias("p95_horas"),
            F.round(
                100.0 * F.avg(F.when(F.col("horas") > F.col("sla_horas"), 1.0).otherwise(0.0)),
                2,
            ).alias("pct_fuera_sla"),
        )
        .orderBy("cliente_id")
    )
    return [row.asDict() for row in res.collect()]


CONSULTAS = {
    "q1": ejecutar_q1,
    "q2": ejecutar_q2,
    "q3": ejecutar_q3,
    "q4": ejecutar_q4,
    "q5": ejecutar_q5,
}