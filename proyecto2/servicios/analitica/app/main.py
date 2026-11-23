"""Servicio interno de analítica con Apache Spark (puerto 8004, solo lectura sobre Parquet)."""

from __future__ import annotations

import io
import os
import time
from contextlib import redirect_stdout
from pathlib import Path

from fastapi import FastAPI, HTTPException, Query
from pyspark.sql import SparkSession

from .consultas import CONSULTAS, df_eventos_limpios

RUTA_DATALAKE = os.getenv("DATALAKE_EVENTOS", "/datalake/eventos")
SPARK_WORKERS = int(os.getenv("SPARK_WORKERS", "1"))
SPARK_APP_NAME = os.getenv("SPARK_APP_NAME", "analitica-plataforma")

app = FastAPI(title="Analítica Spark", version="0.1.0")
_spark: SparkSession | None = None


def obtener_spark() -> SparkSession:
    global _spark
    if _spark is None:
        master = f"local[{SPARK_WORKERS}]"
        _spark = (
            SparkSession.builder.appName(SPARK_APP_NAME)
            .master(master)
            .config("spark.sql.shuffle.partitions", str(max(2, SPARK_WORKERS * 2)))
            .config("spark.ui.enabled", "false")
            .getOrCreate()
        )
        _spark.sparkContext.setLogLevel("WARN")
    return _spark


@app.get("/salud")
def salud() -> dict:
    base = Path(RUTA_DATALAKE)
    existe = base.exists()
    n_archivos = sum(1 for _ in base.rglob("*.parquet")) if existe else 0
    return {
        "servicio": "analitica",
        "estado": "operativo" if existe else "degradado",
        "ruta_datalake": str(base),
        "archivos_parquet": n_archivos,
        "spark_workers": SPARK_WORKERS,
    }


@app.get("/q/{consulta}")
def correr_consulta(
    consulta: str,
    conjunto: str = Query(default="principal", pattern="^(principal|grande)$"),
    incluir_plan: bool = Query(default=False),
) -> dict:
    clave = consulta.lower()
    fn = CONSULTAS.get(clave)
    if fn is None:
        raise HTTPException(status_code=404, detail=f"Consulta desconocida: {consulta}")

    t0 = time.perf_counter()
    spark = obtener_spark()
    df_base = df_eventos_limpios(spark, RUTA_DATALAKE, conjunto=conjunto)

    plan_texto = None
    if incluir_plan:
        buf = io.StringIO()
        with redirect_stdout(buf):
            df_base.explain(mode="simple")
        plan_texto = buf.getvalue()

    filas = fn(df_base)
    elapsed_ms = (time.perf_counter() - t0) * 1000.0

    respuesta = {
        "consulta": clave,
        "conjunto": conjunto,
        "spark_workers": SPARK_WORKERS,
        "duracion_ms": round(elapsed_ms, 2),
        "total_filas": len(filas),
        "filas": filas,
    }
    if plan_texto is not None:
        respuesta["plan_explain"] = plan_texto
    return respuesta
