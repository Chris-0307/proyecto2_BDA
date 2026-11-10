"""API REST interna para recibir eventos historicos del gateway.

Esta aplicacion no publica puertos al host: solo el gateway debe ser accesible
para el simulador. Un worker independiente materializa Parquet.
"""

import os

from fastapi import FastAPI, HTTPException
from fastapi.responses import JSONResponse
from redis import Redis, RedisError

from .eventos import (
    EventoDuplicadoDistinto,
    EventoNegocio,
    EventoRecibido,
    publicar_evento,
)

app = FastAPI(
    title="Servicio de Historial",
    description="Recepcion idempotente de eventos mediante Valkey Streams",
    version="0.2.0",
)

VALKEY_URL = os.getenv("VALKEY_URL", "redis://valkey:6379/0")


def obtener_cliente() -> Redis:
    """Cliente por peticion y con timeouts para evitar bloqueos prolongados."""
    return Redis.from_url(
        VALKEY_URL,
        decode_responses=True,
        socket_connect_timeout=2,
        socket_timeout=2,
    )


@app.get("/salud")
def salud():
    """Verifica la conexion del servicio con Valkey."""
    try:
        with obtener_cliente() as cliente:
            cliente.ping()
        return {"servicio": "historial", "estado": "operativo", "valkey": "disponible"}
    except (RedisError, OSError, ValueError):
        return JSONResponse(
            status_code=503,
            content={
                "servicio": "historial",
                "estado": "degradado",
                "valkey": "no disponible",
            },
        )


@app.post("/eventos", response_model=EventoRecibido)
def recibir_evento(evento: EventoNegocio):
    """Encola una transicion confirmada por MongoDB (gateway/registro).

    202: evento nuevo en Valkey Stream.
    200: reintento del mismo evento (sin duplicar).
    409: event_id reutilizado con diferente contenido.
    503: Valkey no disponible.
    422: evento invalido, manejado por FastAPI/Pydantic.
    """
    try:
        with obtener_cliente() as cliente:
            resultado = publicar_evento(cliente, evento)
    except EventoDuplicadoDistinto as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from None
    except (RedisError, OSError, ValueError, RuntimeError):
        raise HTTPException(status_code=503, detail="Cola de historial no disponible") from None

    return JSONResponse(
        status_code=202 if resultado.resultado == "encolado" else 200,
        content=resultado.model_dump(mode="json"),
    )
