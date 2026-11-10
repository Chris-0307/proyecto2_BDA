"""Consumidor de Valkey Streams con recuperación de mensajes pendientes.

La escritura en Parquet ocurre ANTES de XACK. Ante una caída, las entradas
sin confirmar permanecen en el grupo y un nuevo worker puede reclamarlas
con XAUTOCLAIM tras un intervalo de inactividad configurable.

Este protocolo ofrece procesamiento *al menos una vez*: Spark debe
eliminar duplicados por event_id entre micro-lotes solapados.
"""

import json
from collections.abc import Callable
from typing import Any

from redis import Redis
from redis.exceptions import ResponseError

from .eventos import EventoNegocio, STREAM_KEY

GRUPO = "historial-parquet"
MIN_IDLE_MS = 30_000
MAX_RONDAS_RECLAMACION = 10


def asegurar_grupo(cliente: Redis, grupo: str = GRUPO) -> None:
    """Crea el grupo sin descartar eventos que llegaron antes del worker."""
    try:
        cliente.xgroup_create(name=STREAM_KEY, groupname=grupo, id="0", mkstream=True)
    except ResponseError as exc:
        if "BUSYGROUP" not in str(exc):
            raise


def _recuperar_pendientes(
    cliente: Redis, *, grupo: str, consumidor: str, cantidad: int,
    min_idle_ms: int,
) -> list[tuple[str, dict[str, str]]]:
    """Reclama mensajes inactivos, incluidos los de workers que ya murieron.

    XAUTOCLAIM recorre el PEL con un cursor. Si una página no incluye
    mensajes elegibles, avanza hasta diez páginas para no quedar bloqueado
    en las entradas más recientes. Se procesará como máximo un lote por
    llamada, para que también progresen las solicitudes nuevas.
    """
    cursor = "0-0"
    for _ in range(MAX_RONDAS_RECLAMACION):
        respuesta = cliente.xautoclaim(
            name=STREAM_KEY,
            groupname=grupo,
            consumername=consumidor,
            min_idle_time=min_idle_ms,
            start_id=cursor,
            count=cantidad,
        )
        # redis-py puede devolver [cursor, mensajes] o
        # [cursor, mensajes, ids_eliminados], según la versión de Valkey.
        if not isinstance(respuesta, (tuple, list)) or len(respuesta) < 2:
            raise RuntimeError("Respuesta inesperada de XAUTOCLAIM")
        siguiente, mensajes = respuesta[:2]
        if mensajes:
            return mensajes
        if isinstance(siguiente, bytes):
            siguiente = siguiente.decode("utf-8")
        if siguiente == "0-0" or siguiente == cursor:
            break
        cursor = siguiente
    return []


def _extraer_eventos(
    entradas: list[tuple[str, dict[str, Any]]],
) -> tuple[list[EventoNegocio], list[str]]:
    """Valida todo el lote antes de intentar escribir o confirmar mensajes."""
    eventos: list[EventoNegocio] = []
    identificadores: list[str] = []
    for stream_id, campos in entradas:
        if isinstance(stream_id, bytes):
            stream_id = stream_id.decode("utf-8")
        payload = campos.get("payload_json", campos.get(b"payload_json"))
        if payload is None:
            raise ValueError(f"Evento {stream_id} sin payload_json")
        eventos.append(EventoNegocio.model_validate(json.loads(payload)))
        identificadores.append(stream_id)
    return eventos, identificadores


def procesar_lote(
    cliente: Redis,
    escribir_y_publicar: Callable[[list[EventoNegocio]], object],
    *,
    consumidor: str,
    grupo: str = GRUPO,
    cantidad: int = 100,
    recuperar_pendientes: bool = False,
    min_idle_ms: int = MIN_IDLE_MS,
) -> int:
    """Recupera mensajes inactivos o consume nuevos y publica Parquet.

    La recuperación se activa en el worker (no por defecto) para mantener
    compatibilidad con los consumidores y pruebas del commit anterior.
    Nunca se ejecuta XACK si el escritor falla, la validación falla o el
    proceso termina abruptamente. La recuperación no garantiza exactamente
    una entrega: la deduplicación global corresponde a Spark.
    """
    if not consumidor.strip():
        raise ValueError("consumidor no puede estar vacio")
    if cantidad < 1:
        raise ValueError("cantidad debe ser positiva")
    if min_idle_ms < 1:
        raise ValueError("min_idle_ms debe ser positivo")

    asegurar_grupo(cliente, grupo)
    entradas: list[tuple[str, dict[str, Any]]] = []
    if recuperar_pendientes:
        entradas = _recuperar_pendientes(
            cliente,
            grupo=grupo,
            consumidor=consumidor,
            cantidad=cantidad,
            min_idle_ms=min_idle_ms,
        )

    if not entradas:
        entregas = cliente.xreadgroup(
            groupname=grupo,
            consumername=consumidor,
            streams={STREAM_KEY: ">"},
            count=cantidad,
            block=1000,
        )
        if not entregas:
            return 0
        entradas = [entrada for _, lote in entregas for entrada in lote]
        if not entradas:
            return 0

    eventos, identificadores = _extraer_eventos(entradas)

    # La publicación (incluido fsync + os.replace) debe terminar primero.
    escribir_y_publicar(eventos)
    cliente.xack(STREAM_KEY, grupo, *identificadores)
    return len(eventos)
