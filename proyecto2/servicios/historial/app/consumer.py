"""Base del consumidor de eventos de Valkey Streams.

NO hay worker automatico en este commit, porque aun falta el escritor
Parquet. Confirmar (XACK) antes de publicar el archivo durable perderia
el evento si el proceso falla. En el commit 7 se agregara el writer real.
"""

import json
from collections.abc import Callable

from redis import Redis
from redis.exceptions import ResponseError

from .eventos import EventoNegocio, STREAM_KEY

GRUPO = "historial-parquet"


def asegurar_grupo(cliente: Redis, grupo: str = GRUPO) -> None:
    """Crea el grupo una sola vez sin eliminar eventos existentes."""
    try:
        # ID=0 permite recuperar todos los eventos que ya estan en el Stream.
        cliente.xgroup_create(name=STREAM_KEY, groupname=grupo, id="0", mkstream=True)
    except ResponseError as exc:
        if "BUSYGROUP" not in str(exc):
            raise


def procesar_lote(
    cliente: Redis,
    escribir_y_publicar: Callable[[list[EventoNegocio]], None],
    *,
    consumidor: str,
    grupo: str = GRUPO,
    cantidad: int = 100,
) -> int:
    """Lee un lote y confirma solo DESPUES de la persistencia externa.

    `escribir_y_publicar` debe completar la publicacion atomica de un
    archivo Parquet antes de regresar. No usar callbacks que solo
    mantengan eventos en memoria. La recuperacion de pendientes luego
    de una caida se agregara en el commit 8.
    """
    if cantidad < 1:
        raise ValueError("cantidad debe ser positiva")
    asegurar_grupo(cliente, grupo)
    entregas = cliente.xreadgroup(
        groupname=grupo,
        consumername=consumidor,
        streams={STREAM_KEY: ">"},
        count=cantidad,
        block=1000,
    )
    if not entregas:
        return 0

    # En este contrato cada respuesta incluye las entradas del stream.
    entradas = [entrada for _, lote in entregas for entrada in lote]
    eventos = []
    identificadores = []
    for stream_id, campos in entradas:
        payload = campos.get("payload_json")
        if payload is None:
            raise ValueError("Evento del Stream sin payload_json")
        eventos.append(EventoNegocio.model_validate(json.loads(payload)))
        identificadores.append(stream_id)

    # Si el writer falla, ninguna entrada es reconocida y queda pendiente.
    escribir_y_publicar(eventos)
    cliente.xack(STREAM_KEY, grupo, *identificadores)
    return len(eventos)
