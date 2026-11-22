"""Salud por capa con mensajes en Valkey.

Un monitor en segundo plano consulta cada SALUD_INTERVALO_S segundos el /salud
de cada capa y publica el resultado en Valkey:

- `salud:capa:{capa}`: último estado (JSON) con expiración SALUD_TTL_S. Si el
  monitor deja de publicar, el estado expira y no queda un valor viejo.
- `salud:eventos`: stream con cada cambio de estado (`mensaje` = capa:antes->ahora;
  `inicio` cuando no había estado previo).

GET /salud lee esos mensajes. Si Valkey no responde o aún no hay mensajes de
todas las capas, consulta cada capa directamente (fuente `directo`).

El gateway usa su propia base de Valkey (SALUD_VALKEY_URL, base 2) y solo
llaves `salud:*`: no toca los datos de Sesión ni de Historial (base 0).
"""

import asyncio
import json
import os
from datetime import datetime, timezone

from redis.asyncio import Redis
from redis.exceptions import RedisError

from . import clientes
from .clientes import HISTORIAL_URL, REGISTRO_URL, SESION_URL

VALKEY_URL = os.getenv("SALUD_VALKEY_URL", "redis://valkey:6379/2")
INTERVALO_S = float(os.getenv("SALUD_INTERVALO_S", "5"))
TTL_S = int(os.getenv("SALUD_TTL_S", "15"))
CAPAS = {"registro": REGISTRO_URL, "sesion": SESION_URL, "historial": HISTORIAL_URL}
STREAM_MAXLEN = 1000

_valkey: Redis | None = None


def valkey() -> Redis:
    global _valkey
    if _valkey is None:
        _valkey = Redis.from_url(VALKEY_URL, decode_responses=True,
                                 socket_connect_timeout=1, socket_timeout=1)
    return _valkey


async def cerrar() -> None:
    global _valkey
    if _valkey is not None:
        await _valkey.aclose()
        _valkey = None


def estado_capa(codigo: int | None, cuerpo: dict) -> str:
    if codigo is None:
        return "caido"
    estado = cuerpo.get("estado")
    if codigo == 200 and estado in ("operativo", "degradado"):
        return estado
    return "caido"


async def sondear() -> dict[str, dict]:
    """Consulta el /salud de cada capa en paralelo."""
    ahora = datetime.now(timezone.utc).isoformat()
    respuestas = await asyncio.gather(*(clientes.salud(url) for url in CAPAS.values()))
    return {
        capa: {"estado": estado_capa(c, b), "detalle": b, "revisado_en": ahora}
        for capa, (c, b) in zip(CAPAS, respuestas)
    }


async def publicar(capas: dict[str, dict]) -> None:
    """Escribe el estado de cada capa y registra los cambios en el stream."""
    v = valkey()
    anteriores = await v.mget([f"salud:capa:{c}" for c in capas])
    for (capa, dato), previo in zip(capas.items(), anteriores):
        antes = json.loads(previo)["estado"] if previo else "inicio"
        await v.set(f"salud:capa:{capa}", json.dumps(dato, ensure_ascii=False), ex=TTL_S)
        if antes != dato["estado"]:
            await v.xadd("salud:eventos", {
                "capa": capa, "anterior": antes, "estado": dato["estado"],
                "mensaje": f"{capa}:{antes}->{dato['estado']}",
                "ocurrido_en": dato["revisado_en"],
            }, maxlen=STREAM_MAXLEN, approximate=True)


async def leer() -> dict[str, dict] | None:
    """Estados publicados en Valkey; None si falta alguna capa o Valkey no responde."""
    try:
        valores = await valkey().mget([f"salud:capa:{c}" for c in CAPAS])
    except (RedisError, OSError):
        return None
    if any(v is None for v in valores):
        return None
    return {c: json.loads(v) for c, v in zip(CAPAS, valores)}


async def monitor() -> None:
    """Bucle del monitor: sondea y publica; un fallo de Valkey no lo detiene."""
    while True:
        try:
            await publicar(await sondear())
        except (RedisError, OSError):
            pass
        await asyncio.sleep(INTERVALO_S)


async def estado_global() -> tuple[int, dict]:
    """(código HTTP, cuerpo) de GET /salud."""
    capas, fuente = await leer(), "valkey"
    if capas is None:
        capas, fuente = await sondear(), "directo"
    estados = [c["estado"] for c in capas.values()]
    if capas["registro"]["estado"] == "caido":
        nivel, codigo = "caido", 503
    elif all(e == "operativo" for e in estados):
        nivel, codigo = "operativo", 200
    else:
        nivel, codigo = "degradado", 200
    return codigo, {"servicio": "gateway", "estado": nivel, "fuente": fuente, "capas": capas}
