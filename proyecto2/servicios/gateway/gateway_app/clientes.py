"""Llamadas HTTP a los servicios internos.

Política de reintentos (enunciado: "reintentos seguros"):
- GET: un reintento solo ante error de conexión o timeout (lectura idempotente).
- POST/PUT: NUNCA se reintentan. Un envío no idempotente repetido podría
  duplicar un proyecto o un lote; el enunciado pide 503 sin reintento.

Cualquier fallo de red hacia un servicio interno se traduce a 503.
"""

import os

import httpx
from fastapi import HTTPException

REGISTRO_URL = os.getenv("REGISTRO_URL", "http://registro:8003")
SESION_URL = os.getenv("SESION_URL", "http://sesion:8001")
HISTORIAL_URL = os.getenv("HISTORIAL_URL", "http://historial:8002")
ANALITICA_URL = os.getenv("ANALITICA_URL", "http://analitica:8004")

TIMEOUT = httpx.Timeout(float(os.getenv("GATEWAY_TIMEOUT_S", "10")), connect=2.0)
TIMEOUT_SALUD = httpx.Timeout(float(os.getenv("GATEWAY_TIMEOUT_SALUD_S", "3")), connect=1.0)
TIMEOUT_ANALITICA = httpx.Timeout(float(os.getenv("GATEWAY_TIMEOUT_ANALITICA_S", "120")), connect=2.0)

_cliente: httpx.AsyncClient | None = None


def cliente() -> httpx.AsyncClient:
    global _cliente
    if _cliente is None:
        _cliente = httpx.AsyncClient(timeout=TIMEOUT)
    return _cliente


async def cerrar() -> None:
    global _cliente
    if _cliente is not None:
        await _cliente.aclose()
        _cliente = None


async def llamar(
    servicio: str, base: str, metodo: str, ruta: str, *,
    request_id: str, json=None, params=None,
    timeout: httpx.Timeout | None = None, reintentar: bool = True,
) -> httpx.Response:
    intentos = 2 if (metodo == "GET" and reintentar) else 1
    ultimo: Exception | None = None
    for _ in range(intentos):
        try:
            return await cliente().request(
                metodo, base + ruta, json=json, params=params,
                headers={"X-Request-ID": request_id},
                timeout=timeout if timeout is not None else httpx.USE_CLIENT_DEFAULT,
            )
        except (httpx.ConnectError, httpx.TimeoutException, httpx.RemoteProtocolError) as exc:
            ultimo = exc
    raise HTTPException(
        status_code=503,
        detail=f"Servicio {servicio} no disponible ({type(ultimo).__name__})",
    )


async def salud(base: str) -> tuple[int | None, dict]:
    """(código, cuerpo) del /salud de un servicio; (None, {...}) si no responde."""
    try:
        r = await cliente().get(base + "/salud", timeout=TIMEOUT_SALUD)
        try:
            cuerpo = r.json()
        except ValueError:
            cuerpo = {"respuesta": r.text[:200]}
        return r.status_code, cuerpo
    except httpx.HTTPError as exc:
        return None, {"error": type(exc).__name__}
