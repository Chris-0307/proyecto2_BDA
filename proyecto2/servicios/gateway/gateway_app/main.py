"""Gateway: único punto de entrada (puerto 8000).

Paso 4 (básico): rutas que solo necesitan el registro, más /salud por capa.
Pendientes (coordinan Sesión e Historial): /tareas/siguiente, borrador,
enviar, liberar, revisión, /sesiones/recuperar y /analitica/{q}.

El gateway no accede a ninguna base de datos: solo habla HTTP con los
servicios internos. Propaga X-Request-ID (lo genera si no llega).
"""

import asyncio
from contextlib import asynccontextmanager
from typing import Any
from uuid import UUID, uuid4

from fastapi import Body, FastAPI, HTTPException, Query, Request
from fastapi.responses import JSONResponse, Response

from . import clientes
from .clientes import HISTORIAL_URL, REGISTRO_URL, SESION_URL


@asynccontextmanager
async def ciclo(_app: FastAPI):
    yield
    await clientes.cerrar()


app = FastAPI(
    title="Gateway — Plataforma de anotación",
    description="Punto único de entrada al backend",
    version="0.1.0",
    lifespan=ciclo,
)


@app.middleware("http")
async def request_id(request: Request, call_next):
    rid = request.headers.get("X-Request-ID") or str(uuid4())
    request.state.request_id = rid
    respuesta = await call_next(request)
    respuesta.headers["X-Request-ID"] = rid
    return respuesta


def _uuid(valor: str, nombre: str) -> str:
    try:
        return str(UUID(valor))
    except ValueError:
        raise HTTPException(status_code=422, detail=f"{nombre} debe ser un UUID") from None


def _reenviar(r) -> Response:
    """Devuelve tal cual el código y el cuerpo del servicio interno."""
    return Response(
        content=r.content, status_code=r.status_code,
        media_type=r.headers.get("content-type", "application/json"),
    )


# ---------------------------------------------------------------- registro

@app.post("/proyectos", status_code=201)
async def crear_proyecto(request: Request, cuerpo: dict[str, Any] = Body(...)):
    r = await clientes.llamar(
        "registro", REGISTRO_URL, "POST", "/proyectos",
        request_id=request.state.request_id, json=cuerpo,
    )
    return _reenviar(r)


@app.post("/proyectos/{proyecto_id}/lotes", status_code=201)
async def registrar_lote(request: Request, proyecto_id: str, cuerpo: dict[str, Any] = Body(...)):
    pid = _uuid(proyecto_id, "proyecto_id")
    r = await clientes.llamar(
        "registro", REGISTRO_URL, "POST", f"/proyectos/{pid}/lotes",
        request_id=request.state.request_id, json=cuerpo,
    )
    return _reenviar(r)


@app.get("/proyectos/{proyecto_id}/exportar")
async def exportar(
    request: Request,
    proyecto_id: str,
    despues: str | None = None,
    limite: int = Query(default=100, ge=1, le=1000),
):
    pid = _uuid(proyecto_id, "proyecto_id")
    params = {"limite": limite}
    if despues:
        params["despues"] = _uuid(despues, "despues")
    r = await clientes.llamar(
        "registro", REGISTRO_URL, "GET", f"/proyectos/{pid}/exportar",
        request_id=request.state.request_id, params=params,
    )
    return _reenviar(r)


# ---------------------------------------------------------------- salud

def _estado_capa(codigo: int | None, cuerpo: dict) -> str:
    if codigo is None:
        return "caido"
    estado = cuerpo.get("estado")
    if codigo == 200 and estado in ("operativo", "degradado"):
        return estado
    return "caido"


@app.get("/salud")
async def salud():
    """Salud por capa. El gateway responde siempre, aunque las capas fallen.

    - caido (503): el registro (fuente de verdad) no está disponible.
    - degradado (200): el registro responde, pero alguna capa falla o MongoDB
      perdió un nodo.
    - operativo (200): todas las capas operativas.
    """
    (cr, br), (cs, bs), (ch, bh) = await asyncio.gather(
        clientes.salud(REGISTRO_URL),
        clientes.salud(SESION_URL),
        clientes.salud(HISTORIAL_URL),
    )
    capas = {
        "registro": {"estado": _estado_capa(cr, br), "detalle": br},
        "sesion": {"estado": _estado_capa(cs, bs), "detalle": bs},
        "historial": {"estado": _estado_capa(ch, bh), "detalle": bh},
    }
    estados = [c["estado"] for c in capas.values()]
    if capas["registro"]["estado"] == "caido":
        nivel, codigo = "caido", 503
    elif all(e == "operativo" for e in estados):
        nivel, codigo = "operativo", 200
    else:
        nivel, codigo = "degradado", 200
    return JSONResponse(
        status_code=codigo,
        content={"servicio": "gateway", "estado": nivel, "capas": capas},
    )
