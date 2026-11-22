"""Gateway: único punto de entrada (puerto 8000).

Rutas de registro (proyectos, lotes, exportar), flujo de tareas (siguiente,
borrador, enviar, liberar, revisión, recuperar) y /salud por capa.
Pendiente: /analitica/{q1..q5} (Integrante 3).

El gateway no accede a ninguna base de datos: solo habla HTTP con los
servicios internos. Propaga X-Request-ID (lo genera si no llega).
"""

import asyncio
import os
from contextlib import asynccontextmanager
from typing import Any
from uuid import UUID, uuid4

from fastapi import Body, FastAPI, HTTPException, Query, Request
from fastapi.responses import JSONResponse, Response

from . import clientes, flujo
from .clientes import HISTORIAL_URL, REGISTRO_URL, SESION_URL
from .modelos import Borrador, Envio, DatosSesion, Recuperacion, Revision, SolicitudSiguiente


RECONCILIAR_S = float(os.getenv("GATEWAY_RECONCILIAR_S", "10"))


async def _reconciliador():
    """Cada RECONCILIAR_S segundos reenvía a Historial los eventos pendientes."""
    while True:
        await asyncio.sleep(RECONCILIAR_S)
        try:
            await flujo.reconciliar(f"reconciliador-{uuid4()}")
        except Exception:  # noqa: BLE001 — un fallo aquí no debe tumbar el gateway
            pass


@asynccontextmanager
async def ciclo(_app: FastAPI):
    tarea = asyncio.create_task(_reconciliador()) if RECONCILIAR_S > 0 else None
    yield
    if tarea:
        tarea.cancel()
    await clientes.cerrar()


app = FastAPI(
    title="Gateway — Plataforma de anotación",
    description="Punto único de entrada al backend",
    version="0.2.0",
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


# ---------------------------------------------------------------- flujo de tareas

@app.post("/tareas/siguiente")
async def tarea_siguiente(request: Request, datos: SolicitudSiguiente):
    """Lock + asignación. 200 con tarea, token y borrador; 204 si no hay tareas."""
    d = datos.model_dump(mode="json")
    resultado = await flujo.siguiente(request.state.request_id, d["anotador_id"], d["proyecto_id"])
    if resultado is None:
        return Response(status_code=204)
    return resultado


@app.put("/tareas/{tarea_id}/borrador")
async def guardar_borrador(request: Request, tarea_id: str, datos: Borrador):
    """Guarda el borrador en Valkey y renueva el lock. No toca MongoDB."""
    tid = _uuid(tarea_id, "tarea_id")
    codigo, cuerpo = await flujo.guardar_borrador(
        request.state.request_id, tid, datos.model_dump(mode="json")
    )
    return JSONResponse(status_code=codigo, content=cuerpo)


@app.post("/tareas/{tarea_id}/enviar")
async def enviar(request: Request, tarea_id: str, datos: Envio):
    """200 si MongoDB confirma; 409 token ajeno o tarea no abierta; 503 sin reintento."""
    tid = _uuid(tarea_id, "tarea_id")
    return await flujo.enviar(request.state.request_id, tid, datos.model_dump(mode="json"))


@app.post("/tareas/{tarea_id}/liberar")
async def liberar(request: Request, tarea_id: str, datos: DatosSesion):
    """Devuelve la tarea a la cola y suelta el lock."""
    tid = _uuid(tarea_id, "tarea_id")
    return await flujo.liberar(request.state.request_id, tid, datos.model_dump(mode="json"))


@app.post("/tareas/{tarea_id}/revision")
async def revision(request: Request, tarea_id: str, datos: Revision):
    """Aprueba o rechaza; el rechazo exige motivo y devuelve la tarea a la cola."""
    tid = _uuid(tarea_id, "tarea_id")
    d = datos.model_dump(mode="json")
    return await flujo.revisar(request.state.request_id, tid, d["revisor_id"], d["decision"], d["motivo"])


@app.post("/sesiones/recuperar")
async def recuperar(request: Request, datos: Recuperacion):
    """Sesión y último borrador. `requiere_reasignacion`: pedir /tareas/siguiente."""
    codigo, cuerpo = await flujo.recuperar(request.state.request_id, datos.model_dump(mode="json"))
    return JSONResponse(status_code=codigo, content=cuerpo)


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
