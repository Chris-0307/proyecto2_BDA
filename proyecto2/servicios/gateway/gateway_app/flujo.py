"""Orquestación del flujo de una tarea: Sesión (lock) + Registro (estado) + Historial.

Orden de cada operación:
- siguiente: lock en Sesión (SET NX PX) -> asignar en Registro. Si Registro
  no asigna, se suelta el lock y se prueba la siguiente candidata.
- enviar:    latido en Sesión (token = dueño del lock) -> enviar en Registro
             -> publicar evento -> finalizar sesión.
- liberar:   liberar en Registro -> finalizar sesión.
- revisión:  iniciar_revision -> aprobar/rechazar en Registro -> publicar eventos.

Los eventos se publican en Historial SOLO después de que MongoDB confirmó la
transición. Si Historial falla, la respuesta al cliente no cambia (MongoDB ya
confirmó): el evento queda en `eventos_pendientes` del Registro para
reconciliarse después.
"""

from typing import Any
from uuid import uuid4

from fastapi import HTTPException

from . import clientes
from .clientes import HISTORIAL_URL, REGISTRO_URL, SESION_URL

CANDIDATAS = 20
ANOTADOR_OCUPADO = "El anotador ya tiene una sesion activa"


def _detalle(r) -> Any:
    try:
        return r.json().get("detail", r.text)
    except ValueError:
        return r.text


def _error(r) -> HTTPException:
    """Propaga el código del servicio interno (403, 404, 409, 422, 503...)."""
    return HTTPException(status_code=r.status_code, detail=_detalle(r))


def _limpiar(tarea: dict) -> dict:
    """La cola interna de eventos no se expone al cliente."""
    return {k: v for k, v in tarea.items() if k != "eventos_pendientes"}


async def _registro(rid: str, metodo: str, ruta: str, **kw):
    return await clientes.llamar("registro", REGISTRO_URL, metodo, ruta, request_id=rid, **kw)


async def _sesion(rid: str, ruta: str, cuerpo: dict, metodo: str = "POST"):
    return await clientes.llamar("sesion", SESION_URL, metodo, ruta, request_id=rid, json=cuerpo)


async def _transicion(rid: str, tarea_id: str, **cuerpo):
    return await _registro(rid, "POST", f"/tareas/{tarea_id}/transicion", json=cuerpo)


async def _soltar_lock(rid: str, sesion: dict) -> None:
    """Libera el lock propio; nunca falla hacia el cliente."""
    try:
        await _sesion(rid, "/sesiones/finalizar", sesion)
    except HTTPException:
        pass


async def publicar_evento(rid: str, tarea_id: str, evento: dict | None) -> str:
    """Publica en Historial y, si lo acepta, confirma en Registro.

    publicado: Historial lo aceptó y Registro lo retiró de pendientes.
    publicado_sin_confirmar: Historial lo aceptó; sigue en pendientes
        (reenviarlo es seguro: Historial deduplica por event_id).
    pendiente: Historial no lo aceptó; queda en pendientes del Registro.
    """
    if not evento:
        return "sin_evento"
    try:
        r = await clientes.llamar(
            "historial", HISTORIAL_URL, "POST", "/eventos", request_id=rid, json=evento
        )
    except HTTPException:
        return "pendiente"
    if r.status_code not in (200, 202):
        return "pendiente"
    try:
        c = await _transicion(rid, tarea_id, accion="confirmar_evento", event_id=evento["event_id"])
    except HTTPException:
        return "publicado_sin_confirmar"
    return "publicado" if c.status_code == 200 else "publicado_sin_confirmar"


# ---------------------------------------------------------------- siguiente

async def siguiente(rid: str, anotador_id: str, proyecto_id: str) -> dict | None:
    """Asigna una tarea al anotador. None si no hay ninguna disponible (204)."""
    r = await _registro(rid, "GET", "/tareas", params={
        "proyecto_id": proyecto_id, "candidatas": "true", "limite": CANDIDATAS,
    })
    if r.status_code != 200:
        raise _error(r)

    token = str(uuid4())
    for candidata in r.json():
        tarea_id = candidata["_id"]
        sesion = {
            "anotador_id": anotador_id, "tarea_id": tarea_id,
            "proyecto_id": proyecto_id, "token": token,
        }

        # 1) Lock exclusivo en Valkey.
        s = await _sesion(rid, "/sesiones/iniciar", {**sesion, "etapa": "anotacion"})
        if s.status_code == 409:
            if _detalle(s) == ANOTADOR_OCUPADO:
                raise HTTPException(
                    status_code=409,
                    detail="El anotador ya tiene una tarea activa: recupérela, envíela o libérela",
                )
            continue  # otra petición tiene el lock de esta tarea
        if s.status_code != 201:
            raise _error(s)

        # 2) Asignación atómica en MongoDB. Si falla, se suelta el lock.
        try:
            a = await _transicion(rid, tarea_id, accion="asignar",
                                  anotador_id=anotador_id, token=token)
        except HTTPException:
            await _soltar_lock(rid, sesion)
            raise
        if a.status_code == 409:
            await _soltar_lock(rid, sesion)
            continue  # el estado cambió entre la consulta y la asignación
        if a.status_code != 200:
            await _soltar_lock(rid, sesion)
            raise _error(a)

        cuerpo = a.json()
        estado_evento = await publicar_evento(rid, tarea_id, cuerpo.get("evento"))

        # 3) Borrador recuperable del mismo anotador (p. ej. tras un vencimiento).
        borrador = None
        try:
            rec = await _sesion(rid, "/sesiones/recuperar", sesion)
            if rec.status_code == 200:
                borrador = rec.json().get("borrador")
        except HTTPException:
            pass

        return {
            "tarea": _limpiar(cuerpo["tarea"]),
            "token": token,
            "ttl_ms": s.json().get("ttl_ms"),
            "borrador": borrador,
            "evento": estado_evento,
        }
    return None


# ---------------------------------------------------------------- borrador

async def guardar_borrador(rid: str, tarea_id: str, datos: dict) -> tuple[int, Any]:
    r = await _sesion(rid, f"/tareas/{tarea_id}/borrador", {**datos, "tarea_id": tarea_id}, "PUT")
    return r.status_code, r.json()


# ---------------------------------------------------------------- enviar

async def enviar(rid: str, tarea_id: str, datos: dict) -> dict:
    sesion = {k: datos[k] for k in ("anotador_id", "proyecto_id", "token")} | {"tarea_id": tarea_id}

    # 1) El token debe ser el dueño del lock vigente.
    s = await _sesion(rid, "/sesiones/latido", sesion)
    if s.status_code == 409:
        raise HTTPException(status_code=409, detail="El token no corresponde al lock de la tarea")
    if s.status_code != 200:
        raise _error(s)

    # 2) MongoDB: abierta -> anotada. 503 sin reintento; el borrador sigue en Valkey.
    r = await _transicion(rid, tarea_id, accion="enviar", anotador_id=datos["anotador_id"],
                          token=datos["token"], contenido=datos["contenido"])
    if r.status_code != 200:
        raise _error(r)

    cuerpo = r.json()
    estado_evento = await publicar_evento(rid, tarea_id, cuerpo.get("evento"))
    await _soltar_lock(rid, sesion)
    return {"tarea": _limpiar(cuerpo["tarea"]), "evento": estado_evento}


# ---------------------------------------------------------------- liberar

async def liberar(rid: str, tarea_id: str, datos: dict) -> dict:
    sesion = {k: datos[k] for k in ("anotador_id", "proyecto_id", "token")} | {"tarea_id": tarea_id}
    r = await _transicion(rid, tarea_id, accion="liberar",
                          anotador_id=datos["anotador_id"], token=datos["token"])
    if r.status_code != 200:
        raise _error(r)
    await _soltar_lock(rid, sesion)
    return {"tarea": _limpiar(r.json()["tarea"])}


# ---------------------------------------------------------------- revisión

async def revisar(rid: str, tarea_id: str, revisor_id: str, decision: str,
                  motivo: str | None) -> dict:
    eventos = []
    i = await _transicion(rid, tarea_id, accion="iniciar_revision", revisor_id=revisor_id)
    if i.status_code == 200:
        eventos.append(await publicar_evento(rid, tarea_id, i.json().get("evento")))
    elif i.status_code != 409:
        raise _error(i)
    # 409 en iniciar: puede estar ya en_revision por este revisor; la decisión
    # lo valida (filtro por estado y revisor) y responde 409 si no corresponde.

    cuerpo = {"accion": decision, "revisor_id": revisor_id}
    if decision == "rechazar":
        cuerpo["motivo"] = motivo
    d = await _transicion(rid, tarea_id, **cuerpo)
    if d.status_code != 200:
        raise _error(d)
    eventos.append(await publicar_evento(rid, tarea_id, d.json().get("evento")))
    return {"tarea": _limpiar(d.json()["tarea"]), "eventos": eventos}


# ---------------------------------------------------------------- recuperar

async def recuperar(rid: str, datos: dict) -> tuple[int, Any]:
    r = await _sesion(rid, "/sesiones/recuperar", datos)
    return r.status_code, r.json()


# ---------------------------------------------------------------- reconciliación

async def reconciliar(rid: str, limite: int = 200) -> int:
    """Reenvía los eventos que quedaron pendientes (Historial caído, gateway
    reiniciado entre la transición y la publicación). Es seguro repetirlo:
    Historial deduplica por event_id y confirmar_evento es idempotente.
    Devuelve cuántos eventos quedaron publicados y confirmados.
    """
    r = await _registro(rid, "GET", "/tareas",
                        params={"eventos_pendientes": "true", "limite": limite})
    if r.status_code != 200:
        return 0
    confirmados = 0
    for tarea in r.json():
        for evento in tarea.get("eventos_pendientes") or []:
            if await publicar_evento(rid, tarea["_id"], evento) == "publicado":
                confirmados += 1
    return confirmados
