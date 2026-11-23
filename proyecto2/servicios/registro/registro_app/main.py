"""API interna del registro (MongoDB). Seis rutas.

Esta aplicación no publica puertos al host: solo el gateway es accesible para
el simulador. No consulta Valkey: la regla de aislamiento lo prohíbe.
"""

from datetime import datetime, timezone
from uuid import uuid4

from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import JSONResponse
from pymongo import ReadPreference
from pymongo.errors import PyMongoError

from . import transiciones
from .db import ERRORES_INDISPONIBLE, bd, obtener_cliente
from .modelos import NuevoLote, NuevoProyecto, Transicion

app = FastAPI(
    title="Servicio de Registro",
    description="Estado vigente de proyectos, lotes y tareas (MongoDB en réplica)",
    version="0.1.0",
)

SIN_MONGO = "MongoDB no disponible"


def _ahora() -> datetime:
    return datetime.now(timezone.utc)


@app.post("/proyectos", status_code=201)
def crear_proyecto(datos: NuevoProyecto):
    try:
        d = bd()
        if d.clientes.count_documents({"_id": datos.cliente_id}, limit=1) == 0:
            raise HTTPException(status_code=404, detail="Cliente inexistente")
        proyecto = {
            "_id": str(uuid4()),
            "cliente_id": datos.cliente_id,
            "nombre": datos.nombre,
            "taxonomia": [e.model_dump() for e in datos.taxonomia],
            "sla_horas": datos.sla_horas,
            "estado": "activo",
            "creado_en": _ahora(),
        }
        d.proyectos.insert_one(proyecto)
    except ERRORES_INDISPONIBLE:
        raise HTTPException(status_code=503, detail=SIN_MONGO) from None
    return proyecto


@app.post("/proyectos/{proyecto_id}/lotes", status_code=201)
def registrar_lote(proyecto_id: str, datos: NuevoLote):
    try:
        d = bd()
        proyecto = d.proyectos.find_one({"_id": proyecto_id})
        if proyecto is None:
            raise HTTPException(status_code=404, detail="Proyecto inexistente")
        ahora = _ahora()
        batch_id = str(uuid4())
        tareas = [
            {
                "_id": str(uuid4()),
                "proyecto_id": proyecto_id,
                "batch_id": batch_id,
                "cliente_id": proyecto["cliente_id"],
                "modalidad": t.modalidad,
                "tipo_tarea": t.tipo_tarea,
                "archivo": t.archivo.model_dump(),
                "prioridad": t.prioridad,
                "estado": "disponible",
                "version": 1,
                "asignacion": None,
                "anotacion": None,
                "revision": None,
                "ultimo_evento_id": None,
                "eventos_pendientes": [],
                "sla_horas": proyecto["sla_horas"],
                "creado_en": ahora,
            }
            for t in datos.tareas
        ]
        d.batches.insert_one({
            "_id": batch_id,
            "proyecto_id": proyecto_id,
            "nombre": datos.nombre,
            "n_tareas": len(tareas),
            "creado_en": ahora,
        })
        d.tareas.insert_many(tareas, ordered=False)
    except ERRORES_INDISPONIBLE:
        raise HTTPException(status_code=503, detail=SIN_MONGO) from None
    return {
        "batch_id": batch_id,
        "tareas_creadas": len(tareas),
        "tarea_ids": [t["_id"] for t in tareas],
    }


@app.get("/tareas")
def listar_tareas(
    proyecto_id: str | None = None,
    candidatas: bool = False,
    anotador_id: str | None = None,
    estado: str | None = None,
    eventos_pendientes: bool = False,
    limite: int = Query(default=20, ge=1, le=200),
):
    """Tres consultas, mutuamente excluyentes:

    - `eventos_pendientes=true`: tareas con eventos sin confirmar (reconciliación).
    - `anotador_id=...[&estado=...]`: tareas asignadas a un anotador.
    - `proyecto_id=...&candidatas=true`: tareas ofrecibles de un proyecto, por
      prioridad: primero las disponibles y después las abiertas con plazo vencido.
    """
    try:
        t = bd().tareas
        if eventos_pendientes:
            return list(t.find(
                {"eventos_pendientes.0": {"$exists": True}}, {"anotacion": 0}
            ).limit(limite))

        if anotador_id:
            filtro = {"asignacion.anotador_id": anotador_id}
            if estado:
                filtro["estado"] = estado
            return list(t.find(filtro, {"eventos_pendientes": 0}).limit(limite))

        if candidatas and proyecto_id:
            proyeccion = {"anotacion": 0, "eventos_pendientes": 0}
            # Consulta 1: usa el índice (proyecto_id, estado, prioridad).
            resultado = list(
                t.find({"proyecto_id": proyecto_id, "estado": "disponible"}, proyeccion)
                .sort("prioridad", -1).limit(limite)
            )
            if len(resultado) < limite:
                resultado += list(
                    t.find({
                        "proyecto_id": proyecto_id, "estado": "abierta",
                        "asignacion.vence_en": {"$lt": _ahora()},
                    }, proyeccion)
                    .sort("prioridad", -1).limit(limite - len(resultado))
                )
            return resultado
    except ERRORES_INDISPONIBLE:
        raise HTTPException(status_code=503, detail=SIN_MONGO) from None

    raise HTTPException(
        status_code=422,
        detail="Use eventos_pendientes=true, anotador_id=..., o proyecto_id=...&candidatas=true",
    )


@app.post("/tareas/{tarea_id}/transicion")
def transicion(tarea_id: str, datos: Transicion):
    """Aplica UNA acción de forma atómica.

    404: la tarea (o el anotador/revisor) no existe. 403: el actor no es revisor.
    409: la tarea no estaba en el estado esperado o el token/revisor no coincide.
    503: MongoDB no responde. Ojo: un 503 por fallo de w=majority puede haber
    aplicado la escritura en el primario (ver README, limitaciones).
    """
    try:
        doc = transiciones.aplicar(bd(), tarea_id, datos)
    except (transiciones.TareaNoExiste, transiciones.ActorNoExiste) as exc:
        raise HTTPException(status_code=404, detail=f"No existe: {exc}") from None
    except transiciones.ActorNoAutorizado:
        raise HTTPException(status_code=403, detail="El actor no es revisor") from None
    except transiciones.ConflictoEstado as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from None
    except ERRORES_INDISPONIBLE:
        raise HTTPException(status_code=503, detail=SIN_MONGO) from None

    evento = None
    if datos.accion in transiciones.ACCIONES_CON_EVENTO:
        evento = next(
            (e for e in doc.get("eventos_pendientes") or []
             if e["event_id"] == doc.get("ultimo_evento_id")),
            None,
        )
    return {"tarea": doc, "evento": evento}


@app.get("/proyectos/{proyecto_id}/exportar")
def exportar(
    proyecto_id: str,
    despues: str | None = None,
    limite: int = Query(default=100, ge=1, le=1000),
):
    """Tareas aprobadas, paginadas por _id. `siguiente` es el cursor de la próxima página."""
    try:
        d = bd()
        if d.proyectos.count_documents({"_id": proyecto_id}, limit=1) == 0:
            raise HTTPException(status_code=404, detail="Proyecto inexistente")
        filtro = {"proyecto_id": proyecto_id, "estado": "aprobada"}
        if despues:
            filtro["_id"] = {"$gt": despues}
        items = list(
            d.tareas.find(filtro, {"eventos_pendientes": 0}).sort("_id", 1).limit(limite + 1)
        )
    except ERRORES_INDISPONIBLE:
        raise HTTPException(status_code=503, detail=SIN_MONGO) from None
    siguiente = None
    if len(items) > limite:
        items = items[:limite]
        siguiente = items[-1]["_id"]
    return {"items": items, "siguiente": siguiente}


@app.get("/salud")
def salud():
    """operativo (3 sanos), degradado (hay mayoría pero falta un nodo) o caído."""
    try:
        estado = obtener_cliente().admin.command(
            "replSetGetStatus", read_preference=ReadPreference.PRIMARY_PREFERRED
        )
    except PyMongoError:
        return JSONResponse(
            status_code=503,
            content={"servicio": "registro", "estado": "caido", "mongo": "sin respuesta"},
        )
    miembros = [
        {"nombre": m["name"], "estado": m["stateStr"], "salud": m["health"]}
        for m in estado["members"]
    ]
    sanos = sum(1 for m in miembros if m["salud"] == 1 and m["estado"] in ("PRIMARY", "SECONDARY"))
    hay_primario = any(m["estado"] == "PRIMARY" for m in miembros)
    mayoria = sanos >= len(miembros) // 2 + 1
    if not hay_primario or not mayoria:
        nivel, codigo = "caido", 503
    elif sanos < len(miembros):
        nivel, codigo = "degradado", 200
    else:
        nivel, codigo = "operativo", 200
    return JSONResponse(
        status_code=codigo,
        content={"servicio": "registro", "estado": nivel, "miembros": miembros},
    )