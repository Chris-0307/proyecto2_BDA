"""Transiciones de estado de las tareas, cada una en UNA operación atómica.

Cada acción es un find_one_and_update cuyo filtro exige el estado de origen
(y el token o el revisor cuando corresponde). Si el filtro no coincide, nadie
modificó nada: el llamador recibe 409. Así dos peticiones simultáneas sobre la
misma tarea nunca pueden ganar las dos.

La actualización es un pipeline de agregación para poder construir, en la misma
operación, el evento de historial a partir de los campos de la tarea. Todas las
expresiones de un mismo $set se evalúan contra el documento ANTERIOR, por eso
la versión del evento es la versión anterior + 1, igual que la de la tarea.

Los valores que llegan del cliente se envuelven en $literal: sin eso, un texto
que empiece con "$" se interpretaría como ruta de campo o como operador.
"""

import os
from datetime import datetime, timedelta, timezone
from uuid import uuid4

from pymongo import ReturnDocument
from pymongo.database import Database
from pymongo.collection import Collection

from .modelos import Transicion

# Plazo de una asignación. Es una cota inferior: la guarda real contra dos
# dueños es el lock de Valkey, que el gateway toma ANTES de asignar.
PLAZO_ASIGNACION_S = int(os.getenv("ASIGNACION_PLAZO_S", "120"))

ACCIONES_CON_EVENTO = {
    "asignar", "enviar", "iniciar_revision", "aprobar", "rechazar",
}


class TareaNoExiste(Exception):
    pass


class ConflictoEstado(Exception):
    pass


class ActorNoExiste(Exception):
    pass


class ActorNoAutorizado(Exception):
    pass


def L(valor):
    """Valor literal dentro de una expresión de agregación."""
    return {"$literal": valor}


def _evento(tipo: str, estado: str, ahora: datetime, **extra) -> dict:
    """Expresión del evento (esquema EventoNegocio del servicio Historial)."""
    ev = {
        "event_id": str(uuid4()),
        "tipo_evento": tipo,
        "estado": estado,
        "ocurrido_en": ahora,
        "tarea_id": "$_id",
        "proyecto_id": "$proyecto_id",
        "cliente_id": "$cliente_id",
        "batch_id": "$batch_id",
        "modalidad": "$modalidad",
        "tipo_tarea": "$tipo_tarea",
        "version": {"$add": ["$version", 1]},
        "sla_horas": "$sla_horas",
    }
    ev.update(extra)
    return ev


def _de_la_asignacion() -> dict:
    return {
        "anotador_id": "$asignacion.anotador_id",
        "equipo_id": "$asignacion.equipo_id",
        "tomada_en": "$asignacion.tomada_en",
    }


def _pipeline(cambios: dict, ev: dict | None) -> list:
    """Aplica `cambios`, sube la versión y, si hay evento, lo deja pendiente."""
    campos = dict(cambios)
    campos["version"] = {"$add": ["$version", 1]}
    if ev is not None:
        campos["ultimo_evento_id"] = ev["event_id"]
        campos["eventos_pendientes"] = {
            "$concatArrays": [{"$ifNull": ["$eventos_pendientes", []]}, [ev]]
        }
    return [{"$set": campos}]


def _exigir_actor(bd: Database, actor_id: str, *, revisor: bool = False) -> dict:
    actor = bd.anotadores.find_one({"_id": actor_id})
    if actor is None:
        raise ActorNoExiste(actor_id)
    if revisor and not actor.get("es_revisor"):
        raise ActorNoAutorizado(actor_id)
    return actor


def _construir(bd: Database, tarea_id: str, t: Transicion, ahora: datetime):
    base = {"_id": tarea_id}

    if t.accion == "asignar":
        equipo = _exigir_actor(bd, t.anotador_id).get("equipo_id")
        filtro = {
            **base,
            "$or": [
                {"estado": "disponible"},
                {"estado": "abierta", "asignacion.vence_en": {"$lt": ahora}},
            ],
        }
        ev = _evento(
            "tarea_abierta", "abierta", ahora,
            anotador_id=L(t.anotador_id), equipo_id=L(equipo), tomada_en=ahora,
        )
        cambios = {
            "estado": "abierta",
            "asignacion": {
                "anotador_id": L(t.anotador_id),
                "equipo_id": L(equipo),
                "token": L(t.token),
                "tomada_en": ahora,
                "vence_en": ahora + timedelta(seconds=PLAZO_ASIGNACION_S),
            },
            "anotacion": L(None),
        }
        return filtro, _pipeline(cambios, ev)

    if t.accion == "enviar":
        filtro = {
            **base, "estado": "abierta",
            "asignacion.token": t.token,
            "asignacion.anotador_id": t.anotador_id,
        }
        ev = _evento(
            "tarea_anotada", "anotada", ahora,
            **_de_la_asignacion(),
            duracion_ms={"$dateDiff": {
                "startDate": "$asignacion.tomada_en", "endDate": ahora,
                "unit": "millisecond",
            }},
        )
        cambios = {
            "estado": "anotada",
            "anotacion": {"contenido": L(t.contenido), "enviada_en": ahora},
        }
        return filtro, _pipeline(cambios, ev)

    if t.accion == "liberar":
        filtro = {
            **base, "estado": "abierta",
            "asignacion.token": t.token,
            "asignacion.anotador_id": t.anotador_id,
        }
        # Sin evento: el enunciado no registra la liberación en el datalake.
        return filtro, _pipeline({"estado": "disponible", "asignacion": L(None)}, None)

    if t.accion == "iniciar_revision":
        _exigir_actor(bd, t.revisor_id, revisor=True)
        filtro = {**base, "estado": "anotada"}
        ev = _evento(
            "revision_iniciada", "en_revision", ahora,
            **_de_la_asignacion(), revisor_id=L(t.revisor_id),
        )
        cambios = {
            "estado": "en_revision",
            "revision": {"revisor_id": L(t.revisor_id), "iniciada_en": ahora},
        }
        return filtro, _pipeline(cambios, ev)

    if t.accion == "aprobar":
        filtro = {**base, "estado": "en_revision", "revision.revisor_id": t.revisor_id}
        ev = _evento(
            "revision_aprobada", "aprobada", ahora,
            **_de_la_asignacion(), revisor_id=L(t.revisor_id), aprobada_en=ahora,
        )
        cambios = {
            "estado": "aprobada",
            "revision": {
                "revisor_id": L(t.revisor_id),
                "iniciada_en": "$revision.iniciada_en",
                "resultado": "aprobada",
                "motivo": L(None),
                "revisada_en": ahora,
            },
        }
        return filtro, _pipeline(cambios, ev)

    if t.accion == "rechazar":
        filtro = {**base, "estado": "en_revision", "revision.revisor_id": t.revisor_id}
        ev = _evento(
            "revision_rechazada", "rechazada", ahora,
            **_de_la_asignacion(), revisor_id=L(t.revisor_id),
            motivo_rechazo=L(t.motivo),
        )
        # En MongoDB la tarea vuelve a la cola; en Parquet la fila es "rechazada".
        cambios = {
            "estado": "disponible",
            "asignacion": L(None),
            "anotacion": L(None),
            "revision": {
                "revisor_id": L(t.revisor_id),
                "iniciada_en": "$revision.iniciada_en",
                "resultado": "rechazada",
                "motivo": L(t.motivo),
                "revisada_en": ahora,
            },
        }
        return filtro, _pipeline(cambios, ev)

    raise ValueError(f"Acción desconocida: {t.accion}")


def _confirmar(tareas: Collection, tarea_id: str, event_id: str) -> dict:
    """Retira un evento ya publicado. Idempotente: repetirlo no falla."""
    pipeline = [{"$set": {"eventos_pendientes": {"$filter": {
        "input": {"$ifNull": ["$eventos_pendientes", []]},
        "as": "e",
        "cond": {"$ne": ["$$e.event_id", L(event_id)]},
    }}}}]
    doc = tareas.find_one_and_update(
        {"_id": tarea_id}, pipeline, return_document=ReturnDocument.AFTER
    )
    if doc is None:
        raise TareaNoExiste(tarea_id)
    return doc


def aplicar(bd: Database, tarea_id: str, t: Transicion) -> dict:
    """Ejecuta la transición y devuelve la tarea ya actualizada."""
    tareas = bd.tareas
    if t.accion == "confirmar_evento":
        return _confirmar(tareas, tarea_id, t.event_id)

    ahora = datetime.now(timezone.utc)
    filtro, pipeline = _construir(bd, tarea_id, t, ahora)
    doc = tareas.find_one_and_update(
        filtro, pipeline, return_document=ReturnDocument.AFTER
    )
    if doc is None:
        if tareas.count_documents({"_id": tarea_id}, limit=1) == 0:
            raise TareaNoExiste(tarea_id)
        raise ConflictoEstado(
            f"La tarea no está en el estado esperado para '{t.accion}', "
            "o el token/revisor no coincide"
        )
    return doc