"""Validacion y recepcion idempotente de eventos de negocio.

El gateway publica estos eventos unicamente despues de la confirmacion de
MongoDB. Este servicio no accede a MongoDB ni genera filas para borradores.

La cola se implementa con Valkey Streams y AOF; la escritura de Parquet
se incorporara en el commit 7.
"""

import hashlib
import json
from datetime import datetime, timezone
from typing import Any, Literal
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator
from redis import Redis


TipoEvento = Literal[
    "tarea_abierta",
    "tarea_anotada",
    "revision_iniciada",
    "revision_aprobada",
    "revision_rechazada",
]
EstadoHistorico = Literal[
    "abierta", "anotada", "en_revision", "aprobada", "rechazada"
]
Modalidad = Literal["texto", "imagen", "video", "lidar"]

ESTADO_POR_EVENTO = {
    "tarea_abierta": "abierta",
    "tarea_anotada": "anotada",
    "revision_iniciada": "en_revision",
    "revision_aprobada": "aprobada",
    "revision_rechazada": "rechazada",
}

# Llaves separadas para la cola y la deteccion de reintentos por event_id.
STREAM_KEY = "historial:eventos"
IDS_KEY = "historial:ids"


class EventoNegocio(BaseModel):
    """Contrato preliminar acordable con Registro y Analitica.

    Se incluyen las dimensiones necesarias para las consultas historicas.
    Los valores concretos deben provenir de Registro, no de este servicio.
    """

    model_config = ConfigDict(extra="forbid")

    event_id: UUID
    tipo_evento: TipoEvento
    estado: EstadoHistorico
    ocurrido_en: datetime

    tarea_id: UUID
    proyecto_id: UUID
    cliente_id: UUID
    batch_id: UUID | None = None
    anotador_id: UUID | None = None
    revisor_id: UUID | None = None
    equipo_id: UUID | None = None

    modalidad: Modalidad
    tipo_tarea: str = Field(min_length=1, max_length=128)
    version: int = Field(ge=1)

    # Valores opcionales: no se inventan cuando Registro no los conoce.
    tomada_en: datetime | None = None
    aprobada_en: datetime | None = None
    duracion_ms: int | None = Field(default=None, ge=0)
    sla_horas: float | None = Field(default=None, gt=0)
    motivo_rechazo: str | None = Field(default=None, min_length=1, max_length=128)
    datos_extra: dict[str, Any] = Field(default_factory=dict)

    @field_validator("ocurrido_en", "tomada_en", "aprobada_en")
    @classmethod
    def exigir_zona_horaria(cls, valor: datetime | None) -> datetime | None:
        if valor is None:
            return None
        if valor.tzinfo is None or valor.utcoffset() is None:
            raise ValueError("Las fechas deben incluir zona horaria")
        return valor.astimezone(timezone.utc)

    @model_validator(mode="after")
    def comprobar_transicion(self):
        esperado = ESTADO_POR_EVENTO[self.tipo_evento]
        if self.estado != esperado:
            raise ValueError(
                f"El evento {self.tipo_evento} debe tener estado {esperado}"
            )
        if self.tipo_evento == "revision_rechazada" and not self.motivo_rechazo:
            raise ValueError("Un rechazo debe incluir motivo_rechazo")
        return self


class EventoRecibido(BaseModel):
    """Resultado de la recepcion en la cola Valkey."""

    event_id: UUID
    stream_id: str
    resultado: Literal["encolado", "repetido"]


# El script hace en una transaccion atomica: buscar event_id, verificar
# que su contenido sea el mismo, publicar en Stream y registrar su ID.
# Devuelve: 1 nuevo, 0 repetido con mismo contenido, 2 ID reutilizado.
PUBLICAR_LUA = """
local stream = KEYS[1]
local ids = KEYS[2]
local event_id = ARGV[1]
local payload = ARGV[2]
local huella = ARGV[3]

local anterior = redis.call('HGET', ids, event_id)
if anterior then
    local huella_previa = string.sub(anterior, 1, 64)
    local stream_id_previo = string.sub(anterior, 66)
    if huella_previa ~= huella then
        return {2, stream_id_previo}
    end
    return {0, stream_id_previo}
end

local stream_id = redis.call('XADD', stream, '*',
    'event_id', event_id,
    'payload_json', payload)
redis.call('HSET', ids, event_id, huella .. ':' .. stream_id)
return {1, stream_id}
"""


class EventoDuplicadoDistinto(Exception):
    """Se intenta reutilizar event_id con otros datos."""


def serializar_evento(evento: EventoNegocio) -> str:
    """Representacion canonica para comparar los reintentos."""
    return json.dumps(
        evento.model_dump(mode="json"),
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
        allow_nan=False,
    )


def publicar_evento(cliente: Redis, evento: EventoNegocio) -> EventoRecibido:
    """Agrega el evento a Valkey Stream sin duplicarlo por event_id.

    Se responde despues de que Valkey confirma XADD. No significa que
    Parquet ya lo haya materializado ni que exista transaccion distribuida.
    """
    payload = serializar_evento(evento)
    huella = hashlib.sha256(payload.encode("utf-8")).hexdigest()
    codigo, stream_id = cliente.eval(
        PUBLICAR_LUA,
        2,
        STREAM_KEY,
        IDS_KEY,
        str(evento.event_id),
        payload,
        huella,
    )
    codigo = int(codigo)
    if isinstance(stream_id, bytes):
        stream_id = stream_id.decode("utf-8")
    if codigo == 2:
        raise EventoDuplicadoDistinto("event_id ya existe con otro contenido")
    if codigo not in (0, 1):
        raise RuntimeError(f"Respuesta no esperada de Valkey: {codigo}")
    return EventoRecibido(
        event_id=evento.event_id,
        stream_id=str(stream_id),
        resultado="encolado" if codigo == 1 else "repetido",
    )
