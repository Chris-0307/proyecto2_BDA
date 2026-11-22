"""Cuerpos de las rutas del flujo de tareas (validación básica del gateway)."""

from typing import Any, Literal
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, model_validator


class _Base(BaseModel):
    model_config = ConfigDict(extra="forbid")


class SolicitudSiguiente(_Base):
    anotador_id: UUID
    proyecto_id: UUID


class DatosSesion(_Base):
    """Identidad de la sesión: la entrega /tareas/siguiente."""
    anotador_id: UUID
    proyecto_id: UUID
    token: str = Field(min_length=1, max_length=256)


class Borrador(DatosSesion):
    contenido: dict[str, Any]


class Envio(DatosSesion):
    contenido: dict[str, Any]


class Recuperacion(DatosSesion):
    tarea_id: UUID


class Revision(_Base):
    revisor_id: UUID
    decision: Literal["aprobar", "rechazar"]
    motivo: str | None = Field(default=None, min_length=1, max_length=128)

    @model_validator(mode="after")
    def rechazo_con_motivo(self):
        if self.decision == "rechazar" and not self.motivo:
            raise ValueError("El rechazo requiere un código de motivo")
        return self
