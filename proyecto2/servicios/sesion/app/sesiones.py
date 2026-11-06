"""Modelos de entrada y reglas del servicio de sesiones.

La capa REST y el gateway llaman a esta logica; este servicio no consulta
MongoDB ni decide las transiciones definitivas de las tareas.
"""

from typing import Literal

from pydantic import BaseModel, Field
from redis import Redis

from . import locks


class DatosSesion(BaseModel):
    """Identificadores recibidos del gateway para una sesion."""

    anotador_id: str = Field(min_length=1, max_length=128)
    tarea_id: str = Field(min_length=1, max_length=128)
    proyecto_id: str = Field(min_length=1, max_length=128)
    token: str = Field(min_length=1, max_length=256)


class InicioSesion(DatosSesion):
    """Solicitud de apertura, con etapa inicial de anotacion."""

    etapa: str = Field(default="anotacion", min_length=1, max_length=64)


class ResultadoSesion(BaseModel):
    estado: Literal["activa", "renovada", "finalizada"]
    tarea_id: str
    anotador_id: str
    ttl_ms: int | None = None


class ServicioSesiones:
    """Encapsula las operaciones atómicas de Valkey."""

    def __init__(self, cliente: Redis):
        self.cliente = cliente

    def iniciar(self, datos: InicioSesion) -> int:
        return locks.adquirir(self.cliente, **datos.model_dump())

    def latido(self, datos: DatosSesion) -> bool:
        return locks.renovar(self.cliente, **datos.model_dump())

    def finalizar(self, datos: DatosSesion) -> bool:
        return locks.liberar(self.cliente, **datos.model_dump())
