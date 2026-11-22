"""Modelos de entrada del registro."""

from typing import Annotated, Any, Literal
from uuid import UUID

from pydantic import AfterValidator, BaseModel, ConfigDict, Field, model_validator

Modalidad = Literal["texto", "imagen", "video", "lidar"]

# Geometrías admitidas por modalidad (enunciado, sección 6.1).
GEOMETRIAS = {
    "texto": {"span"},
    "imagen": {"bbox", "poligono"},
    "video": {"bbox_video"},
    "lidar": {"cuboide_3d"},
}


def _como_uuid(valor: str) -> str:
    return str(UUID(valor))  # ValueError -> HTTP 422


IdUUID = Annotated[str, AfterValidator(_como_uuid)]


class Etiqueta(BaseModel):
    etiqueta: str = Field(min_length=1, max_length=128)
    instruccion: str = Field(min_length=1, max_length=500)


class NuevoProyecto(BaseModel):
    cliente_id: IdUUID
    nombre: str = Field(min_length=1, max_length=200)
    taxonomia: list[Etiqueta] = Field(min_length=1)
    sla_horas: float = Field(default=48, gt=0)


class ArchivoRef(BaseModel):
    uri: str = Field(min_length=1, max_length=2048)
    sha256: str = Field(pattern=r"^[0-9a-fA-F]{64}$")
    bytes: int = Field(ge=0)
    mime: str = Field(min_length=1, max_length=128)
    meta: dict[str, Any] = Field(default_factory=dict)


class NuevaTarea(BaseModel):
    modalidad: Modalidad
    tipo_tarea: str = Field(min_length=1, max_length=128)
    prioridad: int = 0
    archivo: ArchivoRef

    @model_validator(mode="after")
    def geometria_compatible(self):
        if self.tipo_tarea not in GEOMETRIAS[self.modalidad]:
            raise ValueError(
                f"tipo_tarea {self.tipo_tarea} no es válido para {self.modalidad}; "
                f"admitidos: {sorted(GEOMETRIAS[self.modalidad])}"
            )
        return self


class NuevoLote(BaseModel):
    nombre: str = Field(min_length=1, max_length=200)
    tareas: list[NuevaTarea] = Field(min_length=1, max_length=5000)


# Campos obligatorios de cada acción de transición.
REQUERIDOS = {
    "asignar": ("anotador_id", "token"),
    "enviar": ("anotador_id", "token", "contenido"),
    "liberar": ("anotador_id", "token"),
    "iniciar_revision": ("revisor_id",),
    "aprobar": ("revisor_id",),
    "rechazar": ("revisor_id", "motivo"),
    "confirmar_evento": ("event_id",),
}


class Transicion(BaseModel):
    model_config = ConfigDict(extra="forbid")

    accion: Literal[
        "asignar", "enviar", "liberar", "iniciar_revision",
        "aprobar", "rechazar", "confirmar_evento",
    ]
    anotador_id: IdUUID | None = None
    revisor_id: IdUUID | None = None
    token: str | None = Field(default=None, min_length=1, max_length=256)
    contenido: dict[str, Any] | None = None
    motivo: str | None = Field(default=None, min_length=1, max_length=128)
    event_id: IdUUID | None = None

    @model_validator(mode="after")
    def exigir_campos(self):
        faltan = [c for c in REQUERIDOS[self.accion] if getattr(self, c) is None]
        if faltan:
            raise ValueError(f"La acción {self.accion} requiere: {', '.join(faltan)}")
        return self