"""Escritura de micro-lotes Parquet con publicación atómica.

El directorio temporal .staging debe estar en el mismo sistema de archivos
que la partición final, para que os.replace() sea atómico.
Este servicio almacena eventos; Spark se encarga de deduplicar por event_id.
"""

import hashlib
import json
import os
import tempfile
from collections import defaultdict
from pathlib import Path
from typing import Iterable
from uuid import UUID

import pyarrow as pa
import pyarrow.parquet as pq

from .eventos import EventoNegocio, serializar_evento


# Esquema explícito: evita que los micro-lotes con muchos campos nulos
# generen tipos incompatibles entre archivos de distintas particiones.
ESQUEMA = pa.schema([
    pa.field("event_id", pa.string(), nullable=False),
    pa.field("tipo_evento", pa.string(), nullable=False),
    pa.field("estado", pa.string(), nullable=False),
    pa.field("ocurrido_en", pa.timestamp("ms", tz="UTC"), nullable=False),
    pa.field("tarea_id", pa.string(), nullable=False),
    pa.field("proyecto_id", pa.string(), nullable=False),
    pa.field("cliente_id", pa.string(), nullable=False),
    pa.field("batch_id", pa.string()),
    pa.field("anotador_id", pa.string()),
    pa.field("revisor_id", pa.string()),
    pa.field("equipo_id", pa.string()),
    pa.field("modalidad", pa.string(), nullable=False),
    pa.field("tipo_tarea", pa.string(), nullable=False),
    pa.field("version", pa.int64(), nullable=False),
    pa.field("tomada_en", pa.timestamp("ms", tz="UTC")),
    pa.field("aprobada_en", pa.timestamp("ms", tz="UTC")),
    pa.field("duracion_ms", pa.int64()),
    pa.field("sla_horas", pa.float64()),
    pa.field("motivo_rechazo", pa.string()),
    pa.field("datos_extra_json", pa.string(), nullable=False),
])


def _uuid_texto(valor: UUID | None) -> str | None:
    return str(valor) if valor is not None else None


def _fila(evento: EventoNegocio) -> dict:
    """Representa cada evento con tipos estables para Spark."""
    return {
        "event_id": str(evento.event_id),
        "tipo_evento": evento.tipo_evento,
        "estado": evento.estado,
        "ocurrido_en": evento.ocurrido_en,
        "tarea_id": str(evento.tarea_id),
        "proyecto_id": str(evento.proyecto_id),
        "cliente_id": str(evento.cliente_id),
        "batch_id": _uuid_texto(evento.batch_id),
        "anotador_id": _uuid_texto(evento.anotador_id),
        "revisor_id": _uuid_texto(evento.revisor_id),
        "equipo_id": _uuid_texto(evento.equipo_id),
        "modalidad": evento.modalidad,
        "tipo_tarea": evento.tipo_tarea,
        "version": evento.version,
        "tomada_en": evento.tomada_en,
        "aprobada_en": evento.aprobada_en,
        "duracion_ms": evento.duracion_ms,
        "sla_horas": evento.sla_horas,
        "motivo_rechazo": evento.motivo_rechazo,
        "datos_extra_json": json.dumps(
            evento.datos_extra, sort_keys=True, ensure_ascii=False,
            separators=(",", ":"), allow_nan=False,
        ),
    }


def _deduplicar_lote(eventos: list[EventoNegocio]) -> list[EventoNegocio]:
    """Elimina repeticiones exactas de event_id DENTRO de un micro-lote.

    Un mismo event_id con datos diferentes es un error de consistencia.
    Este control no reemplaza la deduplicacion global de Spark cuando
    archivos distintos contienen eventos superpuestos.
    """
    huellas: dict[str, str] = {}
    unicos: list[EventoNegocio] = []
    for evento in eventos:
        identificador = str(evento.event_id)
        contenido = serializar_evento(evento)
        anterior = huellas.get(identificador)
        if anterior is None:
            huellas[identificador] = contenido
            unicos.append(evento)
        elif anterior != contenido:
            raise ValueError(f"event_id repetido con contenido diferente: {identificador}")
    return unicos


def _id_microlote(eventos: Iterable[EventoNegocio]) -> str:
    """Nombre reproducible a partir de los IDs que componen el micro-lote."""
    ids = sorted(str(evento.event_id) for evento in eventos)
    huella = hashlib.sha256("\n".join(ids).encode("ascii")).hexdigest()[:24]
    return f"lote-{huella}"


def _sincronizar_directorio(ruta: Path) -> None:
    """Sincroniza la entrada de directorio en sistemas POSIX (WSL/Linux)."""
    if os.name != "posix":
        return
    descriptor = os.open(ruta, os.O_RDONLY | getattr(os, "O_DIRECTORY", 0))
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


class EscritorParquet:
    """Publica eventos en eventos/mes=AAAA-MM/*.parquet.

    No reconoce mensajes de Valkey: solo el consumidor puede hacer XACK
    después de que este método haya terminado sin excepciones.
    """

    def __init__(self, directorio_eventos: str | Path):
        self.directorio = Path(directorio_eventos)

    def publicar(self, eventos: list[EventoNegocio]) -> list[Path]:
        """Materializa los micro-lotes, separando las distintas mensualidades.

        La publicación es atómica POR ARCHIVO, no una transacción entre
        particiones. Si una publicación falla, se relanzará la excepción
        y el consumidor no hará XACK del lote.
        """
        if not eventos:
            raise ValueError("No se puede publicar un micro-lote vacio")

        # No agrega filas repetidas dentro de un mismo lote.
        eventos = _deduplicar_lote(eventos)
        grupos: dict[str, list[EventoNegocio]] = defaultdict(list)
        for evento in eventos:
            # La validación Pydantic exige una zona horaria y normaliza a UTC.
            mes = evento.ocurrido_en.strftime("%Y-%m")
            grupos[mes].append(evento)

        publicados: list[Path] = []
        for mes, grupo in sorted(grupos.items()):
            publicados.append(self._publicar_mes(mes, grupo))
        return publicados

    def _publicar_mes(self, mes: str, eventos: list[EventoNegocio]) -> Path:
        carpeta = self.directorio / f"mes={mes}"
        carpeta.mkdir(parents=True, exist_ok=True)
        destino = carpeta / f"{_id_microlote(eventos)}.parquet"

        # Reintento idéntico después de una caída antes de XACK.
        if destino.is_file():
            return destino

        temporal_dir = self.directorio / ".staging"
        temporal_dir.mkdir(parents=True, exist_ok=True)
        descriptor, nombre_temporal = tempfile.mkstemp(
            prefix="microlote-", suffix=".tmp", dir=temporal_dir,
        )
        os.close(descriptor)
        temporal = Path(nombre_temporal)

        try:
            tabla = pa.Table.from_pylist([_fila(e) for e in eventos], schema=ESQUEMA)
            pq.write_table(tabla, temporal, compression="snappy")

            # Se fuerza la escritura del archivo antes de hacerlo visible.
            with temporal.open("rb") as archivo:
                os.fsync(archivo.fileno())

            # La ruta temporal y la final pertenecen al mismo volumen.
            os.replace(temporal, destino)
            _sincronizar_directorio(carpeta)
            return destino
        finally:
            # En errores previos a os.replace, jamás queda un .parquet parcial.
            temporal.unlink(missing_ok=True)
