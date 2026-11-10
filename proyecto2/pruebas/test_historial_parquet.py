"""Pruebas del escritor Parquet: esquema, particiones y publicación atómica.

Requieren PyArrow, pytest y las dependencias del servicio Historial;
no requieren levantar Valkey para las pruebas del escritor.
"""

import importlib.util
import json
import sys
from datetime import timezone
from pathlib import Path
from unittest.mock import Mock
from uuid import uuid4

import pyarrow.parquet as pq
import pytest


APP = Path(__file__).resolve().parents[1] / "servicios" / "historial" / "app"
spec = importlib.util.spec_from_file_location(
    "historial_app", APP / "__init__.py", submodule_search_locations=[str(APP)],
)
if "historial_app" not in sys.modules:
    package = importlib.util.module_from_spec(spec)
    sys.modules["historial_app"] = package
    spec.loader.exec_module(package)

from historial_app import consumer, eventos, parquet_writer  # noqa: E402


def crear_evento(**cambios):
    datos = {
        "event_id": str(uuid4()),
        "tipo_evento": "revision_aprobada",
        "estado": "aprobada",
        "ocurrido_en": "2026-11-06T12:00:00-06:00",
        "tarea_id": str(uuid4()),
        "proyecto_id": str(uuid4()),
        "cliente_id": str(uuid4()),
        "equipo_id": str(uuid4()),
        "modalidad": "lidar",
        "tipo_tarea": "cuboide_3d",
        "version": 4,
        "tomada_en": "2026-11-06T11:00:00-06:00",
        "aprobada_en": "2026-11-06T12:00:00-06:00",
        "duracion_ms": 3600000,
        "sla_horas": 48,
        "datos_extra": {"numero_cubos": 3},
    }
    datos.update(cambios)
    return eventos.EventoNegocio.model_validate(datos)


def leer_archivo(ruta):
    # ParquetFile no infiere la columna 'mes' a partir del nombre de carpeta.
    return pq.ParquetFile(ruta).read()


def test_esquema_fijo_y_datos_recuperables(tmp_path):
    evento = crear_evento()
    rutas = parquet_writer.EscritorParquet(tmp_path / "eventos").publicar([evento])
    assert len(rutas) == 1
    assert rutas[0].parent.name == "mes=2026-11"
    tabla = leer_archivo(rutas[0])
    assert tabla.num_rows == 1
    assert tabla.schema.equals(parquet_writer.ESQUEMA)
    fila = tabla.to_pylist()[0]
    assert fila["event_id"] == str(evento.event_id)
    assert fila["estado"] == "aprobada"
    assert fila["ocurrido_en"].tzinfo is not None
    assert fila["ocurrido_en"].astimezone(timezone.utc).hour == 18
    assert fila["duracion_ms"] == 3600000
    assert fila["sla_horas"] == 48.0
    assert json.loads(fila["datos_extra_json"]) == {"numero_cubos": 3}


def test_un_lote_abarca_dos_particiones_mensuales(tmp_path):
    octubre = crear_evento(ocurrido_en="2026-10-31T23:59:00Z")
    noviembre = crear_evento(ocurrido_en="2026-11-01T00:01:00Z")
    rutas = parquet_writer.EscritorParquet(tmp_path / "eventos").publicar(
        [octubre, noviembre]
    )
    assert len(rutas) == 2
    assert {p.parent.name for p in rutas} == {"mes=2026-10", "mes=2026-11"}
    assert sum(leer_archivo(p).num_rows for p in rutas) == 2


def test_reintento_identico_no_crea_archivo_extra(tmp_path):
    escritor = parquet_writer.EscritorParquet(tmp_path / "eventos")
    eventos_lote = [crear_evento(), crear_evento()]
    primera = escritor.publicar(eventos_lote)
    segunda = escritor.publicar(list(reversed(eventos_lote)))
    assert primera == segunda
    assert len(list((tmp_path / "eventos").rglob("*.parquet"))) == 1
    assert leer_archivo(primera[0]).num_rows == 2


def test_escritura_temporal_no_expone_parquet_incompleto(tmp_path, monkeypatch):
    base = tmp_path / "eventos"
    original = parquet_writer.pq.write_table
    vistas = []

    def observar(tabla, destino, **kwargs):
        vistas.append(Path(destino))
        assert Path(destino).parent.name == ".staging"
        assert list(base.rglob("*.parquet")) == []
        return original(tabla, destino, **kwargs)

    monkeypatch.setattr(parquet_writer.pq, "write_table", observar)
    ruta = parquet_writer.EscritorParquet(base).publicar([crear_evento()])[0]
    assert len(vistas) == 1
    assert ruta.suffix == ".parquet" and ruta.is_file()
    assert not list((base / ".staging").glob("*.tmp"))


def test_fallo_de_escritura_no_publica_archivo(tmp_path, monkeypatch):
    base = tmp_path / "eventos"

    def escritura_interrumpida(_tabla, ruta, **_kwargs):
        Path(ruta).write_bytes(b"contenido incompleto")
        raise OSError("Fallo simulado antes de publicar")

    monkeypatch.setattr(parquet_writer.pq, "write_table", escritura_interrumpida)
    with pytest.raises(OSError, match="Fallo simulado"):
        parquet_writer.EscritorParquet(base).publicar([crear_evento()])
    assert not list(base.rglob("*.parquet"))
    assert not list((base / ".staging").glob("*.tmp"))


def test_rechazo_conserva_estado_historico(tmp_path):
    rechazado = crear_evento(
        tipo_evento="revision_rechazada", estado="rechazada",
        motivo_rechazo="etiqueta_incorrecta", aprobada_en=None,
    )
    ruta = parquet_writer.EscritorParquet(tmp_path / "eventos").publicar([rechazado])[0]
    fila = leer_archivo(ruta).to_pylist()[0]
    assert fila["estado"] == "rechazada"
    assert fila["motivo_rechazo"] == "etiqueta_incorrecta"


def test_lote_vacio_se_rechaza(tmp_path):
    with pytest.raises(ValueError, match="vacio"):
        parquet_writer.EscritorParquet(tmp_path).publicar([])


def test_consumer_confirma_unicamente_tras_publicar_parquet(tmp_path):
    evento = crear_evento()
    cliente = Mock()
    cliente.xreadgroup.return_value = [
        (eventos.STREAM_KEY, [
            ("1793970000000-0", {"payload_json": eventos.serializar_evento(evento)})
        ])
    ]
    escritor = parquet_writer.EscritorParquet(tmp_path / "eventos")
    total = consumer.procesar_lote(cliente, escritor.publicar, consumidor="test")
    assert total == 1
    assert len(list((tmp_path / "eventos").rglob("*.parquet"))) == 1
    cliente.xack.assert_called_once_with(
        eventos.STREAM_KEY, consumer.GRUPO, "1793970000000-0"
    )


def test_consumer_no_confirma_si_publicacion_falla(tmp_path, monkeypatch):
    evento = crear_evento()
    cliente = Mock()
    cliente.xreadgroup.return_value = [
        (eventos.STREAM_KEY, [
            ("1793970000001-0", {"payload_json": eventos.serializar_evento(evento)})
        ])
    ]

    def fallar(*_args, **_kwargs):
        raise OSError("fallo antes de publicar")

    monkeypatch.setattr(parquet_writer.pq, "write_table", fallar)
    escritor = parquet_writer.EscritorParquet(tmp_path / "eventos")
    with pytest.raises(OSError):
        consumer.procesar_lote(cliente, escritor.publicar, consumidor="test")
    cliente.xack.assert_not_called()
