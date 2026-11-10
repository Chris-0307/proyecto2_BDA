"""Pruebas del commit 8: recuperación, repetición e idempotencia.

Las pruebas con Mock no usan Valkey. Las pruebas con PyArrow crean archivos
reales en tmp_path y no necesitan que Docker esté levantado.
"""

import importlib.util
import sys
from pathlib import Path
from unittest.mock import Mock
from uuid import uuid4

import pyarrow.parquet as pq
import pytest

APP = Path(__file__).resolve().parents[1] / "servicios" / "historial" / "app"
if "historial_app" not in sys.modules:
    spec = importlib.util.spec_from_file_location(
        "historial_app", APP / "__init__.py", submodule_search_locations=[str(APP)]
    )
    paquete = importlib.util.module_from_spec(spec)
    sys.modules["historial_app"] = paquete
    spec.loader.exec_module(paquete)

from historial_app import consumer, eventos, parquet_writer  # noqa: E402


def evento(**cambios):
    base = {
        "event_id": str(uuid4()),
        "tipo_evento": "revision_aprobada",
        "estado": "aprobada",
        "ocurrido_en": "2026-11-09T21:00:00Z",
        "tarea_id": str(uuid4()),
        "proyecto_id": str(uuid4()),
        "cliente_id": str(uuid4()),
        "modalidad": "lidar",
        "tipo_tarea": "cuboide_3d",
        "version": 3,
    }
    base.update(cambios)
    return eventos.EventoNegocio.model_validate(base)


def entrada(e, stream_id="1794260000000-0"):
    return (stream_id, {"payload_json": eventos.serializar_evento(e)})


def archivos(base):
    return list(base.rglob("*.parquet"))


def test_recupera_pendiente_y_confirma_despues_de_parquet(tmp_path):
    e = evento()
    cliente = Mock()
    cliente.xautoclaim.return_value = ("0-0", [entrada(e)], [])
    cliente.xreadgroup.return_value = []
    escritor = parquet_writer.EscritorParquet(tmp_path / "eventos")

    total = consumer.procesar_lote(
        cliente, escritor.publicar, consumidor="worker-nuevo",
        recuperar_pendientes=True, min_idle_ms=30_000,
    )
    assert total == 1
    assert len(archivos(tmp_path)) == 1
    assert pq.ParquetFile(archivos(tmp_path)[0]).metadata.num_rows == 1
    cliente.xreadgroup.assert_not_called()
    cliente.xautoclaim.assert_called_once_with(
        name=eventos.STREAM_KEY, groupname=consumer.GRUPO,
        consumername="worker-nuevo", min_idle_time=30_000,
        start_id="0-0", count=100,
    )
    cliente.xack.assert_called_once_with(eventos.STREAM_KEY, consumer.GRUPO, "1794260000000-0")


def test_si_falla_el_escritor_pendiente_no_se_confirma():
    e = evento()
    cliente = Mock()
    cliente.xautoclaim.return_value = ("0-0", [entrada(e)], [])

    def fallar(_):
        raise OSError("Fallo de almacenamiento simulado")

    with pytest.raises(OSError, match="Fallo de almacenamiento"):
        consumer.procesar_lote(
            cliente, fallar, consumidor="worker-recuperado", recuperar_pendientes=True,
        )
    cliente.xack.assert_not_called()


def test_caida_despues_de_publicar_y_antes_de_xack_no_duplica_archivo(tmp_path):
    e = evento()
    cliente = Mock()
    cliente.xreadgroup.return_value = [(eventos.STREAM_KEY, [entrada(e)])]
    cliente.xack.side_effect = [OSError("Caida antes de confirmar"), 1]
    escritor = parquet_writer.EscritorParquet(tmp_path / "eventos")

    with pytest.raises(OSError):
        consumer.procesar_lote(cliente, escritor.publicar, consumidor="original")
    primero = archivos(tmp_path)
    assert len(primero) == 1

    cliente.xautoclaim.return_value = ("0-0", [entrada(e)], [])
    total = consumer.procesar_lote(
        cliente, escritor.publicar,
        consumidor="nuevo", recuperar_pendientes=True,
    )
    assert total == 1
    assert archivos(tmp_path) == primero
    assert pq.ParquetFile(primero[0]).metadata.num_rows == 1
    assert cliente.xack.call_count == 2


def test_sin_pendientes_elegibles_consumira_eventos_nuevos():
    e = evento()
    cliente = Mock()
    cliente.xautoclaim.return_value = ("0-0", [], [])
    cliente.xreadgroup.return_value = [(eventos.STREAM_KEY, [entrada(e)])]
    escritos = []
    total = consumer.procesar_lote(
        cliente, lambda grupo: escritos.extend(grupo),
        consumidor="worker", recuperar_pendientes=True,
    )
    assert total == 1
    assert escritos == [e]
    cliente.xreadgroup.assert_called_once()
    cliente.xack.assert_called_once()


def test_reclamacion_paginada_hasta_encontrar_pendiente():
    e = evento()
    cliente = Mock()
    cliente.xautoclaim.side_effect = [
        ("1794250000000-0", [], []),
        ("0-0", [entrada(e)], []),
    ]
    total = consumer.procesar_lote(
        cliente, lambda _: None,
        consumidor="worker", recuperar_pendientes=True,
    )
    assert total == 1
    assert cliente.xautoclaim.call_count == 2
    assert cliente.xautoclaim.call_args_list[1].kwargs["start_id"] == "1794250000000-0"
    cliente.xreadgroup.assert_not_called()


def test_xautoclaim_con_respuesta_de_dos_elementos():
    e = evento()
    cliente = Mock()
    cliente.xautoclaim.return_value = ["0-0", [entrada(e)]]
    assert consumer.procesar_lote(
        cliente, lambda _: None, consumidor="worker", recuperar_pendientes=True,
    ) == 1
    cliente.xack.assert_called_once()


def test_entrada_invalida_no_recibe_xack():
    cliente = Mock()
    cliente.xautoclaim.return_value = (
        "0-0", [("1794260000000-5", {"event_id": "sin-payload"})], [],
    )
    with pytest.raises(ValueError, match="sin payload_json"):
        consumer.procesar_lote(
            cliente, lambda _: None, consumidor="worker", recuperar_pendientes=True,
        )
    cliente.xack.assert_not_called()


@pytest.mark.parametrize("argumentos", [
    {"cantidad": 0},
    {"min_idle_ms": 0},
    {"consumidor": "   "},
])
def test_parametros_invalidos_rechazados(argumentos):
    with pytest.raises(ValueError):
        consumer.procesar_lote(Mock(), lambda _: None, consumidor="worker", **argumentos)


def test_deduplicacion_interna_del_microlote(tmp_path):
    a = evento()
    b = evento()
    rutas = parquet_writer.EscritorParquet(tmp_path / "eventos").publicar([a, b, a])
    assert len(rutas) == 1
    filas = pq.ParquetFile(rutas[0]).read().to_pylist()
    assert len(filas) == 2
    assert {f["event_id"] for f in filas} == {str(a.event_id), str(b.event_id)}


def test_mismo_event_id_con_otro_contenido_falla_sin_publicar(tmp_path):
    a = evento()
    distinto = evento(event_id=a.event_id, version=a.version + 1)
    with pytest.raises(ValueError, match="contenido diferente"):
        parquet_writer.EscritorParquet(tmp_path / "eventos").publicar([a, distinto])
    assert not archivos(tmp_path)


def test_lotes_solapados_se_deduplican_en_analitica(tmp_path):
    """Prueba la necesidad de dropDuplicates(event_id) en Spark.

    Aquí comprobamos el conjunto de IDs únicos, sin afirmar que el escritor
    impide duplicaciones globales entre archivos de tamaños distintos.
    """
    a, b, c = evento(), evento(), evento()
    escritor = parquet_writer.EscritorParquet(tmp_path / "eventos")
    escritor.publicar([a, b])
    escritor.publicar([a, c])
    resultados = [fila["event_id"] for ruta in archivos(tmp_path)
                  for fila in pq.ParquetFile(ruta).read().to_pylist()]
    assert len(resultados) == 4
    assert len(set(resultados)) == 3
    assert resultados.count(str(a.event_id)) == 2
