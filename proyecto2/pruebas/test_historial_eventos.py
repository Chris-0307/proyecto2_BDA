"""Pruebas de la recepcion e idempotencia de eventos del servicio Historial.

Las pruebas unitarias no requieren Valkey, pero no reemplazan las pruebas
reales de los scripts Lua y de persistencia AOF que se haran con Docker.
"""

import importlib.util
import json
import os
import sys
from pathlib import Path
from unittest.mock import Mock
from uuid import uuid4

import pytest
from fastapi.testclient import TestClient
from pydantic import ValidationError
from redis import Redis, RedisError

# Importamos con otro nombre de paquete para que pueda coexistir con
# proyecto2/servicios/sesion/app cuando se ejecute toda la suite de pytest.
CARPETA_APP = Path(__file__).resolve().parents[1] / "servicios" / "historial" / "app"
spec = importlib.util.spec_from_file_location(
    "historial_app",
    CARPETA_APP / "__init__.py",
    submodule_search_locations=[str(CARPETA_APP)],
)
paquete = importlib.util.module_from_spec(spec)
sys.modules["historial_app"] = paquete
spec.loader.exec_module(paquete)

from historial_app import consumer, eventos, main  # noqa: E402


class ValkeySimulado:
    """Simula solo el resultado del script de ingreso, no interpreta Lua."""

    def __init__(self):
        self.registros = {}
        self.stream = []
        self.disponible = True

    def __enter__(self):
        return self

    def __exit__(self, _tipo, _valor, _traceback):
        return False

    def ping(self):
        if not self.disponible:
            raise RedisError("Valkey fuera de servicio")
        return True

    def eval(self, _script, llaves, nombre_stream, mapa_ids, event_id, payload, huella):
        assert llaves == 2
        assert nombre_stream == eventos.STREAM_KEY
        assert mapa_ids == eventos.IDS_KEY
        if not self.disponible:
            raise RedisError("Valkey fuera de servicio")
        anterior = self.registros.get(event_id)
        if anterior:
            huella_anterior, id_anterior = anterior
            return [0 if huella == huella_anterior else 2, id_anterior]
        stream_id = f"1700000000000-{len(self.stream)}"
        self.registros[event_id] = (huella, stream_id)
        self.stream.append((stream_id, {"event_id": event_id, "payload_json": payload}))
        return [1, stream_id]


def ejemplo(**cambios):
    datos = {
        "event_id": str(uuid4()),
        "tipo_evento": "revision_rechazada",
        "estado": "rechazada",
        "ocurrido_en": "2026-11-06T19:40:00Z",
        "tarea_id": str(uuid4()),
        "proyecto_id": str(uuid4()),
        "cliente_id": str(uuid4()),
        "modalidad": "lidar",
        "tipo_tarea": "cuboide_3d",
        "version": 5,
        "motivo_rechazo": "etiqueta_incorrecta",
    }
    datos.update(cambios)
    return datos


@pytest.fixture
def api_y_valkey(monkeypatch):
    valkey = ValkeySimulado()
    monkeypatch.setattr(main, "obtener_cliente", lambda: valkey)
    with TestClient(main.app) as api:
        yield api, valkey


def test_evento_nuevo_se_encola_una_vez(api_y_valkey):
    api, valkey = api_y_valkey
    r = api.post("/eventos", json=ejemplo())
    assert r.status_code == 202, r.text
    assert r.json()["resultado"] == "encolado"
    assert len(valkey.stream) == 1
    evento = json.loads(valkey.stream[0][1]["payload_json"])
    assert evento["estado"] == "rechazada"
    assert evento["motivo_rechazo"] == "etiqueta_incorrecta"


def test_reintento_idempotente_no_duplica(api_y_valkey):
    api, valkey = api_y_valkey
    datos = ejemplo()
    primero = api.post("/eventos", json=datos)
    segundo = api.post("/eventos", json=datos)
    assert primero.status_code == 202
    assert segundo.status_code == 200
    assert segundo.json()["resultado"] == "repetido"
    assert primero.json()["stream_id"] == segundo.json()["stream_id"]
    assert len(valkey.stream) == 1


def test_mismo_event_id_distinto_contenido_retorna_409(api_y_valkey):
    api, valkey = api_y_valkey
    datos = ejemplo()
    assert api.post("/eventos", json=datos).status_code == 202
    distinto = dict(datos, motivo_rechazo="dato_corrupto")
    assert api.post("/eventos", json=distinto).status_code == 409
    assert len(valkey.stream) == 1


@pytest.mark.parametrize("cambios", [
    {"tipo_evento": "revision_aprobada"},
    {"motivo_rechazo": None},
    {"ocurrido_en": "2026-11-06T19:40:00"},
    {"version": 0},
])
def test_eventos_invalidos_retorna_422_y_no_encola(api_y_valkey, cambios):
    api, valkey = api_y_valkey
    r = api.post("/eventos", json=ejemplo(**cambios))
    assert r.status_code == 422, r.text
    assert not valkey.stream


def test_borrador_no_es_un_tipo_de_evento_permitido():
    with pytest.raises(ValidationError):
        eventos.EventoNegocio.model_validate(
            ejemplo(tipo_evento="borrador_guardado")
        )


def test_valkey_no_disponible_retorna_503(api_y_valkey):
    api, valkey = api_y_valkey
    valkey.disponible = False
    assert api.get("/salud").status_code == 503
    assert api.post("/eventos", json=ejemplo()).status_code == 503


def test_salud_operativa(api_y_valkey):
    api, _ = api_y_valkey
    r = api.get("/salud")
    assert r.status_code == 200
    assert r.json()["estado"] == "operativo"


def test_consumidor_confirma_solo_tras_escritura_exitosa():
    cliente = Mock()
    cliente.xreadgroup.return_value = [
        (eventos.STREAM_KEY, [("1700000000000-0", {
            "event_id": "id-ejemplo",
            "payload_json": eventos.serializar_evento(
                eventos.EventoNegocio.model_validate(ejemplo())
            ),
        })])
    ]
    vistos = []
    total = consumer.procesar_lote(
        cliente,
        lambda lote: vistos.extend(lote),
        consumidor="pruebas",
    )
    assert total == 1
    assert len(vistos) == 1
    cliente.xack.assert_called_once_with(
        eventos.STREAM_KEY, consumer.GRUPO, "1700000000000-0"
    )


def test_consumidor_no_confirma_si_falla_el_writer():
    cliente = Mock()
    cliente.xreadgroup.return_value = [
        (eventos.STREAM_KEY, [("1700000000000-1", {
            "payload_json": eventos.serializar_evento(
                eventos.EventoNegocio.model_validate(ejemplo())
            ),
        })])
    ]

    def escribir_fallido(_lote):
        raise IOError("fallo al publicar el archivo Parquet")

    with pytest.raises(IOError):
        consumer.procesar_lote(cliente, escribir_fallido, consumidor="pruebas")
    cliente.xack.assert_not_called()


def test_integracion_valkey_real_opcional():
    """Valida el EVAL de ingreso en Valkey real, si se habilita explicitamente.

    No se borran otras llaves de Valkey; solo se elimina la entrada propia
    despues del test. La DB usada debe ser exclusivamente de pruebas.
    """
    url = os.getenv("VALKEY_TEST_URL")
    if not url:
        pytest.skip("Configure VALKEY_TEST_URL para ejecutar con Valkey real")
    cliente = Redis.from_url(url, decode_responses=True, socket_connect_timeout=1)
    try:
        cliente.ping()
    except (RedisError, OSError):
        cliente.close()
        pytest.skip("Valkey no disponible")
    evento = eventos.EventoNegocio.model_validate(ejemplo())
    try:
        primero = eventos.publicar_evento(cliente, evento)
        segundo = eventos.publicar_evento(cliente, evento)
        assert primero.resultado == "encolado"
        assert segundo.resultado == "repetido"
        assert primero.stream_id == segundo.stream_id
    finally:
        cliente.xdel(eventos.STREAM_KEY, primero.stream_id) if 'primero' in locals() else None
        cliente.hdel(eventos.IDS_KEY, str(evento.event_id))
        cliente.close()
