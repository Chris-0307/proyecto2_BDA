"""Pruebas de integracion del guardado de borradores con Valkey real.

Por defecto se usa DB 15 (solo para pruebas); no ejecutar contra datos
productivos. Si Valkey no esta activo, pytest marca los casos SKIPPED.
"""

import json
import os
import sys
from pathlib import Path
from uuid import uuid4

import pytest
from fastapi.testclient import TestClient
from redis import Redis, RedisError

RUTA_SESION = Path(__file__).resolve().parents[1] / "servicios" / "sesion"
sys.path.insert(0, str(RUTA_SESION))
from app import main  # noqa: E402


@pytest.fixture
def entorno(monkeypatch):
    url = os.getenv("VALKEY_TEST_URL", "redis://localhost:6379/15")
    cliente = Redis.from_url(url, decode_responses=True, socket_connect_timeout=1)
    try:
        cliente.ping()
    except (RedisError, OSError):
        cliente.close()
        pytest.skip("Valkey no esta disponible para las pruebas de integracion")

    monkeypatch.setattr(main, "VALKEY_URL", url)
    with TestClient(main.app) as api:
        yield api, cliente
    cliente.close()


def solicitud():
    return {
        "anotador_id": str(uuid4()),
        "tarea_id": str(uuid4()),
        "proyecto_id": str(uuid4()),
        "token": str(uuid4()),
        "etapa": "anotacion",
    }


def enviar_borrador(api, datos, contenido=None):
    return api.put(
        f"/tareas/{datos['tarea_id']}/borrador",
        json={**datos, "contenido": contenido if contenido is not None else {"etiquetas": ["lidar#peaton"]}},
    )


def limpiar(cliente, datos):
    cliente.delete(
        f"lock:tarea:{datos['tarea_id']}",
        f"sesion:{datos['anotador_id']}",
        f"borrador:{datos['tarea_id']}",
    )
    cliente.zrem(f"editando:{datos['proyecto_id']}", datos["anotador_id"])


def test_guarda_borrador_y_renueva_lock(entorno):
    api, cliente = entorno
    datos = solicitud()
    try:
        assert api.post("/sesiones/iniciar", json=datos).status_code == 201
        # Reduce artificialmente la vida del lock para comprobar su renovacion.
        cliente.pexpire(f"lock:tarea:{datos['tarea_id']}", 20_000)
        contenido = {"modalidad": "lidar", "cuboides": [{"clase": "lidar#vehiculo", "x": 1.5}]}
        respuesta = enviar_borrador(api, datos, contenido)
        assert respuesta.status_code == 200, respuesta.text
        assert respuesta.json()["version"] == 1
        assert respuesta.json()["ttl_ms"] == 120000
        almacenado = cliente.hgetall(f"borrador:{datos['tarea_id']}")
        assert json.loads(almacenado["contenido_json"]) == contenido
        assert almacenado["anotador_id"] == datos["anotador_id"]
        assert int(almacenado["actualizado_en_ms"]) > 0
        assert 100_000 < cliente.pttl(f"lock:tarea:{datos['tarea_id']}") <= 120_000
        assert cliente.zscore(f"editando:{datos['proyecto_id']}", datos["anotador_id"]) is not None
    finally:
        limpiar(cliente, datos)


def test_token_incorrecto_no_modifica_el_borrador(entorno):
    api, cliente = entorno
    datos = solicitud()
    try:
        assert api.post("/sesiones/iniciar", json=datos).status_code == 201
        assert enviar_borrador(api, {**datos, "token": str(uuid4())}).status_code == 409
        assert cliente.exists(f"borrador:{datos['tarea_id']}") == 0
    finally:
        limpiar(cliente, datos)


def test_sin_lock_no_guarda_borrador(entorno):
    api, cliente = entorno
    datos = solicitud()
    try:
        assert enviar_borrador(api, datos).status_code == 409
        assert cliente.exists(f"borrador:{datos['tarea_id']}") == 0
    finally:
        limpiar(cliente, datos)


def test_actualizaciones_reemplazan_el_contenido_y_aumentan_version(entorno):
    api, cliente = entorno
    datos = solicitud()
    try:
        assert api.post("/sesiones/iniciar", json=datos).status_code == 201
        primera = enviar_borrador(api, datos, {"etiquetas": ["lidar#peaton"]})
        segunda = enviar_borrador(api, datos, {"etiquetas": ["lidar#ciclista"]})
        assert primera.status_code == 200
        assert segunda.status_code == 200
        assert primera.json()["version"] == 1
        assert segunda.json()["version"] == 2
        almacenado = cliente.hgetall(f"borrador:{datos['tarea_id']}")
        assert json.loads(almacenado["contenido_json"]) == {"etiquetas": ["lidar#ciclista"]}
        assert almacenado["version"] == "2"
    finally:
        limpiar(cliente, datos)


def test_borrador_persiste_tras_cerrar_sesion(entorno):
    api, cliente = entorno
    datos = solicitud()
    try:
        assert api.post("/sesiones/iniciar", json=datos).status_code == 201
        assert enviar_borrador(api, datos).status_code == 200
        assert api.post("/sesiones/finalizar", json=datos).status_code == 200
        assert cliente.exists(f"borrador:{datos['tarea_id']}") == 1
        assert enviar_borrador(api, datos).status_code == 409
    finally:
        limpiar(cliente, datos)


def test_no_guarda_si_el_lock_caduca(entorno):
    api, cliente = entorno
    datos = solicitud()
    try:
        assert api.post("/sesiones/iniciar", json=datos).status_code == 201
        assert enviar_borrador(api, datos, {"avance": 1}).status_code == 200
        # Simula caducidad sin esperar 120 segundos.
        cliente.delete(f"lock:tarea:{datos['tarea_id']}")
        assert enviar_borrador(api, datos, {"avance": 2}).status_code == 409
        almacenado = cliente.hgetall(f"borrador:{datos['tarea_id']}")
        assert json.loads(almacenado["contenido_json"]) == {"avance": 1}
    finally:
        limpiar(cliente, datos)


def test_ruta_y_cuerpo_deben_coincidir(entorno):
    api, cliente = entorno
    datos = solicitud()
    try:
        assert api.post("/sesiones/iniciar", json=datos).status_code == 201
        respuesta = api.put(
            f"/tareas/{uuid4()}/borrador",
            json={**datos, "contenido": {"avance": 1}},
        )
        assert respuesta.status_code == 422
        assert cliente.exists(f"borrador:{datos['tarea_id']}") == 0
    finally:
        limpiar(cliente, datos)
