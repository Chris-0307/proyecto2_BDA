"""Pruebas de integracion del servicio Sesion con una instancia Valkey real.

Por seguridad se utiliza DB 15 o la URL configurada en VALKEY_TEST_URL.
No usar el mismo indice de base de datos que los datos importantes.
"""

import os
import sys
from pathlib import Path
from uuid import uuid4

import pytest
from fastapi.testclient import TestClient
from redis import Redis, RedisError

# Este proyecto todavia no esta instalado como paquete Python.
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


def datos(*, anotador=None, tarea=None, proyecto=None, token=None):
    return {
        "anotador_id": anotador or str(uuid4()),
        "tarea_id": tarea or str(uuid4()),
        "proyecto_id": proyecto or str(uuid4()),
        "token": token or str(uuid4()),
        "etapa": "anotacion",
    }


def limpiar(cliente, *solicitudes):
    for solicitud in solicitudes:
        cliente.delete(f"lock:tarea:{solicitud['tarea_id']}")
        cliente.delete(f"sesion:{solicitud['anotador_id']}")
        cliente.zrem(f"editando:{solicitud['proyecto_id']}", solicitud["anotador_id"])


def test_lock_exclusivo_con_dos_anotadores(entorno):
    api, cliente = entorno
    primero = datos()
    segundo = datos(tarea=primero["tarea_id"], proyecto=primero["proyecto_id"])
    try:
        respuesta = api.post("/sesiones/iniciar", json=primero)
        assert respuesta.status_code == 201, respuesta.text
        assert respuesta.json()["ttl_ms"] == 120000
        assert api.post("/sesiones/iniciar", json=segundo).status_code == 409
        assert cliente.get(f"lock:tarea:{primero['tarea_id']}") == primero["token"]
        assert cliente.hget(f"sesion:{primero['anotador_id']}", "tarea_id") == primero["tarea_id"]
        assert cliente.zscore(f"editando:{primero['proyecto_id']}", primero["anotador_id"]) is not None
    finally:
        limpiar(cliente, primero, segundo)


def test_latido_y_liberacion_requieren_token_correcto(entorno):
    api, cliente = entorno
    solicitud = datos()
    incorrecto = dict(solicitud, token=str(uuid4()))
    try:
        assert api.post("/sesiones/iniciar", json=solicitud).status_code == 201
        assert api.post("/sesiones/latido", json=incorrecto).status_code == 409
        assert api.post("/sesiones/finalizar", json=incorrecto).status_code == 409
        assert cliente.get(f"lock:tarea:{solicitud['tarea_id']}") == solicitud["token"]
        assert api.post("/sesiones/latido", json=solicitud).status_code == 200
        ttl = cliente.pttl(f"lock:tarea:{solicitud['tarea_id']}")
        assert 0 < ttl <= 120000
        assert api.post("/sesiones/finalizar", json=solicitud).status_code == 200
        assert cliente.exists(f"lock:tarea:{solicitud['tarea_id']}") == 0
        assert cliente.exists(f"sesion:{solicitud['anotador_id']}") == 0
    finally:
        limpiar(cliente, solicitud)


def test_anotador_no_puede_mantener_dos_tareas(entorno):
    api, cliente = entorno
    primera = datos()
    segunda = datos(anotador=primera["anotador_id"], proyecto=primera["proyecto_id"])
    try:
        assert api.post("/sesiones/iniciar", json=primera).status_code == 201
        assert api.post("/sesiones/iniciar", json=segunda).status_code == 409
        assert cliente.get(f"lock:tarea:{primera['tarea_id']}") == primera["token"]
    finally:
        limpiar(cliente, primera, segunda)


def test_una_sesion_caducada_no_bloquea_nueva_tarea(entorno):
    api, cliente = entorno
    vieja = datos()
    nueva = datos(anotador=vieja["anotador_id"], proyecto=vieja["proyecto_id"])
    try:
        assert api.post("/sesiones/iniciar", json=vieja).status_code == 201
        # Simula vencimiento: elimina solo el lock, no la sesion.
        cliente.delete(f"lock:tarea:{vieja['tarea_id']}")
        assert api.post("/sesiones/iniciar", json=nueva).status_code == 201
        assert cliente.hget(f"sesion:{nueva['anotador_id']}", "tarea_id") == nueva["tarea_id"]
        assert cliente.get(f"lock:tarea:{nueva['tarea_id']}") == nueva["token"]
    finally:
        limpiar(cliente, vieja, nueva)
