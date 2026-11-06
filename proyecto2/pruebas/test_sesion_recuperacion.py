"""Pruebas de integracion: recuperacion segura de sesiones con Valkey.

Se usa una base de pruebas (DB 15), nunca los datos del despliegue real.
Si no hay Valkey se omiten los casos y NO se consideran aprobados.
"""

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
        pytest.skip("Valkey no esta disponible para pruebas de integracion")

    monkeypatch.setattr(main, "VALKEY_URL", url)
    with TestClient(main.app) as api:
        yield api, cliente
    cliente.close()


def datos():
    return {
        "anotador_id": str(uuid4()),
        "tarea_id": str(uuid4()),
        "proyecto_id": str(uuid4()),
        "token": str(uuid4()),
        "etapa": "anotacion",
    }


def limpiar(cliente, solicitud):
    cliente.delete(
        f"lock:tarea:{solicitud['tarea_id']}",
        f"sesion:{solicitud['anotador_id']}",
        f"borrador:{solicitud['tarea_id']}",
    )
    cliente.zrem(f"editando:{solicitud['proyecto_id']}", solicitud["anotador_id"])


def guardar(api, d, contenido):
    return api.put(
        f"/tareas/{d['tarea_id']}/borrador",
        json={**d, "contenido": contenido},
    )


def recuperar(api, d):
    return api.post("/sesiones/recuperar", json=d)


def test_recupera_borrador_y_renueva_lock_activo(entorno):
    api, cliente = entorno
    d = datos()
    try:
        assert api.post("/sesiones/iniciar", json=d).status_code == 201
        contenido = {"modalidad": "lidar", "etiquetas": ["lidar#ciclista"]}
        assert guardar(api, d, contenido).status_code == 200
        cliente.pexpire(f"lock:tarea:{d['tarea_id']}", 15_000)

        respuesta = recuperar(api, d)
        assert respuesta.status_code == 200, respuesta.text
        cuerpo = respuesta.json()
        assert cuerpo["estado"] == "activa"
        assert cuerpo["borrador"] == contenido
        assert cuerpo["version"] == 1
        assert cuerpo["actualizado_en_ms"] > 0
        assert cuerpo["etapa"] == "anotacion"
        assert cuerpo["ttl_ms"] == 120000
        assert 100_000 < cliente.pttl(f"lock:tarea:{d['tarea_id']}") <= 120_000
        assert cliente.zscore(f"editando:{d['proyecto_id']}", d["anotador_id"]) is not None
    finally:
        limpiar(cliente, d)


def test_recupera_sesion_sin_borrador(entorno):
    api, cliente = entorno
    d = datos()
    try:
        assert api.post("/sesiones/iniciar", json=d).status_code == 201
        respuesta = recuperar(api, d)
        assert respuesta.status_code == 200, respuesta.text
        assert respuesta.json()["borrador"] is None
        assert respuesta.json()["version"] is None
        assert respuesta.json()["estado"] == "activa"
    finally:
        limpiar(cliente, d)


def test_token_incorrecto_no_revela_borrador_ni_renueva(entorno):
    api, cliente = entorno
    d = datos()
    try:
        assert api.post("/sesiones/iniciar", json=d).status_code == 201
        assert guardar(api, d, {"secreto": "contenido"}).status_code == 200
        cliente.pexpire(f"lock:tarea:{d['tarea_id']}", 25_000)
        respuesta = recuperar(api, {**d, "token": str(uuid4())})
        assert respuesta.status_code == 409
        assert "secreto" not in respuesta.text
        assert 0 < cliente.pttl(f"lock:tarea:{d['tarea_id']}") <= 25_000
    finally:
        limpiar(cliente, d)


def test_lock_vencido_devuelve_borrador_solo_lectura(entorno):
    api, cliente = entorno
    d = datos()
    try:
        assert api.post("/sesiones/iniciar", json=d).status_code == 201
        assert guardar(api, d, {"avance": 7}).status_code == 200
        cliente.delete(f"lock:tarea:{d['tarea_id']}")
        respuesta = recuperar(api, d)
        assert respuesta.status_code == 200, respuesta.text
        assert respuesta.json()["estado"] == "requiere_reasignacion"
        assert respuesta.json()["borrador"] == {"avance": 7}
        assert respuesta.json()["ttl_ms"] == 0
        assert cliente.exists(f"lock:tarea:{d['tarea_id']}") == 0
        assert guardar(api, d, {"avance": 8}).status_code == 409
        assert cliente.zscore(f"editando:{d['proyecto_id']}", d["anotador_id"]) is None
    finally:
        limpiar(cliente, d)


def test_no_recupera_si_otro_es_propietario_del_lock(entorno):
    api, cliente = entorno
    d = datos()
    try:
        assert api.post("/sesiones/iniciar", json=d).status_code == 201
        assert guardar(api, d, {"avance": 1}).status_code == 200
        cliente.set(f"lock:tarea:{d['tarea_id']}", "token-de-otro", px=120_000)
        respuesta = recuperar(api, d)
        assert respuesta.status_code == 409
        assert cliente.get(f"lock:tarea:{d['tarea_id']}") == "token-de-otro"
    finally:
        limpiar(cliente, d)


def test_finalizar_conserva_borrador_pero_impide_recuperar_sesion(entorno):
    api, cliente = entorno
    d = datos()
    try:
        assert api.post("/sesiones/iniciar", json=d).status_code == 201
        assert guardar(api, d, {"avance": 1}).status_code == 200
        assert api.post("/sesiones/finalizar", json=d).status_code == 200
        assert recuperar(api, d).status_code == 409
        assert cliente.exists(f"borrador:{d['tarea_id']}") == 1
    finally:
        limpiar(cliente, d)


def test_no_recupera_sesion_inexistente(entorno):
    api, cliente = entorno
    d = datos()
    try:
        assert recuperar(api, d).status_code == 409
    finally:
        limpiar(cliente, d)


def test_borrador_de_otro_anotador_no_se_expone(entorno):
    api, cliente = entorno
    original = datos()
    nuevo = {**original, "anotador_id": str(uuid4()), "token": str(uuid4())}
    try:
        assert api.post("/sesiones/iniciar", json=original).status_code == 201
        assert guardar(api, original, {"dato_privado": "del-anotador-original"}).status_code == 200
        assert api.post("/sesiones/finalizar", json=original).status_code == 200
        assert api.post("/sesiones/iniciar", json=nuevo).status_code == 201
        respuesta = recuperar(api, nuevo)
        assert respuesta.status_code == 200, respuesta.text
        assert respuesta.json()["borrador"] is None
        assert "dato_privado" not in respuesta.text
    finally:
        limpiar(cliente, original)
        limpiar(cliente, nuevo)
