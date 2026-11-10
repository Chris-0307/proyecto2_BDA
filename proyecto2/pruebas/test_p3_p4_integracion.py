"""Pruebas P3/P4 con Valkey REAL y un escritor Parquet real.

Deben usarse en una BD de pruebas independiente. Si VALKEY_TEST_URL no se
establece, se marcan SKIPPED, no PASSED. Se crean IDs UUID para no alterar
datos de otros casos. No usar una DB de producción.

Estas pruebas NO sustituyen la verificación del gateway/MongoDB/Spark.
"""

import importlib.util
import json
import os
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from uuid import uuid4

import pyarrow.parquet as pq
import pytest
from fastapi.testclient import TestClient
from redis import Redis, RedisError

ROOT = Path(__file__).resolve().parents[1]


def cargar_paquete(nombre: str, ruta: Path):
    """Carga módulos de los dos servicios sin colisión del paquete 'app'."""
    if nombre not in sys.modules:
        spec = importlib.util.spec_from_file_location(
            nombre, ruta / "__init__.py", submodule_search_locations=[str(ruta)]
        )
        paquete = importlib.util.module_from_spec(spec)
        sys.modules[nombre] = paquete
        spec.loader.exec_module(paquete)
    return sys.modules[nombre]


cargar_paquete("p3_sesion_app", ROOT / "servicios" / "sesion" / "app")
cargar_paquete("p4_historial_app", ROOT / "servicios" / "historial" / "app")
from p3_sesion_app import main as sesion_main  # noqa: E402
from p4_historial_app import consumer, eventos, main as historial_main, parquet_writer  # noqa: E402


@pytest.fixture
def valkey_real():
    url = os.getenv("VALKEY_TEST_URL")
    if not url:
        pytest.skip("VALKEY_TEST_URL no configurada; prueba real pendiente")
    cliente = Redis.from_url(
        url, decode_responses=True, socket_connect_timeout=2, socket_timeout=3,
    )
    try:
        cliente.ping()
    except (RedisError, OSError) as exc:
        pytest.fail(f"VALKEY_TEST_URL configurada pero Valkey no responde: {exc}")
    try:
        yield cliente, url
    finally:
        cliente.close()


def sesion_demo():
    return dict(
        anotador_id=str(uuid4()), tarea_id=str(uuid4()),
        proyecto_id=str(uuid4()), token=str(uuid4()), etapa="anotacion",
    )


def limpiar_sesion(cliente, datos):
    cliente.delete(
        f"lock:tarea:{datos['tarea_id']}",
        f"sesion:{datos['anotador_id']}",
        f"borrador:{datos['tarea_id']}",
    )
    cliente.zrem(f"editando:{datos['proyecto_id']}", datos["anotador_id"])


def evento_demo():
    return eventos.EventoNegocio.model_validate({
        "event_id": str(uuid4()),
        "tipo_evento": "revision_aprobada",
        "estado": "aprobada",
        "ocurrido_en": datetime.now(timezone.utc).isoformat(),
        "tarea_id": str(uuid4()),
        "proyecto_id": str(uuid4()),
        "cliente_id": str(uuid4()),
        "modalidad": "lidar", "tipo_tarea": "cuboide_3d",
        "version": 3, "sla_horas": 48,
    })


def limpiar_evento(cliente, grupo, event_id, stream_id):
    try:
        cliente.xgroup_destroy(eventos.STREAM_KEY, grupo)
    finally:
        if stream_id:
            cliente.xdel(eventos.STREAM_KEY, stream_id)
        cliente.hdel(eventos.IDS_KEY, str(event_id))


def test_p3_recupera_ultimo_borrador_reciente_y_token(valkey_real, monkeypatch):
    """Verifica la recuperación inmediata (antigüedad <=5 s) y rechaza tokens ajenos."""
    cliente, url = valkey_real
    monkeypatch.setattr(sesion_main, "VALKEY_URL", url)
    datos = sesion_demo()
    contenido = {"modalidad": "lidar", "etiquetas": ["lidar#peaton"]}
    try:
        with TestClient(sesion_main.app) as api:
            assert api.post("/sesiones/iniciar", json=datos).status_code == 201
            r = api.put(f"/tareas/{datos['tarea_id']}/borrador",
                        json={**datos, "contenido": contenido})
            assert r.status_code == 200, r.text
            obtenido = api.post("/sesiones/recuperar", json=datos)
            assert obtenido.status_code == 200, obtenido.text
            body = obtenido.json()
            assert body["borrador"] == contenido
            assert body["estado"] == "activa"
            assert body["version"] == 1
            diferencia_ms = int(time.time() * 1000) - body["actualizado_en_ms"]
            assert 0 <= diferencia_ms <= 5000, diferencia_ms
            malo = api.post("/sesiones/recuperar", json={**datos, "token": str(uuid4())})
            assert malo.status_code == 409
    finally:
        limpiar_sesion(cliente, datos)


def test_p3_vencimiento_no_reanuda_edicion_sin_reasignacion(valkey_real, monkeypatch):
    """Simula vencimiento: el borrador existe, pero no puede editarse sin lock."""
    cliente, url = valkey_real
    monkeypatch.setattr(sesion_main, "VALKEY_URL", url)
    datos = sesion_demo()
    contenido = {"avance": 10}
    try:
        with TestClient(sesion_main.app) as api:
            assert api.post("/sesiones/iniciar", json=datos).status_code == 201
            assert api.put(f"/tareas/{datos['tarea_id']}/borrador",
                           json={**datos, "contenido": contenido}).status_code == 200
            # No se espera 120 segundos: se simula la expiración del lock.
            cliente.delete(f"lock:tarea:{datos['tarea_id']}")
            recuperado = api.post("/sesiones/recuperar", json=datos)
            assert recuperado.status_code == 200, recuperado.text
            assert recuperado.json()["estado"] == "requiere_reasignacion"
            assert recuperado.json()["borrador"] == contenido
            assert recuperado.json()["ttl_ms"] == 0
            guardar = api.put(f"/tareas/{datos['tarea_id']}/borrador",
                              json={**datos, "contenido": {"avance": 11}})
            assert guardar.status_code == 409
            almacenado = json.loads(cliente.hget(f"borrador:{datos['tarea_id']}", "contenido_json"))
            assert almacenado == contenido
    finally:
        limpiar_sesion(cliente, datos)


def test_p4_http_interno_a_parquet_y_xack(valkey_real, tmp_path, monkeypatch):
    """Recepción 202 (Historial) -> Stream -> Parquet -> XACK, sin Spark."""
    cliente, url = valkey_real
    monkeypatch.setattr(historial_main, "VALKEY_URL", url)
    grupo = f"p4-{uuid4().hex}"
    evento = evento_demo()
    sid = None
    # ID='$' limita el grupo de prueba a eventos publicados después de crearlo.
    cliente.xgroup_create(eventos.STREAM_KEY, grupo, id="$", mkstream=True)
    try:
        with TestClient(historial_main.app) as api:
            respuesta = api.post("/eventos", json=evento.model_dump(mode="json"))
        assert respuesta.status_code == 202, respuesta.text
        sid = respuesta.json()["stream_id"]
        inicio = time.perf_counter()
        n = consumer.procesar_lote(
            cliente,
            parquet_writer.EscritorParquet(tmp_path / "eventos").publicar,
            consumidor=f"worker-{uuid4().hex}", grupo=grupo, cantidad=10,
        )
        demora_ms = (time.perf_counter() - inicio) * 1000
        assert n == 1
        assert demora_ms >= 0
        archivos = list((tmp_path / "eventos").rglob("*.parquet"))
        assert len(archivos) == 1
        assert len(list((tmp_path / "eventos" / ".staging").glob("*.tmp"))) == 0
        filas = pq.ParquetFile(archivos[0]).read().to_pylist()
        assert [fila["event_id"] for fila in filas] == [str(evento.event_id)]
        assert cliente.xpending(eventos.STREAM_KEY, grupo)["pending"] == 0
    finally:
        limpiar_evento(cliente, grupo, evento.event_id, sid)


def test_p4_recupera_mensaje_sin_xack_y_no_duplica_archivo(valkey_real, tmp_path):
    """Simula caída después de publicar, luego XAUTOCLAIM y confirmación."""
    cliente, _ = valkey_real
    grupo = f"p4-{uuid4().hex}"
    evento = evento_demo()
    sid = None
    cliente.xgroup_create(eventos.STREAM_KEY, grupo, id="$", mkstream=True)
    escritor = parquet_writer.EscritorParquet(tmp_path / "eventos")
    try:
        recibido = eventos.publicar_evento(cliente, evento)
        sid = recibido.stream_id

        def publicar_y_simular_caida(lote):
            escritor.publicar(lote)
            raise OSError("Falla simulada antes de XACK")

        with pytest.raises(OSError, match="Falla simulada"):
            consumer.procesar_lote(
                cliente, publicar_y_simular_caida,
                consumidor="worker-anterior", grupo=grupo,
            )
        assert cliente.xpending(eventos.STREAM_KEY, grupo)["pending"] == 1
        antes = list((tmp_path / "eventos").rglob("*.parquet"))
        assert len(antes) == 1
        time.sleep(0.10)
        procesados = consumer.procesar_lote(
            cliente, escritor.publicar, consumidor="worker-nuevo", grupo=grupo,
            recuperar_pendientes=True, min_idle_ms=20,
        )
        assert procesados == 1
        assert list((tmp_path / "eventos").rglob("*.parquet")) == antes
        assert pq.ParquetFile(antes[0]).metadata.num_rows == 1
        assert cliente.xpending(eventos.STREAM_KEY, grupo)["pending"] == 0
    finally:
        limpiar_evento(cliente, grupo, evento.event_id, sid)
