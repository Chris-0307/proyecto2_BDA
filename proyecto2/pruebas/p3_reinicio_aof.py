"""Experimento reproducible P3: reinicio REAL de Valkey con AOF.

Llamar desde la raíz del repo, con una instancia Valkey de PRUEBAS:
  VALKEY_TEST_URL=redis://localhost:6379/15 python proyecto2/pruebas/p3_reinicio_aof.py \\
    --restart-command 'docker compose -f proyecto2/docker-compose.yml restart valkey'

El comando debe reiniciar SOLO Valkey y conservar el volumen AOF. La prueba
nunca elimina las demás llaves de Valkey ni ejecuta FLUSHDB.
"""

import argparse
import hashlib
import importlib.util
import json
import os
import platform
import shlex
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from uuid import uuid4

from fastapi.testclient import TestClient
from redis import Redis

PROYECTO = Path(__file__).resolve().parents[1]
ROOT_REPO = PROYECTO.parent
RUTA_APP = PROYECTO / "servicios" / "sesion" / "app"
spec = importlib.util.spec_from_file_location(
    "p3_aof_sesion_app", RUTA_APP / "__init__.py",
    submodule_search_locations=[str(RUTA_APP)],
)
mod = importlib.util.module_from_spec(spec)
sys.modules["p3_aof_sesion_app"] = mod
spec.loader.exec_module(mod)
from p3_aof_sesion_app import main as sesion_main  # noqa: E402


def ahora():
    return datetime.now(timezone.utc).isoformat()


def verificar_comando(texto):
    partes = shlex.split(texto)
    # No se ejecuta un comando arbitrario: únicamente 'docker compose ... restart'.
    if len(partes) < 4 or partes[:2] != ["docker", "compose"] or "restart" not in partes:
        raise ValueError("Utilice 'docker compose [-f archivo] restart valkey'")
    i = partes.index("restart")
    if partes[i + 1:] != ["valkey"]:
        raise ValueError("El comando debe reiniciar solo el servicio 'valkey'")
    return partes


def principal():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--restart-command", required=True,
                        help="Comando Docker Compose que reinicia el servicio valkey")
    parser.add_argument("--evidence-dir", default=str(PROYECTO / "evidence"))
    parser.add_argument("--wait-seconds", type=int, default=60)
    args = parser.parse_args()
    url = os.getenv("VALKEY_TEST_URL")
    if not url:
        parser.error("Establezca VALKEY_TEST_URL con una base exclusiva de pruebas")
    reinicio = verificar_comando(args.restart_command)
    evidencia = Path(args.evidence_dir)
    evidencia.mkdir(parents=True, exist_ok=True)
    nombre = datetime.now(timezone.utc).strftime("p3_aof_%Y%m%dT%H%M%SZ_") + uuid4().hex[:8] + ".json"
    registro = {
        "prueba": "P3-reinicio-AOF", "fecha_inicio_utc": ahora(),
        "objetivo": "Verificar supervivencia de borrador tras reinicio real Valkey",
        "comando_reinicio": reinicio,
        "maquina": {"sistema": platform.platform(), "cpu_logicos": os.cpu_count()},
        "resultado": "fallo", "observaciones": [],
        "limitacion": "No demuestra reintegracion con Registro/MongoDB; no calcula RTO/RPO",
    }
    datos = {
        "anotador_id": str(uuid4()), "tarea_id": str(uuid4()),
        "proyecto_id": str(uuid4()), "token": str(uuid4()), "etapa": "anotacion",
    }
    contenido = {"modalidad": "lidar", "etiquetas": ["lidar#peaton"], "avance": 42}
    cliente = Redis.from_url(url, decode_responses=True, socket_connect_timeout=2,
                             socket_timeout=2)
    try:
        cliente.ping()
        config = {**cliente.config_get("appendonly"), **cliente.config_get("appendfsync")}
        registro["config_aof"] = config
        if config.get("appendonly") != "yes" or config.get("appendfsync") != "everysec":
            raise RuntimeError("AOF appendonly=yes / appendfsync=everysec no configurado")
        sesion_main.VALKEY_URL = url
        with TestClient(sesion_main.app) as api:
            inicio = api.post("/sesiones/iniciar", json=datos)
            if inicio.status_code != 201:
                raise AssertionError(f"Iniciar sesión: {inicio.status_code} {inicio.text}")
            guardado = api.put(f"/tareas/{datos['tarea_id']}/borrador",
                               json={**datos, "contenido": contenido})
            if guardado.status_code != 200:
                raise AssertionError(f"Guardar: {guardado.status_code} {guardado.text}")
            registro["borrador_sha256_antes"] = hashlib.sha256(
                json.dumps(contenido, sort_keys=True).encode("utf-8")
            ).hexdigest()

            # AOF everysec permite hasta ~1 s de ventana de pérdida.
            # Se espera 2.5 s antes de reiniciar para un ensayo reproducible.
            time.sleep(2.5)
            registro["inicio_reinicio_utc"] = ahora()
            inicio_t = time.perf_counter()
            proceso = subprocess.run(
                reinicio, cwd=ROOT_REPO, text=True, capture_output=True,
                timeout=max(args.wait_seconds, 10), check=False,
            )
            registro["salida_reinicio"] = {
                "returncode": proceso.returncode,
                "stdout": proceso.stdout[-4000:], "stderr": proceso.stderr[-4000:]
            }
            if proceso.returncode:
                raise RuntimeError(f"Fallo al reiniciar Valkey: {proceso.stderr}")
            plazo = time.monotonic() + args.wait_seconds
            while time.monotonic() < plazo:
                try:
                    if cliente.ping():
                        break
                except Exception:
                    time.sleep(0.5)
            else:
                raise TimeoutError("Valkey no respondió tras el reinicio")
            registro["tiempo_reinicio_y_ping_ms"] = round(
                (time.perf_counter() - inicio_t) * 1000, 3
            )
            res = api.post("/sesiones/recuperar", json=datos)
            if res.status_code != 200:
                raise AssertionError(f"Recuperación: {res.status_code} {res.text}")
            recuperado = res.json()
            if recuperado["borrador"] != contenido:
                raise AssertionError("El borrador recuperado es diferente")
            if recuperado["estado"] not in ("activa", "requiere_reasignacion"):
                raise AssertionError("Estado de recuperación no válido")
            registro["estado_lock_tras_reinicio"] = recuperado["estado"]
            registro["version_borrador"] = recuperado["version"]
            registro["borrador_sha256_despues"] = hashlib.sha256(
                json.dumps(recuperado["borrador"], sort_keys=True).encode("utf-8")
            ).hexdigest()
            registro["resultado"] = "aprobado"
            registro["observaciones"].append(
                "El borrador recuperado coincide después de reiniciar Valkey conservando el volumen AOF"
            )
    except Exception as exc:
        registro["error"] = f"{type(exc).__name__}: {exc}"
        raise
    finally:
        # Solo se eliminan las llaves temporales creadas por ESTE experimento.
        try:
            cliente.delete(f"lock:tarea:{datos['tarea_id']}",
                           f"sesion:{datos['anotador_id']}",
                           f"borrador:{datos['tarea_id']}")
            cliente.zrem(f"editando:{datos['proyecto_id']}", datos["anotador_id"])
        except Exception as exc:
            registro["advertencia_limpieza"] = str(exc)
        cliente.close()
        registro["fecha_fin_utc"] = ahora()
        salida = evidencia / nombre
        salida.write_text(json.dumps(registro, indent=2, ensure_ascii=False) + "\n",
                          encoding="utf-8")
        print(f"Resultado: {registro['resultado']}; evidencia: {salida}")


if __name__ == "__main__":
    principal()
