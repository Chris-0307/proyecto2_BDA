"""Prueba P6: Carga HTTP contra el gateway con 10, 40 y 100 usuarios.

Usa varios proyectos del dataset (P6_PROYECTOS, por defecto 100): el usuario u
trabaja en el proyecto u % n, así 100 usuarios no agotan el inventario de un
solo proyecto (500 tareas). P6_PROYECTO_ID fija uno solo, como antes.
"""

from __future__ import annotations

import json
import os
import subprocess
import sys
import threading
import time
from datetime import datetime, timezone
from pathlib import Path

BASE_URL = os.getenv("GATEWAY_URL", "http://localhost:8000")
COMPOSE_FILE = os.getenv("COMPOSE_FILE", "proyecto2/docker-compose.yml")
SIMULADOR = Path("proyecto2/simulador/simulador_http.py")


def _ejecutar_mongo_python(codigo: str) -> dict:
    out = subprocess.check_output(
        ["docker", "compose", "-f", COMPOSE_FILE, "exec", "-T", "registro", "python", "-c", codigo],
        text=True,
    ).strip().splitlines()[-1]
    return json.loads(out)


def obtener_ids_y_inventario(proyecto_id_fijo: str = "", n_proyectos: int = 100) -> dict:
    # Proyectos del dataset generado ("Proyecto NNN"), en orden estable.
    filtro_proy = (f"{{'_id': '{proyecto_id_fijo}'}}" if proyecto_id_fijo
                   else "{'nombre': {'$regex': '^Proyecto [0-9]+$'}}")
    codigo = f"""
import json, os
from pymongo import MongoClient
db = MongoClient(os.environ['MONGO_URI'])['plataforma']
ps = list(db.proyectos.find({filtro_proy}, {{'_id': 1}}).sort('nombre', 1).limit({n_proyectos}))
pids = [p['_id'] for p in ps]
disp = db.tareas.count_documents({{'proyecto_id': {{'$in': pids}}, 'estado': 'disponible'}})
a = list(db.anotadores.find({{'es_revisor': False, 'nombre': {{'$regex': '^Anotador E'}}}}, {{'_id': 1}}).sort('_id', 1).limit(100))
r = list(db.anotadores.find({{'es_revisor': True, 'nombre': {{'$regex': '^Revisor E'}}}}, {{'_id': 1}}).sort('_id', 1).limit(30))
print(json.dumps({{
    'proyecto_id': ','.join(pids),
    'proyectos': len(pids),
    'tareas_disponibles': disp,
    'anotadores': ','.join(x['_id'] for x in a),
    'revisores': ','.join(x['_id'] for x in r),
}}))
"""
    return _ejecutar_mongo_python(codigo)


def contar_disponibles(proyecto_id: str) -> int:
    pids = [p for p in proyecto_id.split(",") if p]
    codigo = f"""
import json, os
from pymongo import MongoClient
db = MongoClient(os.environ['MONGO_URI'])['plataforma']
disp = db.tareas.count_documents({{'proyecto_id': {{'$in': {pids!r}}}, 'estado': 'disponible'}})
print(json.dumps({{'tareas_disponibles': disp}}))
"""
    return int(_ejecutar_mongo_python(codigo)["tareas_disponibles"])


def _muestrear_docker_stats(stop_event: threading.Event, muestras: list[dict], intervalo_s: float = 15.0) -> None:
    while not stop_event.is_set():
        ts = datetime.now(timezone.utc).isoformat()
        try:
            out = subprocess.check_output(
                [
                    "docker",
                    "stats",
                    "--no-stream",
                    "--format",
                    "{{.Name}}\t{{.CPUPerc}}\t{{.MemUsage}}",
                ],
                text=True,
                stderr=subprocess.DEVNULL,
            ).strip()
            filas = []
            for linea in out.splitlines():
                partes = linea.split("\t")
                if len(partes) == 3:
                    filas.append({"contenedor": partes[0], "cpu": partes[1], "mem": partes[2]})
            muestras.append({"ts_utc": ts, "contenedores": filas})
        except Exception as exc:
            muestras.append({"ts_utc": ts, "error": str(exc)})
        stop_event.wait(intervalo_s)


def correr_simulador(
    proyecto_id: str,
    anotadores: str,
    revisores: str,
    usuarios: int,
    duracion_s: float,
    semilla: int,
    capturar_stats: bool = False,
) -> dict:
    muestras_stats: list[dict] = []
    stop_evt = threading.Event()
    hilo_stats: threading.Thread | None = None

    if capturar_stats:
        hilo_stats = threading.Thread(
            target=_muestrear_docker_stats,
            args=(stop_evt, muestras_stats, 15.0),
            daemon=True,
        )
        hilo_stats.start()

    try:
        proc = subprocess.run(
            [
                sys.executable,
                str(SIMULADOR),
                "--base-url",
                BASE_URL,
                "--proyecto-id",
                proyecto_id,
                "--anotadores",
                anotadores,
                "--revisores",
                revisores,
                "--usuarios",
                str(usuarios),
                "--duracion-s",
                str(duracion_s),
                "--semilla",
                str(semilla),
            ],
            capture_output=True,
            text=True,
            check=True,
        )
    finally:
        if hilo_stats is not None:
            stop_evt.set()
            hilo_stats.join(timeout=5.0)

    datos = json.loads(proc.stdout)
    if capturar_stats:
        datos["docker_stats_15s"] = muestras_stats
    return datos


def main() -> None:
    warmup_s = float(os.getenv("P6_WARMUP_S", "30"))
    duracion_s = float(os.getenv("P6_DURACION_S", "90"))
    repeticiones = int(os.getenv("P6_REPETICIONES", "3"))
    proyecto_fijo = os.getenv("P6_PROYECTO_ID", "")
    n_proyectos = int(os.getenv("P6_PROYECTOS", "100"))
    niveles = [10, 40, 100]

    ctx = obtener_ids_y_inventario(proyecto_fijo, n_proyectos)
    print(f"=== P6 Carga HTTP — Inicio UTC: {datetime.now(timezone.utc).isoformat()} ===")
    print("Contexto inicial:", json.dumps({
        "proyectos": ctx["proyectos"],
        "tareas_disponibles": ctx["tareas_disponibles"],
        "anotadores": len(ctx["anotadores"].split(",")),
        "revisores": len(ctx["revisores"].split(",")),
        "reparto": "usuario u -> proyecto u % proyectos",
    }, ensure_ascii=False))
    if ctx["proyectos"] == 0:
        raise SystemExit("No hay proyectos del dataset: ejecute make datos.")

    for u in niveles:
        print(f"\n--- Nivel de carga: {u} usuarios (warmup={warmup_s}s, {repeticiones}x{duracion_s}s) ---")
        if warmup_s > 0:
            _ = correr_simulador(
                ctx["proyecto_id"],
                ctx["anotadores"],
                ctx["revisores"],
                u,
                warmup_s,
                20261122 + u * 10,
                capturar_stats=False,
            )
            print(f"Calentamiento {warmup_s}s completado para {u} usuarios.")

        corridas = []
        for rep in range(1, repeticiones + 1):
            disp_antes = contar_disponibles(ctx["proyecto_id"])
            res = correr_simulador(
                ctx["proyecto_id"],
                ctx["anotadores"],
                ctx["revisores"],
                u,
                duracion_s,
                20261122 + u * 100 + rep,
                capturar_stats=True,
            )
            disp_despues = contar_disponibles(ctx["proyecto_id"])
            res["repeticion"] = rep
            res["tareas_disponibles_antes"] = disp_antes
            res["tareas_disponibles_despues"] = disp_despues
            corridas.append(res)

        print(json.dumps({"usuarios": u, "corridas": corridas}, indent=2, ensure_ascii=False))

    print(f"\n=== Fin UTC: {datetime.now(timezone.utc).isoformat()} ===")


if __name__ == "__main__":
    main()