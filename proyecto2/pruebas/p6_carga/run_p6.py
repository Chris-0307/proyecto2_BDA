"""Prueba P6: Carga HTTP contra el gateway con 10, 40 y 100 usuarios."""

from __future__ import annotations

import json
import os
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path

BASE_URL = os.getenv("GATEWAY_URL", "http://localhost:8000")
COMPOSE_FILE = os.getenv("COMPOSE_FILE", "proyecto2/docker-compose.yml")
SIMULADOR = Path("proyecto2/simulador/simulador_http.py")


def obtener_ids_prueba() -> dict:
    codigo = """
import json, os
from pymongo import MongoClient
db = MongoClient(os.environ['MONGO_URI'])['plataforma']
p = db.proyectos.find_one({}, {'_id': 1})
a = list(db.anotadores.find({'es_revisor': False}, {'_id': 1}).limit(10))
r = list(db.anotadores.find({'es_revisor': True}, {'_id': 1}).limit(3))
print(json.dumps({
    'proyecto_id': p['_id'] if p else '',
    'anotadores': ','.join(x['_id'] for x in a),
    'revisores': ','.join(x['_id'] for x in r),
}))
"""
    out = subprocess.check_output(
        ["docker", "compose", "-f", COMPOSE_FILE, "run", "--rm", "registro", "python", "-c", codigo],
        text=True,
    ).strip().splitlines()[-1]
    return json.loads(out)


def main() -> None:
    duracion_s = float(os.getenv("P6_DURACION_S", "10"))
    niveles = [10, 40, 100]
    ids = obtener_ids_prueba()

    print(f"=== P6 Carga HTTP — Inicio UTC: {datetime.now(timezone.utc).isoformat()} ===")
    print("Contexto:", json.dumps(ids))

    for u in niveles:
        print(f"\n--- Nivel de carga: {u} usuarios ({duracion_s}s) ---")
        proc = subprocess.run(
            [
                sys.executable,
                str(SIMULADOR),
                "--base-url",
                BASE_URL,
                "--proyecto-id",
                ids["proyecto_id"],
                "--anotadores",
                ids["anotadores"],
                "--revisores",
                ids["revisores"],
                "--usuarios",
                str(u),
                "--duracion-s",
                str(duracion_s),
                "--semilla",
                str(20261122 + u),
            ],
            capture_output=True,
            text=True,
            check=True,
        )
        datos = json.loads(proc.stdout)
        print(json.dumps(datos, indent=2, ensure_ascii=False))

    print(f"\n=== Fin UTC: {datetime.now(timezone.utc).isoformat()} ===")


if __name__ == "__main__":
    main()
