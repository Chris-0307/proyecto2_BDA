"""Carga de DEMOSTRACIÓN para desarrollar el registro (no es el dataset oficial).

Inserta un cliente y seis anotadores (dos son revisores, en dos equipos)
directamente en MongoDB, y crea un proyecto LiDAR y un lote por la API REST
del registro. Los identificadores de cliente y anotadores son deterministas
(uuid5): ejecutarlo varias veces no duplica ni cambia a esas personas.

Uso (con el sistema levantado):
  docker compose -f proyecto2/docker-compose.yml run --rm registro python /dataset/cargar_demo.py
"""

import hashlib
import json
import os
import urllib.request
from datetime import datetime, timezone
from uuid import NAMESPACE_DNS, uuid5

from pymongo import MongoClient

REGISTRO = os.getenv("REGISTRO_URL", "http://registro:8003")
MONGO_URI = os.getenv("MONGO_URI")
N_TAREAS = int(os.getenv("DEMO_TAREAS", "60"))


def uid(nombre: str) -> str:
    return str(uuid5(NAMESPACE_DNS, nombre))


CLIENTE_ID = uid("demo-cliente")
EQUIPOS = [uid("demo-equipo-1"), uid("demo-equipo-2")]
# (nombre, equipo, es_revisor)
ANOTADORES = [
    ("demo-anotador-1", 0, False), ("demo-anotador-2", 0, False),
    ("demo-anotador-3", 1, False), ("demo-anotador-4", 1, False),
    ("demo-revisor-1", 0, True), ("demo-revisor-2", 1, True),
]

TAXONOMIA = [
    {"etiqueta": "lidar#peaton", "instruccion": "Persona a pie."},
    {"etiqueta": "lidar#vehiculo", "instruccion": "Automóvil, camión o bus."},
    {"etiqueta": "lidar#ciclista", "instruccion": "Ciclista; un skater se clasifica aquí, no como peatón."},
    {"etiqueta": "lidar#signal", "instruccion": "Señal de tránsito o semáforo."},
]

# Reparto aproximado del enunciado: 30 % texto, 30 % imagen, 20 % video, 20 % LiDAR.
CICLO = (["texto"] * 3 + ["imagen"] * 3 + ["video"] * 2 + ["lidar"] * 2)
GEOMETRIA = {"texto": "span", "imagen": "bbox", "video": "bbox_video", "lidar": "cuboide_3d"}
MIME = {"texto": "text/plain", "imagen": "image/png", "video": "video/mp4",
        "lidar": "application/octet-stream"}


def llamar(ruta: str, cuerpo: dict) -> dict:
    req = urllib.request.Request(
        REGISTRO + ruta, data=json.dumps(cuerpo).encode(), method="POST",
        headers={"Content-Type": "application/json"},
    )
    with urllib.request.urlopen(req, timeout=30) as r:
        return json.loads(r.read())


def main() -> None:
    db = MongoClient(MONGO_URI, w="majority", serverSelectionTimeoutMS=5000)["plataforma"]
    ahora = datetime.now(timezone.utc)
    db.clientes.replace_one(
        {"_id": CLIENTE_ID},
        {"_id": CLIENTE_ID, "nombre": "Cliente demostración", "creado_en": ahora},
        upsert=True,
    )
    for nombre, equipo, revisor in ANOTADORES:
        doc = {"_id": uid(nombre), "nombre": nombre, "equipo_id": EQUIPOS[equipo],
               "es_revisor": revisor}
        db.anotadores.replace_one({"_id": doc["_id"]}, doc, upsert=True)

    proyecto = llamar("/proyectos", {
        "cliente_id": CLIENTE_ID, "nombre": "Demo LiDAR", "taxonomia": TAXONOMIA,
        "sla_horas": 48,
    })
    tareas = []
    for i in range(N_TAREAS):
        modalidad = CICLO[i % len(CICLO)]
        uri = f"s3://demo/{modalidad}/{i:05d}"
        tareas.append({
            "modalidad": modalidad,
            "tipo_tarea": GEOMETRIA[modalidad],
            "prioridad": i % 5,
            "archivo": {
                "uri": uri,
                "sha256": hashlib.sha256(uri.encode()).hexdigest(),
                "bytes": 1024 * (i + 1),
                "mime": MIME[modalidad],
                "meta": {"minimo": 0.0, "maximo": 100.0},
            },
        })
    lote = llamar(f"/proyectos/{proyecto['_id']}/lotes", {"nombre": "Lote demo", "tareas": tareas})
    print(json.dumps({
        "cliente_id": CLIENTE_ID,
        "anotadores": len(ANOTADORES),
        "proyecto_id": proyecto["_id"],
        "batch_id": lote["batch_id"],
        "tareas_creadas": lote["tareas_creadas"],
    }, indent=2))


if __name__ == "__main__":
    main()