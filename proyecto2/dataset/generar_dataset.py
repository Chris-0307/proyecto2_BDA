"""Generador reproducible del dataset oficial (MongoDB + datalake Parquet).

Volúmenes del enunciado:
- MongoDB: 60 clientes, 300 anotadores en 30 equipos (10 % revisores),
  120 proyectos activos, 600 lotes y 60 000 tareas
  (30 % texto, 30 % imagen, 20 % video, 20 % LiDAR).
- Parquet: 100 000 tareas históricas de los 180 proyectos (los 120 activos y
  60 cerrados que solo existen en el historial), ~800 000 eventos en los 12
  meses previos a FIN_HISTORIAL, particionados por mes.

Clientes, proyectos, equipos y anotadores tienen los MISMOS ids en MongoDB y
en Parquet (se derivan de la semilla con uuid5), así que las consultas por
cliente, proyecto o equipo cruzan ambos lados.

Cada tarea histórica sigue el flujo real del sistema:
  abierta -> anotada -> en_revision -> (rechazada -> abierta -> ...) -> aprobada
El número de rechazos y la espera de revisión dependen del proyecto, por lo
que Q2, Q4 y Q5 dan resultados distintos entre equipos, proyectos y clientes.

Uso:
  python generar_dataset.py --solo mongo      # en el contenedor del registro
  python generar_dataset.py --solo parquet --dir-eventos /datalake/eventos   # en historial-worker

`--solo mongo` vacía las colecciones del registro antes de cargar (carga desde
cero); `--solo parquet` reemplaza los archivos que generó antes (gen-*.parquet)
y conserva los que publicó el worker.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import random
import sys
import tempfile
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from pathlib import Path
from uuid import NAMESPACE_DNS, uuid5

SEMILLA = 20261122
FIN_HISTORIAL = datetime(2026, 11, 1, tzinfo=timezone.utc)
INICIO_HISTORIAL = FIN_HISTORIAL - timedelta(days=365)
N_CLIENTES, N_EQUIPOS, POR_EQUIPO = 60, 30, 10           # 1 revisor + 9 anotadores por equipo
N_PROY_ACTIVOS, N_PROY_CERRADOS = 120, 60
LOTES_POR_PROYECTO, TAREAS_POR_LOTE = 5, 100             # 600 lotes, 60 000 tareas
N_TAREAS_HIST = 100_000

# (modalidad, tipo_tarea, mime, peso, duración de anotación en s: mín, máx)
MODALIDADES = [
    ("texto", "span", "text/plain", 0.30, 20, 180),
    ("imagen", "bbox", "image/png", 0.15, 30, 300),
    ("imagen", "poligono", "image/png", 0.15, 60, 480),
    ("video", "bbox_video", "video/mp4", 0.20, 120, 900),
    ("lidar", "cuboide_3d", "application/octet-stream", 0.20, 180, 1500),
]
MOTIVOS = ["etiqueta_fuera_taxonomia", "geometria_imprecisa", "oclusion_mal_clasificada",
           "borde_mal_ajustado"]
TAXONOMIA = [
    {"etiqueta": "objeto#principal", "instruccion": "Objeto principal válido de la escena."},
    {"etiqueta": "objeto#secundario", "instruccion": "Objeto secundario o parcial visible."},
    {"etiqueta": "contexto#fondo", "instruccion": "Elemento de contexto sin etiqueta específica."},
]


def gid(semilla: int, tipo: str, i) -> str:
    """Id determinista: el mismo en MongoDB y en Parquet para la misma semilla."""
    return str(uuid5(NAMESPACE_DNS, f"bda-{semilla}-{tipo}-{i}"))


def elegir_modalidad(rng: random.Random):
    r, acum = rng.random(), 0.0
    for m in MODALIDADES:
        acum += m[3]
        if r <= acum:
            return m
    return MODALIDADES[-1]


def catalogo(semilla: int) -> dict:
    """Clientes, equipos, anotadores y proyectos compartidos por ambos lados."""
    rng = random.Random(semilla)
    clientes = [{"_id": gid(semilla, "cliente", i), "nombre": f"Cliente {i + 1:02d}",
                 "creado_en": INICIO_HISTORIAL + timedelta(days=i)} for i in range(N_CLIENTES)]
    equipos = [gid(semilla, "equipo", e) for e in range(N_EQUIPOS)]
    anotadores, revisores, miembros = [], {}, defaultdict(list)
    for e, eq in enumerate(equipos):
        for m in range(POR_EQUIPO):
            es_rev = m == 0
            a = {"_id": gid(semilla, "anotador", f"{e}-{m}"), "equipo_id": eq, "es_revisor": es_rev,
                 "nombre": f"{'Revisor' if es_rev else 'Anotador'} E{e + 1:02d}-{m + 1:02d}"}
            anotadores.append(a)
            if es_rev:
                revisores[eq] = a["_id"]
            else:
                miembros[eq].append(a["_id"])
    proyectos = []
    for p in range(N_PROY_ACTIVOS + N_PROY_CERRADOS):
        proyectos.append({
            "_id": gid(semilla, "proyecto", p),
            "cliente_id": clientes[p % N_CLIENTES]["_id"],
            "nombre": f"Proyecto {p + 1:03d}",
            "sla_horas": float(rng.choice([24, 48, 72])),
            "estado": "activo" if p < N_PROY_ACTIVOS else "cerrado",
            # Comportamiento del proyecto en el historial:
            "p_rechazo": rng.uniform(0.35, 0.70),        # probabilidad de rechazo por revisión
            "cola_revision_h": rng.uniform(2.0, 60.0),    # espera máxima hasta la revisión
        })
    return {"clientes": clientes, "equipos": equipos, "anotadores": anotadores,
            "revisores": revisores, "miembros": miembros, "proyectos": proyectos}


# ---------------------------------------------------------------- MongoDB

def generar_mongo(semilla: int, mongo_uri: str, db_nombre: str) -> dict:
    from pymongo import MongoClient

    cat = catalogo(semilla)
    rng = random.Random(semilla + 1)
    db = MongoClient(mongo_uri, w="majority", serverSelectionTimeoutMS=5000)[db_nombre]

    # Carga desde cero: el registro queda solo con el dataset.
    for col in ("tareas", "batches", "proyectos", "anotadores", "clientes"):
        db[col].delete_many({})

    db.clientes.insert_many(cat["clientes"])
    db.anotadores.insert_many(cat["anotadores"])
    activos = [p for p in cat["proyectos"] if p["estado"] == "activo"]
    db.proyectos.insert_many([
        {"_id": p["_id"], "cliente_id": p["cliente_id"], "nombre": p["nombre"],
         "taxonomia": TAXONOMIA, "sla_horas": p["sla_horas"], "estado": "activo",
         "creado_en": FIN_HISTORIAL - timedelta(days=30)}
        for p in activos
    ])

    lotes, total, buffer = [], 0, []
    for ip, p in enumerate(activos):
        for l in range(LOTES_POR_PROYECTO):
            lote_id = gid(semilla, "lote", f"{ip}-{l}")
            lotes.append({"_id": lote_id, "proyecto_id": p["_id"], "nombre": f"Lote {l + 1}",
                          "n_tareas": TAREAS_POR_LOTE, "creado_en": FIN_HISTORIAL})
            for t in range(TAREAS_POR_LOTE):
                mod, geom, mime, *_ = elegir_modalidad(rng)
                uri = f"s3://dataset/{mod}/{lote_id}/{t:03d}"
                buffer.append({
                    "_id": gid(semilla, "tarea", f"{ip}-{l}-{t}"),
                    "proyecto_id": p["_id"], "batch_id": lote_id, "cliente_id": p["cliente_id"],
                    "modalidad": mod, "tipo_tarea": geom, "prioridad": rng.randint(0, 10),
                    "estado": "disponible", "version": 1, "sla_horas": p["sla_horas"],
                    "archivo": {"uri": uri, "sha256": hashlib.sha256(uri.encode()).hexdigest(),
                                "bytes": rng.randint(512, 50_000_000), "mime": mime,
                                "meta": {"muestra": t}},
                    "asignacion": None, "anotacion": None, "revision": None,
                    "ultimo_evento_id": None, "eventos_pendientes": [], "creado_en": FIN_HISTORIAL,
                })
                if len(buffer) >= 5000:
                    db.tareas.insert_many(buffer, ordered=False)
                    total += len(buffer)
                    buffer.clear()
    if buffer:
        db.tareas.insert_many(buffer, ordered=False)
        total += len(buffer)
    db.batches.insert_many(lotes)

    return {
        "semilla": semilla,
        "clientes": db.clientes.count_documents({}),
        "anotadores": db.anotadores.count_documents({}),
        "revisores": db.anotadores.count_documents({"es_revisor": True}),
        "equipos": len(cat["equipos"]),
        "proyectos": db.proyectos.count_documents({}),
        "lotes": db.batches.count_documents({}),
        "tareas": db.tareas.count_documents({}),
        "tareas_por_modalidad": {
            r["_id"]: r["n"] for r in db.tareas.aggregate(
                [{"$group": {"_id": "$modalidad", "n": {"$sum": 1}}}, {"$sort": {"_id": 1}}])
        },
    }


# ---------------------------------------------------------------- Parquet

def eventos_tarea(semilla: int, idx: int, p: dict, cat: dict, rng: random.Random) -> list[dict]:
    """Eventos de una tarea histórica, en el mismo orden y formato que el sistema."""
    mod, geom, _, _, dmin, dmax = elegir_modalidad(rng)
    eq = rng.choice(cat["equipos"])
    anotador, revisor = rng.choice(cat["miembros"][eq]), cat["revisores"][eq]
    tarea_id = gid(semilla, "tarea-hist", idx)
    base = {"tarea_id": tarea_id, "proyecto_id": p["_id"], "cliente_id": p["cliente_id"],
            "batch_id": gid(semilla, "lote-hist", idx // TAREAS_POR_LOTE), "anotador_id": anotador,
            "equipo_id": eq, "modalidad": mod, "tipo_tarea": geom, "sla_horas": p["sla_horas"],
            "revisor_id": None, "aprobada_en": None, "duracion_ms": None, "motivo_rechazo": None,
            "datos_extra_json": "{}"}
    eventos, version = [], 1
    t = INICIO_HISTORIAL + timedelta(seconds=rng.randint(0, int(timedelta(days=355).total_seconds())))

    def ev(tipo, estado, cuando, tomada, **extra):
        nonlocal version
        version += 1
        eventos.append({**base, **extra, "event_id": gid(semilla, "evento", f"{idx}-{version}"),
                        "tipo_evento": tipo, "estado": estado, "ocurrido_en": cuando,
                        "tomada_en": tomada, "version": version})

    rechazos = 0
    while True:
        tomada = t
        ev("tarea_abierta", "abierta", tomada, tomada)
        dur_ms = rng.randint(dmin, dmax) * 1000
        anotada = tomada + timedelta(milliseconds=dur_ms)
        ev("tarea_anotada", "anotada", anotada, tomada, duracion_ms=dur_ms)
        inicio_rev = anotada + timedelta(hours=rng.uniform(0.1, p["cola_revision_h"]))
        ev("revision_iniciada", "en_revision", inicio_rev, tomada, revisor_id=revisor)
        decision = inicio_rev + timedelta(minutes=rng.uniform(2, 20))
        if rechazos < 4 and rng.random() < p["p_rechazo"]:
            rechazos += 1
            ev("revision_rechazada", "rechazada", decision, tomada, revisor_id=revisor,
               motivo_rechazo=rng.choice(MOTIVOS))
            t = decision + timedelta(hours=rng.uniform(0.2, 12))   # retrabajo: nueva toma
            continue
        ev("revision_aprobada", "aprobada", decision, tomada, revisor_id=revisor, aprobada_en=decision)
        return eventos


def generar_parquet(semilla: int, dir_eventos: Path) -> dict:
    sys.path.insert(0, "/app")  # esquema del worker de Historial (historial-worker)
    import pyarrow as pa
    import pyarrow.parquet as pq
    from app.parquet_writer import ESQUEMA

    cat = catalogo(semilla)
    rng = random.Random(semilla + 2)
    dir_eventos.mkdir(parents=True, exist_ok=True)
    # Reemplaza solo lo que generó antes este script; conserva lo publicado por el worker.
    for viejo in list(dir_eventos.glob("mes=*/gen-*.parquet")) + list(dir_eventos.glob("mes=*/dataset-*.parquet")):
        viejo.unlink()

    staging = dir_eventos / ".staging"
    staging.mkdir(exist_ok=True)
    partes: dict[str, int] = defaultdict(int)
    por_mes: dict[str, list[dict]] = defaultdict(list)
    total = 0

    def volcar():
        nonlocal total
        for mes, filas in por_mes.items():
            if not filas:
                continue
            carpeta = dir_eventos / f"mes={mes}"
            carpeta.mkdir(exist_ok=True)
            fd, tmp = tempfile.mkstemp(prefix="gen-", suffix=".tmp", dir=staging)
            os.close(fd)
            try:
                pq.write_table(pa.Table.from_pylist(filas, schema=ESQUEMA), tmp, compression="snappy")
                os.replace(tmp, carpeta / f"gen-{semilla}-{mes}-p{partes[mes]:03d}.parquet")
            finally:
                Path(tmp).unlink(missing_ok=True)
            partes[mes] += 1
            total += len(filas)
            filas.clear()

    for idx in range(N_TAREAS_HIST):
        p = cat["proyectos"][idx % len(cat["proyectos"])]
        for e in eventos_tarea(semilla, idx, p, cat, rng):
            por_mes[e["ocurrido_en"].strftime("%Y-%m")].append(e)
        if (idx + 1) % 20_000 == 0:
            volcar()
    volcar()

    return {
        "semilla": semilla,
        "tareas_historial": N_TAREAS_HIST,
        "proyectos_historial": len(cat["proyectos"]),
        "eventos_generados": total,
        "archivos_generados": sum(partes.values()),
        "meses": sorted(partes),
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Generador reproducible del dataset")
    parser.add_argument("--semilla", type=int, default=SEMILLA)
    parser.add_argument("--solo", choices=["mongo", "parquet", "todo"], default="todo")
    parser.add_argument("--dir-eventos", default=os.getenv("DATALAKE_EVENTOS", "/datalake/eventos"))
    args = parser.parse_args()

    resumen: dict = {"generado_en": datetime.now(timezone.utc).isoformat()}
    if args.solo in ("mongo", "todo"):
        resumen["mongo"] = generar_mongo(args.semilla, os.environ["MONGO_URI"],
                                         os.getenv("MONGO_DB", "plataforma"))
    if args.solo in ("parquet", "todo"):
        resumen["parquet"] = generar_parquet(args.semilla, Path(args.dir_eventos))
    print(json.dumps(resumen, indent=2, ensure_ascii=False))


if __name__ == "__main__":
    main()
