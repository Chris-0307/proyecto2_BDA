"""Generador reproducible del dataset oficial (MongoDB + Datalake Parquet)."""

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

SEED_DEFECTO = 20261122
MODALIDADES_PESOS = [
    ("texto", "span", "text/plain", 0.30),
    ("imagen", "bbox", "image/png", 0.15),
    ("imagen", "poligono", "image/png", 0.15),
    ("video", "bbox_video", "video/mp4", 0.20),
    ("lidar", "cuboide_3d", "application/octet-stream", 0.20),
]
MOTIVOS_RECHAZO = [
    "Etiqueta fuera de taxonomía",
    "Geometría imprecisa o incompleta",
    "Oclusión mal clasificada",
    "Borde mal ajustado",
]
TAXONOMIA_BASE = [
    {"etiqueta": "objeto#principal", "instruccion": "Objeto principal válido de la escena."},
    {"etiqueta": "objeto#secundario", "instruccion": "Objeto secundario o parcial visible."},
    {"etiqueta": "contexto#fondo", "instruccion": "Elemento de contexto sin etiqueta específica."},
]


def uid(rng: random.Random, etiqueta: str) -> str:
    return str(uuid5(NAMESPACE_DNS, f"{etiqueta}-{rng.getrandbits(128):032x}"))


def elegir_modalidad(rng: random.Random) -> tuple[str, str, str]:
    r = rng.random()
    acum = 0.0
    for mod, geom, mime, peso in MODALIDADES_PESOS:
        acum += peso
        if r <= acum:
            return mod, geom, mime
    return "lidar", "cuboide_3d", "application/octet-stream"


def generar_mongo(semilla: int, mongo_uri: str, db_nombre: str) -> dict:
    from pymongo import MongoClient

    rng = random.Random(semilla)
    cliente_mongo = MongoClient(mongo_uri, w="majority", serverSelectionTimeoutMS=5000)
    db = cliente_mongo[db_nombre]
    base_dt = datetime(2026, 1, 1, 0, 0, tzinfo=timezone.utc)

    clientes = [
        {
            "_id": uid(rng, f"cliente-{i}"),
            "nombre": f"Cliente {i + 1:02d}",
            "creado_en": base_dt + timedelta(days=i * 2),
        }
        for i in range(60)
    ]
    db.clientes.delete_many({"_id": {"$in": [c["_id"] for c in clientes]}})
    db.clientes.insert_many(clientes)

    equipos = [uid(rng, f"equipo-{e}") for e in range(30)]
    anotadores = []
    for idx_eq, eq_id in enumerate(equipos):
        for m in range(10):
            es_revisor = m == 0
            anotadores.append({
                "_id": uid(rng, f"anotador-{idx_eq}-{m}"),
                "nombre": f"{'Revisor' if es_revisor else 'Anotador'} E{idx_eq + 1:02d}-{m + 1:02d}",
                "equipo_id": eq_id,
                "es_revisor": es_revisor,
            })
    db.anotadores.delete_many({"_id": {"$in": [a["_id"] for a in anotadores]}})
    db.anotadores.insert_many(anotadores)

    proyectos_mongo = [
        {
            "_id": uid(rng, f"proy-reg-{p}"),
            "cliente_id": clientes[p % len(clientes)]["_id"],
            "nombre": f"Proyecto {p + 1:03d}",
            "taxonomia": TAXONOMIA_BASE,
            "sla_horas": rng.choice([24.0, 48.0, 72.0]),
            "estado": "activo",
            "creado_en": base_dt + timedelta(days=p),
        }
        for p in range(120)
    ]
    db.proyectos.delete_many({"_id": {"$in": [pr["_id"] for pr in proyectos_mongo]}})
    db.proyectos.insert_many(proyectos_mongo)

    lotes = [
        {
            "_id": uid(rng, f"lote-{l}"),
            "proyecto_id": proyectos_mongo[l % len(proyectos_mongo)]["_id"],
            "nombre": f"Lote {l + 1:03d}",
            "n_tareas": 100,
            "creado_en": proyectos_mongo[l % len(proyectos_mongo)]["creado_en"]
            + timedelta(hours=(l // 120) * 6),
        }
        for l in range(600)
    ]
    db.batches.delete_many({"_id": {"$in": [b["_id"] for b in lotes]}})
    db.batches.insert_many(lotes)

    proy_por_id = {pr["_id"]: pr for pr in proyectos_mongo}
    tareas_batch = []
    total_tareas = 0
    for lote in lotes:
        proy = proy_por_id[lote["proyecto_id"]]
        for t in range(100):
            mod, geom, mime = elegir_modalidad(rng)
            uri = f"s3://dataset/{mod}/{lote['_id']}/{t:03d}"
            sha = hashlib.sha256(f"{semilla}:{uri}".encode()).hexdigest()
            tareas_batch.append({
                "_id": uid(rng, f"tarea-{lote['_id']}-{t}"),
                "proyecto_id": proy["_id"],
                "batch_id": lote["_id"],
                "cliente_id": proy["cliente_id"],
                "modalidad": mod,
                "tipo_tarea": geom,
                "prioridad": rng.randint(0, 10),
                "estado": "disponible",
                "version": 1,
                "sla_horas": proy["sla_horas"],
                "archivo": {
                    "uri": uri,
                    "sha256": sha,
                    "bytes": rng.randint(512, 50_000_000),
                    "mime": mime,
                    "meta": {"muestra": t},
                },
                "asignacion": None,
                "anotacion": None,
                "revision": None,
                "ultimo_evento_id": None,
                "eventos_pendientes": [],
                "creado_en": lote["creado_en"],
            })
            if len(tareas_batch) >= 5000:
                db.tareas.insert_many(tareas_batch, ordered=False)
                total_tareas += len(tareas_batch)
                tareas_batch.clear()

    if tareas_batch:
        db.tareas.insert_many(tareas_batch, ordered=False)
        total_tareas += len(tareas_batch)

    return {
        "semilla": semilla,
        "clientes": len(clientes),
        "anotadores": len(anotadores),
        "revisores": sum(1 for a in anotadores if a["es_revisor"]),
        "equipos": len(equipos),
        "proyectos_mongo": len(proyectos_mongo),
        "proyectos_solo_historial_previstos": 60,
        "lotes": len(lotes),
        "tareas_mongo": total_tareas,
    }


def generar_parquet(semilla: int, dir_eventos: Path) -> dict:
    if "/app" not in sys.path:
        sys.path.insert(0, "/app")
    import pyarrow as pa
    import pyarrow.parquet as pq
    from app.parquet_writer import ESQUEMA

    rng = random.Random(semilla + 999)
    equipos = [uid(rng, f"eq-hist-{e}") for e in range(30)]
    anotadores_por_eq = {
        eq_id: [uid(rng, f"ant-{idx_eq}-{m}") for m in range(9)]
        for idx_eq, eq_id in enumerate(equipos)
    }
    revisores_por_eq = {
        eq_id: [uid(rng, f"rev-{idx_eq}-0")]
        for idx_eq, eq_id in enumerate(equipos)
    }

    clientes_ids = [uid(rng, f"cli-hist-{c}") for c in range(60)]
    proyectos_hist = [
        {
            "proyecto_id": uid(rng, f"proy-hist-{p}"),
            "cliente_id": clientes_ids[p % len(clientes_ids)],
            "sla_horas": float(rng.choice([24, 48, 72])),
        }
        for p in range(180)
    ]

    # Limpia ejecuciones previas del generador para evitar duplicados o residuos de otros meses
    for mes_dir in dir_eventos.glob("mes=*"):
        for old_f in mes_dir.glob(f"dataset-{semilla}-*.parquet"):
            old_f.unlink()
        for old_p in mes_dir.glob(f"gen-{semilla}-*.parquet"):
            old_p.unlink()

    inicio_12m = datetime(2025, 1, 1, 0, 0, tzinfo=timezone.utc)
    span_segundos = int(timedelta(days=362).total_seconds())

    n_tareas = 100_000
    por_mes: dict[str, list[dict]] = defaultdict(list)
    parte_por_mes: dict[str, int] = defaultdict(int)

    def escribir_parte(mes: str, filas: list[dict]) -> None:
        if not filas:
            return
        carpeta = dir_eventos / f"mes={mes}"
        carpeta.mkdir(parents=True, exist_ok=True)
        staging = dir_eventos / ".staging"
        staging.mkdir(parents=True, exist_ok=True)
        idx_parte = parte_por_mes[mes]
        parte_por_mes[mes] += 1

        fd, tmp_name = tempfile.mkstemp(prefix="gen-", suffix=".tmp", dir=staging)
        os.close(fd)
        tmp_path = Path(tmp_name)
        try:
            tabla = pa.Table.from_pylist(filas, schema=ESQUEMA)
            pq.write_table(tabla, tmp_path, compression="snappy")
            destino = carpeta / f"gen-{semilla}-{mes}-p{idx_parte:03d}.parquet"
            os.replace(tmp_path, destino)
        finally:
            tmp_path.unlink(missing_ok=True)

    for t_idx in range(n_tareas):
        proy = proyectos_hist[t_idx % len(proyectos_hist)]
        batch_id = uid(rng, f"batch-hist-{t_idx // 100}")
        tarea_id = uid(rng, f"tarea-hist-{t_idx}")
        eq_id = equipos[t_idx % len(equipos)]
        ant_id = rng.choice(anotadores_por_eq[eq_id])
        rev_id = rng.choice(revisores_por_eq[eq_id])
        mod, geom, _ = elegir_modalidad(rng)

        t0 = inicio_12m + timedelta(seconds=rng.randint(0, span_segundos))
        version = 1

        for ronda in range(2):
            dur_ms = rng.randint(15_000, 180_000)
            t_abierta = t0 + timedelta(seconds=ronda * 600)
            t_anotada = t_abierta + timedelta(milliseconds=dur_ms)
            t_rev_ini = t_anotada + timedelta(seconds=10)

            for tipo_ev, estado, ocur_dt, extra_dur, extra_rev, extra_aprob, extra_motivo in [
                ("tarea_abierta", "abierta", t_abierta, None, None, None, None),
                ("tarea_anotada", "anotada", t_anotada, dur_ms, None, None, None),
                ("revision_iniciada", "en_revision", t_rev_ini, None, rev_id, None, None),
            ]:
                por_mes[ocur_dt.strftime("%Y-%m")].append({
                    "event_id": uid(rng, f"ev-{tarea_id}-{version}"),
                    "tipo_evento": tipo_ev,
                    "estado": estado,
                    "ocurrido_en": ocur_dt,
                    "tarea_id": tarea_id,
                    "proyecto_id": proy["proyecto_id"],
                    "cliente_id": proy["cliente_id"],
                    "batch_id": batch_id,
                    "anotador_id": ant_id,
                    "revisor_id": extra_rev,
                    "equipo_id": eq_id,
                    "modalidad": mod,
                    "tipo_tarea": geom,
                    "version": version,
                    "tomada_en": t_abierta,
                    "aprobada_en": extra_aprob,
                    "duracion_ms": extra_dur,
                    "sla_horas": proy["sla_horas"],
                    "motivo_rechazo": extra_motivo,
                    "datos_extra_json": "{}",
                })
                version += 1

        t_rech = t0 + timedelta(seconds=1300)
        por_mes[t_rech.strftime("%Y-%m")].append({
            "event_id": uid(rng, f"ev-{tarea_id}-{version}"),
            "tipo_evento": "revision_rechazada",
            "estado": "rechazada",
            "ocurrido_en": t_rech,
            "tarea_id": tarea_id,
            "proyecto_id": proy["proyecto_id"],
            "cliente_id": proy["cliente_id"],
            "batch_id": batch_id,
            "anotador_id": ant_id,
            "revisor_id": rev_id,
            "equipo_id": eq_id,
            "modalidad": mod,
            "tipo_tarea": geom,
            "version": version,
            "tomada_en": t0,
            "aprobada_en": None,
            "duracion_ms": None,
            "sla_horas": proy["sla_horas"],
            "motivo_rechazo": rng.choice(MOTIVOS_RECHAZO),
            "datos_extra_json": "{}",
        })
        version += 1

        t_aprob = t0 + timedelta(hours=rng.randint(1, 12))
        por_mes[t_aprob.strftime("%Y-%m")].append({
            "event_id": uid(rng, f"ev-{tarea_id}-{version}"),
            "tipo_evento": "revision_aprobada",
            "estado": "aprobada",
            "ocurrido_en": t_aprob,
            "tarea_id": tarea_id,
            "proyecto_id": proy["proyecto_id"],
            "cliente_id": proy["cliente_id"],
            "batch_id": batch_id,
            "anotador_id": ant_id,
            "revisor_id": rev_id,
            "equipo_id": eq_id,
            "modalidad": mod,
            "tipo_tarea": geom,
            "version": version,
            "tomada_en": t0,
            "aprobada_en": t_aprob,
            "duracion_ms": None,
            "sla_horas": proy["sla_horas"],
            "motivo_rechazo": None,
            "datos_extra_json": "{}",
        })

        if (t_idx + 1) % 25_000 == 0:
            for mes, filas in por_mes.items():
                escribir_parte(mes, filas)
                filas.clear()

    for mes, filas in por_mes.items():
        escribir_parte(mes, filas)
        filas.clear()

    total_eventos = 0
    for mes_dir in sorted(dir_eventos.glob("mes=*")):
        for pq_file in mes_dir.glob("*.parquet"):
            total_eventos += pq.read_metadata(pq_file).num_rows

    return {
        "semilla": semilla,
        "tareas_historial": n_tareas,
        "eventos_parquet": total_eventos,
        "meses": sorted(d.name for d in dir_eventos.glob("mes=*")),
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Generador reproducible de dataset")
    parser.add_argument("--semilla", type=int, default=SEED_DEFECTO)
    parser.add_argument("--solo", choices=["mongo", "parquet", "todo"], default="todo")
    parser.add_argument(
        "--dir-eventos",
        default=os.getenv("DATALAKE_EVENTOS", "/workspace/datalake/eventos"),
    )
    args = parser.parse_args()

    resumen: dict = {}
    if args.solo in ("mongo", "todo"):
        resumen["mongo"] = generar_mongo(
            args.semilla,
            os.environ["MONGO_URI"],
            os.getenv("MONGO_DB", "plataforma"),
        )
    if args.solo in ("parquet", "todo"):
        resumen["parquet"] = generar_parquet(args.semilla, Path(args.dir_eventos))

    print(json.dumps(resumen, indent=2, ensure_ascii=False))


if __name__ == "__main__":
    main()
