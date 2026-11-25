"""Compactación manual del datalake Parquet (sección 7 del informe).

Junta los micro-lotes de cada partición mensual con muchos archivos en un solo
archivo, deduplicado por event_id, y lo publica de forma atómica
(.staging + fsync + os.replace). Los originales se mueven a un respaldo fuera
del patrón que lee Spark (eventos/mes=*/*.parquet); no se borran.

Debe correr con historial-worker detenido (el Makefile lo hace): los eventos
nuevos esperan en Valkey Streams y se escriben al reanudar el worker. Los
archivos del generador (gen-*, dataset-*) no se tocan: `make datos` los
reemplaza.

  make compactar
"""

import argparse
import json
import os
import sys
import tempfile
import time
from datetime import datetime, timezone
from pathlib import Path

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

sys.path.insert(0, "/app")
from app.parquet_writer import ESQUEMA  # noqa: E402

PROPIOS_GENERADOR = ("gen-", "dataset-")


def fsync_dir(ruta: Path) -> None:
    fd = os.open(ruta, os.O_RDONLY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


def compactar_mes(mes_dir: Path, staging: Path, respaldo: Path, sello: str) -> dict | None:
    archivos = sorted(p for p in mes_dir.glob("*.parquet") if not p.name.startswith(PROPIOS_GENERADOR))
    t0 = time.perf_counter()
    tabla = pa.concat_tables([pq.read_table(p).select(ESQUEMA.names).cast(ESQUEMA) for p in archivos])
    filas = tabla.num_rows

    # Deduplicación por event_id: se conserva la primera aparición.
    vistos, indices = set(), []
    for i, eid in enumerate(tabla.column("event_id").to_pylist()):
        if eid not in vistos:
            vistos.add(eid)
            indices.append(i)
    unicos = tabla.take(pa.array(indices))
    unicos = unicos.take(pc.sort_indices(unicos, sort_keys=[("ocurrido_en", "ascending")]))

    destino = mes_dir / f"compactado-{sello}.parquet"
    fd, tmp = tempfile.mkstemp(prefix="compactado-", suffix=".tmp", dir=staging)
    os.close(fd)
    try:
        pq.write_table(unicos, tmp, compression="snappy")
        with open(tmp, "rb") as f:
            os.fsync(f.fileno())
        os.replace(tmp, destino)
        fsync_dir(mes_dir)
    finally:
        Path(tmp).unlink(missing_ok=True)

    # Verificación antes de retirar los originales.
    leido = pq.read_table(destino)
    if leido.num_rows != len(vistos) or set(leido.column("event_id").to_pylist()) != vistos:
        destino.unlink()
        raise RuntimeError(f"{mes_dir.name}: la verificación falló; se conservan los originales")

    dest_resp = respaldo / mes_dir.name
    dest_resp.mkdir(parents=True, exist_ok=True)
    bytes_antes = sum(p.stat().st_size for p in archivos)
    for p in archivos:
        os.replace(p, dest_resp / p.name)
    return {
        "mes": mes_dir.name, "archivos_antes": len(archivos), "archivos_despues": 1,
        "filas": filas, "event_id_unicos": len(vistos), "duplicados_quitados": filas - len(vistos),
        "bytes_antes": bytes_antes, "bytes_despues": destino.stat().st_size,
        "archivo": destino.name, "segundos": round(time.perf_counter() - t0, 2),
    }


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--dir-eventos", default="/datalake/eventos")
    ap.add_argument("--respaldo", default="/datalake/respaldo_compactacion")
    ap.add_argument("--min-archivos", type=int, default=20,
                    help="solo compacta particiones con al menos esta cantidad de micro-lotes")
    args = ap.parse_args()

    base = Path(args.dir_eventos)
    staging = base / ".staging"
    staging.mkdir(parents=True, exist_ok=True)
    sello = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    respaldo = Path(args.respaldo) / sello

    print(f"Inicio UTC: {datetime.now(timezone.utc).isoformat()}")
    print("Objetivo: compactar micro-lotes Parquet por partición mensual (deduplicado por event_id)")
    print(f"Configuración: {base}, mínimo {args.min_archivos} micro-lotes por partición; "
          f"respaldo en {respaldo}; historial-worker detenido")

    resultados = []
    for mes_dir in sorted(base.glob("mes=*")):
        n = sum(1 for p in mes_dir.glob("*.parquet") if not p.name.startswith(PROPIOS_GENERADOR))
        if n < args.min_archivos:
            continue
        r = compactar_mes(mes_dir, staging, respaldo, sello)
        resultados.append(r)
        print(f"[OK] {r['mes']}: {r['archivos_antes']} -> 1 archivo; {r['filas']} filas -> "
              f"{r['event_id_unicos']} event_id únicos; {r['bytes_antes']:,} -> {r['bytes_despues']:,} bytes; "
              f"{r['segundos']} s")
    if not resultados:
        print(f"Ninguna partición con {args.min_archivos} o más micro-lotes; nada que compactar.")

    total = sum(1 for _ in base.glob("mes=*/*.parquet"))
    print(f"Archivos Parquet publicados tras compactar: {total}")
    print("Resumen JSON:", json.dumps(resultados, ensure_ascii=False))
    print(f"Fin UTC: {datetime.now(timezone.utc).isoformat()}\n")
    return 0


if __name__ == "__main__":
    sys.exit(main())
