"""Diferencia observada entre w:1 y w:"majority" en el conjunto rs-plataforma.

Mide con el driver (sin gateway) las dos escrituras que hace el registro:
- insert_one: alta de una tarea.
- find_one_and_update con filtro por estado: la transición atómica.

Configuraciones: w:1 y w:"majority" (wtimeout 5 s), con 1 y 8 hilos. Cada
configuración se repite REPETICIONES veces alternando el orden para que el
calentamiento no favorezca a ninguna. Usa una base aparte (`bench_wc`) que se
borra al inicio y al final; no toca `plataforma`.

Limitación: los tres nodos corren en la misma máquina (Docker local), así que la
replicación no paga latencia de red real. Estas cifras no se pueden extrapolar a
un clúster distribuido en varias máquinas.

  docker compose -f proyecto2/docker-compose.yml run --rm registro \
    python /pruebas/p2_registro/write_concern.py | tee proyecto2/evidence/p2_write_concern.txt
"""

import os
import platform
import statistics
import threading
import time
from datetime import datetime, timezone
from uuid import uuid4

from pymongo import MongoClient, ReturnDocument, WriteConcern

MONGO_URI = os.getenv("MONGO_URI")
OPS = int(os.getenv("WC_OPS", "500"))            # operaciones por hilo y repetición
REPETICIONES = int(os.getenv("WC_REPETICIONES", "3"))
HILOS = [1, 8]
CONCERNS = {"w:1": WriteConcern(w=1), "w:majority": WriteConcern(w="majority", wtimeout=5000)}


def percentil(v, p):
    o = sorted(v)
    return o[max(0, min(len(o) - 1, round(p / 100 * (len(o) - 1))))]


def correr(db, nombre_wc: str, hilos: int) -> dict:
    col = db.get_collection("ops", write_concern=CONCERNS[nombre_wc])
    lat = {"insert_one": [], "find_one_and_update": []}
    candado = threading.Lock()

    def trabajador():
        loc = {"insert_one": [], "find_one_and_update": []}
        for _ in range(OPS):
            tid = str(uuid4())
            t0 = time.perf_counter()
            col.insert_one({"_id": tid, "estado": "disponible", "version": 1, "prioridad": 1})
            t1 = time.perf_counter()
            col.find_one_and_update(
                {"_id": tid, "estado": "disponible"},
                {"$set": {"estado": "abierta"}, "$inc": {"version": 1}},
                return_document=ReturnDocument.AFTER,
            )
            t2 = time.perf_counter()
            loc["insert_one"].append((t1 - t0) * 1000)
            loc["find_one_and_update"].append((t2 - t1) * 1000)
        with candado:
            for k in lat:
                lat[k] += loc[k]

    hs = [threading.Thread(target=trabajador) for _ in range(hilos)]
    inicio = time.perf_counter()
    for h in hs:
        h.start()
    for h in hs:
        h.join()
    return {"lat": lat, "segundos": time.perf_counter() - inicio, "ops": 2 * OPS * hilos}


def main():
    print(f"Inicio UTC: {datetime.now(timezone.utc).isoformat()}")
    print('Objetivo: diferencia observada entre w:1 y w:"majority" (latencia y rendimiento)')
    print(f"Máquina: {platform.platform()}, CPU lógicos: {os.cpu_count()}")
    print(f"Configuración: {OPS} ops por hilo y repetición, {REPETICIONES} repeticiones "
          f"alternadas, hilos {HILOS}; 3 nodos en la misma máquina (Docker local)")

    cliente = MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000, retryWrites=False)
    db = cliente["bench_wc"]
    cliente.drop_database("bench_wc")
    estado = cliente.admin.command("replSetGetStatus")
    print("Conjunto: " + ", ".join(f"{m['name']}={m['stateStr']}" for m in estado["members"]))

    # Calentamiento: conexiones, caché y oplog.
    for nombre in CONCERNS:
        db.get_collection("ops", write_concern=CONCERNS[nombre]).insert_many(
            [{"_id": str(uuid4())} for _ in range(200)])

    acumulado = {(w, h): {"lat": {"insert_one": [], "find_one_and_update": []}, "seg": 0.0, "ops": 0}
                 for w in CONCERNS for h in HILOS}
    for rep in range(REPETICIONES):
        orden = list(CONCERNS) if rep % 2 == 0 else list(reversed(CONCERNS))
        for h in HILOS:
            for w in orden:
                r = correr(db, w, h)
                a = acumulado[(w, h)]
                for k in a["lat"]:
                    a["lat"][k] += r["lat"][k]
                a["seg"] += r["segundos"]
                a["ops"] += r["ops"]
                print(f"  rep {rep + 1} hilos={h} {w}: {r['ops'] / r['segundos']:.0f} ops/s")

    print("\nLatencia por operación (ms) y rendimiento")
    print(f"{'hilos':>5} {'write concern':<13} {'operación':<20} {'n':>6} {'p50':>7} {'p95':>7} "
          f"{'p99':>7} {'media':>7} {'ops/s':>8}")
    resumen = {}
    for h in HILOS:
        for w in CONCERNS:
            a = acumulado[(w, h)]
            rendimiento = a["ops"] / a["seg"]
            for op, v in a["lat"].items():
                resumen[(h, w, op)] = statistics.median(v)
                print(f"{h:>5} {w:<13} {op:<20} {len(v):>6} {percentil(v, 50):>7.2f} "
                      f"{percentil(v, 95):>7.2f} {percentil(v, 99):>7.2f} {statistics.mean(v):>7.2f} "
                      f"{rendimiento:>8.0f}")

    print("\nRelación majority / w:1 (mediana de latencia)")
    for h in HILOS:
        for op in ("insert_one", "find_one_and_update"):
            r = resumen[(h, "w:majority", op)] / resumen[(h, "w:1", op)]
            print(f"  hilos={h} {op}: x{r:.2f}")

    cliente.drop_database("bench_wc")
    print(f"\nFin UTC: {datetime.now(timezone.utc).isoformat()}")


if __name__ == "__main__":
    main()
