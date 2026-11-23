"""P2 por el gateway: 50 solicitudes simultáneas de /tareas/siguiente contra 10 tareas.

Esperado: exactamente 10 respuestas 200 con tareas distintas, 40 respuestas
204 y cero dobles asignaciones en MongoDB. Reporta latencias p50/p95/p99.

Se ejecuta en el contenedor del registro solo para crear los datos de prueba
(cliente y 50 anotadores nuevos) y para verificar MongoDB en modo lectura.
Las 50 solicitudes van al gateway.

  docker compose -f proyecto2/docker-compose.yml run --rm registro \
    python /pruebas/p2_registro/p2_concurrencia_gateway.py | tee proyecto2/evidence/p2_concurrencia_gateway.txt
"""

import json
import os
import platform
import statistics
import sys
import threading
import time
import urllib.error
import urllib.request
from collections import Counter
from datetime import datetime, timezone
from uuid import uuid4

from pymongo import MongoClient

GATEWAY = os.getenv("GATEWAY_URL", "http://gateway:8000")
MONGO_URI = os.getenv("MONGO_URI")
SOLICITUDES = int(os.getenv("P2_SOLICITUDES", "50"))
TAREAS = int(os.getenv("P2_TAREAS", "10"))
resultados: list[bool] = []


def llamar(metodo, ruta, cuerpo=None):
    datos = json.dumps(cuerpo).encode() if cuerpo is not None else None
    req = urllib.request.Request(GATEWAY + ruta, data=datos, method=metodo,
                                 headers={"Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(req, timeout=60) as r:
            crudo = r.read()
            return r.status, json.loads(crudo) if crudo else None
    except urllib.error.HTTPError as e:
        crudo = e.read()
        try:
            return e.code, json.loads(crudo)
        except ValueError:
            return e.code, crudo.decode(errors="replace")


def comprobar(nombre, condicion, detalle=""):
    resultados.append(bool(condicion))
    print(f"[{'OK' if condicion else 'FALLO'}] {nombre}" + (f" — {detalle}" if detalle else ""))


def percentil(valores, p):
    orden = sorted(valores)
    k = max(0, min(len(orden) - 1, round(p / 100 * (len(orden) - 1))))
    return orden[k]


def main():
    print(f"Inicio UTC: {datetime.now(timezone.utc).isoformat()}")
    print(f"Objetivo: P2, {SOLICITUDES} solicitudes simultáneas contra {TAREAS} tareas por el gateway")
    print(f"Máquina: {platform.platform()}, CPU lógicos: {os.cpu_count()}")

    db = MongoClient(MONGO_URI, w="majority", serverSelectionTimeoutMS=5000)["plataforma"]
    cliente, equipo = str(uuid4()), str(uuid4())
    db.clientes.insert_one({"_id": cliente, "nombre": "p2-concurrencia",
                            "creado_en": datetime.now(timezone.utc)})
    anotadores = [str(uuid4()) for _ in range(SOLICITUDES)]
    db.anotadores.insert_many([
        {"_id": a, "nombre": f"p2-{i}", "equipo_id": equipo, "es_revisor": False}
        for i, a in enumerate(anotadores)
    ])

    _, proyecto = llamar("POST", "/proyectos", {
        "cliente_id": cliente, "nombre": "p2-concurrencia",
        "taxonomia": [{"etiqueta": "auto", "instruccion": "Vehículo."}],
    })
    pid = proyecto["_id"]
    tareas = [{"modalidad": "texto", "tipo_tarea": "span", "prioridad": i % 3,
               "archivo": {"uri": f"s3://p2/{uuid4()}", "sha256": "c" * 64, "bytes": 1,
                           "mime": "text/plain"}} for i in range(TAREAS)]
    c, lote = llamar("POST", f"/proyectos/{pid}/lotes", {"nombre": "p2", "tareas": tareas})
    comprobar(f"lote de {TAREAS} tareas creado", c == 201, f"código {c}")

    barrera = threading.Barrier(SOLICITUDES)
    salida: list[tuple[int, object, float]] = [None] * SOLICITUDES

    def trabajador(i):
        barrera.wait()
        t0 = time.perf_counter()
        codigo, cuerpo = llamar("POST", "/tareas/siguiente",
                                {"anotador_id": anotadores[i], "proyecto_id": pid})
        salida[i] = (codigo, cuerpo, (time.perf_counter() - t0) * 1000)

    hilos = [threading.Thread(target=trabajador, args=(i,)) for i in range(SOLICITUDES)]
    inicio = time.perf_counter()
    for h in hilos:
        h.start()
    for h in hilos:
        h.join()
    total_ms = (time.perf_counter() - inicio) * 1000

    codigos = Counter(c for c, _, _ in salida)
    ganadores = [(i, cuerpo) for i, (c, cuerpo, _) in enumerate(salida) if c == 200]
    tareas_ganadas = [cuerpo["tarea"]["_id"] for _, cuerpo in ganadores]
    print(f"Códigos: {dict(codigos)}")

    comprobar(f"exactamente {TAREAS} respuestas 200", codigos[200] == TAREAS, str(codigos[200]))
    comprobar(f"{SOLICITUDES - TAREAS} respuestas 204", codigos[204] == SOLICITUDES - TAREAS,
              str(codigos[204]))
    comprobar("ningún otro código", set(codigos) <= {200, 204}, str(dict(codigos)))
    comprobar("las tareas entregadas son todas distintas",
              len(set(tareas_ganadas)) == len(tareas_ganadas), f"{len(set(tareas_ganadas))} distintas")

    # Verificación en MongoDB (solo lectura): cero dobles asignaciones.
    abiertas = list(db.tareas.find({"proyecto_id": pid, "estado": "abierta"},
                                   {"asignacion.anotador_id": 1, "version": 1}))
    duenos = [t["asignacion"]["anotador_id"] for t in abiertas]
    comprobar(f"MongoDB: {TAREAS} tareas abiertas", len(abiertas) == TAREAS, str(len(abiertas)))
    comprobar("MongoDB: cada tarea tiene un dueño distinto", len(set(duenos)) == len(duenos))
    comprobar("MongoDB: cada tarea se asignó una sola vez (versión 2)",
              all(t["version"] == 2 for t in abiertas), str(sorted({t["version"] for t in abiertas})))
    esperado = {i: anotadores[i] for i, _ in ganadores}
    comprobar("el dueño en MongoDB coincide con quien recibió el 200",
              all(db.tareas.find_one({"_id": cuerpo["tarea"]["_id"]})["asignacion"]["anotador_id"]
                  == esperado[i] for i, cuerpo in ganadores))

    lat = [ms for _, _, ms in salida]
    print(f"\nLatencia /tareas/siguiente (ms), n={len(lat)}: "
          f"p50={percentil(lat, 50):.1f} p95={percentil(lat, 95):.1f} p99={percentil(lat, 99):.1f} "
          f"media={statistics.mean(lat):.1f} máx={max(lat):.1f}")
    print(f"Tiempo total de la ráfaga: {total_ms:.1f} ms")

    # Limpieza: liberar por el gateway para no dejar locks ni sesiones abiertas.
    for i, cuerpo in ganadores:
        llamar("POST", f"/tareas/{cuerpo['tarea']['_id']}/liberar",
               {"anotador_id": anotadores[i], "proyecto_id": pid, "token": cuerpo["token"]})

    ok = sum(resultados)
    print(f"\nResultado: {ok}/{len(resultados)} comprobaciones correctas")
    print(f"Fin UTC: {datetime.now(timezone.utc).isoformat()}")
    sys.exit(0 if ok == len(resultados) else 1)


if __name__ == "__main__":
    main()
