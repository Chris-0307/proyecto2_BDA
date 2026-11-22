"""Prueba del registro: flujo de transiciones, 409 y atomicidad concurrente.

Solo biblioteca estándar. Requiere haber ejecutado antes dataset/cargar_demo.py
(crea el cliente y los anotadores/revisores que usa). Salida con fecha UTC;
guárdela con tee en evidence/.

NO sustituye la prueba P2 del enunciado (50 solicitudes contra 10 tareas por
el gateway): comprueba la atomicidad a nivel de registro, con 50 intentos
simultáneos de asignar UNA misma tarea.
"""

import json
import os
import platform
import sys
import threading
import urllib.error
import urllib.request
from datetime import datetime, timezone
from uuid import NAMESPACE_DNS, uuid4, uuid5

BASE = os.getenv("REGISTRO_URL", "http://registro:8003")
HILOS = 50


def uid(nombre: str) -> str:
    return str(uuid5(NAMESPACE_DNS, nombre))


CLIENTE = uid("demo-cliente")
ANOTADORES = [uid(f"demo-anotador-{i}") for i in range(1, 5)]
REVISOR = uid("demo-revisor-1")

resultados: list[tuple[bool, str]] = []


def llamar(metodo: str, ruta: str, cuerpo=None):
    datos = json.dumps(cuerpo).encode() if cuerpo is not None else None
    req = urllib.request.Request(
        BASE + ruta, data=datos, method=metodo,
        headers={"Content-Type": "application/json"},
    )
    try:
        with urllib.request.urlopen(req, timeout=30) as r:
            return r.status, json.loads(r.read() or b"null")
    except urllib.error.HTTPError as e:
        crudo = e.read()
        try:
            return e.code, json.loads(crudo)
        except ValueError:
            return e.code, crudo.decode(errors="replace")


def comprobar(nombre: str, condicion: bool, detalle: str = "") -> None:
    resultados.append((bool(condicion), nombre))
    print(f"[{'OK' if condicion else 'FALLO'}] {nombre}" + (f" — {detalle}" if detalle else ""))


def transicion(tarea: str, **cuerpo):
    return llamar("POST", f"/tareas/{tarea}/transicion", cuerpo)


def main() -> int:
    print(f"Inicio UTC: {datetime.now(timezone.utc).isoformat()}")
    print(f"Objetivo: flujo del registro y atomicidad con {HILOS} intentos simultáneos")
    print(f"Máquina: {platform.platform()}, CPU lógicos: {os.cpu_count()}")

    codigo, salud = llamar("GET", "/salud")
    comprobar("salud operativa", codigo == 200 and salud["estado"] == "operativo", str(salud.get("estado")))

    # --- Datos de la prueba -------------------------------------------------
    codigo, proyecto = llamar("POST", "/proyectos", {
        "cliente_id": CLIENTE, "nombre": "Smoke registro", "sla_horas": 48,
        "taxonomia": [{"etiqueta": "lidar#peaton", "instruccion": "Persona a pie."}],
    })
    comprobar("crear proyecto responde 201", codigo == 201, f"código {codigo}")
    pid = proyecto["_id"]

    tareas = [{
        "modalidad": "lidar", "tipo_tarea": "cuboide_3d", "prioridad": i % 3,
        "archivo": {"uri": f"s3://smoke/{pid}/{i}", "sha256": "a" * 64, "bytes": 1000,
                    "mime": "application/octet-stream", "meta": {"minimo": 0, "maximo": 1}},
    } for i in range(12)]
    codigo, lote = llamar("POST", f"/proyectos/{pid}/lotes", {"nombre": "Lote smoke", "tareas": tareas})
    comprobar("registrar lote de 12 tareas responde 201", codigo == 201 and lote["tareas_creadas"] == 12)

    codigo, malo = llamar("POST", f"/proyectos/{pid}/lotes", {"nombre": "x", "tareas": [
        {**tareas[0], "tipo_tarea": "bbox"}]})
    comprobar("geometría incompatible con la modalidad responde 422", codigo == 422)

    codigo, cand = llamar("GET", f"/tareas?proyecto_id={pid}&candidatas=true&limite=100")
    comprobar("las 12 tareas son candidatas", codigo == 200 and len(cand) == 12, f"{len(cand)}")
    comprobar("candidatas ordenadas por prioridad descendente",
              [t["prioridad"] for t in cand] == sorted((t["prioridad"] for t in cand), reverse=True))

    # --- Atomicidad: 50 intentos simultáneos sobre UNA tarea ------------------
    tarea = lote["tarea_ids"][0]
    barrera = threading.Barrier(HILOS)
    codigos: list[int] = []
    ganadores: list[dict] = []
    cerrojo = threading.Lock()

    def intentar(i: int) -> None:
        barrera.wait()
        token = str(uuid4())
        c, cuerpo = transicion(tarea, accion="asignar",
                               anotador_id=ANOTADORES[i % len(ANOTADORES)], token=token)
        with cerrojo:
            codigos.append(c)
            if c == 200:
                ganadores.append({"token": token, "anotador": ANOTADORES[i % len(ANOTADORES)],
                                  "cuerpo": cuerpo})

    hilos = [threading.Thread(target=intentar, args=(i,)) for i in range(HILOS)]
    [h.start() for h in hilos]
    [h.join() for h in hilos]
    comprobar(f"{HILOS} asignaciones simultáneas: exactamente 1 gana (200)",
              codigos.count(200) == 1, f"200={codigos.count(200)}")
    comprobar("las demás responden 409", codigos.count(409) == HILOS - 1,
              f"409={codigos.count(409)}, otros={[c for c in codigos if c not in (200, 409)]}")
    g = ganadores[0]
    comprobar("la tarea quedó abierta con versión 2 y el token del ganador",
              g["cuerpo"]["tarea"]["estado"] == "abierta"
              and g["cuerpo"]["tarea"]["version"] == 2
              and g["cuerpo"]["tarea"]["asignacion"]["token"] == g["token"])
    comprobar("la respuesta incluye el evento tarea_abierta con estado abierta",
              g["cuerpo"]["evento"]["tipo_evento"] == "tarea_abierta"
              and g["cuerpo"]["evento"]["estado"] == "abierta"
              and g["cuerpo"]["evento"]["version"] == 2)

    # --- Envío, 409 y revisión ------------------------------------------------
    anotador, token = g["anotador"], g["token"]
    c, _ = transicion(tarea, accion="enviar", anotador_id=anotador, token=str(uuid4()), contenido={"x": 1})
    comprobar("enviar con token ajeno responde 409", c == 409)
    c, r = transicion(tarea, accion="enviar", anotador_id=anotador, token=token,
                      contenido={"cuboides": [{"clase": "lidar#peaton"}]})
    comprobar("enviar con el token correcto responde 200 y estado anotada",
              c == 200 and r["tarea"]["estado"] == "anotada" and r["tarea"]["version"] == 3)
    comprobar("el evento tarea_anotada trae duracion_ms",
              r["evento"]["tipo_evento"] == "tarea_anotada"
              and isinstance(r["evento"].get("duracion_ms"), int))
    c, _ = transicion(tarea, accion="enviar", anotador_id=anotador, token=token, contenido={"x": 2})
    comprobar("segundo envío de una tarea ya enviada responde 409", c == 409)

    c, _ = transicion(tarea, accion="iniciar_revision", revisor_id=ANOTADORES[0])
    comprobar("iniciar revisión con alguien que no es revisor responde 403", c == 403)
    c, r = transicion(tarea, accion="iniciar_revision", revisor_id=REVISOR)
    comprobar("iniciar revisión responde 200 y estado en_revision",
              c == 200 and r["tarea"]["estado"] == "en_revision")
    c, _ = transicion(tarea, accion="rechazar", revisor_id=REVISOR)
    comprobar("rechazar sin código de motivo responde 422", c == 422)
    c, r = transicion(tarea, accion="rechazar", revisor_id=REVISOR, motivo="etiqueta_incorrecta")
    comprobar("rechazar: en MongoDB la tarea vuelve a disponible",
              c == 200 and r["tarea"]["estado"] == "disponible" and r["tarea"]["asignacion"] is None)
    comprobar("rechazar: la fila de historial queda como rechazada con su motivo",
              r["evento"]["estado"] == "rechazada" and r["evento"]["motivo_rechazo"] == "etiqueta_incorrecta")

    codigo, cand = llamar("GET", f"/tareas?proyecto_id={pid}&candidatas=true&limite=100")
    comprobar("la tarea rechazada vuelve a ofrecerse como candidata",
              codigo == 200 and any(t["_id"] == tarea for t in cand))

    # --- Retrabajo y aprobación -----------------------------------------------
    token2 = str(uuid4())
    c, _ = transicion(tarea, accion="asignar", anotador_id=ANOTADORES[1], token=token2)
    comprobar("reasignar tras el rechazo responde 200", c == 200)
    c, _ = transicion(tarea, accion="enviar", anotador_id=ANOTADORES[1], token=token2, contenido={"v": 2})
    comprobar("reenviar el retrabajo responde 200", c == 200)
    transicion(tarea, accion="iniciar_revision", revisor_id=REVISOR)
    c, r = transicion(tarea, accion="aprobar", revisor_id=REVISOR)
    comprobar("aprobar responde 200 y estado aprobada",
              c == 200 and r["tarea"]["estado"] == "aprobada" and r["evento"]["estado"] == "aprobada")
    c, _ = transicion(tarea, accion="aprobar", revisor_id=REVISOR)
    comprobar("aprobar dos veces responde 409", c == 409)

    # --- Exportación -----------------------------------------------------------
    codigo, pag = llamar("GET", f"/proyectos/{pid}/exportar?limite=1")
    comprobar("exportar devuelve la tarea aprobada",
              codigo == 200 and [t["_id"] for t in pag["items"]] == [tarea])

    # --- Eventos pendientes -----------------------------------------------------
    codigo, doc = llamar("GET", f"/tareas?anotador_id={ANOTADORES[1]}&estado=aprobada&limite=50")
    mia = next((t for t in doc if t["_id"] == tarea), None)
    codigo, pend = llamar("GET", "/tareas?eventos_pendientes=true&limite=200")
    propia = next((t for t in pend if t["_id"] == tarea), None)
    n = len(propia["eventos_pendientes"]) if propia else 0
    comprobar("la tarea acumula 8 eventos pendientes (todas las transiciones con evento)", n == 8, f"{n}")
    ids = [e["event_id"] for e in (propia or {"eventos_pendientes": []})["eventos_pendientes"]]
    comprobar("los event_id son distintos", len(set(ids)) == len(ids))
    comprobar("ultimo_evento_id es el del último evento",
              propia is not None and propia["ultimo_evento_id"] == ids[-1])
    for ev in ids:
        transicion(tarea, accion="confirmar_evento", event_id=ev)
    c, r = transicion(tarea, accion="confirmar_evento", event_id=ids[0])
    comprobar("confirmar un evento ya retirado es idempotente (200)", c == 200)
    codigo, pend = llamar("GET", "/tareas?eventos_pendientes=true&limite=200")
    comprobar("tras confirmar, la tarea ya no tiene eventos pendientes",
              codigo == 200 and all(t["_id"] != tarea for t in pend))

    fallos = [n for ok, n in resultados if not ok]
    print(f"\nResultado: {len(resultados) - len(fallos)}/{len(resultados)} comprobaciones correctas")
    for f in fallos:
        print(f"  FALLO: {f}")
    print(f"Fin UTC: {datetime.now(timezone.utc).isoformat()}")
    return 1 if fallos else 0


if __name__ == "__main__":
    sys.exit(main())