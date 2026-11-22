"""P8 — fallas de MongoDB a través del gateway.

Tres escenarios, en orden, restaurando el entorno al final de cada uno:
  A. Se detiene un SECUNDARIO: /salud degradado y se sigue escribiendo.
  B. Se detiene el PRIMARIO: se elige otro; lo que respondió 200 sigue en
     MongoDB una sola vez. Se mide la elección y el tiempo sin escrituras.
  C. Se detienen los DOS SECUNDARIOS (sin mayoría): /salud caído y ninguna
     escritura responde 200. Tras restaurar se reintenta un envío pendiente.

Por defecto la caída es real: se desactiva la política de reinicio del
contenedor (`docker update --restart=no`) y se le envía SIGKILL, sin apagado
ordenado ni stepDown. Al restaurar se arranca y se devuelve la política
`unless-stopped`. Con P8_PARADA=stop se usa `docker compose stop` (apagado
ordenado: el primario cede el rol antes de morir y la elección es casi inmediata).

Se ejecuta en el HOST (WSL/Linux) con Python estándar y Docker, con el Compose
final levantado. Los datos de prueba (cliente y anotadores nuevos) se insertan
con mongosh; todas las operaciones del flujo van al gateway (localhost:8000).

  python3 proyecto2/pruebas/p8_fallas/p8_mongo.py | tee proyecto2/evidence/p8_mongo_fallas.txt
"""

import json
import os
import platform
import subprocess
import sys
import threading
import time
import urllib.error
import urllib.request
from datetime import datetime, timezone
from pathlib import Path
from uuid import uuid4

PROYECTO = Path(__file__).resolve().parents[2]
COMPOSE = ["docker", "compose", "-f", str(PROYECTO / "docker-compose.yml")]
GATEWAY = os.getenv("GATEWAY_URL", "http://localhost:8000")
MODO = os.getenv("P8_PARADA", "kill")  # kill (caída real) | stop (apagado ordenado)
NODOS = ["mongo-1", "mongo-2", "mongo-3"]
URI_RS = ("mongodb://mongo-1:27017,mongo-2:27017,mongo-3:27017/plataforma"
          "?replicaSet=rs-plataforma&w=majority&readPreference=primaryPreferred")

T0 = time.monotonic()
resultados: list[bool] = []
confirmadas: dict[str, dict] = {}     # tarea_id -> estado esperado tras un 200
indeterminadas: list[dict] = []       # operaciones que respondieron 503


# ---------------------------------------------------------------- utilidades

def t() -> str:
    return f"{time.monotonic() - T0:7.1f}s"


def log(msg: str) -> None:
    print(f"[{t()}] {msg}", flush=True)


def comprobar(nombre, condicion, detalle=""):
    resultados.append(bool(condicion))
    print(f"[{'OK' if condicion else 'FALLO'}] {nombre}" + (f" — {detalle}" if detalle else ""),
          flush=True)


def sh(args, timeout=60):
    p = subprocess.run(args, capture_output=True, text=True, timeout=timeout)
    return p.returncode, p.stdout.strip(), p.stderr.strip()


def compose(*args, timeout=120):
    return sh(COMPOSE + list(args), timeout=timeout)


def mongosh(js: str, uri: str | None = None, nodos=NODOS) -> str | None:
    """Evalúa JS en el primer nodo que responda. None si ninguno responde."""
    for n in nodos:
        destino = [uri] if uri else []
        rc, out, _ = compose("exec", "-T", n, "mongosh", *destino, "--quiet", "--eval", js,
                             timeout=40)
        if rc == 0 and out:
            return out.splitlines()[-1]
    return None


def estado_rs() -> dict[str, str]:
    """{servicio: PRIMARY|SECONDARY|...} de los miembros sanos, visto por un nodo vivo."""
    out = mongosh("JSON.stringify(rs.status().members.map(m=>[m.name,m.stateStr,m.health]))")
    if not out:
        return {}
    try:
        return {n.split(":")[0]: s for n, s, h in json.loads(out) if h == 1}
    except ValueError:
        return {}


def contenedor(nodo: str) -> str:
    return compose("ps", "-q", "-a", nodo)[1]


def caer(*nodos: str) -> None:
    """Caída del nodo: SIGKILL sin reinicio automático (o stop ordenado)."""
    if MODO == "stop":
        compose("stop", *nodos)
        return
    for n in nodos:
        cid = contenedor(n)
        sh(["docker", "update", "--restart=no", cid])
        sh(["docker", "kill", "-s", "KILL", cid])


def levantar(*nodos: str) -> None:
    """Arranca los nodos y les devuelve la política restart: unless-stopped."""
    compose("start", *nodos)
    for n in nodos:
        sh(["docker", "update", "--restart=unless-stopped", contenedor(n)])


def primario() -> str | None:
    return next((n for n, s in estado_rs().items() if s == "PRIMARY"), None)


def http(metodo, ruta, cuerpo=None, timeout=30):
    datos = json.dumps(cuerpo).encode() if cuerpo is not None else None
    req = urllib.request.Request(GATEWAY + ruta, data=datos, method=metodo,
                                 headers={"Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(req, timeout=timeout) as r:
            crudo = r.read()
            return r.status, json.loads(crudo) if crudo else None
    except urllib.error.HTTPError as e:
        crudo = e.read()
        try:
            return e.code, json.loads(crudo)
        except ValueError:
            return e.code, crudo.decode(errors="replace")
    except (urllib.error.URLError, TimeoutError, ConnectionError) as e:
        return 0, str(e)


def salud() -> tuple[int, str, str]:
    c, b = http("GET", "/salud", timeout=10)
    if not isinstance(b, dict):
        return c, "?", "?"
    return c, b.get("estado", "?"), b.get("capas", {}).get("registro", {}).get("estado", "?")


def esperar(cond, limite_s, paso=1.0):
    fin = time.monotonic() + limite_s
    while time.monotonic() < fin:
        if cond():
            return True
        time.sleep(paso)
    return False


def esperar_sano(limite_s=180) -> float | None:
    """Espera 1 PRIMARY + 2 SECONDARY y /salud operativo. Devuelve segundos o None."""
    inicio = time.monotonic()

    def listo():
        roles = sorted(estado_rs().values())
        return roles == ["PRIMARY", "SECONDARY", "SECONDARY"] and salud()[1] == "operativo"

    return time.monotonic() - inicio if esperar(listo, limite_s, 2) else None


# ---------------------------------------------------------------- carga de trabajo

class Datos:
    def __init__(self, n_anotadores=300, n_tareas=300):
        self.cliente = str(uuid4())
        self.anotadores = [str(uuid4()) for _ in range(n_anotadores)]
        self.siguiente_anotador = 0
        equipo = str(uuid4())
        docs = [{"_id": a, "nombre": f"p8-{i}", "equipo_id": equipo, "es_revisor": False}
                for i, a in enumerate(self.anotadores)]
        js = (f"const d=db.getSiblingDB('plataforma');"
              f"d.clientes.insertOne({{_id:'{self.cliente}',nombre:'p8',creado_en:new Date()}});"
              f"d.anotadores.insertMany({json.dumps(docs)});print('ok')")
        if mongosh(js, URI_RS) != "ok":
            sys.exit("No se pudieron insertar los datos de prueba")
        _, p = http("POST", "/proyectos", {
            "cliente_id": self.cliente, "nombre": "p8-fallas",
            "taxonomia": [{"etiqueta": "auto", "instruccion": "Vehículo."}],
        })
        self.proyecto = p["_id"]
        tareas = [{"modalidad": "texto", "tipo_tarea": "span", "prioridad": 1,
                   "archivo": {"uri": f"s3://p8/{uuid4()}", "sha256": "d" * 64, "bytes": 1,
                               "mime": "text/plain"}} for _ in range(n_tareas)]
        c, _ = http("POST", f"/proyectos/{self.proyecto}/lotes", {"nombre": "p8", "tareas": tareas})
        if c != 201:
            sys.exit(f"No se pudo crear el lote ({c})")

    def anotador(self) -> str:
        a = self.anotadores[self.siguiente_anotador]
        self.siguiente_anotador += 1
        return a


def ciclo(d: Datos, etiqueta: str) -> tuple[bool, dict]:
    """siguiente + enviar con un anotador nuevo. True si ambas respondieron 200."""
    a = d.anotador()
    c1, s = http("POST", "/tareas/siguiente", {"anotador_id": a, "proyecto_id": d.proyecto})
    if c1 != 200:
        if c1 == 503:
            indeterminadas.append({"escenario": etiqueta, "op": "siguiente", "anotador": a})
        return False, {"siguiente": c1}
    tid = s["tarea"]["_id"]
    confirmadas[tid] = {"estado": "abierta", "anotador": a, "token": s["token"]}
    c2, _ = http("POST", f"/tareas/{tid}/enviar",
                 {"anotador_id": a, "proyecto_id": d.proyecto, "token": s["token"],
                  "contenido": {"span": [0, 4]}})
    if c2 == 200:
        confirmadas[tid]["estado"] = "anotada"
    elif c2 == 503:
        indeterminadas.append({"escenario": etiqueta, "op": "enviar", "tarea": tid})
    return c2 == 200, {"siguiente": c1, "enviar": c2}


def verificar_confirmadas() -> tuple[int, list]:
    """Cada tarea que respondió 200 sigue en MongoDB, una vez, con su dueño y estado."""
    ids = list(confirmadas)
    out = mongosh(
        "const d=db.getSiblingDB('plataforma');"
        f"print(JSON.stringify(d.tareas.find({{_id:{{$in:{json.dumps(ids)}}}}},"
        "{estado:1,'asignacion.anotador_id':1,'asignacion.token':1}).toArray()))",
        URI_RS,
    )
    docs = {x["_id"]: x for x in json.loads(out or "[]")}
    malas = []
    for tid, esp in confirmadas.items():
        doc = docs.get(tid)
        asig = (doc or {}).get("asignacion") or {}
        if (doc is None or doc["estado"] != esp["estado"]
                or asig.get("anotador_id") != esp["anotador"] or asig.get("token") != esp["token"]):
            malas.append({"tarea": tid, "esperado": esp["estado"],
                          "encontrado": doc and doc["estado"]})
    return len(ids), malas


# ---------------------------------------------------------------- escenarios

def escenario_a(d: Datos):
    print("\n=== A. Se detiene un secundario ===", flush=True)
    sec = next(n for n, s in estado_rs().items() if s == "SECONDARY")
    log(f"caída ({MODO}) de {sec}")
    inicio = time.monotonic()
    caer(sec)
    detectado = esperar(lambda: salud()[1] == "degradado", 60, 1)
    comprobar("/salud pasa a degradado (200) con un nodo caído",
              detectado and salud()[0] == 200,
              f"{time.monotonic() - inicio:.1f}s hasta detectarlo" if detectado else "no se detectó")
    ok = [ciclo(d, "A")[0] for _ in range(10)]
    comprobar("con un secundario caído se sigue escribiendo (10/10 siguiente+enviar)",
              all(ok), f"{sum(ok)}/10")
    log(f"arranque de {sec}")
    levantar(sec)
    rec = esperar_sano()
    comprobar("el nodo vuelve como SECONDARY y /salud operativo", rec is not None,
              f"{rec:.1f}s" if rec is not None else "sin recuperar")


def escenario_b(d: Datos):
    print("\n=== B. Se detiene el primario ===", flush=True)
    for _ in range(5):
        ciclo(d, "B-antes")
    viejo = primario()
    log(f"primario actual: {viejo}; caída ({MODO}) de {viejo}")
    nuevo: dict = {}
    restantes = [n for n in NODOS if n != viejo]

    def vigilar_eleccion():
        while not nuevo and time.monotonic() - parada < 120:
            out = mongosh("JSON.stringify(rs.status().members.filter(m=>m.stateStr==='PRIMARY')"
                          ".map(m=>m.name))", nodos=restantes)
            try:
                p = json.loads(out or "[]")
            except ValueError:
                p = []
            if p and not p[0].startswith(viejo):
                nuevo["nombre"], nuevo["t"] = p[0].split(":")[0], time.monotonic() - parada
            time.sleep(0.5)

    parada = time.monotonic()
    caer(viejo)
    hilo = threading.Thread(target=vigilar_eleccion, daemon=True)
    hilo.start()

    primer_fallo = primer_exito_tras_fallo = None
    seguidos, n = 0, 0
    while time.monotonic() - parada < 120:
        ok, cod = ciclo(d, "B")
        n += 1
        ahora = time.monotonic() - parada
        log(f"ciclo {n}: {cod}")
        if not ok and primer_fallo is None:
            primer_fallo = ahora
        if ok and primer_fallo is not None and primer_exito_tras_fallo is None:
            primer_exito_tras_fallo = ahora
        seguidos = seguidos + 1 if ok else 0
        if not ok:
            time.sleep(0.5)  # resolución de ~0,5 s sin agotar los anotadores de prueba
        if seguidos >= 5 and (primer_fallo is None or primer_exito_tras_fallo is not None):
            break
    hilo.join(timeout=5)

    comprobar("se elige otro primario", bool(nuevo),
              f"{nuevo.get('nombre')} en {nuevo.get('t', 0):.1f}s" if nuevo else "sin primario")
    if primer_fallo is None:
        comprobar("escrituras disponibles tras la elección", seguidos >= 5, "no hubo fallos visibles")
    else:
        rto = primer_exito_tras_fallo
        comprobar("las escrituras vuelven a responder 200 tras la elección", rto is not None,
                  f"primer fallo {primer_fallo:.1f}s, RTO (parada -> primer 200) "
                  f"{rto:.1f}s" if rto is not None else "sin recuperar")
    total, malas = verificar_confirmadas()
    comprobar("todo lo que respondió 200 sigue en MongoDB una vez, con su dueño y estado",
              not malas, f"{total} tareas verificadas, {len(malas)} distintas")
    log(f"arranque de {viejo}")
    levantar(viejo)
    rec = esperar_sano()
    roles = estado_rs()
    comprobar("el antiguo primario vuelve como SECONDARY", rec is not None and roles.get(viejo) == "SECONDARY",
              f"{viejo}={roles.get(viejo)} en {rec:.1f}s" if rec is not None else "sin recuperar")


def escenario_c(d: Datos):
    print("\n=== C. Sin mayoría: se detienen los dos secundarios ===", flush=True)
    # Tarea tomada ANTES de perder la mayoría, para reenviarla después.
    a = d.anotador()
    c, s = http("POST", "/tareas/siguiente", {"anotador_id": a, "proyecto_id": d.proyecto})
    if c != 200:
        comprobar("tomar una tarea antes de perder la mayoría", False, f"código {c}")
        return
    pendiente = {"tarea": s["tarea"]["_id"], "anotador_id": a, "token": s["token"]}
    confirmadas[pendiente["tarea"]] = {"estado": "abierta", "anotador": a, "token": s["token"]}

    secs = [n for n, st in estado_rs().items() if st == "SECONDARY"]
    log(f"caída ({MODO}) de {' '.join(secs)}")
    parada = time.monotonic()
    caer(*secs)

    codigos_envio = []
    fin = time.monotonic() + 25
    while time.monotonic() < fin:
        c, _ = http("POST", f"/tareas/{pendiente['tarea']}/enviar",
                    {"anotador_id": a, "proyecto_id": d.proyecto, "token": pendiente["token"],
                     "contenido": {"span": [1, 2]}})
        codigos_envio.append(c)
        log(f"enviar sin mayoría -> {c}")
        if c == 200:
            confirmadas[pendiente["tarea"]]["estado"] = "anotada"
            break
        if c == 409:
            break
    otros = [ciclo(d, "C")[1] for _ in range(3)]
    log(f"ciclos sin mayoría: {otros}")

    caido = esperar(lambda: salud()[:2] == (503, "caido"), 30, 1)
    comprobar("/salud responde 503 caído sin mayoría", caido, str(salud()))
    comprobar("sin mayoría ninguna escritura responde 200",
              200 not in codigos_envio and all(o.get("enviar") != 200 and o.get("siguiente") != 200
                                               for o in otros),
              f"enviar: {sorted(set(codigos_envio))}")
    comprobar("sin mayoría enviar responde 503 (sin reintento)", 503 in codigos_envio,
              f"{codigos_envio.count(503)} respuestas 503")

    log(f"arranque de {' '.join(secs)}")
    arranque = time.monotonic()
    levantar(*secs)
    rec = esperar_sano()
    comprobar("el conjunto se recupera con 1 PRIMARY + 2 SECONDARY", rec is not None,
              f"{rec:.1f}s desde el arranque" if rec is not None else "sin recuperar")

    # Reintento del envío que falló: el borrador y el lock siguen en Valkey.
    c, _ = http("POST", f"/tareas/{pendiente['tarea']}/enviar",
                {"anotador_id": a, "proyecto_id": d.proyecto, "token": pendiente["token"],
                 "contenido": {"span": [1, 2]}})
    if c == 200:
        confirmadas[pendiente["tarea"]]["estado"] = "anotada"
        nota = "200: el envío fallido no se había aplicado y se reenvió"
    elif c == 409:
        confirmadas[pendiente["tarea"]]["estado"] = "anotada"
        nota = "409: el 503 sí se había aplicado en el primario (ver limitaciones de w:majority)"
    else:
        nota = f"código {c}"
    comprobar("tras restaurar, el envío pendiente se resuelve (200 o 409)", c in (200, 409), nota)
    log(f"recuperación (arranque -> sano): {time.monotonic() - arranque:.1f}s; "
        f"sin mayoría durante {arranque - parada:.1f}s")


# ---------------------------------------------------------------- principal

def main():
    print(f"Inicio UTC: {datetime.now(timezone.utc).isoformat()}")
    print("Objetivo: P8 — fallas de MongoDB (secundario, primario, sin mayoría) vía gateway")
    print(f"Máquina: {platform.platform()}, CPU lógicos: {os.cpu_count()}")
    print(f"Caída: {'SIGKILL sin reinicio automático' if MODO != 'stop' else 'docker compose stop (ordenada)'}; "
          f"gateway: {GATEWAY}")
    print("Configuración: rs-plataforma 3 nodos, w=majority, wtimeout 5 s, "
          "serverSelectionTimeout 3 s, retryWrites=false, electionTimeoutMillis 10000")

    rec = esperar_sano(60)
    comprobar("estado inicial: 1 PRIMARY + 2 SECONDARY y /salud operativo", rec is not None,
              json.dumps(estado_rs()))
    if rec is None:
        sys.exit(1)
    d = Datos()
    for _ in range(5):
        ciclo(d, "base")

    try:
        escenario_a(d)
        escenario_b(d)
        escenario_c(d)
    finally:
        # Restaurar siempre un estado conocido.
        levantar(*NODOS)
        esperar_sano()

    print("\n=== Verificación final ===", flush=True)
    total, malas = verificar_confirmadas()
    comprobar("RPO de escrituras confirmadas: ninguna tarea con 200 se perdió",
              not malas, f"{total} tareas verificadas; diferencias: {malas[:5]}")
    print(f"Operaciones con 503 (resultado indeterminado del lado del cliente): {len(indeterminadas)}")
    for x in indeterminadas:
        print(f"  {x}")
    print(f"Estado final del conjunto: {json.dumps(estado_rs())}; /salud: {salud()}")

    ok = sum(resultados)
    print(f"\nResultado: {ok}/{len(resultados)} comprobaciones correctas")
    print(f"Fin UTC: {datetime.now(timezone.utc).isoformat()}")
    sys.exit(0 if ok == len(resultados) else 1)


if __name__ == "__main__":
    main()
