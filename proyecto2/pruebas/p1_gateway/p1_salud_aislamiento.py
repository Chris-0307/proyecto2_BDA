"""P1 — /salud por capa, exposición de puertos y reglas de aislamiento.

1. /salud con cada servicio interno detenido (Sesión, Historial, Registro):
   el gateway sigue respondiendo y marca la capa caída. Se restaura después
   de cada caso.
2. Puertos: solo el gateway publica un puerto al host (8000). Se revisa la
   configuración de Docker y se intenta conectar desde el host a los puertos
   internos.
3. Aislamiento: ningún servicio tiene el cliente ni la configuración de una
   base que no le corresponde (imports del código, paquetes instalados en la
   imagen y variables de entorno).

Se ejecuta en el HOST (WSL/Linux) con Python estándar y Docker, con el Compose
final levantado:

  python3 proyecto2/pruebas/p1_gateway/p1_salud_aislamiento.py | tee proyecto2/evidence/p1_salud_aislamiento.txt
"""

import ast
import json
import os
import platform
import socket
import subprocess
import sys
import time
import urllib.error
import urllib.request
from datetime import datetime, timezone
from pathlib import Path
from uuid import uuid4

PROYECTO = Path(__file__).resolve().parents[2]
COMPOSE = ["docker", "compose", "-f", str(PROYECTO / "docker-compose.yml")]
GATEWAY = os.getenv("GATEWAY_URL", "http://localhost:8000")
resultados: list[bool] = []

# Puertos internos que NO deben responder desde el host.
INTERNOS = {"sesion": 8001, "historial": 8002, "registro": 8003,
            "mongo": 27017, "valkey": 6379}

# Servicio -> (carpeta del código, módulos prohibidos, variables prohibidas)
REGLAS = {
    "gateway": ("servicios/gateway/gateway_app", {"pymongo", "redis"}, {"MONGO_URI", "VALKEY_URL"}),
    "registro": ("servicios/registro/registro_app", {"redis"}, {"VALKEY_URL"}),
    "sesion": ("servicios/sesion/app", {"pymongo"}, {"MONGO_URI"}),
    "historial": ("servicios/historial/app", {"pymongo"}, {"MONGO_URI"}),
}


def comprobar(nombre, condicion, detalle=""):
    resultados.append(bool(condicion))
    print(f"[{'OK' if condicion else 'FALLO'}] {nombre}" + (f" — {detalle}" if detalle else ""),
          flush=True)


def compose(*args, timeout=120):
    p = subprocess.run(COMPOSE + list(args), capture_output=True, text=True, timeout=timeout)
    return p.returncode, p.stdout.strip()


def http(metodo, ruta, cuerpo=None, timeout=15):
    datos = json.dumps(cuerpo).encode() if cuerpo is not None else None
    req = urllib.request.Request(GATEWAY + ruta, data=datos, method=metodo,
                                 headers={"Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(req, timeout=timeout) as r:
            crudo = r.read()
            return r.status, json.loads(crudo) if crudo else None
    except urllib.error.HTTPError as e:
        try:
            return e.code, json.loads(e.read())
        except ValueError:
            return e.code, None
    except (urllib.error.URLError, TimeoutError, ConnectionError) as e:
        return 0, str(e)


def salud():
    c, b = http("GET", "/salud")
    if not isinstance(b, dict):
        return c, "?", {}
    return c, b.get("estado"), {k: v.get("estado") for k, v in b.get("capas", {}).items()}


def esperar(cond, limite_s=90):
    fin = time.monotonic() + limite_s
    while time.monotonic() < fin:
        if cond():
            return True
        time.sleep(1)
    return False


# ---------------------------------------------------------------- 1. salud por capa

def caso_salud(servicio: str, global_esperado: str, codigo_esperado: int):
    print(f"\n--- /salud con {servicio} detenido ---", flush=True)
    compose("stop", servicio)
    t0 = time.monotonic()
    ok = esperar(lambda: salud()[:2] == (codigo_esperado, global_esperado), 30)
    c, est, capas = salud()
    print(f"    /salud: {c} {est} {capas}")
    comprobar(f"con {servicio} caído, /salud responde {codigo_esperado} {global_esperado}",
              ok, f"{time.monotonic() - t0:.1f}s")
    comprobar(f"la capa {servicio} figura como caída y las demás no",
              capas.get(servicio) == "caido"
              and all(v != "caido" for k, v in capas.items() if k != servicio), str(capas))

    # El gateway sigue atendiendo lo que no depende del servicio caído.
    if servicio != "registro":
        c, _ = http("GET", f"/proyectos/{uuid4()}/exportar")
        comprobar("las rutas que solo usan el registro siguen respondiendo (exportar -> 404)",
                  c == 404, f"código {c}")
    if servicio == "sesion":
        c, _ = http("PUT", f"/tareas/{uuid4()}/borrador",
                    {"anotador_id": str(uuid4()), "proyecto_id": str(uuid4()),
                     "token": "x", "contenido": {}})
        comprobar("sin Sesión, guardar borrador responde 503 (sin colgarse)", c == 503, f"código {c}")
    if servicio == "registro":
        c, _ = http("GET", f"/proyectos/{uuid4()}/exportar")
        comprobar("sin registro, exportar responde 503", c == 503, f"código {c}")

    compose("start", servicio)
    rec = esperar(lambda: salud()[:2] == (200, "operativo"), 90)
    comprobar(f"tras arrancar {servicio}, /salud vuelve a operativo", rec, str(salud()))


# ---------------------------------------------------------------- 2. puertos

def puertos():
    print("\n--- Puertos publicados al host ---", flush=True)
    _, salida = compose("ps", "--format", "json")
    filas = []
    for linea in salida.splitlines():
        linea = linea.strip()
        if not linea:
            continue
        dato = json.loads(linea)
        filas += dato if isinstance(dato, list) else [dato]
    publicados = {}
    for f in filas:
        pubs = sorted({p["PublishedPort"] for p in f.get("Publishers") or [] if p.get("PublishedPort")})
        publicados[f["Service"]] = pubs
        print(f"    {f['Service']:<17} {f.get('State', ''):<9} publicados={pubs}")
    con_puerto = {s: p for s, p in publicados.items() if p}
    comprobar("solo el gateway publica puertos y es el 8000", con_puerto == {"gateway": [8000]},
              str(con_puerto))

    c, _ = http("GET", "/salud")
    comprobar("desde el host, localhost:8000/salud responde", c == 200, f"código {c}")
    for nombre, puerto in INTERNOS.items():
        s = socket.socket()
        s.settimeout(2)
        abierto = s.connect_ex(("127.0.0.1", puerto)) == 0
        s.close()
        comprobar(f"desde el host, {nombre} ({puerto}) no es accesible", not abierto,
                  "conexión rechazada" if not abierto else "ABIERTO")


# ---------------------------------------------------------------- 3. aislamiento

def imports_de(carpeta: Path) -> set[str]:
    mods = set()
    for f in carpeta.rglob("*.py"):
        for nodo in ast.walk(ast.parse(f.read_text(encoding="utf-8"))):
            if isinstance(nodo, ast.Import):
                mods |= {a.name.split(".")[0] for a in nodo.names}
            elif isinstance(nodo, ast.ImportFrom) and nodo.module and nodo.level == 0:
                mods.add(nodo.module.split(".")[0])
    return mods


def aislamiento():
    print("\n--- Reglas de aislamiento ---", flush=True)
    for servicio, (carpeta, modulos, variables) in REGLAS.items():
        usados = imports_de(PROYECTO / carpeta) & modulos
        comprobar(f"{servicio}: el código no importa {sorted(modulos)}", not usados, str(sorted(usados)))

        js = ("import importlib.util,json,os;"
              f"print(json.dumps({{'paquetes':[m for m in {sorted(modulos)!r} "
              "if importlib.util.find_spec(m)],"
              f"'variables':[v for v in {sorted(variables)!r} if v in os.environ]}}))")
        rc, out = compose("exec", "-T", servicio, "python", "-c", js, timeout=60)
        if rc != 0:
            comprobar(f"{servicio}: inspección del contenedor", False, "no se pudo ejecutar")
            continue
        dato = json.loads(out.splitlines()[-1])
        comprobar(f"{servicio}: la imagen no tiene instalado {sorted(modulos)}",
                  not dato["paquetes"], str(dato["paquetes"]))
        comprobar(f"{servicio}: el contenedor no recibe {sorted(variables)}",
                  not dato["variables"], str(dato["variables"]))
    print("    Nota: todos los servicios comparten la red de Compose; el aislamiento se")
    print("    garantiza por dependencias y configuración, no por segmentación de red.")
    print("    Analítica y simulador: PENDIENTES (aún no existen en el repositorio).")


def main():
    print(f"Inicio UTC: {datetime.now(timezone.utc).isoformat()}")
    print("Objetivo: P1 — /salud por capa, puertos publicados y reglas de aislamiento")
    print(f"Máquina: {platform.platform()}, CPU lógicos: {os.cpu_count()}")

    ok = esperar(lambda: salud()[:2] == (200, "operativo"), 60)
    comprobar("estado inicial: /salud 200 operativo", ok, str(salud()))
    if not ok:
        sys.exit(1)

    try:
        caso_salud("sesion", "degradado", 200)
        caso_salud("historial", "degradado", 200)
        caso_salud("registro", "caido", 503)
    finally:
        compose("start", "sesion", "historial", "registro")
        esperar(lambda: salud()[:2] == (200, "operativo"), 90)

    puertos()
    aislamiento()

    total = sum(resultados)
    print(f"\nResultado: {total}/{len(resultados)} comprobaciones correctas")
    print(f"Fin UTC: {datetime.now(timezone.utc).isoformat()}")
    sys.exit(0 if total == len(resultados) else 1)


if __name__ == "__main__":
    main()
