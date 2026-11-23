"""Prueba del gateway básico (paso 4): proyectos, lotes, exportar y /salud.

Solo biblioteca estándar. Requiere dataset/cargar_demo.py (cliente demo).
Por defecto habla con http://gateway:8000 desde dentro de la red Docker;
desde el host: GATEWAY_URL=http://localhost:8000 python3 smoke_gateway.py

No sustituye P1 completa: falta el resto de rutas y los estados degradado
y caído (se prueban deteniendo contenedores).
"""

import json
import os
import platform
import sys
import urllib.error
import urllib.request
from datetime import datetime, timezone
from uuid import NAMESPACE_DNS, uuid4, uuid5

BASE = os.getenv("GATEWAY_URL", "http://gateway:8000")
CLIENTE = str(uuid5(NAMESPACE_DNS, "demo-cliente"))
resultados: list[bool] = []


def llamar(metodo, ruta, cuerpo=None, cabeceras=None):
    datos = json.dumps(cuerpo).encode() if cuerpo is not None else None
    h = {"Content-Type": "application/json", **(cabeceras or {})}
    req = urllib.request.Request(BASE + ruta, data=datos, method=metodo, headers=h)
    try:
        with urllib.request.urlopen(req, timeout=30) as r:
            return r.status, json.loads(r.read() or b"null"), dict(r.headers)
    except urllib.error.HTTPError as e:
        crudo = e.read()
        try:
            cuerpo = json.loads(crudo)
        except ValueError:
            cuerpo = crudo.decode(errors="replace")
        return e.code, cuerpo, dict(e.headers)


def comprobar(nombre, condicion, detalle=""):
    resultados.append(bool(condicion))
    print(f"[{'OK' if condicion else 'FALLO'}] {nombre}" + (f" — {detalle}" if detalle else ""))


def tarea(i):
    uri = f"s3://smoke-gateway/{i}"
    return {
        "modalidad": "imagen", "tipo_tarea": "bbox", "prioridad": i,
        "archivo": {"uri": uri, "sha256": "a" * 64, "bytes": 10, "mime": "image/png"},
    }


def main():
    print(f"Inicio UTC: {datetime.now(timezone.utc).isoformat()}")
    print("Objetivo: gateway básico (proyectos, lotes, exportar, salud por capa)")
    print(f"Máquina: {platform.platform()}, CPU lógicos: {os.cpu_count()}")
    print(f"Gateway: {BASE}")

    c, b, _ = llamar("GET", "/salud")
    comprobar("salud responde 200 y operativo", c == 200 and b.get("estado") == "operativo",
              f"{c} {b.get('estado') if isinstance(b, dict) else b}")
    capas = b.get("capas", {}) if isinstance(b, dict) else {}
    comprobar("salud informa las capas registro, sesion e historial",
              set(capas) == {"registro", "sesion", "historial"}, ",".join(capas))

    rid = str(uuid4())
    c, proyecto, h = llamar("POST", "/proyectos", {
        "cliente_id": CLIENTE, "nombre": "Smoke gateway",
        "taxonomia": [{"etiqueta": "auto", "instruccion": "Vehículo."}], "sla_horas": 24,
    }, {"X-Request-ID": rid})
    comprobar("crear proyecto por el gateway responde 201", c == 201, f"código {c}")
    comprobar("el gateway devuelve el X-Request-ID recibido",
              h.get("X-Request-ID", h.get("x-request-id")) == rid)
    _, _, h2 = llamar("GET", "/salud")
    comprobar("sin X-Request-ID el gateway genera uno",
              bool(h2.get("X-Request-ID", h2.get("x-request-id"))))
    pid = proyecto["_id"] if c == 201 else str(uuid4())

    c, b, _ = llamar("POST", "/proyectos", {
        "cliente_id": str(uuid4()), "nombre": "x",
        "taxonomia": [{"etiqueta": "a", "instruccion": "b"}],
    })
    comprobar("cliente inexistente responde 404 (código del registro)", c == 404, f"código {c}")

    c, b, _ = llamar("POST", "/proyectos", {"nombre": "sin cliente"})
    comprobar("cuerpo incompleto responde 422", c == 422, f"código {c}")

    c, lote, _ = llamar("POST", f"/proyectos/{pid}/lotes",
                        {"nombre": "Lote smoke", "tareas": [tarea(i) for i in range(5)]})
    comprobar("registrar lote de 5 tareas responde 201",
              c == 201 and lote.get("tareas_creadas") == 5, f"código {c}")

    c, _, _ = llamar("POST", "/proyectos/no-es-uuid/lotes", {"nombre": "x", "tareas": [tarea(0)]})
    comprobar("id de proyecto que no es UUID responde 422", c == 422, f"código {c}")

    c, _, _ = llamar("POST", f"/proyectos/{uuid4()}/lotes", {"nombre": "x", "tareas": [tarea(0)]})
    comprobar("lote en proyecto inexistente responde 404", c == 404, f"código {c}")

    c, b, _ = llamar("GET", f"/proyectos/{pid}/exportar?limite=10")
    comprobar("exportar responde 200 con items y siguiente",
              c == 200 and isinstance(b, dict) and "items" in b and "siguiente" in b, f"código {c}")
    comprobar("exportar de un proyecto sin aprobadas está vacío",
              c == 200 and b.get("items") == [] and b.get("siguiente") is None)

    c, _, _ = llamar("GET", f"/proyectos/{uuid4()}/exportar")
    comprobar("exportar proyecto inexistente responde 404", c == 404, f"código {c}")

    c, _, _ = llamar("GET", f"/proyectos/{pid}/exportar?limite=0")
    comprobar("exportar con limite fuera de rango responde 422", c == 422, f"código {c}")

    ok = sum(resultados)
    print(f"\nResultado: {ok}/{len(resultados)} comprobaciones correctas")
    print(f"Fin UTC: {datetime.now(timezone.utc).isoformat()}")
    sys.exit(0 if ok == len(resultados) else 1)


if __name__ == "__main__":
    main()
