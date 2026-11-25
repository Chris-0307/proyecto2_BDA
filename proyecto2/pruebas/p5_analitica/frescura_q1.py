"""Frescura hasta Q1: segundos entre el HTTP 200 de una aprobación y el
momento en que GET /analitica/q1 (por el gateway) ya la cuenta.

Por cada muestra: crea una tarea de imagen, la toma, la envía, lee el conteo
base de Q1 para (hoy UTC, imagen), la aprueba y repite Q1 hasta que el conteo
suba en 1. Se ejecuta en el contenedor del registro solo para crear sus datos
de prueba (cliente, anotador y revisor nuevos, con pymongo); todas las
operaciones pasan por el gateway. Ejecutar sin otra carga en el sistema.

  docker compose -f proyecto2/docker-compose.yml run --rm registro \
    python /pruebas/p5_analitica/frescura_q1.py | tee proyecto2/evidence/p5_frescura_q1.txt
"""

import json
import os
import platform
import sys
import time
import urllib.error
import urllib.request
from datetime import datetime, timezone
from uuid import uuid4

sys.path.insert(0, "/pruebas/p1_gateway")
from smoke_flujo import preparar  # noqa: E402

GATEWAY = os.getenv("GATEWAY_URL", "http://gateway:8000")
MUESTRAS = int(os.getenv("FRESCURA_MUESTRAS", "5"))
LIMITE_S = float(os.getenv("FRESCURA_LIMITE_S", "180"))
MODALIDAD = "imagen"
resultados: list[bool] = []


def gw(metodo, ruta, cuerpo=None, timeout=30):
    datos = json.dumps(cuerpo).encode() if cuerpo is not None else None
    req = urllib.request.Request(GATEWAY + ruta, data=datos, method=metodo,
                                 headers={"Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(req, timeout=timeout) as r:
            crudo = r.read()
            return r.status, json.loads(crudo) if crudo else None
    except urllib.error.HTTPError as e:
        return e.code, e.read().decode(errors="replace")


def comprobar(nombre, condicion, detalle=""):
    resultados.append(bool(condicion))
    print(f"[{'OK' if condicion else 'FALLO'}] {nombre}" + (f" — {detalle}" if detalle else ""), flush=True)


def conteo_q1(dia: str) -> tuple[int, float]:
    """Conteo de Q1 para (dia, imagen) y duración de la llamada en s."""
    t0 = time.perf_counter()
    c, r = gw("GET", "/analitica/q1", timeout=150)
    dur = time.perf_counter() - t0
    if c != 200:
        raise RuntimeError(f"/analitica/q1 respondió {c}: {str(r)[:200]}")
    for f in r["filas"]:
        if f["dia"] == dia and f["modalidad"] == MODALIDAD:
            return int(f["tareas_aprobadas"]), dur
    return 0, dur


def pctl(vals, p):
    v = sorted(vals)
    return v[min(len(v) - 1, max(0, round(p / 100 * len(v) + 0.5) - 1))]


def main() -> int:
    print(f"Inicio UTC: {datetime.now(timezone.utc).isoformat()}")
    print("Objetivo: frescura hasta Q1 (HTTP 200 de la aprobación -> Q1 la cuenta), por el gateway")
    print(f"Máquina: {platform.platform()}, CPU lógicos: {os.cpu_count()}")
    print(f"Configuración: {MUESTRAS} muestras, modalidad {MODALIDAD}, límite {LIMITE_S:.0f} s por muestra; "
          "Q1 se repite sin pausa (cada llamada lee todo el datalake)")

    cliente, ids = preparar()
    anotador, revisor = ids["a1"], ids["rev"]
    tax = [{"etiqueta": "auto", "instruccion": "Vehículo."}]
    c, proyecto = gw("POST", "/proyectos", {"cliente_id": cliente, "nombre": "frescura-q1", "taxonomia": tax})
    comprobar("crear proyecto responde 201", c == 201, f"código {c}")
    pid = proyecto["_id"]
    tareas = [{"modalidad": MODALIDAD, "tipo_tarea": "bbox", "prioridad": 1,
               "archivo": {"uri": f"s3://frescura-q1/{uuid4()}", "sha256": "c" * 64,
                           "bytes": 10, "mime": "image/png"}} for _ in range(MUESTRAS)]
    c, _ = gw("POST", f"/proyectos/{pid}/lotes", {"nombre": "lote", "tareas": tareas})
    comprobar(f"registrar lote de {MUESTRAS} tareas responde 201", c == 201, f"código {c}")

    # Arranque de Spark fuera de la medición.
    _, dur = conteo_q1(datetime.now(timezone.utc).date().isoformat())
    print(f"Q1 de precalentamiento: {dur:.2f} s")

    frescuras, consultas_q1 = [], []
    for i in range(1, MUESTRAS + 1):
        c, s = gw("POST", "/tareas/siguiente", {"anotador_id": anotador, "proyecto_id": pid})
        if c != 200:
            comprobar(f"muestra {i}: siguiente responde 200", False, f"código {c}")
            continue
        tid, token = s["tarea"]["_id"], s["token"]
        c, _ = gw("POST", f"/tareas/{tid}/enviar", {"anotador_id": anotador, "proyecto_id": pid,
                                                    "token": token, "contenido": {"cajas": [[1, 2, 3, 4]]}})
        if c != 200:
            comprobar(f"muestra {i}: enviar responde 200", False, f"código {c}")
            continue

        dia = datetime.now(timezone.utc).date().isoformat()
        base, dur_base = conteo_q1(dia)

        c, _ = gw("POST", f"/tareas/{tid}/revision", {"revisor_id": revisor, "decision": "aprobar"})
        t_200 = time.perf_counter()
        if c != 200:
            comprobar(f"muestra {i}: aprobar responde 200", False, f"código {c}")
            continue

        intentos, visto = 0, None
        while time.perf_counter() - t_200 < LIMITE_S:
            intentos += 1
            t_ini = time.perf_counter()
            n, dur = conteo_q1(dia)
            consultas_q1.append(dur)
            if n >= base + 1:
                visto = (time.perf_counter() - t_200, t_ini - t_200, n)
                break
        if visto is None:
            comprobar(f"muestra {i}: Q1 refleja la aprobación antes de {LIMITE_S:.0f} s", False,
                      f"base={base}, intentos={intentos}")
            continue
        frescuras.append(visto[0])
        comprobar(f"muestra {i}: Q1 pasa de {base} a {visto[2]} para ({dia}, {MODALIDAD})",
                  visto[2] == base + 1,
                  f"frescura {visto[0]:.2f} s (la consulta que lo vio empezó a los {visto[1]:.2f} s); "
                  f"intentos={intentos}; Q1 base {dur_base:.2f} s")

    print()
    if frescuras:
        print(f"Frescura hasta Q1 (s), n={len(frescuras)}: p50={pctl(frescuras, 50):.2f} "
              f"p95={pctl(frescuras, 95):.2f} p99={pctl(frescuras, 99):.2f} "
              f"mín={min(frescuras):.2f} máx={max(frescuras):.2f}")
        print(f"Duración de cada GET /analitica/q1 (s), n={len(consultas_q1)}: "
              f"p50={pctl(consultas_q1, 50):.2f} máx={max(consultas_q1):.2f}")
    print("Interpretación: la frescura incluye la publicación del evento (gateway -> Historial -> worker -> Parquet) "
          "y al menos una lectura completa del datalake por Q1; su resolución es la duración de una Q1.")
    ok = sum(resultados)
    print(f"\nResultado: {ok}/{len(resultados)} comprobaciones correctas")
    print(f"Fin UTC: {datetime.now(timezone.utc).isoformat()}")
    return 0 if ok == len(resultados) else 1


if __name__ == "__main__":
    sys.exit(main())
