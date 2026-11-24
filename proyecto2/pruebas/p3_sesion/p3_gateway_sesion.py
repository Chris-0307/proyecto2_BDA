"""P3 integrada: ejecutar desde el host con Docker Compose levantado.

Las operaciones del anotador pasan exclusivamente por el gateway. Docker
se utiliza para preparar actores de prueba, consultar AOF, reiniciar Valkey
y verificar administrativamente el dueño en MongoDB. No borra volúmenes.
"""
import json
import logging
import math
import os
import platform
import subprocess
import sys
import time
import urllib.error
import urllib.request
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
COMPOSE = ["docker", "compose", "-f", "proyecto2/docker-compose.yml"]
GATEWAY = os.getenv("GATEWAY_URL", "http://localhost:8000")
EVIDENCE = ROOT / "proyecto2/evidence/p3_gateway_sesion.txt"
log = logging.getLogger("P3")
resultados = []
latencias = {}


def docker(*args):
    cmd = [*COMPOSE, *args]
    log.info("Comando: %s", " ".join(cmd))
    p = subprocess.run(cmd, cwd=ROOT, capture_output=True, text=True,
                       encoding="utf-8", errors="replace", timeout=90)
    if p.stderr.strip():
        log.info("Docker stderr: %s", p.stderr.strip())
    if p.returncode:
        raise RuntimeError(f"Docker exit={p.returncode}: {p.stdout} {p.stderr}")
    return p.stdout.strip()


def gw(metodo, ruta, cuerpo=None):
    datos = json.dumps(cuerpo).encode() if cuerpo is not None else None
    req = urllib.request.Request(GATEWAY + ruta, data=datos, method=metodo,
                                 headers={"Content-Type": "application/json"})
    inicio = time.perf_counter()
    try:
        with urllib.request.urlopen(req, timeout=15) as r:
            codigo, raw = r.status, r.read()
    except urllib.error.HTTPError as e:
        codigo, raw = e.code, e.read()
    duracion = (time.perf_counter() - inicio) * 1000
    clave = metodo + " " + ("/tareas/{id}/" + ruta.split("/")[-1]
                            if ruta.startswith("/tareas/") and ruta.count("/") == 3
                            else ruta)
    latencias.setdefault(clave, []).append(duracion)
    log.info("%s %s -> HTTP %s (%.2f ms)", metodo, ruta, codigo, duracion)
    try:
        body = json.loads(raw) if raw else None
    except ValueError:
        body = raw.decode(errors="replace")
    return codigo, body


def comprobar(nombre, condicion, detalle=""):
    resultados.append(bool(condicion))
    log.info("[%s] %s %s", "OK" if condicion else "FALLO", nombre, detalle)
    if not condicion:
        raise AssertionError(f"{nombre}: {detalle}")


def esperado(metodo, ruta, cuerpo, codigo):
    c, r = gw(metodo, ruta, cuerpo)
    comprobar(f"{metodo} {ruta} responde {codigo}", c == codigo,
              f"HTTP {c}" if c == codigo else f"HTTP {c}: {r}")
    return r


def preparar():
    # Reutiliza la preparacion administrativa de P1; no es parte del simulador.
    codigo = ("import sys,json; sys.path.insert(0,'/pruebas/p1_gateway'); "
              "from smoke_flujo import preparar; print(json.dumps(preparar()))")
    cliente, ids = json.loads(docker("exec", "-T", "registro", "python", "-c", codigo))
    proyecto = esperado("POST", "/proyectos", {
        "cliente_id": cliente, "nombre": "P3-" + cliente,
        "taxonomia": [{"etiqueta": "auto", "instruccion": "Vehiculo"}],
    }, 201)
    pid = proyecto["_id"]
    lote = esperado("POST", f"/proyectos/{pid}/lotes", {
        "nombre": "p3-unica-tarea", "tareas": [{"modalidad": "imagen",
        "tipo_tarea": "bbox", "prioridad": 1, "archivo": {
            "uri": f"s3://p3/{cliente}", "sha256": "b" * 64,
            "bytes": 10, "mime": "image/png"}}],
    }, 201)
    return pid, lote["tarea_ids"][0], ids


def comprobar_aof():
    for parametro, valor in [("appendonly", "yes"), ("appendfsync", "everysec")]:
        salida = docker("exec", "-T", "valkey", "valkey-cli", "--raw", "CONFIG", "GET", parametro)
        comprobar(f"AOF {parametro}={valor}", salida.splitlines() == [parametro, valor], salida)


def verificar_dueno(tid, sesion):
    # Observacion administrativa de MongoDB; el anotador no accede a la BD.
    codigo = ("import os,sys,json; from pymongo import MongoClient; "
              "d=MongoClient(os.environ['MONGO_URI']).plataforma.tareas.find_one({'_id':sys.argv[1]}); "
              "print(json.dumps({'estado':d['estado'],'asignacion':d['asignacion'],'version':d['version']},default=str))")
    d = json.loads(docker("exec", "-T", "registro", "python", "-c", codigo, tid))
    a = d["asignacion"]
    comprobar("MongoDB tiene un solo dueno vigente y coincide con B",
              d["estado"] == "abierta" and a["anotador_id"] == sesion["anotador_id"]
              and a["token"] == sesion["token"], f"version={d['version']}")
    return d["version"]


def recuperar_tras_reinicio(sesion, tid):
    limite = time.monotonic() + 60
    while time.monotonic() < limite:
        try:
            c, r = gw("POST", "/sesiones/recuperar", {**sesion, "tarea_id": tid})
            if c == 200:
                return r
            if c not in (502, 503, 504):
                raise AssertionError(f"Recuperacion inesperada HTTP {c}: {r}")
        except (urllib.error.URLError, TimeoutError) as e:
            log.info("Gateway temporalmente inaccesible: %s", e)
        time.sleep(0.5)
    raise TimeoutError("No se recupero la sesion despues de reiniciar Valkey")


def principal():
    log.info("Objetivo: P3, borrador reciente, AOF, vencimiento natural y exclusion de propietarios")
    log.info("Inicio UTC: %s", datetime.now(timezone.utc).isoformat())
    log.info("Host: %s; CPU logicos: %s; Python: %s", platform.platform(), os.cpu_count(), platform.python_version())
    log.info("Comando de prueba: %s", subprocess.list2cmdline(sys.argv))
    log.info("Docker Compose: %s", docker("version"))
    try:
        info = subprocess.run(["docker", "info", "--format", "{{.NCPU}} CPU; RAM={{.MemTotal}} bytes; Docker={{.ServerVersion}}"],
                              capture_output=True, text=True, timeout=15, check=True)
        log.info("Recursos del motor Docker: %s", info.stdout.strip())
    except (OSError, subprocess.SubprocessError) as e:
        log.warning("No se pudo consultar docker info: %s", e)
    log.info("Configuracion de servicios: %s", docker("ps"))
    log.info("Gateway: %s; TTL esperado: 120 s; borradores cada 2 s", GATEWAY)
    comprobar_aof()
    pid, tid, ids = preparar()
    log.info("Proyecto de prueba: %s; tarea: %s", pid, tid)
    solicitud_a = {"anotador_id": ids["a1"], "proyecto_id": pid}
    solicitud_b = {"anotador_id": ids["a2"], "proyecto_id": pid}
    a = esperado("POST", "/tareas/siguiente", solicitud_a, 200)
    sa = {**solicitud_a, "token": a["token"]}
    activa = sa
    try:
        comprobar("A recibe la tarea y el TTL de 120 segundos",
                  a["tarea"]["_id"] == tid and a["ttl_ms"] == 120000)
        version_a = a["tarea"]["version"]
        esperado("POST", "/tareas/siguiente", solicitud_b, 204)
        # Tres guardados separados por 2 s, como en el simulador.
        for n in range(1, 4):
            contenido = {"cajas": [[1, 2, 3, 4]], "etiqueta": "auto", "avance": n}
            guardado = esperado("PUT", f"/tareas/{tid}/borrador", {**sa, "contenido": contenido}, 200)
            if n < 3:
                time.sleep(2)
        # Simulamos desconexion: se deja de guardar y luego se recupera.
        time.sleep(2)
        rec = esperado("POST", "/sesiones/recuperar", {**sa, "tarea_id": tid}, 200)
        edad_ms = time.time_ns() // 1_000_000 - rec["actualizado_en_ms"]
        comprobar("Se recupera el ultimo borrador y su version",
                  rec["estado"] == "activa" and rec["borrador"] == contenido
                  and rec["version"] == guardado["version"])
        comprobar("Antiguedad del borrador <= 5 s", 0 <= edad_ms <= 5000,
                  f"antiguedad_ms={edad_ms}; reloj host frente a Valkey")
        # Espera superior al intervalo AOF para probar un reinicio ordenado.
        time.sleep(2.5)
        inicio = time.perf_counter()
        docker("restart", "valkey")
        rec = recuperar_tras_reinicio(sa, tid)
        log.info("Tiempo restart hasta recuperacion HTTP 200: %.3f s", time.perf_counter() - inicio)
        comprobar("AOF conserva el contenido y la version del ultimo borrador",
                  rec["borrador"] == contenido and rec["version"] == guardado["version"])
        comprobar("Lock permanece activo tras reinicio", rec["estado"] == "activa" and rec["ttl_ms"] > 0)
        esperado("POST", "/tareas/siguiente", solicitud_b, 204)
        # La recuperacion anterior renovo el lock; esperar su TTL completo.
        espera = rec["ttl_ms"] / 1000 + 2
        log.info("Esperando vencimiento natural %.1f s sin latidos ni borradores", espera)
        time.sleep(espera)
        vencida = esperado("POST", "/sesiones/recuperar", {**sa, "tarea_id": tid}, 200)
        comprobar("Lock vencido: borrador recuperable sin permiso de edicion",
                  vencida["estado"] == "requiere_reasignacion" and vencida["ttl_ms"] == 0
                  and vencida["borrador"] == contenido)
        esperado("PUT", f"/tareas/{tid}/borrador", {**sa, "contenido": contenido}, 409)
        b = esperado("POST", "/tareas/siguiente", solicitud_b, 200)
        sb = {**solicitud_b, "token": b["token"]}
        activa = sb
        comprobar("B recibe la misma tarea con token distinto y nueva version",
                  b["tarea"]["_id"] == tid and b["token"] != sa["token"]
                  and b["tarea"]["version"] == version_a + 1)
        comprobar("B no recibe el borrador privado de A", b["borrador"] is None)
        esperado("PUT", f"/tareas/{tid}/borrador", {**sa, "contenido": contenido}, 409)
        esperado("POST", f"/tareas/{tid}/enviar", {**sa, "contenido": contenido}, 409)
        esperado("POST", "/sesiones/recuperar", {**sa, "tarea_id": tid}, 409)
        esperado("POST", "/tareas/siguiente", solicitud_a, 204)
        comprobar("Version vigente en MongoDB", verificar_dueno(tid, sb) == b["tarea"]["version"])
        esperado("PUT", f"/tareas/{tid}/borrador", {**sb, "contenido": {"etiqueta": "auto", "avance": 1}}, 200)
        log.info("Interpretacion: la tarea vuelve a ofrecerse tras vencer el lock y el token anterior no permite editar ni enviar.")
    finally:
        fallo_previo = sys.exc_info()[0] is not None
        try:
            c, r = gw("POST", f"/tareas/{tid}/liberar", activa)
            log.info("Restauracion: liberar tarea de prueba -> HTTP %s", c)
            if c != 200:
                raise RuntimeError(f"No se pudo liberar: HTTP {c}: {r}")
        except Exception:
            log.exception("No se pudo liberar la tarea; revisar proyecto %s", pid)
            if not fallo_previo:
                raise
        finally:
            log.info("Se conservan las entidades de prueba y su historial para trazabilidad; no se eliminan datos existentes.")


def main():
    EVIDENCE.parent.mkdir(parents=True, exist_ok=True)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s",
                        handlers=[logging.StreamHandler(sys.stdout), logging.FileHandler(EVIDENCE, mode="w", encoding="utf-8")],
                        force=True)
    # Se conserva la salida original de la ultima corrida; archivar antes de repetir si es necesario.
    codigo = 0
    try:
        principal()
    except Exception:
        log.exception("P3 FALLIDA")
        codigo = 1
    finally:
        log.info("Resultado: %d/%d comprobaciones ejecutadas correctas; exit=%d", sum(resultados), len(resultados), codigo)
        log.info("Latencias HTTP observadas (ms), incluyendo respuestas de error: %s", json.dumps(latencias))
        for ruta, muestras in latencias.items():
            ordenadas = sorted(muestras)
            percentiles = [ordenadas[math.ceil(p * len(ordenadas)) - 1] for p in (0.50, 0.95, 0.99)]
            log.info("%s: n=%d; p50=%.2f ms; p95=%.2f ms; p99=%.2f ms (rango mas cercano)",
                     ruta, len(muestras), *percentiles)
        log.info("P3 funcional: pocas muestras; no sustituye P6 de carga ni P8 de fallas abruptas.")
        log.info("Fin UTC: %s", datetime.now(timezone.utc).isoformat())
        log.info("Evidencia: %s", EVIDENCE)
    return codigo


if __name__ == "__main__":
    sys.exit(main())
