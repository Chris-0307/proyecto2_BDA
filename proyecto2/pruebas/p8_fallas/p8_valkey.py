"""P8 Valkey integrado. Ejecutar desde el host con el Compose final.

Requiere Python 3 y Docker; no publica puertos ni borra volumenes.
Ejecutar sin otras pruebas de carga: interrumpe el Valkey compartido.
Las operaciones de usuario pasan exclusivamente por el gateway.
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
EVIDENCE = ROOT / "proyecto2/evidence/p8_valkey_fallas.txt"
LOG = logging.getLogger("P8-Valkey")
RESULTADOS = []
LATENCIAS = {}
METRICAS = {}


def ejecutar_docker(cmd):
    LOG.info("Comando Docker: %s", subprocess.list2cmdline(cmd))
    p = subprocess.run(cmd, cwd=ROOT, capture_output=True, text=True,
                       encoding="utf-8", errors="replace", timeout=60)
    if p.stderr.strip():
        LOG.info("Docker stderr: %s", p.stderr.strip())
    if p.returncode:
        raise RuntimeError(f"Docker exit={p.returncode}: {p.stdout} {p.stderr}")
    return p.stdout.strip()


def docker(*args):
    return ejecutar_docker([*COMPOSE, *args])


def motor(*args):
    return ejecutar_docker(["docker", *args])


def gw(metodo, ruta, cuerpo=None):
    data = json.dumps(cuerpo).encode() if cuerpo is not None else None
    req = urllib.request.Request(GATEWAY + ruta, data=data, method=metodo,
                                 headers={"Content-Type": "application/json"})
    inicio = time.perf_counter()
    try:
        with urllib.request.urlopen(req, timeout=15) as r:
            codigo, raw = r.status, r.read()
    except urllib.error.HTTPError as e:
        codigo, raw = e.code, e.read()
    except (urllib.error.URLError, TimeoutError, OSError) as e:
        LOG.warning("%s %s -> sin respuesta: %s", metodo, ruta, e)
        return None, {"error": str(e)}
    ms = (time.perf_counter() - inicio) * 1000
    partes = ruta.split("/")
    clave = metodo + " " + (f"/{partes[1]}/{{id}}/{partes[3]}"
            if len(partes) == 4 and partes[1] in ("tareas", "proyectos") else ruta)
    LATENCIAS.setdefault(clave, []).append(ms)
    LOG.info("%s %s -> HTTP %s (%.3f ms)", metodo, ruta, codigo, ms)
    try:
        return codigo, json.loads(raw) if raw else None
    except ValueError:
        return codigo, raw.decode(errors="replace")


def comprobar(nombre, condicion, detalle=""):
    RESULTADOS.append(bool(condicion))
    LOG.info("[%s] %s %s", "OK" if condicion else "FALLO", nombre, detalle)
    if not condicion:
        raise AssertionError(f"{nombre}: {detalle}")


def esperado(metodo, ruta, cuerpo, codigo):
    c, r = gw(metodo, ruta, cuerpo)
    comprobar(f"{metodo} {ruta} responde {codigo}", c == codigo,
              "" if c == codigo else f"HTTP {c}: {r}")
    return r


def esperar_sano(limite=60):
    fin = time.monotonic() + limite
    while time.monotonic() < fin:
        c, r = gw("GET", "/salud")
        if c == 200 and isinstance(r, dict) and r.get("estado") == "operativo":
            LOG.info("Salud operativa: %s", json.dumps(r, ensure_ascii=False))
            return r
        time.sleep(0.5)
    raise TimeoutError("Gateway no regreso a operativo")


def comprobar_aof():
    for parametro, valor in (("appendonly", "yes"), ("appendfsync", "everysec")):
        r = docker("exec", "-T", "valkey", "valkey-cli", "--raw", "CONFIG", "GET", parametro)
        comprobar(f"AOF {parametro}={valor}", r.splitlines() == [parametro, valor], r)


def preparar():
    # Solo el fixture administrativo usa MongoDB; el usuario de prueba usa HTTP.
    codigo = ("import sys,json; sys.path.insert(0,'/pruebas/p1_gateway'); "
              "from smoke_flujo import preparar; print(json.dumps(preparar()))")
    cliente, ids = json.loads(docker("exec", "-T", "registro", "python", "-c", codigo))
    proyecto = esperado("POST", "/proyectos", {
        "cliente_id": cliente, "nombre": "P8-Valkey-" + cliente,
        "taxonomia": [{"etiqueta": "auto", "instruccion": "Vehiculo"}],
    }, 201)
    pid = proyecto["_id"]
    lote = esperado("POST", f"/proyectos/{pid}/lotes", {
        "nombre": "p8-valkey", "tareas": [{"modalidad": "imagen", "tipo_tarea": "bbox",
        "prioridad": 1, "archivo": {"uri": f"s3://p8/{cliente}", "sha256": "b"*64,
                                     "bytes": 10, "mime": "image/png"}}],
    }, 201)
    tid = lote["tarea_ids"][0]
    solicitud = {"anotador_id": ids["a1"], "proyecto_id": pid}
    a = esperado("POST", "/tareas/siguiente", solicitud, 200)
    comprobar("Tarea de prueba adquirida", a["tarea"]["_id"] == tid)
    return tid, {**solicitud, "token": a["token"]}


def guardar(tid, sesion, contenido):
    r = esperado("PUT", f"/tareas/{tid}/borrador", {**sesion, "contenido": contenido}, 200)
    registro = {"version": r["version"], "actualizado_en_ms": r["actualizado_en_ms"],
                "contenido": contenido}
    LOG.info("Borrador confirmado: %s", json.dumps(registro))
    return registro


def recuperar(tid, sesion, limite=60):
    fin = time.monotonic() + limite
    while time.monotonic() < fin:
        c, r = gw("POST", "/sesiones/recuperar", {**sesion, "tarea_id": tid})
        if c == 200:
            LOG.info("Borrador recuperado: %s", json.dumps(r, ensure_ascii=False))
            return r
        if c not in (None, 502, 503, 504):
            raise RuntimeError(f"Recuperacion HTTP {c}: {r}; RPO no determinable por HTTP")
        time.sleep(0.5)
    raise TimeoutError("No se recupero la sesion; RPO no determinable")


def medir_rpo(nombre, confirmados, recuperado):
    ultimo = confirmados[-1]
    version = recuperado.get("version")
    contenido = recuperado.get("borrador")
    fecha = recuperado.get("actualizado_en_ms")
    previo = next((b for b in confirmados if b["version"] == version), None)
    valido = (previo is not None and contenido == previo["contenido"]
              and fecha == previo["actualizado_en_ms"])
    if not valido:
        METRICAS[nombre]["rpo"] = "no determinable: borrador no coincide con versiones conocidas"
        comprobar("Borrador recuperado coincide con una version confirmada", False, str(recuperado))
    perdidas = ultimo["version"] - version
    perdida_ms = ultimo["actualizado_en_ms"] - fecha
    METRICAS[nombre].update(version_confirmada=ultimo["version"], version_recuperada=version,
                            versiones_perdidas=perdidas, rpo_observado_ms=perdida_ms)
    LOG.info("%s RPO observado: %d versiones perdidas; %d ms entre puntos confirmados; ultima=%d recuperada=%d",
             nombre, perdidas, perdida_ms, ultimo["version"], version)
    comprobar("Contenido y marca temporal coinciden con version confirmada", valido)
    return perdidas


def medir_recuperacion(nombre, inicio, arranque, tid, sesion, confirmados, exigir_ultimo):
    METRICAS[nombre] = {"rto_s": None, "rpo": "no determinado aun"}
    r = recuperar(tid, sesion)
    fin = time.perf_counter()
    METRICAS[nombre].update(rto_s=round(fin-inicio, 6),
                             arranque_a_recuperacion_s=round(fin-arranque, 6))
    LOG.info("%s RTO observado (inicio de interrupcion -> HTTP 200): %.3f s; inicio de arranque -> HTTP 200: %.3f s",
             nombre, fin-inicio, fin-arranque)
    comprobar("Sesion recuperada y lock activo", r.get("estado") == "activa" and r.get("ttl_ms", 0) > 0)
    perdidas = medir_rpo(nombre, confirmados, r)
    METRICAS[nombre].pop("rpo", None)
    if exigir_ultimo:
        comprobar("Reinicio/parada ordenada conserva el ultimo borrador", perdidas == 0)
    return r


def principal():
    LOG.info("Objetivo: P8 Valkey, reinicio ordenado, indisponibilidad, SIGKILL, RTO/RPO y restauracion")
    LOG.info("Inicio UTC: %s", datetime.now(timezone.utc).isoformat())
    LOG.info("Host: %s; CPU logicos=%s; Python=%s", platform.platform(), os.cpu_count(), platform.python_version())
    LOG.info("Comando reproducible: python proyecto2/pruebas/p8_fallas/p8_valkey.py (make p8-valkey)")
    LOG.info("Compose: %s", docker("version"))
    info = subprocess.run(["docker", "info", "--format", "{{.NCPU}} CPU; RAM={{.MemTotal}} bytes; Docker={{.ServerVersion}}"],
                          capture_output=True, text=True, timeout=15, check=True)
    LOG.info("Recursos Docker: %s", info.stdout.strip())
    LOG.info("Servicios iniciales:\n%s", docker("ps"))
    esperar_sano()
    comprobar_aof()
    tid, sesion = preparar()
    LOG.info("Proyecto=%s tarea=%s", sesion["proyecto_id"], tid)
    cid = None
    restaurar_politica = False
    try:
        LOG.info("=== A. Reinicio ordenado ===")
        a = guardar(tid, sesion, {"etiqueta": "auto", "avance": 1, "escenario": "A"})
        time.sleep(2.5)
        inicio = time.perf_counter()
        docker("restart", "valkey")
        medir_recuperacion("A_reinicio", inicio, inicio, tid, sesion, [a], True)
        esperar_sano()

        LOG.info("=== B. Indisponibilidad temporal controlada ===")
        b = guardar(tid, sesion, {"etiqueta": "auto", "avance": 2, "escenario": "B"})
        time.sleep(2.5)
        inicio = time.perf_counter()
        docker("stop", "valkey")
        c, r = gw("GET", "/salud")
        LOG.info("Salud durante parada: %s", json.dumps(r, ensure_ascii=False))
        comprobar("Gateway responde 200 degradado con Valkey detenido", c == 200 and isinstance(r, dict)
                  and r.get("estado") == "degradado" and r.get("capas", {}).get("registro", {}).get("estado") == "operativo")
        intento = {**sesion, "contenido": {"etiqueta": "auto", "avance": 99, "escenario": "B-no-confirmado"}}
        esperado("PUT", f"/tareas/{tid}/borrador", intento, 503)
        esperado("POST", f"/tareas/{tid}/enviar", intento, 503)
        arranque = time.perf_counter()
        docker("up", "-d", "--no-deps", "valkey")
        medir_recuperacion("B_parada", inicio, arranque, tid, sesion, [b], True)
        esperar_sano()

        LOG.info("=== C. Caida abrupta mediante SIGKILL ===")
        # Evitar la carrera entre el reinicio automático del motor y Compose.
        # Hacer este cambio ANTES de guardar el último borrador medido.
        cid = docker("ps", "-q", "valkey")
        if not cid or len(cid.splitlines()) != 1:
            raise RuntimeError("No se identifico un unico contenedor Valkey")
        politica = motor("inspect", "--format", "{{.HostConfig.RestartPolicy.Name}}", cid)
        comprobar("Politica original de Valkey: unless-stopped", politica == "unless-stopped", politica)
        restaurar_politica = True
        motor("update", "--restart=no", cid)
        comprobar("Reinicio automatico desactivado temporalmente",
                  motor("inspect", "--format", "{{.HostConfig.RestartPolicy.Name}}", cid) == "no")
        base = guardar(tid, sesion, {"etiqueta": "auto", "avance": 3, "escenario": "C-base"})
        # El borrador base y la sesion tienen oportunidad de persistir antes de la falla.
        time.sleep(2.5)
        ultimo = guardar(tid, sesion, {"etiqueta": "auto", "avance": 4, "escenario": "C-ultimo"})
        inicio = time.perf_counter()
        # No esperar intencionalmente tras el ultimo HTTP 200. El comando Docker
        # tiene su propia latencia y puede permitir que AOF persista el dato.
        docker("kill", "-s", "SIGKILL", "valkey")
        LOG.info("Tiempo desde ultimo HTTP 200 (aprox.) hasta completar kill: %.3f s", time.perf_counter()-inicio)
        # Con el reinicio automático desactivado, wait confirma que el
        # proceso terminó. No usar up: puede observar un estado transitorio.
        salida = motor("wait", cid)
        comprobar("Valkey termino mediante SIGKILL (137)", salida == "137", salida)
        estado = motor("inspect", "--format", "{{.State.Status}}", cid)
        comprobar("Contenedor detenido antes de arrancar", estado == "exited", estado)
        arranque = time.perf_counter()
        docker("start", "valkey")
        try:
            medir_recuperacion("C_sigkill", inicio, arranque, tid, sesion, [base, ultimo], False)
        except Exception:
            LOG.info("Estado tras falla: %s", motor("inspect", "--format", "{{json .State}}", cid))
            LOG.info("Logs Valkey: %s", motor("logs", "--tail", "50", cid))
            raise
        motor("update", "--restart=unless-stopped", cid)
        comprobar("Politica de reinicio restaurada",
                  motor("inspect", "--format", "{{.HostConfig.RestartPolicy.Name}}", cid) == "unless-stopped")
        restaurar_politica = False
        esperar_sano()
        comprobar_aof()
        nuevo = guardar(tid, sesion, {"etiqueta": "auto", "avance": 5, "escenario": "restaurado"})
        r = recuperar(tid, sesion)
        comprobar("Edicion funcional despues de las fallas", r.get("borrador") == nuevo["contenido"] and r.get("version") == nuevo["version"])
    finally:
        fallo_previo = sys.exc_info()[0] is not None
        try:
            # Idempotente si Valkey ya estaba levantado. No elimina volumenes.
            if restaurar_politica:
                # Restaurar la política incluso si el arranque produce error.
                try:
                    docker("start", "valkey")
                finally:
                    motor("update", "--restart=unless-stopped", cid)
                    comprobar("Restauracion: politica de reinicio original",
                              motor("inspect", "--format", "{{.HostConfig.RestartPolicy.Name}}", cid) == "unless-stopped")
            else:
                docker("up", "-d", "--no-deps", "valkey")
            esperar_sano()
            c, r = gw("POST", f"/tareas/{tid}/liberar", sesion)
            comprobar("Restauracion: liberar tarea de prueba", c == 200, str(r) if c != 200 else "HTTP 200")
            LOG.info("Estado final: servicios operativos; entidades de prueba conservadas para trazabilidad")
        except Exception:
            LOG.exception("Restauracion incompleta: revisar Valkey y tarea %s", tid)
            if not fallo_previo:
                raise


def main():
    EVIDENCE.parent.mkdir(parents=True, exist_ok=True)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s",
                        handlers=[logging.StreamHandler(sys.stdout), logging.FileHandler(EVIDENCE, mode="w", encoding="utf-8")], force=True)
    codigo = 0
    try:
        principal()
    except Exception:
        LOG.exception("P8 Valkey FALLIDA")
        codigo = 1
    finally:
        LOG.info("Metricas observadas: %s", json.dumps(METRICAS, ensure_ascii=False))
        for ruta, muestras in LATENCIAS.items():
            orden = sorted(muestras)
            p = [orden[math.ceil(x*len(orden))-1] for x in (0.5, 0.95, 0.99)]
            LOG.info("Latencia HTTP %s: n=%d; p50=%.3f ms; p95=%.3f ms; p99=%.3f ms; muestras=%s (rango mas cercano)", ruta, len(orden), *p, muestras)
        LOG.info("Limitaciones: un ensayo por escenario; RTO incluye comandos Docker y sondeo (B incluye parada controlada); RPO compara versiones de borrador, no garantiza perdida maxima ni durabilidad de todo el historial. SIGKILL no equivale a perdida de energia del host. Latencias incluyen respuestas HTTP de error, no fallas sin respuesta.")
        LOG.info("Resultado: %d/%d comprobaciones ejecutadas correctas; P8=%s; exit=%d", sum(RESULTADOS), len(RESULTADOS), "PASS" if codigo == 0 else "FAIL", codigo)
        LOG.info("Fin UTC: %s", datetime.now(timezone.utc).isoformat())
        LOG.info("Evidencia: %s", EVIDENCE)
    return codigo


if __name__ == "__main__":
    sys.exit(main())
