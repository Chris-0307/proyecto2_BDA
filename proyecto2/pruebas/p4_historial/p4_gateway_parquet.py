"""P4 integrada sobre el Compose final. Ejecutar desde el host.

Solo las solicitudes de usuario pasan por el gateway. La administracion de
pruebas usa docker compose exec; Spark conserva su montaje de solo lectura.
El reprocesamiento y la escritura interrumpida se prueban en una ruta y
llaves aisladas. Los eventos reales del flujo quedan para trazabilidad.
"""
import json
import logging
import math
import os
import platform
import queue
import shutil
import subprocess
import sys
import threading
import time
import urllib.error
import urllib.request
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from uuid import uuid4

ROOT = Path(__file__).resolve().parents[3]
COMPOSE = ["docker", "compose", "-f", "proyecto2/docker-compose.yml"]
GATEWAY = os.getenv("GATEWAY_URL", "http://localhost:8000")
EVIDENCE = ROOT / "proyecto2/evidence/p4_gateway_parquet.txt"
PREFIX = "P4_JSON "
log = logging.getLogger("P4")
resultados = []
latencias = {}
frescura = []

# Se ejecuta en historial-worker, con sus dependencias reales. Las llaves
# se cambian SOLO dentro de este proceso y se usa la DB 15 de pruebas.
FIXTURE = r'''
import json, sys, time
from datetime import datetime, timezone
from pathlib import Path
from uuid import UUID, uuid4
from redis import Redis
from app import consumer, eventos, parquet_writer

base, modo, a_id, b_id, sufijo = sys.argv[1:]
base = Path(base)
base.mkdir(parents=True, exist_ok=True)
escritor = parquet_writer.EscritorParquet(base)

def evento(eid):
    return eventos.EventoNegocio(event_id=UUID(eid), tipo_evento="tarea_anotada",
        estado="anotada", ocurrido_en=datetime.now(timezone.utc),
        tarea_id=UUID(a_id), proyecto_id=UUID(b_id), cliente_id=UUID(a_id),
        modalidad="imagen", tipo_tarea="bbox", version=1, duracion_ms=100)

if modo == "replay":
    a = evento(a_id)
    (base / "evento-a.json").write_text(a.model_dump_json(), encoding="utf-8")
    stream = "p4:test:stream:" + sufijo
    ids = "p4:test:ids:" + sufijo
    grupo = "p4-" + sufijo
    consumer.STREAM_KEY = eventos.STREAM_KEY = stream
    eventos.IDS_KEY = ids
    r = Redis.from_url("redis://valkey:6379/15", decode_responses=True)
    try:
        primero = eventos.publicar_evento(r, a)
        repetido = eventos.publicar_evento(r, a)
        assert primero.resultado == "encolado" and repetido.resultado == "repetido"
        assert primero.stream_id == repetido.stream_id and r.xlen(stream) == 1
        def falla(lote):
            escritor.publicar(lote)
            raise OSError("P4: falla simulada antes de XACK")
        try:
            consumer.procesar_lote(r, falla, consumidor="anterior", grupo=grupo)
        except OSError as e:
            assert str(e) == "P4: falla simulada antes de XACK"
        else:
            raise AssertionError("No se simulo la falla")
        pendientes_antes = r.xpending(stream, grupo)["pending"]
        antes = sorted(base.glob("mes=*/*.parquet"))
        assert pendientes_antes == 1 and len(antes) == 1
        time.sleep(0.1)
        n = consumer.procesar_lote(r, escritor.publicar, consumidor="nuevo",
            grupo=grupo, recuperar_pendientes=True, min_idle_ms=20)
        despues = sorted(base.glob("mes=*/*.parquet"))
        pendientes_despues = r.xpending(stream, grupo)["pending"]
        filas = parquet_writer.pq.ParquetFile(despues[0]).metadata.num_rows
        assert n == 1 and antes == despues and pendientes_despues == 0 and filas == 1
        print("P4_JSON " + json.dumps(dict(pendientes_antes=pendientes_antes,
            pendientes_despues=pendientes_despues, recuperados=n,
            archivos_antes=len(antes), archivos_despues=len(despues), filas=filas,
            reintento_ingreso=repetido.resultado, archivo=str(despues[0]))), flush=True)
    finally:
        r.delete(stream, ids)
        r.close()
else:
    a = eventos.EventoNegocio.model_validate_json((base / "evento-a.json").read_text())
    b = evento(b_id).model_copy(update={"ocurrido_en": a.ocurrido_en})
    original = parquet_writer.pq.write_table
    def escritura_observada(tabla, destino, *args, **kwargs):
        # Simula un archivo aun incompleto en la ruta temporal REAL del writer.
        Path(destino).write_bytes(b"PAR1-incompleto")
        (base / "ready").write_text(str(destino), encoding="utf-8")
        limite = time.monotonic() + 180
        while not (base / "release").exists():
            if time.monotonic() > limite:
                raise TimeoutError("No se libero la escritura de prueba")
            time.sleep(0.1)
        return original(tabla, destino, *args, **kwargs)
    parquet_writer.pq.write_table = escritura_observada
    try:
        rutas = escritor.publicar([a, b])
        assert len(rutas) == 1
        assert not list((base / ".staging").glob("*.tmp"))
        print("P4_JSON " + json.dumps({"archivo_publicado": str(rutas[0]),
            "filas": parquet_writer.pq.ParquetFile(rutas[0]).metadata.num_rows}), flush=True)
    finally:
        parquet_writer.pq.write_table = original
'''

# Una sola JVM de prueba, precalentada antes del flujo HTTP. Se usa el
# mismo lector de produccion, con conjunto grande (sin el filtro de 2025).
# No se escriben archivos desde el contenedor de analitica.
SPARK = r'''
import json, sys, traceback
from pathlib import Path
from app.main import obtener_spark
from app.consultas import df_eventos_limpios
from pyspark.sql import functions as F
spark = obtener_spark()
spark.range(1).count()
print("P4_JSON " + json.dumps({"ready": True, "spark": spark.version,
    "master": spark.sparkContext.master}), flush=True)
try:
    for linea in sys.stdin:
        try:
            req = json.loads(linea)
            if req.get("stop"):
                break
            ruta = req["ruta"]
            archivos = list(Path(ruta).glob("mes=*/*.parquet"))
            if not archivos:
                res = {"filas": [], "archivos": 0}
            else:
                limpio = df_eventos_limpios(spark, ruta, conjunto="grande")
                if req["modo"] == "fixture":
                    raw = spark.read.parquet(ruta.rstrip("/") + "/mes=*/*.parquet")
                    res = {"raw": raw.count(), "unicos": limpio.count(),
                           "ids": sorted(x.event_id for x in limpio.select("event_id").collect()),
                           "temporales": len(list((Path(ruta) / ".staging").glob("*.tmp"))),
                           "archivos": len(archivos)}
                else:
                    filas = (limpio.filter(F.col("tarea_id") == req["tarea"])
                        .select("event_id", "tipo_evento", "estado", "version", "motivo_rechazo")
                        .orderBy("version").collect())
                    res = {"filas": [x.asDict() for x in filas], "archivos": len(archivos)}
            print("P4_JSON " + json.dumps(res), flush=True)
        except Exception:
            print("P4_JSON " + json.dumps({"error": traceback.format_exc()}), flush=True)
finally:
    spark.stop()
'''


def docker(*args, timeout=90):
    cmd = [*COMPOSE, *args]
    log.info("Comando Docker: %s", " ".join(cmd[:8]) + (" [codigo Python]" if "-c" in cmd else ""))
    p = subprocess.run(cmd, cwd=ROOT, capture_output=True, text=True,
                       encoding="utf-8", errors="replace", timeout=timeout)
    if p.stderr.strip():
        log.info("Docker stderr: %s", p.stderr.strip())
    if p.returncode:
        raise RuntimeError(f"Docker exit={p.returncode}: {p.stdout} {p.stderr}")
    return p.stdout.strip()


def extraer(texto):
    return json.loads(next(linea[len(PREFIX):] for linea in reversed(texto.splitlines())
                           if linea.startswith(PREFIX)))


def comprobar(nombre, condicion, detalle=""):
    resultados.append(bool(condicion))
    log.info("[%s] %s %s", "OK" if condicion else "FALLO", nombre, detalle)
    if not condicion:
        raise AssertionError(f"{nombre}: {detalle}")


def gw(metodo, ruta, cuerpo=None):
    req = urllib.request.Request(GATEWAY + ruta,
        data=json.dumps(cuerpo).encode() if cuerpo is not None else None,
        method=metodo, headers={"Content-Type": "application/json"})
    inicio = time.perf_counter()
    try:
        with urllib.request.urlopen(req, timeout=30) as r:
            codigo, raw = r.status, r.read()
    except urllib.error.HTTPError as e:
        codigo, raw = e.code, e.read()
    instante = time.perf_counter()
    clave = metodo + " " + ruta
    if ruta.startswith("/tareas/") and ruta.count("/") == 3:
        clave = metodo + " /tareas/{id}/" + ruta.split("/")[-1]
    elif ruta.startswith("/proyectos/") and ruta.count("/") == 3:
        clave = metodo + " /proyectos/{id}/" + ruta.split("/")[-1]
    ms = (instante - inicio) * 1000
    latencias.setdefault(clave, []).append(ms)
    log.info("%s %s -> HTTP %s (%.2f ms)", metodo, ruta, codigo, ms)
    return codigo, json.loads(raw) if raw else None, instante


def esperado(metodo, ruta, cuerpo, codigo):
    c, r, t = gw(metodo, ruta, cuerpo)
    comprobar(f"{metodo} {ruta} responde {codigo}", c == codigo, f"HTTP {c}: {r}" if c != codigo else "")
    return r, t


class LectorSpark:
    def __init__(self):
        self.proc = None
        self.cola = queue.Queue()

    def __enter__(self):
        self.proc = subprocess.Popen([*COMPOSE, "exec", "-T", "analitica", "python", "-u", "-c", SPARK],
            cwd=ROOT, stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
            text=True, encoding="utf-8", errors="replace", bufsize=1)
        def leer():
            for linea in self.proc.stdout:
                linea = linea.strip()
                if linea.startswith(PREFIX):
                    self.cola.put(json.loads(linea[len(PREFIX):]))
                elif linea:
                    log.info("Spark: %s", linea)
            self.cola.put({"error": "Proceso Spark finalizado"})
        threading.Thread(target=leer, daemon=True).start()
        try:
            listo = self.recibir(120)
            comprobar("Spark inicializado y precalentado", listo.get("ready"), str(listo))
            return self
        except BaseException:
            self.__exit__(None, None, None)
            raise

    def recibir(self, timeout):
        try:
            r = self.cola.get(timeout=timeout)
        except queue.Empty:
            raise TimeoutError("Spark no respondio dentro del plazo") from None
        if "error" in r:
            raise RuntimeError(r["error"])
        return r

    def consultar(self, **req):
        self.proc.stdin.write(json.dumps(req) + "\n")
        self.proc.stdin.flush()
        return self.recibir(120)

    def __exit__(self, *_):
        if self.proc is None:
            return
        try:
            if self.proc.poll() is None:
                self.proc.stdin.write('{"stop": true}\n')
                self.proc.stdin.flush()
                self.proc.stdin.close()
                self.proc.wait(timeout=20)
        except (OSError, subprocess.TimeoutExpired):
            self.proc.kill()
            self.proc.wait()


def fixture(spark):
    sufijo, a, b = uuid4().hex, str(uuid4()), str(uuid4())
    ruta = "/datalake/p4-fixture-" + sufijo
    host = ROOT / "proyecto2" / "datalake" / ("p4-fixture-" + sufijo)
    args = ["exec", "-T", "historial-worker", "python", "-u", "-c", FIXTURE]
    proceso = None
    try:
        replay = extraer(docker(*args, ruta, "replay", a, b, sufijo))
        comprobar("Reingreso idempotente, XAUTOCLAIM y XACK tras publicar", 
            replay["reintento_ingreso"] == "repetido" and replay["pendientes_antes"] == 1
            and replay["pendientes_despues"] == 0 and replay["recuperados"] == 1
            and replay["archivos_antes"] == replay["archivos_despues"] == replay["filas"] == 1,
            json.dumps(replay))
        proceso = subprocess.Popen([*COMPOSE, *args, ruta, "stage", a, b, sufijo],
            cwd=ROOT, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
            text=True, encoding="utf-8", errors="replace")
        limite = time.monotonic() + 30
        while not (host / "ready").exists():
            if proceso.poll() is not None:
                raise RuntimeError(proceso.communicate()[0])
            if time.monotonic() > limite:
                raise TimeoutError("El writer no creo el temporal de prueba")
            time.sleep(0.1)
        durante = spark.consultar(modo="fixture", ruta=ruta)
        comprobar("Spark excluye temporal incompleto mientras el writer esta pausado",
            durante["temporales"] == 1 and durante["archivos"] == 1
            and durante["raw"] == durante["unicos"] == 1 and durante["ids"] == [a],
            json.dumps(durante))
        (host / "release").touch()
        salida, _ = proceso.communicate(timeout=60)
        if proceso.returncode:
            raise RuntimeError(salida)
        log.info("Publicacion del segundo lote: %s", extraer(salida))
        final = spark.consultar(modo="fixture", ruta=ruta)
        comprobar("Dos lotes solapados: tres filas fisicas, dos event_id unicos",
            final["temporales"] == 0 and final["archivos"] == 2 and final["raw"] == 3
            and final["unicos"] == 2 and final["ids"] == sorted([a, b]), json.dumps(final))
    finally:
        if proceso is not None and proceso.poll() is None:
            if host.exists():
                (host / "release").touch()
            try:
                proceso.communicate(timeout=30)
            except subprocess.TimeoutExpired:
                proceso.kill()
                proceso.communicate()
        # Se elimina exclusivamente el directorio UUID fuera de eventos/.
        if host.exists():
            shutil.rmtree(host)
        log.info("Restauracion fixture: directorio aislado eliminado=%s", not host.exists())


def preparar():
    codigo = ("import sys,json; sys.path.insert(0,'/pruebas/p1_gateway'); "
              "from smoke_flujo import preparar; print(json.dumps(preparar()))")
    cliente, ids = json.loads(docker("exec", "-T", "registro", "python", "-c", codigo))
    proyecto, _ = esperado("POST", "/proyectos", {"cliente_id": cliente, "nombre": "P4-" + cliente,
        "taxonomia": [{"etiqueta": "auto", "instruccion": "Vehiculo"}]}, 201)
    pid = proyecto["_id"]
    lote, _ = esperado("POST", f"/proyectos/{pid}/lotes", {"nombre": "p4-tarea", "tareas": [
        {"modalidad": "imagen", "tipo_tarea": "bbox", "prioridad": 1, "archivo": {
            "uri": f"s3://p4/{cliente}", "sha256": "b"*64, "bytes": 10, "mime": "image/png"}}]}, 201)
    return pid, lote["tarea_ids"][0], ids


def observar(spark, tid, eid, t0, tipo, estado, motivo=None):
    limite = t0 + 120
    intentos = 0
    while True:
        r = spark.consultar(modo="tarea", ruta="/datalake/eventos", tarea=tid)
        observado = time.perf_counter()
        intentos += 1
        filas = [f for f in r["filas"] if f["event_id"] == eid]
        if filas:
            demora = observado - t0
            comprobar(f"Spark observa {tipo} con estado {estado}", len(filas) == 1
                and filas[0]["tipo_evento"] == tipo and filas[0]["estado"] == estado
                and (motivo is None or filas[0]["motivo_rechazo"] == motivo), json.dumps(filas))
            log.info("Frescura HTTP 200 -> primera observacion Spark: tipo=%s event_id=%s segundos=%.3f intentos=%d archivos=%d",
                tipo, eid, demora, intentos, r["archivos"])
            if tipo == "tarea_anotada":
                frescura.append(demora)
            return r["filas"]
        if observado >= limite:
            raise TimeoutError(f"Evento {eid} no visible en Spark dentro del plazo")
        time.sleep(0.5)


def flujo(spark):
    pid, tid, ids = preparar()
    log.info("Proyecto=%s tarea=%s", pid, tid)
    datos = {"anotador_id": ids["a1"], "proyecto_id": pid}
    contenido = {"cajas": [[1, 2, 3, 4]], "etiqueta": "auto"}
    sesion = None
    try:
        for vuelta in (1, 2):
            s, _ = esperado("POST", "/tareas/siguiente", datos, 200)
            sesion = {**datos, "token": s["token"]}
            comprobar("Tarea adquirida", s["tarea"]["_id"] == tid)
            cuerpo = {**sesion, "contenido": contenido}
            esperado("PUT", f"/tareas/{tid}/borrador", cuerpo, 200)
            e, t0 = esperado("POST", f"/tareas/{tid}/enviar", cuerpo, 200)
            comprobar("MongoDB confirma anotada", e["tarea"]["estado"] == "anotada")
            sesion = None
            eid = e["tarea"]["ultimo_evento_id"]
            observar(spark, tid, eid, t0, "tarea_anotada", "anotada")
            esperado("POST", f"/tareas/{tid}/enviar", cuerpo, 409)
            decision = "rechazar" if vuelta == 1 else "aprobar"
            revision = {"revisor_id": ids["rev"], "decision": decision}
            if vuelta == 1:
                revision["motivo"] = "caja_incompleta"
            rr, t0 = esperado("POST", f"/tareas/{tid}/revision", revision, 200)
            estado_mongo = "disponible" if vuelta == 1 else "aprobada"
            comprobar(f"MongoDB devuelve {estado_mongo}", rr["tarea"]["estado"] == estado_mongo)
            filas = observar(spark, tid, rr["tarea"]["ultimo_evento_id"], t0,
                "revision_rechazada" if vuelta == 1 else "revision_aprobada",
                "rechazada" if vuelta == 1 else "aprobada", revision.get("motivo"))
        esperado_eventos = Counter({"tarea_abierta": 2, "tarea_anotada": 2,
            "revision_iniciada": 2, "revision_rechazada": 1, "revision_aprobada": 1})
        # Se espera el historial completo, incluso si llega fuera de orden.
        limite = time.monotonic() + 30
        while len(filas) < 8 and time.monotonic() < limite:
            time.sleep(0.5)
            filas = spark.consultar(modo="tarea", ruta="/datalake/eventos", tarea=tid)["filas"]
        conteo = Counter(f["tipo_evento"] for f in filas)
        comprobar("Ocho eventos de negocio; borradores y envios 409 no generan filas",
            conteo == esperado_eventos and len({f["event_id"] for f in filas}) == 8,
            json.dumps(filas))
    finally:
        if sesion:
            try:
                c, _, _ = gw("POST", f"/tareas/{tid}/liberar", sesion)
                log.info("Restauracion: liberar tarea de prueba HTTP %d", c)
            except Exception:
                log.exception("No se pudo liberar la tarea; revisar su estado")
        log.info("Se conservan entidades y eventos del flujo real para trazabilidad; no se borran datos previos.")


def percentiles(valores):
    orden = sorted(valores)
    return "n=%d; " % len(orden) + "; ".join(
        f"p{p}={orden[math.ceil(p*len(orden)/100)-1]:.3f}" for p in (50, 95, 99))


def main():
    EVIDENCE.parent.mkdir(parents=True, exist_ok=True)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s", handlers=[
        logging.StreamHandler(sys.stdout), logging.FileHandler(EVIDENCE, mode="w", encoding="utf-8")])
    log.info("Objetivo P4: materializacion, reprocesamiento, lectura atomica, deduplicacion y frescura")
    log.info("Inicio UTC: %s", datetime.now(timezone.utc).isoformat())
    log.info("Host: %s; CPU logicos=%s; Python=%s", platform.platform(), os.cpu_count(), platform.python_version())
    log.info("Comando reproducible: python proyecto2/pruebas/p4_historial/p4_gateway_parquet.py (make p4)")
    exit_code = 1
    try:
        log.info("Docker: %s", docker("version"))
        info = subprocess.check_output(["docker", "info", "--format", "{{.NCPU}} CPU; RAM={{.MemTotal}} bytes; Docker={{.ServerVersion}}"],
            cwd=ROOT, text=True, encoding="utf-8", errors="replace", timeout=30)
        log.info("Recursos Docker: %s", info.strip())
        log.info("Servicios:\n%s", docker("ps"))
        log.info("Configuracion: micro-lotes max 100; worker recuperacion 30000 ms; fixture usa grupo aislado y 20 ms")
        log.info("Frescura: JVM precalentada, lector de produccion sin filtro historico (conjunto grande); incluye lectura, sondeo y transporte; no es demora exacta de escritura")
        with LectorSpark() as spark:
            fixture(spark)
            flujo(spark)
        exit_code = 0
    except Exception:
        log.exception("P4 fallo")
    finally:
        for ruta, muestras in latencias.items():
            log.info("Latencia HTTP ms %s: %s; muestras=%s (rango mas cercano)", ruta, percentiles(muestras), muestras)
        if frescura:
            log.info("Frescura de envios (s): %s; muestras=%s (rango mas cercano)", percentiles(frescura), frescura)
        log.info("Limitaciones: fixture simula falla antes de XACK; no mata worker real. Pocas muestras; no es carga. Q1 no se midio; se observo event_id directo en Spark.")
        log.info("Resultado: %d/%d comprobaciones ejecutadas correctas; exit=%d", sum(resultados), len(resultados), exit_code)
        log.info("Fin UTC: %s", datetime.now(timezone.utc).isoformat())
        log.info("Evidencia: %s", EVIDENCE)
    return exit_code


if __name__ == "__main__":
    sys.exit(main())
