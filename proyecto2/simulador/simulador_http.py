"""Simulador HTTP concurrente contra el gateway (roles: anotador, revisor, consumidor).

Uso:
  python3 proyecto2/simulador/simulador_http.py \
    --base-url http://localhost:8000 \
    --proyecto-id <UUID_PROYECTO> \
    --anotadores <ID1>,<ID2> \
    --revisores <REV1> \
    --usuarios 10 \
    --duracion-s 15 \
    --semilla 20261122
"""

from __future__ import annotations

import argparse
import json
import random
import threading
import time
import urllib.error
import urllib.request
from collections import Counter, defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone


class MetricasSimulador:
    def __init__(self) -> None:
        self._lock = threading.Lock()
        self.conteo_rutas: Counter[str] = Counter()
        self.conteo_codigos: Counter[str] = Counter()
        self.latencias_ms: dict[str, list[float]] = defaultdict(list)
        self.analitica_latencias_ms: list[float] = []
        self.analitica_codigos: Counter[str] = Counter()
        self.muestras_409_siguiente: list[object] = []
        self.acciones: Counter[str] = Counter()
        self.tareas_revisadas_ids: list[str] = []

    def registrar_http(
        self,
        clave_ruta: str,
        codigo: int,
        ms: float,
        cuerpo: object | None = None,
    ) -> None:
        with self._lock:
            self.conteo_rutas[clave_ruta] += 1
            self.conteo_codigos[f"{clave_ruta}:{codigo}"] += 1
            if clave_ruta == "GET /analitica/{q}":
                self.analitica_latencias_ms.append(ms)
                self.analitica_codigos[str(codigo)] += 1
            else:
                self.latencias_ms[clave_ruta].append(ms)
            if clave_ruta == "POST /tareas/siguiente" and codigo == 409 and len(self.muestras_409_siguiente) < 5:
                self.muestras_409_siguiente.append(cuerpo)

    @staticmethod
    def _percentiles(vals: list[float]) -> dict[str, float]:
        orden = sorted(vals)
        n = len(orden)
        def idx(p: float) -> int:
            return min(n - 1, max(0, int(round((p / 100.0) * (n - 1)))))
        return {
            "muestras": n,
            "prom_ms": round(sum(orden) / n, 2),
            "p50_ms": round(orden[idx(50)], 2),
            "p95_ms": round(orden[idx(95)], 2),
            "p99_ms": round(orden[idx(99)], 2),
        }

    def resumen(self) -> dict:
        with self._lock:
            latencias = {
                ruta: self._percentiles(vals)
                for ruta, vals in self.latencias_ms.items()
                if vals
            }
            analitica_resumen = (
                self._percentiles(self.analitica_latencias_ms)
                if self.analitica_latencias_ms
                else {"muestras": 0}
            )
            analitica_resumen["codigos_http"] = dict(self.analitica_codigos)
            return {
                "acciones": dict(self.acciones),
                "codigos_http": dict(self.conteo_codigos),
                "latencias_ms": latencias,
                "analitica": analitica_resumen,
                "muestras_409_siguiente": list(self.muestras_409_siguiente),
            }

    def registrar_accion(self, nombre: str, tarea_id: str | None = None) -> None:
        with self._lock:
            self.acciones[nombre] += 1
            if tarea_id and tarea_id not in self.tareas_revisadas_ids:
                self.tareas_revisadas_ids.append(tarea_id)

    def pop_tarea_para_revisar(self) -> str | None:
        with self._lock:
            return self.tareas_revisadas_ids.pop(0) if self.tareas_revisadas_ids else None
        
def peticion_json(
    base_url: str,
    metodo: str,
    ruta: str,
    cuerpo: dict | None = None,
    timeout_s: float = 20.0,
) -> tuple[int, dict | list | str | None, float]:
    url = base_url.rstrip("/") + ruta
    datos = json.dumps(cuerpo).encode("utf-8") if cuerpo is not None else None
    req = urllib.request.Request(
        url,
        data=datos,
        method=metodo,
        headers={"Content-Type": "application/json"},
    )
    t0 = time.perf_counter()
    try:
        with urllib.request.urlopen(req, timeout=timeout_s) as resp:
            ms = (time.perf_counter() - t0) * 1000.0
            crudo = resp.read()
            cuerpo_resp = json.loads(crudo.decode("utf-8")) if crudo else None
            return resp.status, cuerpo_resp, ms
    except (urllib.error.URLError, TimeoutError) as exc:
        ms = (time.perf_counter() - t0) * 1000.0
        return 504, {"error": "timeout_o_conexion", "detalle": str(exc)}, ms
        try:
            cuerpo_err = json.loads(crudo.decode("utf-8")) if crudo else None
        except ValueError:
            cuerpo_err = crudo.decode("utf-8", errors="replace")
        return exc.code, cuerpo_err, ms


def worker_anotador(
    idx: int,
    base_url: str,
    proyecto_id: str,
    anotador_id: str,
    fin_ts: float,
    semilla: int,
    metricas: MetricasSimulador,
) -> None:
    rng = random.Random(semilla + idx * 101)
    while time.perf_counter() < fin_ts:
        cod, datos, ms = peticion_json(
            base_url,
            "POST",
            "/tareas/siguiente",
            {"anotador_id": anotador_id, "proyecto_id": proyecto_id},
        )
        metricas.registrar_http("POST /tareas/siguiente", cod, ms, cuerpo=datos)

        if cod == 204 or not isinstance(datos, dict) or "tarea" not in datos:
            time.sleep(0.25)
            continue
        if cod != 200:
            time.sleep(0.15)
            continue

        tarea_obj = datos["tarea"]
        tarea_id = tarea_obj["_id"]
        token = datos["token"]
        metricas.registrar_accion("tarea_tomada")

        # Borrador cada ~2s (acotado para no exceder fin_ts)
        n_borradores = rng.randint(1, 2)
        for paso in range(n_borradores):
            restante = fin_ts - time.perf_counter()
            if restante <= 0.1:
                break
            time.sleep(min(2.0, max(0.05, restante * 0.4)))
            cod_b, _, ms_b = peticion_json(
                base_url,
                "PUT",
                f"/tareas/{tarea_id}/borrador",
                {
                    "anotador_id": anotador_id,
                    "proyecto_id": proyecto_id,
                    "token": token,
                    "contenido": {"paso": paso + 1, "etiqueta": "objeto#principal"},
                },
            )
            metricas.registrar_http("PUT /tareas/{id}/borrador", cod_b, ms_b)
            if cod_b == 200:
                metricas.registrar_accion("borrador_guardado")

        # ~15% de liberaciones, ~85% de envíos
        if rng.random() < 0.15:
            cod_l, _, ms_l = peticion_json(
                base_url,
                "POST",
                f"/tareas/{tarea_id}/liberar",
                {"anotador_id": anotador_id, "proyecto_id": proyecto_id, "token": token},
            )
            metricas.registrar_http("POST /tareas/{id}/liberar", cod_l, ms_l)
            if cod_l == 200:
                metricas.registrar_accion("tarea_liberada")
        else:
            cod_e, _, ms_e = peticion_json(
                base_url,
                "POST",
                f"/tareas/{tarea_id}/enviar",
                {
                    "anotador_id": anotador_id,
                    "proyecto_id": proyecto_id,
                    "token": token,
                    "contenido": {"etiqueta": "objeto#principal", "confianza": 0.92},
                },
            )
            metricas.registrar_http("POST /tareas/{id}/enviar", cod_e, ms_e)
            if cod_e == 200:
                metricas.registrar_accion("tarea_enviada", tarea_id=tarea_id)


def worker_revisor(
    idx: int,
    base_url: str,
    revisor_id: str,
    fin_ts: float,
    semilla: int,
    metricas: MetricasSimulador,
) -> None:
    rng = random.Random(semilla + 5000 + idx * 37)
    while time.perf_counter() < fin_ts:
        tarea_id = metricas.pop_tarea_para_revisar()
        if not tarea_id:
            time.sleep(0.2)
            continue

        rechazar = rng.random() < 0.25
        cuerpo = (
            {
                "revisor_id": revisor_id,
                "decision": "rechazar",
                "motivo": "Geometría incompleta en simulación",
            }
            if rechazar
            else {"revisor_id": revisor_id, "decision": "aprobar"}
        )
        cod, _, ms = peticion_json(
            base_url,
            "POST",
            f"/tareas/{tarea_id}/revision",
            cuerpo,
        )
        metricas.registrar_http("POST /tareas/{id}/revision", cod, ms)
        if cod == 200:
            metricas.registrar_accion("tarea_rechazada" if rechazar else "tarea_aprobada")
        time.sleep(0.1)


def worker_consumidor(
    idx: int,
    base_url: str,
    proyecto_id: str,
    fin_ts: float,
    semilla: int,
    metricas: MetricasSimulador,
) -> None:
    rng = random.Random(semilla + 9000 + idx * 19)
    consultas = ["q1", "q2", "q3", "q4", "q5"]
    analitica_consultada = False
    while time.perf_counter() < fin_ts:
        cod_ex, _, ms_ex = peticion_json(
            base_url,
            "GET",
            f"/proyectos/{proyecto_id}/exportar?limite=20",
        )
        metricas.registrar_http("GET /proyectos/{id}/exportar", cod_ex, ms_ex)
        if cod_ex == 200:
            metricas.registrar_accion("exportar_ok")

        if not analitica_consultada:
            analitica_consultada = True
            q = rng.choice(consultas)
            cod_q, _, ms_q = peticion_json(base_url, "GET", f"/analitica/{q}", timeout_s=120.0)
            metricas.registrar_http("GET /analitica/{q}", cod_q, ms_q)
            if cod_q == 200:
                metricas.registrar_accion("analitica_ok")

        time.sleep(0.5)


def main() -> None:
    parser = argparse.ArgumentParser(description="Simulador HTTP contra el gateway")
    parser.add_argument("--base-url", default="http://localhost:8000")
    parser.add_argument("--proyecto-id", required=True)
    parser.add_argument("--anotadores", required=True, help="IDs separados por coma")
    parser.add_argument("--revisores", required=True, help="IDs separados por coma")
    parser.add_argument("--usuarios", type=int, default=6)
    parser.add_argument("--duracion-s", type=float, default=10.0)
    parser.add_argument("--semilla", type=int, default=20261122)
    args = parser.parse_args()

    anotadores = [x.strip() for x in args.anotadores.split(",") if x.strip()]
    revisores = [x.strip() for x in args.revisores.split(",") if x.strip()]
    if not anotadores or not revisores:
        raise SystemExit("Debe indicar al menos un anotador y un revisor.")

    metricas = MetricasSimulador()
    fin_ts = time.perf_counter() + args.duracion_s
    t_inicio_utc = datetime.now(timezone.utc).isoformat()

    futures = []
    with ThreadPoolExecutor(max_workers=max(3, args.usuarios)) as pool:
        for u in range(args.usuarios):
            rol = u % 10
            if rol < 6:
                ant = anotadores[u % len(anotadores)]
                futures.append(
                    pool.submit(
                        worker_anotador,
                        u,
                        args.base_url,
                        args.proyecto_id,
                        ant,
                        fin_ts,
                        args.semilla,
                        metricas,
                    )
                )
            elif rol < 8:
                rev = revisores[u % len(revisores)]
                futures.append(
                    pool.submit(
                        worker_revisor,
                        u,
                        args.base_url,
                        rev,
                        fin_ts,
                        args.semilla,
                        metricas,
                    )
                )
            else:
                futures.append(
                    pool.submit(
                        worker_consumidor,
                        u,
                        args.base_url,
                        args.proyecto_id,
                        fin_ts,
                        args.semilla,
                        metricas,
                    )
                )

        for fut in as_completed(futures):
            fut.result()

    salida = {
        "inicio_utc": t_inicio_utc,
        "fin_utc": datetime.now(timezone.utc).isoformat(),
        "base_url": args.base_url,
        "proyecto_id": args.proyecto_id,
        "usuarios": args.usuarios,
        "duracion_s": args.duracion_s,
        "semilla": args.semilla,
        "resultados": metricas.resumen(),
    }
    print(json.dumps(salida, indent=2, ensure_ascii=False))


if __name__ == "__main__":
    main()
