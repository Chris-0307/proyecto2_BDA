"""Prueba P5: Analítica Spark (1ra vs siguientes ejecuciones, 1 vs 2 workers y explain Q1/Q3)."""

from __future__ import annotations

import json
import os
import subprocess
import time
from datetime import datetime, timezone

COMPOSE_FILE = os.getenv("COMPOSE_FILE", "proyecto2/docker-compose.yml")


def consultar(ruta: str, timeout_s: float = 180.0) -> tuple[dict, float]:
    codigo_remoto = f"""
import json, time, urllib.request
t0 = time.perf_counter()
with urllib.request.urlopen("http://localhost:8004{ruta}", timeout={timeout_s}) as resp:
    data = json.loads(resp.read().decode("utf-8"))
ms = round((time.perf_counter() - t0) * 1000.0, 2)
print(json.dumps({{"data": data, "wall_ms": ms}}))
"""
    t_host0 = time.perf_counter()
    proc = subprocess.run(
        [
            "docker",
            "compose",
            "-f",
            COMPOSE_FILE,
            "exec",
            "-T",
            "analitica",
            "python",
            "-c",
            codigo_remoto,
        ],
        capture_output=True,
        text=True,
        timeout=timeout_s + 15.0,
        check=True,
    )
    ultima_linea = proc.stdout.strip().splitlines()[-1]
    envuelto = json.loads(ultima_linea)
    wall_ms = envuelto.get("wall_ms", round((time.perf_counter() - t_host0) * 1000.0, 2))
    return envuelto["data"], wall_ms


def set_workers(workers: int) -> None:
    env = os.environ.copy()
    env["SPARK_WORKERS"] = str(workers)
    subprocess.run(
        ["docker", "compose", "-f", COMPOSE_FILE, "up", "-d", "--force-recreate", "analitica"],
        env=env,
        check=True,
    )
    limite = time.perf_counter() + 60.0
    while time.perf_counter() < limite:
        try:
            salud, _ = consultar("/salud", timeout_s=10.0)
            if salud.get("estado") == "operativo":
                return
        except Exception:
            time.sleep(1.0)
    raise RuntimeError(f"analitica no respondió operativo con SPARK_WORKERS={workers}")


def main() -> None:
    print(f"=== P5 Analítica Spark — Inicio UTC: {datetime.now(timezone.utc).isoformat()} ===")
    reporte: dict = {"workers": {}}

    for w in (1, 2):
        print(f"\n--- Configurando SPARK_WORKERS={w} ---")
        set_workers(w)
        salud, _ = consultar("/salud")
        print("Salud:", json.dumps(salud))

        datos_w: dict = {"salud": salud, "consultas": {}}
        for q in ("q1", "q2", "q3", "q4", "q5"):
            incluir_plan = "true" if (w == 1 and q in ("q1", "q3")) else "false"
            r1, wall1 = consultar(f"/q/{q}?conjunto=principal&incluir_plan={incluir_plan}")
            r2, wall2 = consultar(f"/q/{q}?conjunto=principal&incluir_plan=false")
            datos_w["consultas"][q] = {
                "primera_ejecucion_wall_ms": wall1,
                "primera_ejecucion_srv_ms": r1.get("duracion_ms"),
                "segunda_ejecucion_wall_ms": wall2,
                "segunda_ejecucion_srv_ms": r2.get("duracion_ms"),
                "total_filas": r1.get("total_filas"),
            }
            if "plan_explain" in r1:
                datos_w["consultas"][q]["plan_explain"] = r1["plan_explain"]

            print(
                f"{q.upper()} (w={w}): 1ra={wall1} ms (srv={r1.get('duracion_ms')} ms) | "
                f"2da={wall2} ms (srv={r2.get('duracion_ms')} ms) | filas={r1.get('total_filas')}"
            )

        reporte["workers"][str(w)] = datos_w

    print("\n--- Restaurando SPARK_WORKERS=1 ---")
    set_workers(1)

    print("\n=== Resumen JSON P5 ===")
    print(json.dumps(reporte, indent=2, ensure_ascii=False))
    print(f"=== Fin UTC: {datetime.now(timezone.utc).isoformat()} ===")


if __name__ == "__main__":
    main()
