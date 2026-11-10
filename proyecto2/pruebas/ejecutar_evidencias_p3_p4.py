"""Ejecuta tests existentes y P3/P4 reales; conserva logs y resumen en evidence/.

No genera métricas que no se midieron. Los SKIPPED quedan reflejados
explícitamente en la salida de pytest y no equivalen a PASSED.
"""

import json
import os
import platform
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path
from uuid import uuid4

PROYECTO = Path(__file__).resolve().parents[1]
REPO = PROYECTO.parent
EVIDENCIA = PROYECTO / "evidence"


def principal():
    EVIDENCIA.mkdir(parents=True, exist_ok=True)
    identificador = datetime.now(timezone.utc).strftime("p3_p4_%Y%m%dT%H%M%SZ_") + uuid4().hex[:8]
    pruebas = [
        "proyecto2/pruebas/test_sesion_locks.py",
        "proyecto2/pruebas/test_sesion_borradores.py",
        "proyecto2/pruebas/test_sesion_recuperacion.py",
        "proyecto2/pruebas/test_historial_eventos.py",
        "proyecto2/pruebas/test_historial_parquet.py",
        "proyecto2/pruebas/test_historial_reprocesamiento.py",
        "proyecto2/pruebas/test_p3_p4_integracion.py",
    ]
    comando = [sys.executable, "-m", "pytest", *pruebas, "-v", "-ra"]
    inicio = datetime.now(timezone.utc)
    proceso = subprocess.run(comando, cwd=REPO, capture_output=True, text=True,
                             check=False, env=os.environ.copy())
    fin = datetime.now(timezone.utc)
    txt = EVIDENCIA / f"{identificador}.txt"
    txt.write_text(
        f"Inicio UTC: {inicio.isoformat()}\nFin UTC: {fin.isoformat()}\n"
        f"Comando: {' '.join(comando)}\nExit code: {proceso.returncode}\n\n"
        + proceso.stdout + "\n" + proceso.stderr,
        encoding="utf-8",
    )
    resumen = {
        "objetivo": "Ejecutar suite P3/P4 y conservar evidencia original",
        "inicio_utc": inicio.isoformat(), "fin_utc": fin.isoformat(),
        "comando": comando, "codigo_salida": proceso.returncode,
        "resultado": "pytest_sin_errores" if proceso.returncode == 0 else "fallo",
        "maquina": {"plataforma": platform.platform(), "cpu_logicos": os.cpu_count()},
        "valkey_real_configurado": bool(os.environ.get("VALKEY_TEST_URL")),
        "evidencia_txt": txt.name,
        "nota": "Revisar pruebas SKIPPED en el log; no son aprobadas. No se midieron latencias de gateway ni Spark.",
        "metricas_p50_p95_p99": "no_disponibles; requieren muestras instrumentadas de la integración final",
    }
    (EVIDENCIA / f"{identificador}.json").write_text(
        json.dumps(resumen, indent=2, ensure_ascii=False) + "\n", encoding="utf-8"
    )
    print(f"Resumen y salida original en: {EVIDENCIA / identificador}(.json/.txt)")
    sys.exit(proceso.returncode)


if __name__ == "__main__":
    principal()
