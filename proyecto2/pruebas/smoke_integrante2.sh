#!/usr/bin/env bash
# Prueba de humo aislada para los servicios del integrante 2.
# Necesita Docker Compose y Python 3 en el HOST (WSL/Linux).
# NO utiliza el gateway y NO equivale a la prueba integrada del proyecto.
# No ejecutar contra una instalación de producción.
set -Eeuo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROYECTO="$(cd "$SCRIPT_DIR/.." && pwd)"
COMPOSE=(docker compose -f "$PROYECTO/compose.integrante2.yml" -p bda-integrante2)

command -v docker >/dev/null || { echo 'ERROR: falta docker' >&2; exit 1; }
command -v python3 >/dev/null || { echo 'ERROR: falta python3' >&2; exit 1; }
mkdir -p "$PROYECTO/evidence"
LOG="$PROYECTO/evidence/smoke_integrante2_$(date -u +%Y%m%dT%H%M%SZ).txt"
exec > >(tee "$LOG") 2>&1

printf 'Prueba iniciada (UTC): %s\n' "$(date -u +%Y-%m-%dT%H:%M:%SZ)"
printf 'Log: %s\n' "$LOG"
printf 'Objetivo: validar P3 (borrador AOF) y P4 (recepcion a Parquet), entorno aislado.\n'
printf 'Configuracion: proyecto Compose bda-integrante2, cuatro servicios internos.\n'

ID_ANNOTATOR="$(python3 -c 'import uuid; print(uuid.uuid4())')"
ID_TASK="$(python3 -c 'import uuid; print(uuid.uuid4())')"
ID_PROJECT="$(python3 -c 'import uuid; print(uuid.uuid4())')"
TOKEN="$(python3 -c 'import uuid; print(uuid.uuid4())')"

limpiar_sesion() {
  # Solo claves de los UUID generados por este script; nunca FLUSHDB.
  "${COMPOSE[@]}" exec -T valkey valkey-cli DEL \
    "lock:tarea:$ID_TASK" "sesion:$ID_ANNOTATOR" "borrador:$ID_TASK" >/dev/null 2>&1 || true
  "${COMPOSE[@]}" exec -T valkey valkey-cli ZREM \
    "editando:$ID_PROJECT" "$ID_ANNOTATOR" >/dev/null 2>&1 || true
}
trap limpiar_sesion EXIT

printf '\n[1/5] Levantar el entorno de integración\n'
"${COMPOSE[@]}" config -q
"${COMPOSE[@]}" up -d --build
"${COMPOSE[@]}" ps

printf '\n[2/5] Valkey AOF y servicios disponibles\n'
aof=$("${COMPOSE[@]}" exec -T valkey valkey-cli --raw CONFIG GET appendonly | tail -n 1 | tr -d '\r')
fsync=$("${COMPOSE[@]}" exec -T valkey valkey-cli --raw CONFIG GET appendfsync | tail -n 1 | tr -d '\r')
printf 'Configuracion AOF: appendonly=%s; appendfsync=%s\n' "$aof" "$fsync"
[[ "$aof" == yes && "$fsync" == everysec ]] || {
  echo 'ERROR: Valkey no utiliza AOF everysec'; exit 1;
}
for servicio in sesion historial; do
  listo=0
  for intento in $(seq 1 30); do
    if "${COMPOSE[@]}" exec -T "$servicio" python -c \
      "import urllib.request; urllib.request.urlopen('http://127.0.0.1:$(test "$servicio" = sesion && echo 8001 || echo 8002)/salud', timeout=2)" \
      >/dev/null 2>&1; then
      listo=1
      break
    fi
    sleep 1
  done
  test "$listo" -eq 1 || { echo "ERROR: $servicio no responde /salud"; exit 1; }
  echo "Servicio $servicio: disponible"
done

printf '\n[3/5] P3: crear sesión y borrador a través de la API interna\n'
"${COMPOSE[@]}" exec -T \
  -e ID_ANNOTATOR="$ID_ANNOTATOR" -e ID_TASK="$ID_TASK" \
  -e ID_PROJECT="$ID_PROJECT" -e TOKEN="$TOKEN" sesion python - <<'PY'
import json, os, urllib.request
base = 'http://127.0.0.1:8001'
datos = dict(anotador_id=os.environ['ID_ANNOTATOR'], tarea_id=os.environ['ID_TASK'],
             proyecto_id=os.environ['ID_PROJECT'], token=os.environ['TOKEN'])
def enviar(ruta, cuerpo, metodo='POST'):
    payload = json.dumps(cuerpo).encode('utf-8')
    req = urllib.request.Request(base + ruta, data=payload, method=metodo,
                                 headers={'Content-Type': 'application/json'})
    with urllib.request.urlopen(req, timeout=5) as r:
        return r.status, json.load(r)
assert enviar('/sesiones/iniciar', {**datos, 'etapa': 'anotacion'})[0] == 201
status, respuesta = enviar(f"/tareas/{datos['tarea_id']}/borrador",
                          {**datos, 'contenido': {'avance': 42, 'modalidad': 'lidar'}}, 'PUT')
assert status == 200 and respuesta['version'] == 1
print('P3: borrador creado correctamente. ID de tarea:', datos['tarea_id'])
PY

# AOF everysec: esperar para que la sincronización periódica ocurra.
sleep 3
"${COMPOSE[@]}" restart valkey
"${COMPOSE[@]}" exec -T \
  -e ID_ANNOTATOR="$ID_ANNOTATOR" -e ID_TASK="$ID_TASK" \
  -e ID_PROJECT="$ID_PROJECT" -e TOKEN="$TOKEN" sesion python - <<'PY'
import json, os, time, urllib.request
base = 'http://127.0.0.1:8001'
datos = dict(anotador_id=os.environ['ID_ANNOTATOR'], tarea_id=os.environ['ID_TASK'],
             proyecto_id=os.environ['ID_PROJECT'], token=os.environ['TOKEN'])
for _ in range(40):
    try:
        req = urllib.request.Request(base + '/sesiones/recuperar',
            data=json.dumps(datos).encode('utf-8'), method='POST',
            headers={'Content-Type': 'application/json'})
        with urllib.request.urlopen(req, timeout=3) as r:
            respuesta = json.load(r)
        break
    except (OSError, TimeoutError):
        time.sleep(1)
else:
    raise RuntimeError('No se recuperó la sesión tras reiniciar Valkey')
assert respuesta['borrador'] == {'avance': 42, 'modalidad': 'lidar'}, respuesta
assert respuesta['estado'] in ('activa', 'requiere_reasignacion'), respuesta
print('P3: borrador recuperado tras reinicio; estado =', respuesta['estado'])
PY

printf '\n[4/5] P4: evento a través de Historial\n'
EVENT_ID="$(python3 -c 'import uuid; print(uuid.uuid4())')"
CLIENT_ID="$(python3 -c 'import uuid; print(uuid.uuid4())')"
"${COMPOSE[@]}" exec -T -e EVENT_ID="$EVENT_ID" \
  -e ID_TASK="$ID_TASK" -e ID_PROJECT="$ID_PROJECT" \
  -e CLIENT_ID="$CLIENT_ID" historial python - <<'PY'
import json, os, urllib.request
from datetime import datetime, timezone
payload = {
    'event_id': os.environ['EVENT_ID'], 'tipo_evento': 'revision_aprobada',
    'estado': 'aprobada', 'ocurrido_en': datetime.now(timezone.utc).isoformat(),
    'tarea_id': os.environ['ID_TASK'], 'proyecto_id': os.environ['ID_PROJECT'],
    'cliente_id': os.environ['CLIENT_ID'], 'modalidad': 'lidar',
    'tipo_tarea': 'cuboide_3d', 'version': 3, 'sla_horas': 48,
}
req = urllib.request.Request('http://127.0.0.1:8002/eventos',
    data=json.dumps(payload).encode('utf-8'), method='POST',
    headers={'Content-Type': 'application/json'})
with urllib.request.urlopen(req, timeout=10) as r:
    respuesta = json.load(r)
    assert r.status == 202 and respuesta['resultado'] == 'encolado', respuesta
print('P4: evento encolado', respuesta['stream_id'])
PY

printf '\n[5/5] P4: esperar archivo Parquet visible al worker\n'
"${COMPOSE[@]}" exec -T -e EVENT_ID="$EVENT_ID" historial-worker python - <<'PY'
import os, time
from pathlib import Path
import pyarrow.parquet as pq
identificador = os.environ['EVENT_ID']
base = Path('/datalake/eventos')
for _ in range(45):
    for archivo in base.glob('mes=*/*.parquet'):
        tabla = pq.ParquetFile(archivo).read(columns=['event_id', 'estado'])
        for fila in tabla.to_pylist():
            if fila['event_id'] == identificador:
                assert fila['estado'] == 'aprobada', fila
                print('P4: archivo publicado:', archivo, 'event_id:', identificador)
                raise SystemExit(0)
    time.sleep(1)
raise SystemExit('ERROR: el evento no apareció en Parquet en el intervalo observado')
PY

printf '\nResultado del smoke aislado: COMPLETADO.\n'
printf 'No sustituye pruebas con gateway/MongoDB/Spark ni mediciones oficiales de P3/P4.\n'
printf 'Finalizado (UTC): %s\n' "$(date -u +%Y-%m-%dT%H:%M:%SZ)"
printf 'Evidencia: %s\n' "$LOG"
