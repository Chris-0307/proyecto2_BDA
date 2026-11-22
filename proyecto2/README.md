# Proyecto 2 — Plataforma distribuida de anotación de datos

Capa de datos de una plataforma de anotación basada en servicios REST y
contenedores: registro operacional (MongoDB en réplica), sesiones y locks
(Valkey con AOF), historial en Parquet y analítica con Apache Spark.

> **Estado del proyecto:** en desarrollo. Este README solo describe lo que
> existe hoy en el repositorio. Las secciones marcadas **PENDIENTE** se
> completarán al integrar cada componente. No se reportan cifras ni
> resultados de pruebas que no tengan un archivo en `evidence/`.

## 1. Arquitectura

| Componente | Responsabilidad | Tecnología | Estado |
|---|---|---|---|
| Gateway | Único punto de entrada (puerto 8000), validación, enrutamiento, salud por capa | REST | PENDIENTE |
| Registro | Clientes, proyectos, lotes, tareas, asignaciones, revisiones, exportación | MongoDB, 3 nodos | PENDIENTE |
| Sesión | Locks `SET NX PX` de 120 s, latidos, borradores, sesiones activas, recuperación | Valkey con AOF `everysec` | Implementado, sin ensayos ejecutados |
| Historial | Recepción de eventos, micro-lotes, escritura del datalake | Valkey Streams + Parquet | Implementado, sin ensayos ejecutados |
| Analítica | Consultas Q1–Q5 de solo lectura sobre Parquet | Apache Spark | PENDIENTE |
| Simulador | Anotadores, revisores y consumidores, solo vía HTTP al gateway | Cliente HTTP | PENDIENTE |

**Reglas de aislamiento:**

- Solo el gateway publica un puerto al host (8000).
- Registro no consulta Valkey.
- Sesión no decide el estado definitivo de una tarea ni accede a MongoDB.
- Analítica lee únicamente el lago Parquet.
- El simulador se comunica solo con el gateway.

## 2. Estructura del repositorio

```text
proyecto2/
├── compose.integrante2.yml   # entorno aislado de Sesión + Historial (pruebas)
├── servicios/
│   ├── sesion/               # API de sesiones, locks, borradores, valkey.conf
│   └── historial/            # API de eventos y worker de Parquet
├── pruebas/                  # pytest, script de reinicio AOF, smoke, evidencias
├── evidence/                 # resultados originales de las pruebas
└── docs/                     # contratos, guías de operación e informe base
```

Carpetas previstas, aún no creadas: `servicios/gateway/`,
`servicios/registro/`, `servicios/analitica/`, `simulador/`, `dataset/`,
`datalake/` y el `docker-compose.yml` final.

## 3. Requisitos previos

- Docker Engine o Docker Desktop con integración WSL, y `docker compose` v2.
- Python 3 en el host (solo para el script de reinicio AOF y la prueba de humo).
- Bash (WSL o Linux).

## 4. Levantar y detener el entorno aislado (Sesión + Historial)

Ejecute siempre desde la **raíz del repositorio**. Este Compose es solo para
probar Sesión e Historial; **no reemplaza** al `docker-compose.yml` final.

```bash
docker compose -f proyecto2/compose.integrante2.yml -p bda-integrante2 config -q
docker compose -f proyecto2/compose.integrante2.yml -p bda-integrante2 up -d --build
docker compose -f proyecto2/compose.integrante2.yml -p bda-integrante2 ps
```

Verificar que la persistencia AOF está activa (esperado: `yes` y `everysec`):

```bash
docker compose -f proyecto2/compose.integrante2.yml -p bda-integrante2 \
  exec valkey valkey-cli CONFIG GET appendonly
docker compose -f proyecto2/compose.integrante2.yml -p bda-integrante2 \
  exec valkey valkey-cli CONFIG GET appendfsync
```

Ver logs y detener:

```bash
docker compose -f proyecto2/compose.integrante2.yml -p bda-integrante2 \
  logs --tail=100 sesion historial historial-worker
docker compose -f proyecto2/compose.integrante2.yml -p bda-integrante2 down
```

> `down` conserva los volúmenes. **No use `down -v`** salvo que quiera borrar
> intencionalmente el estado de Valkey (incluidos los borradores).

En este entorno ningún servicio publica puertos al host. Los archivos Parquet
de prueba se escriben en `proyecto2/.sandbox_integrante2/datalake/` (ignorada
por Git).

## 5. Servicios internos

### Sesión (`sesion:8001`)

| Método y ruta | Efecto | Códigos |
|---|---|---|
| `GET /salud` | Comprueba Valkey | 200, 503 |
| `POST /sesiones/iniciar` | Lock + sesión + `editando` | 201, 409, 503 |
| `POST /sesiones/latido` | Renueva el lock si el token coincide | 200, 409, 503 |
| `POST /sesiones/finalizar` | Libera lock y sesión propios | 200, 409, 503 |
| `PUT /tareas/{tarea_id}/borrador` | Guarda el último borrador y renueva el lock | 200, 409, 422, 503 |
| `POST /sesiones/recuperar` | Recupera sesión y borrador | 200, 409, 503 |

La recuperación devuelve `activa` si el lock sigue vigente, o
`requiere_reasignacion` si venció (borrador en solo lectura; la decisión de
reasignar corresponde al registro, vía gateway).

### Historial (`historial:8002` y worker)

| Método y ruta | Efecto | Códigos |
|---|---|---|
| `GET /salud` | Comprueba Valkey | 200, 503 |
| `POST /eventos` | Encola el evento de forma idempotente por `event_id` | 202 nuevo, 200 repetido, 409 conflicto, 422 inválido, 503 |

El worker (`python -m app.worker`) lee el grupo `historial-parquet`, escribe
cada micro-lote en `.staging`, lo publica con `os.replace` en
`eventos/mes=AAAA-MM/` y solo entonces confirma con `XACK`. Los mensajes
pendientes se reclaman con `XAUTOCLAIM`.

### Registro (PROPUESTA, pendiente de confirmar por el equipo)

Máximo seis rutas: `POST /proyectos`, `POST /proyectos/{id}/lotes`,
`GET /tareas`, `POST /tareas/{id}/transicion` (con `accion`),
`GET /proyectos/{id}/exportar` y `GET /salud`.

### Gateway (PENDIENTE)

Rutas definidas por el enunciado (sección 7): proyectos, lotes,
`POST /tareas/siguiente`, borrador, enviar, liberar, revisión,
`POST /sesiones/recuperar`, `GET /analitica/{q1..q5}`, exportar y `GET /salud`.

## 6. Datalake Parquet

```text
datalake/eventos/
├── .staging/               # temporales; Spark nunca los lee
├── mes=2026-10/lote-<hash-ids>.parquet
└── mes=2026-11/lote-<hash-ids>.parquet
```

- Partición mensual calculada con `ocurrido_en` en UTC.
- El consumo es *al menos una vez*: dos micro-lotes solapados pueden repetir
  un `event_id` en archivos distintos. **Spark debe aplicar
  `dropDuplicates(["event_id"])` antes de Q1–Q5.**
- Spark debe leer solo `*.parquet` publicados, con el directorio montado en
  solo lectura.
- Un rechazo queda como `rechazada` en Parquet aunque en MongoDB la tarea
  vuelva a `disponible`. Q4 cuenta esas filas.

## 7. Carga de datos

**PENDIENTE.** El generador reproducible (60 clientes, 180 proyectos, 600
lotes, 60 000 tareas, 300 anotadores en 30 equipos y ~800 000 eventos) irá en
`dataset/` con sus conteos y comandos documentados.

## 8. Consultas analíticas Q1–Q5

**PENDIENTE.** Se documentarán el comando de ejecución, los planes de Q1 y Q3
y la comparación de tiempos con las limitaciones del entorno.

## 9. Pruebas

Las pruebas escriben su evidencia en `evidence/`. Un test `SKIPPED` significa
**no ejecutado**, nunca aprobado.

| Prueba | Estado |
|---|---|
| P1 REST y despliegue | PENDIENTE |
| P2 Registro y concurrencia (50 contra 10) | PENDIENTE |
| P3 Sesión (borrador, AOF, vencimiento) | Scripts listos; ejecución y evidencia pendientes |
| P4 Historial (micro-lotes, reproceso, frescura) | Scripts listos; ejecución y evidencia pendientes |
| P5 Analítica | PENDIENTE |
| P6 Carga (10, 40 y 100 usuarios) | PENDIENTE |
| P8 Fallas | PENDIENTE |

**Prueba de humo del entorno aislado** (levanta el Compose, crea una sesión y
un borrador, reinicia solo Valkey, recupera el borrador y espera un evento en
Parquet). Deja un `.txt` en `evidence/`:

```bash
bash proyecto2/pruebas/smoke_integrante2.sh
```

No usa el gateway, así que **no sustituye** las pruebas P3/P4 de extremo a
extremo.

**Suite pytest dentro de Docker** (usa la base 15 de Valkey solo para pruebas
y no ejecuta `FLUSHDB`):

```bash
docker compose -f proyecto2/compose.integrante2.yml -p bda-integrante2 \
  run --rm \
  -v "$(pwd)/proyecto2:/workspace:ro" -w /workspace \
  -e VALKEY_TEST_URL=redis://valkey:6379/15 \
  -e PYTHONDONTWRITEBYTECODE=1 historial \
  sh -c 'pip install --no-cache-dir -r servicios/sesion/requirements-dev.txt -r servicios/historial/requirements-dev.txt && python -m pytest -p no:cacheprovider pruebas/ -v -ra'
```

**Suite con registro de evidencia** (conserva el log y un resumen JSON):

```bash
python proyecto2/pruebas/ejecutar_evidencias_p3_p4.py
```

**Reinicio real de Valkey con AOF** (requiere el `docker-compose.yml`
integrado con el servicio `valkey`):

```bash
VALKEY_TEST_URL=redis://localhost:6379/15 \
python proyecto2/pruebas/p3_reinicio_aof.py \
  --restart-command 'docker compose -f proyecto2/docker-compose.yml restart valkey'
```

Cada ejecución manual se documenta con fecha UTC, objetivo, máquina,
comando exacto, configuración, resultado, interpretación y archivo de
evidencia (plantilla en `docs/`).

## 10. Solución de problemas

| Síntoma | Qué revisar |
|---|---|
| Tests `SKIPPED` | Falta `VALKEY_TEST_URL` o Valkey no responde. No cuentan como aprobados |
| `appendonly` no es `yes` | Revise que `valkey.conf` esté montado en el contenedor `valkey` |
| El borrador desapareció tras un reinicio | ¿Se usó `down -v`? Borra el volumen `valkey_datos` |
| Historial no escribe Parquet | Revise los logs de `historial-worker` y el volumen del datalake |
| Mensajes sin confirmar | `XPENDING historial:eventos historial-parquet`; se reclaman tras `HISTORIAL_RECUPERACION_IDLE_MS` (30 000 ms por defecto) |
| Choque de nombres de paquete en pytest | Los servicios usan el paquete `app`; los tests los cargan con nombres distintos |

## 11. Limitaciones conocidas

- No hay transacción única entre MongoDB y Valkey. El intervalo entre
  confirmar en MongoDB y publicar el evento requiere reconciliación o un
  marcador durable en el registro.
- AOF `everysec` puede perder aproximadamente el último segundo ante una caída
  abrupta; un reinicio ordenado no equivale a ese ensayo.
- La publicación Parquet es atómica por archivo, no entre particiones.
- Hasta ejecutar los ensayos, **no hay mediciones disponibles** (latencias
  p50/p95/p99, RTO, RPO, frescura).

## 12. Documentación adicional

En `docs/`: contratos de Sesión e Historial, guía de integración y operación,
guía de pruebas P3/P4 e informe técnico base del Integrante 2.