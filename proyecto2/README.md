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
| Registro | Clientes, proyectos, lotes, tareas, asignaciones, revisiones, exportación | MongoDB, 3 nodos | Réplica e índices verificados (`evidence/p2_replica_20261122T014246Z.txt`); smoke del registro 30/30 (`evidence/p2_registro_smoke_20261122T015847Z.txt`) |
| Sesión | Locks `SET NX PX` de 120 s, latidos, borradores, sesiones activas, recuperación | Valkey con AOF `everysec` | Implementado, sin ensayos ejecutados |
| Historial | Recepción de eventos, micro-lotes, escritura del datalake | Valkey Streams + Parquet | Implementado, sin ensayos ejecutados |
| Analítica | Consultas Q1–Q5 de solo lectura sobre Parquet | Apache Spark | PENDIENTE |
| Simulador | Anotadores, revisores y consumidores, solo vía HTTP al gateway | Cliente HTTP | PENDIENTE |

**Reglas de aislamiento:**

- Solo el gateway publicará un puerto al host (8000). Hoy ningún servicio publica puertos.
- Registro no consulta Valkey.
- Sesión no decide el estado definitivo de una tarea ni accede a MongoDB.
- Analítica lee únicamente el lago Parquet.
- El simulador se comunica solo con el gateway.

## 2. Estructura del repositorio

```text
proyecto2/
├── docker-compose.yml        # Compose final: MongoDB, Valkey, Sesión, Historial, Registro
├── compose.integrante2.yml   # entorno aislado de Sesión + Historial (solo pruebas)
├── servicios/
│   ├── registro/             # API del registro (registro_app) y scripts de MongoDB
│   │   └── mongo/            # init-replica.js, configurar-registro.js, inicializar.sh
│   ├── sesion/               # API de sesiones, locks, borradores, valkey.conf
│   └── historial/            # API de eventos y worker de Parquet
├── dataset/                  # carga de datos (hoy: cargador de demostración)
├── datalake/                 # Parquet publicado por el worker (no se versiona)
├── pruebas/                  # pytest, smoke, reinicio AOF, verificación de réplica
├── evidence/                 # resultados originales de las pruebas
└── docs/                     # contratos, guías de operación e informe base
```

Carpetas previstas, aún no creadas: `servicios/gateway/`,
`servicios/analitica/` y `simulador/`.

## 3. Requisitos previos

- Docker Engine o Docker Desktop con integración WSL, y `docker compose` v2.
- Bash (WSL o Linux).
- Unos 4 GB de RAM libres: cada nodo de MongoDB usa 300–500 MB.
- Python 3 en el host solo para `p3_reinicio_aof.py`.

## 4. Levantar y detener el sistema

Ejecute siempre desde la **raíz del repositorio**.

```bash
docker compose -f proyecto2/docker-compose.yml up -d --build
docker compose -f proyecto2/docker-compose.yml ps
```

Estado esperado: los tres `mongo`, `valkey`, `sesion`, `historial`,
`historial-worker` y `registro` en `running`; `mongo-init` en `Exited (0)`.

`mongo-init` es un servicio de un solo uso e **idempotente**: inicializa el
conjunto `rs-plataforma` (si no existe), fija `w: majority` por defecto y crea
colecciones e índices. El servicio `registro` espera a que termine con éxito.

Verificar réplica, mayoría e índices (deja evidencia en `evidence/`):

```bash
bash proyecto2/pruebas/verificar_replica.sh
```

Verificar AOF de Valkey (esperado: `yes` y `everysec`):

```bash
docker compose -f proyecto2/docker-compose.yml exec valkey valkey-cli CONFIG GET appendonly
docker compose -f proyecto2/docker-compose.yml exec valkey valkey-cli CONFIG GET appendfsync
```

Logs y detención:

```bash
docker compose -f proyecto2/docker-compose.yml logs --tail=100 registro sesion historial historial-worker
docker compose -f proyecto2/docker-compose.yml down
```

> `down` conserva los volúmenes. **`down -v` borra los datos de MongoDB y los
> borradores de Valkey**. Úselo solo para probar un arranque desde cero.

### Entorno aislado de Sesión + Historial

`compose.integrante2.yml` levanta solo Valkey, Sesión e Historial (proyecto
`bda-integrante2`, Parquet en `proyecto2/.sandbox_integrante2/`). Sirve para
`smoke_integrante2.sh`. **No lo ejecute a la vez que el Compose final**: cada
uno trae su propio Valkey y consumen memoria innecesaria.

```bash
docker compose -f proyecto2/compose.integrante2.yml -p bda-integrante2 up -d --build
docker compose -f proyecto2/compose.integrante2.yml -p bda-integrante2 down
```

## 5. Servicios internos

### Registro (`registro:8003`)

Detalle completo (documento de tarea, atomicidad, contratos de eventos y
notas de P8) en [`servicios/registro/README.md`](servicios/registro/README.md).

Seis rutas. Los clientes y anotadores se cargan con `dataset/` (sin ruta REST).

| Método y ruta | Efecto | Códigos |
|---|---|---|
| `POST /proyectos` | Crea proyecto, taxonomía y SLA | 201, 404 (cliente), 422, 503 |
| `POST /proyectos/{id}/lotes` | Registra un lote y sus tareas | 201, 404, 422, 503 |
| `GET /tareas` | Consulta: candidatas de un proyecto, tarea de un anotador o eventos pendientes | 200, 422, 503 |
| `POST /tareas/{id}/transicion` | Una acción por llamada (ver abajo) | 200, 403, 404, 409, 422, 503 |
| `GET /proyectos/{id}/exportar` | Tareas aprobadas, paginadas por `_id` | 200, 404, 503 |
| `GET /salud` | Estado de la réplica (operativo, degradado, caído) | 200, 503 |

Acciones de `transicion`:

| Acción | De → a | Evento generado |
|---|---|---|
| `asignar` | `disponible` (o `abierta` vencida) → `abierta` | `tarea_abierta` |
| `enviar` | `abierta` → `anotada` | `tarea_anotada` |
| `liberar` | `abierta` → `disponible` | ninguno |
| `iniciar_revision` | `anotada` → `en_revision` | `revision_iniciada` |
| `aprobar` | `en_revision` → `aprobada` | `revision_aprobada` |
| `rechazar` | `en_revision` → `disponible` | `revision_rechazada` (estado `rechazada`) |
| `confirmar_evento` | no cambia el estado; retira un evento ya publicado | ninguno |

Cada transición es una única operación atómica (`findOneAndUpdate` con
filtro por estado, token o revisor). La versión sube en cada transición
válida. Un filtro que no coincide responde 409 (o 404 si la tarea no existe).
El evento de historial se arma dentro de la misma operación y queda en
`eventos_pendientes` hasta que el gateway lo publique y llame a
`confirmar_evento`.

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
- `datalake/` está en `.gitignore`: lo produce el worker, no se versiona.

## 7. Carga de datos

**Carga de demostración** (solo para desarrollo del registro): crea un
cliente, seis anotadores (dos son revisores, en dos equipos), un proyecto
LiDAR y un lote de tareas. Requiere el sistema levantado.

```bash
docker compose -f proyecto2/docker-compose.yml run --rm registro python /dataset/cargar_demo.py
```

**PENDIENTE:** el generador reproducible oficial (60 clientes, 180 proyectos,
600 lotes, 60 000 tareas, 300 anotadores en 30 equipos y ~800 000 eventos)
irá también en `dataset/`, con sus conteos y comandos documentados. Debe
respetar el esquema de documentos del registro.

## 8. Consultas analíticas Q1–Q5

**PENDIENTE.** Se documentarán el comando de ejecución, los planes de Q1 y Q3
y la comparación de tiempos con las limitaciones del entorno.

## 9. Pruebas

Las pruebas escriben su evidencia en `evidence/`. Un test `SKIPPED` significa
**no ejecutado**, nunca aprobado.

| Prueba | Estado |
|---|---|
| P1 REST y despliegue | PENDIENTE (requiere gateway) |
| P2 Registro y concurrencia (50 contra 10) | Réplica verificada (`evidence/p2_replica_20261122T014246Z.txt`). Atomicidad a nivel de registro: 30/30, 50 asignaciones simultáneas → 1×200 y 49×409 (`evidence/p2_registro_smoke_20261122T015847Z.txt`). La prueba completa requiere el gateway |
| P3 Sesión (borrador, AOF, vencimiento) | Scripts listos; ejecución y evidencia pendientes |
| P4 Historial (micro-lotes, reproceso, frescura) | Scripts listos; ejecución y evidencia pendientes |
| P5 Analítica | PENDIENTE |
| P6 Carga (10, 40 y 100 usuarios) | PENDIENTE |
| P8 Fallas | PENDIENTE. Los nodos de MongoDB usan `restart: unless-stopped`: use `docker stop` (no `docker kill`, que los reinicia) |

**Prueba del registro** (flujo completo de transiciones, 409 por token o
estado, 50 asignaciones concurrentes sobre una misma tarea, eventos
pendientes). Primero cargue la demostración; luego:

```bash
docker compose -f proyecto2/docker-compose.yml run --rm registro python /pruebas/smoke_registro.py \
  | tee proyecto2/evidence/p2_registro_smoke_$(date -u +%Y%m%dT%H%M%SZ).txt
```

**Prueba de humo del entorno aislado de Sesión + Historial:**

```bash
bash proyecto2/pruebas/smoke_integrante2.sh
```

No usa el gateway, así que **no sustituye** las pruebas P3/P4 de extremo a
extremo.

**Suite pytest de Sesión e Historial dentro de Docker** (usa la base 15 de
Valkey solo para pruebas y no ejecuta `FLUSHDB`):

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

**Reinicio real de Valkey con AOF** (contra el Compose final):

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
| `mongo-init` falla o `registro` no arranca | `docker compose -f proyecto2/docker-compose.yml logs mongo-init`. Repita con `run --rm mongo-init`; es idempotente |
| `no replset config has been received` | El conjunto no está inicializado: `mongo-init` debe ejecutar `rs.initiate()`. Verifique que el contenedor vea la versión actual de `init-replica.js` |
| Tests `SKIPPED` | Falta `VALKEY_TEST_URL` o Valkey no responde. No cuentan como aprobados |
| `appendonly` no es `yes` | Revise que `valkey.conf` esté montado en el contenedor `valkey` |
| El borrador o los datos desaparecieron tras un reinicio | ¿Se usó `down -v`? Borra los volúmenes |
| Historial no escribe Parquet | Revise los logs de `historial-worker` y el volumen `datalake/` |
| Mensajes sin confirmar | `XPENDING historial:eventos historial-parquet`; se reclaman tras `HISTORIAL_RECUPERACION_IDLE_MS` (30 000 ms por defecto) |
| Poca memoria / contenedores que se reinician | Cada `mongod` usa 300–500 MB; cierre el Compose aislado de Sesión + Historial |
| Choque de nombres de paquete en pytest | Sesión e Historial usan el paquete `app`; los tests los cargan con nombres distintos. El registro usa `registro_app` |

## 11. Limitaciones conocidas

- No hay transacción única entre MongoDB y Valkey. El evento se guarda en la
  tarea (`eventos_pendientes`) dentro de la misma operación atómica; el gateway
  lo publica a Historial y lo confirma. Si el gateway cae entre ambos pasos, el
  evento queda pendiente y debe reconciliarse (`GET /tareas?eventos_pendientes=true`).
- Un 503 del registro no garantiza que la escritura no ocurrió: un fallo de
  `w: majority` puede dejar la operación aplicada en el primario. Se estudiará
  en las pruebas de fallas (P8).
- AOF `everysec` puede perder aproximadamente el último segundo ante una caída
  abrupta; un reinicio ordenado no equivale a ese ensayo.
- La publicación Parquet es atómica por archivo, no entre particiones.
- Registrar un lote inserta las tareas sin transacción multidocumento: una caída
  a mitad puede dejar un lote parcial.
- Hasta ejecutar los ensayos, **no hay mediciones disponibles** (latencias
  p50/p95/p99, RTO, RPO, frescura).

## 12. Documentación adicional

En `docs/`: contratos de Sesión e Historial, guía de integración y operación,
guía de pruebas P3/P4 e informe técnico base del Integrante 2.