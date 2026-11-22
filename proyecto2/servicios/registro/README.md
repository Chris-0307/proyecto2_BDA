# Servicio de Registro (`registro:8003`)

Estado vigente de la plataforma en MongoDB: clientes, proyectos, lotes, tareas,
anotadores, asignaciones y revisiones. Responsable: Integrante 1.

- Puerto interno 8003, **sin publicar al host**. Solo lo consume el gateway.
- No consulta Valkey (regla de aislamiento). El lock de 120 s lo toma el
  gateway en Sesión **antes** de llamar a `asignar`.
- Python 3.12, FastAPI, PyMongo. Paquete `registro_app` (no `app`, para no
  chocar con Sesión e Historial en pytest).

## Estructura

```text
servicios/registro/
├── Dockerfile
├── requirements.txt
├── registro_app/
│   ├── main.py          # rutas REST
│   ├── db.py            # cliente MongoDB (w=majority, sin reintentos)
│   ├── modelos.py       # validación de entrada (Pydantic)
│   └── transiciones.py  # máquina de estados atómica
└── mongo/
    ├── inicializar.sh          # entrypoint de mongo-init
    ├── init-replica.js         # rs.initiate() de rs-plataforma (idempotente)
    └── configurar-registro.js  # w:majority por defecto, colecciones e índices
```

## MongoDB

Conjunto `rs-plataforma` con tres nodos (`mongo-1`, `mongo-2`, `mongo-3`),
base `plataforma`. `mongo-init` es un servicio de un solo uso e idempotente;
`registro` arranca solo cuando termina con éxito.

Colecciones: `clientes`, `proyectos`, `batches`, `tareas`, `anotadores`.

| Índice | Campos | Uso |
|---|---|---|
| `tareas_proyecto_estado_prioridad` | `proyecto_id, estado, prioridad:-1` | Candidatas por proyecto (ESR) |
| `tareas_anotador_estado` | `asignacion.anotador_id, estado` | Tarea actual de un anotador |
| `tareas_batch` | `batch_id` | Tareas de un lote |
| `proyectos_cliente_estado` | `cliente_id, estado` | Proyectos de un cliente |

### Conexión (`db.py`)

| Parámetro | Valor | Motivo |
|---|---|---|
| `w` | `majority` | Escritura confirmada por mayoría |
| `retryWrites` / `retryReads` | `false` | El enunciado pide 503 sin reintento |
| `serverSelectionTimeoutMS` | 3000 (`MONGO_SELECCION_MS`) | 503 rápido si no hay primario |
| `wtimeoutMS` | 5000 (`MONGO_WTIMEOUT_MS`) | Cota de espera de la mayoría |

Errores que se traducen a 503: `ConnectionFailure` (incluye
`ServerSelectionTimeoutError` y `NotPrimaryError`), `WriteConcernError` y
`ExecutionTimeout`.

### `w:1` frente a `w:majority`

`pruebas/p2_registro/write_concern.py` (`make write-concern`) mide con el
driver las dos escrituras del registro (`insert_one` y la transición
`find_one_and_update`) con cada write concern, con 1 y 8 hilos, y reporta
p50/p95/p99 y ops/s. Usa una base aparte (`bench_wc`) que borra al terminar.

Los tres nodos corren en la misma máquina, así que la replicación no paga
latencia de red real: la diferencia medida no se extrapola a un clúster
distribuido.

Resultado (`evidence/p2_write_concern.txt`, 3 repeticiones alternadas):

| Hilos | Operación | p50 `w:1` | p50 `majority` | Relación | ops/s `w:1` -> `majority` |
|---|---|---|---|---|---|
| 1 | `insert_one` | 0,52 ms | 2,66 ms | ×5,15 | 1552 -> 346 |
| 1 | `find_one_and_update` | 0,71 ms | 2,77 ms | ×3,91 | 1552 -> 346 |
| 8 | `insert_one` | 3,44 ms | 6,28 ms | ×1,83 | 2123 -> 1229 |
| 8 | `find_one_and_update` | 3,51 ms | 6,37 ms | ×1,82 | 2123 -> 1229 |

Con un hilo, la espera de confirmación de la mayoría domina cada escritura;
con 8 hilos esas esperas se solapan y la diferencia relativa baja.

Además de la latencia, `w:1` cambia la garantía: confirma una
escritura que aún no está en la mayoría y que puede perderse en un rollback
tras una elección (P8, caso C).

### Variables de entorno

| Variable | Defecto |
|---|---|
| `MONGO_URI` | `mongodb://mongo-1:27017,mongo-2:27017,mongo-3:27017/plataforma?replicaSet=rs-plataforma&w=majority` |
| `MONGO_DB` | `plataforma` |
| `ASIGNACION_PLAZO_S` | `120` |
| `MONGO_SELECCION_MS` | `3000` |
| `MONGO_WTIMEOUT_MS` | `5000` |

## Documento de tarea

```json
{
  "_id": "uuid", "proyecto_id": "uuid", "batch_id": "uuid", "cliente_id": "uuid",
  "modalidad": "lidar", "tipo_tarea": "cuboide_3d", "prioridad": 5,
  "archivo": {"uri": "...", "sha256": "...", "bytes": 0, "mime": "...", "meta": {}},
  "estado": "disponible", "version": 1, "sla_horas": 48,
  "asignacion": {"anotador_id": "...", "equipo_id": "...", "token": "...",
                 "tomada_en": "...", "vence_en": "..."},
  "anotacion": {"contenido": {}, "enviada_en": "..."},
  "revision": {"revisor_id": "...", "iniciada_en": "...", "resultado": "...",
               "motivo": "...", "revisada_en": "..."},
  "ultimo_evento_id": "uuid",
  "eventos_pendientes": [],
  "creado_en": "..."
}
```

Geometrías admitidas por modalidad (otra combinación -> 422):

| Modalidad | `tipo_tarea` |
|---|---|
| `texto` | `span` |
| `imagen` | `bbox`, `poligono` |
| `video` | `bbox_video` |
| `lidar` | `cuboide_3d` |

## Rutas

| Método y ruta | Efecto | Códigos |
|---|---|---|
| `POST /proyectos` | Crea proyecto con taxonomía y SLA (`sla_horas`, defecto 48) | 201, 404 (cliente), 422, 503 |
| `POST /proyectos/{id}/lotes` | Registra un lote (1–5000 tareas) en estado `disponible` | 201, 404, 422, 503 |
| `GET /tareas` | Tres consultas excluyentes (ver abajo) | 200, 422, 503 |
| `POST /tareas/{id}/transicion` | Una acción atómica por llamada | 200, 403, 404, 409, 422, 503 |
| `GET /proyectos/{id}/exportar` | Tareas `aprobada`, paginadas por `_id` | 200, 404, 503 |
| `GET /salud` | Estado de la réplica | 200, 503 |

Clientes y anotadores no tienen ruta REST: se cargan desde `dataset/`.

### `GET /tareas`

| Parámetros | Devuelve |
|---|---|
| `proyecto_id=...&candidatas=true` | Primero `disponible` por prioridad descendente; si faltan, `abierta` con `vence_en` pasado |
| `anotador_id=...[&estado=...]` | Tareas asignadas a ese anotador |
| `eventos_pendientes=true` | Tareas con eventos aún no confirmados (reconciliación) |

`limite` entre 1 y 200 (defecto 20). Sin ninguno de los tres modos -> 422.

### `POST /tareas/{id}/transicion`

Cuerpo con `accion` y los campos que esa acción exige (campos extra -> 422):

| Acción | Requiere | De -> a | Evento |
|---|---|---|---|
| `asignar` | `anotador_id`, `token` | `disponible` o `abierta` vencida -> `abierta` | `tarea_abierta` |
| `enviar` | `anotador_id`, `token`, `contenido` | `abierta` -> `anotada` | `tarea_anotada` |
| `liberar` | `anotador_id`, `token` | `abierta` -> `disponible` | ninguno |
| `iniciar_revision` | `revisor_id` | `anotada` -> `en_revision` | `revision_iniciada` |
| `aprobar` | `revisor_id` (el mismo que inició) | `en_revision` -> `aprobada` | `revision_aprobada` |
| `rechazar` | `revisor_id`, `motivo` | `en_revision` -> `disponible` | `revision_rechazada` (estado `rechazada`) |
| `confirmar_evento` | `event_id` | sin cambio de estado | ninguno |

Respuesta: `{"tarea": {...}, "evento": {...} | null}`.

Códigos:

- **404**: la tarea, el anotador o el revisor no existe.
- **403**: `iniciar_revision` con un anotador que no es revisor.
- **409**: la tarea no estaba en el estado de origen, o el token/revisor no
  coincide. Nada se modificó.
- **503**: MongoDB no responde (ver limitaciones).

`asignar` fija `vence_en = tomada_en + ASIGNACION_PLAZO_S`. Es una cota
inferior; la garantía real contra dos dueños es el lock de Valkey.

### `GET /proyectos/{id}/exportar`

`?despues=<_id>&limite=N` (1–1000, defecto 100). Devuelve
`{"items": [...], "siguiente": "<_id>" | null}`; `siguiente` es el cursor de la
próxima página.

### `GET /salud`

| Estado | Condición | Código |
|---|---|---|
| `operativo` | 3 nodos sanos y primario | 200 |
| `degradado` | Hay primario y mayoría, pero falta un nodo | 200 |
| `caido` | Sin primario o sin mayoría | 503 |

## Atomicidad

Cada transición es **un solo** `find_one_and_update` cuyo filtro exige el
estado de origen (y el token o el revisor cuando corresponde). Si dos
peticiones compiten por la misma tarea, solo una coincide con el filtro; la
otra recibe 409. No hay lectura previa seguida de escritura.

La actualización es un pipeline de agregación (`[{"$set": ...}]`) para que, en
la misma operación:

1. se cambie el estado y suba `version`;
2. se construya el evento de historial con los campos de la propia tarea;
3. se agregue el evento a `eventos_pendientes` y se fije `ultimo_evento_id`.

Todas las expresiones de un mismo `$set` se evalúan sobre el documento
anterior, por eso la `version` del evento es la anterior + 1, igual que la de
la tarea. Los valores del cliente van en `$literal` para que un texto que
empiece con `$` no se interprete como campo u operador.

### Eventos pendientes (outbox)

No hay transacción entre MongoDB y Valkey. El flujo previsto con el gateway:

1. El gateway llama a la transición; el evento queda en `eventos_pendientes`.
2. El gateway publica el `evento` de la respuesta en `POST /eventos` de Historial.
3. Con 200/202 de Historial, llama a `confirmar_evento` con ese `event_id`.

`confirmar_evento` es idempotente. Si Historial falla o el gateway cae entre 1
y 3, el evento queda en pendientes; el reconciliador del gateway lo recupera con
`GET /tareas?eventos_pendientes=true` y lo reenvía. Historial deduplica por
`event_id`.

## Contratos con otros integrantes

**Integrante 2 (Historial).** Los eventos siguen el esquema `EventoNegocio`
de Historial (`extra=forbid`). Campos comunes: `event_id`, `tipo_evento`,
`estado`, `ocurrido_en`, `tarea_id`, `proyecto_id`, `cliente_id`, `batch_id`,
`modalidad`, `tipo_tarea`, `version`, `sla_horas`. Campos por evento:

| Evento | Campos adicionales |
|---|---|
| `tarea_abierta` | `anotador_id`, `equipo_id`, `tomada_en` |
| `tarea_anotada` | `anotador_id`, `equipo_id`, `tomada_en`, `duracion_ms` |
| `revision_iniciada` | `anotador_id`, `equipo_id`, `tomada_en`, `revisor_id` |
| `revision_aprobada` | `anotador_id`, `equipo_id`, `tomada_en`, `revisor_id`, `aprobada_en` |
| `revision_rechazada` | `anotador_id`, `equipo_id`, `tomada_en`, `revisor_id`, `motivo_rechazo` |

`pruebas/p1_gateway/smoke_flujo.py` (`make smoke-flujo`) comprueba que los cinco
tipos de evento pasan la validación de `POST /eventos` (campo `evento: publicado`).

**Integrante 3 (Analítica).**

- `duracion_ms` solo viaja en `tarea_anotada` (tiempo entre tomar y enviar).
  Q3 debe leerlo de ese evento.
- Q5 (toma -> aprobación) se calcula con `tomada_en` y `aprobada_en` del evento
  `revision_aprobada`.
- Un rechazo queda en Parquet como `rechazada`, aunque en MongoDB la tarea
  vuelva a `disponible`.
- El generador oficial debe respetar el documento de tarea descrito arriba.

## Ejecución

Desde la raíz del repositorio:

```bash
docker compose -f proyecto2/docker-compose.yml up -d --build
docker compose -f proyecto2/docker-compose.yml ps
docker compose -f proyecto2/docker-compose.yml run --rm registro python /dataset/cargar_demo.py
docker compose -f proyecto2/docker-compose.yml run --rm registro python /pruebas/p2_registro/smoke_registro.py \
  | tee proyecto2/evidence/p2_registro_smoke.txt
```

Esperado: todas las líneas en `[OK]` y `Resultado: N/N comprobaciones
correctas`. El nombre de una comprobación fallida indica qué transición revisar.

Verificación de réplica, mayoría e índices:

```bash
bash proyecto2/pruebas/p2_registro/verificar_replica.sh
```

Logs:

```bash
docker compose -f proyecto2/docker-compose.yml logs --tail=100 registro mongo-init
```

Un `OperationFailure` en los logs de `registro` suele venir del pipeline de
actualización de `transiciones.py`.

## Evidencia

| Archivo | Contenido | Resultado |
|---|---|---|
| `evidence/p2_replica.txt` | Estado de `rs-plataforma`, mayoría e índices | 1 primario, 2 secundarios |
| `evidence/p2_write_concern.txt` | Latencia y rendimiento con `w:1` frente a `w:majority` (driver, 1 y 8 hilos) | Ver tabla en la sección `w:1` frente a `w:majority` |
| `evidence/p8_mongo_fallas.txt` | P8: secundario, primario y sin mayoría; elección, RTO y RPO | 14/14; ver resultados en la sección P8 |
| `evidence/p2_concurrencia_gateway.txt` | 50 contra 10 por el gateway (con locks de Valkey) | 9/9; 10×200, 40×204, cero dobles asignaciones |
| `evidence/p2_registro_smoke.txt` | Flujo completo, 409/403/422, 50 asignaciones simultáneas, eventos pendientes | 30/30; 50 intentos -> 1×200, 49×409 |

`p2_registro_smoke.txt` valida la atomicidad a nivel de registro;
`p2_concurrencia_gateway.txt`, la prueba completa por el gateway.

## P8: fallas de MongoDB

`pruebas/p8_fallas/p8_mongo.py` (`make p8-mongo`) corre en el host con Python
estándar y Docker, con el Compose final levantado. Crea sus propios datos
(cliente, 300 anotadores y 300 tareas) y hace todo el tráfico por el gateway.

| Caso | Acción | Qué se comprueba |
|---|---|---|
| A | Detener un secundario | `/salud` degradado (200); 10/10 ciclos `siguiente` + `enviar` responden 200 |
| B | Detener el primario | Se elige otro primario (tiempo de elección); tiempo sin escrituras y RTO (parada -> primer 200); todo lo que respondió 200 sigue una vez, con su dueño y estado; el antiguo primario vuelve como SECONDARY |
| C | Detener los dos secundarios | `/salud` caído (503); ninguna escritura responde 200; `enviar` da 503 sin reintento; tras restaurar, el envío pendiente se resuelve (200 o 409) |
| Final | — | RPO de escrituras confirmadas: ninguna tarea con 200 se perdió; lista de operaciones con 503 (resultado indeterminado) |

Después de cada caso se arrancan los nodos y se espera a 1 PRIMARY + 2
SECONDARY y `/salud` operativo, para dejar un estado conocido.

Resultado con SIGKILL (`evidence/p8_mongo_fallas.txt`, 14/14):

| Caso | Observado |
|---|---|
| A | `/salud` degradado a los 12,5 s; 10/10 escrituras con 200; vuelta a operativo en 3,0 s |
| B | Nuevo primario en ~10 s (`electionTimeoutMillis` 10 000). Último ciclo con 503 a 7,6 s, primer 200 a 9,7 s: **RTO de escritura entre 7,6 y 9,7 s** (resolución ~4 s, porque cada ciclo fallido espera los 3 s de selección de servidor). 25/25 tareas con 200 intactas |
| C | 7 respuestas 503 y ninguna 200 sin mayoría; `/salud` 503 caído. Recuperación en 5,6 s. El reintento del envío dio 409: el primer 503 se había aplicado en el primario aislado y se conservó |
| Final | **RPO 0** de escrituras confirmadas (26/26 intactas). 5 asignaciones con 503 de resultado indeterminado |

- La caída es real: se desactiva la política de reinicio del contenedor
  (`docker update --restart=no`) y se le envía SIGKILL, sin apagado ordenado.
  Al restaurar se arranca y se devuelve `restart: unless-stopped`.
- `P8_PARADA=stop` usa `docker compose stop` (apagado ordenado). En la primera
  corrida, con `stop -t 0`, el primario alcanzó a ceder el rol antes de morir:
  la elección tomó 1,3 s y no hubo escrituras fallidas, así que esa corrida no
  sirve para medir el RTO de una caída.
- En C, antes de que el primario aislado se degrade (hasta
  `electionTimeoutMillis`, 10 s), una escritura puede aplicarse en él y responder
  503 por `wtimeout`. Al volver la mayoría puede conservarse (el reintento da
  409) o revertirse con un rollback (el reintento da 200). El script informa cuál
  ocurrió.

## Limitaciones

- Un 503 por `WriteConcernError` (p. ej. sin mayoría) no garantiza que la
  escritura no ocurrió: pudo aplicarse en el primario. Se estudiará en P8.
- Registrar un lote hace `insert_one` del lote y `insert_many` de las tareas
  sin transacción multidocumento: una caída a mitad puede dejar un lote parcial.
- `liberar` no genera evento: el enunciado no la registra en el datalake.
