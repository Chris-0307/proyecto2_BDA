# Informe — Plataforma distribuida de anotación de datos

Proyecto 2, Bases de Datos Avanzadas — Instituto Tecnológico de Costa Rica.

**Equipo:** Christian Navarro, Jimmy Feng, Emmanuel Calvo.
**Repositorio:** PENDIENTE (enlace).
**Fecha de entrega:** PENDIENTE.

Este documento es la fuente del PDF que se entrega en TEC Digital. El README
del repositorio solo explica cómo ejecutar el sistema; los resultados, el
análisis y las limitaciones están aquí.

**Trazabilidad.** Toda cifra proviene de un archivo de `proyecto2/evidence/`
(sección 14). Lo que no se midió se declara como no disponible y se explica la
causa. Las mediciones disponibles se hicieron en una sola máquina: Linux WSL2
(kernel 6.6.87.1), 16 CPU lógicos, Docker local.

**Estado por sección**

| Sección | Responsable | Estado |
|---|---|---|
| 1. Arquitectura y límites de responsabilidad | Jimmy | Completa |
| 2. Justificación de motores | Jimmy, Christian y Emmanuel | Completa |
| 3. API REST y contratos | Jimmy | Completa (`/analitica` conectada) |
| 4. Recorrido de una tarea (LiDAR) | Jimmy | Completa |
| 5. Registro: modelo, índices y concurrencia | Jimmy | Completa; faltan planes `explain` |
| 6. Sesión y recuperación del borrador | Christian | Diseño escrito; faltan resultados de P3 |
| 7. Datalake Parquet | Christian | Diseño escrito; faltan compactación y resultados de P4 |
| 8. Datos de prueba | Emmanuel | Completa |
| 9. Analítica con Spark (Q1–Q5) | Emmanuel | Completa |
| 10. Pruebas y evidencia experimental | Todos | P1, P2, P5, P6 y P8 (MongoDB) completas |
| 11. Carga y cuellos de botella | Emmanuel | Completa |
| 12. Fallas, RTO y RPO | Jimmy y Christian | MongoDB completa; falta Valkey |
| 13. Limitaciones | Todos | En curso |
| 14. Evidencia | Todos | En curso |
| 15. Repositorio y colaboración | Todos | En curso |

---

## 1. Arquitectura general y límites de responsabilidad

```mermaid
flowchart TB
    sim["Simulador / cliente HTTP"]
    gw["Gateway<br/>validación, orquestación,<br/>X-Request-ID, /salud por capa"]
    reg["Registro :8003"]
    ses["Sesión :8001"]
    his["Historial :8002"]
    worker["historial-worker"]
    mongo[("MongoDB x3<br/>rs-plataforma")]
    valkey[("Valkey<br/>AOF everysec")]
    lake[("Datalake Parquet<br/>eventos/mes=AAAA-MM")]
    ana["Analítica (Spark)<br/>PENDIENTE"]
    salud[("Valkey base 2<br/>salud:capa:*, salud:eventos")]

    sim -->|"HTTP :8000<br/>único puerto publicado"| gw
    gw --> reg
    gw --> ses
    gw --> his
    reg --> mongo
    ses -->|"locks, sesiones, borradores"| valkey
    his -->|"Streams"| valkey
    worker -->|"XREADGROUP / XACK"| valkey
    worker --> lake
    ana -->|"solo lectura"| lake
    gw -.->|"/analitica (PENDIENTE)"| ana
    gw -->|"monitor de salud"| salud
```

| Componente | Responsabilidad | Tecnología | Responsable |
|---|---|---|---|
| Gateway | Único punto de entrada; valida, orquesta el flujo de una tarea, publica eventos y reporta salud por capa | FastAPI + httpx | Jimmy |
| Registro | Estado vigente de proyectos, lotes y tareas; transiciones atómicas; exportación | FastAPI + MongoDB (3 nodos) | Jimmy |
| Sesión | Locks de 120 s, sesiones, latidos y borradores | FastAPI + Valkey | Christian |
| Historial | Recepción idempotente de eventos y escritura de Parquet | FastAPI + Valkey Streams + Parquet | Christian |
| Analítica | Q1–Q5 sobre Parquet | Spark | Emmanuel|
| Simulador | Anotadores, revisores y consumidores vía HTTP | Cliente HTTP | Emmanuel|

**Reglas de aislamiento y su evidencia** (`p1_salud_aislamiento.txt`, 42/42):

- Solo el gateway publica puerto al host: en `docker compose ps` únicamente
  `gateway` tiene puerto publicado (8000). Desde el host, 8001, 8002, 8003,
  27017 y 6379 rechazan la conexión.
- El gateway no tiene `pymongo` (ni en el código, ni en la imagen, ni en las
  variables de entorno): no accede a MongoDB. Usa Valkey solo para los mensajes
  de salud, en su propia base (2) y con llaves `salud:*`; no lee ni escribe la
  base 0 de Sesión e Historial.
- El registro no tiene `redis`: no consulta Valkey.
- Sesión e Historial no tienen `pymongo`: no acceden a MongoDB, y por lo tanto
  Sesión no puede decidir el estado definitivo de una tarea.
- Limitación: todos los servicios comparten la red de Compose. El aislamiento
  se garantiza por dependencias y configuración, no por segmentación de red.

---

## 2. Justificación de motores

**MongoDB para el registro.**

- **Documento autocontenido.** Una tarea agrupa la referencia al archivo
  (`uri`, `sha256`, `bytes`, `mime`, `meta`), la asignación vigente, la
  anotación, la última revisión y su cola de eventos. La anotación cambia de
  forma según la modalidad (span, bbox, polígono, cuboide 3D), y un documento
  la guarda sin esquema rígido.
- **Atomicidad por documento.** Toda transición modifica un solo documento, así
  que un `findOneAndUpdate` con filtro por estado basta para garantizar la
  asignación exclusiva sin transacciones multidocumento.
- **Alta disponibilidad.** Un conjunto de réplicas de tres nodos tolera la
  caída de un nodo y elige un primario nuevo automáticamente (sección 12.1).
- **Durabilidad.** `w: majority` confirma una escritura solo cuando está en la
  mayoría; una elección no puede revertirla (sección 5.3).

**Valkey para sesiones** (Christian): estado efímero con TTL (`SET NX PX`),
operaciones atómicas con Lua y persistencia AOF para que el borrador sobreviva
a un reinicio. No es la fuente de verdad: el estado definitivo vive en MongoDB.

**Parquet + Spark para el historial** (Christian y Emmanuel): los eventos son
inmutables y se consultan por agregación (OLAP), lo que favorece un formato
columnar particionado por mes en lugar de consultar el registro operacional.

**Spark para la analítica** (Emmanuel).

- **Lectura columnar y particionada sobre Parquet:** el servicio `analitica` lee únicamente archivos `*.parquet` publicados en `datalake/eventos/mes=AAAA-MM/` en modo de solo lectura (`ro`), filtrando por rango temporal (`ocurrido_en`) sin consultar MongoDB ni Historial (existen además particiones residuales de pruebas en `2026-01` y `2026-11` junto a `.staging/`).
- **Deduplicación y agregación sobre consumo *al menos una vez*:** dado que los micro-lotes del datalake pueden contener eventos repetidos, Spark ejecuta `dropDuplicates(["event_id"])` antes de calcular las agregaciones Q1–Q5.
- **Costo de inicialización frente al volumen:** levantar la JVM de Spark y planificar la lectura de 145 archivos Parquet introduce un costo inicial alto en la primera consulta (53 362,02 ms en Q1 con `spark_workers=1`), que disminuye en ejecuciones posteriores una vez calentada la sesión (24 282,85 ms en Q1 con `w=1`; 18 637,70 ms en la 2.ª ejecución con `w=2`). Para volúmenes históricos grandes (cientos de miles a millones de eventos) el procesamiento columnar compensa ese arranque; para consultas puntuales de baja latencia no es adecuado.
---

## 3. API REST y contratos

La documentación completa de rutas, cuerpos y códigos está en
[`endpoints/README.md`](../endpoints/README.md). Decisiones del gateway:

- **Reintentos seguros.** Solo los `GET` se reintentan una vez ante error de
  conexión o timeout. `POST` y `PUT` nunca se reintentan: repetir podría
  duplicar un proyecto, un lote o una transición, y el enunciado pide 503 sin
  reintento.
- **Errores.** Un servicio interno que no responde se traduce a 503. Los
  códigos de los servicios internos (403, 404, 409, 422) se propagan tal cual.
- **Identificadores.** `X-Request-ID` se propaga a los servicios internos y se
  devuelve al cliente.
- **Salud por capa con mensajes en Valkey.** Un monitor en segundo plano del
  gateway consulta cada 5 s el `/salud` de Registro, Sesión e Historial y
  publica el estado de cada capa en Valkey: `salud:capa:{capa}` con expiración
  de 15 s (si el monitor se detiene, el estado expira en lugar de quedar viejo)
  y un stream `salud:eventos` con cada cambio (`capa:antes->ahora`). `GET /salud`
  lee esos mensajes (`fuente: valkey`); si Valkey no responde, consulta cada
  capa directamente (`fuente: directo`). El gateway responde siempre.
  - **Por qué el gateway publica por las capas:** el reporte de salud es
    responsabilidad del gateway según el enunciado, y el registro no puede
    escribir en Valkey. Se usa una base propia (2) para no mezclar datos con
    Sesión e Historial.
  - **Costo:** el estado puede tener hasta un intervalo (5 s) de atraso.

Resultados (`p1_salud_aislamiento.txt`, 42/42):

| Servicio detenido | `/salud` | Reflejado en | Además |
|---|---|---|---|
| Sesión | 200 `degradado`, solo `sesion` caída | 5,0 s | Cambio registrado en `salud:eventos`; `exportar` sigue respondiendo; guardar borrador da 503 |
| Historial | 200 `degradado`, solo `historial` caída | 6,0 s | Cambio registrado en `salud:eventos`; `exportar` sigue respondiendo |
| Registro | 503 `caido` | 5,0 s | Cambio registrado en `salud:eventos`; `exportar` da 503 |
| Valkey | 200 `degradado`, `fuente: directo`; Sesión e Historial caídos | 16,0 s | Al volver Valkey, `fuente: valkey` y `operativo` |

En todos los casos `/salud` vuelve a `operativo` al arrancar el servicio. Los
tiempos de 5–6 s corresponden al intervalo del monitor. El caso de Valkey tardó
16,0 s en reflejarse; no se midió el desglose de ese tiempo.

Resultados funcionales: rutas básicas 14/14 (`p1_gateway_smoke.txt`) y flujo
completo 26/26 (`p1_gateway_flujo.txt`), que incluye que los cinco tipos de
evento pasan la validación de Historial y que ningún evento queda pendiente.

**Contrato de eventos.** El registro arma cada evento dentro de la transición y
el gateway lo publica sin modificarlo; Historial lo valida con un esquema
estricto (`extra=forbid`). Los campos por tipo de evento están en
[`endpoints/README.md`](../endpoints/README.md). Para la analítica:

- Q3 lee `duracion_ms` del evento `tarea_anotada` (toma -> envío).
- Q5 calcula toma -> aprobación con `tomada_en` y `aprobada_en` del evento
  `revision_aprobada`.
- Q4 cuenta las filas `rechazada`.

---

## 4. Recorrido completo de una tarea (ejemplo LiDAR)

Ejemplo **ilustrativo**: los cuerpos tienen la forma real de la API
(`endpoints/README.md`), pero los valores y los ids (abreviados)
son de ejemplo. El flujo completo está verificado en `p1_gateway_flujo.txt`
(26/26) con tareas de imagen; la forma es la misma para LiDAR.

**1. Crear el proyecto** — `POST /proyectos`

```json
{"cliente_id": "c1…", "nombre": "Conducción urbana",
 "taxonomia": [
   {"etiqueta": "lidar#peaton", "instruccion": "Persona a pie."},
   {"etiqueta": "lidar#vehiculo", "instruccion": "Automóvil, camión o bus."},
   {"etiqueta": "lidar#ciclista", "instruccion": "Ciclista; un skater se clasifica aquí."}],
 "sla_horas": 48}
```

El gateway lo reenvía al registro, que valida que el cliente exista (404 si
no) y responde 201 con el proyecto.

**2. Registrar un lote** — `POST /proyectos/{id}/lotes`

```json
{"nombre": "Barrido 2026-11-20",
 "tareas": [{"modalidad": "lidar", "tipo_tarea": "cuboide_3d", "prioridad": 5,
   "archivo": {"uri": "s3://lidar/barrido-20/frame-00042.pcd",
               "sha256": "9f2c…", "bytes": 18874368,
               "mime": "application/octet-stream",
               "meta": {"sensor": "VLP-32", "puntos": 589824}}}]}
```

El registro comprueba que la geometría corresponda a la modalidad (LiDAR solo
admite `cuboide_3d`; otra combinación da 422) e inserta las tareas en
`disponible`, versión 1. El archivo original no se guarda: solo su referencia.

**3. Pedir la siguiente tarea** — `POST /tareas/siguiente`
`{"anotador_id": "a7…", "proyecto_id": "p3…"}`

1. Gateway -> Registro: candidatas del proyecto (`disponible` por prioridad
   descendente y, después, `abierta` con plazo vencido).
2. Gateway -> Sesión: `SET NX PX 120000` sobre `lock:tarea:{id}`. Si otro tiene
   el lock, prueba la siguiente candidata.
3. Gateway -> Registro: `asignar`, un `findOneAndUpdate` que exige estado
   `disponible` (o `abierta` vencida). Si responde 409, el gateway suelta el lock
   y prueba otra candidata.
4. Gateway -> Historial: publica `tarea_abierta` y lo confirma en el registro.
5. Respuesta 200: `tarea` (estado `abierta`, versión 2), `token`, `ttl_ms` y
   `borrador` si el anotador tenía uno de esa tarea. Si no hay ninguna libre: 204.

| MongoDB | Valkey | Parquet |
|---|---|---|
| `abierta`, versión 2, `asignacion.token` | `lock:tarea:{id}`, `sesion:{anotador}` | fila `abierta` |

**4. Borrador cada 2 s** — `PUT /tareas/{id}/borrador`

```json
{"anotador_id": "a7…", "proyecto_id": "p3…", "token": "t9…",
 "contenido": {"cuboides": [{"etiqueta": "lidar#ciclista",
   "centro": [12.4, -3.1, 0.9], "dimensiones": [1.8, 0.6, 1.7], "yaw": 0.12}]}}
```

Sesión guarda el último borrador y renueva el lock. No toca MongoDB ni genera
fila en Parquet. Si la conexión se corta, `POST /sesiones/recuperar` devuelve
el borrador (estado `activa` si el lock sigue vigente, `requiere_reasignacion`
si venció).

**5. Enviar** — `POST /tareas/{id}/enviar` (mismo cuerpo que el borrador)

1. Gateway -> Sesión: latido; el token debe ser el dueño del lock (409 si no).
2. Gateway -> Registro: `enviar`, filtrando por estado `abierta` y token. Un
   segundo envío responde 409. Si MongoDB no responde: 503 sin reintento, y el
   borrador y el lock siguen en Valkey para reenviar.
3. Publica `tarea_anotada` (con `duracion_ms` = toma -> envío) y suelta el lock.

**6. Revisión con rechazo** — `POST /tareas/{id}/revision`
`{"revisor_id": "r2…", "decision": "rechazar", "motivo": "cuboide_incompleto"}`

`iniciar_revision` (`anotada -> en_revision`) y `rechazar`. En MongoDB la tarea
vuelve a `disponible`; en Parquet la fila queda `rechazada` con su
`motivo_rechazo`. Sin `motivo` responde 422; un actor que no es revisor, 403.

**7. Retrabajo y aprobación.** La tarea vuelve a ofrecerse en
`/tareas/siguiente`; tras el nuevo envío, `{"decision": "aprobar"}` la deja en
`aprobada` y publica `revision_aprobada` con `tomada_en` y `aprobada_en`
(base de Q5). Aprobar otra vez responde 409.

**8. Exportar** — `GET /proyectos/{id}/exportar?limite=100`: tareas
`aprobada` paginadas por `_id`, con el cursor `siguiente`.

| Paso | MongoDB | Fila en Parquet |
|---|---|---|
| siguiente | `abierta` | `abierta` |
| enviar | `anotada` | `anotada` |
| inicio de revisión | `en_revision` | `en_revision` |
| rechazo | `disponible` | `rechazada` |
| aprobación | `aprobada` | `aprobada` |

Liberar una tarea y guardar borradores no generan filas.

---

## 5. Registro operacional: modelo, índices y concurrencia

### 5.1 Modelo del registro e índices

Base `plataforma`, colecciones `clientes`, `proyectos`, `batches`, `tareas` y
`anotadores`, con identificadores UUID.

```json
{"_id": "uuid", "proyecto_id": "uuid", "batch_id": "uuid", "cliente_id": "uuid",
 "modalidad": "lidar", "tipo_tarea": "cuboide_3d", "prioridad": 5,
 "archivo": {"uri": "…", "sha256": "…", "bytes": 0, "mime": "…", "meta": {}},
 "estado": "disponible", "version": 1, "sla_horas": 48,
 "asignacion": {"anotador_id": "…", "equipo_id": "…", "token": "…",
                "tomada_en": "…", "vence_en": "…"},
 "anotacion": {"contenido": {}, "enviada_en": "…"},
 "revision": {"revisor_id": "…", "iniciada_en": "…", "resultado": "…",
              "motivo": "…", "revisada_en": "…"},
 "ultimo_evento_id": "uuid", "eventos_pendientes": [], "creado_en": "…"}
```

**Estados y transiciones.** Estados en MongoDB: `disponible`, `abierta`,
`anotada`, `en_revision`, `aprobada`. Cada transición es **un único**
`findOneAndUpdate` cuyo filtro exige el estado de origen (y el token o el
revisor cuando corresponde). Si el filtro no coincide no se modifica nada y se
responde 409. No hay lectura previa seguida de escritura. La versión sube en
cada transición válida.

**Evento en la misma operación.** La actualización es un pipeline de
agregación: en la misma escritura atómica cambia el estado, sube la versión y
construye el evento a partir de los campos de la tarea, que queda en
`eventos_pendientes`. Esto evita perder el evento si el proceso cae entre la
escritura y la publicación (patrón *outbox*). El gateway lo publica y lo
confirma; si Historial falla, un reconciliador del gateway reenvía los
pendientes cada 10 s, y Historial deduplica por `event_id`.

**Índices** (`p2_replica.txt` lista los índices creados):

| Índice | Campos | Consulta que lo usa |
|---|---|---|
| `tareas_proyecto_estado_prioridad` | `proyecto_id, estado, prioridad:-1` | Candidatas de `/tareas/siguiente`: igualdad por proyecto y estado, orden por prioridad (regla ESR: igualdad antes que orden, así el orden sale del índice sin ordenar en memoria) |
| `tareas_anotador_estado` | `asignacion.anotador_id, estado` | Tareas de un anotador (`GET /tareas?anotador_id=…&estado=…`) |
| `tareas_batch` | `batch_id` | Tareas de un lote |
| `proyectos_cliente_estado` | `cliente_id, estado` | Proyectos de un cliente por estado |

Son los cuatro índices mínimos del enunciado; no se creó ningún índice
adicional. Las transiciones buscan por `_id`, que ya tiene su índice. La
exportación filtra por `proyecto_id` y `estado = aprobada` y ordena por `_id`:
aprovecha el prefijo del primer índice, aunque el orden por `_id` no sale de
ese índice. **No se capturaron planes de ejecución (`explain`)** de estas
consultas; es una medición no disponible.

### 5.2 Prueba de que no hay doble adquisición

**Mecanismo, en dos capas.** El lock de Valkey (`SET NX PX`) evita que dos
peticiones trabajen la misma tarea a la vez. La garantía final es el
`findOneAndUpdate` filtrado por estado en MongoDB: aunque dos peticiones
llegaran juntas al registro, solo una encuentra la tarea en `disponible`.

**A nivel de registro** (`p2_registro_smoke.txt`, 30/30): 50 intentos
simultáneos de asignar **la misma tarea** -> exactamente 1 respuesta 200 y 49
respuestas 409. La tarea quedó `abierta`, versión 2, con el token del ganador.
Un segundo envío responde 409.

**A nivel de gateway, 50 contra 10** (`p2_concurrencia_gateway.txt`, 9/9): 50
anotadores piden `/tareas/siguiente` a la vez sobre un proyecto con 10 tareas.

| Resultado | Valor |
|---|---|
| Respuestas 200 | 10, todas con tareas distintas |
| Respuestas 204 | 40 |
| Otros códigos | 0 |
| En MongoDB | 10 tareas `abierta`, cada una con un dueño distinto, todas en versión 2 (una sola asignación) y con el mismo dueño que recibió el 200 |
| Latencia de `/tareas/siguiente` (n = 50) | p50 1056,5 ms; p95 1090,9 ms; p99 1093,5 ms; máx. 1093,5 ms |
| Duración de la ráfaga | 1102,8 ms |

**Cero dobles asignaciones** en ambas pruebas. La latencia es alta bajo
contención. No se midió el desglose; la hipótesis es que cada petición que no
consigue tarea recorre hasta 10 candidatas haciendo una llamada HTTP a Sesión
por cada una. Es un candidato a optimizar antes de la prueba de carga (P6).

### 5.3 Diferencia observada entre `w:1` y `w:majority`

Medición con el driver, sin gateway, de las dos escrituras del registro
(`p2_write_concern.txt`): 500 operaciones por hilo y repetición, 3
repeticiones en orden alternado, con 1 y 8 hilos.

| Hilos | Operación | p50 `w:1` | p50 `majority` | Relación | ops/s `w:1` | ops/s `majority` |
|---|---|---|---|---|---|---|
| 1 | `insert_one` | 0,52 ms | 2,66 ms | ×5,15 | 1552 | 346 |
| 1 | `find_one_and_update` | 0,71 ms | 2,77 ms | ×3,91 | 1552 | 346 |
| 8 | `insert_one` | 3,44 ms | 6,28 ms | ×1,83 | 2123 | 1229 |
| 8 | `find_one_and_update` | 3,51 ms | 6,37 ms | ×1,82 | 2123 | 1229 |

**Interpretación.**

- Con un hilo, `w:majority` espera a que un secundario aplique la escritura: la
  latencia se multiplica entre 3,9 y 5,2 veces y el rendimiento cae de 1552 a
  346 ops/s (−78 %).
- Con 8 hilos las esperas se solapan y la diferencia relativa baja a ×1,8
  (2123 frente a 1229 ops/s, −42 %).
- **Limitación:** los tres nodos corren en la misma máquina, así que la
  replicación no paga latencia de red real. Estas cifras no permiten inferir el
  comportamiento de un clúster distribuido en varias máquinas.
- **Garantía:** `w:1` confirma una escritura que aún no está en la mayoría y
  que puede perderse en un rollback tras una elección. El caso C de la sección 12.1
  muestra que una escritura puede quedar aplicada en un primario aislado sin
  haber llegado a la mayoría. Con `w:majority` el cliente recibió 503; con `w:1`
  habría recibido 200 por una escritura que no estaba replicada. Esto último no
  se midió con `w:1`.

Se mantiene `w:majority`: el costo medido es de milisegundos y la garantía es
que ninguna escritura confirmada se pierde (RPO 0, sección 12.1).

---

## 6. Sesión, recuperación del borrador y reinicio de Valkey

*Responsable: Christian.*

**Diseño.**

- La fuente de verdad de la tarea es MongoDB. Valkey solo conserva sesiones
  temporales y borradores, sin modificar el estado definitivo.
- Scripts Lua adquieren, renuevan, liberan y validan los locks sin intercalar
  operaciones concurrentes. El token impide liberar un lock ajeno.
- Llaves: `lock:tarea:{id}` (`SET NX PX`, 120 s, se renueva con el latido y con
  cada borrador), `sesion:{anotador}` (tarea, proyecto, etapa y token),
  `borrador:{tarea}` (último borrador, sin TTL para poder recuperarlo) y
  `editando:{proyecto}` (sesiones activas).
- Los borradores no generan eventos.
- Si el lock venció, la recuperación devuelve el borrador en modo de solo
  lectura y `requiere_reasignacion`; la decisión de reasignar la toma el
  registro a través del gateway (`/tareas/siguiente` vuelve a ofrecer la tarea
  y devuelve el borrador al mismo anotador).
- Persistencia: AOF con `appendonly yes` y `appendfsync everysec`.

**Qué pasa al reiniciar Valkey.**

> **PENDIENTE** — resultado de P3 sobre el Compose final. Como referencia, la prueba
del entorno aislado (`smoke_integrante2_20261122T055742Z.txt`) recuperó el
borrador después de reiniciar Valkey, con la sesión en estado `activa`. Ese
entorno no usa el gateway, así que no sustituye a P3.

**Resultados de P3** (criterios del enunciado):

| Criterio | Resultado | Evidencia |
|---|---|---|
| El borrador recuperado tiene como máximo 5 s de antigüedad | PENDIENTE | PENDIENTE |
| Tras reiniciar Valkey, el borrador sigue | PENDIENTE (ver referencia del entorno aislado) | PENDIENTE |
| Un lock vencido devuelve la tarea a la cola | PENDIENTE | PENDIENTE |
| Ninguna tarea queda con dos dueños | PENDIENTE (ver también sección 5.2) | PENDIENTE |

---

## 7. Datalake Parquet

*Responsable: Christian.*

**Diseño.**

- El gateway publica cada evento en `POST /eventos` solo después de que
  MongoDB confirmó la transición. Historial lo encola en Valkey Streams de
  forma idempotente por `event_id` (202 nuevo, 200 repetido, 409 si el mismo
  `event_id` llega con otro contenido).
- Un worker independiente lee el grupo `historial-parquet` y escribe cada
  micro-lote primero en `.staging`; luego lo sincroniza y lo publica con
  `os.replace` dentro del mismo volumen. Solo después confirma con `XACK`.
- Partición mensual por `ocurrido_en` en UTC: `eventos/mes=AAAA-MM/`.
- Si un worker muere antes del `XACK`, el mensaje queda pendiente y otro lo
  reclama con `XAUTOCLAIM`. Repetir el mismo micro-lote produce el mismo nombre
  de archivo.

**Archivos a medias.** Spark solo debe leer `*.parquet` publicados; los
temporales viven en `.staging/`. La publicación es atómica por archivo.

**Duplicados por `event_id`.** El consumo es *al menos una vez*: dos
micro-lotes solapados pueden contener el mismo `event_id` en archivos
distintos. Spark aplica `dropDuplicates(["event_id"])` antes de Q1–Q5.

**Fila rechazada.** Un rechazo deja la tarea en `disponible` en MongoDB y una
fila `rechazada` en Parquet (sección 4). Q4 cuenta esas filas.

**Compactación.**

> **PENDIENTE** — el enunciado pide documentar la compactación de los micro-lotes
(archivos pequeños). Falta definir si se compacta, cuándo y cómo se mantiene
la deduplicación.

**Resultados de P4** (criterios del enunciado):

| Criterio | Resultado | Evidencia |
|---|---|---|
| Archivos Parquet válidos, sin lectura a medias | PENDIENTE | PENDIENTE |
| Reproceso de un micro-lote y deduplicación por `event_id` | PENDIENTE | PENDIENTE |
| El historial se materializa en el datalake | Referencia: el entorno aislado publicó un archivo en `mes=2026-11/` (`smoke_integrante2_20261122T055742Z.txt`) | PENDIENTE sobre el Compose final |
| Frescura: segundos entre un HTTP 200 y el evento visible en Spark | PENDIENTE | PENDIENTE |

---

## 8. Datos de prueba

*Responsable: Emmanuel.*

Generador reproducible implementado en `proyecto2/dataset/generar_dataset.py` con semilla fija (`20261122`), cargado directamente en MongoDB (base `plataforma`) mediante `pymongo` y materializado como archivos Parquet mensuales en `proyecto2/datalake/eventos/mes=AAAA-MM/` siguiendo el esquema de `servicios/historial/app/parquet_writer.py` (145 archivos Parquet publicados en el datalake).

| Entidad | Volumen de referencia (enunciado) | Generado | Evidencia |
|---|---|---|---|
| Clientes | 60 | 60 (volumen objetivo configurado en `generar_dataset.py`; conteo exacto en MongoDB no volcado a `evidence/`) | `proyecto2/dataset/generar_dataset.py` |
| Proyectos | 180: 120 en el registro y 60 cerrados solo en el historial | 180 (120 activos en MongoDB + 60 cerrados solo en Parquet; conteo exacto no volcado a `evidence/`) | `proyecto2/dataset/generar_dataset.py` |
| Batches | 600 | 600 (configurado en `generar_dataset.py`) | `proyecto2/dataset/generar_dataset.py` |
| Tareas del registro | 60 000: 30 % texto, 30 % imagen, 20 % video, 20 % LiDAR | 60 000 con distribución 30 % texto, 30 % imagen, 20 % video, 20 % LiDAR (configurado en `generar_dataset.py`) | `proyecto2/dataset/generar_dataset.py` |
| Anotadores | 300 en 30 equipos; 10 % revisores | 300 en 30 equipos, 10 % revisores (configurado en `generar_dataset.py`) | `proyecto2/dataset/generar_dataset.py` |
| Eventos históricos | ~800 000 en doce meses y 100 000 tareas | ~800 000 eventos distribuidos en 12 meses (`mes=2025-01` a `mes=2025-12`, 145 archivos Parquet publicados) | `p5_analitica.txt` (`archivos_parquet: 145`) |
| `eventos_grande` (opcional) | Hasta ~10 millones de filas | No generado (medición no disponible; se evaluó sobre el conjunto `principal` de 145 archivos) | `p5_analitica.txt` |

- **Método de generación y semilla:** generación determinista en Python (`random.Random(20261122)`), respetando los contratos de documento de tarea (sección 5.1, incluyendo geometrías válidas por modalidad) y el esquema estricto de eventos Parquet (sección 3). En `proyecto2/datalake/eventos` hay 145 archivos Parquet repartidos en las particiones históricas `mes=2025-01` a `mes=2025-12`, además de `.staging` y particiones residuales de pruebas (`mes=2026-01` y `mes=2026-11`). El parámetro `conjunto="grande"` existe en la validación del servicio de analítica, pero no se generó el conjunto ampliado `eventos_grande` (~10M de filas).
- **Comandos de carga:**
  - Carga en MongoDB: `docker compose -f proyecto2/docker-compose.yml run --rm registro python /dataset/generar_dataset.py --solo mongo`
  - Generación del datalake Parquet: `docker compose -f proyecto2/docker-compose.yml run --rm analitica python /dataset/generar_dataset.py --solo parquet`
- **Registro de regeneraciones:** se ejecutó una única carga inicial limpia con semilla `20261122` antes de las pruebas P5 y P6. En P6 se utilizó el **Proyecto 084** (`600d1c8a-fc7b-5126-81a0-8775bd5adb36`) con `420` tareas disponibles al inicio (`377` al comenzar la repetición 1 de 10 usuarios), las cuales se agotaron a `0` durante la repetición 1 de 40 usuarios; por ello, las repeticiones 2 y 3 de 40 usuarios y las tres repeticiones de 100 usuarios operaron con `0` tareas disponibles y respondieron `204` en `POST /tareas/siguiente`.

---

## 9. Analítica con Spark (Q1–Q5)

*Responsable: Emmanuel.*

Servicio interno `analitica` (`proyecto2/servicios/analitica/`, puerto interno `8004` sin exposición al host) implementado con FastAPI + PySpark (`local[N]`), montando `./datalake` en solo lectura (`ro`) y filtrando exclusivamente `*.parquet` publicados (ignorando `.staging/`).

| Consulta | Pregunta de negocio (enunciado) | Definición / implementación | Resultado validado (`p5_analitica.txt`) |
|---|---|---|---|
| Q1 | Tareas aprobadas por día y modalidad, últimos 90 días | Deduplicación por `event_id`, filtro `tipo_evento == 'revision_aprobada'` en ventana de 90 días, agrupación por fecha (`ocurrido_en`) y `modalidad` | 60 filas retornadas (`total_filas: 60`) |
| Q2 | Aprobadas / revisadas, por equipo y mes | Deduplicación por `event_id`, agrupado por `modalidad` y `equipo_id` con conteo aproximado de `tarea_id` y total de eventos | 120 filas retornadas (`total_filas: 120`) |
| Q3 | Duración media y p95, por modalidad y tipo de tarea | Deduplicación por `event_id`, filtro `tipo_evento == 'tarea_anotada'`, cálculo de promedio y percentil 95 de `duracion_ms` por `modalidad` y `tipo_tarea` | 4 filas retornadas (`total_filas: 4`, una por modalidad/geometría) |
| Q4 | Rechazadas / revisadas, por cliente y proyecto | Deduplicación por `event_id`, eventos `revision_aprobada` y `revision_rechazada`; proporción de rechazadas sobre revisadas por `cliente_id` y `proyecto_id` | 180 filas retornadas (`total_filas: 180`, una por proyecto) |
| Q5 | Toma -> aprobación: p50 y p95 por cliente y % fuera de SLA | Deduplicación por `event_id`, filtro `tipo_evento == 'revision_aprobada'`; p50 y p95 de `(aprobada_en - tomada_en)` en horas y porcentaje con `horas > sla_horas`, por `cliente_id` | 60 filas retornadas (`total_filas: 60`, una por cliente) |

- **Validación con datos conocidos o casos controlados:** las cinco consultas ejecutan `dropDuplicates(["event_id"])` antes de filtrar/agregar y devuelven conteos consistentes con el modelo generado (por ejemplo, Q3 devuelve exactamente 4 combinaciones modalidad/geometría y Q1 devuelve 60 filas diarias/modalidad). No se ejecutó una suite unitaria aislada con micro-datasets sintéticos de respuesta fija; la validación se realizó sobre los 145 archivos Parquet del conjunto principal (`p5_analitica.txt`).
- **Plan de ejecución de Q1 (`explain` en `p5_analitica.txt`):**
  - `FileScan parquet` sobre 145 rutas en `/datalake/eventos` con *pushdown* de predicados sobre `ocurrido_en` (`IsNotNull(ocurrido_en)`, `GreaterThanOrEqual`, `LessThan`).
  - Para deduplicar por `event_id`, Spark aplica un `SortAggregate` parcial local (`partial_first`), seguido de intercambio por red/memoria `Exchange hashpartitioning(event_id, 2)`, reordenamiento `Sort [event_id ASC]` y `SortAggregate` final antes del agrupamiento por día y modalidad.
- **Plan de ejecución de Q3 (`explain` en `p5_analitica.txt`):**
  - Presenta la misma etapa inicial de lectura columnar (`FileScan parquet` con `Batched: true` y `PushedFilters` en `ocurrido_en`) seguida de `Sort [event_id ASC]` -> `SortAggregate` parcial -> `Exchange hashpartitioning(event_id, 2)` -> `Sort` -> `SortAggregate` final para garantizar unicidad por `event_id` previo al cálculo de promedio y percentil de `duracion_ms`.
- **Tiempos (`p5_analitica.txt`, 145 archivos Parquet, ventana `2026-11-24T21:58:56Z` a `22:07:25Z`):**

| Consulta | `w=1` — 1.ª ejec. (ms) | `w=1` — 2.ª ejec. (ms) | `w=2` — 1.ª ejec. (ms) | `w=2` — 2.ª ejec. (ms) | Filas |
|---|---|---|---|---|---|
| Q1 | 53 362,02 (srv: 53 315,37) | 24 282,85 (srv: 24 258,99) | 37 851,87 (srv: 37 480,12) | 18 637,70 (srv: 18 402,61) | 60 |
| Q2 | 29 364,12 (srv: 29 296,77) | 26 617,58 (srv: 26 505,75) | 18 657,03 (srv: 18 555,07) | 15 988,97 (srv: 15 520,10) | 120 |
| Q3 | 28 206,96 (srv: 28 029,89) | 23 760,64 (srv: 23 451,22) | 17 581,89 (srv: 17 436,97) | 16 120,31 (srv: 15 852,12) | 4 |
| Q4 | 25 008,40 (srv: 24 251,22) | 27 282,06 (srv: 26 499,47) | 16 362,99 (srv: 16 296,08) | 16 065,22 (srv: 16 022,95) | 180 |
| Q5 | 25 851,09 (srv: 25 399,97) | 23 368,76 (srv: 23 297,77) | 16 377,97 (srv: 16 308,51) | 15 583,54 (srv: 15 486,18) | 60 |

  - *Comparativa 1 frente a 2 trabajadores y conjunto ampliado:* tras parametrizar `SPARK_WORKERS` y recrear `analitica`, `/salud` reportó `"spark_workers": 1` en la primera ronda y `"spark_workers": 2` en la segunda (`local[2]` dentro del mismo contenedor en WSL2, no un clúster multi-nodo). En las segundas ejecuciones los tiempos pasan de 23,37–27,28 s (`w=1`) a 15,58–18,64 s (`w=2`), manteniéndose el efecto de calentamiento de JVM/caché entre la 1.ª y la 2.ª ejecución. El conjunto ampliado `eventos_grande` (~10M filas) no está generado y se declara **no disponible**.
- **Limitaciones de Spark en el entorno utilizado:**
  - Ejecución local dentro de un contenedor en WSL2 (`local[1]` y `local[2]`), sin distribución multi-nodo real.
  - La deduplicación global `dropDuplicates(["event_id"])` sobre 145 archivos exige un `Sort` + `Exchange` completo de todas las columnas antes de agregar, lo que domina el costo de CPU/memoria en cada consulta.
  - En los planes físicos de Q1 y Q3 `PartitionFilters: []` aparece vacío porque el filtro se aplicó sobre `ocurrido_en` (`>= 2025-01-01 00:00:00` y `< 2026-01-01 00:00:00`) y no sobre la columna de partición `mes`, leyendo los 145 archivos.

---

## 10. Pruebas y evidencia experimental

Cada prueba deja un archivo en `evidence/` con la fecha y hora UTC, el
objetivo, la máquina y la configuración. Las latencias se reportan como p50,
p95 y p99 con el número de muestras. El formato de cada entrada está en el
apéndice A.

| Prueba | Criterio del enunciado | Resultado | Sección |
|---|---|---|---|
| P1. REST y despliegue | Rutas, `/salud` normal, degradado y caído; solo el gateway expuesto | Rutas 14/14; flujo 26/26; salud por capa con Valkey, puertos y aislamiento 42/42. Falta el tráfico del simulador | 3 |
| P2. Registro y concurrencia | 10 asignaciones y 40 sin tarea; ninguna tarea con dos personas; segundo envío 409 | 10×200 y 40×204, cero dobles asignaciones; segundo envío 409 | 5.2 |
| P3. Sesión | Borrador ≤ 5 s; sigue tras reiniciar Valkey; ninguna tarea con dos dueños | PENDIENTE | 6 |
| P4. Historial | Parquet válido sin lectura a medias; segundos entre HTTP 200 y Spark | PENDIENTE | 7 |
| P5. Analítica | Resultados validados, planes de Q1 y Q3, limitaciones | Q1–Q5 ejecutadas sobre 145 archivos Parquet (`spark_workers=1` y `spark_workers=2`, 1.ª vs. 2.ª ejecución) y planes `explain` de Q1 y Q3 capturados; conjunto grande no disponible | 9 |
| P6. Carga | p50/p95/p99 por endpoint, envíos por minuto, errores, `docker stats` | Probado con 10, 40 y 100 usuarios (3 repeticiones de 90 s por nivel sobre Proyecto 084, 420 tareas iniciales): latencias prom/p50/p95/p99, códigos HTTP (`200`, `204`, `504`), acciones, agotamiento a `0` tareas en 40u rep. 1 y `docker stats` cada 15 s | 11 |
| P8. Fallas | Con un nodo caído se escribe; sin mayoría no; lo confirmado sigue tras la elección; el borrador sobrevive al reinicio de Valkey | MongoDB 14/14; Valkey PENDIENTE | 12 |

---

## 11. Resultados de carga y cuellos de botella (P6)

*Responsable: Emmanuel.*

Prueba ejecutada sobre el gateway (`http://localhost:8000`) con **10, 40 y 100 usuarios virtuales concurrentes** (3 repeticiones de 90 s por nivel, muestreo de `docker stats` cada 15 s y fin registrado en `2026-11-24T16:34:31Z` en `p6_carga.txt`) contra el **Proyecto 084** (`600d1c8a-fc7b-5126-81a0-8775bd5adb36`), que partió con `420` tareas disponibles (`377` al iniciar la repetición 1 de 10 usuarios).

### 11.1 Nivel de 10 usuarios (3 repeticiones)

| Rep. (tareas disp.) | Endpoint | Muestras | Prom (ms) | p50 (ms) | p95 (ms) | p99 (ms) | Códigos HTTP / Acciones |
|---|---|---|---|---|---|---|---|
| Rep. 1 (`377` -> `262`) | `POST /tareas/siguiente` | 170 | 211,97 | 172,12 | 480,58 | 642,39 | `200`: 170 (`tarea_tomada`: 170) |
| Rep. 1 (`377` -> `262`) | `PUT /tareas/{id}/borrador` | 248 | 41,27 | 30,05 | 97,35 | 171,00 | `200`: 248 (`borrador_guardado`: 248) |
| Rep. 1 (`377` -> `262`) | `POST /tareas/{id}/enviar` | 138 | 168,30 | 131,26 | 354,39 | 585,76 | `200`: 138 (`tarea_enviada`: 138) |
| Rep. 1 (`377` -> `262`) | `POST /tareas/{id}/revision` | 132 | 236,37 | 204,47 | 406,48 | 626,49 | `200`: 132 (aprob.: 109, rech.: 23) |
| Rep. 1 (`377` -> `262`) | `POST /tareas/{id}/liberar` | 32 | 63,32 | 52,62 | 131,77 | 148,97 | `200`: 32 (`tarea_liberada`: 32) |
| Rep. 1 (`377` -> `262`) | `GET /proyectos/{id}/exportar` | 2 | 232,38 | 223,85 | 240,91 | 240,91 | `200`: 2 (`exportar_ok`: 2) |
| Rep. 1 (`377` -> `262`) | `GET /analitica/{q}` | 2 | 103 597,96 | 103 562,92 | 103 633,00 | 103 633,00 | `200`: 2 (`analitica_ok`: 2) |
| Rep. 2 (`262` -> `160`) | `POST /tareas/siguiente` | 156 | 321,31 | 292,15 | 637,92 | 878,66 | `200`: 156 (`tarea_tomada`: 156) |
| Rep. 2 (`262` -> `160`) | `PUT /tareas/{id}/borrador` | 229 | 61,04 | 48,72 | 163,38 | 213,50 | `200`: 229 (`borrador_guardado`: 229) |
| Rep. 2 (`262` -> `160`) | `POST /tareas/{id}/enviar` | 133 | 240,51 | 200,30 | 507,31 | 771,36 | `200`: 133 (`tarea_enviada`: 133) |
| Rep. 2 (`262` -> `160`) | `POST /tareas/{id}/revision` | 127 | 350,90 | 334,26 | 564,84 | 743,39 | `200`: 127 (aprob.: 96, rech.: 31) |
| Rep. 2 (`262` -> `160`) | `POST /tareas/{id}/liberar` | 23 | 122,35 | 101,47 | 288,87 | 435,21 | `200`: 23 (`tarea_liberada`: 23) |
| Rep. 2 (`262` -> `160`) | `GET /proyectos/{id}/exportar` | 2 | 127,81 | 94,76 | 160,86 | 160,86 | `200`: 2 (`exportar_ok`: 2) |
| Rep. 2 (`262` -> `160`) | `GET /analitica/{q}` | 2 | 120 077,00 | 120 053,09 | 120 100,92 | 120 100,92 | `504`: 2 |
| Rep. 3 (`160` -> `59`) | `POST /tareas/siguiente` | 148 | 351,85 | 316,25 | 887,27 | 1211,89 | `200`: 148 (`tarea_tomada`: 148) |
| Rep. 3 (`160` -> `59`) | `PUT /tareas/{id}/borrador` | 227 | 63,00 | 45,73 | 149,77 | 337,68 | `200`: 227 (`borrador_guardado`: 227) |
| Rep. 3 (`160` -> `59`) | `POST /tareas/{id}/enviar` | 128 | 250,84 | 193,93 | 594,18 | 1023,63 | `200`: 128 (`tarea_enviada`: 128) |
| Rep. 3 (`160` -> `59`) | `POST /tareas/{id}/revision` | 122 | 378,48 | 339,77 | 742,05 | 1231,75 | `200`: 122 (aprob.: 95, rech.: 27) |
| Rep. 3 (`160` -> `59`) | `POST /tareas/{id}/liberar` | 20 | 123,53 | 83,10 | 291,50 | 406,26 | `200`: 20 (`tarea_liberada`: 20) |
| Rep. 3 (`160` -> `59`) | `GET /proyectos/{id}/exportar` | 2 | 284,10 | 252,94 | 315,26 | 315,26 | `200`: 2 (`exportar_ok`: 2) |
| Rep. 3 (`160` -> `59`) | `GET /analitica/{q}` | 2 | 120 093,23 | 120 088,09 | 120 098,37 | 120 098,37 | `504`: 2 |

### 11.2 Nivel de 40 usuarios (3 repeticiones)

| Rep. (tareas disp.) | Endpoint | Muestras | Prom (ms) | p50 (ms) | p95 (ms) | p99 (ms) | Códigos HTTP / Acciones |
|---|---|---|---|---|---|---|---|
| Rep. 1 (`44` -> `0`) | `POST /tareas/siguiente` | 1741 | 776,83 | 616,66 | 1707,66 | 3334,81 | `200`: 84, `204`: 144, `504`: 1513 (`tarea_tomada`: 84) |
| Rep. 1 (`44` -> `0`) | `PUT /tareas/{id}/borrador` | 114 | 503,23 | 395,35 | 966,64 | 2021,91 | `200`: 114 (`borrador_guardado`: 114) |
| Rep. 1 (`44` -> `0`) | `POST /tareas/{id}/enviar` | 78 | 1349,37 | 1257,48 | 2556,72 | 3015,01 | `200`: 78 (`tarea_enviada`: 78) |
| Rep. 1 (`44` -> `0`) | `POST /tareas/{id}/revision` | 75 | 1771,30 | 1610,21 | 3402,28 | 3434,17 | `200`: 75 (aprob.: 53, rech.: 22) |
| Rep. 1 (`44` -> `0`) | `POST /tareas/{id}/liberar` | 6 | 677,53 | 392,95 | 1402,52 | 1402,52 | `200`: 6 (`tarea_liberada`: 6) |
| Rep. 1 (`44` -> `0`) | `GET /proyectos/{id}/exportar` | 8 | 912,80 | 1089,32 | 1119,46 | 1119,46 | `200`: 8 (`exportar_ok`: 8) |
| Rep. 1 (`44` -> `0`) | `GET /analitica/{q}` | 8 | 120 091,33 | 120 099,25 | 120 102,99 | 120 102,99 | `504`: 8 |
| Rep. 2 (`0` -> `0`) | `POST /tareas/siguiente` | 5710 | 127,52 | 87,71 | 367,32 | 643,17 | `204`: 5710 (sin tareas disponibles) |
| Rep. 2 (`0` -> `0`) | `GET /proyectos/{id}/exportar` | 8 | 527,32 | 577,14 | 851,13 | 851,13 | `200`: 8 (`exportar_ok`: 8) |
| Rep. 2 (`0` -> `0`) | `GET /analitica/{q}` | 8 | 120 072,02 | 120 097,91 | 120 100,82 | 120 100,82 | `504`: 8 |
| Rep. 3 (`0` -> `0`) | `POST /tareas/siguiente` | 6398 | 86,77 | 51,96 | 256,33 | 495,64 | `204`: 6398 (sin tareas disponibles) |
| Rep. 3 (`0` -> `0`) | `GET /proyectos/{id}/exportar` | 8 | 805,91 | 910,15 | 1221,94 | 1221,94 | `200`: 8 (`exportar_ok`: 8) |
| Rep. 3 (`0` -> `0`) | `GET /analitica/{q}` | 8 | 120 059,27 | 120 083,46 | 120 101,64 | 120 101,64 | `504`: 8 |

### 11.3 Nivel de 100 usuarios (3 repeticiones, `0` -> `0` tareas disponibles)

| Rep. | Endpoint | Muestras | Prom (ms) | p50 (ms) | p95 (ms) | p99 (ms) | Códigos HTTP / Acciones |
|---|---|---|---|---|---|---|---|
| Rep. 1 | `POST /tareas/siguiente` | 7199 | 500,88 | 475,75 | 797,95 | 1023,06 | `204`: 7199 |
| Rep. 1 | `GET /proyectos/{id}/exportar` | 20 | 736,94 | 654,22 | 1051,03 | 1066,68 | `200`: 20 (`exportar_ok`: 20) |
| Rep. 1 | `GET /analitica/{q}` | 20 | 120 088,81 | 120 101,94 | 120 114,60 | 120 117,90 | `504`: 20 |
| Rep. 2 | `POST /tareas/siguiente` | 7469 | 473,06 | 435,13 | 798,79 | 918,31 | `204`: 7469 |
| Rep. 2 | `GET /proyectos/{id}/exportar` | 20 | 652,03 | 625,58 | 933,18 | 946,66 | `200`: 20 (`exportar_ok`: 20) |
| Rep. 2 | `GET /analitica/{q}` | 20 | 120 071,30 | 120 082,70 | 120 101,61 | 120 102,28 | `504`: 20 |
| Rep. 3 | `POST /tareas/siguiente` | 7413 | 479,43 | 447,63 | 773,38 | 939,11 | `204`: 7413 |
| Rep. 3 | `GET /proyectos/{id}/exportar` | 20 | 612,76 | 587,07 | 914,79 | 934,50 | `200`: 20 (`exportar_ok`: 20) |
| Rep. 3 | `GET /analitica/{q}` | 20 | 120 086,29 | 120 099,92 | 120 106,92 | 120 112,78 | `504`: 20 |

### 11.4 Cuellos de botella y observaciones

- **Agotamiento del inventario y paso a `204`:** con 10 usuarios las tareas disponibles pasan de `377` -> `262` -> `160` -> `59` sin agotarse. En la repetición 1 de 40 usuarios entran `44` tareas disponibles y caen a `0`; desde ese punto hasta el final de las 3 repeticiones de 100 usuarios, los anotadores no encuentran tareas libres y `POST /tareas/siguiente` responde `204` de forma sostenida (5710 y 6398 en 40u reps. 2–3; 7199, 7469 y 7413 en 100u reps. 1–3).
- **Contención y timeouts (`504`) bajo concurrencia de 40 usuarios:** en la repetición 1 de 40 usuarios, `POST /tareas/siguiente` registra `1513` respuestas `504` frente a `84` respuestas `200` y `144` `204`, con `p95` de `1707,66 ms` y `p99` de `3334,81 ms`, mientras `enviar` (`p95` `2556,72 ms`) y `revision` (`p95` `3402,28 ms`) suben con respecto a 10 usuarios (`p95` de `354,39–594,18 ms` y `406,48–742,05 ms`).
- **Saturación de `GET /analitica/{q}` al agotarse las tareas:** en la repetición 1 de 10 usuarios (`2` muestras) responde `200` en ~`103,6 s` (`103 597,96 ms` prom), pero a partir de la repetición 2 de 10 usuarios y en todos los niveles de 40 y 100 usuarios todas las consultas a `/analitica/{q}` alcanzan el umbral de ~`120,1 s` y devuelven `504` mientras Spark compite por CPU/memoria en el mismo host WSL2.
- **Recursos del sistema (`docker stats`):** capturados cada 15 s dentro de cada repetición en `p6_carga.txt` (por ejemplo, al cierre de la repetición 3 de 100 usuarios se observa `bda-mongo-2` en `102.55%` de CPU y `462.2 MiB / 11.53 GiB` de memoria, y `bda-proyecto2-valkey-1` en `1.18%` de CPU y `12.2 MiB / 11.53 GiB`).

---

## 12. Pruebas de fallas, recuperación, RTO y RPO (P8)

### 12.1 MongoDB

*Responsable: Jimmy.*

**Configuración** (`p8_mongo_fallas.txt`): conjunto `rs-plataforma` de 3 nodos,
`w: majority`, `wtimeout` 5 s, `serverSelectionTimeoutMS` 3 s,
`retryWrites=false`, `electionTimeoutMillis` 10 000.

**Método.**

- **Caída real:** se desactiva la política de reinicio del contenedor y se le
  envía SIGKILL, sin apagado ordenado ni cesión del rol de primario.
- **Tráfico:** todo pasa por el gateway, en ciclos `siguiente` + `enviar` con
  un anotador nuevo por ciclo.
- **Restauración:** tras cada caso se arrancan los nodos, se devuelve
  `restart: unless-stopped` y se espera a 1 PRIMARY + 2 SECONDARY y `/salud`
  operativo. Cada acción queda registrada con su marca de tiempo.

**Definiciones.**

- **RTO de escritura:** tiempo desde la caída hasta la primera escritura que
  vuelve a responder 200 por el gateway.
- **RPO:** escrituras confirmadas con 200 que se perdieron.

| Caso | Acción | Observado |
|---|---|---|
| A | SIGKILL a un secundario | `/salud` degradado (200) a los 6,4 s. 10/10 ciclos con 200: con un nodo caído se sigue escribiendo. Vuelta a operativo 3,5 s después del arranque |
| B | SIGKILL al primario | 2 ciclos con 503; el último fallido terminó a los 7,3 s y el primer 200 llegó a los 9,4 s. Nuevo primario detectado a los 9,3 s. 25/25 tareas con 200 intactas (una vez, mismo dueño y estado). El antiguo primario volvió como SECONDARY en 3,1 s |
| C | SIGKILL a los dos secundarios | 35,4 s sin mayoría: 7 envíos con 503 y ninguna escritura con 200; `/salud` 503 caído. Recuperación a 1 PRIMARY + 2 SECONDARY en 5,7 s. El reintento del envío dio 409 |
| Final | — | 26/26 tareas con 200 intactas. 5 asignaciones con 503 de resultado indeterminado |

**Análisis.**

- **Elección (B).** El nuevo primario se detectó a los 9,3 s, coherente con
  `electionTimeoutMillis` de 10 s: tras un SIGKILL los secundarios esperan ese
  plazo antes de declarar caído al primario. La detección se hace consultando
  `rs.status()` cada ~0,5 s, así que la cifra tiene esa resolución más la latencia
  de `docker exec`.
- **RTO de escritura entre 7,3 y 9,4 s.** Se reporta como rango porque cada
  ciclo fallido espera los 3 s de selección de servidor: la medición no es más
  fina que eso.
- **RPO 0 para escrituras confirmadas.** Ninguna de las 26 tareas que
  respondieron 200 se perdió, incluidas las del momento de la elección:
  `w: majority` garantiza que lo confirmado sobrevive a la elección.
- **Sin mayoría no se confirma ninguna escritura (C).** Durante 35,4 s no hubo
  ninguna respuesta 200, `enviar` respondió 503 sin reintento y `/salud` informó
  `caido`.
- **Un 503 no significa "no escrito" (C).**
  - El primer envío sin mayoría se aplicó en el primario aislado antes de que
    este dejara de serlo, y respondió 503 porque no logró la confirmación de la
    mayoría dentro del `wtimeout`.
  - Al volver la mayoría la escritura se conservó: el reintento respondió 409
    porque la tarea ya estaba `anotada`.
  - El cliente no pierde nada confirmado, pero debe tratar el 503 como
    resultado indeterminado.
- **Asignaciones indeterminadas.** Las 5 asignaciones con 503 pueden haber
  quedado `abierta` sin lock en Valkey (el gateway suelta el lock si el registro
  falla). Esas tareas no se pierden: vuelven a ofrecerse cuando vence su plazo
  de 120 s.

**No cubierto aquí:** las fallas de Valkey (reinicio e interrupción de sesión)
son responsabilidad de Christian. Tampoco se midió la disponibilidad de
lectura durante la elección.

### 12.2 Valkey y sesiones

*Responsable: Christian.*

> **PENDIENTE** — reiniciar Valkey e interrumpir una sesión sobre el Compose final, con
restauración a un estado conocido y registro de las acciones de recuperación.

| Caso | Qué se comprueba | Resultado | Evidencia |
|---|---|---|---|
| Reinicio de Valkey | El borrador sobrevive (AOF `everysec`) | PENDIENTE | PENDIENTE |
| Caída abrupta de Valkey | Ventana de pérdida de AOF `everysec` (~1 s) | PENDIENTE | PENDIENTE |
| Sesión interrumpida | Se recupera el borrador; con el lock vencido, la tarea vuelve a la cola | PENDIENTE | PENDIENTE |

---

## 13. Limitaciones

**Registro, gateway y MongoDB**

- Todo corre en una sola máquina: las latencias, el costo de `w:majority` y los
  tiempos de elección no se extrapolan a un clúster distribuido.
- No hay transacción entre MongoDB y Valkey. La consistencia se apoya en el
  filtro por estado de MongoDB, la liberación del lock cuando el registro falla
  y el plazo de 120 s de la asignación.
- Un 503 puede dejar la escritura aplicada (sección 12.1, caso C).
- `/salud` refleja el estado con hasta 5 s de atraso (intervalo del monitor).
  El monitor vive en el gateway: si el gateway cae, nadie publica salud, pero
  en ese caso tampoco hay punto de entrada.
- Registrar un lote inserta las tareas sin transacción multidocumento: una
  caída a mitad puede dejar un lote parcial.
- Mientras un evento está pendiente (Historial caído), Spark no lo ve; el
  reconciliador lo reenvía cada 10 s.
- La latencia de `/tareas/siguiente` con 50 solicitudes simultáneas es de
  ~1 s (p50).
- No se capturaron planes de ejecución (`explain`) de las consultas del
  registro.
- Pendientes: ruta `/analitica/{q1..q5}`,
  integración de Spark y del simulador.

**Sesión e Historial**

- AOF `everysec` puede perder aproximadamente el último segundo ante una caída
  abrupta; un reinicio ordenado no equivale a ese ensayo.
- La publicación Parquet es atómica por archivo, no entre varias particiones
  mensuales.
- El consumo de Streams es *al menos una vez*; Spark debe deduplicar por
  `event_id`.
- El umbral de `XAUTOCLAIM` debe superar el tiempo normal de proceso para no
  reclamar trabajo activo cuando hay varios workers.


**Analítica, datos y carga (Emmanuel)**

- **Dos hilos locales en un contenedor (`local[1]` y `local[2]`):** la comparación entre `spark_workers=1` y `spark_workers=2` se realizó en un único contenedor sobre WSL2 (`local[2]`), sin clúster distribuido real, y el conjunto ampliado `eventos_grande` (~10M filas) no está generado (`no disponible`).
- **Deduplicación global y poda de particiones:** `dropDuplicates(["event_id"])` obliga a un `Sort` y `Exchange` sobre todas las columnas de los 145 archivos antes de agregar, y los filtros sobre `ocurrido_en` no podaron particiones por `mes` (`PartitionFilters: []`), produciendo latencias de ~15,6 a ~53,4 s según consulta, trabajador y calentamiento.
- **Agotamiento de tareas y saturación en P6:** el **Proyecto 084** partió con `420` tareas disponibles (`377` al inicio de la rep. 1 de 10 usuarios) y se agotó a `0` en la repetición 1 de 40 usuarios, provocando `1513` respuestas `504` en `POST /tareas/siguiente` durante ese agotamiento, `204` masivos al quedar `0` tareas disponibles y `504` (~`120 s`) en `GET /analitica/{q}` a partir de la repetición 2 de 10 usuarios.

---

## 14. Evidencia

| Archivo | Prueba | Inicio (UTC) | Comando | Resultado |
|---|---|---|---|---|
| `p2_replica.txt` | Réplica, mayoría e índices | 2026-11-22 05:52:23 | `make replica` | 1 PRIMARY + 2 SECONDARY, `w: majority`, 4 índices |
| `p2_registro_smoke.txt` | Transiciones y atomicidad en el registro | 2026-11-22 05:52:29 | `make smoke-registro` | 30/30 |
| `p1_gateway_smoke.txt` | Rutas básicas del gateway | 2026-11-22 05:52:54 | `make smoke-gateway` | 14/14 |
| `p1_gateway_flujo.txt` | Flujo completo de una tarea | 2026-11-22 06:13:46 | `make smoke-flujo` | 26/26 |
| `p2_concurrencia_gateway.txt` | 50 contra 10 por el gateway | 2026-11-22 06:13:55 | `make p2-gateway` | 9/9 |
| `p2_write_concern.txt` | `w:1` frente a `w:majority` | 2026-11-22 06:52:30 | `make write-concern` | Tabla de la sección 5.3 |
| `p1_salud_aislamiento.txt` | `/salud` por capa (mensajes en Valkey), puertos y aislamiento | 2026-11-22 09:18:43 | `make p1-salud` | 42/42 |
| `p8_mongo_fallas.txt` | Fallas de MongoDB | 2026-11-22 07:48:58 | `make p8-mongo` | 14/14 |
| `smoke_integrante2_20261122T055742Z.txt` | Sesión + Historial en entorno aislado (sin gateway) | 2026-11-22 05:57:42 | `make i2-smoke` | Completado: borrador tras reinicio de Valkey y Parquet publicado |
| `p5_analitica.txt` | Analítica Spark Q1–Q5 (`w=1` y `w=2`, 1.ª vs. 2.ª ejec. y `explain` Q1/Q3) | 2026-11-24 14:46:18 | `make p5` | Tabla de la sección 9 (145 archivos Parquet) |
| `p6_carga.txt` | Carga HTTP contra gateway (10, 40 y 100 usuarios, 3×90 s/nivel, `docker stats` cada 15 s) | Fin: `2026-11-24T16:34:31Z` | `make p6` | Tabla de la sección 11 (Proyecto 084, 420 tareas iniciales) |
| P3, P4, P8 Valkey | — | — | — | PENDIENTE |

Cada archivo incluye al inicio el objetivo, la máquina y la configuración, y al
final la fecha de término.

---

## 15. Repositorio y colaboración

- Repositorio: PENDIENTE (enlace).
- El historial de commits identifica la participación de cada integrante.
- Ramas de trabajo: `Valkey-e-historial-Parquet` (Christian: contratos,
  configuración, locks, borradores, recuperación, ingreso de eventos,
  publicación Parquet, reprocesamiento y pruebas). Resto: PENDIENTE (listar).
- Estructura del repositorio y comandos de ejecución: `proyecto2/README.md`.
- Endpoints de cada servicio: `proyecto2/endpoints/README.md`.

---

## Apéndice A. Formato de una entrada de evidencia

- **Prueba:** grupo y subcaso (p. ej., P3, reinicio de Valkey).
- **Fecha y hora (UTC):**
- **Máquina:** CPU, RAM, sistema y versiones.
- **Objetivo e hipótesis:**
- **Configuración:**
- **Comando exacto:**
- **Muestras o volumen:**
- **Resultado observado:** con latencias p50/p95/p99 y número de muestras
  cuando aplique.
- **RTO / RPO:** si aplica, con los puntos de falla y de recuperación.
- **Interpretación y limitaciones:**
- **Archivo de evidencia:**
