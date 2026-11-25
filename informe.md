# Informe — Plataforma distribuida de anotación de datos

**Proyecto 2 — TI4601 Bases de Datos Avanzadas.** Instituto Tecnológico de
Costa Rica. Profesor: Manuel Zumbado Corrales.

**Equipo:** Christian Navarro Ellerbrock (2023071460), Emmanuel Calvo Mora
(2023213365) y Jimmy Feng Feng (2023060347).

- **Repositorio:** <https://github.com/Chris-0307/proyecto2_BDA>
- **Fecha de entrega:** 24-11-2026
- **Tecnologías:** FastAPI, MongoDB (3 nodos en réplica), Valkey (AOF),
  Parquet y Apache Spark, desplegados con Docker Compose.
- **Entorno de medición:** todas las pruebas se ejecutaron en una misma
  corrida completa desde cero (`make reset`, 2026-11-25 entre 01:12 y
  02:06 UTC), en Linux WSL2 (kernel 6.6.87.1) con 16 CPU lógicos y
  15,5 GiB de RAM disponibles para Docker 28.3.3 (Compose v2.39.2);
  MongoDB 8.0.38 y Spark 3.5.3 (`maquina.txt`). Cada archivo de evidencia
  registra además la máquina en que se ejecutó.
- **Trazabilidad:** toda cifra de este informe proviene de un archivo de
  `proyecto2/evidence/` (sección 14).

---

## 1. Arquitectura y límites de responsabilidad

El único punto de entrada es el gateway (puerto 8000). Registro, Sesión
e Historial son internos; MongoDB y Valkey no se exponen al host. La
analítica con Spark lee el datalake en modo de solo lectura.

```mermaid
flowchart TB
    sim["Simulador HTTP"]
    gw["Gateway :8000<br/>validación, orquestación,<br/>X-Request-ID, /salud por capa"]
    reg["Registro :8003"]
    ses["Sesión :8001"]
    his["Historial :8002"]
    ana["Analítica Spark :8004"]
    worker["historial-worker"]
    mongo[("MongoDB x3<br/>rs-plataforma")]
    valkey[("Valkey base 0<br/>locks, sesiones, borradores, Streams")]
    salud[("Valkey base 2<br/>salud:capa:*, salud:eventos")]
    lake[("Datalake Parquet<br/>eventos/mes=AAAA-MM")]

    sim -->|"HTTP, único puerto publicado"| gw
    gw --> reg --> mongo
    gw --> ses --> valkey
    gw --> his -->|"Streams"| valkey
    gw -->|"/analitica/{q}"| ana
    gw -->|"monitor de salud"| salud
    worker -->|"XREADGROUP / XACK"| valkey
    worker --> lake
    ana -->|"solo lectura"| lake
```

*Arquitectura del sistema, con la ruta `/analitica` del gateway hacia Spark.*

| Componente | Responsabilidad | Tecnología | Responsable |
|---|---|---|---|
| Gateway | Único punto de entrada; valida, orquesta el flujo de una tarea, publica eventos y reporta salud por capa | FastAPI + httpx | Jimmy |
| Registro | Estado vigente de proyectos, lotes y tareas; transiciones atómicas; exportación | FastAPI + MongoDB (3 nodos) | Jimmy |
| Sesión | Locks de 120 s, sesiones, latidos y borradores | FastAPI + Valkey | Christian |
| Historial | Recepción idempotente de eventos y escritura de Parquet | FastAPI + Valkey Streams + Parquet | Christian |
| Analítica | Consultas Q1–Q5 sobre Parquet | Spark | Emmanuel |
| Simulador | Anotadores, revisores y consumidores vía HTTP | Cliente HTTP | Emmanuel |

**Reglas de aislamiento** (`p1_salud_aislamiento.txt`, 54/54):

- Solo el gateway publica puerto al host: en `docker compose ps` únicamente
  `gateway` tiene puerto publicado (8000). Desde el host, 8001, 8002, 8003,
  8004, 27017 y 6379 rechazan la conexión.

- El gateway no tiene `pymongo` (ni en el código, ni en la imagen, ni en las
  variables de entorno): no accede a MongoDB. Usa Valkey solo para los
  mensajes de salud, en su propia base (2) y con llaves `salud:*`; no lee ni
  escribe la base 0 de Sesión e Historial.

- El registro no tiene `redis`: no consulta Valkey.

- Sesión e Historial no tienen `pymongo`: no acceden a MongoDB, y por lo tanto
  Sesión no puede decidir el estado definitivo de una tarea.

- Analítica no tiene `pymongo` ni `redis` y el datalake está montado en solo
  lectura: lee únicamente el lago Parquet.

- El simulador no importa clientes de bases (`pymongo`, `redis`, `pyspark`) ni
  menciona hosts o puertos internos: solo se comunica con el gateway
  (`localhost:8000`).

- **Limitación:** todos los servicios comparten la red de Compose; el
  aislamiento se garantiza por dependencias y configuración, no por
  segmentación de red.

---

## 2. Justificación de motores

- **MongoDB para el registro.** Una tarea es un documento autocontenido
  (referencia al archivo, asignación, anotación, revisión y cola de
  eventos), y la anotación cambia de forma según la modalidad (span,
  bbox, polígono, cuboide 3D). Toda transición modifica un solo
  documento, así que un `findOneAndUpdate` con filtro por estado
  garantiza la asignación exclusiva sin transacciones multidocumento. El
  conjunto de réplicas de tres nodos tolera la caída de uno, y
  `w: majority` impide que una elección revierta una escritura
  confirmada (secciones 5.3 y 12).

- **Valkey para sesiones.** Estado efímero con TTL (`SET NX PX`),
  operaciones atómicas con Lua y persistencia AOF para que el borrador
  sobreviva a un reinicio. No es la fuente de verdad: el estado
  definitivo vive en MongoDB.

- **Parquet + Spark para el historial.** Los eventos son inmutables y se
  consultan por agregación (OLAP), lo que favorece un formato columnar
  particionado por mes en lugar de consultar el registro operacional.

- **Spark para la analítica.** El servicio `analitica` lee solo los
  `*.parquet` publicados en `datalake/eventos/mes=AAAA-MM/` (montaje
  `ro`), sin consultar MongoDB ni Historial. Como el consumo es *al
  menos una vez*, ejecuta `dropDuplicates(["event_id"])` antes de
  Q1–Q5. Levantar la JVM y planificar la lectura de todos los meses
  publicados tiene un costo inicial alto (19,9 s en la primera Q1, sección 9);
  compensa con volúmenes históricos grandes (de cientos de miles a
  millones de eventos), pero no es adecuado para consultas puntuales de
  baja latencia.

---

## 3. API REST y contratos

La documentación completa de rutas, cuerpos y códigos está en
`endpoints/README.md`. Decisiones del gateway:

- **Reintentos seguros.** Solo los `GET` se reintentan una vez ante
  error de conexión o timeout. `POST` y `PUT` nunca se reintentan:
  repetir podría duplicar un proyecto, un lote o una transición, y el
  enunciado pide 503 sin reintento.

- **Errores.** Un servicio interno que no responde se traduce a 503; los
  códigos 403, 404, 409 y 422 de los servicios internos se propagan tal
  cual.

- **Identificadores.** `X-Request-ID` se propaga a los servicios
  internos y se devuelve al cliente.

- **Salud por capa con mensajes en Valkey.** Un monitor del gateway
  consulta cada 5 s el `/salud` de Registro, Sesión, Historial y Analítica y
  publica el estado en `salud:capa:{capa}` (expira a los 15 s, de modo que un
  monitor detenido no deja estado viejo) y en el stream `salud:eventos` (cada
  cambio, como `capa:antes->ahora`). `GET /salud` lee esos mensajes
  (`fuente: valkey`); si Valkey no responde, consulta cada capa directamente
  (`fuente: directo`). El gateway siempre responde. El gateway publica por las
  capas porque el reporte de salud es su responsabilidad y el registro no
  puede escribir en Valkey; usa una base propia (2) para no mezclar datos con
  Sesión e Historial. Estado global: `caido` (503) si el registro no está
  disponible; `degradado` (200) si alguna otra capa falla o MongoDB perdió un
  nodo; `operativo` (200) si todas responden. *Costo:* el estado puede tener
  hasta 5 s de atraso.

- **Ruta de analítica.** `GET /analitica/{q1..q5}?conjunto=principal|grande`
  reenvía la consulta a `analitica:8004/q/{q}`. Una consulta de Spark tarda
  de 3 a 20 s sin carga (sección 9) y bastante más si hay otras en cola,
  así que el gateway espera hasta 120 s y **no reintenta**: repetirla duplicaría el costo. Una consulta desconocida
  responde 404, un `conjunto` inválido 422 y Analítica sin respuesta 503. En
  P6, el simulador registró como `timeout` (código 504 en su propio registro)
  las solicitudes que no recibieron respuesta dentro de su timeout de cliente
  de 120 s; no es una respuesta del gateway ni de Analítica.

Resultados de salud (`p1_salud_aislamiento.txt`, 54/54):

| Servicio detenido | `/salud` | Reflejado en | Además |
|---|---|---|---|
| Sesión | 200 `degradado`, solo `sesion` caída | 3,0 s | Cambio en `salud:eventos`; `exportar` responde; guardar borrador da 503 |
| Historial | 200 `degradado`, solo `historial` caída | 5,0 s | Cambio en `salud:eventos`; `exportar` responde |
| Analítica | 200 `degradado`, solo `analitica` caída | 6,0 s | Cambio en `salud:eventos`; `exportar` responde |
| Registro | 503 `caido` | 5,0 s | Cambio en `salud:eventos`; `exportar` da 503 |
| Valkey | 200 `degradado`, `fuente: directo`; Sesión e Historial caídos | 16,4 s | Al volver Valkey: `fuente: valkey` y `operativo` |

En todos los casos `/salud` vuelve a `operativo` al arrancar el servicio.
Los tiempos de 3–6 s dependen de en qué momento del intervalo del monitor
(5 s) se detuvo el servicio; el caso de Valkey tardó 16,4 s y no se midió el
desglose de ese tiempo.

**`/salud` de Analítica tras P6.** En el primer intento de P1, ejecutado
justo después de P6, `/salud` informó `analitica: caido` sin que el
servicio estuviera detenido: Spark seguía procesando la cola de consultas
`/analitica` que dejó P6, y el `/salud` de Analítica (una función
síncrona) esperaba un hilo libre detrás de ellas. Se cambió a `async` para
que responda en el event loop sin esperar a las consultas; tras
reconstruir el servicio, P1 dio 54/54.

**Resultados funcionales:** rutas básicas 14/14 (`p1_gateway_smoke.txt`)
y flujo completo 26/26 (`p1_gateway_flujo.txt`), que incluye que los
cinco tipos de evento pasan la validación de Historial y que ningún
evento queda pendiente.

**Contrato de eventos.** El registro arma cada evento dentro de la
transición y el gateway lo publica sin modificarlo; Historial lo valida
con un esquema estricto (`extra=forbid`). Para la analítica: Q3 lee
`duracion_ms` de `tarea_anotada` (toma->envío); Q5 calcula
toma->aprobación con `tomada_en` y `aprobada_en` de
`revision_aprobada`; Q4 cuenta las filas `rechazada`.

---

## 4. Recorrido completo de una tarea (ejemplo LiDAR)

Ejemplo **ilustrativo**: los cuerpos tienen la forma real de la API,
pero los valores y los ids (abreviados) son de ejemplo. El flujo
completo está verificado en `p1_gateway_flujo.txt` (26/26) con tareas de
imagen; la forma es la misma para LiDAR.

| N.º | Llamada | Qué ocurre |
|---|---|---|
| 1 | `POST /proyectos` | El registro valida que el cliente exista (404 si no) y responde 201. La taxonomía lleva etiqueta e instrucción (p. ej. `lidar#ciclista`). |
| 2 | `POST /proyectos/{id}/lotes` | Se valida que la geometría corresponda a la modalidad (LiDAR solo admite `cuboide_3d`; otra combinación da 422). Las tareas se insertan en `disponible`, versión 1. Solo se guarda la referencia al archivo. |
| 3 | `POST /tareas/siguiente` | Gateway->Registro: candidatas por prioridad. Gateway->Sesión: `SET NX PX 120000` sobre `lock:tarea:{id}` (si está ocupado, prueba la siguiente). Gateway->Registro: `asignar` (`findOneAndUpdate` que exige `disponible` o `abierta` vencida; con 409 suelta el lock y prueba otra). Publica `tarea_abierta`. Responde 200 con `tarea`, `token`, `ttl_ms` y el borrador si existía; 204 si no hay ninguna libre. |
| 4 | `PUT /tareas/{id}/borrador` | Cada 2 s. Sesión guarda el último borrador y renueva el lock; no toca MongoDB ni genera fila en Parquet. Tras una desconexión, `POST /sesiones/recuperar` devuelve el borrador (`activa` si el lock sigue vigente, `requiere_reasignacion` si venció). |
| 5 | `POST /tareas/{id}/enviar` | Latido (el token debe ser el dueño del lock; 409 si no). Registro filtra por estado `abierta` y token: un segundo envío da 409. Si MongoDB no responde, 503 sin reintento y el borrador y el lock siguen en Valkey. Publica `tarea_anotada` (con `duracion_ms`) y suelta el lock. |
| 6 | `POST /tareas/{id}/revision` | `iniciar_revision` (`anotada`->`en_revision`) y `rechazar`. Sin `motivo`: 422; actor que no es revisor: 403. |
| 7 | `siguiente` + `revision` | La tarea rechazada se vuelve a ofrecer; tras el nuevo envío, `aprobar` la deja en `aprobada` y publica `revision_aprobada` (base de Q5). Aprobar otra vez da 409. |
| 8 | `GET /proyectos/{id}/exportar` | Tareas `aprobada` paginadas por `_id`, con cursor `siguiente`. |

Ejemplo del cuerpo del borrador (paso 4) para una tarea LiDAR:

```json
{"anotador_id": "a7...", "proyecto_id": "p3...", "token": "t9...",
 "contenido": {"cuboides": [{"etiqueta": "lidar#ciclista",
   "centro": [12.4, -3.1, 0.9], "dimensiones": [1.8, 0.6, 1.7], "yaw": 0.12}]}}
```

| Paso | Estado en MongoDB | Fila en Parquet |
|---|---|---|
| siguiente | `abierta` (versión 2, `asignacion.token`) | `abierta` |
| enviar | `anotada` | `anotada` |
| inicio de revisión | `en_revision` | `en_revision` |
| rechazo | `disponible` | `rechazada` (con `motivo_rechazo`) |
| aprobación | `aprobada` | `aprobada` |

Liberar una tarea y guardar borradores no generan filas.

---

## 5. Registro operacional: modelo, índices y concurrencia

### 5.1 Modelo e índices

Base `plataforma`, colecciones `clientes`, `proyectos`, `batches`,
`tareas` y `anotadores`, con identificadores UUID. Documento de una
tarea:

```json
{"_id": "uuid", "proyecto_id": "uuid", "batch_id": "uuid", "cliente_id": "uuid",
 "modalidad": "lidar", "tipo_tarea": "cuboide_3d", "prioridad": 5,
 "archivo": {"uri": "...", "sha256": "...", "bytes": 0, "mime": "...", "meta": {}},
 "estado": "disponible", "version": 1, "sla_horas": 48,
 "asignacion": {"anotador_id": "...", "equipo_id": "...", "token": "...",
                "tomada_en": "...", "vence_en": "..."},
 "anotacion": {"contenido": {}, "enviada_en": "..."},
 "revision": {"revisor_id": "...", "iniciada_en": "...", "resultado": "...",
              "motivo": "...", "revisada_en": "..."},
 "ultimo_evento_id": "uuid", "eventos_pendientes": [], "creado_en": "..."}
```

**Estados y transiciones.** `disponible`, `abierta`, `anotada`,
`en_revision`, `aprobada`. Cada transición es **un único**
`findOneAndUpdate` cuyo filtro exige el estado de origen (y el token o
el revisor cuando corresponde); si no coincide, no se modifica nada y se
responde 409. No hay lectura previa seguida de escritura, y la versión
sube en cada transición válida.

**Evento en la misma operación (*outbox*).** La actualización es un
pipeline de agregación: en la misma escritura atómica cambia el estado,
sube la versión y construye el evento, que queda en
`eventos_pendientes`. Así no se pierde el evento si el proceso cae entre
la escritura y la publicación. El gateway lo publica y lo confirma; si
Historial falla, un reconciliador del gateway reenvía los pendientes
cada 10 s, y Historial deduplica por `event_id`.

**Índices** (`p2_replica.txt` lista los cuatro creados; son los mínimos
del enunciado):

| Índice | Campos | Consulta que lo usa |
|---|---|---|
| `tareas_proyecto_estado_prioridad` | `proyecto_id, estado, prioridad:-1` | Candidatas de `/tareas/siguiente`. Regla ESR: igualdad antes que orden, así el orden sale del índice sin ordenar en memoria |
| `tareas_anotador_estado` | `asignacion.anotador_id, estado` | Tareas de un anotador |
| `tareas_batch` | `batch_id` | Tareas de un lote |
| `proyectos_cliente_estado` | `cliente_id, estado` | Proyectos de un cliente por estado |

Las transiciones buscan por `_id`, que ya tiene índice. La exportación
filtra por `proyecto_id` y `estado = aprobada` y ordena por `_id`:
aprovecha el prefijo del primer índice, aunque el orden por `_id` no
sale de él.

**Planes de ejecución (`explain`).** Planes con `executionStats` sobre el
dataset recién cargado (`p2_explain_registro.txt`, 10/10; MongoDB 8.0.38,
60.000 tareas, 120 proyectos y 300 anotadores, sin datos de otras
pruebas):

| Consulta | Índice usado | Etapas | Ordenamiento en memoria | Devueltos | Claves examinadas | Docs examinados |
|---|---|---|---|---|---|---|
| Candidatas disponibles (`/tareas/siguiente`) | `tareas_proyecto_estado_prioridad` | LIMIT -> FETCH -> IXSCAN | No | 20 | 20 | 20 |
| Candidatas abiertas vencidas | `tareas_proyecto_estado_prioridad` | LIMIT -> FETCH -> IXSCAN | No | 0 | 0 | 0 |
| Tareas de un anotador | `tareas_anotador_estado` | FETCH -> IXSCAN | No | 0 | 0 | 0 |
| Tareas de un lote | `tareas_batch` | FETCH -> IXSCAN | No | 100 | 100 | 100 |
| Exportar aprobadas (orden por `_id`) | `tareas_proyecto_estado_prioridad` | SORT -> FETCH -> IXSCAN | Sí | 0 | 0 | 0 |
| Proyectos de un cliente | `proyectos_cliente_estado` | FETCH -> IXSCAN | No | 2 | 2 | 2 |
| Transición por `_id` + estado | `_id_` | FETCH -> IXSCAN | No | 1 | 1 | 1 |

Todas las consultas reportaron 0 ms de ejecución en el servidor.

- Todas las consultas usan un índice (`IXSCAN`); ninguna recorre la colección
  completa.
- En las consultas que devolvieron documentos, las claves y los documentos
  examinados coinciden con los devueltos: el índice filtra exactamente lo
  necesario. Las candidatas de `/tareas/siguiente` examinaron 20 documentos
  para devolver 20, y el orden por prioridad sale del índice (regla ESR), sin
  ordenamiento en memoria.
- Las consultas de abiertas vencidas, de tareas de un anotador y de
  exportación devolvieron 0 documentos (recién cargado no hay tareas
  abiertas ni aprobadas en el registro); aun así el plan elegido usa el
  índice previsto.
- La exportación usa el prefijo `proyecto_id, estado` de
  `tareas_proyecto_estado_prioridad`, pero ordena por `_id` en memoria.
  Recién cargado el costo es nulo; con muchas aprobadas por proyecto, ese
  ordenamiento crece con el número de aprobadas. En P6 la latencia de
  `exportar` subió de ~9 ms a 400–640 ms (p50) a medida que el proyecto
  acumuló aprobadas y aumentó la concurrencia (sección 11), lo que es
  consistente con este costo, aunque no se aisló. Un índice
  `(proyecto_id, estado, _id)` lo evitaría, pero no se creó porque el
  enunciado exige justificar cada índice adicional con una medición, y no hay
  una medición con volumen de aprobadas que lo respalde.

### 5.2 Prueba de que no hay doble adquisición

**Mecanismo en dos capas.** El lock de Valkey (`SET NX PX`) evita que
dos peticiones trabajen la misma tarea a la vez. La garantía final es el
`findOneAndUpdate` filtrado por estado en MongoDB: aunque dos peticiones
llegaran juntas al registro, solo una encuentra la tarea en
`disponible`.

**A nivel de registro** (`p2_registro_smoke.txt`, 30/30): 50 intentos
simultáneos de asignar **la misma tarea** dieron exactamente 1 respuesta
200 y 49 respuestas 409. La tarea quedó `abierta`, versión 2, con el
token del ganador; un segundo envío responde 409.

**A nivel de gateway, 50 contra 10** (`p2_concurrencia_gateway.txt`,
9/9): 50 anotadores piden `/tareas/siguiente` a la vez sobre un proyecto
con 10 tareas.

| Resultado | Valor |
|---|---|
| Respuestas 200 / 204 / otros | 10 (todas con tareas distintas) / 40 / 0 |
| En MongoDB | 10 tareas `abierta`, cada una con un dueño distinto, todas en versión 2 (una sola asignación) y con el mismo dueño que recibió el 200 |
| Latencia de `/tareas/siguiente` (n = 50) | p50 1.262,8 ms; p95 1.294,5 ms; p99 1.299,5 ms; máx. 1.299,5 ms |
| Duración de la ráfaga | 1.308,7 ms |

**Cero dobles asignaciones** en ambas pruebas. La latencia es alta bajo
contención; no se midió el desglose, y la hipótesis es que cada petición
sin tarea recorre hasta 10 candidatas con una llamada HTTP a Sesión por
cada una. Es un candidato a optimizar (ver
sección 11).

### 5.3 Diferencia observada entre `w:1` y `w:majority`

Medición con el driver, sin gateway, de las dos escrituras del registro
(`p2_write_concern.txt`): 500 operaciones por hilo y repetición, 3
repeticiones en orden alternado, con 1 y 8 hilos.

| Hilos | Operación | p50 `w:1` | p50 `majority` | Relación | ops/s `w:1` | ops/s `majority` |
|---|---|---|---|---|---|---|
| 1 | `insert_one` | 0,49 ms | 2,78 ms | ×5,66 | 1.605 | 331 |
| 1 | `find_one_and_update` | 0,68 ms | 3,00 ms | ×4,42 | 1.605 | 331 |
| 8 | `insert_one` | 3,55 ms | 6,45 ms | ×1,82 | 2.029 | 918 |
| 8 | `find_one_and_update` | 3,71 ms | 6,62 ms | ×1,79 | 2.029 | 918 |

- Con un hilo, `w:majority` espera a que un secundario aplique la
  escritura: la latencia se multiplica entre 4,4 y 5,7 veces y el
  rendimiento cae de 1.605 a 331 ops/s (−79 %). Con 8 hilos las esperas
  se solapan y la diferencia de la mediana baja a ×1,8 (2.029 frente a
  918 ops/s, −55 %).

- Con 8 hilos y `w:majority` el p99 sube a 96–101 ms (frente a 9 ms con
  `w:1`) y el rendimiento varió entre repeticiones (630 a 1.206 ops/s):
  hay pausas ocasionales esperando la confirmación de la mayoría, que no
  se aislaron.

- **Limitación:** los tres nodos corren en la misma máquina, así que la
  replicación no paga latencia de red real; estas cifras no permiten
  inferir el comportamiento de un clúster en varias máquinas.

- **Garantía:** `w:1` confirma una escritura que aún no está en la
  mayoría y que puede perderse en un rollback tras una elección. En el
  caso C de la sección 12.1 una escritura quedó aplicada en un primario
  aislado sin llegar a la mayoría: con `w:majority` el cliente recibió
  503; con `w:1` habría recibido 200 por una escritura no replicada
  (esto último no se midió con `w:1`).

Se mantiene `w:majority`: el costo medido es de milisegundos y la
garantía es que ninguna escritura confirmada se pierde (RPO 0).

---

## 6. Sesión, recuperación del borrador y reinicio de Valkey

**Diseño.**

- La fuente de verdad de la tarea es MongoDB. Valkey solo conserva
  sesiones temporales y borradores, sin modificar el estado definitivo.

- Scripts Lua adquieren, renuevan, liberan y validan los locks sin
  intercalar operaciones concurrentes; el token impide liberar un lock
  ajeno.

- Llaves: `lock:tarea:{id}` (`SET NX PX`, 120 s, se renueva con el
  latido y con cada borrador), `sesion:{anotador}` (tarea, proyecto,
  etapa y token), `borrador:{tarea}` (último borrador, sin TTL para
  poder recuperarlo) y `editando:{proyecto}` (sesiones activas). Los
  borradores no generan eventos.

- Si el lock venció, la recuperación devuelve el borrador en solo
  lectura y `requiere_reasignacion`; la decisión de reasignar la toma el
  registro a través del gateway (`/tareas/siguiente` vuelve a ofrecer la
  tarea y devuelve el borrador al mismo anotador).

- Persistencia: AOF con `appendonly yes` y `appendfsync everysec`.

**Qué pasa al reiniciar Valkey.** La prueba P3 se ejecutó sobre el
Compose final, utilizando el gateway como punto de entrada. Se verificó
la configuración de AOF con `appendonly yes` y `appendfsync everysec`.

Después de guardar tres borradores con intervalos de dos segundos, se
reinició el contenedor de Valkey conservando su volumen persistente. La
recuperación mediante el gateway respondió HTTP 200 en 0,537 s desde el
inicio del reinicio, conservando el contenido y la versión del último
borrador, así como el lock activo.

La prueba obtuvo 29/29 comprobaciones correctas. La evidencia se
encuentra en `p3_gateway_sesion.txt`.

**Resultados de P3** (criterios del enunciado):

| Criterio | Resultado | Evidencia |
|---|---|---|
| El borrador recuperado tiene como máximo 5 s de antigüedad | 2,013 s | `p3_gateway_sesion.txt` |
| Tras reiniciar Valkey, el borrador sigue | Contenido y versión conservados. Recuperación en 0,537 s | `p3_gateway_sesion.txt` |
| Un lock vencido devuelve la tarea a la cola | Reasignación exitosa tras esperar 122 s sin actividad | `p3_gateway_sesion.txt` |
| Ninguna tarea queda con dos dueños | Un único propietario vigente en MongoDB. El token anterior responde HTTP 409 | `p3_gateway_sesion.txt` |

Una vez vencido el lock, el anotador original pudo recuperar su borrador
en modo de solo lectura, pero no modificarlo. Un segundo anotador
adquirió la misma tarea con un token diferente, sin recibir el borrador
del propietario anterior.

MongoDB confirmó un único propietario vigente. Los intentos de guardar,
enviar y recuperar la sesión utilizando el token anterior respondieron
HTTP 409 después de la reasignación.

Finalmente, se liberó la tarea de prueba mediante el gateway, obteniendo
HTTP 200.

---

## 7. Datalake Parquet

**Diseño.**

- El gateway publica cada evento en `POST /eventos` solo después de que
  MongoDB confirmó la transición. Historial lo encola en Valkey Streams
  de forma idempotente por `event_id` (202 nuevo, 200 repetido, 409 si
  el mismo `event_id` llega con otro contenido).

- Un worker independiente lee el grupo `historial-parquet` y escribe
  cada micro-lote primero en `.staging`; luego lo sincroniza y lo
  publica con `os.replace` dentro del mismo volumen. Solo después
  confirma con `XACK`.

- Partición mensual por `ocurrido_en` en UTC: `eventos/mes=AAAA-MM/`.

- Si un worker muere antes del `XACK`, el mensaje queda pendiente y otro
  lo reclama con `XAUTOCLAIM`; repetir el mismo micro-lote produce el
  mismo nombre de archivo.

- **Archivos a medias:** Spark solo lee `*.parquet` publicados (los
  temporales viven en `.staging/`); la publicación es atómica por
  archivo.

- **Duplicados:** el consumo es *al menos una vez*, así que dos
  micro-lotes solapados pueden contener el mismo `event_id`; Spark
  aplica `dropDuplicates(["event_id"])` antes de Q1–Q5.

- **Fila rechazada:** un rechazo deja la tarea en `disponible` en
  MongoDB y una fila `rechazada` en Parquet; Q4 cuenta esas filas.

**Compactación.**

La escritura continua de micro-lotes puede generar una cantidad elevada
de archivos Parquet pequeños, aumentando el costo de apertura, lectura y
planificación de las consultas en Spark.

Se propone una estrategia de compactación manual por partición mensual,
preferiblemente sobre meses cerrados. Como política inicial, se plantea
evaluar la compactación cuando una partición supere los 100 archivos,
generando archivos de aproximadamente 128 MiB cuando el volumen de datos
lo permita. Estos parámetros son propuestas de diseño y no resultados
experimentales.

1.  Suspender las operaciones del gateway, procesar los eventos
    pendientes y detener el worker y los lectores de Spark.

2.  Leer los archivos publicados de la partición mensual seleccionada.

3.  Verificar la consistencia del contenido de los eventos repetidos y
    deduplicar por `event_id`.

4.  Escribir los archivos compactados en una ubicación temporal fuera
    del patrón de lectura de Spark.

5.  Validar el esquema, el contenido y los identificadores únicos antes
    y después de la compactación.

6.  Respaldar los archivos originales y publicar los archivos
    compactados dentro de la partición correspondiente.

7.  Restaurar los servicios y permitir nuevamente la lectura y escritura
    del datalake.

La compactación conserva las diferentes transiciones de cada tarea y los
estados históricos, incluyendo los eventos de rechazo. Spark mantiene la
deduplicación por `event_id` para manejar posibles reprocesamientos
posteriores.

La compactación automática no se encuentra implementada en la versión
actual. Se documenta como una estrategia de mantenimiento y mejora
futura.

**Resultados de P4** (criterios del enunciado):

| Criterio | Resultado | Evidencia |
|---|---|---|
| Archivos Parquet válidos, sin lectura a medias | Spark leyó correctamente mientras existía un archivo temporal incompleto. | `p4_gateway_parquet.txt` |
| Reproceso de un micro-lote y deduplicación por `event_id` | Un mensaje recuperado, cero pendientes y ningún archivo adicional. Tres filas físicas se redujeron a dos eventos únicos. | `p4_gateway_parquet.txt` |
| El historial se materializa en el datalake | Ocho eventos de negocio correspondientes al ciclo completo de una tarea con retrabajo. | `p4_gateway_parquet.txt` |
| Frescura: segundos entre un HTTP 200 y el evento visible en Spark | Envíos: p50 9,134 s; p95 10,080 s; p99 10,080 s (n=2). Los cuatro eventos: 9,1–10,1 s. | `p4_gateway_parquet.txt` |

La prueba P4 obtuvo 27/27 comprobaciones correctas sobre el Compose
final. Se simuló una falla después de publicar un micro-lote y antes de
ejecutar `XACK`. El mensaje permaneció pendiente hasta que fue
recuperado mediante `XAUTOCLAIM`, sin generar un archivo adicional.

También se comprobó que Spark no intenta leer archivos temporales
durante la escritura. Al publicar dos micro-lotes solapados, las tres
filas físicas se redujeron a dos eventos únicos mediante la
deduplicación por `event_id`.

En el flujo completo de una tarea, MongoDB registró el rechazo como
`disponible`, mientras que Parquet conservó el estado `rechazada`. Los
borradores y los envíos duplicados que respondieron HTTP 409 no
generaron eventos adicionales.

**Mediciones de frescura.**

| Evento | Tiempo (s) | Intentos |
|---|---|---|
| Primer envío | 10,080 | 1 |
| Rechazo | 9,362 | 1 |
| Segundo envío | 9,134 | 1 |
| Aprobación | 9,659 | 1 |

Las mediciones se realizaron con Spark 3.5.3, `local[1]` y la JVM
precalentada, usando el mismo lector que Analítica
(`df_eventos_limpios`) sobre `/datalake/eventos`. El tiempo corresponde
al intervalo entre la respuesta HTTP 200 y la primera observación del
evento en Spark, incluyendo el costo de lectura, sondeo y transporte.

Los cuatro eventos se vieron en el **primer** sondeo, así que el evento
ya estaba publicado cuando Spark terminó de leer: la cifra mide sobre
todo el costo de una lectura completa. Cada sondeo leyó 81 a 86 archivos
(los 60 del dataset y los de las pruebas) y deduplicó ~830.000 eventos,
del mismo orden que una consulta de P5 con `w=1` (6–12 s). Una corrida
anterior, con un datalake mucho menor, observó 1,9–2,6 s, lo que es
consistente con esta explicación, aunque no se aisló la demora propia
del worker.

Debido al reducido número de muestras, los resultados son funcionales y
no representan una prueba de rendimiento bajo carga.

---

## 8. Datos de prueba

*Responsable: Emmanuel.*

Generador reproducible en `proyecto2/dataset/generar_dataset.py`, con
semilla fija (`20261122`, `random.Random`). Todos los identificadores se
derivan de la semilla con `uuid5`, así que el mismo cliente, proyecto,
equipo y tarea tiene el mismo id en MongoDB y en los eventos Parquet.
Carga MongoDB (base `plataforma`) con `pymongo`, vaciando antes las
colecciones del registro para que la carga sea repetible, y materializa
Parquet mensual en `datalake/eventos/mes=AAAA-MM/` con el esquema de
`servicios/historial/app/parquet_writer.py` (publicación vía `.staging`
y `os.replace`, archivos `gen-20261122-AAAA-MM-pNNN.parquet`). Respeta
el documento de tarea de la sección 5.1 (incluidas las geometrías
válidas por modalidad) y el esquema estricto de eventos.

**Realismo del historial.** Cada tarea histórica sigue el flujo real:
`tarea_abierta` -> `tarea_anotada` -> `revision_iniciada` -> (`revision_rechazada`
-> retrabajo)\* -> `revision_aprobada`, con hasta 4 rechazos, versión
creciente y `tomada_en` igual a la toma de la ronda vigente. Cada
proyecto tiene su propia probabilidad de rechazo (0,35–0,70), su cola de
revisión (2–60 h) y un SLA de 24, 48 o 72 h, por lo que Q4 y Q5
muestran diferencias reales entre clientes y proyectos. La carga
publicó 830.256 eventos de 100.000 tareas y 180 proyectos en 60 archivos
Parquet (`mes=2025-11` a `mes=2026-10`); en una simulación local con la
misma semilla, ~8,6 % de las tareas quedó fuera de SLA y el rechazo por
proyecto quedó entre 0,33 y 0,65.

| Entidad | Volumen del enunciado | Generado |
|---|---|---|
| Clientes | 60 | 60 |
| Proyectos | 180: 120 en el registro y 60 cerrados solo en el historial | 180 (120 activos en MongoDB y 60 cerrados solo en Parquet) |
| Batches | 600 | 600 |
| Tareas del registro | 60.000: 30 % texto, 30 % imagen, 20 % video, 20 % LiDAR | 60.000: 18.065 texto, 18.038 imagen, 11.932 video, 11.965 LiDAR |
| Anotadores | 300 en 30 equipos; 10 % revisores | 300 en 30 equipos, 30 revisores (10 %) |
| Eventos históricos | ~800.000 en doce meses y 100.000 tareas | 830.256 eventos de 100.000 tareas en 12 meses (`mes=2025-11` a `mes=2026-10`), 60 archivos Parquet |
| `eventos_grande` (opcional) | Hasta ~10 millones de filas | No generado (medición no disponible) |

Los conteos de la tabla son los que el generador reportó al insertar en
MongoDB y al publicar Parquet (`datos_generacion.txt`, 2026-11-25
01:34:15 UTC; la generación es determinista, así que las cargas
anteriores dieron los mismos conteos).

**Comandos de carga:**

```bash
make datos
# Equivale a:
# MongoDB
docker compose -f proyecto2/docker-compose.yml run --rm registro python /dataset/generar_dataset.py --solo mongo
# Datalake Parquet (en historial-worker: tiene el esquema del worker y el datalake con escritura)
docker compose -f proyecto2/docker-compose.yml run --rm -v "$(pwd)/proyecto2/dataset:/dataset:ro" historial-worker \
  python /dataset/generar_dataset.py --solo parquet --dir-eventos /datalake/eventos
```

**Regeneraciones.** Con la semilla fija cada carga produce exactamente
los mismos datos. En la corrida final el dataset se cargó dos veces:

| Fecha (UTC) | Motivo | Efecto sobre la comparabilidad |
|---|---|---|
| 2026-11-25 ~01:14 | Carga inicial tras `make reset` (volúmenes vacíos), antes de P5 y de los planes `explain` del registro | P5 y `p2_explain_registro.txt` se midieron sobre el dataset puro |
| 2026-11-25 01:34 | P1–P4 consumen tareas y escriben eventos; se recarga para que P6 parta con el inventario completo | P6 parte del mismo estado que la carga inicial; los eventos que escribieron P3, P4 y P6 en `mes=2026-11` se conservan |

P6 usa un solo proyecto (el primero del registro, *Proyecto 001*), que
tiene 500 tareas: el inventario se agota en el nivel de 40 usuarios
(sección 11).


---

## 9. Analítica con Spark (Q1–Q5)

*Responsable: Emmanuel.*

Servicio interno `analitica` (`servicios/analitica/`, puerto interno
8004, sin exposición al host) implementado con FastAPI + PySpark
(`local[N]`), montando `./datalake` en solo lectura (`ro`) y filtrando
exclusivamente los `*.parquet` publicados (`mes=*/*.parquet`),
ignorando `.staging/`. Todas las consultas parten de
`dropDuplicates(["event_id"])` sobre todo el conjunto, sin filtro fijo
de año, y la sesión Spark usa zona horaria UTC. `conjunto=grande` lee
`datalake/eventos_grande`; si no existe responde 404. El gateway
expone la ruta `GET /analitica/{q}` (timeout de 120 s y sin reintento)
que reenvía a este servicio.

| Consulta | Pregunta de negocio (enunciado) | Definición / implementación | Filas (`p5_analitica.txt`) |
|---|---|---|---|
| Q1 | Tareas aprobadas por día y modalidad, últimos 90 días | `revision_aprobada` en los 90 días anteriores al último `ocurrido_en` del conjunto; `countDistinct(tarea_id)` por día (`to_date(ocurrido_en)`) y `modalidad` | 359 |
| Q2 | Aprobadas / revisadas, por equipo y mes | `revision_aprobada` + `revision_rechazada` por `equipo_id` y mes (`yyyy-MM`); `aprobadas / revisadas` | 360 |
| Q3 | Duración media y p95, por modalidad y tipo de tarea | `tarea_anotada` con `duracion_ms`; `avg` y `percentile_approx(0.95)` por `modalidad` y `tipo_tarea` | 5 |
| Q4 | Rechazadas / revisadas, por cliente y proyecto | `revision_aprobada` + `revision_rechazada` por `cliente_id` y `proyecto_id`; `rechazadas / revisadas` | 180 |
| Q5 | Toma -> aprobación: p50 y p95 por cliente y % fuera de SLA | `revision_aprobada`; horas = `aprobada_en` − `tomada_en`; p50, p95 y % con horas > `sla_horas`, por `cliente_id` | 60 |

**Validación.** `pruebas/p5_analitica/validar_consultas.py` ejecuta las
mismas funciones de `app/consultas.py` sobre un micro-dataset de 10
filas con un `event_id` duplicado y compara contra valores calculados a
mano: deduplicación 10 -> 9 y resultados esperados de Q1–Q5 (ventana de
90 días, proporciones, media y p95, percentiles y % fuera de SLA).
Evidencia: `p5_validacion_consultas.txt` (`make p5-validar`), 6/6.

Sobre el conjunto principal las filas coinciden con lo esperado para
cada agrupación: Q2 da 360 = 30 equipos × 12 meses; Q3 da 5, una por
combinación de modalidad y tipo de tarea; Q4 da 180, una por proyecto;
Q5 da 60, una por cliente; y Q1 da 359 de un máximo de 364 (91 días
tocados por la ventana × 4 modalidades).

Versiones anteriores de Q1–Q3 no respondían el enunciado (Q1 agrupaba
por mes y tipo de evento, Q2 y Q3 por otras claves) y el conjunto
principal se limitaba con un filtro fijo a 2025; se reescribieron en
`app/consultas.py`. Los tiempos y planes de esta sección se midieron
con el código y el dataset corregidos.

**Planes de ejecución (`explain`) de Q1 y Q3** (`p5_analitica.txt`,
`plan_explain` con `w=1`):

- **Lectura:** `Scan parquet` sobre los 60 archivos
  (`InMemoryFileIndex`), leyendo solo las columnas que usa cada consulta
  (5 de 20 en Q1 y Q3). No hay `PushedFilters` ni `PartitionFilters`: el
  filtro va después de la deduplicación y la lectura con
  `mes=*/*.parquet` no expone `mes` como columna, así que se leen todas
  las filas.

- **Deduplicación por `event_id`:** `Sort` + `SortAggregate` parcial,
  `Exchange hashpartitioning(event_id, N)`, `Sort` + `SortAggregate`
  final (con `first(...)` por columna). Es la parte común a las cinco
  consultas.

- **Q1:** `Filter` (`revision_aprobada` y `ocurrido_en > 2026-07-31
  19:38:26`, la ventana de 90 días ya resuelta), `countDistinct` en dos
  fases (`HashAggregate` por día, modalidad y tarea; `Exchange`;
  `HashAggregate` por día y modalidad; `Exchange`; final) y
  `Exchange rangepartitioning` + `Sort` por día y modalidad.

- **Q3:** `Filter` (`tarea_anotada` y `duracion_ms` no nulo),
  `ObjectHashAggregate` parcial con `count`, `avg` y
  `percentile_approx(0.95)`, `Exchange hashpartitioning(modalidad,
  tipo_tarea)`, agregación final y `rangepartitioning` + `Sort`.

**Tiempos** (`p5_analitica.txt`, 60 archivos Parquet, 2026-11-25
01:15:14 a 01:17:54 UTC). Tiempo medido en el cliente, en ms; el del
servidor difiere en menos de 25 ms. P5 mide el servicio `analitica`
directamente, no por el gateway:

| Consulta | `w=1`, 1.ª ejec. | `w=1`, 2.ª ejec. | `w=2`, 1.ª ejec. | `w=2`, 2.ª ejec. | Filas |
|---|---|---|---|---|---|
| Q1 | 19.947,30 | 12.184,95 | 12.707,77 | 6.122,94 | 359 |
| Q2 | 7.324,28 | 6.397,51 | 3.807,42 | 3.855,93 | 360 |
| Q3 | 6.767,46 | 6.326,63 | 4.212,25 | 3.614,07 | 5 |
| Q4 | 6.448,99 | 6.661,15 | 4.008,40 | 3.445,40 | 180 |
| Q5 | 6.652,97 | 6.357,38 | 4.206,72 | 3.666,05 | 60 |

**Primera frente a segunda ejecución.** La diferencia grande está en Q1
(19,95 s frente a 12,18 s con `w=1`; 12,71 s frente a 6,12 s con
`w=2`). Q1 es la primera consulta tras recrear el servicio, así que su
primera ejecución incluye el arranque de la sesión Spark y la primera
lectura de los archivos; no se aisló con una medición aparte. En Q2–Q5
la diferencia es de hasta 0,93 s, o negativa (Q4 con `w=1`, Q2 con
`w=2`), y se toma como ruido de medición.

**Comparativa de 1 frente a 2 trabajadores.** Se recreó `analitica`
con `SPARK_WORKERS=1` y luego con `2`; `/salud` reportó
`spark_workers: 1` y `2` (`local[N]` dentro del mismo contenedor en
WSL2, no un clúster multi-nodo). En las segundas ejecuciones los tiempos
bajan de 6,33–12,18 s (`w=1`) a 3,45–6,12 s (`w=2`), una mejora de
1,66× a 1,99×. Q2–Q5 quedan en un rango estrecho (3,45–3,86 s con
`w=2`) pese a calcular cosas distintas, consistente con que la lectura y
la deduplicación comunes dominen el costo; Q1 cuesta más por el
`countDistinct` en dos fases. Limitaciones: una sola muestra por celda y
`w=1` siempre antes que `w=2`, con la caché del sistema operativo ya
caliente.

El conjunto ampliado `eventos_grande` (~10 millones de filas) no está
generado y se declara **no disponible**.

**Limitaciones de Spark en el entorno utilizado.**

- Ejecución local dentro de un contenedor en WSL2 (`local[1]` y
  `local[2]`), sin distribución multi-nodo real.

- La deduplicación global `dropDuplicates(["event_id"])` exige un
  `Exchange` de todas las filas antes de agregar; es el costo común a
  todas las consultas.

- Los filtros se aplican sobre `ocurrido_en` y no sobre la columna de
  partición `mes`, así que no hay poda de particiones: se leen todos los
  meses publicados.

- El servicio no limita las consultas simultáneas: bajo carga (P6) se
  acumulan en cola, cada una tarda más y las del final superan los 120 s
  del cliente (sección 11).


---

## 10. Pruebas y evidencia experimental

Cada prueba deja un archivo en `evidence/` con fecha y hora UTC,
objetivo, máquina y configuración. Las latencias se reportan como p50,
p95 y p99 con el número de muestras cuando fue posible. Todas las
cifras son de la corrida completa del 2026-11-25 (sección 14).

| Prueba | Criterio del enunciado | Resultado | Secc. |
|---|---|---|---|
| P1. REST y despliegue | Rutas, `/salud` normal, degradado y caído; solo el gateway expuesto; el gateway recibe el tráfico del simulador | Rutas 14/14; flujo 26/26; salud por capa, puertos y aislamiento 54/54; el simulador opera solo contra el gateway (`p6_carga.txt`; código verificado en `p1_salud_aislamiento.txt`) | 3 |
| P2. Registro y concurrencia | 10 asignaciones y 40 sin tarea; ninguna tarea con dos personas; segundo envío 409 | 10×200 y 40×204, cero dobles asignaciones; segundo envío 409; p50 de `/tareas/siguiente` 1,26 s bajo la ráfaga | 5.2 |
| P3. Sesión | Borrador ≤ 5 s; sigue tras reiniciar Valkey; ninguna tarea con dos dueños | 29/29 comprobaciones correctas; borrador de 2,013 s; recuperación tras reinicio en 0,537 s; sin dobles asignaciones observadas | 6 |
| P4. Historial | Materialización Parquet, reprocesamiento, publicación atómica, deduplicación y frescura | 27/27 comprobaciones correctas; 3 filas físicas reducidas a 2 eventos únicos; frescura de 9,1–10,1 s (4 eventos, dominada por la lectura completa del datalake) | 7 |
| P5. Analítica | Resultados validados, planes de Q1 y Q3, limitaciones | Q1–Q5 validadas contra valores a mano, 6/6; sobre 830.256 eventos (60 archivos), segundas ejecuciones de 6,3–12,2 s con `w=1` y 3,4–6,1 s con `w=2` (1,66×–1,99×); planes `explain` de Q1 y Q3 capturados; conjunto grande no disponible | 9 |
| P6. Carga | p50/p95/p99 por endpoint, envíos por minuto, errores, `docker stats` | 10, 40 y 100 usuarios, 30 s de calentamiento y 3 × 90 s por nivel. Con 10 usuarios: 106–114 envíos/min, 0 errores en las rutas de tareas, p95 de 52–219 ms. Con 40 y 100 el inventario ya estaba agotado: solo se midió el camino sin tarea (`204`, p50 6–15 ms). `/analitica` supera los 120 s bajo carga. `docker stats` cada 15 s | 11 |
| P8. Fallas | Con un nodo caído se escribe; sin mayoría no; lo confirmado sigue tras la elección; el borrador sobrevive al reinicio de Valkey | MongoDB 14/14: RTO de escritura 11,2 s tras la caída del primario, RPO 0. Valkey 31/31: RTO de 1,094 s en reinicio ordenado, 12,649 s en parada controlada y 1,155 s en caída abrupta, sin pérdida de versiones de borrador observada | 12 |

---

## 11. Resultados de carga y cuellos de botella (P6)

*Responsable: Emmanuel.*

Prueba ejecutada sobre el gateway (`localhost:8000`) con `make p6`
(`p6_carga.txt`, 2026-11-25 01:34:45 a 01:58:58 UTC), justo después de
recargar el dataset. **10, 40 y 100 usuarios virtuales**, cada nivel con
**30 s de calentamiento** y **3 repeticiones de 90 s**, muestreo de
`docker stats` cada 15 s. Roles del simulador: 60 % anotadores
(siguiente, borradores, enviar; liberan el 15 %), 20 % revisores
(aprueban o rechazan el 25 %) y 20 % consumidores (exportan en bucle y
hacen una consulta `/analitica/{q}` por corrida, que los bloquea hasta
que responde o vence su timeout de 120 s; por eso hay pocas
exportaciones en las corridas donde la consulta tarda). Proyecto: *Proyecto 001*
(`df6aed72-19b1-59e2-bc5f-048ccd10a3c0`), con 500 tareas disponibles al
inicio. Latencias en ms. Los `504` de `/analitica` los registra el
simulador cuando su propio timeout de 120 s vence; no son respuestas del
gateway.

### 11.1 Nivel de 10 usuarios

| Rep. | Endpoint | n | Prom. | p50 | p95 | p99 | Códigos |
|---|---|---|---|---|---|---|---|
| 1 | `POST /tareas/siguiente` | 195 | 58,93 | 35,70 | 219,58 | 295,81 | `200`: 195 |
| 1 | `PUT /tareas/{id}/borrador` | 284 | 7,72 | 6,85 | 13,04 | 19,22 | `200`: 284 |
| 1 | `POST /tareas/{id}/enviar` | 159 | 47,81 | 30,37 | 187,87 | 346,03 | `200`: 159 |
| 1 | `POST /tareas/{id}/revision` | 149 | 68,90 | 39,69 | 326,96 | 473,52 | `200`: 149 |
| 1 | `POST /tareas/{id}/liberar` | 36 | 18,98 | 14,06 | 54,58 | 63,55 | `200`: 36 |
| 1 | `GET /proyectos/{id}/exportar` | 214 | 9,97 | 8,48 | 14,35 | 18,70 | `200`: 214 |
| 1 | `GET /analitica/{q}` | 2 | 35.584,43 | 35.573,12 | 35.595,73 | 35.595,73 | `200`: 2 |
| 2 | `POST /tareas/siguiente` | 197 | 52,68 | 35,19 | 155,60 | 294,18 | `200`: 197 |
| 2 | `PUT /tareas/{id}/borrador` | 287 | 7,92 | 6,82 | 15,13 | 24,75 | `200`: 287 |
| 2 | `POST /tareas/{id}/enviar` | 171 | 36,93 | 29,01 | 67,44 | 125,31 | `200`: 171 |
| 2 | `POST /tareas/{id}/revision` | 158 | 54,90 | 39,37 | 80,53 | 543,94 | `200`: 158 |
| 2 | `POST /tareas/{id}/liberar` | 26 | 30,75 | 14,09 | 133,28 | 189,74 | `200`: 26 |
| 2 | `GET /proyectos/{id}/exportar` | 2 | 91,23 | 86,67 | 95,80 | 95,80 | `200`: 2 |
| 2 | `GET /analitica/{q}` | 2 | 92.799,03 | 92.762,45 | 92.835,61 | 92.835,61 | `200`: 2 |
| 3 | `POST /tareas/siguiente` | 189 | 40,64 | 34,20 | 65,18 | 171,84 | `200`: 189 |
| 3 | `PUT /tareas/{id}/borrador` | 289 | 7,23 | 6,30 | 12,01 | 20,01 | `200`: 289 |
| 3 | `POST /tareas/{id}/enviar` | 166 | 30,98 | 28,57 | 52,34 | 57,77 | `200`: 166 |
| 3 | `POST /tareas/{id}/revision` | 154 | 40,10 | 37,39 | 57,41 | 67,92 | `200`: 154 |
| 3 | `POST /tareas/{id}/liberar` | 23 | 13,36 | 12,64 | 19,80 | 20,63 | `200`: 23 |
| 3 | `GET /proyectos/{id}/exportar` | 2 | 102,54 | 98,45 | 106,63 | 106,63 | `200`: 2 |
| 3 | `GET /analitica/{q}` | 2 | 120.016,98 | 120.010,61 | 120.023,36 | 120.023,36 | `504`: 2 |

| Rep. | Inicio (UTC) | Solicitudes | Envíos 200 | Envíos/min | Aprob. / rech. | Errores (no 2xx) | Tareas disp. antes -> después | CPU máx. (`docker stats`) |
|---|---|---|---|---|---|---|---|---|
| 1 | 01:35:23 | 1039 | 159 | 106,0 | 121 / 28 | 0 (0,0 %) | 446 -> 315 | analitica 90 %, mongo-2 65 %, mongo-1 64 % |
| 2 | 01:37:07 | 843 | 171 | 114,0 | 119 / 39 | 0 (0,0 %) | 315 -> 183 | analitica 67 %, mongo-1 62 %, mongo-2 61 % |
| 3 | 01:38:52 | 825 | 166 | 110,7 | 124 / 30 | 2 (0,2 %) | 183 -> 47 | analitica 177 %, mongo-1 56 %, mongo-3 55 % |

### 11.2 Nivel de 40 usuarios

| Rep. | Endpoint | n | Prom. | p50 | p95 | p99 | Códigos |
|---|---|---|---|---|---|---|---|
| 1 | `POST /tareas/siguiente` | 8363 | 7,99 | 6,40 | 10,47 | 18,84 | `200`: 2, `204`: 8361 |
| 1 | `PUT /tareas/{id}/borrador` | 2 | 12,69 | 6,58 | 18,81 | 18,81 | `200`: 2 |
| 1 | `POST /tareas/{id}/enviar` | 1 | 28,71 | 28,71 | 28,71 | 28,71 | `200`: 1 |
| 1 | `POST /tareas/{id}/revision` | 1 | 37,49 | 37,49 | 37,49 | 37,49 | `200`: 1 |
| 1 | `POST /tareas/{id}/liberar` | 1 | 21,40 | 21,40 | 21,40 | 21,40 | `200`: 1 |
| 1 | `GET /proyectos/{id}/exportar` | 8 | 351,16 | 365,05 | 470,09 | 470,09 | `200`: 8 |
| 1 | `GET /analitica/{q}` | 8 | 120.022,14 | 120.015,60 | 120.050,13 | 120.050,13 | `504`: 8 |
| 2 | `POST /tareas/siguiente` | 8374 | 8,02 | 6,37 | 10,38 | 16,54 | `204`: 8374 |
| 2 | `GET /proyectos/{id}/exportar` | 8 | 399,37 | 420,46 | 487,69 | 487,69 | `200`: 8 |
| 2 | `GET /analitica/{q}` | 8 | 120.017,98 | 120.020,21 | 120.040,43 | 120.040,43 | `504`: 8 |
| 3 | `POST /tareas/siguiente` | 8385 | 7,64 | 6,17 | 9,74 | 15,53 | `204`: 8385 |
| 3 | `GET /proyectos/{id}/exportar` | 8 | 397,92 | 424,00 | 458,51 | 458,51 | `200`: 8 |
| 3 | `GET /analitica/{q}` | 8 | 120.023,35 | 120.025,42 | 120.034,95 | 120.034,95 | `504`: 8 |

| Rep. | Inicio (UTC) | Solicitudes | Envíos 200 | Envíos/min | Aprob. / rech. | Errores (no 2xx) | Tareas disp. antes -> después | CPU máx. (`docker stats`) |
|---|---|---|---|---|---|---|---|---|
| 1 | 01:43:15 | 8384 | 1 | 0,7 | 1 / 0 | 8 (0,1 %) | 1 -> 0 | analitica 143 %, mongo-1 61 %, mongo-2 55 % |
| 2 | 01:45:34 | 8390 | 0 | 0,0 | 0 / 0 | 8 (0,1 %) | 0 -> 0 | analitica 181 %, mongo-1 65 %, mongo-3 57 % |
| 3 | 01:47:49 | 8401 | 0 | 0,0 | 0 / 0 | 8 (0,1 %) | 0 -> 0 | analitica 86 %, mongo-1 42 %, mongo-2 32 % |

### 11.3 Nivel de 100 usuarios

| Rep. | Endpoint | n | Prom. | p50 | p95 | p99 | Códigos |
|---|---|---|---|---|---|---|---|
| 1 | `POST /tareas/siguiente` | 20048 | 19,02 | 14,35 | 38,23 | 73,31 | `204`: 20048 |
| 1 | `GET /proyectos/{id}/exportar` | 20 | 487,39 | 591,70 | 768,68 | 890,21 | `200`: 20 |
| 1 | `GET /analitica/{q}` | 20 | 120.046,56 | 120.046,73 | 120.093,09 | 120.097,03 | `504`: 20 |
| 2 | `POST /tareas/siguiente` | 19972 | 20,03 | 15,10 | 44,94 | 64,71 | `204`: 19972 |
| 2 | `GET /proyectos/{id}/exportar` | 20 | 379,25 | 421,50 | 636,54 | 729,35 | `200`: 20 |
| 2 | `GET /analitica/{q}` | 20 | 120.047,00 | 120.057,20 | 120.069,55 | 120.075,94 | `504`: 20 |
| 3 | `POST /tareas/siguiente` | 20073 | 18,71 | 13,98 | 39,99 | 67,90 | `204`: 20073 |
| 3 | `GET /proyectos/{id}/exportar` | 20 | 510,27 | 639,38 | 889,83 | 902,79 | `200`: 20 |
| 3 | `GET /analitica/{q}` | 20 | 120.042,35 | 120.032,33 | 120.088,14 | 120.112,49 | `504`: 20 |

| Rep. | Inicio (UTC) | Solicitudes | Envíos 200 | Envíos/min | Aprob. / rech. | Errores (no 2xx) | Tareas disp. antes -> después | CPU máx. (`docker stats`) |
|---|---|---|---|---|---|---|---|---|
| 1 | 01:52:13 | 20088 | 0 | 0,0 | 0 / 0 | 20 (0,1 %) | 0 -> 0 | gateway 88 %, mongo-1 78 %, analitica 70 % |
| 2 | 01:54:29 | 20012 | 0 | 0,0 | 0 / 0 | 20 (0,1 %) | 0 -> 0 | gateway 90 %, analitica 82 %, mongo-1 80 % |
| 3 | 01:56:46 | 20113 | 0 | 0,0 | 0 / 0 | 20 (0,1 %) | 0 -> 0 | gateway 86 %, mongo-1 70 %, mongo-3 59 % |

**Cuellos de botella y observaciones.**

1.  **Agotamiento del inventario.** El calentamiento de 10 usuarios
    consumió 54 tareas (500 -> 446) y las tres repeticiones bajaron a 315,
    183 y 47. El calentamiento de 40 usuarios consumió el resto: la
    primera repetición empezó con 1 tarea. Desde ahí `POST
    /tareas/siguiente` responde `204` en todos los casos, así que **los
    niveles de 40 y 100 usuarios solo miden el camino sin tarea** (y
    `exportar`/`analitica`), no el flujo completo. No se puede concluir
    cuántos envíos por minuto soporta el sistema con 40 o 100 usuarios.

2.  **Flujo completo con 10 usuarios.** 106–114 envíos confirmados por
    minuto, sin errores en las rutas de tareas. Las rutas que escriben
    en MongoDB con `w: majority` (`siguiente`, `enviar`, `revision`)
    tienen p50 de 28–40 ms y p95 de 52–327 ms; las que solo tocan Valkey
    (`borrador`) quedan en p50 ~7 ms y p95 12–15 ms. La primera
    repetición muestra las colas más altas (p99 de hasta 474 ms), que
    coinciden con la consulta Spark en curso (analítica al 90 % de CPU).

3.  **Camino sin tarea con 40 y 100 usuarios.** `POST /tareas/siguiente`
    con `204` sostiene ~93 solicitudes/s con 40 usuarios (p50 6,4 ms) y
    ~223/s con 100 (p50 14–15 ms, p99 65–73 ms). Con 100 usuarios el
    gateway llega a 86–90 % de CPU y `registro` a 52–54 %: el gateway es
    el primer componente que se acerca a su límite en este camino.

4.  **Exportación.** El p50 de `exportar` pasa de 8,5 ms (10 usuarios,
    primera repetición) a 365–424 ms con 40 usuarios y 421–639 ms con
    100. Coincide con más aprobadas en el proyecto y más concurrencia;
    es consistente con el orden por `_id` en memoria visto en el plan
    `explain` (sección 5.1), aunque no se aisló.

5.  **Analítica bajo carga.** Con 10 usuarios las consultas tardaron
    35,6 s y 92,8 s (respuestas 200) y desde la tercera repetición
    superan los 120 s. Sin carga tardan 6–12 s (P5). Spark (`local[1]`)
    procesa las consultas una tras otra; el servicio no limita la
    concurrencia, así que se acumulan, y como el gateway no las cancela
    al vencer su timeout, Spark sigue trabajando en consultas que nadie
    espera (analítica llegó a 177–181 % de CPU y 1,8 GiB de memoria).
    Al terminar P6 la cola seguía activa (sección 3).

6.  **Recursos.** Los tres nodos de MongoDB llegaron a 55–80 % de CPU y
    ~500–520 MiB cada uno; Valkey se mantuvo por debajo de 1 % de CPU y
    ~7 MiB. Durante P6 seguía levantado el Compose aislado de Sesión +
    Historial (`bda-integrante2-*`), inactivo (≤ 0,2 % de CPU, ~130 MiB
    en total), que se excluyó de las tablas.

**Mejoras que quedan propuestas:** repartir a los usuarios del
simulador entre varios proyectos (o regenerar entre niveles) para medir
40 y 100 usuarios con trabajo efectivo; limitar las consultas Spark
simultáneas y responder 503 cuando la cola está llena.

---

## 12. Fallas, recuperación, RTO y RPO (P8)

### 12.1 MongoDB

*Responsable: Jimmy.*

**Configuración** (`p8_mongo_fallas.txt`): conjunto `rs-plataforma` de 3
nodos, `w: majority`, `wtimeout` 5 s, `serverSelectionTimeoutMS` 3 s,
`retryWrites=false`, `electionTimeoutMillis` 10.000.

**Método.** Caída real: se desactiva la política de reinicio del
contenedor y se envía SIGKILL, sin apagado ordenado ni cesión del rol de
primario. Todo el tráfico pasa por el gateway, en ciclos `siguiente` +
`enviar` con un anotador nuevo por ciclo. Tras cada caso se arrancan los
nodos, se devuelve `restart: unless-stopped` y se espera a 1 PRIMARY + 2
SECONDARY y `/salud` operativo.

**Definiciones.** *RTO de escritura:* tiempo desde la caída hasta la
primera escritura que vuelve a responder 200 por el gateway. *RPO:*
escrituras confirmadas con 200 que se perdieron.

| Caso | Acción | Observado |
|---|---|---|
| A | SIGKILL a un secundario | `/salud` degradado (200) a los 16,4 s. 10/10 ciclos con 200: con un nodo caído se sigue escribiendo. Vuelta a SECONDARY y operativo 5,5 s después del arranque |
| B | SIGKILL al primario | 3 ciclos con 503 (el primero a los 3,4 s) y el primer 200 a los 11,2 s. Nuevo primario (`mongo-3`) detectado a los 10,1 s. 25/25 tareas con 200 intactas (una vez, mismo dueño y estado). El antiguo primario volvió como SECONDARY en 8,1 s |
| C | SIGKILL a los dos secundarios | 38,6 s sin mayoría: 8 envíos y 3 asignaciones con 503, ninguna escritura con 200; `/salud` 503 `caido`. Recuperación a 1 PRIMARY + 2 SECONDARY en 5,5 s. El reintento del envío dio 409 |
| Final | — | 26/26 tareas con 200 intactas. 6 asignaciones con 503 de resultado indeterminado |

**Análisis.**

- **Detección del secundario caído (A).** Tardó 16,4 s: MongoDB marca
  un miembro como inalcanzable tras ~10 s sin latidos, y el monitor del
  gateway consulta cada 5 s, así que el total puede llegar a ~15 s más el
  sondeo de la prueba. Mientras tanto las escrituras siguieron con 200.

- **Elección (B).** El nuevo primario se detectó a los 10,1 s, coherente
  con `electionTimeoutMillis` de 10 s. La detección consulta
  `rs.status()` cada ~0,5 s, así que la cifra tiene esa resolución más
  la latencia de `docker exec`.

- **RTO de escritura de 11,2 s.** Desde la caída hasta el primer 200. La
  resolución es de unos 3 s, porque cada ciclo fallido espera los 3 s de
  selección de servidor.

- **RPO 0 para escrituras confirmadas.** Ninguna de las 26 tareas con
  200 se perdió, incluidas las del momento de la elección.

- **Sin mayoría no se confirma ninguna escritura (C).** Durante 38,6 s
  no hubo ningún 200, `enviar` respondió 503 sin reintento y `/salud`
  informó `caido`.

- **Un 503 no significa «no escrito» (C).** El primer envío sin
  mayoría se aplicó en el primario aislado antes de que dejara de serlo
  y respondió 503 porque no logró la confirmación de la mayoría dentro
  del `wtimeout`. Al volver la mayoría la escritura se conservó (el
  reintento dio 409 porque la tarea ya estaba `anotada`). El cliente no
  pierde nada confirmado, pero debe tratar el 503 como resultado
  indeterminado.

- **Asignaciones indeterminadas.** Las 6 asignaciones con 503 pueden
  haber quedado `abierta` sin lock en Valkey (el gateway suelta el lock
  si el registro falla). No se pierden: vuelven a ofrecerse cuando vence
  su plazo de 120 s.

**No cubierto aquí:** las fallas de Valkey
(sección 12.2) y la disponibilidad de lectura durante la
elección, que no se midió.

### 12.2 Valkey y sesiones

| Caso | Qué se comprueba | Resultado | Evidencia |
|---|---|---|---|
| Reinicio de Valkey | El borrador sobrevive (AOF `everysec`) | RTO: 1,094 s; RPO observado: 0 ms | `p8_valkey_fallas.txt` |
| Caída abrupta de Valkey | Ventana de pérdida de AOF `everysec` (~1 s) | RTO: 1,155 s; RPO observado: 0 ms | `p8_valkey_fallas.txt` |
| Sesión interrumpida | Se recupera el borrador; con el lock vencido, la tarea vuelve a la cola | Recuperación y reasignación correctas | `p3_gateway_sesion.txt` |

La prueba P8 de Valkey obtuvo 31/31 comprobaciones correctas sobre el
Compose final. Se evaluaron tres escenarios: reinicio ordenado,
indisponibilidad temporal y caída abrupta mediante `SIGKILL`.

Durante la indisponibilidad de Valkey, el gateway respondió HTTP 200 en
estado degradado, mientras que las operaciones de guardar borrador y
enviar una tarea respondieron HTTP 503.

En la caída abrupta se confirmó la terminación del contenedor mediante
el código de salida 137. El último borrador fue recuperado sin pérdida
de contenido ni de versión.

Finalmente, se restauró la política de reinicio `unless-stopped`, se
verificó la configuración de AOF y se comprobó que el anotador podía
continuar editando.

| Escenario | RTO (s) | RPO (ms) | Versiones perdidas |
|---|---|---|---|
| Reinicio ordenado | 1,094 | 0 | 0 |
| Parada controlada | 12,649 | 0 | 0 |
| Caída abrupta | 1,155 | 0 | 0 |

*RTO y RPO observados ante fallas de Valkey.*

El RTO observado incluye la ejecución de los comandos Docker y el sondeo
hasta obtener una respuesta HTTP 200 de recuperación. En la parada
controlada también incluye el tiempo empleado en comprobar el
comportamiento del gateway durante la indisponibilidad; desde el
arranque de Valkey hasta el HTTP 200 pasaron 0,906 s.

---

## 13. Limitaciones

**Registro, gateway y MongoDB**

- Todo corre en una sola máquina: las latencias, el costo de
  `w: majority` y los tiempos de elección no se extrapolan a un clúster
  distribuido.

- No hay transacción entre MongoDB y Valkey. La consistencia se apoya en
  el filtro por estado de MongoDB, la liberación del lock cuando el
  registro falla y el plazo de 120 s de la asignación.

- Un 503 puede dejar la escritura aplicada
  (sección 12.1, caso C).

- `/salud` refleja el estado con hasta 5 s de atraso. El monitor vive en
  el gateway: si el gateway cae, nadie publica salud, pero tampoco hay
  punto de entrada.

- Registrar un lote inserta las tareas sin transacción multidocumento:
  una caída a mitad puede dejar un lote parcial.

- Mientras un evento está pendiente (Historial caído), Spark no lo ve;
  el reconciliador lo reenvía cada 10 s.

- La latencia de `/tareas/siguiente` con 50 solicitudes simultáneas es
  de ~1,3 s (p50).

**Sesión e Historial**

- AOF `everysec` puede perder aproximadamente el último segundo ante una
  caída abrupta; un reinicio ordenado no equivale a ese ensayo.

- La publicación Parquet es atómica por archivo, no entre varias
  particiones mensuales.

- El consumo de Streams es *al menos una vez*; Spark debe deduplicar por
  `event_id`.

- El umbral de `XAUTOCLAIM` debe superar el tiempo normal de proceso
  para no reclamar trabajo activo cuando hay varios workers.

- La prueba de reprocesamiento P4 simuló una falla antes de `XACK`, sin
  terminar abruptamente el proceso real del worker.

- La frescura se midió con cuatro eventos y Spark precalentado, y está
  dominada por la lectura completa del datalake (~9 s); no se aisló la
  demora propia del worker ni se evaluó bajo carga.

- Se verificó la visibilidad de los eventos directamente mediante Spark
  y su `event_id`. No se midió la frescura específica hasta Q1.

- La compactación de micro-lotes Parquet se plantea como una operación
  manual de mantenimiento. No existe un compactador automático ni se
  realizaron mediciones comparativas de rendimiento antes y después de
  compactar. La estrategia requiere una ventana sin lectores ni
  escritores activos.

- Las pruebas de fallas de Valkey se realizaron con un ensayo por
  escenario. El RPO observado de 0 ms no garantiza la ausencia de
  pérdida de datos en todas las condiciones.

- La caída abrupta mediante `SIGKILL` simula la terminación del proceso
  de Valkey, pero no una pérdida eléctrica del equipo anfitrión.

- El RPO se calculó sobre las versiones confirmadas de los borradores,
  no sobre la totalidad de los datos de Valkey ni del historial de
  eventos.

**Analítica, datos y carga**

Spark se ejecutó localmente con `local[1]` y `local[2]` dentro del mismo
contenedor en WSL2; por lo tanto, la comparación entre 1 y 2
trabajadores no representa una prueba de escalabilidad en un clúster
multi-nodo real. El conjunto opcional `eventos_grande`, de
aproximadamente 10 millones de filas, no fue generado y su medición se
declara no disponible.

La deduplicación global por `event_id` y la falta de poda de particiones
por mes producen un costo elevado de lectura e intercambio entre
particiones: los filtros van sobre `ocurrido_en` y no sobre `mes`, así
que se leen todos los meses publicados.

P6 usa un único proyecto de 500 tareas: el inventario se agotó en el
calentamiento de 40 usuarios, por lo que los niveles de 40 y 100 usuarios
solo midieron el camino sin tarea (`204`), `exportar` y `analitica`; no
hay medición del flujo completo con esa concurrencia. Cada nivel tuvo
una sola corrida de 3 repeticiones.

`GET /analitica/{q}` superó los 120 s del cliente bajo carga: Spark
procesa las consultas en serie, el servicio no limita la cola y el
gateway no cancela la consulta al vencer su timeout.

---

## 14. Evidencia

Todas las filas son de la corrida completa del 2026-11-25 (sección 4
del `README.md`), en el orden en que se ejecutaron.

| Archivo | Prueba | Inicio (UTC) | Comando | Resultado |
|---|---|---|---|---|
| `maquina.txt` | Máquina de medición | 01:12:24 | ver `README.md` | WSL2, 16 CPU, 15,5 GiB para Docker |
| `p2_replica.txt` | Réplica, mayoría e índices | 01:13:42 | `make replica` | 1 PRIMARY + 2 SECONDARY, `w: majority`, 4 índices |
| `p5_validacion_consultas.txt` | Validación de Q1–Q5 contra valores a mano | 01:14:59 | `make p5-validar` | 6/6 |
| `p5_analitica.txt` | Analítica Q1–Q5, `w=1`/`w=2` y `explain` Q1/Q3 | 01:15:14 | `make p5` | Tabla de la sección 9 (60 archivos) |
| `p2_explain_registro.txt` | Planes `explain` de las consultas del registro | 01:18:47 | `make explain-registro` | 10/10 |
| `p2_registro_smoke.txt` | Transiciones y atomicidad | 01:24:27 | `make smoke-registro` | 30/30 |
| `p1_gateway_smoke.txt` | Rutas básicas del gateway | 01:24:34 | `make smoke-gateway` | 14/14 |
| `p1_gateway_flujo.txt` | Flujo completo de una tarea | 01:24:39 | `make smoke-flujo` | 26/26 |
| `p2_concurrencia_gateway.txt` | 50 contra 10 por el gateway | 01:24:44 | `make p2-gateway` | 9/9 |
| `p2_write_concern.txt` | `w:1` frente a `w: majority` | 01:24:50 | `make write-concern` | Tabla de la sección 5.3 |
| `p3_gateway_sesion.txt` | Sesión, recuperación, AOF y vencimiento de locks | 01:26:24 | `make p3` | 29/29 |
| `p4_gateway_parquet.txt` | Materialización, reprocesamiento, deduplicación y frescura | 01:33:12 | `make p4` | 27/27 |
| `datos_generacion.txt` | Conteos del generador (recarga antes de P6) | 01:34:15 | `make datos` | 60.000 tareas en MongoDB; 830.256 eventos en 60 archivos Parquet |
| `p6_carga.txt` | Carga HTTP (10, 40 y 100 usuarios) | 01:34:45 | `make p6` | Tablas de la sección 11, con p50/p95/p99 y `docker stats` |
| `p1_salud_aislamiento.txt` | `/salud` por capa, puertos y aislamiento | 02:02:03 | `make p1-salud` | 54/54 |
| `p8_mongo_fallas.txt` | Fallas de MongoDB | 02:03:42 | `make p8-mongo` | 14/14 |
| `p8_valkey_fallas.txt` | Fallas de Valkey, AOF, RTO y RPO | 02:05:31 | `make p8-valkey` | 31/31 |

Otros archivos de `evidence/` (`*_anterior.txt`,
`p8_valkey_intento1.txt` y `smoke_integrante2_20261122T055742Z.txt`) son
corridas previas que se conservan por trazabilidad; ninguna cifra de
este informe sale de ellos.

Cada archivo incluye al inicio el objetivo, la máquina y la
configuración, y al final la fecha de término.

---

## 15. Repositorio y colaboración

- Repositorio: <https://github.com/Chris-0307/proyecto2_BDA>.

- El historial de commits identifica la participación de cada
  integrante.

- Ramas de trabajo: `Valkey-e-historial-Parquet` (Christian: contratos,
  configuración, locks, borradores, recuperación, ingreso de eventos,
  publicación Parquet, reprocesamiento y pruebas). La rama develop usada
  por Jimmy y Emmanuel para el desarollo de pruebas y mediciones
  obtenidas.

- Estructura del repositorio y comandos de ejecución:
  `proyecto2/README.md`. Endpoints de cada servicio:
  `proyecto2/endpoints/README.md`.

---

## 16. Conclusiones

El registro garantiza la asignación exclusiva de tareas con una sola
operación atómica por transición: 50 intentos sobre la misma tarea
dieron un 200 y 49 respuestas 409, y 50 solicitudes contra 10 tareas
dieron 10 asignaciones distintas, sin dobles asignaciones. Con
`w: majority` el costo medido es de milisegundos (de 4,4 a 5,7 veces la
latencia de `w:1` con un hilo) y a cambio ninguna escritura confirmada
se perdió ante la caída del primario (RPO 0, RTO de escritura de
11,2 s).

La analítica con Spark calcula Q1–Q5 tal como las pide el enunciado,
validadas contra valores calculados a mano, sobre un historial generado
con los mismos ids que el registro y un flujo de revisión realista. La
deduplicación global por `event_id` es el costo común a todas las
consultas; la comparación entre `local[1]` y `local[2]` en el mismo
contenedor no equivale a un clúster multi-nodo, y el conjunto ampliado
de ~10 millones de filas no se evaluó. Sobre 830.256 eventos, las
segundas ejecuciones tardaron 6,3–12,2 s con un trabajador y 3,4–6,1 s
con dos (1,66×–1,99×).

La prueba de carga P6 mostró que, con 10 usuarios, el sistema completa
106–114 envíos por minuto sin errores y con p50 de 28–40 ms en las rutas
que escriben en MongoDB. Con 40 y 100 usuarios el inventario del único
proyecto usado ya estaba agotado, así que solo se midió el camino sin
tarea (hasta ~223 solicitudes/s con p50 de 15 ms, con el gateway cerca
del 90 % de CPU); la capacidad del flujo completo con esa concurrencia
queda sin medir. `/analitica/{q}` es el punto débil bajo carga: las
consultas se encolan en Spark y superan los 120 s.

Las pruebas de Sesión e Historial obtuvieron 29/29 y 27/27
comprobaciones correctas, respectivamente. Valkey conservó el borrador
tras un reinicio y permitió recuperar la sesión en 0,537 s, sin observar
pérdida del contenido.

El historial Parquet permitió recuperar micro-lotes pendientes sin
generar archivos adicionales y eliminar eventos duplicados mediante
Spark. Los cuatro eventos del flujo de una tarea fueron visibles en Spark
entre 9,1 y 10,1 s después del HTTP 200, en el primer sondeo; ese tiempo
lo marca la lectura completa del datalake, no la escritura.

Las pruebas de tolerancia a fallas de Valkey demostraron la recuperación
de los borradores mediante AOF después de reinicios ordenados y caídas
abruptas. En los tres escenarios evaluados no se observó pérdida de
versiones confirmadas.

Durante la indisponibilidad, el gateway mantuvo su funcionamiento en
modo degradado y rechazó las operaciones dependientes de Valkey. Una vez
restaurado el servicio, el anotador pudo continuar su trabajo sin perder
el borrador previamente guardado.
