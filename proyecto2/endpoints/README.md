# Endpoints

Documentación de los endpoints del gateway y de los servicios internos:
rutas, cuerpos, códigos HTTP y contratos entre servicios. El detalle de cada
servicio está en su README.

## Servicios y exposición

| Servicio | Puerto | Publicado al host | Rutas | Detalle |
|---|---|---|---|---|
| Gateway | 8000 | Sí (único) | 10 | [`servicios/gateway/README.md`](../servicios/gateway/README.md) |
| Registro | 8003 | No | 6 | [`servicios/registro/README.md`](../servicios/registro/README.md) |
| Sesión | 8001 | No | 6 | [`servicios/sesion/README.md`](../servicios/sesion/README.md) |
| Historial | 8002 | No | 2 | [`servicios/historial/README.md`](../servicios/historial/README.md) |
| Analítica | — | No | — | PENDIENTE (Emmanuel) |

Los servicios internos respetan el máximo de seis rutas por servicio. El
simulador y cualquier cliente externo solo hablan con el gateway.

## Documentación generada por FastAPI

Todos los servicios están hechos con FastAPI, que genera a partir del código la
especificación OpenAPI y una página interactiva para probar las rutas. Es la
referencia exacta de cuerpos y esquemas; este README es el resumen.

| Servicio | Página interactiva | Especificación |
|---|---|---|
| Gateway | <http://localhost:8000/docs> | <http://localhost:8000/openapi.json> |
| Registro, Sesión, Historial | No publicados al host | Ver abajo |

Para tener la especificación de los cuatro servicios como archivos (los
internos no publican puerto), con el Compose levantado y desde la raíz del repo:

```bash
make endpoints
```

Escribe `endpoints/openapi/{gateway,registro,sesion,historial}.json`. Vuelva a
correrlo si cambia alguna ruta.

## Gateway (`http://localhost:8000`)

Todas las respuestas incluyen `X-Request-ID` (el recibido o uno generado), que
se propaga a los servicios internos. Un servicio interno que no responde se
traduce a 503. Solo los `GET` se reintentan una vez ante error de conexión.

| Método y ruta | Cuerpo / parámetros | Respuesta | Códigos |
|---|---|---|---|
| `POST /proyectos` | `cliente_id`, `nombre`, `taxonomia[]`, `sla_horas` | Proyecto creado | 201, 404, 422, 503 |
| `POST /proyectos/{id}/lotes` | `nombre`, `tareas[]` (`modalidad`, `tipo_tarea`, `prioridad`, `archivo`) | `batch_id`, `tareas_creadas`, `tarea_ids` | 201, 404, 422, 503 |
| `GET /proyectos/{id}/exportar` | `despues` (cursor), `limite` (1–1000) | `items`, `siguiente` | 200, 404, 422, 503 |
| `POST /tareas/siguiente` | `anotador_id`, `proyecto_id` | `tarea`, `token`, `ttl_ms`, `borrador`, `evento` | 200, 204, 404, 409, 422, 503 |
| `PUT /tareas/{id}/borrador` | `anotador_id`, `proyecto_id`, `token`, `contenido` | `estado`, `version`, `ttl_ms` | 200, 409, 422, 503 |
| `POST /tareas/{id}/enviar` | `anotador_id`, `proyecto_id`, `token`, `contenido` | `tarea`, `evento` | 200, 409, 422, 503 |
| `POST /tareas/{id}/liberar` | `anotador_id`, `proyecto_id`, `token` | `tarea` | 200, 409, 422, 503 |
| `POST /tareas/{id}/revision` | `revisor_id`, `decision` (`aprobar`/`rechazar`), `motivo` | `tarea`, `eventos` | 200, 403, 404, 409, 422, 503 |
| `POST /sesiones/recuperar` | `anotador_id`, `proyecto_id`, `tarea_id`, `token` | `estado`, `borrador`, `version`, `ttl_ms` | 200, 409, 422, 503 |
| `GET /salud` | — | `estado` global y por capa | 200, 503 |
| `GET /analitica/{q1..q5}?conjunto=` | — | — | PENDIENTE |

Significado de los códigos del flujo:

| Código | Cuándo |
|---|---|
| 204 | `siguiente`: ninguna tarea disponible |
| 403 | `revision`: el actor no es revisor |
| 409 | Token ajeno o lock vencido; la tarea ya no está en el estado esperado (segundo envío, aprobar dos veces); el anotador ya tiene una tarea activa |
| 422 | Cuerpo inválido, id que no es UUID, rechazo sin `motivo` |
| 503 | MongoDB, Valkey o un servicio interno no responde. `enviar` no reintenta |

`/salud`: `operativo` (200), `degradado` (200: Sesión o Historial caídos, o
MongoDB sin un nodo) o `caido` (503: el registro no está disponible). El estado
de cada capa sale de los mensajes que el monitor del gateway guarda en Valkey
(base 2: `salud:capa:{capa}` y el stream `salud:eventos`); `fuente` indica
`valkey` o `directo` (Valkey no disponible).

## Registro (`registro:8003`)

Fuente de verdad del estado de proyectos, lotes y tareas (MongoDB en réplica).

| Método y ruta | Uso | Códigos |
|---|---|---|
| `POST /proyectos` | Crea proyecto, taxonomía y SLA | 201, 404, 422, 503 |
| `POST /proyectos/{id}/lotes` | Registra un lote (1–5000 tareas) en `disponible` | 201, 404, 422, 503 |
| `GET /tareas` | `proyecto_id&candidatas=true`, `anotador_id[&estado]` o `eventos_pendientes=true` | 200, 422, 503 |
| `POST /tareas/{id}/transicion` | `accion`: `asignar`, `enviar`, `liberar`, `iniciar_revision`, `aprobar`, `rechazar`, `confirmar_evento` | 200, 403, 404, 409, 422, 503 |
| `GET /proyectos/{id}/exportar` | Tareas `aprobada`, paginadas por `_id` | 200, 404, 503 |
| `GET /salud` | Estado de la réplica | 200, 503 |

Cada transición es un único `findOneAndUpdate` con filtro por estado (y token
o revisor). La respuesta incluye la tarea y el evento generado, que queda en
`eventos_pendientes` hasta que el gateway lo publica y llama a
`confirmar_evento`.

## Sesión (`sesion:8001`)

Locks, sesiones y borradores en Valkey (Christian). Cuerpo común:
`anotador_id`, `tarea_id`, `proyecto_id`, `token`.

| Método y ruta | Uso | Códigos |
|---|---|---|
| `POST /sesiones/iniciar` | `SET NX PX` 120 s + sesión (`etapa` opcional) | 201, 409, 422, 503 |
| `POST /sesiones/latido` | Renueva el lock si el token coincide | 200, 409, 422, 503 |
| `POST /sesiones/finalizar` | Libera lock y sesión propios | 200, 409, 422, 503 |
| `PUT /tareas/{id}/borrador` | Guarda el último borrador (`contenido`) y renueva el lock | 200, 409, 422, 503 |
| `POST /sesiones/recuperar` | Sesión y borrador: `activa` o `requiere_reasignacion` | 200, 409, 422, 503 |
| `GET /salud` | Comprueba Valkey | 200, 503 |

`iniciar` responde 409 con dos detalles distintos: la tarea ya está bloqueada
(el gateway prueba otra candidata) o el anotador ya tiene una sesión activa
(el gateway responde 409 al cliente).

## Historial (`historial:8002`)

Recepción idempotente de eventos (Christian). El worker `historial-worker`
no expone HTTP.

| Método y ruta | Uso | Códigos |
|---|---|---|
| `POST /eventos` | Encola un evento de negocio | 202 nuevo, 200 repetido, 409 `event_id` con otro contenido, 422 inválido, 503 |
| `GET /salud` | Comprueba Valkey | 200, 503 |

## Contrato de eventos (Registro -> Gateway -> Historial)

El registro arma el evento dentro de la transición; el gateway lo publica sin
modificarlo. Esquema `EventoNegocio` de Historial (`extra=forbid`):

| Campo | Presente en |
|---|---|
| `event_id`, `tipo_evento`, `estado`, `ocurrido_en`, `tarea_id`, `proyecto_id`, `cliente_id`, `batch_id`, `modalidad`, `tipo_tarea`, `version`, `sla_horas` | Todos |
| `anotador_id`, `equipo_id`, `tomada_en` | Todos |
| `duracion_ms` (toma -> envío) | `tarea_anotada` |
| `revisor_id` | `revision_iniciada`, `revision_aprobada`, `revision_rechazada` |
| `aprobada_en` | `revision_aprobada` |
| `motivo_rechazo` | `revision_rechazada` |

| Transición | `tipo_evento` | `estado` en Parquet | Estado en MongoDB |
|---|---|---|---|
| `siguiente` | `tarea_abierta` | `abierta` | `abierta` |
| `enviar` | `tarea_anotada` | `anotada` | `anotada` |
| inicio de revisión | `revision_iniciada` | `en_revision` | `en_revision` |
| aprobación | `revision_aprobada` | `aprobada` | `aprobada` |
| rechazo | `revision_rechazada` | `rechazada` | `disponible` |

`liberar` y los borradores no generan evento.
