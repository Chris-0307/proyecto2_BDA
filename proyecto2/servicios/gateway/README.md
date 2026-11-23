# Gateway (`gateway:8000`)

Único punto de entrada del backend y **único servicio publicado al host**
(`8000:8000`). No accede a ninguna base de datos: solo habla HTTP con los
servicios internos. Responsable: Jimmy.

Python 3.12, FastAPI, httpx. Paquete `gateway_app`.

## Estado

| Paso | Rutas | Estado |
|---|---|---|
| 4 — básico | `POST /proyectos`, `POST /proyectos/{id}/lotes`, `GET /proyectos/{id}/exportar`, `GET /salud` | Implementado |
| 5 — flujo de tareas | `POST /tareas/siguiente`, `PUT /tareas/{id}/borrador`, `POST /tareas/{id}/enviar`, `POST /tareas/{id}/liberar`, `POST /tareas/{id}/revision`, `POST /sesiones/recuperar` | Implementado |
| 6 — analítica | `GET /analitica/{q1..q5}?conjunto=` | PENDIENTE (Emmanuel) |

## Estructura

```text
servicios/gateway/
├── Dockerfile
├── requirements.txt
└── gateway_app/
    ├── main.py      # rutas y tareas de fondo (monitor de salud, reconciliador)
    ├── salud.py     # monitor de salud por capa con mensajes en Valkey
    ├── flujo.py     # orquestación Sesión + Registro + Historial
    ├── modelos.py   # cuerpos de las rutas del flujo
    └── clientes.py  # httpx, reintentos seguros, traducción a 503
```

## Rutas de registro

| Método y ruta | Servicio interno | Códigos |
|---|---|---|
| `POST /proyectos` | `registro POST /proyectos` | 201, 404, 422, 503 |
| `POST /proyectos/{id}/lotes` | `registro POST /proyectos/{id}/lotes` | 201, 404, 422, 503 |
| `GET /proyectos/{id}/exportar?despues=&limite=` | `registro GET /proyectos/{id}/exportar` | 200, 404, 422, 503 |

El cuerpo y el código del servicio interno se devuelven tal cual. El esquema
detallado de cada cuerpo está en [`../registro/README.md`](../registro/README.md).

## Flujo de tareas

| Método y ruta | Cuerpo | Respuesta | Códigos |
|---|---|---|---|
| `POST /tareas/siguiente` | `anotador_id`, `proyecto_id` | `tarea`, `token`, `ttl_ms`, `borrador`, `evento` | 200, 204, 404, 409, 422, 503 |
| `PUT /tareas/{id}/borrador` | `anotador_id`, `proyecto_id`, `token`, `contenido` | `estado`, `version`, `ttl_ms` | 200, 409, 422, 503 |
| `POST /tareas/{id}/enviar` | `anotador_id`, `proyecto_id`, `token`, `contenido` | `tarea`, `evento` | 200, 409, 422, 503 |
| `POST /tareas/{id}/liberar` | `anotador_id`, `proyecto_id`, `token` | `tarea` | 200, 409, 422, 503 |
| `POST /tareas/{id}/revision` | `revisor_id`, `decision` (`aprobar`/`rechazar`), `motivo` | `tarea`, `eventos` | 200, 403, 404, 409, 422, 503 |
| `POST /sesiones/recuperar` | `anotador_id`, `proyecto_id`, `tarea_id`, `token` | `estado`, `borrador`, `version`, `ttl_ms` | 200, 409, 422, 503 |

El `token` lo entrega `/tareas/siguiente` y se usa en todas las demás rutas
de la tarea. Campos extra en el cuerpo -> 422.

### Qué hace cada ruta

**`siguiente`**

1. Pide al registro las candidatas del proyecto (disponibles por prioridad y
   abiertas vencidas).
2. Por cada candidata intenta el lock en Sesión (`SET NX PX`, 120 s). Si otro
   lo tiene, pasa a la siguiente.
3. Con el lock, asigna en el registro (`findOneAndUpdate` atómico). Si el
   registro responde 409, suelta el lock y prueba la siguiente; si falla de
   otra forma, suelta el lock y propaga el error.
4. Publica `tarea_abierta` en Historial e incluye el borrador recuperable del
   mismo anotador, si existe.

Responde **204** si ninguna candidata quedó libre y **409** si el anotador ya
tiene una tarea activa (debe recuperarla, enviarla o liberarla).

**`enviar`**

1. Latido en Sesión: el token debe ser el dueño del lock vigente -> si no, 409.
2. Registro: `abierta -> anotada`, filtrando por token -> 409 si ya no está
   abierta (segundo envío). **503 sin reintento** si MongoDB no responde; el
   borrador y el lock siguen en Valkey y se puede reenviar.
3. Publica `tarea_anotada` y suelta el lock.

**`liberar`**: registro `abierta -> disponible` (filtra por token) y suelta el lock.

**`revision`**: `iniciar_revision` y luego `aprobar` o `rechazar` en el
registro. El rechazo exige `motivo` (422 sin él) y devuelve la tarea a
`disponible`. Un 409 en `iniciar_revision` no corta el flujo: la tarea puede
estar ya `en_revision` por el mismo revisor, y la decisión lo valida.

**`borrador`** y **`recuperar`** se reenvían a Sesión. `recuperar` responde
`activa` (lock vigente, se renueva) o `requiere_reasignacion` (lock vencido;
el borrador es de solo lectura y el anotador debe pedir `/tareas/siguiente`,
que le devolverá el borrador si vuelve a obtener la tarea).

### Publicación de eventos

Los eventos se publican en Historial **solo después** de que MongoDB confirmó
la transición. El campo `evento` (o `eventos` en revisión) informa el resultado:

| Valor | Significado |
|---|---|
| `publicado` | Historial lo aceptó y el registro lo retiró de `eventos_pendientes` |
| `publicado_sin_confirmar` | Historial lo aceptó, pero sigue en pendientes |
| `pendiente` | Historial no lo aceptó; queda en `eventos_pendientes` del registro |

Un fallo de Historial **no cambia** la respuesta al cliente: MongoDB ya
confirmó.

**Reconciliador:** una tarea de fondo del gateway consulta cada
`GATEWAY_RECONCILIAR_S` segundos (10 por defecto) los eventos pendientes del
registro y los reenvía. Es seguro repetir: Historial deduplica por `event_id`
y `confirmar_evento` es idempotente.

## Validación básica

- Ids de ruta y el cursor `despues` deben ser UUID -> si no, 422 sin llamar
  a los servicios internos.
- Los cuerpos del flujo se validan con Pydantic (`modelos.py`).
- `limite` de exportar entre 1 y 1000 -> si no, 422.
- La validación de fondo (taxonomía, geometría por modalidad, etc.) la hace
  el registro.

## Identificador de petición

Si la petición trae `X-Request-ID`, se propaga a los servicios internos y se
devuelve en la respuesta. Si no trae, el gateway genera un UUID.

## Reintentos seguros

| Método | Reintento | Motivo |
|---|---|---|
| `GET` | 1, solo ante error de conexión o timeout | Lectura idempotente |
| `POST` / `PUT` | ninguno | Repetir podría duplicar un proyecto, un lote o una transición; el enunciado pide 503 sin reintento |

Un servicio interno que no responde (conexión, timeout) se traduce a **503**.

## `GET /salud`

Cada capa notifica su estado con mensajes guardados en Valkey:

1. Un monitor en segundo plano del gateway consulta cada 5 s
   (`SALUD_INTERVALO_S`) el `/salud` de Registro, Sesión e Historial.
2. Publica el estado de cada capa en `salud:capa:{capa}` (JSON con `estado`,
   `detalle` y `revisado_en`), con expiración de 15 s (`SALUD_TTL_S`): si el
   monitor deja de publicar, el estado expira y no queda un valor viejo.
3. Cada cambio de estado se agrega al stream `salud:eventos`, con
   `mensaje` = `capa:antes->ahora` (`inicio` si no había estado previo).
4. `GET /salud` lee esos mensajes (`fuente: valkey`). Si Valkey no responde o
   todavía no hay mensajes de las tres capas, consulta cada capa directamente
   (`fuente: directo`). El gateway responde siempre.

El gateway usa **su propia base de Valkey (2)** y solo llaves `salud:*`.
Sesión e Historial usan la base 0; el gateway no lee ni escribe sus datos.
El registro no puede publicar en Valkey (regla del enunciado), así que el
monitor del gateway, responsable del reporte de salud, publica en nombre de
cada capa.

| Estado global | Condición | Código |
|---|---|---|
| `operativo` | Las tres capas operativas | 200 |
| `degradado` | El registro responde, pero Sesión o Historial fallan, o MongoDB perdió un nodo | 200 |
| `caido` | El registro (fuente de verdad) no está disponible | 503 |

Ejemplo:

```json
{
  "servicio": "gateway",
  "estado": "operativo",
  "fuente": "valkey",
  "capas": {
    "registro":  {"estado": "operativo", "detalle": {"...": "/salud del registro"}, "revisado_en": "2026-11-22T09:00:00+00:00"},
    "sesion":    {"estado": "operativo", "detalle": {"...": "/salud de sesión"}, "revisado_en": "..."},
    "historial": {"estado": "operativo", "detalle": {"...": "/salud de historial"}, "revisado_en": "..."}
  }
}
```

Consultar los mensajes:

```bash
docker compose -f proyecto2/docker-compose.yml exec valkey valkey-cli -n 2 KEYS 'salud:*'
docker compose -f proyecto2/docker-compose.yml exec valkey valkey-cli -n 2 XREVRANGE salud:eventos + - COUNT 10
```

## Variables de entorno

| Variable | Defecto |
|---|---|
| `REGISTRO_URL` | `http://registro:8003` |
| `SESION_URL` | `http://sesion:8001` |
| `HISTORIAL_URL` | `http://historial:8002` |
| `GATEWAY_TIMEOUT_S` | `10` |
| `GATEWAY_TIMEOUT_SALUD_S` | `3` |
| `GATEWAY_RECONCILIAR_S` | `10` (0 lo desactiva) |
| `SALUD_VALKEY_URL` | `redis://valkey:6379/2` |
| `SALUD_INTERVALO_S` | `5` |
| `SALUD_TTL_S` | `15` |

## Ejecución

Desde la raíz del repositorio:

```bash
make up              # o: docker compose -f proyecto2/docker-compose.yml up -d --build
make demo            # smoke-gateway usa el cliente de la demostración
make salud           # curl -i http://localhost:8000/salud
make smoke-gateway   # rutas básicas
make smoke-flujo     # flujo completo de tareas
make p2-gateway      # 50 solicitudes simultáneas contra 10 tareas
```

`smoke-flujo` y `p2-gateway` corren en el contenedor del registro solo para
crear sus datos de prueba con pymongo (cliente y anotadores nuevos en cada
corrida); todas las operaciones del flujo pasan por el gateway.

Sin `make`:

```bash
docker compose -f proyecto2/docker-compose.yml exec -T gateway python /pruebas/p1_gateway/smoke_gateway.py \
  | tee proyecto2/evidence/p1_gateway_smoke.txt
docker compose -f proyecto2/docker-compose.yml run --rm registro python /pruebas/p1_gateway/smoke_flujo.py \
  | tee proyecto2/evidence/p1_gateway_flujo.txt
docker compose -f proyecto2/docker-compose.yml run --rm registro python /pruebas/p2_registro/p2_concurrencia_gateway.py \
  | tee proyecto2/evidence/p2_concurrencia_gateway.txt
```

Esperado: todas las líneas en `[OK]`.

Probar estados de `/salud` (base de P1):

```bash
docker compose -f proyecto2/docker-compose.yml stop historial   # -> degradado (200)
docker compose -f proyecto2/docker-compose.yml stop registro    # -> caido (503)
docker compose -f proyecto2/docker-compose.yml start historial registro
```

## Evidencia

| Archivo | Contenido | Comando |
|---|---|---|
| `evidence/p1_gateway_smoke.txt` | Rutas básicas, códigos, `X-Request-ID`, salud por capa | `make smoke-gateway` |
| `evidence/p1_gateway_flujo.txt` | Flujo completo: siguiente, borrador, recuperar, enviar, liberar, revisión, exportar | `make smoke-flujo` |
| `evidence/p1_salud_aislamiento.txt` | `/salud` con Sesión, Historial y Registro caídos; puertos publicados; aislamiento | `make p1-salud` |
| `evidence/p2_concurrencia_gateway.txt` | 50 contra 10 por el gateway, latencias p50/p95/p99 | `make p2-gateway` |
| `evidence/p8_mongo_fallas.txt` | `/salud` degradado y caído por fallas de MongoDB | `make p8-mongo` |

Resultados: [informe](../../docs/informe.md), secciones 3, 5.2 y 12.1.

### `p1_salud_aislamiento.py` (`make p1-salud`)

Corre en el host con Python estándar y Docker. Restaura cada servicio después
de detenerlo.

| Bloque | Qué comprueba |
|---|---|
| `/salud` por capa | Sesión caída -> 200 `degradado`; Historial caído -> 200 `degradado`; Registro caído -> 503 `caido`. Solo la capa detenida figura caída, el cambio queda en `salud:eventos`, las rutas que no dependen de ella siguen respondiendo y todo vuelve a `operativo` con `fuente: valkey` |
| Valkey caído | 200 `degradado` con `fuente: directo`; Sesión e Historial caídos, Registro no; al volver, `fuente: valkey` |
| Puertos | En `docker compose ps` solo `gateway` publica, y es el 8000; desde el host, 8001, 8002, 8003, 27017 y 6379 rechazan la conexión |
| Aislamiento | Gateway sin `pymongo`; Registro sin `redis`; Sesión e Historial sin `pymongo`. Se revisa en tres niveles: imports del código, paquetes instalados en la imagen y variables de entorno del contenedor. Además: el gateway usa la base 2 de Valkey, ahí solo hay llaves `salud:*` y no hay llaves `salud:*` en la base 0 |

Todos los servicios comparten la red de Compose: el aislamiento se garantiza
por dependencias y configuración, no por segmentación de red. Si algo propio
del host escucha en 27017 o 6379 (un MongoDB o Redis local), la comprobación de
puertos fallará aunque Docker esté bien configurado.
