# Gateway (`gateway:8000`)

Único punto de entrada del backend y **único servicio publicado al host**
(`8000:8000`). No accede a ninguna base de datos: solo habla HTTP con los
servicios internos. Responsable: Integrante 1.

Python 3.12, FastAPI, httpx. Paquete `gateway_app`.

## Estado

| Paso | Rutas | Estado |
|---|---|---|
| 4 — básico | `POST /proyectos`, `POST /proyectos/{id}/lotes`, `GET /proyectos/{id}/exportar`, `GET /salud` | Implementado |
| 5 — flujo de tareas | `POST /tareas/siguiente`, `PUT /tareas/{id}/borrador`, `POST /tareas/{id}/enviar`, `POST /tareas/{id}/liberar`, `POST /tareas/{id}/revision`, `POST /sesiones/recuperar` | PENDIENTE (coordina Sesión, Registro e Historial) |
| 6 — analítica | `GET /analitica/{q1..q5}?conjunto=` | PENDIENTE (Integrante 3) |

## Estructura

```text
servicios/gateway/
├── Dockerfile
├── requirements.txt
└── gateway_app/
    ├── main.py      # rutas y /salud
    └── clientes.py  # httpx, reintentos seguros, traducción a 503
```

## Rutas actuales

| Método y ruta | Servicio interno | Códigos |
|---|---|---|
| `POST /proyectos` | `registro POST /proyectos` | 201, 404, 422, 503 |
| `POST /proyectos/{id}/lotes` | `registro POST /proyectos/{id}/lotes` | 201, 404, 422, 503 |
| `GET /proyectos/{id}/exportar?despues=&limite=` | `registro GET /proyectos/{id}/exportar` | 200, 404, 422, 503 |
| `GET /salud` | `/salud` de registro, sesión e historial | 200, 503 |

El cuerpo y el código del servicio interno se devuelven tal cual. El esquema
detallado de cada cuerpo está en [`../registro/README.md`](../registro/README.md).

### Validación básica

- Ids de ruta (`proyecto_id`) y el cursor `despues` deben ser UUID → si no, 422
  sin llamar al registro.
- El cuerpo debe ser un objeto JSON → si no, 422.
- `limite` de exportar entre 1 y 1000 → si no, 422.
- La validación de fondo (taxonomía, geometría por modalidad, etc.) la hace
  el registro.

### Identificador de petición

Si la petición trae `X-Request-ID`, se propaga a los servicios internos y se
devuelve en la respuesta. Si no trae, el gateway genera un UUID.

### Reintentos seguros

| Método | Reintento | Motivo |
|---|---|---|
| `GET` | 1, solo ante error de conexión o timeout | Lectura idempotente |
| `POST` / `PUT` | ninguno | Repetir podría duplicar un proyecto o un lote; el enunciado pide 503 sin reintento |

Un servicio interno que no responde (conexión, timeout) se traduce a **503**.

## `GET /salud`

Consulta en paralelo el `/salud` de cada capa con timeout corto (3 s). El
gateway responde siempre, aunque las capas fallen.

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
  "capas": {
    "registro":  {"estado": "operativo", "detalle": {"...": "/salud del registro"}},
    "sesion":    {"estado": "operativo", "detalle": {"...": "/salud de sesión"}},
    "historial": {"estado": "operativo", "detalle": {"...": "/salud de historial"}}
  }
}
```

**PENDIENTE:** el enunciado pide que cada capa notifique su estado con
mensajes guardados en Valkey. Hoy `/salud` consulta cada servicio
directamente.

## Variables de entorno

| Variable | Defecto |
|---|---|
| `REGISTRO_URL` | `http://registro:8003` |
| `SESION_URL` | `http://sesion:8001` |
| `HISTORIAL_URL` | `http://historial:8002` |
| `GATEWAY_TIMEOUT_S` | `10` |
| `GATEWAY_TIMEOUT_SALUD_S` | `3` |

## Ejecución

Desde la raíz del repositorio:

```bash
make up            # o: docker compose -f proyecto2/docker-compose.yml up -d --build
make demo          # el smoke usa el cliente de la demostración
make salud         # curl -i http://localhost:8000/salud
make smoke-gateway # smoke con evidencia en evidence/
```

Sin `make`:

```bash
docker compose -f proyecto2/docker-compose.yml exec -T gateway python /pruebas/smoke_gateway.py \
  | tee proyecto2/evidence/p1_gateway_smoke_$(date -u +%Y%m%dT%H%M%SZ).txt
```

Desde el host (prueba el puerto publicado):

```bash
GATEWAY_URL=http://localhost:8000 python3 proyecto2/pruebas/smoke_gateway.py
```

Esperado: todas las líneas en `[OK]`.

Probar estados de `/salud` (base de P1):

```bash
docker compose -f proyecto2/docker-compose.yml stop historial   # → degradado (200)
docker compose -f proyecto2/docker-compose.yml stop registro    # → caido (503)
docker compose -f proyecto2/docker-compose.yml start historial registro
```

## Evidencia

PENDIENTE: ejecutar `make smoke-gateway` y registrar el archivo aquí.
