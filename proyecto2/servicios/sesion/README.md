# Servicio de Sesión — commits 3, 4 y 5

Implementación del candado exclusivo (`SET NX PX 120000`), el hash de sesión
(`sesion:{anotador}`) y el conjunto ordenado de usuarios editando
(`editando:{proyecto}`). Los scripts Lua realizan las operaciones de forma
atómica en una instancia independiente de Valkey.

## API REST interna

| Ruta | Uso | Respuesta esperada |
| --- | --- | --- |
| `GET /salud` | Comprobar conexión Valkey | 200 / 503 |
| `POST /sesiones/iniciar` | Adquirir lock y registrar sesión | 201 / 409 / 503 |
| `POST /sesiones/latido` | Renovar lock de su propietario | 200 / 409 / 503 |
| `POST /sesiones/finalizar` | Liberar lock y cerrar sesión | 200 / 409 / 503 |
| `POST /sesiones/recuperar` | Recuperar sesión interrumpida y borrador | 200 / 409 / 503 |

Ejemplo del cuerpo JSON de `POST /sesiones/iniciar`:

```json
{
  "anotador_id": "4fa28927-9255-412b-9408-8b6b834cb697",
  "tarea_id": "62937a24-7e05-4bc9-b0f7-1f7e7484f390",
  "proyecto_id": "2d975dab-a069-45b7-a41b-7ffec0a17c30",
  "token": "9721af89-4266-4499-8e1d-c6451d701b4a",
  "etapa": "anotacion"
}
```

Para `POST /sesiones/latido` y `POST /sesiones/finalizar` se envían los
mismos cuatro identificadores sin necesidad de `etapa`.

## Separación de responsabilidades

1. El gateway solicita el lock mediante este servicio.
2. El gateway coordina el `findOneAndUpdate` atómico en Registro (MongoDB).
3. Si MongoDB no confirma la asignación, **el gateway debe llamar a
   `/sesiones/finalizar` para liberar el lock**.
4. La sesión no modifica estados definitivos ni accede a MongoDB.
5. El simulador no llama directamente a esta API interna, sino al gateway.

El servicio de Sesión incluye guardado y recuperación de borradores.
El vencimiento y la nueva asignación operacional deben coordinarse con
Registro por medio del gateway; Sesión no cambia estados en MongoDB.

> El contrato REST interno y los nombres de los servicios deben validarse
> con los otros dos integrantes antes de hacer la integración final. Se respeta el máximo de seis rutas por servicio REST interno.

## Pruebas en WSL (más adelante)

Con Valkey disponible localmente en `localhost:6379` y aislando la base 15:

```bash
cd proyecto2/servicios/sesion
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements-dev.txt
cd ../../
VALKEY_TEST_URL=redis://localhost:6379/15 python -m pytest pruebas/test_sesion_locks.py -v
```

Si Valkey no está activo, las pruebas de integración se marcan `SKIPPED`.
No deben contarse como aprobadas. En el despliegue definitivo Valkey debe
usar AOF `everysec` y un volumen persistente.

## Guardado de borradores — commit 4

La API agrega una quinta ruta interna:

| Ruta | Uso | Respuesta esperada |
| --- | --- | --- |
| `PUT /tareas/{tarea_id}/borrador` | Guarda el ultimo borrador y renueva el lock | 200 / 409 / 422 / 503 |

Ejemplo de cuerpo JSON (los identificadores son ilustrativos):

```json
{
  "anotador_id": "anotador-01",
  "tarea_id": "tarea-01",
  "proyecto_id": "proyecto-01",
  "token": "token-de-asignacion",
  "contenido": {
    "modalidad": "lidar",
    "cuboides": [{"etiqueta": "lidar#peaton", "centro": [1.2, 2.3, 3.4]}]
  }
}
```

Este endpoint corresponde al servicio **interno**. El simulador debe seguir
usando el gateway en el puerto 8000, nunca este servicio directamente.

- El gateway envia un borrador cada dos segundos mientras hay edicion activa.
- Un script Lua verifica el token de propiedad y la sesion antes de escribir.
- El hash `borrador:{tarea_id}` conserva el contenido JSON, anotador, proyecto,
  fecha de actualizacion en milisegundos y una version incremental.
- Cada guardado renueva el lock a 120 segundos y actualiza `editando:{proyecto}`.
- Si vence el lock o el token no coincide, devuelve HTTP 409 sin alterar el borrador.
- No se impone TTL al borrador, para permitir recuperacion posterior.
- La accion no escribe en MongoDB ni genera eventos para Parquet.
- El gateway debe verificar el estado operacional de la tarea y controlar
  los envios definitivos. La persistencia AOF en Valkey se probara en P3.

### Pruebas pendientes de ejecutar en WSL

Con una instancia de Valkey **de pruebas** en DB 15:

```bash
cd proyecto2/servicios/sesion
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements-dev.txt
cd ../../
VALKEY_TEST_URL=redis://localhost:6379/15 python -m pytest pruebas/test_sesion_borradores.py -v
```

Si Valkey no esta disponible, se indica `SKIPPED`; no equivale a pasar.
Para la prueba P3 completa se requiere reiniciar el contenedor Valkey,
comprobar que el ultimo borrador se conserva y documentar la evidencia.


## Recuperación de sesiones — commit 5

La sexta ruta interna es `POST /sesiones/recuperar`, con el mismo JSON de
`/sesiones/latido`: `anotador_id`, `tarea_id`, `proyecto_id` y `token`.
Solo el gateway puede llamar a este servicio; el simulador usa el gateway.

El script Lua valida **atómicamente** la sesión y el candado:

- **HTTP 200, estado `activa`**: el token coincide y el lock sigue vigente.
  Se renueva a 120 segundos y se devuelve el último borrador (si existe).
- **HTTP 200, estado `requiere_reasignacion`**: la sesión coincide, pero el
  candado ya venció. Se devuelve el borrador en **solo lectura**, sin crear
  un lock nuevo. El gateway debe validar con Registro si la tarea puede
  reasignarse antes de permitir que se edite o envíe.
- **HTTP 409**: sesión inexistente, token incorrecto o lock de otro
  propietario. Nunca devuelve el contenido de otro anotador.
- **HTTP 503**: Valkey no está disponible.

Ejemplo de respuesta exitosa con borrador:

```json
{
  "estado": "activa",
  "tarea_id": "tarea-01",
  "anotador_id": "anotador-01",
  "proyecto_id": "proyecto-01",
  "etapa": "anotacion",
  "ttl_ms": 120000,
  "borrador": {"etiquetas": ["lidar#peaton"]},
  "version": 2,
  "actualizado_en_ms": 1793970000000
}
```

Los identificadores y la fecha anteriores son ilustrativos. Si todavía no
se ha guardado un borrador, `borrador`, `version` y `actualizado_en_ms`
son `null`. El borrador no se convierte en anotación definitiva.

### Pruebas de recuperación en WSL

Desde `proyecto2/servicios/sesion` instalar dependencias de desarrollo y
regresar a `proyecto2` antes de ejecutar:

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements-dev.txt
cd ../..
VALKEY_TEST_URL=redis://localhost:6379/15 python -m pytest pruebas/test_sesion_recuperacion.py -v
```

Si no existe Valkey, los tests se marcan SKIPPED, **no aprobados**. No se
ha realizado todavía la prueba de persistencia AOF P3. Para P3, con un
volumen persistente en el Compose definitivo, se debe crear una sesión,
guardar un borrador, reiniciar **únicamente** Valkey mediante
`docker compose restart valkey`, consultar `/sesiones/recuperar` y comparar
los datos recuperados. El AOF `everysec` puede perder aproximadamente
el último segundo de cambios ante una caída abrupta; un reinicio normal
y un fallo abrupto no deben presentarse como el mismo experimento.

La asignación/retorno a cola y las comprobaciones del estado MongoDB
son responsabilidad del gateway y Registro; no quedan completas con
este commit.
