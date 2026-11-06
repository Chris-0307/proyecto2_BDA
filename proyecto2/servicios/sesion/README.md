# Servicio de Sesión — commits 3 y 4

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

El servicio de Sesión ya incluye el guardado de borradores, pero aún no
implementa la recuperación por API ni el vencimiento en MongoDB.
Estos aspectos se incorporarán e integrarán después.

> El contrato REST interno y los nombres de los servicios deben validarse
> con los otros dos integrantes antes de hacer la integración final. Para
> respetar el máximo de seis rutas por servicio, se reserva una ruta para
> recuperar borradores, además de las cinco rutas actuales.

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
