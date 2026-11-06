# Servicio de Historial - commit 6

Primer componente del historial de eventos. **Todavia no genera archivos
Parquet**: ese trabajo corresponde al commit 7. El objetivo actual es recibir
transiciones confirmadas y conservarlas en una cola Valkey Stream.

## Que se incluye

- API REST interna con `GET /salud` y `POST /eventos` (puerto interno 8002).
- Validacion de un esquema preliminar de evento con Pydantic.
- Recepcion idempotente: `event_id` repetido con igual contenido no se encola
  dos veces; distinto contenido devuelve HTTP 409.
- Cola `historial:eventos`, indice `historial:ids` y grupo `historial-parquet`.
- Base del consumidor con `XREADGROUP` y `XACK` **solo despues** de que un
  callback haya publicado almacenamiento persistente (futuro Parquet).

## Respuestas de `POST /eventos`

- **202**: evento nuevo, almacenado en Valkey Stream.
- **200**: reintento identico, sin duplicar en el Stream.
- **409**: `event_id` ya registrado con otro contenido.
- **422**: cuerpo invalido o transicion incoherente.
- **503**: Valkey no disponible.

Las respuestas 200/202 de este endpoint **no significan** que Spark ya pueda
leer el evento. La materializacion Parquet es asincrona.

## Ejemplo de evento

`POST /eventos` (desde el gateway, nunca directamente desde el simulador):

```json
{
  "event_id": "07490a49-d845-47db-ad68-7783693d0c29",
  "tipo_evento": "revision_rechazada",
  "estado": "rechazada",
  "ocurrido_en": "2026-11-06T19:40:00Z",
  "tarea_id": "e0ab7e15-b554-46e2-89db-53fb59a53b99",
  "proyecto_id": "66f31da5-c6f5-49a7-a60d-261b9bfdaf95",
  "cliente_id": "ffb5d8cc-1be9-4d53-9aa6-d7129da300fe",
  "modalidad": "lidar",
  "tipo_tarea": "cuboide_3d",
  "version": 5,
  "motivo_rechazo": "etiqueta_incorrecta"
}
```

El rechazo queda como `rechazada` en el historial, aunque Registro vuelva
la tarea a `disponible`. Los **borradores** no emiten estos eventos.

## Integracion con el resto del equipo

1. **Registro / Gateway:** confirmacion de MongoDB antes de publicar cada
   evento. Si un HTTP 200 del gateway debe sobrevivir a una interrupcion
   antes de `XADD`, aun hace falta acordar un mecanismo durable de recuperacion
   (p. ej., outbox/reconciliacion); este commit no ofrece transacciones
   distribuidas entre Mongo y Valkey.
2. **Analitica:** acordar nombres, tipos y nulos del esquema antes de fijar
   la estructura fisica de Parquet, especialmente para Q1-Q5.
3. **Docker Compose:** agregar un servicio `historial` construido desde
   `./servicios/historial`, con `VALKEY_URL=redis://valkey:6379/0` y sin
   `ports:` hacia el host. El Valkey compartido debe tener AOF `everysec`
   y volumen duradero.
4. **Consumidor:** no iniciar todavia un worker en produccion. `procesar_lote`
   es una base para el escritor del commit 7 y solo hace XACK cuando el
   callback de persistencia completa su trabajo. Recuperar entradas del
   grupo pendientes tras caidas se desarrollara en el commit 8.

## Pruebas en WSL (cuando esten listos)

Desde `proyecto2/servicios/historial`:

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements-dev.txt
cd ../..
python -m pytest pruebas/test_historial_eventos.py -v
```

Las pruebas unitarias no necesitan Valkey. Las pruebas de integracion con
Valkey usan por defecto DB 15 y se marcan `SKIPPED` si no hay servidor;
`SKIPPED` **no equivale** a un resultado satisfactorio para la rubrica.

**Limitaciones pendientes:** no hay archivos Parquet, no hay publicacion de
micro-lotes ni recuperacion de eventos pendientes. No se reportan mediciones
hasta ejecutar los experimentos reales.
