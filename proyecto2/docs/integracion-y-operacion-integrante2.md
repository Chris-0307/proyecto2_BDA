# Integracion y operacion — Sesion e Historial (integrante 2)

> Este documento acompana el commit 10. **No constituye evidencia de pruebas
> ejecutadas**. Describe como levantar un entorno aislado en WSL/Linux y como
> integrar los servicios con el repositorio del equipo.

## 1. Componentes implementados

| Componente | Carpeta / comando | Dependencia | Expone al host |
|---|---|---|---|
| Valkey | Imagen `valkey/valkey:8.1.11-alpine` | Volumen AOF | No |
| Sesion | `servicios/sesion/` | Valkey (`redis://valkey:6379/0`) | No |
| Historial API | `servicios/historial/` | Valkey Streams | No |
| Historial worker | `python -m app.worker` | Valkey + directorio Parquet | No |

El simulador final solo puede acceder al **gateway** en el puerto 8000.
El Compose `compose.integrante2.yml` es **autonomo y exclusivo para pruebas de
nuestros servicios**, no reemplaza al `docker-compose.yml` del equipo.
Los endpoints internos se prueban directamente solo en escenarios de
mantenimiento e integracion.

## 2. Levantar el entorno aislado (WSL/Linux)

Requisitos: Docker Engine/ Docker Desktop con integracion WSL, `docker compose`
y Python 3 instalado en el host. Desde la **raiz del repositorio**:

```bash
docker compose -f proyecto2/compose.integrante2.yml -p bda-integrante2 config -q
docker compose -f proyecto2/compose.integrante2.yml -p bda-integrante2 up -d --build
docker compose -f proyecto2/compose.integrante2.yml -p bda-integrante2 ps
```

Verificar la persistencia real (NO basta con que exista `valkey.conf`):

```bash
docker compose -f proyecto2/compose.integrante2.yml -p bda-integrante2 \
  exec valkey valkey-cli CONFIG GET appendonly
docker compose -f proyecto2/compose.integrante2.yml -p bda-integrante2 \
  exec valkey valkey-cli CONFIG GET appendfsync
```

Resultados esperados de configuracion: `yes` y `everysec`. El servicio utiliza
un volumen nombrado `valkey_datos`; los archivos Parquet del **entorno aislado** van a
`proyecto2/.sandbox_integrante2/datalake/eventos/mes=AAAA-MM/` en el host.
El Compose definitivo debe montar `proyecto2/datalake/` como fuente de verdad
del historial para Spark; el sandbox no toca esa carpeta.
Valkey, Sesion e Historial **no tienen** `ports:` publicados.

Para revisar los logs:

```bash
docker compose -f proyecto2/compose.integrante2.yml -p bda-integrante2 logs --tail=100 sesion historial historial-worker
```

### Prueba de humo automatizada

```bash
bash proyecto2/pruebas/smoke_integrante2.sh
```

El script levanta el Compose de prueba, crea una sesion con UUID unicos,
guarda un borrador, reinicia **solo** Valkey, comprueba la recuperacion del
borrador, encola un evento y espera su aparicion en Parquet. Produce un
archivo `.txt` en `proyecto2/evidence/` con la salida observada y tiempos de
inicio/fin de la ejecucion. No altera los datos del proyecto principal.

**Alcance del smoke:** utiliza APIs internas de forma intencional, no usa
simulador ni gateway; por lo tanto **no cumple por si solo** las pruebas
extremo a extremo P3/P4 del enunciado. No calcula percentiles ni garantiza
RTO/RPO. No simula dos consumidores trabajando simultaneamente.

El script es para una instalacion de pruebas; por seguridad no ejecutarlo
contra una base de Valkey ni un directorio de datos reales. Los contenedores
y volumen Valkey usan el proyecto Compose `bda-integrante2`; los Parquet de
prueba van a `.sandbox_integrante2` (carpeta ignorada por Git).

### Suite completa de pytest dentro de Docker, sin publicar Valkey al host

Desde la raiz del repositorio:

```bash
docker compose -f proyecto2/compose.integrante2.yml -p bda-integrante2 \
  run --rm \
  -v "$(pwd)/proyecto2:/workspace:ro" -w /workspace \
  -e VALKEY_TEST_URL=redis://valkey:6379/15 \
  -e PYTHONDONTWRITEBYTECODE=1 historial \
  sh -c 'pip install --no-cache-dir -r servicios/sesion/requirements-dev.txt -r servicios/historial/requirements-dev.txt && python -m pytest -p no:cacheprovider pruebas/ -v -ra'
```

Esta orden usa la base **15 solo para pruebas** en el Valkey de desarrollo.
No utiliza `FLUSHDB`; aun asi debe ejecutarse en un entorno aislado. Las
pruebas `SKIPPED` deben anotarse como **no ejecutadas**, no aprobadas.

### Detencion

```bash
docker compose -f proyecto2/compose.integrante2.yml -p bda-integrante2 down
```

`down` conserva los volúmenes. **No ejecutar `down -v`** salvo que se quiera
eliminar intencionalmente los datos de esta instalacion de prueba.

## 3. Contrato REST interno de Sesion

| Metodo y ruta | Efecto | Resultados principales |
|---|---|---|
| GET `/salud` | Comprueba Valkey | 200 o 503 |
| POST `/sesiones/iniciar` | Lock + hash sesion + ZSET `editando` | 201, 409, 503 |
| POST `/sesiones/latido` | Renueva lock si coincide token | 200, 409, 503 |
| POST `/sesiones/finalizar` | Libera lock y sesion propios | 200, 409, 503 |
| PUT `/tareas/{tarea_id}/borrador` | Guarda ultimo borrador y renueva lock | 200, 409, 422, 503 |
| POST `/sesiones/recuperar` | Recupera borrador de sesion propia | 200, 409, 503 |

Todas son **rutas internas**. Las peticiones ordinarias incluyen:
`anotador_id`, `tarea_id`, `proyecto_id`, `token` (cadena). `iniciar`
acepta `etapa`; `borrador` agrega un objeto JSON en `contenido`.

Requisitos de coordinacion con el gateway/Registro:

1. Solicitud de siguiente tarea: conseguir lock temporal y ejecutar
   `findOneAndUpdate` atomico en MongoDB para asignacion definitiva. Si Mongo
   falla, liberar el lock. Evitar doble asignacion en las 50 peticiones
   concurrentes con 10 tareas disponibles.
2. Borradores: enviar PUT aproximadamente cada 2 s mientras se edita;
   consultar estado definitivo desde Registro en el gateway. Las escrituras
   de borrador no producen eventos.
3. Envio definitivo: validar token y estado `abierta`. Tras confirmacion
   MongoDB, responder segun el contrato del gateway. Segundo envio: 409;
   MongoDB no disponible: 503 sin reintento automatico.
4. Recuperacion: si respuesta es `requiere_reasignacion`, mostrar borrador
   solo en lectura hasta comprobar con Registro que puede reasignarse;
   **no reutilizar un lock vencido**.
5. Liberacion: el token impide borrar locks de otro usuario. Para una tarea
   rechazada, Registro la vuelve a `disponible`; Sesion no modifica MongoDB.

## 4. Contrato REST interno de Historial

| Metodo y ruta | Efecto | Resultados |
|---|---|---|
| GET `/salud` | Comprueba Valkey | 200 o 503 |
| POST `/eventos` | Agrega a Valkey Stream de forma idempotente | 202 nuevo, 200 repetido, 409 conflicto, 422 invalido, 503 falla |

Los eventos se definen en `servicios/historial/app/eventos.py` y llevan
`event_id` (UUID), tipo de evento, estado, `ocurrido_en` con zona horaria,
identificadores de tarea/proyecto/cliente, modalidad, tipo de tarea y version.
Campos como equipo, revisor, duracion y SLA se rellenan solo si los conoce
Registro. La transicion de rechazo conserva `estado="rechazada"` en Parquet,
aunque MongoDB marque la tarea `disponible`.

La cola usa las llaves `historial:eventos` y `historial:ids`. El worker
lee por grupo `historial-parquet`, publica Parquet en `.staging` y luego
lo traslada atomica y localmente a `mes=AAAA-MM/`. **XACK sucede despues**
de publicar. `XAUTOCLAIM` recupera pendientes al superar el umbral de
inactividad. No se garantiza una transaccion atomica entre MongoDB y Valkey.

### Integracion necesaria para no perder eventos

El gateway **no debe interpretar** una respuesta 202 de Historial como que
Spark ya leyó Parquet. Hay una ventana entre confirmar MongoDB y publicar
a Valkey; para cubrirla, el integrante responsable de Registro y el gateway
deben acordar un **outbox transaccional o reconciliacion durable**. El
servicio actual por si solo no elimina esa ventana.

## 5. Contrato de lectura de Spark

- Leer **solo** `proyecto2/datalake/eventos/` montado como solo lectura.
- Seleccionar exclusivamente archivos `*.parquet` publicados (nunca `.staging`).
- Deduplicar `event_id` entre micro-lotes distintos **antes** de Q1–Q5.
- Mantener `ocurrido_en` como timestamp UTC; la particion `mes=AAAA-MM`
  tambien usa UTC.
- Para Q4, contar filas historicas `rechazada` del datalake. No consultar
  estado actual de MongoDB para inferir rechazos.
- Los archivos Parquet comparten un esquema fisico declarado en
  `servicios/historial/app/parquet_writer.py`. Cambios en campos deben
  coordinarse entre Registro, Historial y Analitica.

Ejemplo **orientativo** para el integrante de analitica:

```python
# En el componente Spark, no dentro del servicio Historial:
eventos = spark.read.parquet('/datalake/eventos')
eventos_unicos = eventos.dropDuplicates(['event_id'])
```

## 6. Pendientes antes de la entrega final conjunta

- [ ] Integrar los servicios en `proyecto2/docker-compose.yml` del equipo.
- [ ] Confirmar contratos JSON con Gateway, Registro y Analitica.
- [ ] Acordar la solucion durable para el intervalo MongoDB -> Valkey.
- [ ] Registrar salud de cada capa mediante los mensajes en Valkey requeridos
      por el gateway; el `GET /salud` actual es una sonda local.
- [ ] Ejecutar suite real, reinicio AOF, pruebas P3/P4, y guardar evidencia.
- [ ] Verificar que el simulador solo hable con el gateway (puerto 8000).
- [ ] Medir visibilidad HTTP 200 gateway -> Spark, p50/p95/p99 y RTO/RPO
      cuando exista el sistema integrado; anotar limitaciones reales.
- [ ] No reportar cifras ni aprobaciones de pruebas no ejecutadas.
