# Servicio de Historial — commits 6, 7 y 8

Este servicio mantiene un historial de eventos de negocio, separado de MongoDB,
para su posterior consulta mediante Spark. Desde el **commit 7** escribe
archivos Parquet y desde el **commit 8** recupera pendientes; el consumidor es un proceso distinto de la API REST.

## Componentes

- **API (`app.main`)**: `GET /salud` y `POST /eventos` en el puerto interno 8002.
  Recibe transiciones confirmadas y agrega eventos a `historial:eventos` mediante
  Valkey Streams. Conserva la idempotencia por `event_id` del commit 6.
- **Worker (`python -m app.worker`)**: lee hasta 100 eventos por iteración del
  grupo `historial-parquet`, escribe los micro-lotes y solo después ejecuta `XACK`.
- **Escritor (`app.parquet_writer`)**: usa PyArrow con un esquema explícito y
  publica cada archivo por una operación `os.replace()` atómica.

## Organización del datalake

La variable `DATALAKE_DIR` apunta a la carpeta `eventos` (predeterminado:
`/datalake/eventos`). Se recomienda montar `proyecto2/datalake/` como `/datalake/`
para el worker y Spark.

```text
proyecto2/datalake/
└── eventos/
    ├── .staging/                  # rutas temporales, ocultas para Spark
    ├── mes=2026-10/
    │   └── lote-<hash-ids>.parquet
    └── mes=2026-11/
        └── lote-<hash-ids>.parquet
```

La partición se calcula con la fecha `ocurrido_en` normalizada a **UTC**.
Si un micro-lote contiene fechas de diferentes meses, se publica un archivo
independiente por cada mes.

Cada archivo mantiene las columnas `event_id`, `tipo_evento`, `estado`,
`ocurrido_en`, `tarea_id`, `proyecto_id`, `cliente_id`, `batch_id`,
`anotador_id`, `revisor_id`, `equipo_id`, `modalidad`, `tipo_tarea`, `version`,
`tomada_en`, `aprobada_en`, `duracion_ms`, `sla_horas`, `motivo_rechazo` y
`datos_extra_json`. Los campos de fecha son timestamps en UTC y los
identificadores UUID se representan como cadenas. `datos_extra_json` es texto
JSON, para mantener un tipo fijo entre micro-lotes.

Los eventos de rechazo mantienen `estado="rechazada"` en Parquet. Los
borradores nunca se registran aquí.

## Publicación segura

1. El worker lee un lote de Valkey Streams mediante `XREADGROUP`.
2. PyArrow escribe el archivo temporal bajo `eventos/.staging/`.
3. Se sincroniza el contenido del archivo y se mueve con `os.replace()` a
   `eventos/mes=AAAA-MM/`. Ambos directorios deben estar en **el mismo
   volumen de archivos** para asegurar la atomicidad.
4. Después de terminar todas las particiones del lote, el consumidor confirma
   los mensajes mediante `XACK`.

El nombre de cada archivo se deriva de los `event_id` del micro-lote mensual:
un reintento idéntico no genera otro archivo. Si fallan las escrituras,
**no se realiza XACK**. Spark debe deduplicar por `event_id`, como exige el
proyecto, pues combinaciones distintas de eventos aún pueden repetir filas.

**Límite actual:** la publicación es atómica por archivo, no existe una
transacción distribuida entre distintas particiones. El worker consume
mensajes nuevos y puede recuperar mensajes pendientes con `XAUTOCLAIM`.
Tampoco se resuelve todavía el intervalo entre confirmación de MongoDB y
publicación al Stream; requiere coordinación con Registro y Gateway
(outbox/reconciliación).

## Integración con Docker Compose

El integrante que mantiene `docker-compose.yml` debe crear dos servicios
**internos** usando el mismo directorio de construcción:

```yaml
# Fragmento orientativo, NO es un compose completo.
historial:
  build: ./servicios/historial
  environment:
    VALKEY_URL: redis://valkey:6379/0
  # No agregar ports:; solo el gateway se expone al host.

historial-worker:
  build: ./servicios/historial
  command: ["python", "-m", "app.worker"]
  environment:
    VALKEY_URL: redis://valkey:6379/0
    DATALAKE_DIR: /datalake/eventos
    HISTORIAL_TAMANO_LOTE: "100"
    HISTORIAL_RECUPERACION_IDLE_MS: "30000"
  volumes:
    - ./datalake:/datalake
  # No publicar puertos del worker.
```

Valkey debe estar configurado con AOF `appendfsync everysec` y un volumen
persistente. Spark debe montar el mismo datalake de forma **solo lectura**.
Estos son fragmentos de integración: los nombres y redes deben acordarse
con los compañeros.

## Pruebas posteriores en WSL

```bash
cd proyecto2/servicios/historial
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements-dev.txt
cd ../..
python -m pytest pruebas/p4_historial/test_historial_parquet.py pruebas/p4_historial/test_historial_reprocesamiento.py -v
```

Las pruebas del escritor usan directorios temporales, sin servidor Valkey.
Para probar el worker de extremo a extremo habrá que levantar Valkey con AOF,
conectar el gateway y ejecutar P4. Las mediciones de frescura, fallas y
rendimiento siguen pendientes y deben conservarse en `proyecto2/evidence/`.

## Recuperación de pendientes y deduplicación — commit 8

- El worker ejecuta `XAUTOCLAIM` en el grupo `historial-parquet` antes de
  leer mensajes nuevos. Reclama mensajes que llevan sin actividad al menos
  `HISTORIAL_RECUPERACION_IDLE_MS` (predeterminado: **30 000 ms**).
- Un worker reiniciado con otro nombre de consumidor puede recuperar
  mensajes entregados a consumidores anteriores. Hasta que cumplan el
  tiempo de inactividad, el worker sigue procesando entradas nuevas.
- `XACK` ocurre **después** de que todos los archivos Parquet del lote
  han sido publicados. Si la escritura, la validación o la confirmación
  falla, el mensaje sigue pendiente y se reintentará.
- Repetir exactamente el mismo micro-lote por un fallo entre Parquet y
  `XACK` no crea otro archivo: el nombre depende de sus `event_id`.
- Dentro de cada micro-lote, el escritor elimina repetidos con idéntico
  `event_id` y contenido. Si el mismo ID lleva un contenido diferente,
  falla sin publicar ese lote.
- **Entre archivos diferentes** pueden quedar filas con el mismo
  `event_id` si dos micro-lotes se solapan: el consumo es **al menos una
  vez**, no exactamente una vez. La consulta Spark debe deduplicar usando
  `dropDuplicates(["event_id"])` antes de calcular Q1–Q5.
- `XAUTOCLAIM` es compatible con Valkey y Redis que admiten Streams y
  el comando (Redis 6.2+). No ejecutar varios workers lentos con un
  umbral de inactividad menor que su tiempo normal de escritura; podrían
  reclamar trabajo aún activo. Para las pruebas del curso se recomienda
  un worker único y verificar tiempos antes de escalar.
- Los mensajes inválidos permanecen sin `XACK` y requieren inspección
  manual: el worker registra la excepción, pero no elimina eventos.


### Pruebas y evidencias P4

La prueba P4 integrada se ejecuta desde la raíz del repositorio
con el Compose final levantado:

```bash
make p4
```

En Windows también puede ejecutarse directamente:

```powershell
python proyecto2/pruebas/p4_historial/p4_gateway_parquet.py
```

La ejecución del 24 de noviembre de 2026 obtuvo 27/27
comprobaciones correctas.

Resultados principales:

- Recuperación de un mensaje pendiente mediante XAUTOCLAIM.
- Confirmación mediante XACK después de publicar Parquet.
- Reprocesamiento sin generar archivos adicionales.
- Lectura correcta de Spark durante una escritura temporal.
- Tres filas físicas reducidas a dos eventos únicos.
- Ocho eventos de negocio materializados en el flujo de prueba.
- Frescura de los dos envíos: 2,177 s y 2,366 s.
- Conservación del estado histórico rechazada cuando MongoDB
  devuelve la tarea a disponible.

La evidencia completa se encuentra en:

proyecto2/evidence/p4_gateway_parquet.txt

La falla antes de XACK fue simulada. La frescura se midió
mediante la primera observación del event_id en Spark,
no mediante Q1 ni bajo condiciones de carga.



## Estrategia de compactación de micro-lotes

El worker genera archivos Parquet a partir de micro-lotes de hasta
100 eventos. Con el uso continuo del sistema pueden acumularse
muchos archivos pequeños, aumentando el costo de apertura,
lectura y planificación de consultas en Spark.

### Política de compactación

La compactación se plantea como una operación manual de
mantenimiento, preferiblemente sobre particiones mensuales
cerradas o con una cantidad elevada de archivos pequeños.

Como referencia inicial, se propone evaluar la compactación
cuando una partición acumule más de 100 archivos y generar
archivos de aproximadamente 128 MiB cuando el volumen de
datos lo permita.

Estos valores son parámetros propuestos, no resultados de
una optimización experimental.

### Procedimiento propuesto

1. Suspender la entrada de nuevas operaciones al gateway y
   comprobar que no existan eventos pendientes de publicación
   desde Registro.

2. Esperar a que el worker procese los eventos restantes de
   Valkey Streams y verificar los mensajes pendientes.

3. Detener el worker de Historial y el servicio de Analítica,
   asegurando que ningún proceso lea o escriba la partición
   durante la compactación.

4. Leer los archivos Parquet publicados de la partición
   mensual seleccionada.

5. Verificar que los eventos con el mismo event_id tengan
   contenido equivalente. Si existen diferencias, abortar
   la compactación para investigar la inconsistencia.

6. Deduplicar las filas por event_id, conservando las
   diferentes versiones y transiciones de cada tarea.

7. Escribir los archivos compactados en un directorio
   temporal fuera del patrón de lectura de Spark.

8. Validar el esquema, el contenido, los identificadores
   únicos, la partición mensual y la cantidad de eventos
   antes y después de la compactación.

9. Respaldar los archivos originales fuera del directorio
   consultado por Spark y publicar los archivos compactados
   dentro de la partición correspondiente.

10. Si la validación falla, restaurar los archivos originales.
    Si es correcta, reanudar el worker, Analítica y el gateway.

### Consistencia y deduplicación

La compactación no modifica MongoDB, Valkey Streams ni los
estados históricos de las tareas. Tampoco genera eventos de
negocio adicionales.

Los eventos de rechazo conservan el estado rechazada y su
motivo original. Las diferentes transiciones de una tarea
se conservan porque poseen identificadores event_id distintos.

Spark debe continuar aplicando dropDuplicates(["event_id"])
antes de ejecutar las consultas Q1-Q5, incluso después de
una compactación. Esto permite manejar posibles eventos
reprocesados o incorporados posteriormente.

El intercambio de varios archivos no constituye una
transacción atómica de toda la partición. Por esta razón,
la estrategia requiere una ventana de mantenimiento sin
lectores ni escritores activos.

### Estado de implementación

La compactación automática no está implementada en la
versión actual. Se documenta como estrategia de mantenimiento
y mejora futura del datalake.

Las pruebas P4 validaron la publicación atómica por archivo,
el reprocesamiento y la deduplicación mediante Spark, pero
no evaluaron el rendimiento antes y después de compactar.

Evidencia relacionada:

- proyecto2/evidence/p4_gateway_parquet.txt
