# Servicio de Historial — commits 6 y 7

Este servicio mantiene un historial de eventos de negocio, separado de MongoDB,
para su posterior consulta mediante Spark. Con el **commit 7** ya puede escribir
archivos Parquet; el consumidor es un proceso distinto de la API REST.

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
mensajes nuevos; la recuperación de mensajes pendientes del grupo tras
caídas se desarrollará en el **commit 8**. Tampoco se resuelve todavía el
posible intervalo entre confirmación de MongoDB y publicación al Stream;
requiere coordinación con el equipo (outbox/reconciliación).

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
python -m pytest pruebas/test_historial_parquet.py -v
```

Las pruebas del escritor usan directorios temporales, sin servidor Valkey.
Para probar el worker de extremo a extremo habrá que levantar Valkey con AOF,
conectar el gateway y ejecutar P4. Las mediciones de frescura, fallas y
rendimiento siguen pendientes y deben conservarse en `proyecto2/evidence/`.
