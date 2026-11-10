# Informe tecnico base — integrante 2

**Area:** servicio Sesion (Valkey) e Historial (Parquet).  
**Estado:** implementacion inicial en rama individual; integracion y
validacion experimental final pendientes.

Este documento es un **borrador tecnico para el informe grupal**. No incluye
resultados ni datos medidos. Completarlo una vez que los servicios se ejecuten
junto al gateway, Registro, Analitica y el simulador.

## 1. Distribucion de responsabilidades

| Capa | Modulos y funcionalidad | Ubicacion |
|---|---|---|
| Sesion | Candado `SET NX PX` 120 s, tokens, sesiones activas, latidos, borradores, recuperacion | `servicios/sesion/app/` |
| Valkey | AOF `appendonly yes`, `appendfsync everysec` | `servicios/sesion/valkey.conf` |
| Historial | Validacion de eventos, Stream de mensajes, idempotencia por `event_id` | `servicios/historial/app/eventos.py` |
| Consumidor | Lectura por grupo, `XAUTOCLAIM`, `XACK` tras persistencia | `servicios/historial/app/consumer.py`, `worker.py` |
| Datalake | Esquema PyArrow, particion UTC mensual, `.staging` y publicacion con `os.replace` | `servicios/historial/app/parquet_writer.py` |
| Pruebas | Pruebas unitarias e integracion P3/P4, script de reinicio AOF | `pruebas/` |

## 2. Decisiones tecnicas y consistencia

### Sesion

La fuente de verdad de la tarea es MongoDB (Registro). Valkey solo conserva
las sesiones temporales y los borradores, sin modificar el estado definitivo.
Se usan scripts Lua para adquirir, renovar, liberar y validar candados sin
intercalar operaciones concurrentes. El token protege contra liberaciones
ajenas. Los borradores no generan eventos y no tienen un TTL automatico,
para permitir recuperacion.

Si el lock expiró, la API de recuperacion devuelve el borrador en modo de
solo lectura y marca `requiere_reasignacion`; la decision de reasignar
se toma posteriormente con Registro a traves del gateway.

### Historial

El gateway/Registro debe crear eventos despues de las transiciones
confirmadas en MongoDB. Se encolan en Valkey Streams, y un worker
independiente los escribe en Parquet. El archivo se genera primero en
`.staging`, se sincroniza y se publica mediante renombrado atomico
**por archivo** dentro del mismo volumen. Solo despues se confirma el
mensaje (XACK).

Si un worker muere antes del XACK, la entrada queda pendiente y otro
puede reclamarla mediante `XAUTOCLAIM`. Repetir el mismo micro-lote produce
el mismo nombre de archivo; eso reduce duplicados de reintentos identicos.
Micro-lotes solapados **pueden** contener el mismo event_id en archivos
distintos, por lo que Spark debe aplicar deduplicacion global.

### Aislamiento

La Sesion no consulta MongoDB. Historial no consulta Registro. Analitica
lee solo Parquet. El Compose de prueba no publica puertos al host; en el
sistema conjunto solo el gateway publica 8000.

## 3. Pruebas que deben respaldar el informe

| Prueba | Criterio esperado | Evidencia a adjuntar | Estado a completar |
|---|---|---|---|
| P3. Locks | Una tarea no tiene dos propietarios; token obligatorio | Log de integracion + identificadores | No medido aun |
| P3. Recuperacion | Se conserva el ultimo borrador y se impide escribir sin lock | JSON antes/despues, timestamps | No medido aun |
| P3. AOF | Reiniciar Valkey conserva borrador | Salida `CONFIG GET`, comando reinicio, lectura | No medido aun |
| P3. Vencimiento | Tarea vuelve a cola por decision de Registro | Respuestas gateway + estado MongoDB | Requiere integracion |
| P4. Publicacion | Spark no ve archivo a medias | Listado de `.staging`, Parquet final, log worker | Requiere integracion |
| P4. Reproceso | Se puede reclamar evento pendiente y deduplicar por `event_id` | XPENDING antes/despues, Parquet, Spark | Requiere integracion |
| P4. Frescura | Segundos entre HTTP 200 del gateway y consulta Spark | Timestamps comprobables | Requiere integracion |

**No marcar ninguna fila como aprobada sin ejecutar el ensayo y guardar
la evidencia original.** Los tests `SKIPPED` indican ausencia de ensayo.
Documentar fecha, objetivo, comandos, configuracion, resultado e
interpretacion en `evidence/`. Todas las cifras deben apuntar a archivos
identificables. El entorno de prueba aislado no sustituye P1/P2/P5/P6/P8.

## 4. Plantilla para incorporar un resultado medido

- **Prueba:** P3 / P4 (subcaso).
- **Fecha y hora (UTC):** pendiente.
- **Hardware y software:** CPU, RAM, disco, sistema, versiones, workers.
- **Objetivo e hipotesis:** pendiente.
- **Comando exacto:** pendiente.
- **Muestras/volumen:** pendiente.
- **Resultado observado:** pendiente.
- **Latencia p50 / p95 / p99:** no disponible hasta medir.
- **RTO / RPO:** no disponible hasta medir y definir puntos de fallo/recuperacion.
- **Interpretacion y limitaciones:** pendiente.
- **Archivo(s) de evidencia:** pendiente.

## 5. Riesgos y limites conocidos

1. La persistencia AOF `everysec` puede tener una ventana de perdida de
   aproximadamente un segundo ante una caida abrupta. Un reinicio ordenado
   no equivale al ensayo de corte abrupto.
2. No hay transaccion unica entre MongoDB y Valkey. Requiere outbox o
   reconciliacion durable coordinada con Registro y Gateway.
3. La publicacion con `os.replace` es atomica por archivo, no entre
   multiples particiones mensuales.
4. El consumo de Streams es *al menos una vez*. Spark deduplica por event_id.
5. El umbral de `XAUTOCLAIM` debe superar el tiempo normal de proceso para
   evitar reclamar trabajo activo en una configuracion con varios workers.
6. El endpoint local de salud no sustituye la agregacion por capa en Gateway
   ni el sistema de notificaciones de salud usando Valkey.
7. Los datos de prueba del proyecto y Q1–Q5 dependen del integrante de
   Analitica y de contratos de eventos acordados con Registro.

## 6. Repositorio y trazabilidad

La rama `Valkey-e-historial-Parquet` conserva commits separados para:
contratos, configuracion, locks, borradores, recuperacion, ingreso de eventos,
publicacion Parquet, reprocesamiento, pruebas y documentacion/integracion.
La defensa exige conocer tambien la asignacion atomica en MongoDB, el
comportamiento del gateway y la deduplicacion en Spark.

## 7. Pasos de cierre

1. Revisar `docs/integracion-y-operacion-integrante2.md`.
2. Ejecutar `bash proyecto2/pruebas/smoke_integrante2.sh` sobre el entorno aislado.
3. Ejecutar pytest real, prueba AOF y completar evidencia P3/P4.
4. Integrar via pull request con los otros integrantes, revisar sus contratos.
5. Hacer pruebas completas a traves del gateway y Spark.
6. Trasladar conclusiones y referencias a evidencia al PDF del grupo.
