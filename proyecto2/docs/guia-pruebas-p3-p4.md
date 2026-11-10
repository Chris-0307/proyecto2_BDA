# Guía de pruebas P3 y P4 — integrante 2 (commit 9)

Este commit **añade scripts y pruebas**, no resultados medidos. Los archivos de
`proyecto2/evidence/` deben generarse al ejecutarlos, nunca inventarse.

## Preparación (WSL o Linux)

Desde la raíz del repositorio:

```bash
python3 -m venv .venv
source .venv/bin/activate
python -m pip install -r proyecto2/servicios/sesion/requirements-dev.txt \
  -r proyecto2/servicios/historial/requirements-dev.txt
```

Iniciar una instancia **de pruebas** de Valkey, con AOF `appendonly yes`,
`appendfsync everysec`, con volumen persistente. No usar el Valkey de datos
reales para esta suite. No ejecutar `FLUSHDB`, `FLUSHALL` ni `docker compose down -v`.
En producción, solo el gateway debe exponer un puerto al host. Para los tests
locales se puede usar un entorno aislado, un túnel local o ejecutar Python
junto a los servicios en la red Docker.

Definir la URL de la **base de pruebas**, por ejemplo si es accesible desde WSL:

```bash
export VALKEY_TEST_URL='redis://localhost:6379/15'
```

Este comando es solo un **ejemplo**: el puerto y host reales dependen de la
configuración del equipo. El servicio de producción no debe abrir ese puerto
al host únicamente para realizar las pruebas.

## P3: sesión, borrador y vencimiento

```bash
python -m pytest proyecto2/pruebas/test_p3_p4_integracion.py -v -ra
```

Los casos P3 prueban por la API interna: escritura y recuperación de un borrador
menor o igual a 5 segundos de antigüedad, rechazo de token ajeno, y un lock
vencido que permite leer el borrador pero **no** reanudar la edición.

### P3: prueba real de reinicio AOF

Con la configuración `docker-compose.yml` integrada y el servicio llamado
`valkey`:

```bash
python proyecto2/pruebas/p3_reinicio_aof.py \
  --restart-command 'docker compose -f proyecto2/docker-compose.yml restart valkey'
```

El script comprueba que Valkey realmente use `appendonly=yes` y
`appendfsync=everysec`; crea un borrador, espera 2.5 segundos, ejecuta el
reinicio del servicio (conservando el volumen), espera la reconexión y
verifica que el contenido recuperado sea idéntico. Guarda resultado, comandos,
fechas, configuración y tiempos observados en `proyecto2/evidence/`.

No confundir "borrador recuperado" con "tarea reasignada en MongoDB": esta
última exige las pruebas con gateway/Registro. Para demostrar que una tarea
vuelve a la cola tras caducar, integrar y registrar esa transición mediante el
gateway y probar que ningún segundo usuario logra adquirirla simultáneamente.

## P4: historial real y reprocesamiento

La suite `test_p3_p4_integracion.py` también tiene dos casos P4:

- Petición HTTP a la API interna de Historial con código 202, Stream real,
  materialización Parquet mediante PyArrow y `XACK` posterior.
- Fallo simulado después de publicar el archivo y antes de `XACK`, seguido de
  `XAUTOCLAIM`: el mensaje se recupera, se confirma y el mismo archivo no se
  duplica. El consumidor de producción sigue teniendo semántica *al menos una vez*.

Estos casos usan grupos y eventos de prueba aislados por UUID y borran solo
sus propias entradas. No prueban la frescura del gateway ni la lectura Spark.

Para registrar el resultado de TODA la suite, con pruebas originales de los
commits anteriores:

```bash
python proyecto2/pruebas/ejecutar_evidencias_p3_p4.py
```

El script conserva el log de Pytest y un resumen JSON con fecha, plataforma,
comando, código de salida y limitaciones. Si no se configura `VALKEY_TEST_URL`,
los tests reales aparecen `SKIPPED`: **no se deben reportar como aprobados**.

## Evidencias adicionales que requiere el enunciado

| Requisito | Responsable de integración | Estado tras commit 9 |
| --- | --- | --- |
| Borrador con antigüedad máxima 5 s | Sesión | Test automatizado añadido; ejecución pendiente |
| Persistencia AOF después de reinicio | Sesión + Docker Compose | Script de reinicio añadido; ejecución pendiente |
| Lock vencido devuelve tarea a cola en MongoDB | Sesión + Gateway + Registro | **Pendiente** de prueba extremo a extremo |
| Sin doble dueño | Sesión + Registro | Tests locales disponibles; prueba completa pendiente |
| Micro-lote válido / sin lectura parcial | Historial | Tests PyArrow disponibles; Spark pendiente |
| Reprocesamiento / deduplicación `event_id` | Historial + Spark | Test local disponible; `dropDuplicates` Spark pendiente |
| Frescura HTTP 200 de /enviar -> evento visible en Spark | Gateway + Historial + Spark | **Pendiente** de medir integración real |
| Latencias p50, p95 y p99, n muestras y carga de máquina | Equipo | **Pendiente** de mediciones reales |

**Protocolo sugerido para frescura P4:** desde el simulador llamar al gateway
`POST /tareas/{id}/enviar`, registrar instante de su HTTP 200, y consultar
repetidamente Q1 o una lectura autorizada del datalake en Spark hasta
reconocer el `event_id` correspondiente. Registrar intervalo observado,
configuración, número de muestras y hora en UTC. No medir únicamente la
respuesta interna `POST /eventos` y presentarla como frescura completa.

Para p50/p95/p99 usar muestras reales ordenadas; indicar la convención de
percentiles, `n`, y características CPU/RAM. No inferir rendimiento distribuido
fuera del entorno local. Los datos, mediciones y conclusiones del informe deben
estar vinculados a los archivos generados en `evidence/`.

### Plantilla para documentar cada ejecución manual

```text
Fecha/hora UTC:
Objetivo:
Equipo: CPU, RAM, SO, Docker, Valkey, PyArrow, Spark:
Comando exacto:
Configuración (servicios, réplicas, volumen, dataset):
Resultado observado:
HTTP / estados:
Cantidad de muestras:
Latencias p50/p95/p99 (si aplica):
Archivo(s) evidence/:
Interpretación y limitaciones:
Restauración del entorno:
```

**Límites de este avance:** la prueba reinicio requiere Docker Compose y
Valkey disponibles, y las medidas completas P3/P4 dependen del gateway,
Registro y Spark. Los scripts no sustituyen la ejecución real ni su evidencia.
