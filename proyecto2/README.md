# Proyecto 2 — Plataforma distribuida de anotación de datos

Capa de datos de una plataforma de anotación basada en servicios REST y
contenedores: registro operacional (MongoDB en réplica), sesiones y locks
(Valkey con AOF), historial en Parquet y analítica con Apache Spark.

Este README explica cómo ejecutar el sistema. La arquitectura, el diseño, los
resultados de las pruebas y las limitaciones están en el informe:
[`docs/informe.md`](docs/informe.md). Las rutas de cada servicio están en
[`endpoints/README.md`](endpoints/README.md).

## 1. Componentes

| Componente | Tecnología | Puerto | Documentación | Estado |
|---|---|---|---|---|
| Gateway | FastAPI + httpx | 8000, único publicado al host | [`servicios/gateway/`](servicios/gateway/README.md) | Integrado; falta `/analitica` |
| Registro | FastAPI + MongoDB (3 nodos en réplica) | 8003, interno | [`servicios/registro/`](servicios/registro/README.md) | Integrado |
| Sesión | FastAPI + Valkey (AOF `everysec`) | 8001, interno | [`servicios/sesion/`](servicios/sesion/README.md) | Integrado |
| Historial | FastAPI + Valkey Streams + worker Parquet | 8002, interno | [`servicios/historial/`](servicios/historial/README.md) | Integrado |
| Analítica | FastAPI + PySpark (`local`, solo lectura sobre Parquet) | 8004, interno | `servicios/analitica/` | Integrado (`/analitica/{q}` en el gateway) |
| Simulador | Cliente HTTP (`simulador_http.py`) contra el gateway | — | `simulador/` | Implementado |

## 2. Estructura del repositorio

```text
proyecto2_BDA/
├── Makefile                          # atajos de ejecución (make help)
├── README.md                         # portada; remite a proyecto2/README.md
└── proyecto2/
    ├── README.md                     # este documento
    ├── .gitignore
    ├── docker-compose.yml            # Compose final: MongoDB, Valkey, Sesión, Historial, Registro, Gateway
    ├── compose.integrante2.yml       # entorno aislado de Sesión + Historial (solo pruebas)
    ├── servicios/
    │   ├── gateway/                  # API pública, puerto 8000
    │   │   ├── Dockerfile, requirements.txt, README.md
    │   │   └── gateway_app/          # main.py, flujo.py, modelos.py, clientes.py
    │   ├── registro/                 # API sobre MongoDB, puerto interno 8003
    │   │   ├── Dockerfile, requirements.txt, README.md
    │   │   ├── registro_app/         # main.py, db.py, modelos.py, transiciones.py
    │   │   └── mongo/                # inicializar.sh, init-replica.js, configurar-registro.js
    │   ├── sesion/                   # API de sesiones y locks, puerto interno 8001
    │   │   ├── Dockerfile, requirements*.txt, README.md, valkey.conf
    │   │   └── app/                  # main.py, locks.py, sesiones.py, borradores.py, recuperacion.py
    │   └── historial/                # API de eventos y worker Parquet, puerto interno 8002
    │       ├── Dockerfile, requirements*.txt, README.md
    │       └── app/                  # main.py, eventos.py, consumer.py, worker.py, parquet_writer.py
    ├── dataset/                      # cargar_demo.py (demo) y generar_dataset.py (dataset reproducible)
    ├── datalake/                     # Parquet publicado por el worker (no se versiona)
    ├── mantenimiento/                # compactar_parquet.py (make compactar)
    ├── pruebas/                      # una carpeta por grupo de prueba del enunciado
    │   ├── p1_gateway/               # smoke_gateway.py, smoke_flujo.py, p1_salud_aislamiento.py
    │   ├── p2_registro/              # smoke_registro.py, verificar_replica.sh, p2_concurrencia_gateway.py, write_concern.py, explain_registro.py
    │   ├── p3_sesion/                # test_sesion_*.py, p3_reinicio_aof.py
    │   ├── p4_historial/             # test_historial_*.py
    │   ├── p3_p4_integracion/        # test_p3_p4_integracion.py, smoke_integrante2.sh, ejecutar_evidencias_p3_p4.py
    │   └── p8_fallas/                # p8_mongo.py (fallas de MongoDB)
    ├── endpoints/                    # README de rutas, cuerpos y códigos; openapi/*.json (make endpoints)
    ├── evidence/                     # salidas de las pruebas (ver sección 9)
    └── docs/                         # informe.md (informe del proyecto), contratos y guías de Sesión e Historial
```

## 3. Requisitos previos

- Docker Engine o Docker Desktop con integración WSL, y `docker compose` v2.
- Bash (WSL o Linux).
- Unos 4 GB de RAM libres: cada nodo de MongoDB usa 300–500 MB.
- Python 3 en el host para las pruebas que detienen contenedores o prueban
  puertos (`make p1-salud`, `make p8-mongo`) y para `p3_reinicio_aof.py`.

## 4. Levantar y detener el sistema

Ejecute siempre desde la **raíz del repositorio**.

### Atajos con `make`

El `Makefile` de la raíz envuelve los comandos de esta guía. Se ejecuta
desde la raíz del repositorio; `make` sin argumentos lista los objetivos.
Requiere `make` (en WSL: `sudo apt install make`). Los comandos completos
siguen abajo.

**Sistema completo**

| Objetivo | Qué hace |
|---|---|
| `make up` | Construye y levanta el Compose final |
| `make ps` | Estado de los contenedores |
| `make logs s=registro` | Logs recientes de uno o más servicios |
| `make down` | Detiene el sistema (conserva volúmenes) |
| `make reset` | `down -v` + `up`; pide confirmación y **borra los datos** |

**Registro y MongoDB (P2)**

| Objetivo | Qué hace | Evidencia |
|---|---|---|
| `make replica` | Verifica réplica, mayoría e índices | `evidence/p2_replica.txt` |
| `make demo` | Carga de demostración (cliente, anotadores, proyecto, lote) | — |
| `make smoke-registro` | Transiciones, 409 y 50 asignaciones concurrentes | `evidence/p2_registro_smoke.txt` |
| `make registro` | Todo seguido: `up` + `replica` + `demo` + `smoke-registro` | las dos anteriores |
| `make p2-gateway` | 50 solicitudes simultáneas contra 10 tareas por el gateway; latencias p50/p95/p99 | `evidence/p2_concurrencia_gateway.txt` |
| `make write-concern` | Latencia y rendimiento de las escrituras del registro con `w:1` frente a `w:majority` | `evidence/p2_write_concern.txt` |
| `make explain-registro` | Planes `explain` de las consultas del registro (tras `make datos`) | `evidence/p2_explain_registro.txt` |

**Gateway (P1)**

| Objetivo | Qué hace | Evidencia |
|---|---|---|
| `make salud` | `curl -i http://localhost:8000/salud` | — |
| `make smoke-gateway` | Rutas básicas, códigos y salud por capa (requiere `make demo`) | `evidence/p1_gateway_smoke.txt` |
| `make endpoints` | Exporta el OpenAPI de cada servicio a `endpoints/openapi/` | — |
| `make smoke-flujo` | Flujo completo: siguiente, borrador, recuperar, enviar, liberar, revisión, exportar | `evidence/p1_gateway_flujo.txt` |
| `make p1-salud` | `/salud` con Sesión, Historial, Analítica, Registro y Valkey caídos; mensajes de salud en Valkey; puertos publicados; reglas de aislamiento. Corre en el host | `evidence/p1_salud_aislamiento.txt` |

**Fallas de MongoDB (P8)**

**Datos, analítica Spark, simulador y carga (P5/P6, Emmanuel)**

| Objetivo | Qué hace | Evidencia |
|---|---|---|
| `make datos` | Carga el dataset reproducible en MongoDB y genera Parquet mensuales con semilla fija | `datalake/eventos/` |
| `make simulador` | Ejecuta el simulador HTTP (`simulador_http.py`) contra el gateway | — |
| `make p5` | Ejecuta Q1–Q5 en Spark (1.ª vs. 2.ª corrida y planes `explain` de Q1/Q3) | `evidence/p5_analitica.txt` |
| `make p5-validar` | Valida Q1–Q5 con un micro-dataset de resultado conocido | `evidence/p5_validacion_consultas.txt` |
| `make p6` | Ejecuta pruebas de carga HTTP (10, 40 y 100 usuarios) contra el gateway | `evidence/p6_carga.txt` |

**Sesión e Historial (P3/P4, Christian)**

| Objetivo | Qué hace |
|---|---|
| `make aof` | `CONFIG GET appendonly` y `appendfsync` en Valkey |
| `make i2-up` / `make i2-down` | Levanta / detiene el entorno aislado de Sesión + Historial |
| `make i2-smoke` | `smoke_integrante2.sh` sobre el entorno aislado |
| `make i2-pytest` | Suite pytest de Sesión e Historial en Docker |
| `make p3-p4` | `ejecutar_evidencias_p3_p4.py` (suite con evidencia) |

**Flujo típico desde cero**

```bash
make up
make registro        # réplica + demo + smoke del registro
make smoke-gateway   # rutas básicas del gateway
make smoke-flujo     # flujo completo de tareas
make p2-gateway      # 50 contra 10
make p8-mongo        # fallas de MongoDB
```

```bash
docker compose -f proyecto2/docker-compose.yml up -d --build
docker compose -f proyecto2/docker-compose.yml ps
```

Estado esperado: los tres `mongo`, `valkey`, `sesion`, `historial`,
`historial-worker`, `registro` y `gateway` en `running` (solo `gateway` con `0.0.0.0:8000->8000`); `mongo-init` en `Exited (0)`.

`mongo-init` es un servicio de un solo uso e **idempotente**: inicializa el
conjunto `rs-plataforma` (si no existe), fija `w: majority` por defecto y crea
colecciones e índices. El servicio `registro` espera a que termine con éxito.

Verificar réplica, mayoría e índices (deja evidencia en `evidence/`):

```bash
bash proyecto2/pruebas/p2_registro/verificar_replica.sh
```

Verificar AOF de Valkey (esperado: `yes` y `everysec`):

```bash
docker compose -f proyecto2/docker-compose.yml exec valkey valkey-cli CONFIG GET appendonly
docker compose -f proyecto2/docker-compose.yml exec valkey valkey-cli CONFIG GET appendfsync
```

Logs y detención:

```bash
docker compose -f proyecto2/docker-compose.yml logs --tail=100 registro sesion historial historial-worker
docker compose -f proyecto2/docker-compose.yml down
```

> `down` conserva los volúmenes. **`down -v` borra los datos de MongoDB y los
> borradores de Valkey**. Úselo solo para probar un arranque desde cero.

### Corrida completa desde cero (todas las pruebas)

Regenera los datos y toda la evidencia de `evidence/` en la máquina actual.
Se ejecuta desde la raíz del repositorio y **en este orden**: cada paso deja
el sistema listo para el siguiente y sobrescribe su archivo de evidencia
(`compactacion_parquet.txt` acumula las corridas). Duración total: ~60–75 min
(P6 sola tarda ~20 min). No correr dos pasos en paralelo.

**0. Preparación.**

```bash
make i2-down     # apaga el Compose aislado de Sesión + Historial si quedó levantado
{ date -u; uname -srm; echo "CPU lógicos: $(nproc)"; free -h | head -2; \
  docker info --format 'Docker: {{.NCPU}} CPU, {{.MemTotal}} bytes, {{.ServerVersion}}'; } \
  | tee proyecto2/evidence/maquina.txt
```

> Si el repo está en `/mnt/c/...` (disco de Windows), Spark abre cada Parquet
> a través del puente Windows–Linux y todo es más lento. Clonarlo en el disco
> de WSL (`~/`) acelera P4, P5, P6 y la frescura.

**1. Sistema limpio.** Borra los volúmenes de MongoDB y Valkey, reconstruye
y levanta todo (pide confirmar con `s`). Esperar ~30 s a que la réplica
elija primario:

```bash
make reset
make ps          # todos "running" / "healthy"
make salud       # "estado": "operativo" en las 4 capas
make replica     # 1 PRIMARY + 2 SECONDARY, w: majority, índices -> p2_replica.txt
make aof         # appendonly yes / appendfsync everysec
```

**2. Datos** (~1 min):

```bash
make datos       # 60.000 tareas en MongoDB y ~830.000 eventos Parquet -> datos_generacion.txt
```

**3. Analítica (P5).** Justo después de los datos, con el datalake limpio:
Q1 ancla su ventana de 90 días en el último evento del datalake.

```bash
make p5-validar  # Q1-Q5 contra valores a mano -> p5_validacion_consultas.txt
make p5          # w=1 y w=2, tiempos y planes de Q1/Q3 -> p5_analitica.txt (~3 min)
```

**4. Registro y gateway (P1, P2).** `smoke-registro` y `smoke-gateway`
cargan antes la demo (`make demo`), porque `make datos` vacía el registro.

```bash
make explain-registro   # planes de las consultas del registro -> p2_explain_registro.txt
make smoke-registro     # demo + smoke -> p2_registro_smoke.txt
make smoke-gateway      # demo + smoke -> p1_gateway_smoke.txt
make smoke-flujo        # -> p1_gateway_flujo.txt
make p2-gateway         # 50 contra 10 -> p2_concurrencia_gateway.txt
make write-concern      # w:1 frente a w:majority -> p2_write_concern.txt (~1 min)
```

**5. Sesión, historial y frescura (P3, P4).**

```bash
make p3          # -> p3_gateway_sesion.txt (~2,5 min: espera el vencimiento del lock)
make p4          # -> p4_gateway_parquet.txt (~1 min)
make compactar   # junta los micro-lotes que dejaron las pruebas -> compactacion_parquet.txt
make p5-frescura # HTTP 200 de la aprobación -> Q1 la cuenta -> p5_frescura_q1.txt (~3 min)
```

`p5-frescura` debe correr sin otra carga y después de compactar: cada Q1
lee todo el datalake, y con miles de micro-lotes pasaría de 120 s.

**6. Carga (P6).** Se recarga el dataset para que P6 parta con el
inventario completo, se dan más núcleos a Spark y, al terminar, se compacta
y se devuelve Analítica a su configuración normal (eso además vacía la cola
de consultas que deja P6):

```bash
make datos
SPARK_WORKERS=8 docker compose -f proyecto2/docker-compose.yml up -d analitica
make salud       # esperar "analitica": "operativo" (~10 s)
make p6          # 10, 40 y 100 usuarios, 30 s de calentamiento y 3 x 90 s -> p6_carga.txt (~20 min)
make compactar   # P6 deja miles de micro-lotes nuevos
docker compose -f proyecto2/docker-compose.yml up -d analitica   # vuelve a SPARK_WORKERS por defecto
```

P6 reparte a los usuarios entre 100 proyectos del dataset (el usuario `u`
trabaja en el proyecto `u % 100`), así 100 usuarios no agotan las 500 tareas
de un solo proyecto. `P6_PROYECTOS=n` cambia la cantidad y `P6_PROYECTO_ID=<id>`
vuelve a un solo proyecto. La duración la fija el enunciado (3 niveles x
(30 s + 3 x 90 s)); más recursos no la acortan, pero `SPARK_WORKERS` alto
evita que `/analitica` se ahogue en cola.

**7. Salud y fallas (P1, P8).** Detienen y reinician contenedores; van al
final. Cada script deja el sistema como estaba:

```bash
make salud       # todo "operativo" antes de empezar
make p1-salud    # /salud con cada capa caída, puertos y aislamiento -> p1_salud_aislamiento.txt (~1,5 min)
make p8-mongo    # secundario, primario y sin mayoría -> p8_mongo_fallas.txt (~2-5 min)
make p8-valkey   # reinicio, AOF, RTO y RPO -> p8_valkey_fallas.txt (~1 min)
```

**8. Cierre.**

```bash
make endpoints   # opcional: OpenAPI actualizado en endpoints/openapi/
make down
```

**Resumen para copiar** (los mismos pasos, sin comentarios):

```bash
make i2-down
make reset && make ps && make salud && make replica && make aof
make datos
make p5-validar && make p5
make explain-registro && make smoke-registro && make smoke-gateway && make smoke-flujo && make p2-gateway && make write-concern
make p3 && make p4 && make compactar && make p5-frescura
make datos
SPARK_WORKERS=8 docker compose -f proyecto2/docker-compose.yml up -d analitica && sleep 15
make p6 && make compactar
docker compose -f proyecto2/docker-compose.yml up -d analitica && sleep 15
make p1-salud && make p8-mongo && make p8-valkey
make down
```

Si un paso falla, revisar `make logs s=<servicio>` y repetir solo ese paso.
Si la corrida se interrumpe, se puede retomar desde el paso que falló sin
`make down`: el sistema queda arriba entre pasos.

### Entorno aislado de Sesión + Historial

`compose.integrante2.yml` levanta solo Valkey, Sesión e Historial (proyecto
`bda-integrante2`, Parquet en `proyecto2/.sandbox_integrante2/`). Sirve para
`smoke_integrante2.sh`. **No lo ejecute a la vez que el Compose final**: cada
uno trae su propio Valkey y consumen memoria innecesaria.

```bash
docker compose -f proyecto2/compose.integrante2.yml -p bda-integrante2 up -d --build
docker compose -f proyecto2/compose.integrante2.yml -p bda-integrante2 down
```

## 5. Datalake Parquet

El worker de Historial escribe los eventos en `proyecto2/datalake/eventos/`,
particionados por mes (`mes=AAAA-MM/`). Los archivos en `.staging/` son
temporales y no deben leerse. `datalake/` está en `.gitignore`: lo produce el
worker y no se versiona.

```bash
ls -R proyecto2/datalake/eventos | head
```

El diseño (publicación atómica, deduplicación por `event_id`, fila
`rechazada`) está en el informe, sección 7.

**Compactación.** Cada micro-lote es un archivo; después de P6 una partición
puede tener miles de archivos pequeños y cada consulta Spark tiene que
abrirlos todos (más lento aún con el repo en `/mnt/c`). `make compactar`:

1. detiene `historial-worker` (los eventos nuevos esperan en Valkey Streams);
2. en cada partición con 20 o más micro-lotes, junta los archivos, deduplica
   por `event_id`, escribe uno solo en `.staging/` y lo publica con
   `os.replace`; verifica filas y `event_id` antes de retirar los originales;
3. mueve los originales a `datalake/respaldo_compactacion/<fecha>/` (fuera de
   lo que lee Spark; no se borran);
4. reanuda el worker.

No toca los archivos del generador (`gen-*`). Deja el resultado en
`evidence/compactacion_parquet.txt` (cada corrida se agrega al final).

## 6. Carga de datos

**Carga de demostración** (solo para desarrollo del registro): crea un
cliente, seis anotadores (dos son revisores, en dos equipos), un proyecto
LiDAR y un lote de tareas. Requiere el sistema levantado.

```bash
docker compose -f proyecto2/docker-compose.yml run --rm registro python /dataset/cargar_demo.py
```

**Dataset oficial** (generador reproducible, semilla fija `20261122`): 60
clientes, 300 anotadores en 30 equipos (10 % revisores), 120 proyectos, 600
lotes y 60 000 tareas en MongoDB; ~800 000 eventos de 100 000 tareas de 180
proyectos (los 120 activos y 60 cerrados) en el datalake Parquet, en los 12
meses previos al 2026-11-01. Clientes, proyectos y equipos tienen los mismos
ids en ambos lados.

> **`make datos` vacía las colecciones del registro** (clientes, anotadores,
> proyectos, lotes y tareas) antes de cargar, y reemplaza los Parquet que
> generó antes; conserva los que publicó el worker.

```bash
make datos
```

Equivale a:

```bash
docker compose -f proyecto2/docker-compose.yml run --rm registro python /dataset/generar_dataset.py --solo mongo
docker compose -f proyecto2/docker-compose.yml run --rm -v "$(pwd)/proyecto2/dataset:/dataset:ro" historial-worker \
  python /dataset/generar_dataset.py --solo parquet --dir-eventos /datalake/eventos
```

## 7. Consultas analíticas Q1–Q5

El servicio `analitica` expone internamente las consultas `q1` a `q5` sobre
`proyecto2/datalake/eventos/` en modo de solo lectura. Para ejecutar la
prueba P5 completa (1.ª y 2.ª ejecución de Q1–Q5 y planes `explain` de Q1 y
Q3):

```bash
make p5
```

Validación de Q1–Q5 con un micro-dataset de resultado conocido:

```bash
make p5-validar
```

Una consulta individual, por el gateway:

```bash
curl -s "http://localhost:8000/analitica/q1?conjunto=principal"
```

## 8. Pruebas

Las pruebas escriben su evidencia en `evidence/`. Un test `SKIPPED` significa
**no ejecutado**, nunca aprobado.

Las pruebas de gateway y registro guardan su evidencia en **un archivo fijo por
tema**, sin fecha ni hora en el nombre; cada ejecución sobrescribe la anterior
y el historial queda en git. La fecha y hora UTC de la corrida van dentro del
archivo.

| Archivo en `evidence/` | Lo genera |
|---|---|
| `p1_gateway_smoke.txt` | `make smoke-gateway` (`pruebas/p1_gateway/smoke_gateway.py`) |
| `p2_replica.txt` | `make replica` (`pruebas/p2_registro/verificar_replica.sh`) |
| `p2_registro_smoke.txt` | `make smoke-registro` (`pruebas/p2_registro/smoke_registro.py`) |
| `p1_gateway_flujo.txt` | `make smoke-flujo` (`pruebas/p1_gateway/smoke_flujo.py`) |
| `p1_salud_aislamiento.txt` | `make p1-salud` (`pruebas/p1_gateway/p1_salud_aislamiento.py`) |
| `p2_concurrencia_gateway.txt` | `make p2-gateway` (`pruebas/p2_registro/p2_concurrencia_gateway.py`) |
| `p2_write_concern.txt` | `make write-concern` (`pruebas/p2_registro/write_concern.py`) |
| `p2_explain_registro.txt` | `make explain-registro` (`pruebas/p2_registro/explain_registro.py`) |
| `p8_mongo_fallas.txt` | `make p8-mongo` (`pruebas/p8_fallas/p8_mongo.py`) |

Los resultados de cada prueba y su análisis están en el informe
([`docs/informe.md`](docs/informe.md), sección 10).

**Prueba del registro** (flujo completo de transiciones, 409 por token o
estado, 50 asignaciones concurrentes sobre una misma tarea, eventos
pendientes). Primero cargue la demostración; luego:

```bash
docker compose -f proyecto2/docker-compose.yml run --rm registro python /pruebas/p2_registro/smoke_registro.py \
  | tee proyecto2/evidence/p2_registro_smoke.txt
```

**Prueba de humo del entorno aislado de Sesión + Historial:**

```bash
bash proyecto2/pruebas/p3_p4_integracion/smoke_integrante2.sh
```

No usa el gateway, así que **no sustituye** las pruebas P3/P4 de extremo a
extremo.

**Suite pytest de Sesión e Historial dentro de Docker** (usa la base 15 de
Valkey solo para pruebas y no ejecuta `FLUSHDB`):

```bash
docker compose -f proyecto2/compose.integrante2.yml -p bda-integrante2 \
  run --rm \
  -v "$(pwd)/proyecto2:/workspace:ro" -w /workspace \
  -e VALKEY_TEST_URL=redis://valkey:6379/15 \
  -e PYTHONDONTWRITEBYTECODE=1 historial \
  sh -c 'pip install --no-cache-dir -r servicios/sesion/requirements-dev.txt -r servicios/historial/requirements-dev.txt && python -m pytest -p no:cacheprovider pruebas/ -v -ra'
```

**Suite con registro de evidencia** (conserva el log y un resumen JSON):

```bash
python proyecto2/pruebas/p3_p4_integracion/ejecutar_evidencias_p3_p4.py
```

**Reinicio real de Valkey con AOF** (contra el Compose final):

```bash
VALKEY_TEST_URL=redis://localhost:6379/15 \
python proyecto2/pruebas/p3_sesion/p3_reinicio_aof.py \
  --restart-command 'docker compose -f proyecto2/docker-compose.yml restart valkey'
```

Cada ejecución manual se documenta con fecha UTC, objetivo, máquina,
comando exacto, configuración, resultado, interpretación y archivo de
evidencia (formato en el apéndice A del informe).

## 9. Solución de problemas

| Síntoma | Qué revisar |
|---|---|
| `mongo-init` falla o `registro` no arranca | `docker compose -f proyecto2/docker-compose.yml logs mongo-init`. Repita con `run --rm mongo-init`; es idempotente |
| `no replset config has been received` | El conjunto no está inicializado: `mongo-init` debe ejecutar `rs.initiate()`. Verifique que el contenedor vea la versión actual de `init-replica.js` |
| Tests `SKIPPED` | Falta `VALKEY_TEST_URL` o Valkey no responde. No cuentan como aprobados |
| `appendonly` no es `yes` | Revise que `valkey.conf` esté montado en el contenedor `valkey` |
| El borrador o los datos desaparecieron tras un reinicio | ¿Se usó `down -v`? Borra los volúmenes |
| Historial no escribe Parquet | Revise los logs de `historial-worker` y el volumen `datalake/` |
| Mensajes sin confirmar | `XPENDING historial:eventos historial-parquet`; se reclaman tras `HISTORIAL_RECUPERACION_IDLE_MS` (30 000 ms por defecto) |
| Poca memoria / contenedores que se reinician | Cada `mongod` usa 300–500 MB; cierre el Compose aislado de Sesión + Historial |
| Choque de nombres de paquete en pytest | Sesión e Historial usan el paquete `app`; los tests los cargan con nombres distintos. El registro usa `registro_app` |
| `set: pipefail: invalid option name` en un `.sh` | El archivo tiene fin de línea CRLF (checkout en Windows). Conviértalo: `sed -i 's/\r$//' <archivo>` |
| `/salud` marca `analitica: caido` justo después de P6 | Spark sigue procesando las consultas `/analitica` que P6 dejó en cola (el gateway ya cortó a los 120 s). Espere a que termine o `docker compose -f proyecto2/docker-compose.yml restart analitica` |
| `make: getcwd: No such file or directory` | La terminal quedó en una carpeta que se borró y recreó: `cd ~ && cd <ruta del repo>` |

## 10. Documentación

| Documento | Contenido |
|---|---|
| [`docs/informe.md`](docs/informe.md) | Informe del proyecto: arquitectura, recorrido LiDAR, modelo e índices, sesión, datalake, analítica, pruebas, fallas (RTO/RPO), limitaciones y evidencia |
| [`endpoints/README.md`](endpoints/README.md) | Rutas, cuerpos y códigos de cada servicio y contrato de eventos |
| `servicios/*/README.md` | Configuración y ejecución de cada servicio |
| `docs/contratos-sesion-historial.md`, `docs/guia-pruebas-p3-p4.md`, `docs/integracion-y-operacion-integrante2.md` | Contratos, guía de pruebas P3/P4 y operación de Sesión e Historial |
