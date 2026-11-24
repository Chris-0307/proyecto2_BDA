# Atajos del Proyecto 2. Ejecutar desde la raíz del repositorio (WSL/Linux).
# `make` o `make help` lista los objetivos.

SHELL       := /bin/bash
COMPOSE     := docker compose -f proyecto2/docker-compose.yml
COMPOSE_I2  := docker compose -f proyecto2/compose.integrante2.yml -p bda-integrante2
EVIDENCIA   := proyecto2/evidence

# Servicio para `make logs s=registro`
s ?= registro mongo-init sesion historial historial-worker

.DEFAULT_GOAL := help
.PHONY: help up ps down reset logs \
        replica aof demo smoke-registro registro smoke-gateway salud smoke-flujo \
        p2-gateway p8-mongo p8-valkey write-concern p1-salud endpoints \
        i2-up i2-down i2-smoke i2-pytest p3-p4 p3 p4 \
        datos simulador p5 p6

help: ## Lista los objetivos
	@grep -hE '^[a-zA-Z0-9_-]+:.*## ' $(MAKEFILE_LIST) | \
	  awk 'BEGIN{FS=":.*## "}{printf "  \033[36m%-16s\033[0m %s\n", $$1, $$2}'

## ---- Sistema completo ----
up: ## Construye y levanta todo el Compose final
	$(COMPOSE) up -d --build

ps: ## Estado de los contenedores
	$(COMPOSE) ps

down: ## Detiene el sistema (conserva volúmenes)
	$(COMPOSE) down

reset: ## BORRA volúmenes (MongoDB y Valkey) y levanta desde cero
	@read -p "Esto borra los datos de MongoDB y Valkey. ¿Seguir? [s/N] " r; [ "$$r" = "s" ]
	$(COMPOSE) down -v
	$(COMPOSE) up -d --build

logs: ## Logs recientes (make logs s=registro)
	$(COMPOSE) logs --tail=100 $(s)

## ---- Registro / MongoDB (Jimmy) ----
replica: ## Verifica réplica, mayoría e índices (deja evidencia)
	bash proyecto2/pruebas/p2_registro/verificar_replica.sh

demo: ## Carga de demostración del registro
	$(COMPOSE) run --rm registro python /dataset/cargar_demo.py

smoke-registro: ## Smoke del registro (deja evidencia)
	@mkdir -p $(EVIDENCIA)
	set -o pipefail; $(COMPOSE) run --rm registro python /pruebas/p2_registro/smoke_registro.py \
	  | tee $(EVIDENCIA)/p2_registro_smoke.txt

registro: up replica demo smoke-registro ## Flujo completo: up + réplica + demo + smoke

## ---- Gateway ----
salud: ## GET /salud del gateway desde el host
	curl -s -i http://localhost:8000/salud

smoke-gateway: ## Smoke del gateway básico (deja evidencia)
	@mkdir -p $(EVIDENCIA)
	set -o pipefail; $(COMPOSE) exec -T gateway python /pruebas/p1_gateway/smoke_gateway.py \
	  | tee $(EVIDENCIA)/p1_gateway_smoke.txt

smoke-flujo: ## Flujo completo de tareas por el gateway (deja evidencia)
	@mkdir -p $(EVIDENCIA)
	set -o pipefail; $(COMPOSE) run --rm registro python /pruebas/p1_gateway/smoke_flujo.py \
	  | tee $(EVIDENCIA)/p1_gateway_flujo.txt

p1-salud: ## P1: /salud con cada servicio caído, puertos y aislamiento (host, deja evidencia)
	@mkdir -p $(EVIDENCIA)
	set -o pipefail; python3 proyecto2/pruebas/p1_gateway/p1_salud_aislamiento.py \
	  | tee $(EVIDENCIA)/p1_salud_aislamiento.txt

p2-gateway: ## P2: 50 solicitudes simultáneas contra 10 tareas (deja evidencia)
	@mkdir -p $(EVIDENCIA)
	set -o pipefail; $(COMPOSE) run --rm registro python /pruebas/p2_registro/p2_concurrencia_gateway.py \
	  | tee $(EVIDENCIA)/p2_concurrencia_gateway.txt

write-concern: ## Latencia y rendimiento con w:1 frente a w:majority (deja evidencia)
	@mkdir -p $(EVIDENCIA)
	set -o pipefail; $(COMPOSE) run --rm registro python /pruebas/p2_registro/write_concern.py \
	  | tee $(EVIDENCIA)/p2_write_concern.txt

endpoints: ## Exporta el OpenAPI de cada servicio a proyecto2/endpoints/openapi/
	@mkdir -p proyecto2/endpoints/openapi
	@for par in gateway:gateway_app.main registro:registro_app.main sesion:app.main historial:app.main; do \
	  s=$${par%%:*}; m=$${par#*:}; \
	  $(COMPOSE) exec -T $$s python -c "import json; from $$m import app; print(json.dumps(app.openapi(), ensure_ascii=False, indent=2))" \
	    > proyecto2/endpoints/openapi/$$s.json && echo "OK proyecto2/endpoints/openapi/$$s.json"; \
	done

## ---- Fallas (P8) ----
p8-mongo: ## P8: caída de secundario, primario y mayoría de MongoDB (deja evidencia, ~3-5 min)
	@mkdir -p $(EVIDENCIA)
	set -o pipefail; python3 proyecto2/pruebas/p8_fallas/p8_mongo.py \
	  | tee $(EVIDENCIA)/p8_mongo_fallas.txt

p8-valkey: ## P8: fallas de Valkey, persistencia AOF, RTO y RPO
	python3 proyecto2/pruebas/p8_fallas/p8_valkey.py

## ---- Valkey / Sesión / Historial (Christian) ----
aof: ## Verifica AOF de Valkey (esperado: yes / everysec)
	$(COMPOSE) exec valkey valkey-cli CONFIG GET appendonly
	$(COMPOSE) exec valkey valkey-cli CONFIG GET appendfsync

i2-up: ## Levanta el entorno aislado de Sesión + Historial
	$(COMPOSE_I2) up -d --build

i2-down: ## Detiene el entorno aislado
	$(COMPOSE_I2) down

i2-smoke: ## Smoke del entorno aislado
	bash proyecto2/pruebas/p3_p4_integracion/smoke_integrante2.sh

i2-pytest: ## Suite pytest de Sesión e Historial en Docker
	$(COMPOSE_I2) run --rm \
	  -v "$(CURDIR)/proyecto2:/workspace:ro" -w /workspace \
	  -e VALKEY_TEST_URL=redis://valkey:6379/15 \
	  -e PYTHONDONTWRITEBYTECODE=1 historial \
	  sh -c 'pip install --no-cache-dir -r servicios/sesion/requirements-dev.txt -r servicios/historial/requirements-dev.txt && python -m pytest -p no:cacheprovider pruebas/ -v -ra'

p3-p4: ## Suite P3/P4 con registro de evidencia
	python3 proyecto2/pruebas/p3_p4_integracion/ejecutar_evidencias_p3_p4.py

p3: ## P3: sesiones, recuperacion, AOF y vencimiento de locks
	python3 proyecto2/pruebas/p3_sesion/p3_gateway_sesion.py

p4: ## P4: materializacion, reprocesamiento, deduplicacion y frescura Parquet
	python3 proyecto2/pruebas/p4_historial/p4_gateway_parquet.py

	
## ---- Datos, analítica Spark, simulador y carga (Emmanuel) ----
datos: ## Genera dataset en MongoDB y datalake Parquet (semilla fija)
	$(COMPOSE) run --rm registro python /dataset/generar_dataset.py --solo-mongo
	$(COMPOSE) run --rm analitica python /dataset/generar_dataset.py --solo-parquet

simulador: ## Ejecuta el simulador HTTP contra el gateway
	python3 proyecto2/simulador/simulador_http.py

p5: ## P5: analítica Spark Q1-Q5 y planes explain (deja evidencia)
	@mkdir -p $(EVIDENCIA)
	set -o pipefail; python3 proyecto2/pruebas/p5_analitica/run_p5.py \
	  | tee $(EVIDENCIA)/p5_analitica.txt

p6: ## P6: carga HTTP 10, 40 y 100 usuarios contra el gateway (deja evidencia)
	@mkdir -p $(EVIDENCIA)
	set -o pipefail; python3 proyecto2/pruebas/p6_carga/run_p6.py \
	  | tee $(EVIDENCIA)/p6_carga.txt