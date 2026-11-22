# Atajos del Proyecto 2. Ejecutar desde la raíz del repositorio (WSL/Linux).
# `make` o `make help` lista los objetivos.

SHELL       := /bin/bash
COMPOSE     := docker compose -f proyecto2/docker-compose.yml
COMPOSE_I2  := docker compose -f proyecto2/compose.integrante2.yml -p bda-integrante2
EVIDENCIA   := proyecto2/evidence
FECHA       = $(shell date -u +%Y%m%dT%H%M%SZ)

# Servicio para `make logs s=registro`
s ?= registro mongo-init sesion historial historial-worker

.DEFAULT_GOAL := help
.PHONY: help up ps down reset logs \
        replica aof demo smoke-registro registro smoke-gateway salud \
        i2-up i2-down i2-smoke i2-pytest p3-p4

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

## ---- Registro / MongoDB (Integrante 1) ----
replica: ## Verifica réplica, mayoría e índices (deja evidencia)
	bash proyecto2/pruebas/verificar_replica.sh

demo: ## Carga de demostración del registro
	$(COMPOSE) run --rm registro python /dataset/cargar_demo.py

smoke-registro: ## Smoke del registro (deja evidencia)
	@mkdir -p $(EVIDENCIA)
	set -o pipefail; $(COMPOSE) run --rm registro python /pruebas/smoke_registro.py \
	  | tee $(EVIDENCIA)/p2_registro_smoke_$(FECHA).txt

registro: up replica demo smoke-registro ## Flujo completo: up + réplica + demo + smoke

## ---- Gateway ----
salud: ## GET /salud del gateway desde el host
	curl -s -i http://localhost:8000/salud

smoke-gateway: ## Smoke del gateway básico (deja evidencia)
	@mkdir -p $(EVIDENCIA)
	set -o pipefail; $(COMPOSE) exec -T gateway python /pruebas/smoke_gateway.py \
	  | tee $(EVIDENCIA)/p1_gateway_smoke_$(FECHA).txt

## ---- Valkey / Sesión / Historial (Integrante 2) ----
aof: ## Verifica AOF de Valkey (esperado: yes / everysec)
	$(COMPOSE) exec valkey valkey-cli CONFIG GET appendonly
	$(COMPOSE) exec valkey valkey-cli CONFIG GET appendfsync

i2-up: ## Levanta el entorno aislado de Sesión + Historial
	$(COMPOSE_I2) up -d --build

i2-down: ## Detiene el entorno aislado
	$(COMPOSE_I2) down

i2-smoke: ## Smoke del entorno aislado
	bash proyecto2/pruebas/smoke_integrante2.sh

i2-pytest: ## Suite pytest de Sesión e Historial en Docker
	$(COMPOSE_I2) run --rm \
	  -v "$(CURDIR)/proyecto2:/workspace:ro" -w /workspace \
	  -e VALKEY_TEST_URL=redis://valkey:6379/15 \
	  -e PYTHONDONTWRITEBYTECODE=1 historial \
	  sh -c 'pip install --no-cache-dir -r servicios/sesion/requirements-dev.txt -r servicios/historial/requirements-dev.txt && python -m pytest -p no:cacheprovider pruebas/ -v -ra'

p3-p4: ## Suite P3/P4 con registro de evidencia
	python3 proyecto2/pruebas/ejecutar_evidencias_p3_p4.py
