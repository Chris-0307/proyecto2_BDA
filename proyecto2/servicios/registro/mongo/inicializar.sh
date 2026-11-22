#!/bin/sh
# Fase 1: conexión directa a mongo-1 para crear el conjunto.
# Fase 2: conexión por el conjunto (w=majority) para colecciones e índices.
set -eu

mongosh --host mongo-1 --port 27017 --quiet --file /scripts/init-replica.js

mongosh "mongodb://mongo-1:27017,mongo-2:27017,mongo-3:27017/plataforma?replicaSet=rs-plataforma&w=majority" \
  --quiet --file /scripts/configurar-registro.js

echo "mongo-init: conjunto e índices listos"