#!/usr/bin/env bash
# Verifica el conjunto de réplicas y los índices; guarda evidencia.
set -Eeuo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROYECTO="$(cd "$SCRIPT_DIR/.." && pwd)"
COMPOSE=(docker compose -f "$PROYECTO/docker-compose.yml")

mkdir -p "$PROYECTO/evidence"
LOG="$PROYECTO/evidence/p2_replica_$(date -u +%Y%m%dT%H%M%SZ).txt"
exec > >(tee "$LOG") 2>&1

echo "Inicio UTC: $(date -u +%Y-%m-%dT%H:%M:%SZ)"
echo "Objetivo: estado del conjunto rs-plataforma, mayoría e índices"
echo "Mongo: $("${COMPOSE[@]}" exec -T mongo-1 mongod --version | head -n 1)"
echo

URI='mongodb://mongo-1:27017,mongo-2:27017,mongo-3:27017/plataforma?replicaSet=rs-plataforma&w=majority'

"${COMPOSE[@]}" exec -T mongo-1 mongosh "$URI" --quiet --eval '
const st = rs.status();
print("set=" + st.set + " term=" + st.term);
printjson(st.members.map(m => ({ name: m.name, state: m.stateStr, health: m.health, syncSource: m.syncSourceHost })));
const cfg = rs.conf();
print("electionTimeoutMillis=" + cfg.settings.electionTimeoutMillis);
printjson(cfg.members.map(m => ({ host: m.host, votes: m.votes, priority: m.priority })));
printjson(db.adminCommand({ getDefaultRWConcern: 1 }).defaultWriteConcern);
["clientes","proyectos","batches","tareas","anotadores"].forEach(n => {
  print("== " + n + " ==");
  db.getCollection(n).getIndexes().forEach(i => print("  " + i.name + " " + JSON.stringify(i.key)));
});
'

echo
echo "Fin UTC: $(date -u +%Y-%m-%dT%H:%M:%SZ)"
echo "Evidencia: $LOG"