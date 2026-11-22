// Se ejecuta contra el conjunto (URI con replicaSet y w=majority).
// Crea la base `plataforma`, sus colecciones y los índices obligatorios.
const bd = db.getSiblingDB("plataforma");

// Garantía de escritura por defecto del conjunto: mayoría.
printjson(
  db.adminCommand({ setDefaultRWConcern: 1, defaultWriteConcern: { w: "majority" } })
);

const colecciones = ["clientes", "proyectos", "batches", "tareas", "anotadores"];
const existentes = bd.getCollectionNames();
colecciones.forEach((n) => {
  if (!existentes.includes(n)) bd.createCollection(n);
});

// Índices mínimos exigidos por el enunciado (sección 6.1).
// Regla ESR: igualdad primero, luego orden. Prioridad descendente
// (mayor número = más prioritaria).
bd.tareas.createIndex(
  { proyecto_id: 1, estado: 1, prioridad: -1 },
  { name: "tareas_proyecto_estado_prioridad" }
);
bd.tareas.createIndex(
  { "asignacion.anotador_id": 1, estado: 1 },
  { name: "tareas_anotador_estado" }
);
bd.tareas.createIndex({ batch_id: 1 }, { name: "tareas_batch" });
bd.proyectos.createIndex(
  { cliente_id: 1, estado: 1 },
  { name: "proyectos_cliente_estado" }
);

colecciones.forEach((n) => {
  print("== " + n + " ==");
  bd[n].getIndexes().forEach((i) => print("  " + i.name + " " + JSON.stringify(i.key)));
});