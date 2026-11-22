// Inicializa el conjunto de réplicas rs-plataforma. Idempotente.
// No depende de capturar errores concretos: usa hello() para saber si el
// nodo ya pertenece a un conjunto (setName presente) y cualquier fallo
// transitorio durante la espera se trata como "todavía no está listo".
const NOMBRE = "rs-plataforma";
const MIEMBROS = [
  { _id: 0, host: "mongo-1:27017" },
  { _id: 1, host: "mongo-2:27017" },
  { _id: 2, host: "mongo-3:27017" },
];

const h = db.hello();
if (!h.setName) {
  print("Inicializando el conjunto " + NOMBRE);
  printjson(rs.initiate({ _id: NOMBRE, members: MIEMBROS }));
} else {
  print("El conjunto ya estaba inicializado (set=" + h.setName + ")");
}

function rolesSanos() {
  try {
    const e = rs.status();
    if (e.members.length !== 3 || !e.members.every((m) => m.health === 1)) return null;
    return e.members.map((m) => m.stateStr).sort().join(",");
  } catch (err) {
    return null;
  }
}

// Espera hasta tener 1 PRIMARY y 2 SECONDARY sanos (máximo ~90 s).
let listo = false;
for (let i = 0; i < 90 && !listo; i++) {
  listo = rolesSanos() === "PRIMARY,SECONDARY,SECONDARY";
  if (!listo) sleep(1000);
}
if (!listo) {
  print("ERROR: el conjunto no alcanzó 1 PRIMARY + 2 SECONDARY a tiempo");
  quit(1);
}
printjson(
  rs.status().members.map((m) => ({ name: m.name, state: m.stateStr, health: m.health }))
);