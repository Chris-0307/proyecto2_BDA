"""Flujo completo de tareas por el gateway (P1): siguiente, borrador, recuperar,
enviar, liberar, revisión (rechazo y aprobación) y exportar.

Se ejecuta en el contenedor del registro solo para crear sus datos de prueba
(cliente, anotadores y un revisor nuevos en cada corrida, con pymongo). Todas
las operaciones del flujo pasan por el gateway.

  docker compose -f proyecto2/docker-compose.yml run --rm registro \
    python /pruebas/p1_gateway/smoke_flujo.py | tee proyecto2/evidence/p1_gateway_flujo.txt
"""

import json
import os
import platform
import sys
import urllib.error
import urllib.request
from datetime import datetime, timezone
from uuid import uuid4

from pymongo import MongoClient

GATEWAY = os.getenv("GATEWAY_URL", "http://gateway:8000")
REGISTRO = os.getenv("REGISTRO_URL", "http://registro:8003")
MONGO_URI = os.getenv("MONGO_URI")
resultados: list[bool] = []


def llamar(metodo, url, cuerpo=None):
    datos = json.dumps(cuerpo).encode() if cuerpo is not None else None
    req = urllib.request.Request(url, data=datos, method=metodo,
                                 headers={"Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(req, timeout=30) as r:
            crudo = r.read()
            return r.status, json.loads(crudo) if crudo else None
    except urllib.error.HTTPError as e:
        crudo = e.read()
        try:
            return e.code, json.loads(crudo)
        except ValueError:
            return e.code, crudo.decode(errors="replace")


def gw(metodo, ruta, cuerpo=None):
    return llamar(metodo, GATEWAY + ruta, cuerpo)


def comprobar(nombre, condicion, detalle=""):
    resultados.append(bool(condicion))
    print(f"[{'OK' if condicion else 'FALLO'}] {nombre}" + (f" — {detalle}" if detalle else ""))


def preparar():
    """Cliente, dos anotadores y un revisor nuevos (no reutiliza sesiones viejas)."""
    db = MongoClient(MONGO_URI, w="majority", serverSelectionTimeoutMS=5000)["plataforma"]
    cliente, equipo = str(uuid4()), str(uuid4())
    db.clientes.insert_one({"_id": cliente, "nombre": "smoke-flujo",
                            "creado_en": datetime.now(timezone.utc)})
    ids = {"a1": str(uuid4()), "a2": str(uuid4()), "rev": str(uuid4())}
    db.anotadores.insert_many([
        {"_id": ids["a1"], "nombre": "smoke-a1", "equipo_id": equipo, "es_revisor": False},
        {"_id": ids["a2"], "nombre": "smoke-a2", "equipo_id": equipo, "es_revisor": False},
        {"_id": ids["rev"], "nombre": "smoke-rev", "equipo_id": equipo, "es_revisor": True},
    ])
    return cliente, ids


def tarea(prioridad):
    uri = f"s3://smoke-flujo/{uuid4()}"
    return {"modalidad": "imagen", "tipo_tarea": "bbox", "prioridad": prioridad,
            "archivo": {"uri": uri, "sha256": "b" * 64, "bytes": 10, "mime": "image/png"}}


def main():
    print(f"Inicio UTC: {datetime.now(timezone.utc).isoformat()}")
    print("Objetivo: flujo de tareas por el gateway (Sesión + Registro + Historial)")
    print(f"Máquina: {platform.platform()}, CPU lógicos: {os.cpu_count()}")

    cliente, ids = preparar()
    a1, a2, rev = ids["a1"], ids["a2"], ids["rev"]
    taxonomia = [{"etiqueta": "auto", "instruccion": "Vehículo."}]

    _, vacio = gw("POST", "/proyectos", {"cliente_id": cliente, "nombre": "vacío", "taxonomia": taxonomia})
    c, _ = gw("POST", "/tareas/siguiente", {"anotador_id": a1, "proyecto_id": vacio["_id"]})
    comprobar("siguiente en un proyecto sin tareas responde 204", c == 204, f"código {c}")

    _, proyecto = gw("POST", "/proyectos", {"cliente_id": cliente, "nombre": "flujo", "taxonomia": taxonomia})
    pid = proyecto["_id"]
    _, lote = gw("POST", f"/proyectos/{pid}/lotes", {"nombre": "lote", "tareas": [tarea(3), tarea(2), tarea(1)]})
    t3, t2, _t1 = lote["tarea_ids"]

    # --- asignación
    c, s1 = gw("POST", "/tareas/siguiente", {"anotador_id": a1, "proyecto_id": pid})
    comprobar("siguiente responde 200 con la tarea de mayor prioridad",
              c == 200 and s1["tarea"]["_id"] == t3, f"código {c}")
    comprobar("la tarea queda abierta con el token entregado",
              c == 200 and s1["tarea"]["estado"] == "abierta"
              and s1["tarea"]["asignacion"]["token"] == s1["token"])
    comprobar("el evento tarea_abierta se publicó en Historial", c == 200 and s1["evento"] == "publicado",
              s1.get("evento") if isinstance(s1, dict) else "")
    comprobar("la respuesta no expone eventos_pendientes", "eventos_pendientes" not in s1["tarea"])
    tok1 = s1["token"]
    ses1 = {"anotador_id": a1, "proyecto_id": pid, "token": tok1}

    c, b = gw("POST", "/tareas/siguiente", {"anotador_id": a1, "proyecto_id": pid})
    comprobar("un anotador con tarea activa no recibe otra (409)", c == 409, f"código {c}")

    # --- borrador y recuperación
    contenido = {"cajas": [[1, 2, 3, 4]], "etiqueta": "auto"}
    c, b = gw("PUT", f"/tareas/{t3}/borrador", {**ses1, "contenido": contenido})
    comprobar("guardar borrador responde 200", c == 200 and b.get("estado") == "guardado", f"código {c}")
    c, _ = gw("PUT", f"/tareas/{t3}/borrador", {**ses1, "token": str(uuid4()), "contenido": {}})
    comprobar("borrador con token ajeno responde 409", c == 409, f"código {c}")
    c, r = gw("POST", "/sesiones/recuperar", {**ses1, "tarea_id": t3})
    comprobar("recuperar devuelve la sesión activa y el último borrador",
              c == 200 and r["estado"] == "activa" and r["borrador"] == contenido, f"código {c}")

    # --- envío
    c, _ = gw("POST", f"/tareas/{t3}/enviar", {**ses1, "token": str(uuid4()), "contenido": contenido})
    comprobar("enviar con token ajeno responde 409", c == 409, f"código {c}")
    c, e = gw("POST", f"/tareas/{t3}/enviar", {**ses1, "contenido": contenido})
    comprobar("enviar responde 200 y la tarea queda anotada",
              c == 200 and e["tarea"]["estado"] == "anotada", f"código {c}")
    comprobar("el evento tarea_anotada se publicó", c == 200 and e["evento"] == "publicado")
    c, _ = gw("POST", f"/tareas/{t3}/enviar", {**ses1, "contenido": contenido})
    comprobar("segundo envío responde 409", c == 409, f"código {c}")

    # --- liberar
    c, s2 = gw("POST", "/tareas/siguiente", {"anotador_id": a2, "proyecto_id": pid})
    comprobar("otro anotador recibe la siguiente tarea", c == 200 and s2["tarea"]["_id"] == t2, f"código {c}")
    ses2 = {"anotador_id": a2, "proyecto_id": pid, "token": s2["token"]}
    c, l = gw("POST", f"/tareas/{t2}/liberar", ses2)
    comprobar("liberar responde 200 y la tarea vuelve a disponible",
              c == 200 and l["tarea"]["estado"] == "disponible", f"código {c}")
    c, s2b = gw("POST", "/tareas/siguiente", {"anotador_id": a2, "proyecto_id": pid})
    comprobar("tras liberar, la tarea vuelve a ofrecerse", c == 200 and s2b["tarea"]["_id"] == t2, f"código {c}")
    gw("POST", f"/tareas/{t2}/liberar", {"anotador_id": a2, "proyecto_id": pid, "token": s2b["token"]})

    # --- revisión: rechazo
    c, _ = gw("POST", f"/tareas/{t3}/revision", {"revisor_id": a2, "decision": "aprobar"})
    comprobar("revisar con alguien que no es revisor responde 403", c == 403, f"código {c}")
    c, _ = gw("POST", f"/tareas/{t3}/revision", {"revisor_id": rev, "decision": "rechazar"})
    comprobar("rechazar sin motivo responde 422", c == 422, f"código {c}")
    c, rr = gw("POST", f"/tareas/{t3}/revision",
               {"revisor_id": rev, "decision": "rechazar", "motivo": "caja_incompleta"})
    comprobar("rechazar responde 200 y la tarea vuelve a disponible",
              c == 200 and rr["tarea"]["estado"] == "disponible", f"código {c}")
    comprobar("se publicaron revision_iniciada y revision_rechazada",
              c == 200 and rr["eventos"] == ["publicado", "publicado"], str(rr.get("eventos")))

    # --- retrabajo y aprobación
    c, s3 = gw("POST", "/tareas/siguiente", {"anotador_id": a1, "proyecto_id": pid})
    comprobar("la tarea rechazada vuelve a asignarse", c == 200 and s3["tarea"]["_id"] == t3, f"código {c}")
    c, _ = gw("POST", f"/tareas/{t3}/enviar",
              {"anotador_id": a1, "proyecto_id": pid, "token": s3["token"], "contenido": contenido})
    comprobar("reenviar el retrabajo responde 200", c == 200, f"código {c}")
    c, ap = gw("POST", f"/tareas/{t3}/revision", {"revisor_id": rev, "decision": "aprobar"})
    comprobar("aprobar responde 200 y estado aprobada",
              c == 200 and ap["tarea"]["estado"] == "aprobada", f"código {c}")
    c, _ = gw("POST", f"/tareas/{t3}/revision", {"revisor_id": rev, "decision": "aprobar"})
    comprobar("aprobar otra vez responde 409", c == 409, f"código {c}")

    c, ex = gw("GET", f"/proyectos/{pid}/exportar")
    comprobar("exportar devuelve la tarea aprobada",
              c == 200 and [t["_id"] for t in ex["items"]] == [t3], f"código {c}")

    # --- verificación interna: nada quedó pendiente de publicar
    _, pendientes = llamar("GET", REGISTRO + "/tareas?eventos_pendientes=true&limite=200")
    propios = [t["_id"] for t in pendientes if t.get("proyecto_id") == pid]
    comprobar("ningún evento del proyecto quedó pendiente en el registro", propios == [], str(propios))

    ok = sum(resultados)
    print(f"\nResultado: {ok}/{len(resultados)} comprobaciones correctas")
    print(f"Fin UTC: {datetime.now(timezone.utc).isoformat()}")
    sys.exit(0 if ok == len(resultados) else 1)


if __name__ == "__main__":
    main()
