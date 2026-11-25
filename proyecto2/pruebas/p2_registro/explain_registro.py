"""Planes de ejecución (explain executionStats) de las consultas del registro.

Para cada consulta real del servicio de registro reporta el índice usado, las
claves y documentos examinados, los documentos devueltos, el tiempo y si hubo
ordenamiento en memoria (etapa SORT). Usa los datos cargados (se recomienda
`make datos` antes) y elige valores de ejemplo de esos mismos datos.

  docker compose -f proyecto2/docker-compose.yml run --rm registro \
    python /pruebas/p2_registro/explain_registro.py | tee proyecto2/evidence/p2_explain_registro.txt
"""

import os
import platform
import sys
from datetime import datetime, timezone

from pymongo import MongoClient

MONGO_URI = os.getenv("MONGO_URI")
resultados: list[bool] = []


def comprobar(nombre, condicion, detalle=""):
    resultados.append(bool(condicion))
    print(f"[{'OK' if condicion else 'FALLO'}] {nombre}" + (f" — {detalle}" if detalle else ""))


def etapas(plan) -> list[tuple[str, str | None]]:
    """(etapa, índice) de un plan, de la raíz a las hojas (clásico o SBE)."""
    if not isinstance(plan, dict):
        return []
    if "queryPlan" in plan:  # motor SBE: el plan está anidado
        plan = plan["queryPlan"]
    salida = [(plan.get("stage", "?"), plan.get("indexName"))]
    for clave in ("inputStage", "outerStage", "innerStage"):
        salida += etapas(plan.get(clave))
    for hijo in plan.get("inputStages", []):
        salida += etapas(hijo)
    return salida


def explicar(db, coleccion, filtro, sort=None, limit=0):
    cmd = {"find": coleccion, "filter": filtro}
    if sort:
        cmd["sort"] = sort
    if limit:
        cmd["limit"] = limit
    r = db.command("explain", cmd, verbosity="executionStats")
    st = r["executionStats"]
    pasos = etapas(r["queryPlanner"]["winningPlan"])
    return {
        "etapas": " -> ".join(e for e, _ in pasos),
        "indice": next((i for _, i in pasos if i), None),
        "sort_memoria": any(e == "SORT" for e, _ in pasos),
        "devueltos": st["nReturned"],
        "claves": st["totalKeysExamined"],
        "docs": st["totalDocsExamined"],
        "ms": st["executionTimeMillis"],
    }


def main():
    print(f"Inicio UTC: {datetime.now(timezone.utc).isoformat()}")
    print("Objetivo: planes de ejecución de las consultas del registro (explain executionStats)")
    print(f"Máquina: {platform.platform()}, CPU lógicos: {os.cpu_count()}")

    cliente = MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)
    db = cliente["plataforma"]
    print(f"MongoDB {cliente.server_info()['version']}; "
          f"tareas={db.tareas.estimated_document_count()}, "
          f"proyectos={db.proyectos.estimated_document_count()}, "
          f"anotadores={db.anotadores.estimated_document_count()}")
    print("Índices de tareas: " + ", ".join(i["name"] for i in db.tareas.list_indexes()))

    # Valores de ejemplo tomados de los datos.
    def mayor(estado):
        r = list(db.tareas.aggregate([{"$match": {"estado": estado}},
                                      {"$group": {"_id": "$proyecto_id", "n": {"$sum": 1}}},
                                      {"$sort": {"n": -1}}, {"$limit": 1}]))
        return r[0]["_id"] if r else None

    p_disp = mayor("disponible")
    p_aprob = mayor("aprobada") or p_disp
    t = db.tareas.find_one({"asignacion.anotador_id": {"$exists": True, "$ne": None}},
                           {"asignacion.anotador_id": 1}) or {}
    anotador = (t.get("asignacion") or {}).get("anotador_id") or db.anotadores.find_one()["_id"]
    lote = db.tareas.find_one({}, {"batch_id": 1})["batch_id"]
    proy = db.proyectos.find_one({}, {"cliente_id": 1})
    tarea = db.tareas.find_one({}, {"_id": 1})["_id"]
    ahora = datetime.now(timezone.utc)

    consultas = [
        ("Candidatas disponibles (/tareas/siguiente)", "tareas",
         {"proyecto_id": p_disp, "estado": "disponible"}, {"prioridad": -1}, 20,
         "tareas_proyecto_estado_prioridad", False),
        ("Candidatas abiertas vencidas (/tareas/siguiente)", "tareas",
         {"proyecto_id": p_disp, "estado": "abierta", "asignacion.vence_en": {"$lt": ahora}},
         {"prioridad": -1}, 20, "tareas_proyecto_estado_prioridad", False),
        ("Tareas de un anotador", "tareas",
         {"asignacion.anotador_id": anotador, "estado": "abierta"}, None, 0,
         "tareas_anotador_estado", None),
        ("Tareas de un lote", "tareas", {"batch_id": lote}, None, 0, "tareas_batch", None),
        ("Exportar aprobadas (orden por _id)", "tareas",
         {"proyecto_id": p_aprob, "estado": "aprobada"}, {"_id": 1}, 101, None, None),
        ("Proyectos de un cliente", "proyectos",
         {"cliente_id": proy["cliente_id"], "estado": "activo"}, None, 0,
         "proyectos_cliente_estado", None),
        ("Transición por _id + estado", "tareas",
         {"_id": tarea, "estado": "disponible"}, None, 0, None, None),
    ]

    print(f"\n{'consulta':<48} {'índice':<34} {'SORT mem.':<9} {'devueltos':>9} "
          f"{'claves':>7} {'docs':>7} {'ms':>4}")
    for nombre, col, filtro, sort, limit, indice_esperado, _ in consultas:
        r = explicar(db, col, filtro, sort, limit)
        print(f"{nombre:<48} {str(r['indice']):<34} {'sí' if r['sort_memoria'] else 'no':<9} "
              f"{r['devueltos']:>9} {r['claves']:>7} {r['docs']:>7} {r['ms']:>4}")
        print(f"    etapas: {r['etapas']}")
        if indice_esperado:
            comprobar(f"{nombre}: usa {indice_esperado}", r["indice"] == indice_esperado,
                      str(r["indice"]))
        if nombre.startswith("Candidatas"):
            comprobar(f"{nombre}: el orden por prioridad sale del índice (sin SORT en memoria)",
                      not r["sort_memoria"])
        if indice_esperado and r["devueltos"] and "vencidas" not in nombre:
            comprobar(f"{nombre}: no examina documentos de más (docs <= devueltos)",
                      r["docs"] <= r["devueltos"], f"docs={r['docs']} devueltos={r['devueltos']}")
        if nombre.startswith("Exportar"):
            print(f"    Nota: exportación usa {r['indice']} y "
                  f"{'ordena en memoria' if r['sort_memoria'] else 'no ordena en memoria'}; "
                  f"examinó {r['docs']} documentos para devolver {r['devueltos']}.")

    ok = sum(resultados)
    print(f"\nResultado: {ok}/{len(resultados)} comprobaciones correctas")
    print(f"Fin UTC: {datetime.now(timezone.utc).isoformat()}")
    sys.exit(0 if ok == len(resultados) else 1)


if __name__ == "__main__":
    main()
