"""API interna del servicio de Sesion con Valkey.

Este servicio no se expone directamente al host en el despliegue final.
El gateway coordinara la asignacion atomica en MongoDB y, si falla,
solicitara la liberacion del candado obtenido aqui.
"""

import os

from fastapi import FastAPI, HTTPException, status
from fastapi.responses import JSONResponse
from redis import Redis, RedisError

from .locks import LOCK_TTL_MS
from .sesiones import DatosSesion, InicioSesion, ResultadoSesion, ServicioSesiones

app = FastAPI(
    title="Servicio de Sesion",
    description="Gestion temporal de sesiones y locks con Valkey",
    version="0.2.0",
)

VALKEY_URL = os.getenv("VALKEY_URL", "redis://valkey:6379/0")


def obtener_cliente() -> Redis:
    """Crea un cliente por peticion con timeouts definidos."""
    return Redis.from_url(
        VALKEY_URL,
        decode_responses=True,
        socket_connect_timeout=2,
        socket_timeout=2,
    )


@app.get("/salud")
def consultar_salud():
    """Informa si la dependencia Valkey esta disponible."""
    try:
        with obtener_cliente() as cliente:
            cliente.ping()
        return {"servicio": "sesion", "estado": "operativo", "valkey": "disponible"}
    except (RedisError, OSError, ValueError):
        return JSONResponse(
            status_code=503,
            content={"servicio": "sesion", "estado": "degradado", "valkey": "no disponible"},
        )


@app.post("/sesiones/iniciar", response_model=ResultadoSesion, status_code=status.HTTP_201_CREATED)
def iniciar_sesion(datos: InicioSesion):
    """Adquiere el lock exclusivo y registra la sesion en Valkey."""
    try:
        with obtener_cliente() as cliente:
            resultado = ServicioSesiones(cliente).iniciar(datos)
    except (RedisError, OSError, ValueError):
        raise HTTPException(status_code=503, detail="Valkey no disponible") from None

    if resultado == 0:
        raise HTTPException(status_code=409, detail="La tarea ya esta bloqueada")
    if resultado == 2:
        raise HTTPException(status_code=409, detail="El anotador ya tiene una sesion activa")
    if resultado != 1:
        raise HTTPException(status_code=503, detail="No se pudo adquirir el lock")

    return ResultadoSesion(
        estado="activa", tarea_id=datos.tarea_id,
        anotador_id=datos.anotador_id, ttl_ms=LOCK_TTL_MS,
    )


@app.post("/sesiones/latido", response_model=ResultadoSesion)
def renovar_sesion(datos: DatosSesion):
    """Renueva el candado si el token y la sesion son validos."""
    try:
        with obtener_cliente() as cliente:
            renovado = ServicioSesiones(cliente).latido(datos)
    except (RedisError, OSError, ValueError):
        raise HTTPException(status_code=503, detail="Valkey no disponible") from None

    if not renovado:
        raise HTTPException(status_code=409, detail="Lock vencido o token incorrecto")
    return ResultadoSesion(
        estado="renovada", tarea_id=datos.tarea_id,
        anotador_id=datos.anotador_id, ttl_ms=LOCK_TTL_MS,
    )


@app.post("/sesiones/finalizar", response_model=ResultadoSesion)
def finalizar_sesion(datos: DatosSesion):
    """Libera el lock si pertenece al anotador y token recibidos."""
    try:
        with obtener_cliente() as cliente:
            liberado = ServicioSesiones(cliente).finalizar(datos)
    except (RedisError, OSError, ValueError):
        raise HTTPException(status_code=503, detail="Valkey no disponible") from None

    if not liberado:
        raise HTTPException(status_code=409, detail="Sesion inexistente o token incorrecto")
    return ResultadoSesion(
        estado="finalizada", tarea_id=datos.tarea_id,
        anotador_id=datos.anotador_id,
    )
