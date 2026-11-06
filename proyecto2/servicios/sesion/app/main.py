"""API interna del servicio de Sesion con Valkey.

Este servicio no se expone directamente al host en el despliegue final.
El gateway coordinara la asignacion atomica en MongoDB y, si falla,
solicitara la liberacion del candado obtenido aqui.
"""

import os

from fastapi import FastAPI, HTTPException, status
from fastapi.responses import JSONResponse
from redis import Redis, RedisError

from .borradores import GuardarBorrador, ResultadoBorrador, ServicioBorradores
from .locks import LOCK_TTL_MS
from .recuperacion import ResultadoRecuperacion, ServicioRecuperacion
from .sesiones import DatosSesion, InicioSesion, ResultadoSesion, ServicioSesiones

app = FastAPI(
    title="Servicio de Sesion",
    description="Gestion temporal de sesiones y locks con Valkey",
    version="0.4.0",
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


@app.put("/tareas/{tarea_id}/borrador", response_model=ResultadoBorrador)
def guardar_borrador(tarea_id: str, datos: GuardarBorrador):
    """Guarda el borrador si el lock coincide y renueva los 120 segundos.

    El gateway debe comprobar el estado de la tarea en Registro. Aqui solo
    se valida la propiedad de la sesion temporal, sin consultar MongoDB.
    """
    if tarea_id != datos.tarea_id:
        raise HTTPException(status_code=422, detail="El ID de la ruta no coincide con la tarea")
    try:
        with obtener_cliente() as cliente:
            guardado = ServicioBorradores(cliente).guardar(datos)
    except (RedisError, OSError, ValueError, TypeError):
        raise HTTPException(status_code=503, detail="Valkey no disponible o contenido invalido") from None

    if guardado is None:
        raise HTTPException(status_code=409, detail="Lock vencido, sesion inexistente o token incorrecto")
    return guardado


@app.post("/sesiones/recuperar", response_model=ResultadoRecuperacion)
def recuperar_sesion(datos: DatosSesion):
    """Recupera la sesion interrumpida y, si existe, su ultimo borrador.

    Con lock valido se renueva y se devuelve la sesion activa. Sin lock,
    el borrador es de solo lectura: el gateway debe consultar Registro
    antes de permitir una nueva asignacion de la tarea.
    """
    try:
        with obtener_cliente() as cliente:
            recuperacion = ServicioRecuperacion(cliente).recuperar(datos)
    except (RedisError, OSError, ValueError, TypeError):
        raise HTTPException(status_code=503, detail="No se pudo consultar Valkey") from None

    if recuperacion is None:
        raise HTTPException(status_code=409, detail="Sesion inexistente, token incorrecto o lock ajeno")
    return recuperacion
