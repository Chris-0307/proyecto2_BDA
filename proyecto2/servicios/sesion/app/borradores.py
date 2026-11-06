"""Guardado atomico de borradores y renovacion del lock en Valkey.

El borrador no es una anotacion confirmada. Este modulo no consulta
MongoDB, no cambia estados definitivos ni emite eventos al datalake.
"""

import json
from typing import Any, Literal

from pydantic import BaseModel, Field
from redis import Redis

from .locks import LOCK_TTL_MS, editando_key, lock_key, sesion_key
from .sesiones import DatosSesion


class GuardarBorrador(DatosSesion):
    """El gateway envia el contenido y la identidad de la sesion activa."""

    contenido: dict[str, Any] = Field(
        description="Datos parciales de la anotacion; admite geometria y etiquetas."
    )


class ResultadoBorrador(BaseModel):
    """Resultado de una actualizacion exitosa del ultimo borrador."""

    estado: Literal["guardado"] = "guardado"
    tarea_id: str
    anotador_id: str
    version: int
    actualizado_en_ms: int
    ttl_ms: int


# El script comprueba el propietario, reemplaza el ultimo borrador y renueva
# el candado en una sola operacion. No hay escrituras parciales si el token
# es incorrecto o el candado ya vencio.
GUARDAR_BORRADOR_LUA = """
local lock_key = KEYS[1]
local sesion_key = KEYS[2]
local editando_key = KEYS[3]
local borrador_key = KEYS[4]

local token = ARGV[1]
local tarea = ARGV[2]
local proyecto = ARGV[3]
local anotador = ARGV[4]
local contenido = ARGV[5]
local ttl = tonumber(ARGV[6])

if redis.call('GET', lock_key) ~= token then
    return {0, 0, 0}
end
if redis.call('HGET', sesion_key, 'token') ~= token
    or redis.call('HGET', sesion_key, 'tarea_id') ~= tarea
    or redis.call('HGET', sesion_key, 'proyecto_id') ~= proyecto then
    return {0, 0, 0}
end

local tiempo = redis.call('TIME')
local ahora_ms = tonumber(tiempo[1]) * 1000 + math.floor(tonumber(tiempo[2]) / 1000)

-- Solo se conserva el borrador mas reciente; el hash persiste sin TTL
-- para poder recuperarlo despues de una desconexion o vencimiento.
redis.call('HSET', borrador_key,
    'contenido_json', contenido,
    'anotador_id', anotador,
    'proyecto_id', proyecto,
    'actualizado_en_ms', tostring(ahora_ms))
local version = redis.call('HINCRBY', borrador_key, 'version', 1)

-- El PUT de borrador tambien cuenta como actividad de la sesion.
redis.call('PEXPIRE', lock_key, ttl)
redis.call('ZADD', editando_key, ahora_ms + ttl, anotador)
return {1, version, ahora_ms}
"""


class ServicioBorradores:
    """Encapsula el guardado de borradores en la instancia de Valkey."""

    def __init__(self, cliente: Redis):
        self.cliente = cliente

    def guardar(self, datos: GuardarBorrador) -> ResultadoBorrador | None:
        """Guarda el ultimo contenido y renueva el lock si el token coincide.

        Devuelve None cuando el candado/sesion ya no pertenece al anotador.
        """
        contenido_json = json.dumps(
            datos.contenido, ensure_ascii=False, separators=(",", ":"),
            allow_nan=False,
        )
        resultado = self.cliente.eval(
            GUARDAR_BORRADOR_LUA,
            4,
            lock_key(datos.tarea_id),
            sesion_key(datos.anotador_id),
            editando_key(datos.proyecto_id),
            f"borrador:{datos.tarea_id}",
            datos.token,
            datos.tarea_id,
            datos.proyecto_id,
            datos.anotador_id,
            contenido_json,
            LOCK_TTL_MS,
        )
        if int(resultado[0]) != 1:
            return None
        return ResultadoBorrador(
            tarea_id=datos.tarea_id,
            anotador_id=datos.anotador_id,
            version=int(resultado[1]),
            actualizado_en_ms=int(resultado[2]),
            ttl_ms=LOCK_TTL_MS,
        )
