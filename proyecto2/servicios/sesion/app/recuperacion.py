"""Recuperacion segura del ultimo borrador y la sesion temporal.

La sesion NO decide si una tarea puede volver a asignarse en MongoDB.
Si el candado caduco se devuelve el contenido en modo de solo lectura:
el gateway debe verificar el estado y solicitar una nueva asignacion.
"""

import json
from typing import Any, Literal

from pydantic import BaseModel
from redis import Redis

from .locks import LOCK_TTL_MS, editando_key, lock_key, sesion_key
from .sesiones import DatosSesion


class ResultadoRecuperacion(BaseModel):
    """Contenido recuperado y estado real del lock en el momento de consulta."""

    estado: Literal["activa", "requiere_reasignacion"]
    tarea_id: str
    anotador_id: str
    proyecto_id: str
    etapa: str
    ttl_ms: int
    borrador: dict[str, Any] | None = None
    version: int | None = None
    actualizado_en_ms: int | None = None


# Se valida la sesion y se consulta el borrador en una operacion atomica.
# Si el lock sigue siendo del mismo token, se renueva por 120 segundos.
# Si caduco, NO se vuelve a adquirir, pues Registro tiene que decidir
# si la tarea puede reofrecerse. Nunca se renueva un candado ajeno.
RECUPERAR_LUA = """
local lock_key = KEYS[1]
local sesion_key = KEYS[2]
local editando_key = KEYS[3]
local borrador_key = KEYS[4]

local token = ARGV[1]
local tarea = ARGV[2]
local proyecto = ARGV[3]
local anotador = ARGV[4]
local ttl = tonumber(ARGV[5])

-- Sin una sesion que coincida no se entrega contenido del borrador.
if redis.call('HGET', sesion_key, 'token') ~= token
    or redis.call('HGET', sesion_key, 'tarea_id') ~= tarea
    or redis.call('HGET', sesion_key, 'proyecto_id') ~= proyecto then
    return {0}
end

local propietario = redis.call('GET', lock_key)
if propietario and propietario ~= token then
    return {0}
end

local etapa = redis.call('HGET', sesion_key, 'etapa') or 'anotacion'
local contenido = ''
local version = ''
local actualizado = ''

-- Un borrador de un propietario anterior no puede recuperarse por otro.
if redis.call('HGET', borrador_key, 'anotador_id') == anotador
    and redis.call('HGET', borrador_key, 'proyecto_id') == proyecto then
    contenido = redis.call('HGET', borrador_key, 'contenido_json') or ''
    version = redis.call('HGET', borrador_key, 'version') or ''
    actualizado = redis.call('HGET', borrador_key, 'actualizado_en_ms') or ''
end

if not propietario then
    -- La tarea ya no puede editarse sin una reasignacion desde el gateway.
    redis.call('ZREM', editando_key, anotador)
    return {2, 0, contenido, version, actualizado, etapa}
end

-- La sesion es valida y el proceso acaba de recuperar la actividad.
redis.call('PEXPIRE', lock_key, ttl)
local tiempo = redis.call('TIME')
local ahora_ms = tonumber(tiempo[1]) * 1000 + math.floor(tonumber(tiempo[2]) / 1000)
redis.call('ZADD', editando_key, ahora_ms + ttl, anotador)
return {1, ttl, contenido, version, actualizado, etapa}
"""


class ServicioRecuperacion:
    """Lee la sesion y el ultimo borrador sin acceder a MongoDB."""

    def __init__(self, cliente: Redis):
        self.cliente = cliente

    def recuperar(self, datos: DatosSesion) -> ResultadoRecuperacion | None:
        """Devuelve None si la sesion/token no coincide o el lock es ajeno."""
        resultado = self.cliente.eval(
            RECUPERAR_LUA,
            4,
            lock_key(datos.tarea_id),
            sesion_key(datos.anotador_id),
            editando_key(datos.proyecto_id),
            f"borrador:{datos.tarea_id}",
            datos.token,
            datos.tarea_id,
            datos.proyecto_id,
            datos.anotador_id,
            LOCK_TTL_MS,
        )
        codigo = int(resultado[0])
        if codigo == 0:
            return None

        contenido = json.loads(resultado[2]) if resultado[2] else None
        return ResultadoRecuperacion(
            estado="activa" if codigo == 1 else "requiere_reasignacion",
            tarea_id=datos.tarea_id,
            anotador_id=datos.anotador_id,
            proyecto_id=datos.proyecto_id,
            etapa=resultado[5],
            ttl_ms=int(resultado[1]),
            borrador=contenido,
            version=int(resultado[3]) if resultado[3] else None,
            actualizado_en_ms=int(resultado[4]) if resultado[4] else None,
        )
