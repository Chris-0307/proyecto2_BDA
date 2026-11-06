"""Operaciones atomicas de locks y sesiones sobre Valkey.

Los scripts Lua impiden que dos solicitudes concurrentes puedan modificar
un candado o su sesion en pasos separados. La asignacion definitiva de una
tarea sigue siendo responsabilidad de MongoDB, a traves del gateway.
"""

from redis import Redis

# La duracion del candado es de 120 segundos, como solicita el enunciado.
LOCK_TTL_MS = 120_000

# Codigos del script de adquisicion:
# 1 = candado adquirido; 0 = tarea bloqueada; 2 = anotador ya ocupado.
ADQUIRIR_LUA = """
local lock_key = KEYS[1]
local sesion_key = KEYS[2]
local editando_key = KEYS[3]

local token = ARGV[1]
local tarea = ARGV[2]
local proyecto = ARGV[3]
local etapa = ARGV[4]
local anotador = ARGV[5]
local ttl = tonumber(ARGV[6])

-- Si el anotador mantiene otra tarea con un lock vigente, no se le
-- permite comenzar una segunda sesion de manera simultanea.
local vieja_tarea = redis.call('HGET', sesion_key, 'tarea_id')
if vieja_tarea then
    local viejo_token = redis.call('HGET', sesion_key, 'token')
    local viejo_proyecto = redis.call('HGET', sesion_key, 'proyecto_id')
    if redis.call('GET', 'lock:tarea:' .. vieja_tarea) == viejo_token then
        return 2
    end

    -- El lock anterior vencio: se limpia la sesion obsoleta, pero
    -- nunca se elimina un borrador que pudiera recuperarse despues.
    if viejo_proyecto then
        redis.call('ZREM', 'editando:' .. viejo_proyecto, anotador)
    end
    redis.call('DEL', sesion_key)
end

-- SET NX PX es la adquisicion exclusiva solicitada.
local adquirido = redis.call('SET', lock_key, token, 'NX', 'PX', ttl)
if not adquirido then
    return 0
end

redis.call('HSET', sesion_key,
    'tarea_id', tarea,
    'proyecto_id', proyecto,
    'etapa', etapa,
    'token', token)

local tiempo = redis.call('TIME')
local ahora_ms = tonumber(tiempo[1]) * 1000 + math.floor(tonumber(tiempo[2]) / 1000)
redis.call('ZADD', editando_key, ahora_ms + ttl, anotador)
return 1
"""

# Solo quien conserva el token puede renovar el candado.
RENOVAR_LUA = """
local lock_key = KEYS[1]
local sesion_key = KEYS[2]
local editando_key = KEYS[3]
local token = ARGV[1]
local tarea = ARGV[2]
local proyecto = ARGV[3]
local anotador = ARGV[4]
local ttl = tonumber(ARGV[5])

if redis.call('GET', lock_key) ~= token then
    return 0
end
if redis.call('HGET', sesion_key, 'token') ~= token
    or redis.call('HGET', sesion_key, 'tarea_id') ~= tarea
    or redis.call('HGET', sesion_key, 'proyecto_id') ~= proyecto then
    return 0
end

redis.call('PEXPIRE', lock_key, ttl)
local tiempo = redis.call('TIME')
local ahora_ms = tonumber(tiempo[1]) * 1000 + math.floor(tonumber(tiempo[2]) / 1000)
redis.call('ZADD', editando_key, ahora_ms + ttl, anotador)
return 1
"""

# La liberacion comprueba el token y nunca borra el lock de otro usuario.
LIBERAR_LUA = """
local lock_key = KEYS[1]
local sesion_key = KEYS[2]
local editando_key = KEYS[3]
local token = ARGV[1]
local tarea = ARGV[2]
local proyecto = ARGV[3]
local anotador = ARGV[4]

if redis.call('HGET', sesion_key, 'token') ~= token
    or redis.call('HGET', sesion_key, 'tarea_id') ~= tarea
    or redis.call('HGET', sesion_key, 'proyecto_id') ~= proyecto then
    return 0
end

local propietario = redis.call('GET', lock_key)
if propietario and propietario ~= token then
    return 0
end
if propietario == token then
    redis.call('DEL', lock_key)
end

redis.call('DEL', sesion_key)
redis.call('ZREM', editando_key, anotador)
-- No se elimina borrador:{tarea}; se resolvera en la fase de borradores.
return 1
"""


def lock_key(tarea_id: str) -> str:
    """Devuelve la llave del candado correspondiente a una tarea."""
    return f"lock:tarea:{tarea_id}"


def sesion_key(anotador_id: str) -> str:
    """Devuelve la llave de sesion de un anotador."""
    return f"sesion:{anotador_id}"


def editando_key(proyecto_id: str) -> str:
    """Devuelve la llave del conjunto ordenado del proyecto."""
    return f"editando:{proyecto_id}"


def adquirir(redis: Redis, *, anotador_id: str, tarea_id: str,
             proyecto_id: str, token: str, etapa: str) -> int:
    """Adquiere un lock y crea su sesion de manera atomica."""
    return int(redis.eval(
        ADQUIRIR_LUA, 3,
        lock_key(tarea_id), sesion_key(anotador_id), editando_key(proyecto_id),
        token, tarea_id, proyecto_id, etapa, anotador_id, LOCK_TTL_MS,
    ))


def renovar(redis: Redis, *, anotador_id: str, tarea_id: str,
            proyecto_id: str, token: str) -> bool:
    """Renueva un lock solo si la sesion y el token coinciden."""
    return bool(redis.eval(
        RENOVAR_LUA, 3,
        lock_key(tarea_id), sesion_key(anotador_id), editando_key(proyecto_id),
        token, tarea_id, proyecto_id, anotador_id, LOCK_TTL_MS,
    ))


def liberar(redis: Redis, *, anotador_id: str, tarea_id: str,
            proyecto_id: str, token: str) -> bool:
    """Libera el lock y la sesion, sin afectar locks ajenos."""
    return bool(redis.eval(
        LIBERAR_LUA, 3,
        lock_key(tarea_id), sesion_key(anotador_id), editando_key(proyecto_id),
        token, tarea_id, proyecto_id, anotador_id,
    ))
