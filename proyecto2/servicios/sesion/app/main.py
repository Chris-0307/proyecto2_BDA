"""
Servicio de Sesion.

Este servicio administra la informacion temporal
de las tareas utilizando Valkey.

En esta primera version se implementa el endpoint
de salud y la conexion basica con Valkey.
"""

import os

from fastapi import FastAPI
from fastapi.responses import JSONResponse
from redis import Redis, RedisError


# Se crea la aplicacion REST.
app = FastAPI(
    title="Servicio de Sesion",
    description="Gestion de sesiones y borradores con Valkey",
    version="0.1.0"
)


# Se obtiene la direccion de Valkey desde una variable
# de entorno. El valor predeterminado corresponde
# al nombre esperado del servicio en Docker Compose.
VALKEY_URL = os.getenv(
    "VALKEY_URL",
    "redis://valkey:6379/0"
)


@app.get("/salud")
def consultar_salud():
    """
    Comprueba la disponibilidad de Valkey.

    Si responde correctamente, el servicio informa
    que se encuentra operativo.

    Si no responde, informa que esta degradado
    sin detener el proceso de FastAPI.
    """

    try:
        # Se establece una conexion con Valkey.
        with Redis.from_url(
            VALKEY_URL,
            socket_connect_timeout=2,
            socket_timeout=2
        ) as cliente:

            # Se envia PING para comprobar la conexion.
            cliente.ping()

        return {
            "servicio": "sesion",
            "estado": "operativo",
            "valkey": "disponible"
        }

    except (RedisError, OSError, ValueError):

        # Si Valkey falla, se reporta su indisponibilidad.
        return JSONResponse(
            status_code=503,
            content={
                "servicio": "sesion",
                "estado": "degradado",
                "valkey": "no disponible"
            }
        )
