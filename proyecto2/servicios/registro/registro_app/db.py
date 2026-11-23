"""Conexión a MongoDB: w=majority, 503 rápido y sin reintentos del driver.

El enunciado pide que, si MongoDB no responde, el envío devuelva 503 sin
reintento. Por eso se desactivan retryWrites y retryReads y se usa un
timeout corto de selección de servidor.
"""

import os

from pymongo import MongoClient
from pymongo.database import Database
from pymongo.errors import ConnectionFailure, ExecutionTimeout, WriteConcernError

MONGO_URI = os.getenv(
    "MONGO_URI",
    "mongodb://mongo-1:27017,mongo-2:27017,mongo-3:27017/plataforma"
    "?replicaSet=rs-plataforma&w=majority",
)
BASE_DE_DATOS = os.getenv("MONGO_DB", "plataforma")

# ConnectionFailure incluye ServerSelectionTimeoutError, NetworkTimeout y
# NotPrimaryError. WriteConcernError incluye WTimeoutError: ahí la escritura
# PUDO aplicarse en el primario aunque el cliente reciba el error.
ERRORES_INDISPONIBLE = (ConnectionFailure, WriteConcernError, ExecutionTimeout)

_cliente: MongoClient | None = None


def obtener_cliente() -> MongoClient:
    global _cliente
    if _cliente is None:
        _cliente = MongoClient(
            MONGO_URI,
            w="majority",
            wtimeoutMS=int(os.getenv("MONGO_WTIMEOUT_MS", "5000")),
            serverSelectionTimeoutMS=int(os.getenv("MONGO_SELECCION_MS", "3000")),
            connectTimeoutMS=2000,
            socketTimeoutMS=5000,
            retryWrites=False,
            retryReads=False,
            tz_aware=True,
        )
    return _cliente


def bd() -> Database:
    return obtener_cliente()[BASE_DE_DATOS]