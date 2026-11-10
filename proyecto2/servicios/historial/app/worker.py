"""Proceso independiente que pasa eventos de Valkey Streams a Parquet.

Ejecutar como segundo servicio Docker usando la misma imagen del historial:
    python -m app.worker

Todavía consume entradas nuevas (XREADGROUP '>'). La recuperación de
entradas pendientes tras un fallo se implementará en el commit 8.
"""

import logging
import os
import socket
import time

from redis import Redis

from .consumer import procesar_lote
from .parquet_writer import EscritorParquet

LOG = logging.getLogger("historial.worker")


def main() -> None:
    logging.basicConfig(
        level=os.getenv("LOG_LEVEL", "INFO"),
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    valkey_url = os.getenv("VALKEY_URL", "redis://valkey:6379/0")
    datalake_dir = os.getenv("DATALAKE_DIR", "/datalake/eventos")
    consumidor = os.getenv("HISTORIAL_CONSUMIDOR", socket.gethostname())
    cantidad = int(os.getenv("HISTORIAL_TAMANO_LOTE", "100"))
    if not 1 <= cantidad <= 1000:
        raise ValueError("HISTORIAL_TAMANO_LOTE debe estar entre 1 y 1000")

    escritor = EscritorParquet(datalake_dir)
    cliente = Redis.from_url(
        valkey_url, decode_responses=True,
        socket_connect_timeout=3, socket_timeout=5,
    )
    LOG.info("Iniciando consumidor %s; datalake=%s", consumidor, datalake_dir)
    try:
        while True:
            try:
                total = procesar_lote(
                    cliente, escritor.publicar,
                    consumidor=consumidor, cantidad=cantidad,
                )
                if total:
                    LOG.info("Micro-lote publicado: %d eventos confirmados", total)
            except Exception:
                # No se confirma el lote al fallar la publicación.
                # El pendiente requerirá reprocesamiento (commit 8).
                LOG.exception("No se pudo procesar el micro-lote")
                time.sleep(3)
    except KeyboardInterrupt:
        LOG.info("Deteniendo consumidor")
    finally:
        cliente.close()


if __name__ == "__main__":
    main()
