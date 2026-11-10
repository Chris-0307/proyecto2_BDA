"""Worker de historial con recuperación de pendientes de Valkey Streams.

Ejecutar con: python -m app.worker
Los mensajes no confirmados por una interrupción se reclaman con XAUTOCLAIM
tras el tiempo mínimo de inactividad configurado.
"""

import logging
import os
import socket
import time

from redis import Redis

from .consumer import MIN_IDLE_MS, procesar_lote
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
    min_idle_ms = int(os.getenv("HISTORIAL_RECUPERACION_IDLE_MS", str(MIN_IDLE_MS)))
    if not 1 <= cantidad <= 1000:
        raise ValueError("HISTORIAL_TAMANO_LOTE debe estar entre 1 y 1000")
    if min_idle_ms < 1:
        raise ValueError("HISTORIAL_RECUPERACION_IDLE_MS debe ser positivo")

    escritor = EscritorParquet(datalake_dir)
    cliente = Redis.from_url(
        valkey_url, decode_responses=True,
        socket_connect_timeout=3, socket_timeout=5,
    )
    LOG.info(
        "Worker %s iniciado; datalake=%s; recuperación después de %d ms",
        consumidor, datalake_dir, min_idle_ms,
    )
    try:
        while True:
            try:
                total = procesar_lote(
                    cliente, escritor.publicar,
                    consumidor=consumidor,
                    cantidad=cantidad,
                    recuperar_pendientes=True,
                    min_idle_ms=min_idle_ms,
                )
                if total:
                    LOG.info("Parquet publicado; %d eventos confirmados", total)
            except Exception:
                # Si Parquet no se publicó, no se ha reconocido el lote.
                # Un reintento posterior o un worker nuevo lo reclamará.
                LOG.exception("Error al consumir o publicar micro-lote")
                time.sleep(3)
    except KeyboardInterrupt:
        LOG.info("Deteniendo worker de historial")
    finally:
        cliente.close()


if __name__ == "__main__":
    main()
