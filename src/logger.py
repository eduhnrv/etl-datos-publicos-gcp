'''
logger.py
=========

Configura y retorna un logger estándar para el proyecto ETL,
permite trazabilidad y compatibilidad con Cloud Logging.

Proyecto: ETL Datos Públicos
Autor: E. Henríquez. N.
Fecha: 3 de enero de 2026.

'''

import logging

def setup_logger(level: int = logging.INFO) -> logging.Logger:

    logging.basicConfig(level = level, format = "%(asctime)s | %(levelname)s | %(name)s | %(message)s")
    logger = logging.getLogger("etl-datos-publicos")

    return logger



