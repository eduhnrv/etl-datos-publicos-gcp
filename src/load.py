"""
load.py
=======
Módulo responsable de persistir datasets transformados
en el filesystem local.

Proyecto: ETL Datos Públicos
Autor: E. Henríquez N.
Fecha: 3 de enero de 2026.

"""

import pandas as pd
from pathlib import Path
from src.logger import setup_logger

logger = setup_logger()

def load_csv(df: pd.DataFrame, output_dir: Path, file_name: str) -> None:
    """
    Guarda un DataFrame como CSV en el directorio indicado.

    Parámetros:
    - df: DataFrame transformado
    - output_dir: Path donde se guardará el archivo
    - file_name: nombre del archivo CSV final
    """

    # Crea el directorio si no existe
    output_dir.mkdir(parents=True, exist_ok=True)

    output_path = output_dir / file_name

    # Guarda el DataFrame
    df.to_csv(output_path, index=False, encoding = "utf-8")

    logger.info(
            f"Archivo cargado correctamente *local* |"
            f"Ruta: {output_path} |"
            f"Filas: {len(df)} | Columnas: {len(df.columns)}"
    )

