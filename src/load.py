# load.py

import pandas as pd
from pathlib import Path


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
    df.to_csv(output_path, index=False)

    print(f"Archivo cargado correctamente en: {output_path}")

