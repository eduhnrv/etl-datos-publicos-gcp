"""
test_main.py
============

Test unitarios para el módulo main.py.

Responsabilidades a validar:
    - Orquestación correcta del pipeline ETL
    - Invocación de transform, load local y load GCS en orden

Proyecto: ETL Datos Públicos
Autor: E. Henríquez N.
Fecha: 09-01-2026
"""

from unittest.mock import patch
from src.main import main

@patch("src.main.load_csv_to_gcs") #se mockea para evitar la subida real a GCS
@patch("src.main.load_csv") #se mockea para evitar escritura real en directorio
@patch("src.main.transform_dataset")#se mockea para controlar el dataframe devuelto

def test_main_pipeline(mock_transform, mock_load_csv, mock_load_gcs):
    """
    Valida que el pipeline invoque transform, load, load_gcs en orden
    """
    import pandas as pd
    df = pd.DataFrame({
        "A": [1, 2],
        "B": ["x", "y"]
    })
    mock_transform.return_value = df

    main()

    mock_transform.assert_called_once()
    mock_load_csv.assert_called_once()
    mock_load_gcs.assert_called_once()


