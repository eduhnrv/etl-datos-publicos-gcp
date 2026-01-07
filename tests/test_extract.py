"""
test_extract.py
===============

Tests unitarios para el módulo extract.py.

Este módulo valida el correcto funcionamiento de la etapa de extracción
del pipeline ETL, asegurando que:
- Los archivos CSV existentes se cargan correctamente.
- Se lanzan excepciones apropiadas cuando el archivo no existe.
- El DataFrame retornado cumple con las expectativas básicas.

Proyecto: ETL Datos Públicos
Autor: E. Henríquez N.
Fecha: 04-01-2026 
"""

import pandas as pd 
import pytest
from pathlib import Path
import pandas.errors as pd_errors

from src.extract import extract_csv

def test_extract_csv_success(monkeypatch):
     """
     Valida que extract_csv:
        - Cargue correctamente un csv existente.
        - Retorne un Dataframe válido.

     """

     #path al archivo de prueba
     test_data_dir = Path(__file__).parent / "data"

     #monkeypatch para redirigir RAW_DIR al path de test
     import src.extract as extract
     monkeypatch.setattr(extract, "RAW_DIR", test_data_dir)
     
     #ejecución
     df = extract_csv("raw_data.csv")
     
     #Validaciones
     assert isinstance(df, pd.DataFrame)
     assert not df.empty
     assert len(df.columns) == 7


def test_extract_csv_file_not_found(monkeypatch):
    """
    Valida que extract_csv lance FileNotFoundError cuando el archivo no existe
    Se utiliza 'archivo_inexistente.csv' de forma intencional:
    - No debe existir bajo ningún escenario.
    - El objetivo del test es validar el comportamiento ante error,
      no la presencia real de un archivo.
    - Evita falsos positivos si el filesystem cambia.

    """
    test_data_dir = Path(__file__).parent / "data"

    import src.extract as extract
    monkeypatch.setattr(extract, "RAW_DIR", test_data_dir)

    with pytest.raises(FileNotFoundError):
        extract_csv("archivo_inexistente.csv")


def test_extract_csv_malformed(monkeypatch):
    """
    Valida el comportamiento de 'extract.csv' frente a un csv con mal formato.
        - El archivo existe, pero su estructura no cumple el formato que espero.
        - El pipeline debe fallar.
    """
    test_data_dir = Path(__file__).parent / "data"

    import src.extract as extract
    monkeypatch.setattr(extract, "RAW_DIR", test_data_dir)

    #el archivo existe pero no cumple el formato deseado,
    with pytest.raises((ValueError, pd_errors.ParserError)):
        extract_csv("malformed_raw.csv")

def test_extract_csv_invalid_schema(monkeypatch):
    """
    Valida que extract_csv lance un ValueError cuando el csv
    no cumple con el esquema esperado (columnas faltantes).
    """

    test_data_dir = Path(__file__).parent /"data"

    import src.extract as extract
    monkeypatch.setattr(extract, "RAW_DIR", test_data_dir)
    
    with pytest.raises(ValueError):
        extract_csv("invalid_schema_raw.csv")
