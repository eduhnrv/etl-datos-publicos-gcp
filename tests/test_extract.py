"""
test_extract.py
===============

Tests unitarios para el módulo extract.py.

Valida:
- Carga correcta de CSV válido.
- Error si el archivo no existe.
- Error ante CSV malformado.
- Error ante esquema inválido.
- Error ante CSV vacío.
- Propagación de errores inesperados de pandas.


Proyecto: ETL Datos Públicos
Autor: E. Henríquez N.
Fecha: 04-01-2026 
"""

import pandas as pd 
import pytest
from pathlib import Path
import pandas.errors as pd_errors
from unittest.mock import patch

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

def test_extract_csv_empty_dataframe(monkeypatch, tmp_path):
    """
    Valida que se lance ValueError cuando el csv existe pero no contiene filas
    """

    csv_file = tmp_path / "empty.csv"
    csv_file.write_text("ANO_ESTADISTICO|SEMANA_ESTADISTICA\n")

    import src.extract as extract 
    monkeypatch.setattr(extract, "RAW_DIR", tmp_path)

    with pytest.raises(ValueError):
        extract_csv("empty.csv")

def test_extract_csv_generic_pandas_error(monkeypatch, tmp_path):
    """
    Valida que errores inesperados de pandas se propaguen correctamente
    """

    csv_file = tmp_path / "data.csv"
    csv_file.write_text("A|B\n1|2")

    import src.extract as extract
    monkeypatch.setattr(extract, "RAW_DIR", tmp_path)

    with patch("pandas.read_csv", side_effect=RuntimeError("Error interno")):
        with pytest.raises(RuntimeError):
            extract_csv("data.csv")
