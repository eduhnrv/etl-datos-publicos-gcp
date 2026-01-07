"""
test_load.py
============

Tests unitarios para el módulo load.py.

Responsabilidades validadas:
    - Escritura correcta de DataFrame a CSV
    - Creación automática del directorio destino
    - Manejo de errores ante entradas inválidas

Proyecto: ETL Datos Públicos
Autor: E. Henríquez N.
Fecha: 07-01-2026
"""

from pathlib import Path
import pandas as pd
import pytest

from src.load import load_csv

def test_load_csv_success(tmp_path):
    """
    Valida que load_csv (carga local):
        - Cree el archivo CSV
        - retorne el path correcto
        - guarde correctamente el contenido
    """
    df = pd.DataFrame({
        "A": [1, 2],
        "B": ["x", "y"]

    })

    output_dir = tmp_path / "output"
    file_name = "test_load_local.csv"

    result_path = load_csv(df, output_dir, file_name)
    assert result_path.exists()
    assert result_path.name == file_name

    df_loaded = pd.read_csv(result_path)
    pd.testing.assert_frame_equal(df, df_loaded)

def test_load_csv_creates_directory(tmp_path):
    """
    Valida que load_csv cree el directorio destino
    si no existe.
    """

    df = pd.DataFrame({"A": [1]})
    output_dir = tmp_path / "no_existe"
    file_name = "data.csv"

    assert not output_dir.exists()

    load_csv(df, output_dir, file_name)
    
    assert output_dir.exists()
    assert output_dir.is_dir()

def test_load_csv_invalid_df(tmp_path):
    """
    Valida que load_csv lance TypeError
    cuando df no es un DataFrame.
    """

    with pytest.raises(TypeError):
        load_csv(
            df = "no_dataframe",
            output_dir = tmp_path,
            file_name = "a1.csv"
        )

def test_load_csv_empty_df(tmp_path):
    """
    Valida que load_csv lance ValueError
    cuando el DataFrame está vacío.
    """
    df = pd.DataFrame()
    
    with pytest.raises(ValueError):
        load_csv(
            df = df,
            output_dir = tmp_path,
            file_name = "empty.csv"
        )

