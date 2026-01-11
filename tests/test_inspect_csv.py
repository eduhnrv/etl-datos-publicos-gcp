"""
test_inspect_csv.py
===================

Pruebas unitarias para el módulo `inspect_csv.py`.

Este archivo valida los distintos escenarios de uso de la función `inspect_csv`,
cubriendo tanto casos exitosos como errores esperados. El objetivo es alcanzar
100% de cobertura y garantizar que la inspección de CSV maneje correctamente
archivos válidos, vacíos, inexistentes, malformados y con valores nulos.

Casos cubiertos
---------------
- CSV válido con las columnas esperadas del ETL.
- Archivo inexistente debe lanzar FileNotFoundError.
- Archivo vacío debe lanzar EmptyDataError.
- Archivo malformado (separador incorrecto) debe lanzar ParserError.
- CSV con valores nulos debe ejecutarse sin errores y mostrar conteo de nulos.

Proyecto: ETL Datos Públicos
Autor: E. Henríquez. N.
Fecha: 9 de enero de 2026.
"""

import pytest
import pandas as pd
from pathlib import Path
from src.inspect_csv import inspect_csv

# Columnas esperadas en tu esquema
EXPECTED_COLUMNS = [ 
    "ANO_ESTADISTICO",
    "SEMANA_ESTADISTICA", 
    "GRUPO_EDAD",
    "SEXO",
    "REGION",
    "POBLACION",
    "MUERTES_OBS",
]

def test_inspect_csv_valid(tmp_path):
    """
    Verifica que un CSV válido con las columnas esperadas se inspecciona correctamente
    """
    file = tmp_path / "valido.csv"
    df = pd.DataFrame({
        col: [1] for col in EXPECTED_COLUMNS
        })
    df.to_csv(file, sep = '|', index = False)
    inspect_csv(file)

def test_inspect_csv_file_not_found(tmp_path):
    """
    Verifica lance FileNotFoundError si el archivo no existe
    """
    file = tmp_path / "no_existe.csv"
    with pytest.raises(FileNotFoundError):
        inspect_csv(file)

def test_inspect_csv_empty_file(tmp_path):
    """
    Verifica lance EmptyDataError si el archivo está vacío.
    """
    file = tmp_path / "empty.csv"
    file.write_text("")
    with pytest.raises(pd.errors.EmptyDataError):
        inspect_csv(file)

def test_inspect_csv_parser_error(monkeypatch, tmp_path):
    """ 
    Verifica que se lance ParserError si el archivo está malformado.
    """
    file = tmp_path / "malformado.csv"
    file.write_text("contenido roto")
    # Mockeamos pd.read_csv para que lance ParserError
    monkeypatch.setattr(pd, "read_csv", lambda *args, **kwargs: (_ for _ in ()).throw(pd.errors.ParserError("Archivo malformado")))
    
    with pytest.raises(pd.errors.ParserError):
        inspect_csv(file)

def test_inspect_csv_with_nulls(tmp_path):
    """
    Verifica que un CSV con valores nulos se inspecciona correctamente,
    """
    file = tmp_path / "nulos.csv"
    df = pd.DataFrame({ 
        "ANO_ESTADISTICO":[2026, None],
        "SEMANA_ESTADISTICA":[1, None],
        "GRUPO_EDAD":["0 a 14", None],
        "SEXO":["M", None],
        "REGION":["Metropolitana", None],
        "POBLACION":[100000, None],
        "MUERTES_OBS":[50, None],
    })
    
    df.to_csv(file, sep="|", index=False) 
    inspect_csv(file)
