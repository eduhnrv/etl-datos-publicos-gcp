"""
test_transform.py
=================

Tests unitarios para el módulo transform.py.

Este módulo valida la correcta ejecución de las transformaciones
de negocio del pipeline ETL, asegurando que:
- El parseo de rangos etarios sea correcto.
- Se creen correctamente las columnas derivadas.
- La transformación no dependa de la extracción real.

Proyecto: ETL Datos Públicos
Autor: E. Henríquez N.
Fecha: 04-01-2026
"""

import pandas as pd
import pytest
from datetime import date

from src.transform import parse_rango, transform_dataset

#===============
#Tests unitarios
#===============

def test_parse_rango_standard():
    """
    Valida rangos en forma de '0 a 14' , '15 a 39'....
    Rango de edad en el dataset
    """
    min_, max_ = parse_rango("0 a 14")
    assert min_ == 0
    assert max_ == 14

def test_parse_rango_plus():
    """
    Valida rangos tipo '80 +'
    """
    min_ ,max_ = parse_rango("80 +")
    assert min_ == 80
    assert max_ == 100

def test_parse_rango_invalid():
    """
    Valida comportamiento ante rango inválido.
    """
    min_, max_ = parse_rango("desconocido")
    assert min_ is None
    assert max_ is None

#======================
#Test de transformación
#======================

def test_transform_dataset_success(monkeypatch):
    """
    Valida que transform_dataset:
        - Cree columnas derivadas correctamente
        - No modifique la cant. de filas
        - Agregue FECHA_CARGA
    """

    #Dataframe simulado (raw)
    raw_df = pd.DataFrame({
        "ANO_ESTADISTICO": [2011],
        "SEMANA_ESTADISTICA": [1],
        "GRUPO_EDAD": ["0 a 14"],
        "SEXO": [1],
        "REGION": [" Metropolitana "],
        "POBLACION": [10101],
        "MUERTES_OBS": [11],

    })
    
    #Mock de extract.csv
    def mock_extract_csv(file_name: str):
        return raw_df.copy()

    import src.transform as transform
    monkeypatch.setattr(transform, "extract_csv", mock_extract_csv)

    #Ejecución
    df = transform.transform_dataset("falso.csv")

    #Validaciones generales
    assert isinstance(df, pd.DataFrame)
    assert len(df) == 1

    # Columnas nuevas
    assert "EDAD_MIN" in df.columns
    assert "EDAD_MAX" in df.columns
    assert "EDAD_PROMEDIO" in df.columns
    assert "FECHA_CARGA" in df.columns

    # Valores esperados
    assert df.loc[0, "EDAD_MIN"] == 0
    assert df.loc[0, "EDAD_MAX"] == 14
    assert df.loc[0, "EDAD_PROMEDIO"] == 7
    assert df.loc[0, "SEXO"] == "M"
    assert df.loc[0, "REGION"] == "Metropolitana"
    assert df.loc[0, "FECHA_CARGA"] == date.today()

