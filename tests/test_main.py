""" 
test_main.py
============ 
Pruebas unitarias para el módulo main.py.
Este archivo valida los distintos escenarios de ejecución de la función main,
cubriendo tanto el flujo exitoso como los errores esperados.
El objetivo es  garantizar que el pipeline ETL maneje correctamente
credenciales faltantes, archivos inexistentes y errores en la transformación
    
    Casos cubiertos
    --------------- 
        - Ejecución exitosa del pipeline (mock de extract y transform).
        - Credenciales faltantes debe lanzar SystemExit.
        - Archivo inexistente debe lanzar FileNotFoundError.
        - Error en transformación debe lanzar RuntimeError.

Proyecto: ETL Datos Públicos
Autor: E. Henríquez. N.
Fecha: 9 de enero de 2026.
"""
import pytest
import pandas as pd
from unittest.mock import patch
from src.main import main

@patch("src.main.load_csv_to_gcs") #se mockea para evitar la subida real a GCS
@patch("src.main.load_csv") #se mockea para evitar escritura real en directorio
@patch("src.main.transform_dataset")#se mockea para controlar el dataframe devuelto


def test_main_pipeline(mock_transform_dataset, mock_load_csv, mock_load_gcs, monkeypatch):
    """
    Valida que el pipeline invoque:
    - transform_dataset
    - load_csv
    - load_csv_to_gcs
    exactamente una vez cada uno.
    """
    #Credenciales GCP simuladas
    monkeypatch.setenv("GOOGLE_APPLICATION_CREDENTIALS", "fake_credentials.json")
    
    #Dataframe simulado retornado por transform_dataset
    df = pd.DataFrame({
        "ANO_ESTADISTICO": [2020],
        "SEMANA_ESTADISTICA": [1],
        "GRUPO_EDAD": ["0 a 14"],
        "SEXO": ["M"],
        "REGION": ["Metropolitana"],
        "POBLACION": [100000],
        "MUERTES_OBS": [50],
    })
    
    mock_transform_dataset.return_value = df

    with patch.dict("os.environ", {"GOOGLE_APPLICATION_CREDENTIALS": "/fake/credentials.json"}):
    
        main()

    #Validaciones
    mock_transform_dataset.assert_called_once()
    mock_load_csv.assert_called_once()
    mock_load_gcs.assert_called_once()

def test_main_missing_credentials(monkeypatch):
    """
    Verifica que el pipeline se detenga con SystemExit cuando
    no existen credenciales de Google Cloud configuradas.

    Este comportamiento es obligatorio para evitar ejecuciones
    inconsistentes en ambientes mal configurados.
    """
    #Se asegura que la variable no exista
    monkeypatch.delenv("GOOGLE_APPLICATION_CREDENTIALS", raising=False)

    with pytest.raises(SystemExit):
        main()


def test_main_transform_error(monkeypatch):
    """
    Verifica que un error ocurrido durante la transformación
    se propague correctamente y no sea capturado silenciosamente.

    El pipeline debe fallar de forma explícita si la etapa de
    transformación falla.
    """
    monkeypatch.setenv("GOOGLE_APPLICATION_CREDENTIALS", "fake_credentials.json")

    #Forzar error en transform_dataset
    monkeypatch.setattr("src.main.transform_dataset", lambda _:(_ for _ in ()).throw(RuntimeError("Error en la transformacion")))

    with pytest.raises(RuntimeError):
        main()
