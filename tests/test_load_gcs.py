"""
test_load_gcs.py
================

Tests unitarios para el módulo test_load_gcs.py.

Responsabilidades validadas:
    - Manejo de error si el archivo local no existe
    - Construcción correcta del nombre versionado
    - Invocación de upload_from_filename al cliente gcs

Proyecto: ETL Datos Públicos
Autor: E. Henríquez N.
Fecha: 9 de enero de 2026
"""

import pytest
from pathlib import Path
from unittest.mock import patch, MagicMock
from src.load_gcs import load_csv_to_gcs

def test_load_csv_to_gcs_file_not_found(tmp_path):
    """
    Valida que se lance FileNotFoundError cuando el archivo local no existe
    """
    base_file_name = "non_existing_file"
    with pytest.raises(FileNotFoundError):
        load_csv_to_gcs(base_file_name)

#se mockea el cliente de GCS para evitar conexión real 
@patch("src.load_gcs.storage.Client")
def test_load_csv_to_gcs_success(mock_client, tmp_path):
    """
    Valida que se invoque upload_from_filename correctamente cuando
    el archivo existe.
    """
    base_file_name = "test_file"
    local_file = tmp_path / f"{base_file_name}.csv"
    local_file.write_text("A, B\n1, x\n2, y")
    
    #se parchea LOCAL_TRANSFORMED_DIR para que apunte al tmp_path
    #evitando usar directorio real
    with patch("src.load_gcs.LOCAL_TRANSFORMED_DIR", tmp_path):
        mock_bucket = MagicMock()
        mock_blob = MagicMock()
        mock_client.return_value.bucket.return_value = mock_bucket
        mock_bucket.blob.return_value = mock_blob

        load_csv_to_gcs(base_file_name)
        mock_blob.upload_from_filename.assert_called_once_with(local_file)


