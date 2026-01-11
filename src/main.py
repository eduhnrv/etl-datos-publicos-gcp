"""
main.py
=======

Orquestador ETL, etapas:
    1.- Transformación de datos(transform).
    2.- Carga local del dataset transformado.
    3.- Carga del dataset tranformado al Data Lake(GCS)

Proyecto: ETL Datos Públicos.
Autor: E. Henríquez N.
Fecha: 3 de enero de 2026.

"""

from pathlib import Path
import os
import sys

from src.transform import transform_dataset
from src.load import load_csv
from src.load_gcs import load_csv_to_gcs
from src.logger import setup_logger

logger = setup_logger()

def main():
    """
    Orquesta el pipeline ETL completo:
    Transform -> Load local -> Load GCP
    """
    logger.info("Inicio del pipeline ETL")

    #=======================
    #VALIDACION CREDENCIALES
    #=======================
    if not os.getenv("GOOGLE_APPLICATION_CREDENTIALS"):
        logger.error("Credenciales de GCP no configuradas")
        raise SystemExit("Credenciales GCP Faltantes")

    # ======================
    # CONFIGURACIÓN
    # ======================
    BASE_DIR = Path(__file__).resolve().parent.parent
    input_file = "def_semana_epidemiologica.csv"
    output_dir = BASE_DIR /"data" / "transformed"
    base_output_file = "def_semana_epidemiologica_transformed"

    # ======================
    # TRANSFORM
    # ======================
    logger.info("Etapa transform iniciada")
    df_transformed = transform_dataset(input_file)

    # ======================
    # LOAD LOCAL
    # ======================

    logger.info("Etapa Load Local iniciada")
    load_csv(
        df = df_transformed,
        output_dir = output_dir,
        file_name = f"{base_output_file}.csv"
    )

    #=======================
    #LOAD GCS
    #=======================
    logger.info("Etapa Load GCS iniciada")
    
    load_csv_to_gcs(base_output_file)
    
    logger.info("Pipeline  ETL finalizado correctamente")


if __name__ == "__main__":
    try:
        main()
    except Exception:
        logger.error("Ejecucion del pipeline fallida")
        raise


