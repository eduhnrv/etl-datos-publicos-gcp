"""
main.py
=======

Orquestador ETL, etapas:
    1.- Extracción de datos(extract).
    2.- Transformación de datos(transform).
    3.- Carga local del dataset transformado.
    4.- Carga del dataset tranformado al Data Lake(GCS)

Proyecto: ETL Datos Públicos.
Autor: E. Henríquez N.
Fecha: 3 de enero de 2026.

"""

from pathlib import Path
from transform import transform_dataset
from load import load_csv
from load_gcs import load_csv_to_gcs
from logger import setup_logger

logger = setup_logger()

def main():
    """
    Orquesta el pipeline ETL completo:
    Extract -> Transform -> Load
    """
    logger.info("Inicio del pipeline ETL")

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
        logger.error("Ejecución del pipeline fallida", exc_info = True)
        raise


