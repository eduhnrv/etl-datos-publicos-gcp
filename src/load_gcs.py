"""
load_gcs.py
===========

Módulo responsable de cargar dataset transformados desde el filesystem local
hacia Google Cloud Storage(Data Lake), aplicando versionado temporal mediante
fecha de ejecución

Proyecto: ETL Datos Públicos
Autor: E. Henríquez. N.
Fecha: 3 de enero de 2026.

"""

from pathlib import Path
from datetime import datetime
from google.cloud import storage
from src.logger import setup_logger

logger = setup_logger()
#=================
#Config. Global.
#=================

BASE_DIR = Path(__file__).resolve().parent.parent

BUCKET_NAME = "etl-dp-bucket"
GCS_LAYER = "transformed"
LOCAL_TRANSFORMED_DIR = BASE_DIR / "data" / "transformed"

def load_csv_to_gcs(base_file_name: str) -> None:
    """
    Carga archivo csv transformado al bucket de GCS, agregando
    la fecha de ejecución al nombre del archivo para versionado.
    
    ------------
    Parámetros.
    ------------
    base_file_name: str
        -> Nombre base del archivo csv sin extensión ni fecha.
           ej: 'def_semana_epidemiologica_transformed'
    
    ------------
    Flujo.
    ------------
    1.- Valida la existencia del archivo local.
    2.- Genera nombre versionado con fecha (YYYY-MM-DD).
    3.- Enlaza a GCS.
    4.- Carga el archivo al Data Lake (layer: transformed).

    ------------
    Execpciones.
    ------------
    FileNotFileFoundError -> si el archivo csv no existe.
    """
   
    logger.info("Inicio de carga de dataset a Google Cloud Storage")

    #fecha de ejecución en formato ISO(8601)
    execution_date = datetime.utcnow().strftime("%Y-%m-%d")

    #rutas de archivos
    local_file_path = LOCAL_TRANSFORMED_DIR / f"{base_file_name}.csv"
    gcs_file_name = f"{base_file_name}_{execution_date}.csv"
    gcs_object_path = f"{GCS_LAYER}/{gcs_file_name}"

    #Validación de existencia local
    if not local_file_path.exists():
        logger.error(f"No se ha encontrado el archivo transformado: {local_file_path}")
        raise FileNotFoundError(local_file_path)

    logger.info(f"Archivo origen validado | Ruta local: {local_file_path}")

    #cliente GCS con credenciales de entorno
    client = storage.Client()
    bucket = client.bucket(BUCKET_NAME)
    blob = bucket.blob(gcs_object_path)

    #carga del archivo
    blob.upload_from_filename(local_file_path)

    logger.info(f"Archivo cargado correctamente en GCS | gs://{BUCKET_NAME}/{gcs_object_path}")
    


