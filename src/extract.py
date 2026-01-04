'''
extract.py
==========

Validación de existencia y extracción del archivo 

Proyecto: ETL Datos Públicos
Autor: E. Henríquez. N.
Fecha: 3 de enero de 2026.

'''


import pandas as pd
from pathlib import Path
from logger import setup_logger

logger = setup_logger()

#Directorio donde se almacenan los csv crudos descargados
BASE_DIR = Path(__file__).resolve().parent.parent
RAW_DIR = BASE_DIR / "data" / "raw"

#lee un csv desde carpeta ../data/raw y retorna un Dataframe
def extract_csv(file_name: str) -> pd.DataFrame:

    file_path = RAW_DIR / file_name #construye la ruta completa al archivo
    
    if not file_path.exists():
        logger.error(f"No se encontró el archivo raw: {file_path}")
        raise FileNotFoundError(file_path)

    df = pd.read_csv(file_path, sep = '|') #lee el .csv '|' separador
    logger.info(f"CSV cargado correctamente | Filas = {len(df)} | Columnas = {len(df.columns)}")

    return df

#ejecución local
if __name__ == "__main__":

    df = extract_csv("def_semana_epidemiologica.csv")
    logger.info(df.info())
    

