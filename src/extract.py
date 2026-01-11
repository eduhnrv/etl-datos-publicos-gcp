'''
extract.py
==========

Validación de existencia y extracción del archivo 

Responsabilidades:
- Verificar existencia del archivo.
- Leer el CSV con separador esperado.
- Validar que el archivo no esté vacío.
- Validar esquema mínimo requerido.


Proyecto: ETL Datos Públicos
Autor: E. Henríquez. N.
Fecha: 3 de enero de 2026.

'''

import pandas as pd
from pathlib import Path
from pandas.errors import EmptyDataError
from src.logger import setup_logger

logger = setup_logger()

#Directorio donde se almacenan los csv crudos descargados
BASE_DIR = Path(__file__).resolve().parent.parent

# Directorio donde se almacenan los CSV crudos
RAW_DIR = BASE_DIR / "data" / "raw"

# Esquema mínimo esperado
EXPECTED_COLUMNS = {
    "ANO_ESTADISTICO",
    "SEMANA_ESTADISTICA",
    "GRUPO_EDAD",
    "SEXO",
    "REGION",
    "POBLACION",
    "MUERTES_OBS",
}


def extract_csv(file_name: str) -> pd.DataFrame:
    """
    Carga un archivo CSV desde el directorio raw y valida su estructura básica.

    Parámetros
    ----------
    file_name : str
        Nombre del archivo CSV a cargar.
    
    Retorna
    -------
    pd.DataFrame
        DataFrame con los datos crudos validados.

    Excepciones
    -----------
    FileNotFoundError
        Si el archivo no existe.
    ValueError
        Si el CSV no cumple con el esquema esperado.
    EmptyDataError
        Si el archivo está vacío.
    PermissionError
        Si no se puede leer el archivo por permisos.
    """

    file_path = RAW_DIR / file_name #construye la ruta completa al archivo
    
    if not file_path.exists():
        logger.error(f"No se encontró el archivo raw: {file_path}")
        raise FileNotFoundError(file_path)

    logger.info(f"Cargando archivo RAW: {file_path}")
    try:
        df = pd.read_csv(file_path, sep = '|') #lee el .csv '|' separador
    except EmptyDataError:
        logger.error(f"El archivo {file_name} está vacío")
        raise
    except PermissionError as e:
        logger.error(f"No se pudo leer el archivo {file_name}: {e}")
        raise

    if df.empty:
        logger.error(f"El Archivo {file_name} no contiene filas")
        raise ValueError("El archivo csv está vacío")

    #=====================
    #Validación de esquema
    #=====================
    
    missing_columns = EXPECTED_COLUMNS - set(df.columns)
    if missing_columns:
        logger.error(
                f"CSV inválido. Columnas faltantes: {missing_columns}"
        )
        raise ValueError(
            f"El archivo {file_name} no cumple el esquema que se espera..."
            f"Columnas faltantes: {missing_columns}"
        )
    
    logger.info(f"CSV cargado correctamente | Filas = {len(df)} | Columnas = {len(df.columns)}")

    return df
