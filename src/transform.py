'''
transform.py
============

Módulo responsable de aplicar transformaciones al dataset epidemiológico
extraído desde fuentes públicas del Ministerio de Salud de Chile.

Las transformaciones incluyen:
- Limpieza de campos categóricos.
- Normalización de variables demográficas.
- Derivación de rangos etarios en valores numéricos.
- Creación de métricas auxiliares
- Incorporación de metadatos de carga.

Proyecto: ETL Datos Públicos.
Autor: E. Henríquez N.
Fecha: 3 de enero de 2026.

'''

import pandas as pd
from datetime import date
from typing import Tuple, Optional
from src.extract import extract_csv
from src.logger import setup_logger

logger = setup_logger()

def parse_rango(rango: str) -> Tuple[Optional[int],Optional [int]]:
    """
    Convierte un rango de edad en edad mínima y máxima.
    Ejemplos:
    - '0 a 14' -> (0, 14)
    - '80 +'   -> (80, 100)
    """
    rango = str(rango).strip()

    # Caso 80 +
    if rango.endswith("+"):
        min_ = int(rango.replace("+", "").strip())
        max_ = 100  # límite arbitrario
        return min_, max_

    # Caso 0 a 14, 15 a 29, etc.
    if " a " in rango:
        min_, max_ = rango.split(" a ")
        return int(min_.strip()), int(max_.strip())

    return None, None


def transform_dataset(file_name: str) -> pd.DataFrame:
    """
    Ejecuta todas las transformaciones del dataset epidemiológico.
    Retorna un DataFrame transformado.

    Incluye:
    - Limpieza de columnas categóricas.
    - Normalización de valores de sexo.
    - Transformación de rangos etarios en variables numéricas.
    - Cálculo de edad promedio.
    - Incorporación de fecha de carga.

    """
    logger.info("inicio de transformaciones del dataset")

    # ======================
    # CARGA DATASET CRUDO
    # ======================
    df = extract_csv(file_name)

    # ======================
    # LIMPIEZA DE DATOS
    # ======================

    # Eliminación de espacios innecesarios
    df["GRUPO_EDAD"] = df["GRUPO_EDAD"].str.strip()
    df["REGION"] = df["REGION"].str.strip()

    # Mapea sexo numérico a categórico
    df["SEXO"] = df["SEXO"].map({1: "M", 2: "F"})

    # ======================
    # TRANSFORMACIÓN GRUPO_EDAD
    # ======================

    df[["EDAD_MIN", "EDAD_MAX"]] = df["GRUPO_EDAD"].apply(
        lambda x: pd.Series(parse_rango(x))
    )

    # Edad promedio
    df["EDAD_PROMEDIO"] = (df["EDAD_MIN"] + df["EDAD_MAX"]) / 2


    #============================
    #CREACIÓN COLUMNA FECHA_CARGA
    #============================
     
    df["FECHA_CARGA"] = date.today()

    # ======================
    # VALIDACIÓN BÁSICA
    # ======================
    #print(df[["GRUPO_EDAD", "EDAD_MIN", "EDAD_MAX", "EDAD_PROMEDIO"]].head())
    #print(df.info())

    logger.info("Transformación completada 100%")

    return df

