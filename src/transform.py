# transform.py

import pandas as pd
from extract import extract_csv


def parse_rango(rango: str) -> tuple[int, int]:
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
    if "a" in rango:
        min_, max_ = rango.split(" a ")
        return int(min_.strip()), int(max_.strip())

    return None, None


def transform_dataset(file_name: str) -> pd.DataFrame:
    """
    Ejecuta todas las transformaciones del dataset epidemiológico.
    Retorna un DataFrame transformado.
    """

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

    # ======================
    # VALIDACIÓN BÁSICA
    # ======================
    #print(df[["GRUPO_EDAD", "EDAD_MIN", "EDAD_MAX", "EDAD_PROMEDIO"]].head())
    #print(df.info())

    return df

