#transform.py

import pandas as pd
from extract import extract_csv
from pathlib import Path

df = extract_csv("def_semana_epidemiologica.csv")

#Limpieza de datos

df["GRUPO_EDAD"] = df["GRUPO_EDAD"].str.strip()
df["REGION"] = df["REGION"].str.strip()
df["SEXO"] = df["SEXO"].map({1: 'M', 2: 'F'})

#transformación de GRUPO_EDAD

def parse_rango(rango: str):
    rango = str(rango).strip()
    if rango.endswith("+"): #caso '80 + '
        min_ = int(rango.replace("+", "").strip())
        max_ = 100 #límite arbitrario 
        return min_, max_
    if "a" in rango: #caso '0 a 14'
        min_, max_ = rango.split(" a ")
        return int(min_.strip()), int(max_.strip())
    return None, None # si hay un valor inesperado


#crear columnas, EDAD_MIN, EDAD_MAX

df[["EDAD_MIN", "EDAD_MAX"]] = df["GRUPO_EDAD"].apply(lambda x: pd.Series(parse_rango(x)))

#crear columna de edad promedio

df["EDAD_PROMEDIO"] = (df["EDAD_MIN"] + df["EDAD_MAX"]) / 2

#verificamos el resultado

print(df[["GRUPO_EDAD", "EDAD_MIN", "EDAD_MAX", "EDAD_PROMEDIO"]].head())
print(df.info())

#guardar Dataset transformado

Path("../data/transformed").mkdir(parents = True, exist_ok = True)
df.to_csv("../data/transformed/def_semana_epidemiologica_transformed.csv", index = False)
print("Archivo transformado guardado en ../data/transformed/")


