import pandas as pd
from pathlib import Path

#Directorio donde se almacenan los csv crudos descargados
RAW_DIR = Path("../data/raw")

#lee un csv desde carpeta ../data/raw y retorna un Dataframe
def extract_csv(file_name: str) -> pd.DataFrame:

    file_path = RAW_DIR / file_name #construye la ruta completa al archivo
    df = pd.read_csv(file_path, sep = '|') #lee el .csv '|' separador
    print(f"CSV cargado con {len(df)} filas y {len(df.columns)} columnas") #log básico para trazabilidad

    return df

#ejecución local
if __name__ == "__main__":

    df = extract_csv("def_semana_epidemiologica.csv")
    print(df.info())

