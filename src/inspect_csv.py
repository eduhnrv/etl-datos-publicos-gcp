import pandas as pd
from pathlib import Path

RAW_DIR = Path("../data/raw")
CSV_FILE = "def_semana_epidemiologica.csv"
FILE_PATH = RAW_DIR / CSV_FILE

def inspect_csv(file_path: Path):

    print(f"Cargando csv desde: {file_path}")
    df = pd.read_csv(file_path, sep= '|')

    print("\n --dimensiones--")
    print(f"Filas: {df.shape[0]}, Columnas: {df.shape[1]}")

    print("\n --info--")
    print(df.info())

    print("\n --estadisticas numéricas--")
    print(df.describe())

    print("\n --valores nulos por columna--")
    print(df.isnull().sum())

    print(df.head())

if __name__ == "__main__":

    inspect_csv(FILE_PATH)

    
