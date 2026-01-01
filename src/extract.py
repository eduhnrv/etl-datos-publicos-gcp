import pandas as pd
from pathlib import Path

RAW_DIR = Path("../data/raw")

def extract_csv(file_name: str) -> pd.DataFrame:

    file_path = RAW_DIR / file_name
    df = pd.read_csv(file_path, sep = '|')
    
    print(f"CSV cargado con {len(df)} filas y {len(df.columns)} columnas")

    return df

if __name__ == "__main__":

    df = extract_csv("def_semana_epidemiologica.csv")
    print(df.info())

