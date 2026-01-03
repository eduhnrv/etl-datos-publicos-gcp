# main.py

from pathlib import Path
from transform import transform_dataset
from load import load_csv


def main():
    """
    Orquesta el pipeline ETL completo:
    Extract -> Transform -> Load
    """

    # ======================
    # CONFIGURACIÓN
    # ======================
    input_file = "def_semana_epidemiologica.csv"
    output_dir = Path("../data/transformed")
    output_file = "def_semana_epidemiologica_transformed.csv"

    # ======================
    # TRANSFORM
    # ======================
    df_transformed = transform_dataset(input_file)

    # ======================
    # LOAD
    # ======================
    load_csv(
        df=df_transformed,
        output_dir=output_dir,
        file_name=output_file
    )


if __name__ == "__main__":
    main()

