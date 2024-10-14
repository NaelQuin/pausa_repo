'''
1. Data preparation

  1.1. Load datasets
  1.2. Dropout uneeded columns
  1.3. Join datasets
  1.4. Export datasets
'''

import os

import pandas as pd

# Global parameters
DATASET_PATH = "./dataset/"
OUTPUT_DATASET = "./1_PAUSA.csv"
DROPOUT_COLUMNS = [
    "DTM_UTC", "COD_PARAMETRO", "COD_SENSOR", "LOCAL",
    "LATITUDE", "LONGITUDE", "UNIDADE", "ETIQUETA_NIVEL",
    "COR_NIVEL"
]

PARAMETRO_UNIDADE = {
    "ME": {
        1: "%",
        2: "mbar",
        3: "mm",
        4: "graus Celsius",
        5: "quantidade",
        6: "graus",
        7: "km/h"
    },

    "QA": {
        1: "μg/m3",
        2: "μg/m3",
        3: "μg/m3",
        4: "μg/m3",
        5: "μg/m3",
        6: "mg/m3"
    },

    "RUIDO": {
        1: "dB",
    },

    "VTH": {

    }
}

# Get the list of datasets into folder
datasets = os.listdir(DATASET_PATH)

# Init the joined dataset
joinedDataset = pd.DataFrame()

# Looping through the datasets
for i, dataset in enumerate(datasets):
    print(
        f"Processing dataset {i+1:02}/{len(datasets):02}: {dataset:<25}",
        end="\r"
    )

    # Skip VTH (Traffic) datasets
    if dataset.startswith("VTH"):
        continue

    # Read the CSV file into a DataFrame
    dataset = pd.read_csv(
        f"{DATASET_PATH}/{dataset}",
        sep=",", low_memory=False,
        encoding="utf-8"
    )

    # Dropout uneeded columns
    dataset = dataset.drop(
        columns=DROPOUT_COLUMNS
    )

    # Join the datasets
    joinedDataset = pd.concat(
        [joinedDataset, dataset],
        axis=0
    )

# Dataset remaining
datasets = [dataset for dataset in datasets if dataset.startswith("VTH")]

# Looping through the datasets
for dataset in datasets:
    # Read the CSV file into a DataFrame
    dataset = pd.read_csv(
        f"{DATASET_PATH}/{dataset}",
        sep=",", low_memory=False,
        encooding="utf-8"
    )

    # Dropout uneeded columns
    dataset = dataset.drop(
        columns=DROPOUT_COLUMNS
    )

    # Join the datasets
    joinedDataset = pd.concat(
        [joinedDataset, dataset],
        axis=0
    )

# Export the joined dataset
joinedDataset.to_csv(
    OUTPUT_DATASET, index=False
)