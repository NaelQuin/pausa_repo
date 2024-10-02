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
DATASET_PATH = "./currentApproach/dataset/"
OUTPUT_DATASET = "./currentApproach/dataset/PAUSA.csv"
DROPOUT_COLUMNS = [
    "DTM_UTC", "COD_PARAMETRO", "COD_SENSOR", "LOCAL", "COR_NIVEL"
]

# Get the list of datasets into folder
datasets = os.listdir(DATASET_PATH)

# Init the joined dataset
joinedDataset = pd.DataFrame()

# Looping through the datasets
for dataset in datasets:

    # Skip VTH (Traffic) datasets
    if dataset.startswith("VTH"):
        continue

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