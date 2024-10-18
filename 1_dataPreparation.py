'''
1. Data preparation

  1.1. Load datasets
  1.2. Dropout uneeded columns
  1.3. Join datasets
  1.4. Export datasets
'''

import os
import datetime

import pandas as pd

from pyidebug import debug

# Global parameters
DATASET_PATH = "./dataset/"
OUTPUT_DATASET = "./1_PAUSA_%02d.csv"
DROPOUT_COLUMNS = [
    "DTM_UTC", "COD_PARAMETRO", "COD_SENSOR", "LOCAL",
    "LATITUDE", "LONGITUDE", "UNIDADE", "ETIQUETA_NIVEL",
    "COR_NIVEL"
]

COLUMNS_ORDER = [
    "TEMATICA", "NR_ESTACAO", "DTM_LOCAL", "PARAMETRO", "VALOR"
]

SKIP_TABLES = [
    # "PAUSA",
    # "ME",
    # "QA",
    "VTH",
    "RUIDO"
]
TABLES_AMOUNT = None
STEP_TABLES = None

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
        1: "Ve\u00edculos"
    }
}

PARAMETRO = {
    "ME": {
        "HR": 1, 
        "PA": 2,
        "PP": 3,
        "TEMP": 4,
        "UV": 5,
        "VD": 6,
        "VI": 7,
    },

    "QA": {
        "CO": 1,
        "NO": 2,
        "NO2": 3,
        "O3": 4,
        "PM10": 5,
        "PM25": 6,
        "SO2": 7
    },

    "RUIDO": {
        "LAEQ": 1,
    },

    "VTH": {
        "TMD": 1,
        "VTH": 2
    }
}

TEMATICA = {
    "ME": 1,
    "QA": 2,
    "RUIDO": 3,
    "VTH": 4
}

"VTH: VALOR/hour = SMO_FROMTO_TOTAL + SMO_TOFROM_TOTAL"


def adjustValues(dataset: pd.DataFrame, category: str) -> (pd.DataFrame):
    codebook = PARAMETRO[category]

    #dataset = fixValues(category, column, dataset)

    for k, v in codebook.items():
        dataset.loc[dataset["PARAMETRO"] == k, "PARAMETRO"] = int(v)
        #dataset["PARAMETRO"] = dataset["PARAMETRO"].replace(k, v)

    dataset["DTM_LOCAL"] = formatDate(dataset["DTM_LOCAL"])

    dataset["TEMATICA"] = dataset["TEMATICA"].replace(TEMATICA)

    return None


def formatDate(values: pd.Series) -> (pd.Series):

    currentFormat = {
        1: "%Y-%m-%d %H:%M:%S.%f", 
        2: "%d-%m-%Y %H:%M", 
    }[1]
    newFormat = "%Y%m%d.%H"

    formatedValues = values.apply(
        lambda v: float(
            datetime.datetime.strptime(v, currentFormat).strftime(newFormat)
        )
    )

    return formatedValues


def fixValues(
        category: str,
        column: str,
        values: pd.Series
        ) -> (pd.Series):

    replacements = {
        # Meteorological dataset
        "ME": {
            "UNIDADE": {
                "          ": "",
                "%         ": "%",
                "km/h      ": "km",
                "mbar      ": "mbar",
                "mm        ": "mm",
                "\u00ba         ": "\u00ba",
                "\u00baC        ": "\u00baC",
            },
            "ETIQUETA_NIVEL": {
                "@ NA": "@NA"
            },
        },

        # Air quality dataset
        "QA": {
            "UNIDADE": {
                "mg/m3     ": "mg/m3",
                "\u00b5g/m3     ": "\u00b5g/m3",
            },
            "ETIQUETA_NIVEL": {
                "@ NA": "@NA"
            },
        },

        # Noise dataset
        "RUIDO": {
            "UNIDADE": {
                "dB(A)     ": "dB(A)",
            },
            "ETIQUETA_NIVEL": {
                "@ NA": "@NA"
            },
        },

        # Traffic dataset
        "VTH": {
            "UNIDADE": {
                "Ve\u00edculos  ": "Ve\u00edculos",
            },
        }
    }[category][column]

    fixedValues = values.replace(replacements)

    return fixedValues


if __name__ == "__main__":
    # Get the list of datasets into folder
    datasets = [d for d in os.listdir(DATASET_PATH) if d.endswith(".csv")]
    
    if (TABLES_AMOUNT, STEP_TABLES) != (None, None):
        # Get a piece of the datasets
        datasets = datasets[:TABLES_AMOUNT*STEP_TABLES:STEP_TABLES]

    # Init the joined dataset
    joinedDataset = pd.DataFrame()

    # Looping through the datasets
    for i, dataset in enumerate(datasets):
        print(
            f"Processing dataset {i+1:02}/{len(datasets):02}: {dataset:<25}",
            end="\r"
        )

        # Skip tables
        if any([term in dataset for term in SKIP_TABLES]):
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

        # Adjust the values to the respective codebook keys
        adjustValues(dataset, dataset.TEMATICA[0]) 

        # Sort the columns
        dataset = dataset[COLUMNS_ORDER]

        # Join the datasets
        joinedDataset = pd.concat(
            [joinedDataset, dataset],
            axis=0
        )

        if (i % 3) == 0 and i > 0:
            # Export the joined dataset
            joinedDataset.to_csv(
                OUTPUT_DATASET % (i//3), index=False
            )

            # Resetting the joined dataset
            joinedDataset = pd.DataFrame()