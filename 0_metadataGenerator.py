'''
0. Metadata Generator

  0.1. Load datasets
  0.2. Get the metadata from datasets
  0.3. Generate Data Dictionary (DD) and respective Codebook
  0.4. Export the metadata json file
'''

import time
import os
import json
import re
from typing import Any
from copy import deepcopy

import pandas as pd
import numpy as np

from pyidebug import debug

DATASET_PATH = "./dataset/"
MEATADA_OUTPUT_PATH = "./0_metadata.json"

FILL_DD = True
TABLES_AMOUNT = None
EXPORT_METADATA = True

SAMPLE_SIZE = None
TOP_UNIQUE_VALUES = None
PRINT_DELAY = None
FORMAT_INT = False

SUPPRESS_ARRAY = 15 # None: no suppress, any int for suppression

CATEGORY = [
    "PAUSA", "ME", "QA", "VTH"
]

DD = {
    "PAUSA": {
        "NR_ESTACAO": [
            "Sensor station id",
        ],
        "DTM_UTC": [
            "Datetime in UTC",
        ],
        "DTM_LOCAL": [
            "Datetime in local time (Lisbon)",
        ],
        "COD_PARAMETRO": [
            "Object parameter id",
        ],
        "COD_SENSOR": [
            "Sensor id",
        ],
        "LOCAL": [
            "Sensor place",
        ],
        "LONGITUDE": [
            "Longitude coordinate of the sensor",
        ],
        "LATITUDE": [
            "Latitude coordinate of the sensor",
        ],
        "UNIDADE": [
            "Measurement unit",
        ],
        "ETIQUETA_NIVEL": [
            "Meaning of the criteria for issuing the Lisbon environmental index color",
        ],
        "COR_NIVEL": [
            "Criteria for issuing the Lisbon environmental index color",
        ],
        "VALOR": [
            "Measured value",
        ],
    },
    "ME": {
        "DTM_UTC": [
            "Datetime in UTC",
        ],
        "DTM_LOCAL": [
            "Datetime in local time (Lisbon)",
        ],
        "TEMATICA": [
            "Measurement type",
        ],
        "COD_PARAMETRO": [
            "Meteorological parameter id",
        ],
        "PARAMETRO": [
            "Meteorological parameter",
        ],
        "NR_ESTACAO": [
            "Sensor station id",
        ],
        "COD_SENSOR": [
            "Sensor id",
        ],
        "LOCAL": [
            "Sensor place",
        ],
        "LONGITUDE": [
            "Longitude coordinate of the sensor",
        ],
        "LATITUDE": [
            "Latitude coordinate of the sensor",
        ],
        "UNIDADE": [
            "Measurement unit",
        ],
        "ETIQUETA_NIVEL": [
            "Meaning of the criteria for issuing the Lisbon environmental index color",
        ],
        "COR_NIVEL": [
            "Criteria for issuing the Lisbon environmental index color",
        ],
        "VALOR": [
            "Measured value",
        ]
    },
    "QA": {
        "DTM_UTC": [
            "Datetime in UTC",
        ],
        "DTM_LOCAL": [
            "Datetime in local time (Lisbon)",
        ],
        "TEMATICA": [
            "Measurement type",
        ],
        "COD_PARAMETRO": [
            "Gas or particles id",
        ],
        "PARAMETRO": [
            "Gas or particles",
        ],
        "NR_ESTACAO": [
            "Sensor station id",
        ],
        "COD_SENSOR": [
            "Sensor id",
        ],
        "LOCAL": [
            "Sensor place",
        ],
        "LONGITUDE": [
            "Longitude coordinate of the sensor",
        ],
        "LATITUDE": [
            "Latitude coordinate of the sensor",
        ],
        "UNIDADE": [
            "Measurement unit",
        ],
        "ETIQUETA_NIVEL": [
            "Meaning of the criteria for issuing the Lisbon environmental index color",
        ],
        "COR_NIVEL": [
            "Criteria for issuing the Lisbon environmental index color",
        ],
        "VALOR": [
            "Measured value"
        ],
    },
    "VTH": {
        "DTM_UTC": [
            "Datetime in UTC",
        ],
        "DTM_LOCAL": [
            "Datetime in local time (Lisbon)",
        ],
        "TEMATICA": [
            "Measurement type",
        ],
        "COD_PARAMETRO": [
            "Traffic parameter id",
        ],
        "PARAMETRO": [
            "Traffic parameter",
        ],
        "NR_ESTACAO": [
            "Sensor station id",
        ],
        "COD_SENSOR": [
            "Sensor id",
        ],
        "slo_id": [
            "???",
        ],
        "LOCAL": [
            "Sensor place",
        ],
        "LATITUDE": [
            "Latitude coordinate of the sensor",
        ],
        "LONGITUDE": [
            "Longitude coordinate of the sensor",
        ],
        "UNIDADE": [
            "Measurement unit",
        ],
        "SLO_FROMTO": [
            "Traffic flow direction (FROM TO)",
        ],
        "SMO_FROMTO_TOTAL": [
            "???",
        ],
        "SMO_FROMTO_L": [
            "???",
        ],
        "SMO_FROMTO_M": [
            "???",
        ],
        "SMO_FROMTO_P": [
            "???",
        ],
        "SLO_TOFROM": [
            "Traffic flow reverse direction (TO FROM)",
        ],
        "SMO_TOFROM_TOTAL": [
            "???",
        ],
        "SMO_TOFROM_L": [
            "???",
        ],
        "SMO_TOFROM_M": [
            "???",
        ],
        "SMO_TOFROM_P": [
            "???"
        ],
    }
}

CODEBOOK = {
    "PAUSA": {},
    "ME": {},
    "QA": {},
    "VTH": {},
    # Template
    "VARS": {
        "UNIQUE_VALUES": [],
        "EXPLAINED": {},
    },
}

metadata = {
    "PAUSA": {},
    'ME': {},
    'QA': {},
    'VTH': {}
}

particularMetadata = {
    'DD': {},
    'CODEBOOK': {},
    'INFO': {
        "NNONEMPTY": 0,
        "NMISSING": 0,
        "NVARS": set(),
        "TVARS": {},
        "VARS": set(),
        "NUNIQUE": {},
        "UNIQUE": {},
    }
}

def formatInt(number: int) -> (str):
    if number >= 1000000:
        output = f"{number // 1000000}M"
    elif number >= 1000:
        output = f"{number // 1000}K"
    else:
        output = str(number)
    return output

def getMissingNonemptyAmount(df) -> (tuple[int, int]):
    missing = df.isna().sum().sum()
    if '' in df.values:
        missing += df[df == ''].sum().sum()
    nonempty = df.size - missing
    return int(nonempty), int(missing)


def getDatasetMetadata(directory):
    # MECols = set()
    # QACols = set()
    # VTHCols = set()

    files = os.listdir(directory)[:TABLES_AMOUNT]

    print()
    for i, filename in enumerate(files):
        # Get the dataset category
        category = filename.split("_")[0]

        if not ('.' in filename and category in CATEGORY):
            continue

        if any(metadata[category]):
            # Get current metadata
            currentMetadata = metadata[category]
        else:
            # Initialize current metadata
            currentMetadata = deepcopy(particularMetadata)

            # Fill the DD and CODEBOOK
            currentMetadata['DD'] = DD[category]
            currentMetadata['CODEBOOK'] = CODEBOOK[category]

        print(
            f"\033[28G\t[{i+1:03}/{len(files):03}] {filename:<25}",
            "\033[63G"+24*" ",
            end="\r"
        )

        if filename.endswith('.csv'):
            # Get the file path
            filePath = os.path.join(directory, filename)

            # Read the CSV file into a DataFrame
            print("\tReading csv file", end="\r")
            df = pd.read_csv(
                filePath, sep=",", nrows=SAMPLE_SIZE
            )

            # Update the current metadata
            currentMetadata["INFO"]["NVARS"] |= set(df.columns)
            currentMetadata["INFO"]["VARS"] |= set(df.columns)
            nonempty, missing = getMissingNonemptyAmount(df)
            currentMetadata["INFO"]["NNONEMPTY"] += nonempty
            currentMetadata["INFO"]["NMISSING"] += missing
            if any(currentMetadata["INFO"]["NUNIQUE"]):
                for k in currentMetadata["INFO"]["NUNIQUE"]:
                    currentMetadata["INFO"]["NUNIQUE"][k] += int(df.nunique(dropna=True)[k])
            else:
                currentMetadata["INFO"]["NUNIQUE"] = {
                    k: int(v) for k, v in df.nunique(dropna=True).to_dict().items()
                }

            # Show progress title
            print("\tStoring col data", end="\r")

            # Loop through each column in the DataFrame
            for k, column in enumerate(df.columns):
                # Show progress
                print(
                    f"\033[63G [{k+1:03}/{len(df.columns):03}] {column:<15}",
                    end="\r"
                )

                if column not in currentMetadata["INFO"]["TVARS"]:
                    # Initialize the column key
                    currentMetadata["INFO"]["TVARS"][column] = []
                    currentMetadata["INFO"]["UNIQUE"][column] = []

                # Get column values type
                types = str(df[column].dtypes)

                # Update the current metadata
                currentMetadata["INFO"]["TVARS"][column].extend(
                    types if type(types) is list else [types]
                )
                currentMetadata["INFO"]["UNIQUE"][column].extend(
                    df[column].unique().tolist()
                )

                if PRINT_DELAY is not None:
                    # Delay to best show
                    time.sleep(PRINT_DELAY)

            if not any(metadata[category]):
                # Update the metadata
                metadata[category] = currentMetadata

            # if "ME" in filename:
            #     MECols |= set(file_metadata['columns'])
            # elif "QA" in filename:
            #     QACols |= set(file_metadata['columns'])
            # elif "VTH" in filename:
            #     VTHCols |= set(file_metadata['columns'])

    for category in metadata:

        if "INFO" in metadata[category]:
            # Get set lenght
            metadata[category]["INFO"]["NVARS"] = len(
                metadata[category]["INFO"]["NVARS"]
            )

            # Convert set to list
            metadata[category]["INFO"]["VARS"] = list(
                metadata[category]["INFO"]["VARS"]
            )

            if "TVARS" in metadata[category]["INFO"]:
                for k, v in metadata[category]["INFO"]["TVARS"].items():
                    # Convert array to list
                    metadata[category]["INFO"]["TVARS"][k] = (
                        np.unique(v).tolist()
                    )

                    v = metadata[category]["INFO"]["TVARS"][k]
                    if len(v) == 1:
                        metadata[category]["INFO"]["TVARS"][k] = v[0]

            if "UNIQUE" in metadata[category]["INFO"]:

                for k, v in metadata[category]["INFO"]["UNIQUE"].items():
                    # Unique values
                    unique_v = np.unique(v).tolist()

                    # Convert dict to list
                    metadata[category]["INFO"]["UNIQUE"][k] = unique_v

                    # Recalculate the unique values amount
                    metadata[category]["INFO"]["NUNIQUE"][k] = len(unique_v)

    if FORMAT_INT:
        # Convert all int values to stringInt based on metadata dictionary
        for category in metadata:
            if "INFO" in metadata[category]:
                if "NUNIQUE" in metadata[category]["INFO"]:
                    for column, value in metadata[category]["INFO"]["NUNIQUE"].items():
                        metadata[category]["INFO"]["NUNIQUE"][column] = formatInt(
                            value
                        )

                if "NNONEMPTY" in metadata[category]["INFO"]:
                    metadata[category]["INFO"]["NNONEMPTY"] = formatInt(
                        metadata[category]["INFO"]["NNONEMPTY"]
                    )

                if "NMISSING" in metadata[category]["INFO"]:
                    metadata[category]["INFO"]["NMISSING"] = formatInt(
                        metadata[category]["INFO"]["NMISSING"]
                    )

                if "NVARS" in metadata[category]["INFO"]:
                    metadata[category]["INFO"]["NVARS"] = formatInt(
                        metadata[category]["INFO"]["NVARS"]
                    )

    if FILL_DD:
        for category in metadata:
            if "DD" in metadata[category]:
                fillDD(
                    metadata[category]["DD"],
                    metadata[category]["INFO"]["UNIQUE"],
                    codebook=metadata[category]["CODEBOOK"],
                )

    return metadata

def fillDD(
        DD: dict[str: list[str]],
        values: dict[str: Any],
        deepcopy: bool = False,
        codebook: dict[str: str] = None
        ) -> dict[str: list[str]]:
    """
    Fill the dictionary with the values
    """
    if deepcopy:
        DD = deepcopy(DD)
        codebook = deepcopy(codebook)

    for k, v in values.items():

        if k not in DD:
            v = DD[k] = []

        if numberDate(v[0]):
            DD[k].insert(0, f"{v[0]} -- {v[-1]}")
        elif codebook is not None:
            if k not in codebook:
                codebook[k] = {}

            for i, vi in enumerate(v):
                codebook[k][i+1] = vi
            DD[k].insert(0, f"1 -- {i+1}")

    output = DD\
        if codebook is None\
        else DD, codebook

    return output

def numberDate(value):

    if type(value) in (int, float):
        output = True
    elif re.match(r"^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\.\d$", str(value)):
        output = True
    elif type(value) is str and (value.isdigit() or all(n.isdigit() for n in value.split('.'))):
        output = True
    else:
        output = False

    return output

def exportMetadata(metadata, output_file):
    with open(output_file, 'w', encoding="utf-8") as f:
        json.dump(metadata, f, indent=2, ensure_ascii=True)
    return None


def suppressArray(metadata, suppressSize=SUPPRESS_ARRAY):
    suppressed = False
    output = deepcopy(metadata)
    for k, v in output.items():
        if type(v) is dict:
            output[k] = suppressArray(v, suppressSize)
        elif type(v) in (list, tuple):
            if len(v) > suppressSize:
                output[k] = v[:suppressSize] + ["..."] + v[-suppressSize:]

    return output

if __name__ == "__main__":
    print("Reading files from DATASET_PATH", end="...\n")
    metadata = getDatasetMetadata(DATASET_PATH)
    if SUPPRESS_ARRAY:
        metadata = suppressArray(metadata)
    print("\033[2F\033[35GDone")
    if EXPORT_METADATA:
        exportMetadata(metadata, MEATADA_OUTPUT_PATH)
        print(f"Metadata exported to {MEATADA_OUTPUT_PATH}")
    else:
        print(
            json.dumps(metadata, indent=2, ensure_ascii=True)
        )
