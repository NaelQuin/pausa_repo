'''
0. Metadata Generator

  0.1. Load datasets
  0.2. Get the metadata from datasets
  0.3. Generate Data Dictionary (DD) and respective Codebook
  0.4. Export the metadata json file
'''

PARAMETRO_EXPLAIN = {
    "ME": {
        "1": ["HR", "Humidade Relativa média horária [%]"],
        "2": ["PA", "Pressão Atmosférica média horária [mbar]"],
        "3": ["PP", "Preciptação média mensal [mm]"],
        "4": ["TEMP", "Temperatura média horária [graus Celsius]"],
        "5": ["UV", "Ultravioleta unidade [quantidade]"],
        "6": ["VD", "Vento Direção média horária [graus]"],
        "7": ["VI", "Vento Intensidade média horária [km/h]"]
    },

    "QA": {
        "1": ["NO2", "Dióxido de Azoto média horária [µg/m3]"],
        "2": ["O3", "Ozono média horária [µg/m3]"],
        "3": ["PM10", "Partículas suspensas menores que 10µm média horária [µg/m3]"],
        "4": ["PM25", "Partículas suspensas menores que 2,5µm média horária [µg/m3]"],
        "5": ["SO2", "Dióxido de Enxofre média horária [µg/m3]"],
        "6": ["CO", "Monóxido de Carbono média horária [mg/m3]"]
    },

    "RUIDO": {
        "1": ["LAEQ", "Nível sonoro contínuo equivalente [dB(A)]"],
    },

    "VTH": {
        "1": ["VTH", "Volume de Tráfego Horário"]
    }
}

'''
    "PARAMETRO": {
        "1": ["HR", "Humidade Relativa média horária (%)"],
        "2": ["PA", "Pressão Atmosférica média horária (mbar)"],
        "3": ["PP", "Preciptação média mensal (mm)"],
        "4": ["TEMP", "Temperatura média horária (graus Celsius)"],
        "5": ["UV", "Ultravioleta unidade (quantidade)"],
        "6": ["VD", "Vento Direção média horária (graus)"],
        "7": ["VI", "Vento Intensidade média horária (km/h)"]
      },

    "PARAMETRO": {
        "1": ["CO", "Monóxido de Carbono média horária (mg/m3)"],
        "2": ["NO", "Monóxido de Azoto média horária (µg/m3)"],
        "3": ["NO2", "Dióxido de Azoto média horária (µg/m3)"],
        "4": ["O3", "Ozono média horária (µg/m3)"],
        "5": ["PM10", "Partículas suspensas menores que 10µm média horária (µg/m3)"],
        "6": ["PM25", "Partículas suspensas menores que 2,5µm média horária (µg/m3)"],
        "7": ["SO2", "Dióxido de Enxofre média horária (µg/m3)"],
    },

    "PARAMETRO": {
        "1": ["LAEQ", "Nível sonoro contínuo equivalente média horária (dB(A))"],
    },

    "PARAMETRO": {
        "1": ["VTH", "Volume de Tráfego Horário, número de veiculos por sentido/hora"]
    }
}
'''

import os
import re
import time
import json
import datetime
from typing import Any
from copy import deepcopy as copy

import pandas as pd
import numpy as np

from pyidebug import debug

DATASET_PATH = "./dataset/"
MEATADA_OUTPUT_PATH = "./0_metadata.json"

FILL_DD = True
EXPORT_METADATA = True

TABLES_AMOUNT = None
SAMPLE_SIZE = None

TOP_UNIQUE_VALUES = None
PRINT_DELAY = None
FORMAT_INT = False

SKIP_TABLES = [
    "ME",
    "QA",
    "VTH",
    #"RUIDO"
]

VALUES_TO_SORT = {
    "COR_NIVEL": {
        "CINZENTO": 1,
        "VERDE": 2,
        "AMARELO": 3,
        "LARANJA": 4,
        "ENCARNADO": 5,
    },
    "ETIQUETA_NIVEL": {
        "@NA": 1,
        "BAIXO": 2,
        "NORMAL": 3,
        "MODERADO": 4,
        "ELEVADO": 5,
        "MUITO ELEVADO": 6
    }
}

SUPPRESS_ARRAY = 15 # None: no suppress, any int for suppression

CATEGORY = [
    "PAUSA", "ME", "QA", "VTH", "RUIDO"
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
            "Measurement category",
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
            "Measurement category",
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
    "RUIDO": {
        "DTM_UTC": [
            "Datetime in UTC",
        ],
        "DTM_LOCAL": [
            "Datetime in local time (Lisbon)",
        ],
        "TEMATICA": [
            "Measurement category",
        ],
        "COD_PARAMETRO": [
            "Noise id",
        ],
        "PARAMETRO": [
            "Noise index",
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
            "Measurement category",
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
    "RUIDO": {},
    # Template
    "VARS": [
        "UNIQUE_VALUES_RANGE",
        "MEAN",
    ],
}

metadata = {
    "PAUSA": {},
    'ME': {},
    'QA': {},
    'RUIDO': {},
    'VTH': {},
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


def formatDate(values: pd.Series) -> (pd.Series):

    currentFormat = [
        "%Y-%m-%d %H:%M:%S.%f", 
        "%d-%m-%Y %H:%M", 
    ][1]
    newFormat = "%Y%m%d.%H"

    formatedValues = values.apply(
        lambda v: eval(
            datetime.datetime.strptime(v, currentFormat).strftime(newFormat)
        )
    )

    return formatedValues


def formatInt(number: int) -> (str):
    if number >= 1000000:
        output = f"{number // 1000000}M"
    elif number >= 1000:
        output = f"{number // 1000}K"
    else:
        output = str(number)
    return output


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


def getMissingNonemptyAmount(df) -> (tuple[int, int]):
    missing = df.isna().sum().sum()
    if '' in df.values:
        missing += df[df == ''].sum().sum()
    nonempty = df.size - missing
    return int(nonempty), int(missing)


def getDatasetMetadata(directory):

    # Getting all filenames in directory
    files = os.listdir(directory)[:TABLES_AMOUNT]

    # Break a line in console
    print()

    # Looping in filenames
    for i, filename in enumerate(files):

        if any([key in filename for key in SKIP_TABLES]):
            continue

        # Get the dataset category
        category = filename.split("_")[0]

        if not ('.' in filename and category in CATEGORY):
            continue

        if any(metadata[category]):
            # Get current metadata
            currentMetadata = metadata[category]
        else:
            # Initialize current metadata
            currentMetadata = copy(particularMetadata)

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

                if column in ["UNIDADE", "ETIQUETA_NIVEL"]:
                    # Fix some values
                    df[column] = fixValues(category, column, df[column])

                elif "DTM" in column:
                    # Format datetime
                    df[column] = formatDate(df[column])

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
        codebook: dict[str: str] = None
        ) -> dict[str: list[str]]:
    """
    Fill the dictionary with the values
    """

    for k, v in values.items():

        if k not in DD:
            # Init a DD list to respective key
            v = DD[k] = []

        if numberDate(v[0]):
            # Set the range of values string
            rangeOfValues = f"{v[0]} -- {v[-1]}"

            # Insert range of values into first position
            DD[k].insert(0, rangeOfValues)

            if k not in codebook:
                # Init respective dictionary
                codebook[k] = {}

            # Add the respective range of values into codebook
            codebook[k][rangeOfValues] = "Range of values"

        elif codebook is not None:

            if k not in codebook:
                # Init respective dictionary
                codebook[k] = {}

            if k in VALUES_TO_SORT:
                v = sorted(v, key=lambda x: VALUES_TO_SORT[k][x])

            # Looping in each values
            for i, vi in enumerate(v):
                # Append the map (code, value)
                codebook[k][i+1] = vi

            # Insert range of values into first position
            DD[k].insert(0, f"1 -- {i+1}")

    # Set the output format
    output = DD\
        if codebook is None\
        else DD, codebook

    return output


def numberDate(value):

    # Numbers
    if type(value) in (int, float):
        output = True

    # Datetime
    elif re.match(r"^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\.\d$", str(value)):
        output = True

    # Numbers in string
    elif type(value) is str and (value.isdigit() or all(n.isdigit() for n in value.split('.'))):
        output = True

    # Others
    else:
        output = False

    return output


def exportMetadata(metadata, output_file):
    with open(output_file, 'w', encoding="utf-8") as f:
        json.dump(metadata, f, indent=2, ensure_ascii=True)
    return None


def suppressArray(metadata, suppressSize=SUPPRESS_ARRAY):
    output = copy(metadata)
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
