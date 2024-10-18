import os

import pandas as pd
import matplotlib.pyplot as plt

DATASET_FOLDER = './'

datasets = [d for d in os.listdir(DATASET_FOLDER) if d.endswith(".csv")]

"""
MODEL

parameters_by_station = {
    PARAM: {
        STATION: {
            YEAR: {
                MONTH: {
                    DAY: [HOURS]
                }
            },
        }
    }
}
"""

for i, dataset in enumerate(datasets):
    print(
        f"Processing dataset {i+1:02}/{len(datasets):02}: {dataset:<25}",
        end="\r"
    )
    # Read dataset
    df = pd.read_csv(DATASET_FOLDER + dataset)

    byparam = df.groupby()
