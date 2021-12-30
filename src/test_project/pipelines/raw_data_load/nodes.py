"""
This is a boilerplate pipeline 'raw_data_load'
generated using Kedro 0.17.6
"""

from typing import List, Dict
from pandas_profiling import ProfileReport

import pandas as pd
import ast
import json
import os

def create_descriptives(
    data: pd.DataFrame, 
    parameters: Dict
): 
    table_name = parameters["name"]
    
    print(os.getcwd())

    # Create the profiling report
    profile = ProfileReport(
        data, title=f"{table_name} Profiling Report", 
        config_file="config_minimal.yml"
    )

    # Save the report as an HTML file and JSON for further usage
    profile.to_file(f"./data/descriptives/{table_name}.html") 
    # It doesn't save this as data/descriptives/train.html but instead it saves the whole dataframe
    # I really dont know why
    
    json_data = json.loads(profile.to_json())

    with open(f"./data/02_intermediate/{table_name}.json", "w") as file:
        json.dump(json_data, file)

    data.to_pickle(f"./data/02_intermediate/{table_name}.pkl")

    return json_data