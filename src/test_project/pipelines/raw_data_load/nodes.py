"""
This is a boilerplate pipeline 'raw_data_load'
generated using Kedro 0.17.6
"""

from typing import Any, Dict, List, Tuple
from pandas_profiling import ProfileReport

import pandas as pd


def create_descriptives(
    data: pd.DataFrame, table_name: str
) -> Tuple[Dict[str, Any], str, pd.DataFrame]:

    profile = ProfileReport(
        data, title=f"{table_name} Profiling Report", config_file="config_minimal.yml"
    )

    profile_json = profile.to_json()
    profile_html_string = profile.to_html()

    return profile_json, profile_html_string, data
