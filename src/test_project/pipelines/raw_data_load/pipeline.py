"""
This is a boilerplate pipeline 'raw_data_load'
generated using Kedro 0.17.6
"""

from json import load
from kedro.pipeline import Pipeline, node
from kedro.pipeline.modular_pipeline import pipeline
from kedro.config import ConfigLoader
from .nodes import (
    create_descriptives
)

def base_pipeline():

    return Pipeline(
        [
            node(
                func = create_descriptives,
                inputs = ["data", "parameters"],
                outputs = "json_data",
                name = "create_descriptives"
            )
        ]
    )

def create_pipeline(**kwargs) -> Pipeline:

    conf_paths = ["conf/base", "conf/local"]
    conf_loader = ConfigLoader(conf_paths)
    sql_data = conf_loader.get("parameters*", "parameters*/**")

    table_pipelines = [
        pipeline(
            pipe = base_pipeline(),
            parameters = {
                "parameters": f"params:{table_name}"
            },
            inputs = {
                "data": f"{table_name}"
            },
            outputs = {
                "json_data": f"json_{table_name}"
            },
            namespace = table_name,
        )
        for table_name in sql_data
    ]

    return Pipeline(table_pipelines)
