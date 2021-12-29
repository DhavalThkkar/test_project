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
                inputs = ["data", "table_name"],
                outputs = ["json_data", "html_data", "output_data"],
                name = "create_descriptives"
            )
        ]
    )

def create_pipeline(**kwargs) -> Pipeline:

    conf_paths = ["conf/base", "conf/local"]
    conf_loader = ConfigLoader(conf_paths)
    parameters = conf_loader.get("parameters*", "parameters*/**")
    
    sql_data = parameters["tables"]

    table_pipelines = [
        pipeline(
            pipe = base_pipeline(),
            inputs = {
                "data": f"{table_name}", 
                "table_name": str(f"{table_name}"),
            },
            outputs = {
                "json_data": f"{table_name}.json_profile",
                "html_data": f"{table_name}.html_profile",
                "output_data" : f"{table_name}.pickled"
            },
            namespace = table_name,
        )
        for table_name in sql_data
    ]
    
    # Second level of namespace nesting!
    return pipeline(sum(table_pipelines), namespace='profiling')
