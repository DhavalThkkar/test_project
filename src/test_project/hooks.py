"""Project hooks."""
from typing import Any, Dict, Iterable, Optional

from kedro.config import ConfigLoader
from kedro.framework.hooks import hook_impl
from kedro.io import DataCatalog
from kedro.versioning import Journal

from kedro.extras.datasets.pickle import PickleDataSet, pickle_dataset
from kedro.extras.datasets.json import JSONDataSet
from kedro.extras.datasets.text import TextDataSet


class ProjectHooks:
    @hook_impl
    def register_config_loader(
        self, conf_paths: Iterable[str], env: str, extra_params: Dict[str, Any],
    ) -> ConfigLoader:
        return ConfigLoader(conf_paths)

    @hook_impl
    def register_catalog(
        self,
        catalog: Optional[Dict[str, Dict[str, Any]]],
        credentials: Dict[str, Dict[str, Any]],
        load_versions: Dict[str, str],
        save_version: str,
        journal: Journal,
    ) -> DataCatalog:
        return DataCatalog.from_config(
            catalog, credentials, load_versions, save_version, journal
        )


class ProfilingHooks:

    @hook_impl
    def after_catalog_created(self, catalog, conf_catalog, conf_creds, feed_dict, save_version, load_versions, run_id):
        """This is an advanced use of the catalog hook so that we create the right
        catalog entries at runtime based on the inputs to the `params:tables`. The alternative
        to this is that your non-technical users would have to create the three output
        dataset entries in the catalog for every input they declare
        """

        profiling_output_tables = feed_dict['params:tables']
        folder = 'data/02_intermediate'

        for table in profiling_output_tables:
            new_entries = {
                f"{table}.html_profile" : TextDataSet(f"{folder}/{table}.html"),
                f"{table}.json_profile" : JSONDataSet(f"{folder}/{table}.json"),
                f"{table}.pickled" : PickleDataSet(f"{folder}/{table}.pkl")
            }
            catalog.add_all(new_entries, replace=True)