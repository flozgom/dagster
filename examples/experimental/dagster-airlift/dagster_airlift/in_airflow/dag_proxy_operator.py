import json
import os
from typing import Any, Iterable, Mapping, Sequence

import requests
from airflow import DAG
from airflow.utils.context import Context

from dagster_airlift.constants import DAG_MAPPING_METADATA_KEY
from dagster_airlift.in_airflow.base_asset_operator import BaseDagsterAssetsOperator


class BaseProxyDAGToDagsterOperator(BaseDagsterAssetsOperator):
    """An operator that proxies task execution to Dagster assets with metadata that map to this task's dag ID and task ID."""

    def filter_asset_nodes(
        self, context: Context, asset_nodes: Sequence[Mapping[str, Any]]
    ) -> Iterable[Mapping[str, Any]]:
        for asset_node in asset_nodes:
            if matched_dag_id(asset_node, self.get_airflow_dag_id(context)):
                yield asset_node


class DefaultProxyDAGToDagsterOperator(BaseProxyDAGToDagsterOperator):
    """The default task proxying operator - which opens a blank session and expects the dagster URL to be set in the environment.
    The dagster url is expected to be set in the environment as DAGSTER_URL.
    """

    def get_dagster_session(self, context: Context) -> requests.Session:
        return requests.Session()

    def get_dagster_url(self, context: Context) -> str:
        return os.environ["DAGSTER_URL"]


def build_dag_level_proxied_task(dag: DAG) -> DefaultProxyDAGToDagsterOperator:
    return DefaultProxyDAGToDagsterOperator(
        task_id=f"DAGSTER_OVERRIDE_DAG_{dag.dag_id}",
        dag=dag,
    )


def matched_dag_id(asset_node: Mapping[str, Any], dag_id: str) -> bool:
    json_metadata_entries = {
        entry["label"]: entry["jsonString"]
        for entry in asset_node["metadataEntries"]
        if entry["__typename"] == "JsonMetadataEntry"
    }

    if mapping_entry := json_metadata_entries.get(DAG_MAPPING_METADATA_KEY):
        dag_ids = json.loads(mapping_entry)
        return dag_id in dag_ids
    return False
