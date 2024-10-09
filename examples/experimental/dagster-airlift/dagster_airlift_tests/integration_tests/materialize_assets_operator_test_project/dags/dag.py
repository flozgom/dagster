import logging
from datetime import datetime

from airflow import DAG
from dagster_airlift.in_airflow.materialize_assets_operator import BlankSessionAssetsOperator

logging.basicConfig()
logging.getLogger().setLevel(logging.INFO)
requests_log = logging.getLogger("requests.packages.urllib3")
requests_log.setLevel(logging.INFO)
requests_log.propagate = True


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
}

dag = DAG(
    "the_dag",
    default_args=default_args,
    schedule_interval=None,
    is_paused_upon_creation=False,
    start_date=datetime(2023, 1, 1),
)
the_task = BlankSessionAssetsOperator(
    # Test both string syntax and list of strings syntax.
    task_id="some_task",
    dag=dag,
    asset_key_paths=["some_asset", ["other_asset"]],
)
