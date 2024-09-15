from pathlib import Path
from typing import Sequence

from dagster import (
    AssetKey,
    AssetsDefinition,
    AssetSpec,
    Definitions,
    JsonMetadataValue,
    SourceAsset,
    asset,
    asset_check,
    executor,
    job,
    logger,
    multi_asset,
    schedule,
    sensor,
)
from dagster._core.definitions.cacheable_assets import (
    AssetsDefinitionCacheableData,
    CacheableAssetsDefinition,
)
from dagster._core.test_utils import environ
from dagster_airlift.constants import AIRFLOW_COUPLING_METADATA_KEY
from dagster_airlift.core import (
    build_defs_from_airflow_instance,
    dag_defs,
    mark_as_shared,
    task_defs,
)
from dagster_airlift.test import make_instance
from dagster_airlift.utils import DAGSTER_AIRLIFT_MIGRATION_STATE_DIR_ENV_VAR

from dagster_airlift_tests.unit_tests.conftest import (
    assert_dependency_structure_in_assets,
    build_definitions_airflow_asset_graph,
    fully_loaded_repo_from_airflow_asset_graph,
)


@executor
def nonstandard_executor(init_context):
    pass


@logger
def nonstandard_logger(init_context):
    pass


@sensor(job_name="the_job")
def some_sensor():
    pass


@schedule(cron_schedule="0 0 * * *", job_name="the_job")
def some_schedule():
    pass


@asset
def a():
    pass


b_spec = AssetSpec(key="b")


@asset_check(asset=a)
def a_check():
    pass


@job
def the_job():
    pass


def test_defs_passthrough() -> None:
    """Test that passed-through definitions are present in the final definitions."""
    defs = build_defs_from_airflow_instance(
        airflow_instance=make_instance({"dag": ["task"]}),
        dag_defs_list=[
            Definitions(
                assets=[a, b_spec],
                asset_checks=[a_check],
                jobs=[the_job],
                sensors=[some_sensor],
                schedules=[some_schedule],
                loggers={"the_logger": nonstandard_logger},
                executor=nonstandard_executor,
            )
        ],
    )
    assert defs.executor == nonstandard_executor
    assert defs.loggers
    assert len(defs.loggers) == 1
    assert next(iter(defs.loggers.keys())) == "the_logger"
    assert defs.sensors
    assert len(list(defs.sensors)) == 2
    our_sensor = next(
        iter(sensor_def for sensor_def in defs.sensors if sensor_def.name == "some_sensor")
    )
    assert our_sensor == some_sensor
    assert defs.schedules
    assert len(list(defs.schedules)) == 1
    assert next(iter(defs.schedules)) == some_schedule
    assert defs.jobs
    assert len(list(defs.jobs)) == 1
    assert next(iter(defs.jobs)) == the_job
    repo = defs.get_repository_def()
    # Ensure that asset specs get properly coerced into asset defs
    assert set(repo.assets_defs_by_key.keys()) == {
        a.key,
        b_spec.key,
        AssetKey(["airflow_instance", "dag", "dag"]),
    }
    assert isinstance(repo.assets_defs_by_key[b_spec.key], AssetsDefinition)


def test_coerce_specs() -> None:
    """Test that asset specs are properly coerced into asset keys."""
    # Initialize an airflow instance with a dag "dag", which contains a task "task". There are no task instances or runs.

    spec = AssetSpec(
        key="a", metadata={AIRFLOW_COUPLING_METADATA_KEY: JsonMetadataValue([("dag", "task")])}
    )
    defs = build_defs_from_airflow_instance(
        airflow_instance=make_instance({"dag": ["task"]}),
        dag_defs_list=[
            Definitions(
                assets=[spec],
            )
        ],
    )
    repo = defs.get_repository_def()
    assert len(repo.assets_defs_by_key) == 2
    assert AssetKey("a") in repo.assets_defs_by_key
    assets_def = repo.assets_defs_by_key[AssetKey("a")]
    # Asset metadata properties have been glommed onto the asset
    assert next(iter(assets_def.specs)).metadata["Dag ID"] == "dag"


def test_invalid_dagster_named_tasks_and_dags() -> None:
    """Test that invalid dagster names are converted to valid names."""
    a = AssetKey("a")
    spec = AssetSpec(
        key=a,
        metadata={
            AIRFLOW_COUPLING_METADATA_KEY: JsonMetadataValue(
                [("dag-with-hyphens", "task-with-hyphens")]
            )
        },
    )
    defs = build_defs_from_airflow_instance(
        airflow_instance=make_instance({"dag-with-hyphens": ["task-with-hyphens"]}),
        dag_defs_list=[
            Definitions(
                assets=[spec],
            )
        ],
    )

    repo = defs.get_repository_def()
    assert len(repo.assets_defs_by_key) == 2
    assert a in repo.assets_defs_by_key
    assets_def = repo.assets_defs_by_key[a]
    assert not assets_def.is_executable

    assert AssetKey(["airflow_instance", "dag", "dag_with_hyphens"]) in repo.assets_defs_by_key
    dag_def = repo.assets_defs_by_key[AssetKey(["airflow_instance", "dag", "dag_with_hyphens"])]
    assert not dag_def.is_executable


def test_transitive_asset_deps() -> None:
    """Test that cross-dag transitive asset dependencies are correctly generated."""
    # Asset graph is a -> b -> c where a and c are in different dags, and b isn't in any dag.
    repo_def = fully_loaded_repo_from_airflow_asset_graph(
        assets_per_task={
            "dag1": {"task": [("a", [])]},
            "dag2": {"task": [("c", ["b"])]},
        },
        additional_defs=Definitions(assets=[AssetSpec(key="b", deps=["a"])]),
    )
    repo_def.load_all_definitions()
    dag1_key = AssetKey(["airflow_instance", "dag", "dag1"])
    dag2_key = AssetKey(["airflow_instance", "dag", "dag2"])
    a_key = AssetKey(["a"])
    b_key = AssetKey(["b"])
    c_key = AssetKey(["c"])
    assert len(repo_def.assets_defs_by_key) == 5
    assert set(repo_def.assets_defs_by_key.keys()) == {
        dag1_key,
        dag2_key,
        a_key,
        b_key,
        c_key,
    }
    dag1_asset = repo_def.assets_defs_by_key[dag1_key]
    assert [dep.asset_key for dep in next(iter(dag1_asset.specs)).deps] == [a_key]
    dag2_asset = repo_def.assets_defs_by_key[dag2_key]
    assert [dep.asset_key for dep in next(iter(dag2_asset.specs)).deps] == [c_key]
    a_asset = repo_def.assets_defs_by_key[a_key]
    assert [dep.asset_key for dep in next(iter(a_asset.specs)).deps] == []
    assert "airlift/dag_id" in next(iter(a_asset.specs)).metadata
    assert next(iter(a_asset.specs)).metadata["airlift/dag_id"] == "dag1"
    assert "airlift/task_id" in next(iter(a_asset.specs)).metadata
    assert next(iter(a_asset.specs)).metadata["airlift/task_id"] == "task"

    b_asset = repo_def.assets_defs_by_key[b_key]
    assert [dep.asset_key for dep in next(iter(b_asset.specs)).deps] == [a_key]
    assert "airlift/dag_id" not in next(iter(b_asset.specs)).metadata
    assert "airlift/task_id" not in next(iter(b_asset.specs)).metadata
    c_asset = repo_def.assets_defs_by_key[c_key]
    assert [dep.asset_key for dep in next(iter(c_asset.specs)).deps] == [b_key]
    assert "airlift/dag_id" in next(iter(c_asset.specs)).metadata
    assert next(iter(c_asset.specs)).metadata["airlift/dag_id"] == "dag2"
    assert "airlift/task_id" in next(iter(c_asset.specs)).metadata
    assert next(iter(c_asset.specs)).metadata["airlift/task_id"] == "task"


def test_peered_dags() -> None:
    """Test peered dags show up, and that linkage is preserved downstream of dags."""
    defs = build_definitions_airflow_asset_graph(
        assets_per_task={
            "dag1": {"task": []},
            "dag2": {"task": []},
            "dag3": {"task": []},
        },
        additional_defs=Definitions(
            assets=[
                # We actually rely on b and airflow_instance/dag/dag1 are initially loaded as separate definitions objects. If
                # they were loaded as part of the same definitions object, we would not correctly disregard the stub asset for the dag.
                AssetSpec(
                    key="a",
                    deps=[AssetKey.from_user_string("airflow_instance/dag/dag1"), AssetKey("b")],
                ),
            ]
        ),
    )
    assert defs.assets
    # Assets are not loaded until repository is loaded
    assert len(list(defs.assets)) == 1
    repo_def = defs.get_repository_def()
    repo_def.load_all_definitions()
    assert len(repo_def.assets_defs_by_key) == 5
    assert_dependency_structure_in_assets(
        repo_def=repo_def,
        expected_deps={
            "airflow_instance/dag/dag1": [],
            "airflow_instance/dag/dag2": [],
            "airflow_instance/dag/dag3": [],
            "a": ["airflow_instance/dag/dag1", "b"],
            "b": [],
        },
    )


def test_observed_assets() -> None:
    """Test that observed assets are properly linked to dags."""
    # Asset graph structure:
    #   a
    #  / \
    # b   c
    #  \ /
    #   d
    #  / \
    # e   f
    defs = build_definitions_airflow_asset_graph(
        assets_per_task={
            "dag": {
                "task1": [("a", []), ("b", ["a"]), ("c", ["a"])],
                "task2": [("d", ["b", "c"]), ("e", ["d"]), ("f", ["d"])],
            },
        },
    )
    assert defs.assets
    assert len(list(defs.assets)) == 1
    repo_def = defs.get_repository_def()
    repo_def.load_all_definitions()
    repo_def.load_all_definitions()
    assert len(repo_def.assets_defs_by_key) == 7
    assert_dependency_structure_in_assets(
        repo_def=repo_def,
        expected_deps={
            "a": [],
            "b": ["a"],
            "c": ["a"],
            "d": ["b", "c"],
            "e": ["d"],
            "f": ["d"],
            # Only leaf assets should be immediately upstream of the dag
            "airflow_instance/dag/dag": ["e", "f"],
        },
    )


def test_local_airflow_instance() -> None:
    """Test that a local-backed airflow instance can be correctly peered, and errors when the correct info can't be found."""
    defs = build_definitions_airflow_asset_graph(
        assets_per_task={
            "dag": {"task": [("a", [])]},
        },
        create_assets_defs=True,
    )

    assert defs.assets
    assert len(list(defs.assets)) == 1
    repo_def = defs.get_repository_def()
    a_asset = repo_def.assets_defs_by_key[AssetKey("a")]
    assert next(iter(a_asset.specs)).tags.get("airlift/task_migrated") == "False"

    with environ(
        {
            DAGSTER_AIRLIFT_MIGRATION_STATE_DIR_ENV_VAR: str(
                Path(__file__).parent / "migration_state_for_sqlite_test"
            ),
        }
    ):
        defs = build_definitions_airflow_asset_graph(
            assets_per_task={
                "dag": {"task": [("a", [])]},
            },
            create_assets_defs=True,
        )
        repo_def = defs.get_repository_def()
        assert defs.assets
        assert len(list(defs.assets)) == 1
        repo_def = defs.get_repository_def()
        assert len(repo_def.assets_defs_by_key) == 2
        task_asset = repo_def.assets_defs_by_key[AssetKey("a")]
        assert next(iter(task_asset.specs)).tags.get("airlift/task_migrated") == "True"


def test_multi_task_mapping() -> None:
    """Test that we can map the same asset to multiple tasks."""
    instance = make_instance({"dag1": ["task1"], "dag2": ["task2"]})
    defs = build_defs_from_airflow_instance(
        airflow_instance=instance,
        dag_defs_list=[
            dag_defs(
                "dag1",
                [task_defs("task1", mark_as_shared("a"))],
            ),
            dag_defs(
                "dag2",
                [task_defs("task2", mark_as_shared("a"))],
            ),
        ],
        shared_task_defs={"a": Definitions(assets=[AssetSpec(key="a")])},
    )
    repo_def = defs.get_repository_def()
    assert len(repo_def.assets_defs_by_key) == 3
    assert {a for a in repo_def.assets_defs_by_key.keys()} == {
        AssetKey(["airflow_instance", "dag", "dag1"]),
        AssetKey(["airflow_instance", "dag", "dag2"]),
        AssetKey("a"),
    }


def test_provide_cacheable_asset_to_build_defs() -> None:
    """Tests that a cacheable asset can be successfully provided to build_defs."""
    compute_count = [0]
    build_count = [0]

    class MyCacheableAsset(CacheableAssetsDefinition):
        def compute_cacheable_data(self) -> Sequence[AssetsDefinitionCacheableData]:
            compute_count[0] += 1
            return []

        def build_definitions(
            self, data: Sequence[AssetsDefinitionCacheableData]
        ) -> Sequence[AssetsDefinition]:
            build_count[0] += 1

            @multi_asset(
                specs=[
                    AssetSpec(
                        key="a",
                        metadata={"unrelated": "value"},
                    )
                ]
            )
            def my_asset():
                pass

            return [my_asset]

    my_cacheable = MyCacheableAsset("blah")
    defs = build_defs_from_airflow_instance(
        airflow_instance=make_instance({"dag": ["task"]}),
        dag_defs_list=[
            dag_defs(
                "dag",
                [task_defs("task", Definitions(assets=[my_cacheable]))],
            ),
        ],
    )
    repo_def = defs.get_repository_def()
    # Ensure that we didn't somehow re-trigger computation of the underlying cacheable asset multiple times.
    assert compute_count[0] == 1
    assert build_count[0] == 1
    assert len(repo_def.assets_defs_by_key) == 2
    a_asset = repo_def.assets_defs_by_key[AssetKey("a")]
    assert len(list(a_asset.specs)) == 1
    a_metadata = next(iter(a_asset.specs)).metadata
    assert a_metadata["unrelated"] == "value"
    assert a_metadata[AIRFLOW_COUPLING_METADATA_KEY] == JsonMetadataValue([["dag", "task"]])
    assert a_metadata["Computed in Task ID"] == "task"
    dag_asset = repo_def.assets_defs_by_key[AssetKey(["airflow_instance", "dag", "dag"])]
    assert len(list(dag_asset.specs)) == 1
    dag_spec = next(iter(dag_asset.specs))
    assert [dep.asset_key for dep in dag_spec.deps] == [AssetKey("a")]


def test_provide_source_assets_to_build_defs() -> None:
    source1 = SourceAsset(key="source1")
    source2 = SourceAsset(key="source2")
    my_spec_with_deps = AssetSpec(key="has_sources", deps=[source1.key, source2.key])

    # First, test sources not mapped to a task.
    defs = build_defs_from_airflow_instance(
        airflow_instance=make_instance({"dag": ["task"]}),
        dag_defs_list=[
            dag_defs(
                "dag",
                [task_defs("task", Definitions(assets=[my_spec_with_deps]))],
            ),
            Definitions(assets=[source1, source2]),
        ],
    )
    repo_def = defs.get_repository_def()
    has_sources_asset = repo_def.assets_defs_by_key[AssetKey("has_sources")]
    assert len(list(has_sources_asset.specs)) == 1
    spec = next(iter(has_sources_asset.specs))
    assert [dep.asset_key for dep in spec.deps] == [source1.key, source2.key]

    # Now, test sources mapped to a task.
    defs = build_defs_from_airflow_instance(
        airflow_instance=make_instance({"dag": ["task"]}),
        dag_defs_list=[
            dag_defs(
                "dag",
                [task_defs("task", Definitions(assets=[my_spec_with_deps, source1, source2]))],
            ),
        ],
    )

    repo_def = defs.get_repository_def()
    source1_asset = repo_def.assets_defs_by_key[source1.key]
    assert len(list(source1_asset.specs)) == 1
    assert next(iter(source1_asset.specs)).deps == []
    assert next(iter(source1_asset.specs)).metadata[
        DAG_AND_TASK_ID_METADATA_KEY
    ] == JsonMetadataValue([["dag", "task"]])
    source2_asset = repo_def.assets_defs_by_key[source2.key]
    assert len(list(source2_asset.specs)) == 1
    assert next(iter(source2_asset.specs)).deps == []
    assert next(iter(source2_asset.specs)).metadata[
        DAG_AND_TASK_ID_METADATA_KEY
    ] == JsonMetadataValue([["dag", "task"]])
    dag_asset = repo_def.assets_defs_by_key[AssetKey(["airflow_instance", "dag", "dag"])]
    assert len(list(dag_asset.specs)) == 1
    assert [dep.asset_key for dep in next(iter(dag_asset.specs)).deps] == [has_sources_asset.key]
