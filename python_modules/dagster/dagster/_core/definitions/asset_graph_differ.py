from enum import Enum
from typing import (
    TYPE_CHECKING,
    AbstractSet,
    Callable,
    List,
    Mapping,
    Optional,
    Sequence,
    Tuple,
    Union,
)

from typing_extensions import Annotated

import dagster._check as check
from dagster._core.definitions.remote_asset_graph import RemoteAssetGraph
from dagster._core.errors import DagsterInvariantViolationError
from dagster._core.remote_representation import ExternalRepository
from dagster._core.workspace.context import BaseWorkspaceRequestContext
from dagster._record import ImportFrom, record
from dagster._serdes import whitelist_for_serdes

if TYPE_CHECKING:
    from dagster._core.definitions.events import AssetKey


@whitelist_for_serdes
class AssetDefinitionChangeType(Enum):
    """What change an asset has undergone between two deployments. Used
    in distinguishing asset definition changes in branch deployment and
    in subsequent other deployments.
    """

    NEW = "NEW"
    CODE_VERSION = "CODE_VERSION"
    DEPENDENCIES = "DEPENDENCIES"
    PARTITIONS_DEFINITION = "PARTITIONS_DEFINITION"
    TAGS = "TAGS"
    METADATA = "METADATA"
    REMOVED = "REMOVED"


class AssetDefinitionChangeData:
    """Base interface for classes which
    hold data about asset definition changes.
    """

    pass


@whitelist_for_serdes
@record
class CodeVersionChangeData(AssetDefinitionChangeData):
    base_code_version: Optional[str]
    branch_code_version: Optional[str]


@whitelist_for_serdes
@record
class DependenciesChangeData(AssetDefinitionChangeData):
    base_dependencies: AbstractSet[
        Annotated["AssetKey", ImportFrom("dagster._core.definitions.events")]
    ]
    branch_dependencies: AbstractSet[
        Annotated["AssetKey", ImportFrom("dagster._core.definitions.events")]
    ]


@whitelist_for_serdes
@record
class TagsChangeData(AssetDefinitionChangeData):
    base_tags: Mapping[str, str]
    branch_tags: Mapping[str, str]


@whitelist_for_serdes
@record
class MetadataChangeData(AssetDefinitionChangeData):
    added_keys: AbstractSet[str]
    modified_keys: AbstractSet[str]
    removed_keys: AbstractSet[str]


def _get_external_repo_from_context(
    context: BaseWorkspaceRequestContext, code_location_name: str, repository_name: str
) -> Optional[ExternalRepository]:
    """Returns the ExternalRepository specified by the code location name and repository name
    for the provided workspace context. If the repository doesn't exist, return None.
    """
    if context.has_code_location(code_location_name):
        cl = context.get_code_location(code_location_name)
        if cl.has_repository(repository_name):
            return cl.get_repository(repository_name)


class AssetGraphDiffer:
    """Given two asset graphs, base_asset_graph and branch_asset_graph, we can compute how the
    assets in branch_asset_graph have changed with respect to base_asset_graph. The ChangeReason
    enum contains the list of potential changes an asset can undergo. If the base_asset_graph is None,
    this indicates that the branch_asset_graph does not yet exist in the base deployment. In this case
    we will consider every asset New.
    """

    _branch_asset_graph: Optional["RemoteAssetGraph"]
    _branch_asset_graph_load_fn: Optional[Callable[[], "RemoteAssetGraph"]]
    _base_asset_graph: Optional["RemoteAssetGraph"]
    _base_asset_graph_load_fn: Optional[Callable[[], "RemoteAssetGraph"]]

    def __init__(
        self,
        branch_asset_graph: Union["RemoteAssetGraph", Callable[[], "RemoteAssetGraph"]],
        base_asset_graph: Optional[
            Union["RemoteAssetGraph", Callable[[], "RemoteAssetGraph"]]
        ] = None,
    ):
        if base_asset_graph is None:
            # if base_asset_graph is None, then the asset graph in the branch deployment does not exist
            # in the base deployment
            self._base_asset_graph = None
            self._base_asset_graph_load_fn = None
        elif isinstance(base_asset_graph, RemoteAssetGraph):
            self._base_asset_graph = base_asset_graph
            self._base_asset_graph_load_fn = None
        else:
            self._base_asset_graph = None
            self._base_asset_graph_load_fn = base_asset_graph

        if isinstance(branch_asset_graph, RemoteAssetGraph):
            self._branch_asset_graph = branch_asset_graph
            self._branch_asset_graph_load_fn = None
        else:
            self._branch_asset_graph = None
            self._branch_asset_graph_load_fn = branch_asset_graph

    @classmethod
    def from_external_repositories(
        cls,
        code_location_name: str,
        repository_name: str,
        branch_workspace: BaseWorkspaceRequestContext,
        base_workspace: BaseWorkspaceRequestContext,
    ) -> "AssetGraphDiffer":
        """Constructs an AssetGraphDiffer for a particular repository in a code location for two
        deployment workspaces, the base deployment and the branch deployment.

        We cannot make RemoteAssetGraphs directly from the workspaces because if multiple code locations
        use the same asset key, those asset keys will override each other in the dictionaries the RemoteAssetGraph
        creates (see from_repository_handles_and_external_asset_nodes in RemoteAssetGraph). We need to ensure
        that we are comparing assets in the same code location and repository, so we need to make the
        RemoteAssetGraph from an ExternalRepository to ensure that there are no duplicate asset keys
        that could override each other.
        """
        check.inst_param(branch_workspace, "branch_workspace", BaseWorkspaceRequestContext)
        check.inst_param(base_workspace, "base_workspace", BaseWorkspaceRequestContext)

        branch_repo = _get_external_repo_from_context(
            branch_workspace, code_location_name, repository_name
        )
        if branch_repo is None:
            raise DagsterInvariantViolationError(
                f"Repository {repository_name} does not exist in code location {code_location_name} for the branch deployment."
            )
        base_repo = _get_external_repo_from_context(
            base_workspace, code_location_name, repository_name
        )
        return AssetGraphDiffer(
            branch_asset_graph=lambda: branch_repo.asset_graph,
            base_asset_graph=(lambda: base_repo.asset_graph) if base_repo is not None else None,
        )

    def _compare_base_and_branch_assets(
        self, asset_key: "AssetKey", record_changes: bool
    ) -> Sequence[Tuple[AssetDefinitionChangeType, Optional[AssetDefinitionChangeData]]]:
        """Computes the diff between a branch deployment asset and the
        corresponding base deployment asset.

        Args:
            asset_key (AssetKey): The asset key to compare.
            record_changes (bool): Whether to record the actual changes alongside the change types.
                If False, the AssetDefinitionChangeData will be None.

        Returns:
            List[Tuple[ChangeReason, Optional[AssetDefinitionChangeData]]]: List of tuples where the first element
                is the change type and the second element is the change data, if requested.
        """
        if self.base_asset_graph is None:
            # if the base asset graph is None, it is because the asset graph in the branch deployment
            # is new and doesn't exist in the base deployment. Thus all assets are new.
            return [(AssetDefinitionChangeType.NEW, None)]

        if asset_key not in self.base_asset_graph.all_asset_keys:
            return [(AssetDefinitionChangeType.NEW, None)]

        if asset_key not in self.branch_asset_graph.all_asset_keys:
            return [(AssetDefinitionChangeType.REMOVED, None)]

        branch_asset = self.branch_asset_graph.get(asset_key)
        base_asset = self.base_asset_graph.get(asset_key)

        changes: List[Tuple[AssetDefinitionChangeType, Optional[AssetDefinitionChangeData]]] = []
        if branch_asset.code_version != base_asset.code_version:
            data = None
            if record_changes:
                data = CodeVersionChangeData(
                    base_code_version=base_asset.code_version,
                    branch_code_version=branch_asset.code_version,
                )
            changes.append((AssetDefinitionChangeType.CODE_VERSION, data))

        if branch_asset.parent_keys != base_asset.parent_keys:
            data = None
            if record_changes:
                data = DependenciesChangeData(
                    base_dependencies=(base_asset.parent_keys),
                    branch_dependencies=(branch_asset.parent_keys),
                )
            changes.append((AssetDefinitionChangeType.DEPENDENCIES, data))
        else:
            # if the set of upstream dependencies is different, then we don't need to check if the partition mappings
            # for dependencies have changed since ChangeReason.DEPENDENCIES is already in the list of changes
            for upstream_asset in branch_asset.parent_keys:
                if self.branch_asset_graph.get_partition_mapping(
                    asset_key, upstream_asset
                ) != self.base_asset_graph.get_partition_mapping(asset_key, upstream_asset):
                    changes.append((AssetDefinitionChangeType.DEPENDENCIES, None))
                    break

        if branch_asset.partitions_def != base_asset.partitions_def:
            changes.append((AssetDefinitionChangeType.PARTITIONS_DEFINITION, None))

        if branch_asset.tags != base_asset.tags:
            data = None
            if record_changes:
                data = TagsChangeData(base_tags=base_asset.tags, branch_tags=branch_asset.tags)
            changes.append((AssetDefinitionChangeType.TAGS, data))

        branch_keys = set(branch_asset.metadata.keys())
        base_keys = set(base_asset.metadata.keys())

        new_keys = branch_keys - base_keys
        removed_keys = base_keys - branch_keys
        key_overlap = branch_keys.intersection(base_keys)
        keys_changed = {
            k for k in key_overlap if branch_asset.metadata[k] != base_asset.metadata[k]
        }
        if new_keys or removed_keys or keys_changed:
            data = None
            if record_changes:
                data = MetadataChangeData(
                    added_keys=new_keys,
                    modified_keys=keys_changed,
                    removed_keys=removed_keys,
                )
            changes.append((AssetDefinitionChangeType.METADATA, data))

        return changes

    def get_changes_for_asset(self, asset_key: "AssetKey") -> Sequence[AssetDefinitionChangeType]:
        """Returns list of ChangeReasons for asset_key as compared to the base deployment."""
        return [
            change_type
            for change_type, _ in self._compare_base_and_branch_assets(
                asset_key, record_changes=False
            )
        ]

    def get_changes_for_asset_with_data(
        self, asset_key: "AssetKey"
    ) -> Sequence[Tuple[AssetDefinitionChangeType, Optional[AssetDefinitionChangeData]]]:
        """Returns list of ChangeReasons and corresponding change data for asset_key as compared to the base deployment."""
        return self._compare_base_and_branch_assets(asset_key, record_changes=True)

    @property
    def branch_asset_graph(self) -> "RemoteAssetGraph":
        if self._branch_asset_graph is None:
            self._branch_asset_graph = check.not_none(self._branch_asset_graph_load_fn)()
        return self._branch_asset_graph

    @property
    def base_asset_graph(self) -> Optional["RemoteAssetGraph"]:
        if self._base_asset_graph is None and self._base_asset_graph_load_fn is not None:
            self._base_asset_graph = self._base_asset_graph_load_fn()
        return self._base_asset_graph
