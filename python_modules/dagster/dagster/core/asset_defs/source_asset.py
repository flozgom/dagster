from typing import NamedTuple, Optional, Sequence, Union

import dagster._check as check
from dagster.core.definitions.events import AssetKey, CoerceableToAssetKey
from dagster.core.definitions.metadata import (
    MetadataEntry,
    MetadataMapping,
    MetadataUserInput,
    PartitionMetadataEntry,
    normalize_metadata,
)
from dagster.core.definitions.partition import PartitionsDefinition


class SourceAsset(
    NamedTuple(
        "_SourceAsset",
        [
            ("key", AssetKey),
            ("metadata_entries", Sequence[Union[MetadataEntry, PartitionMetadataEntry]]),
            ("io_manager_key", str),
            ("description", Optional[str]),
            ("partitions_def", Optional[PartitionsDefinition]),
        ],
    )
):
    """A SourceAsset represents an asset that will be loaded by (but not updated by) Dagster.

    Attributes:
        key (Union[AssetKey, Sequence[str], str]): The key of the asset.
        metadata_entries (List[MetadataEntry]): Metadata associated with the asset.
        io_manager_key (str): The key for the IOManager that will be used to load the contents of
            the asset when it's used as an input to other assets inside a job.
        description (Optional[str]): The description of the asset.
        partitions_def (Optional[PartitionsDefinition]): Defines the set of partition keys that
            compose the asset.
    """

    def __new__(
        cls,
        key: CoerceableToAssetKey,
        metadata: Optional[MetadataUserInput] = None,
        io_manager_key: str = "io_manager",
        description: Optional[str] = None,
        partitions_def: Optional[PartitionsDefinition] = None,
    ):

        metadata = check.opt_dict_param(metadata, "metadata", key_type=str)
        metadata_entries = normalize_metadata(metadata, [], allow_invalid=True)
        return super().__new__(
            cls,
            key=AssetKey.from_coerceable(key),
            metadata_entries=metadata_entries,
            io_manager_key=check.str_param(io_manager_key, "io_manager_key"),
            description=check.opt_str_param(description, "description"),
            partitions_def=check.opt_inst_param(
                partitions_def, "partitions_def", PartitionsDefinition
            ),
        )

    @property
    def metadata(self) -> MetadataMapping:
        # PartitionMetadataEntry (unstable API) case is unhandled
        return {entry.label: entry.entry_data for entry in self.metadata_entries}  # type: ignore
