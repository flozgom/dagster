from abc import ABC, abstractmethod
from typing import NamedTuple, Optional, Sequence

from dagster._core.host_representation.origin import (
    GrpcServerRepositoryLocationOrigin,
    RepositoryLocationOrigin,
)

from dagster._seven import import_module_from_path

from .load import (
    location_origin_from_module_name,
    location_origin_from_package_name,
    location_origin_from_python_file,
    location_origins_from_yaml_paths,
)


class WorkspaceLoadTarget(ABC):
    @abstractmethod
    def create_origins(self) -> Sequence[RepositoryLocationOrigin]:
        """Reloads the RepositoryLocationOrigins for this workspace."""


from dataclasses import dataclass
from typing import Optional


@dataclass
class Project:
    python_package: Optional[str]


class ProjectPythonFileTarget(
    NamedTuple("ProjectLoadTarget", [("path", str)]), WorkspaceLoadTarget
):
    def create_origins(self):
        module = import_module_from_path("__dagster_project", self.path)
        project = module.__dict__["project"]
        return [
            location_origin_from_package_name(
                project.python_package,
                attribute=None,
                working_directory=None,
                location_name=None,
                executable_path=None,
            )
        ]


class WorkspaceFileTarget(
    NamedTuple("WorkspaceFileTarget", [("paths", Sequence[str])]), WorkspaceLoadTarget
):
    def create_origins(self):
        return location_origins_from_yaml_paths(self.paths)


class PythonFileTarget(
    NamedTuple(
        "PythonFileTarget",
        [
            ("python_file", str),
            ("attribute", Optional[str]),
            ("working_directory", Optional[str]),
            ("location_name", Optional[str]),
        ],
    ),
    WorkspaceLoadTarget,
):
    def create_origins(self):
        return [
            location_origin_from_python_file(
                python_file=self.python_file,
                attribute=self.attribute,
                working_directory=self.working_directory,
                location_name=self.location_name,
            )
        ]


class ModuleTarget(
    NamedTuple(
        "ModuleTarget",
        [
            ("module_name", str),
            ("attribute", Optional[str]),
            ("working_directory", Optional[str]),
            ("location_name", Optional[str]),
        ],
    ),
    WorkspaceLoadTarget,
):
    def create_origins(self):
        return [
            location_origin_from_module_name(
                self.module_name,
                self.attribute,
                self.working_directory,
                location_name=self.location_name,
            )
        ]


class PackageTarget(
    NamedTuple(
        "ModuleTarget",
        [
            ("package_name", str),
            ("attribute", Optional[str]),
            ("working_directory", Optional[str]),
            ("location_name", Optional[str]),
        ],
    ),
    WorkspaceLoadTarget,
):
    def create_origins(self):
        return [
            location_origin_from_package_name(
                self.package_name,
                self.attribute,
                self.working_directory,
                location_name=self.location_name,
            )
        ]


class GrpcServerTarget(
    NamedTuple(
        "ModuleTarget",
        [
            ("host", str),
            ("port", Optional[int]),
            ("socket", Optional[str]),
            ("location_name", Optional[str]),
        ],
    ),
    WorkspaceLoadTarget,
):
    def create_origins(self):
        return [
            GrpcServerRepositoryLocationOrigin(
                port=self.port,
                socket=self.socket,
                host=self.host,
                location_name=self.location_name,
            )
        ]


#  Utility target for graphql commands that do not require a workspace, e.g. downloading schema
class EmptyWorkspaceTarget(NamedTuple("EmptyWorkspaceTarget", []), WorkspaceLoadTarget):
    def create_origins(self):
        return []
