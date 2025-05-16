# Copyright 2025 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#   https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""
This module defines the TagTemplate and EntryGroup classes, which represent
entities in the Google Cloud Data Catalog. These classes provide methods for
constructing instances from search results and for representing the entities
as strings.

Classes:
- TagTemplate: Represents a tag template in the Data Catalog.
- EntryGroup: Represents an entry group in the Data Catalog.
"""

import re
from enum import StrEnum

from google.cloud import datacatalog
from google.cloud import asset

from common.exceptions import IncorrectTypeException, FormatException


class ManagingSystem(StrEnum):
    """
    Enum representing the managing system for resources.
    """

    DATA_CATALOG = "DATA_CATALOG"
    DATAPLEX = "DATAPLEX"


class TagTemplate:
    """
    Represents a tag template in the Google Cloud Data Catalog.
    """

    resource_name: str
    dataplex_resource_name: str | None
    project_id: str
    location: str
    id: str
    public: bool
    managing_system: str

    def __init__(
        self,
        project_id: str,
        location: str,
        tag_template_id: str,
        public: bool,
        transferred: bool,
    ) -> None:
        """
        Initializes a TagTemplate instance.
        """
        self.project_id = project_id
        self.location = location
        self.id = tag_template_id
        self.resource_name = self.get_old_fqn(
            project_id, location, tag_template_id
        )
        self.dataplex_resource_name = None
        self.public = public
        self.managing_system = (
            ManagingSystem.DATAPLEX
            if transferred
            else ManagingSystem.DATA_CATALOG
        )

    @staticmethod
    def get_old_fqn(project_id: str, location: str, name: str) -> str:
        """
        Constructs the old fully qualified name for a tag template.
        """
        return (
            f"projects/{project_id}/"
            f"locations/{location}/"
            f"tagTemplates/{name}"
        )

    @staticmethod
    def get_new_fqn(project_id: str, _: str, name: str) -> str:
        """
        Constructs a fully qualified name for an aspect type.
        """
        return f"projects/{project_id}/locations/global/aspectTypes/{name}"

    @staticmethod
    def parse_tag_template_resource(resource_name: str) -> dict[str, str]:
        """
        Parses a tag template resource name into its components.
        """
        pattern = (
            r"projects/(?P<project_id>[^/]+)/"
            r"locations/(?P<location>[^/]+)/"
            r"(?:tagTemplates|aspectTypes)/(?P<tag_template_id>[^/]+)"
        )
        match = re.match(pattern, resource_name)

        if match is None:
            raise FormatException(
                f"Incorrect tag_template name: {resource_name}"
            )

        return match.groupdict()

    @staticmethod
    def proto_to_tag_template(
        msg: datacatalog.SearchCatalogResult, public: bool, transferred: bool
    ) -> "TagTemplate":
        """
        Converts a SearchCatalogResult message to a TagTemplate instance.
        """
        if msg.search_result_type != 2:
            raise IncorrectTypeException(
                f"Expected type 2, got {msg.search_result_type}"
            )

        parsed_components = TagTemplate.parse_tag_template_resource(
            msg.relative_resource_name
        )

        return TagTemplate(
            project_id=parsed_components["project_id"],
            location=parsed_components["location"],
            tag_template_id=parsed_components["tag_template_id"],
            public=public,
            transferred=transferred,
        )

    def __repr__(self) -> str:
        """
        Returns a string representation of the TagTemplate instance.
        """
        return f"{self.resource_name} ({self.managing_system})"


class EntryGroup:
    """
    Represents an entry group in the Google Cloud Data Catalog.
    """

    resource_name: str
    dataplex_resource_name: str | None
    project_id: str
    location: str
    id: str
    managing_system: str

    def __init__(
        self,
        project_id: str,
        location: str,
        entry_group_id: str,
        transferred: bool,
    ) -> None:
        """
        Initializes an EntryGroup instance.
        """
        self.project_id = project_id
        self.location = location
        self.id = entry_group_id
        self.resource_name = self.get_old_fqn(
            project_id, location, entry_group_id
        )
        self.dataplex_resource_name = None
        self.managing_system = (
            ManagingSystem.DATAPLEX
            if transferred
            else ManagingSystem.DATA_CATALOG
        )

    @staticmethod
    def get_old_fqn(project_id: str, location: str, name: str) -> str:
        """
        Constructs the old fully qualified name for an entry group.
        """
        return (
            f"projects/{project_id}/"
            f"locations/{location}/"
            f"entryGroups/{name}"
        )

    @staticmethod
    def get_new_fqn(project_id: str, location: str, name: str) -> str:
        """
        Constructs the new fully qualified name for an entry group.
        """
        return (
            f"projects/{project_id}/"
            f"locations/{location}/"
            f"entryGroups/{name}"
        )

    @staticmethod
    def parse_entry_group_resource(resource_name: str) -> dict[str, str]:
        """
        Parses an entry group resource name into its components.
        """
        pattern = (
            r"projects/(?P<project_id>[^/]+)/"
            r"locations/(?P<location>[^/]+)/"
            r"entryGroups/(?P<entry_group_id>[^/]+)"
        )
        match = re.match(pattern, resource_name)

        if match is None:
            raise FormatException(
                f"Incorrect entry_group name: {resource_name}"
            )

        return match.groupdict()

    @staticmethod
    def proto_to_entry_group(
        msg: datacatalog.SearchCatalogResult, transferred: bool
    ) -> "EntryGroup":
        """
        Converts a SearchCatalogResult message to an EntryGroup instance.
        """
        if msg.search_result_type != 3:
            raise IncorrectTypeException(
                f"Expected type 3, got {msg.search_result_type}"
            )

        parsed_components = EntryGroup.parse_entry_group_resource(
            msg.relative_resource_name
        )

        return EntryGroup(
            project_id=parsed_components["project_id"],
            location=parsed_components["location"],
            entry_group_id=parsed_components["entry_group_id"],
            transferred=transferred,
        )

    def __repr__(self) -> str:
        """
        Returns a string representation of the EntryGroup instance.
        """
        return f"{self.resource_name} ({self.managing_system})"


class Project:
    """
    Represents a project in the Google Cloud.
    """

    class AncestryType(StrEnum):
        """
        Enum representing the ancestry type for a project.
        """

        ORGANIZATION = "ORGANIZATION"
        FOLDER = "FOLDER"

    project_id: str
    project_number: int
    data_catalog_api_enabled: bool
    dataplex_api_enabled: bool
    ancestry: list[tuple[AncestryType, str]]

    def __init__(self, project_id: str, project_number: int) -> None:
        """
        Initializes a Project instance.
        """
        self.project_id = project_id
        self.project_number = project_number
        self.data_catalog_api_enabled = False
        self.dataplex_api_enabled = False
        self.ancestry = []

    def set_ancestry(self, ancestry: list[tuple[AncestryType, str]]) -> None:
        """
        Sets the ancestry of the project.
        """
        self.ancestry = ancestry

    def set_data_catalog_api_enabled(
        self, data_catalog_api_enabled: bool
    ) -> None:
        """
        Sets the value of the Data Catalog API enabled.
        """
        self.data_catalog_api_enabled = data_catalog_api_enabled

    def set_dataplex_api_enabled(self, dataplex_api_enabled: bool) -> None:
        """
        Sets the value of the Dataplex API enabled.
        """
        self.dataplex_api_enabled = dataplex_api_enabled

    @staticmethod
    def proto_to_project(msg: asset.ResourceSearchResult) -> "Project":
        """
        Converts a ResourceSearchResult message to a Project instance.
        """
        pattern = r".*projects/(?P<project>[^/]+)$"

        project_id_match = re.match(pattern, msg.parent_full_resource_name)

        if project_id_match is None:
            raise FormatException(
                f"Incorrect parent name: {msg.parent_full_resource_name}"
            )

        project_number_match = re.match(pattern, msg.project)

        if project_number_match is None:
            raise FormatException(f"Incorrect project name: {msg.project}")

        project = Project(
            project_id_match.group("project"),
            project_number_match.group("project"),
        )

        if msg.display_name == "datacatalog.googleapis.com":
            project.set_data_catalog_api_enabled(True)

        if msg.display_name == "dataplex.googleapis.com":
            project.set_dataplex_api_enabled(True)

        return project

    def to_dict(self) -> dict:
        """
        Converts the Project instance to a dictionary representation.
        """
        return {
            "project_id": self.project_id,
            "project_number": self.project_number,
            "data_catalog_api_enabled": self.data_catalog_api_enabled,
            "dataplex_api_enabled": self.dataplex_api_enabled,
            "ancestry": self.ancestry,
        }

    def __repr__(self) -> str:
        """
        Returns a string representation of the Project instance.
        """
        return f"{self.project_id}, number: {self.project_number}"


type Entity = Project | TagTemplate | EntryGroup
