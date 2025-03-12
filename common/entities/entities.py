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
    DATA_CATALOG = "DATA_CATALOG"
    DATAPLEX = "DATAPLEX"


class TagTemplate:
    """
    Represents a tag template in the Google Cloud Data Catalog.
    """

    resource_name: str
    dataplex_resource_name: str
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
    ):
        """
        Initializes a TagTemplate instance.
        """
        self.project_id = project_id
        self.location = location
        self.id = tag_template_id
        self.resource_name = (
            f"projects/{project_id}/"
            f"locations/{location}/"
            f"tagTemplates/{tag_template_id}"
        )
        self.dataplex_resource_name = ""
        self.public = public
        self.managing_system = ManagingSystem.DATAPLEX \
            if transferred \
            else ManagingSystem.DATA_CATALOG

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

        pattern = (
            r"projects/(?P<project_id>[^/]+)/"
            r"locations/(?P<location>[^/]+)/"
            r"tagTemplates/(?P<tag_template_id>[^/]+)"
        )
        m = re.match(pattern, msg.relative_resource_name)

        if m is None:
            raise FormatException(f"Incorrect tag_template name: "
                                  f"{msg.relative_resource_name}")

        return TagTemplate(
            m.group("project_id"),
            m.group("location"),
            m.group("tag_template_id"),
            public,
            transferred,
        )

    def __repr__(self):
        """
        Returns a string representation of the TagTemplate instance.
        """
        return f"{self.resource_name} ({self.managing_system})"


class EntryGroup:
    """
    Represents an entry group in the Google Cloud Data Catalog.
    """

    resource_name: str
    dataplex_resource_name: str
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
    ):
        """
        Initializes an EntryGroup instance.
        """
        self.project_id = project_id
        self.location = location
        self.id = entry_group_id
        self.resource_name = (
            f"projects/{project_id}/"
            f"locations/{location}/"
            f"entryGroups/{entry_group_id}"
        )
        self.dataplex_resource_name = ""
        self.managing_system = ManagingSystem.DATAPLEX \
            if transferred \
            else ManagingSystem.DATA_CATALOG

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

        pattern = (
            r"projects/(?P<project_id>[^/]+)/"
            r"locations/(?P<location>[^/]+)/"
            r"entryGroups/(?P<entry_group_id>[^/]+)"
        )
        m = re.match(pattern, msg.relative_resource_name)

        if m is None:
            raise FormatException(f"Incorrect entry_group name: "
                                  f"{msg.relative_resource_name}")

        return EntryGroup(
            m.group("project_id"),
            m.group("location"),
            m.group("entry_group_id"),
            transferred,
        )

    def __repr__(self):
        """
        Returns a string representation of the EntryGroup instance.
        """
        return f"{self.resource_name} ({self.managing_system})"


class Project:
    """
    Represents a project in the Google Cloud.
    """

    class AncestryType(StrEnum):
        ORGANIZATION = "ORGANIZATION"
        FOLDER = "FOLDER"

    project_id: str
    project_number: int
    data_catalog_api_enabled: bool
    dataplex_api_enabled: bool
    ancestry: list[tuple[AncestryType, str]]

    def __init__(self, project_id: str, project_number: int):
        self.project_id = project_id
        self.project_number = project_number
        self.data_catalog_api_enabled = False
        self.dataplex_api_enabled = False
        self.ancestry = []

    def set_ancestry(self, ancestry: list[tuple[AncestryType, str]]) -> None:
        self.ancestry = ancestry

    def set_data_catalog_api_enabled(self, data_catalog_api_enabled: bool):
        self.data_catalog_api_enabled = data_catalog_api_enabled

    def set_dataplex_api_enabled(self, dataplex_api_enabled: bool):
        self.dataplex_api_enabled = dataplex_api_enabled

    @staticmethod
    def proto_to_project(msg: asset.ResourceSearchResult) -> "Project":
        """
        Converts a ResourceSearchResult message to a Project instance.
        """
        pattern = r".*projects/(?P<project>[^/]+)$"

        project_id_match = re.match(
            pattern, msg.parent_full_resource_name
        )

        if project_id_match is None:
            raise FormatException(f"Incorrect parent name: "
                                  f"{msg.parent_full_resource_name}")

        project_number_match = re.match(
            pattern, msg.project
        )

        if project_number_match is None:
            raise FormatException(f"Incorrect project name: "
                                  f"{msg.project}")

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
        return {
            "project_id": self.project_id,
            "project_number": self.project_number,
            "data_catalog_api_enabled": self.data_catalog_api_enabled,
            "dataplex_api_enabled": self.dataplex_api_enabled,
            "ancestry": self.ancestry,
        }

    def __repr__(self):
        return f"{self.project_id}, number: {self.project_number}"


type Entity = Project | TagTemplate | EntryGroup
