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
Defines entities and Pydantic models for resource and policy management.
"""

from datetime import date
from typing import Optional, Literal

from pydantic import BaseModel, model_validator


class ResourceData(BaseModel):
    """
    Represents basic resource data like project, location, and name.
    """

    project_id: str
    location: str
    resource_name: str


class ExtendedResourceData(ResourceData):
    """
    Extends ResourceData with a system type (DATA_CATALOG or DATAPLEX).
    """

    system: Literal["DATA_CATALOG", "DATAPLEX"]


class ResourceTaskData(BaseModel):
    """
    Model for finding resource names by type and creation date.
    """

    resource_type: Literal["EntryGroup", "TagTemplate"]
    resource: ResourceData


class FetchResourcesTaskData(BaseModel):
    """
    Model for fetching resources with scope, type, and transfer status.
    """

    scope: str
    resource_type: Literal["entry_group", "tag_template"]
    next_page_token: Optional[str] = None
    is_transferred: bool = False
    created_at: date
    is_public: Optional[bool] = None

    @model_validator(mode="after")
    @classmethod
    def validate_is_public(
        cls, values: "FetchResourcesTaskData"
    ) -> "FetchResourcesTaskData":
        """
        Validates that 'is_public' is provided when the resource
        type is 'tag_template'.
        """
        if values.resource_type == "tag_template" and values.is_public is None:
            raise ValueError(
                "'is_public' is required when 'resource_type' is 'tag_template'"
            )
        return values


class FetchProjectsTaskData(BaseModel):
    """
    Model for project data including APIs, ancestry, and creation date.
    """

    project_id: str
    project_number: int
    data_catalog_api_enabled: bool = False
    dataplex_api_enabled: bool = False
    created_at: date


class FetchPoliciesTaskData(BaseModel):
    """
    Model for fetching policies by resource type and creation date.
    """

    resource_type: Literal["EntryGroup", "TagTemplate"]
    created_at: date
    resource: ExtendedResourceData


class ConvertPrivateTagTemplatesTaskData(ResourceData):
    """
    Model for fetching policies by resource type and creation date.
    """
