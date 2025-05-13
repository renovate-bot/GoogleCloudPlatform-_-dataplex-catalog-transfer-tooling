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
This module provides an adapter for interacting with
the Google Cloud Data Catalog API.
It includes functionality for searching and retrieving
tag templates and entry groups.

Classes:
- DatacatalogApiAdapter: An adapter class for interacting
  with the Data Catalog API.
"""

from google.api_core.exceptions import PermissionDenied
from google.cloud import asset
from google.cloud.asset_v1.services.asset_service.pagers import (
    SearchAllResourcesPager,
)
from common.entities import Project


class CloudAssetApiAdapter:
    """
    An adapter class for interacting with the Google Cloud Asset API.
    """

    def __init__(self, organization: int):
        """
        Initializes the AssetApiAdapter with a AssetService client.
        """
        self._client = asset.AssetServiceClient()
        self.organization = f"organizations/{organization}"

    def fetch_projects(self) -> list[Project]:
        """
        Fetches all projects within the organization which have
        datacatalog or dataplex API enabled.
        """
        response = list(
            self._search(
                self.organization,
                ["serviceusage.googleapis.com/Service"],
                "name:(datacatalog.googleapis.com OR dataplex.googleapis.com)",
            )
        )

        return list(map(Project.proto_to_project, response))

    def _search(
        self, scope: str, asset_types: list[str], query: str
    ) -> SearchAllResourcesPager:
        """
        Performs a search in the Assets with the specified scope and query.
        """
        try:
            return self._client.search_all_resources(
                scope=scope, asset_types=asset_types, query=query
            )
        except PermissionDenied as e:
            raise PermissionDenied(f"Not enough permissions for scope {scope} "
                                   f"or scope doesn't exists") from e
