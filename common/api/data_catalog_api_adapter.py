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
from enum import StrEnum

from google.cloud import datacatalog
from google.cloud.datacatalog_v1.types import SearchCatalogResponse
from google.cloud.datacatalog_v1.services.data_catalog.pagers import (
    SearchCatalogPager,
)
from common.entities import EntryGroup, TagTemplate


class DatacatalogApiAdapter:
    """
    An adapter class for interacting with the Google Cloud Data Catalog API.
    """
    class ResourceType(StrEnum):
        TAG_TEMPLATE = "tag_template"
        ENTRY_GROUP = "entry_group"


    def __init__(self):
        """
        Initializes the DataCatalogApiAdapter with a Data Catalog client.
        """
        self.client = datacatalog.DataCatalogClient()

    def _search_all(
        self,
        scope: list[str],
        query: str,
        page_size: int = 500,
        next_page_token=None,
    ) -> SearchCatalogPager:
        """
        Performs a search in the Data Catalog with the
        specified scope and query.
        """
        request = {
            "scope": {"include_project_ids": scope},
            "query": query,
            "admin_search": True,
            "order_by": "default",
            "page_size": page_size,
            "page_token": next_page_token,
        }

        return self.client.search_catalog(request=request)

    def _search_page(
        self,
        scope: list[str],
        query: str,
        page_size: int = 500,
        next_page_token=None,
    ) -> SearchCatalogResponse:
        """
        Performs a search in the Data Catalog with the
        specified scope and query.
        """
        return next(self._search_all(
            scope,
            query,
            page_size,
            next_page_token
        ).pages)

    def search_tag_templates(
        self,
        projects: list[str],
        public: bool,
        transferred: bool,
        page_size: int = 500,
        page_token: str = None,
    ) -> tuple[list[TagTemplate], str]:
        """
        This method constructs a query to find tag templates based on their
        public visibility and whether they have been marked as transferred.
        It executes the search across the provided list of project IDs.
        """
        transferred_query = (
            "transferred=transferred"
            if transferred
            else "-transferred=transferred"
        )
        public_query = "is_public_tag_template=" + (
            "true" if public else "false"
        )
        query = (f"type={self.ResourceType.TAG_TEMPLATE} AND "
                 f"{transferred_query} AND "
                 f"{public_query}")
        result = self._search_page(projects, query, page_size, page_token)

        return (
            list(
                map(
                    lambda msg: TagTemplate.proto_to_tag_template(
                        msg, public, transferred
                    ),
                    result.results,
                )
            ),
            result.next_page_token,
        )

    def search_entry_groups(
        self,
        projects: list[str],
        transferred: bool,
        page_size: int = 500,
        page_token: str = None,
    ) -> tuple[list[EntryGroup], str]:
        """
        This method constructs a query to find entry groups based on whether
        they have been marked as transferred. It then executes the search across
        the provided list of project IDs.
        """
        transferred_query = (
            "transferred=transferred"
            if transferred
            else "-transferred=transferred"
        )
        query = f"type={self.ResourceType.ENTRY_GROUP} AND {transferred_query}"
        result = self._search_page(projects, query, page_size, page_token)

        return (
            list(
                map(
                    lambda msg: EntryGroup.proto_to_entry_group(
                        msg, transferred
                    ),
                    result.results,
                )
            ),
            result.next_page_token,
        )

    def get_entry_group(self, project: str, location: str, name: str):
        """
        Retrieves an entry group by its fully qualified name.
        """
        fqn = f"projects/{project}/locations/{location}/entryGroups/{name}"
        return self.client.get_entry_group(request={"name": fqn})

    def get_tag_template(self, project: str, location: str, name: str):
        """
        Retrieves a tag template by its fully qualified name.
        """
        fqn = f"projects/{project}/locations/{location}/tagTemplates/{name}"
        return self.client.get_tag_template(request={"name": fqn})
