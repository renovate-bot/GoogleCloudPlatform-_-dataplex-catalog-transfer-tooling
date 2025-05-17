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

from google.api_core.exceptions import NotFound, GoogleAPIError
from google.cloud import datacatalog
from google.cloud.datacatalog_v1.types import (
    SearchCatalogResponse,
    tags,
    EntryGroup as EntryGroupProto,
    TagTemplate as TagTemplateType,
)
from google.cloud.datacatalog_v1.services.data_catalog.pagers import (
    SearchCatalogPager,
)

from common.entities import EntryGroup, TagTemplate
from common.exceptions import IncorrectTypeException
from common.utils import get_logger


class DatacatalogApiAdapter:
    """
    An adapter class for interacting with the Google Cloud Data Catalog API.
    """

    class ResourceType(StrEnum):
        TAG_TEMPLATE = "tag_template"
        ENTRY_GROUP = "entry_group"

    def __init__(self) -> None:
        """
        Initializes the DataCatalogApiAdapter with a Data Catalog client.
        """
        self._client = datacatalog.DataCatalogClient()
        self._logger = get_logger()

    def _search_all(
        self,
        scope: list[str],
        query: str,
        page_size: int = 500,
        next_page_token = None,
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

        return self._client.search_catalog(request=request)

    def _search_page(
        self,
        scope: list[str],
        query: str,
        page_size: int = 500,
        next_page_token = None,
    ) -> SearchCatalogResponse:
        """
        Performs a search in the Data Catalog with the
        specified scope and query.
        """
        return next(
            self._search_all(scope, query, page_size, next_page_token).pages
        )

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
        query = (
            f"type={self.ResourceType.TAG_TEMPLATE} AND "
            f"{transferred_query} AND "
            f"{public_query}"
        )
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

    def get_entry_group(
        self, project: str, location: str, name: str
    ) -> EntryGroupProto:
        """
        Retrieves an entry group by its fully qualified name.
        """
        fqn = EntryGroup.get_old_fqn(project, location, name)
        return self._client.get_entry_group(request={"name": fqn})

    def get_tag_template(
        self, project: str, location: str, name: str
    ) -> TagTemplateType:
        """
        Retrieves a tag template by its fully qualified name.
        """
        fqn = TagTemplate.get_old_fqn(project, location, name)
        return self._client.get_tag_template(request={"name": fqn})

    def get_resource_policy(
        self, resource_type: str, project: str, location: str, name: str
    ) -> list:
        """
        Retrieves the IAM policy for a resource.
        """
        if resource_type == TagTemplate.__name__:
            fqn = TagTemplate.get_old_fqn(project, location, name)
        elif resource_type == EntryGroup.__name__:
            fqn = EntryGroup.get_old_fqn(project, location, name)
        else:
            raise IncorrectTypeException(
                f"Unknown resource type: {resource_type}"
            )

        try:
            response = self._client.get_iam_policy(resource=fqn)
        except NotFound:
            return []

        return [
            {"role": binding.role, "members": binding.members}
            for binding in response.bindings
        ]

    def transfer_tag_template(
        self, fqn: str
    ) -> TagTemplateType | GoogleAPIError:
        """
        Transfer Tag Template
        """
        tag_template = tags.TagTemplate(
            {
                "name": fqn,
                "dataplex_transfer_status": (
                    tags.TagTemplate.DataplexTransferStatus.TRANSFERRED
                ),
            }
        )

        return self._update_tag_template(tag_template, "dataplexTransferStatus")

    def transfer_entry_group(
        self, fqn: str
    ) -> EntryGroupProto | GoogleAPIError:
        """
        Transfer Entry Group
        """
        entry_group = EntryGroupProto(
            {"name": fqn, "transferred_to_dataplex": True}
        )

        return self._update_entry_group(entry_group, "transferredToDataplex")

    def _update_tag_template(
        self, tag_template: tags.TagTemplate, update_mask: str
    ) -> TagTemplateType | GoogleAPIError:
        """
        Update TagTemplate
        """
        try:
            response = self._client.update_tag_template(
                tag_template=tag_template,
                update_mask=update_mask,
            )
            return response
        except GoogleAPIError as e:
            self._logger.error(
                "Error updating tag template %s. %s", tag_template.name, e
            )
            return e

    def _update_entry_group(
        self, entry_group: EntryGroupProto, update_mask: str
    ) -> EntryGroupProto | GoogleAPIError:
        """
        Update EntryGroup
        """
        try:
            response = self._client.update_entry_group(
                entry_group=entry_group,
                update_mask=update_mask,
            )
            return response
        except GoogleAPIError as e:
            self._logger.error(
                "Error updating entry group %s. %s", entry_group.name, e
            )
            return e

    def convert_private_tag_template(self, tt_name: str) -> TagTemplateType:
        """
        Converts a private tag template to public by updating its
        `isPubliclyReadable` property.
        """
        request = datacatalog.UpdateTagTemplateRequest({
            "tag_template": TagTemplateType({
                "name": tt_name, "is_publicly_readable": True
            }),
            "update_mask": "isPubliclyReadable",
        })

        response = self._client.update_tag_template(request=request)
        return response

    def create_entry_group(
        self, project: str, location: str, name: str
    ) -> None:
        """
        Creates an entry group.
        """
        self._client.create_entry_group(
            parent=f"projects/{project}/locations/{location}",
            entry_group_id=name,
        )

    def create_tag_template(
        self,
        project: str,
        location: str,
        name: str,
        fields: dict,
        public: bool = True,
    ) -> None:
        """
        Creates a tag template.
        """
        tag_template = TagTemplateType(
            {
                "is_publicly_readable": public,
                "fields": fields,
            }
        )
        self._client.create_tag_template(
            parent=f"projects/{project}/locations/{location}",
            tag_template_id=name,
            tag_template=tag_template,
        )

    def delete_entry_group(
        self, project: str, location: str, name: str
    ) -> None:
        """
        Deletes an entry group.
        """
        self._client.delete_entry_group(
            name=f"projects/{project}/locations/{location}/entryGroups/{name}"
        )

    def delete_tag_template(
        self, project: str, location: str, name: str, force: bool = None
    ) -> None:
        """
        Deletes a tag template.
        """
        self._client.delete_tag_template(
            name=f"projects/{project}/locations/{location}/tagTemplates/{name}",
            force=force,
        )
