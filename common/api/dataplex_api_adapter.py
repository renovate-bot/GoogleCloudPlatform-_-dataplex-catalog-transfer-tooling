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
the Google Cloud Dataplex API.
It includes functionality for searching and retrieving
tag templates and entry groups.

Classes:
- DataplexApiAdapter: An adapter class for interacting
with the Data Catalog API.
"""

from typing import Generator

import google_auth_httplib2
import google.auth as auth
import google.cloud.dataplex as dataplex
import google.cloud.dataplex_v1.types as dataplex_types
from google.cloud import dataplex_v1
from google.api_core import retry
from google.api_core.exceptions import NotFound, ResourceExhausted
from google.api_core.gapic_v1.client_info import ClientInfo
from google.protobuf import struct_pb2
from google.cloud.dataplex_v1 import AspectType
from googleapiclient import discovery
from googleapiclient.errors import HttpError
from googleapiclient.http import HttpRequest

from common.entities import TagTemplate, EntryGroup
from common.exceptions import IncorrectTypeException
from common.utils import get_logger


class CustomRequestBuilder(HttpRequest):
    """
    A custom request builder that extends `googleapiclient.http.HttpRequest`
    to include a custom `User-Agent` header for all outgoing HTTP requests.
    """

    def __init__(
        self,
        http,
        postproc,
        uri,
        method="GET",
        body=None,
        headers=None,
        methodId=None,
        resumable=None,
    ):
        """
        Initializes the CustomRequestBuilder with the specified parameters and
        adds a custom `User-Agent` header to the request.
        """
        if headers is None:
            headers = {}
        headers["User-Agent"] = "TransferTooling/1.0.0"
        super().__init__(
            http, postproc, uri, method, body, headers, methodId, resumable
        )


class DataplexApiAdapter:
    """
    An adapter class for interacting with the Google Cloud Dataplex API.
    """

    def __init__(self) -> None:
        """
        Initializes the DataplexApiAdapter with a Data Catalog client.
        """
        self._client = dataplex.CatalogServiceClient(
            client_info=ClientInfo(user_agent="TransferTooling/1.0.0"),
        )
        self._dataplex_service_client = dataplex_v1.DataplexServiceClient()
        self._metadata_service_client = dataplex_v1.MetadataServiceClient()
        self._plain_client = discovery.build(
            "dataplex", "v1", requestBuilder=CustomRequestBuilder
        )
        self._logger = get_logger()
        self._credentials, _ = auth.default(
            scopes=["https://www.googleapis.com/auth/cloud-platform"]
        )

    def get_entry_group(self, fqn: str) -> dataplex_types.EntryGroup | None:
        """
        Get entry group info
        """
        try:
            return self._client.get_entry_group(name=fqn)
        except NotFound:
            return None

    def get_aspect_type(self, fqn: str) -> dict | None:
        """
        Get aspect type info
        """
        http = google_auth_httplib2.AuthorizedHttp(self._credentials)
        try:
            answer = (
                self._plain_client.projects()
                .locations()
                .aspectTypes()
                .get(name=fqn)
                .execute(http=http)
            )
            return answer
        except HttpError as e:
            if e.status_code == 404:
                return None
            raise e

    def create_aspect_type(
        self,
        project: str,
        region: str,
        aspect_type_id: str,
        name: str,
        description: str,
        metadata: dict,
    ) -> AspectType:
        """
        Creates a new Dataplex aspect type in the specified project and region.
        """
        parent = f"projects/{project}/locations/{region}"
        aspect_type = self._client.create_aspect_type(
            parent=parent,
            aspect_type_id=aspect_type_id,
            aspect_type=AspectType(
                {
                    "display_name": name,
                    "description": description,
                    "metadata_template": metadata,
                }
            ),
        ).result()

        return aspect_type

    def delete_entry_group(
        self, project: str, location: str, name: str
    ) -> None:
        """
        Deletes an entry group.
        """
        fqn = EntryGroup.get_new_fqn(project, location, name)
        self._client.delete_entry_group(name=fqn)

    def delete_aspect_type(
        self, project: str, location: str, name: str
    ) -> dict | None:
        """
        Deletes an aspect type.
        """
        fqn = TagTemplate.get_new_fqn(project, location, name)
        http = google_auth_httplib2.AuthorizedHttp(self._credentials)
        try:
            answer = (
                self._plain_client.projects()
                .locations()
                .aspectTypes()
                .delete(name=fqn)
                .execute(http=http)
            )
            return answer
        except HttpError as e:
            if e.status_code == 404:
                return None
            raise e

    def get_resource_policy(
        self, resource_type: str, project: str, location: str, name: str
    ) -> list:
        """
        Retrieves the IAM policy bindings for a resource.
        """
        http = google_auth_httplib2.AuthorizedHttp(self._credentials)
        if resource_type == TagTemplate.__name__:
            fqn = TagTemplate.get_new_fqn(project, location, name)
            response = (
                self._plain_client.projects()
                .locations()
                .aspectTypes()
                .getIamPolicy(resource=fqn)
                .execute(http=http)
            )
            return response.get("bindings", [])
        elif resource_type == EntryGroup.__name__:
            fqn = EntryGroup.get_new_fqn(project, location, name)
            response = (
                self._plain_client.projects()
                .locations()
                .entryGroups()
                .getIamPolicy(resource=fqn)
                .execute(http=http)
            )
            return response.get("bindings", [])
        else:
            raise IncorrectTypeException(
                f"Unknown resource type " f"{resource_type}"
            )

    def list_entities(
        self, zone_name: str, rate_limiter: Generator, page_size: int = 500
    ) -> Generator[list]:
        """
        Lists entities (tables) in a given Dataplex zone and yields
        their metadata in batches.
        """
        try:
            request = dataplex_types.ListEntitiesRequest(
                parent=zone_name,
                view=dataplex_types.ListEntitiesRequest.EntityView.TABLES,
                page_size=page_size,
            )
            response = self._metadata_service_client.list_entities(request)

            for page in response.pages:
                tmp_res = list(
                    map(
                        lambda msg: {
                            "fqn": msg.name,
                            "data_path": msg.data_path,
                            "type": msg.type_.name,
                        },
                        page.entities,
                    )
                )
                yield tmp_res
                next(rate_limiter)
        except Exception as e:
            self._logger.error(
                f"Failed to list entities for zone {zone_name}: {e}"
            )
            raise e

    def get_bq_asset(self, fqn: str, limiter: Generator) -> str | None:
        """
        Retrieves the BigQuery dataset name from a Dataplex asset
        if it's a BigQuery dataset.
        """
        next(limiter)

        retry_policy = retry.Retry(
            predicate=retry.if_exception_type(ResourceExhausted),
            initial=20.0,
            maximum=60.0,
            multiplier=1.5,
            deadline=300.0,
        )

        asset = self._dataplex_service_client.get_asset(
            name=fqn, retry=retry_policy
        )
        resource_type = asset.resource_spec.type_
        if (
            resource_type
            == dataplex_types.Asset.ResourceSpec.Type.BIGQUERY_DATASET
        ):
            return asset.resource_spec.name
        return None

    def update_entry(
        self,
        entry_resource_name: str,
        aspect_type_project: str,
        lake_id: str,
        zone_id: str,
    ) -> dataplex_v1.Entry:
        """
        Updates an entry in Dataplex with lake and zone details.
        """

        with dataplex_v1.CatalogServiceClient() as client:
            entry = dataplex_v1.Entry(
                name=entry_resource_name,
                entry_source=dataplex_v1.EntrySource(
                    description="updated description of the entry"
                ),
                aspects={
                    f"{aspect_type_project}.global.lake-details": (
                        dataplex_v1.Aspect(
                            aspect_type=(
                                f"projects/{aspect_type_project}/locations/"
                                "global/aspectTypes/lake-details"
                            ),
                            data=struct_pb2.Struct(
                                fields={
                                    "lake": struct_pb2.Value(
                                        string_value=f"{lake_id}"
                                    ),
                                    "zone": struct_pb2.Value(
                                        string_value=f"{zone_id}"
                                    ),
                                }
                            ),
                        )
                    )
                },
            )

            update_mask = {"paths": ["aspects"]}
            return client.update_entry(entry=entry, update_mask=update_mask)
