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

import google_auth_httplib2
import google.auth as auth
import google.cloud.dataplex as dataplex
import google.cloud.dataplex_v1.types as dataplex_types
from google.api_core.exceptions import NotFound
from google.api_core.gapic_v1.client_info import ClientInfo
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
