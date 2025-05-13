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
This module provides an adapter for interacting with the 
Google Cloud Data Catalog API.
It includes functionality for searching and retrieving tag templates
and entry groups.

Classes:
- DatacatalogApiAdapter: An adapter class for interacting with the 
  Data Catalog API.
"""


from functools import cache
from google.cloud import resourcemanager
from googleapiclient import discovery
from googleapiclient.errors import HttpError
from common.utils import get_logger
from common.exceptions import FormatException
from common.entities import Project


class ResourceManagerApiAdapter:
    """
    An adapter class for interacting with the Google Cloud Asset API.
    """

    def __init__(self):
        """
        Initializes the ResourceManagerApiAdapter with a ResourceManager client.
        """
        self._project_client = resourcemanager.ProjectsClient()
        self._plain_api_client = discovery.build("cloudresourcemanager", "v1")
        self._logger = get_logger()

    @cache
    def get_project_number(self, project_id: str) -> str:
        project = self._project_client.get_project(
            name=f"projects/{project_id}"
        )

        return self._project_client.parse_common_project_path(
            project.name
        )["project"]

    @cache
    def get_organization_number(self, project_id: str) -> str:
        ancestry = self.get_project_ancestry(project_id)

        for ancestry_type, resource_id in ancestry:
            if ancestry_type == Project.AncestryType.ORGANIZATION:
                return resource_id

    def get_project_ancestry(
        self, project_id: str
    ) -> list[tuple[Project.AncestryType, str]]:
        try:
            response = (
                self._plain_api_client.projects()
                .getAncestry(projectId=project_id)
                .execute()
            )
        except HttpError as e:
            if e.status_code == 403:
                error_msg = (f"Not enough permissions for project {project_id}"
                             f" or project does not exists")
                raise HttpError(
                    e.resp,
                    error_msg.encode("utf-8"),
                    uri=e.uri
                ) from e
            elif e.status_code == 400:
                error_msg = f"Incorrect project name: {project_id}"
                raise HttpError(
                    e.resp,
                    error_msg.encode("utf-8"),
                    uri=e.uri
                ) from e

            raise e

        result = []

        for item in response["ancestor"]:
            ancestor = item["resourceId"]

            if ancestor["type"] == "folder":
                result.append((Project.AncestryType.FOLDER, ancestor["id"]))
            elif ancestor["type"] == "organization":
                result.append(
                    (Project.AncestryType.ORGANIZATION, ancestor["id"])
                )
            elif ancestor["type"] == "project":
                pass
            else:
                raise FormatException(
                    "The parent is neither a folder, an organization, "
                    f"nor a project: {ancestor["type"]}"
                )

        return result
