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
This module defines the TransferController class, which orchestrates the
transfer of data from the Google Cloud Data Catalog to BigQuery. It utilizes the
DatacatalogApiAdapter and BigQueryAdapter to fetch and store data, respectively.

Classes:
- TransferController: A controller class for managing the transfer of tag
  templates and entry groups from the Data Catalog to BigQuery.
"""

import datetime
from google.api_core.exceptions import PermissionDenied
from common.api import CloudAssetApiAdapter
from common.big_query import BigQueryAdapter
from common.entities import Project
from common.cloud_task import CloudTaskPublisher
from common.api.resource_manager_api_adapter import ResourceManagerApiAdapter


class TransferController:
    """
    A controller class for managing the transfer of data from the Google Cloud
    Data Catalog to BigQuery. It handles the retrieval of projects, tag
    templates, and entry groups, and writes them to BigQuery tables.
    """

    def __init__(self, app_config: dict):
        """
        Initializes the TransferController with the specified project.
        """
        self.project = app_config["project_name"]
        self.location = app_config["service_location"]
        self.handler_name = app_config["handler_name"]
        self.queue = app_config["queue"]
        self._resource_manager_client = ResourceManagerApiAdapter()
        self.organization_number = self._get_organization_number(self.project)
        self.api_client = CloudAssetApiAdapter(self.organization_number)
        self.big_query_client = BigQueryAdapter(app_config)
        self.cloud_task_client = CloudTaskPublisher(
            self.project, self.location, self.queue
        )

    def _get_organization_number(self, project):
        try:
            return self._resource_manager_client.get_organization_number(
                project
            )
        except PermissionDenied as e:
            raise PermissionDenied(
                f"Not enough permissions or {self.project} doesn't exists"
            ) from e

    def start_transfer(self):
        """
        Initiates the data transfer process by fetching projects and resources,
        and writing them to BigQuery tables.
        """
        projects = self.fetch_projects()
        projects = self.merge_projects(projects)

        self.create_cloud_tasks(projects)

    def fetch_projects(self) -> list[Project]:
        """
        Fetches all projects within the organization which have datacatalog
        or dataplex API enabled.
        """
        return self.api_client.fetch_projects()

    def create_cloud_tasks(self, projects: list[Project]):
        """
        Create cloud tasks for further processing
        """
        if not self.cloud_task_client.check_queue_exists():
            self.cloud_task_client.create_queue()

        today = datetime.datetime.today().isoformat()

        for project in projects:
            payload = project.to_dict()
            payload["created_at"] = (
                today  # TODO: move "created_at to constants.py"
            )

            self.cloud_task_client.create_task(
                payload,
                self.handler_name,
                self.project,
                self.location,
            )

    @staticmethod
    def merge_projects(projects: list[Project]) -> list[Project]:
        """
        Remove duplicates from the API response
        """
        name2projects = {}

        for project in projects:
            if project.project_id not in name2projects:
                name2projects[project.project_id] = project
            else:
                dataplex_api = (
                    project.dataplex_api_enabled
                    or name2projects[project.project_id].dataplex_api_enabled
                )
                datacatalog_api = (
                    project.data_catalog_api_enabled
                    or name2projects[
                        project.project_id
                    ].data_catalog_api_enabled
                )

                name2projects[project.project_id].set_dataplex_api_enabled(
                    dataplex_api
                )
                name2projects[project.project_id].set_data_catalog_api_enabled(
                    datacatalog_api
                )

        return list(name2projects.values())
