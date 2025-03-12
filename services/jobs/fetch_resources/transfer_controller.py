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

from common.api import DatacatalogApiAdapter
from common.big_query import BigQueryAdapter
from common.cloud_task import CloudTaskPublisher


class TransferController:
    """
    A controller class for managing the transfer
    of data from the Google CloudData Catalog to BigQuery.
    It handles the retrieval of projects, tag
    templates, and entry groups, and writes them to BigQuery tables.
    """

    def __init__(self, app_config: dict):
        """
        Initializes the TransferController with the specified project.
        """
        self.project = app_config["project_name"]
        self.location = app_config["service_location"]
        self.handler_name = app_config["handler_name"]
        self.queue_name = app_config["queue"]
        self.api_client = DatacatalogApiAdapter()
        self.big_query_client = BigQueryAdapter(app_config)
        self.cloud_task_client = CloudTaskPublisher(
            self.project,
            self.location,
            self.queue_name
        )


    def start_transfer(self):
        """
        Initiates the data transfer process by fetching projects and resources,
        and writing them to BigQuery tables.
        """
        projects = self.get_projects_to_fetch()
        self.create_cloud_tasks(projects)

    def get_projects_to_fetch(self) -> list[str]:
        """
        Retrieves a list of project IDs to fetch data from.
        """
        return list(set(self.big_query_client.get_projects_to_fetch()))

    def create_cloud_tasks(self, projects: list[str]):
        """
        Create initial cloud tasks for cloud task handler
        """
        if not self.cloud_task_client.check_queue_exists():
            self.cloud_task_client.create_queue()

        today = datetime.date.today()
        for project in projects:
            transferred_public_tag_payload = self.build_cloud_task_payload(
                project,
                DatacatalogApiAdapter.ResourceType.TAG_TEMPLATE,
                True,
                today,
                True
            )
            non_transferred_public_tag_payload = self.build_cloud_task_payload(
                project,
                DatacatalogApiAdapter.ResourceType.TAG_TEMPLATE,
                False,
                today,
                True
            )
            transferred_private_tag_payload = self.build_cloud_task_payload(
                project,
                DatacatalogApiAdapter.ResourceType.TAG_TEMPLATE,
                True,
                today,
                False
            )
            non_transferred_private_tag_payload = self.build_cloud_task_payload(
                project,
                DatacatalogApiAdapter.ResourceType.TAG_TEMPLATE,
                False,
                today,
                False
            )
            transferred_entry_payload = self.build_cloud_task_payload(
                project,
                DatacatalogApiAdapter.ResourceType.ENTRY_GROUP,
                True,
                today
            )
            non_transferred_entry_payload = self.build_cloud_task_payload(
                project,
                DatacatalogApiAdapter.ResourceType.ENTRY_GROUP,
                False,
                today
            )

            self.cloud_task_client.create_task(
                transferred_public_tag_payload,
                self.handler_name,
                self.project,
                self.location
            )
            self.cloud_task_client.create_task(
                transferred_private_tag_payload,
                self.handler_name,
                self.project,
                self.location
            )
            self.cloud_task_client.create_task(
                non_transferred_public_tag_payload,
                self.handler_name,
                self.project,
                self.location
            )
            self.cloud_task_client.create_task(
                non_transferred_private_tag_payload,
                self.handler_name,
                self.project,
                self.location
            )
            self.cloud_task_client.create_task(
                transferred_entry_payload,
                self.handler_name,
                self.project,
                self.location
            )
            self.cloud_task_client.create_task(
                non_transferred_entry_payload,
                self.handler_name,
                self.project,
                self.location
            )



    @staticmethod
    def build_cloud_task_payload(
            project: str,
            entity_type: str,
            transferred: bool,
            created_at: datetime.date,
            public: bool = None
    ) -> dict:
        """
        Build payload for cloud task
        """
        payload = {
            "scope": project,
            "type": entity_type,
            "nextPageToken": None,
            "is_transferred": transferred,
            "createdAt": created_at.isoformat()
        }

        if entity_type == DatacatalogApiAdapter.ResourceType.TAG_TEMPLATE:
            payload["is_public"] = public

        return payload
