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
This module defines the `CloudTaskHandler` class, which processes cloud tasks
related to Google Cloud Platform (GCP) resources such as entry groups and tag
templates. It integrates with the Datacatalog API, BigQuery, and Cloud Tasks
to manage resource discovery and transfer workflows.
"""

from common.api import DatacatalogApiAdapter
from common.big_query import BigQueryAdapter, TableNames
from common.cloud_task import CloudTaskPublisher
from common.entities import FetchResourcesTaskData


class CloudTaskHandler:
    """
    Handles cloud tasks, validates task data, and manages the lifecycle of
    the application. This class interacts with GCP resources to manage entry
    groups and tag templates.
    """

    def __init__(self, app_config):
        """
        Initializes the CloudTaskHandler with configuration for the GCP project
        and other settings.
        """
        self.project_name = app_config["project_name"]
        self.service_location = app_config["service_location"]
        self.queue = app_config["queue"]
        self.handler_name = app_config["handler_name"]
        self.dataset_name = app_config["dataset_name"]
        self.api_client = DatacatalogApiAdapter()
        self._big_query_client = BigQueryAdapter(
            self.project_name,
            app_config["dataset_location"],
            self.dataset_name,
        )
        self.cloud_task_publisher = CloudTaskPublisher(
            self.project_name, self.service_location, self.queue
        )

    def handle_cloud_task(self, task_data: FetchResourcesTaskData):
        """
        Processes the task data, interacts with the Datacatalog API, and writes
        results to BigQuery.
        """
        match task_data.resource_type:
            case "entry_group":
                api_result, next_page_token = (
                    self.api_client.search_entry_groups(
                        [task_data.scope],
                        task_data.is_transferred,
                        page_token=task_data.next_page_token,
                    )
                )
                table_name = TableNames.ENTRY_GROUPS
            case "tag_template":
                api_result, next_page_token = (
                    self.api_client.search_tag_templates(
                        [task_data.scope],
                        task_data.is_public,
                        task_data.is_transferred,
                        page_token=task_data.next_page_token,
                    )
                )
                table_name = TableNames.TAG_TEMPLATES
            case _:
                api_result, next_page_token = (None, None)

        if api_result:
            self._big_query_client.write_entities_to_table(
                table_name,
                api_result,
                task_data.created_at,
            )

        if next_page_token:
            payload_data = task_data.model_dump().copy()
            payload_data["next_page_token"] = next_page_token

            payload = FetchResourcesTaskData(**payload_data).model_dump(
                mode="json"
            )

            self.cloud_task_publisher.create_task(
                payload,
                self.handler_name,
                self.project_name,
                self.service_location,
            )

        return {"message": "Task processed"}, 200
