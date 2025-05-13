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
from concurrent.futures import ThreadPoolExecutor, as_completed

from google.api_core.exceptions import GoogleAPICallError

from common.big_query import BigQueryAdapter, ViewNames, TableNames
from common.cloud_task import CloudTaskPublisher
from common.api.resource_manager_api_adapter import ResourceManagerApiAdapter
from common.entities import EntryGroup, TagTemplate, FindResourceNamesTaskData
from common.utils import get_logger


class TransferController:
    """
    A controller class for managing the transfer of data from the Google Cloud
    Data Catalog to BigQuery. It handles the retrieval of tag
    templates, and entry groups, finding new names
    and writes them to BigQuery tables.
    """

    def __init__(self, app_config: dict):
        """
        Initializes the TransferController with the specified project.
        """
        self.project = app_config["project_name"]
        self.location = app_config["service_location"]
        self.handler_name = app_config["handler_name"]
        self.queue = app_config["queue"]
        self.dataset_name = app_config["dataset_name"]
        self._resource_manager_client = ResourceManagerApiAdapter()
        self._big_query_client = BigQueryAdapter(
            self.project,
            app_config["dataset_location"],
            app_config["dataset_name"],
        )
        self._cloud_task_client = CloudTaskPublisher(
            self.project, self.location, self.queue, max_rps=2
        )
        self._logger = get_logger()

    def start_transfer(self):
        """
        Initiates the data transfer process by fetching resources
        from tables and creating tasks
        """
        self._setup_tables_and_views()

        entry_groups, tag_templates = self.fetch_resources()

        self._logger.info(f"fetched {len(entry_groups)} entry groups")
        self._logger.info(f"fetched {len(tag_templates)} tag templates")

        self.create_cloud_tasks(entry_groups + tag_templates)

    def _setup_tables_and_views(self):
        """
        Ensures that all required BigQuery tables and views are set up for the
        data transfer process. If any of the required tables or views do not
        exist, they are created.
        """
        required_tables = [
            TableNames.ENTRY_GROUPS_RESOURCE_MAPPING,
            TableNames.TAG_TEMPLATES_RESOURCE_MAPPING,
        ]
        required_views = [
            ViewNames.ENTRY_GROUPS_VIEW,
            ViewNames.TAG_TEMPLATES_VIEW,
        ]

        for table_name in required_tables:
            table_id = f"{self.project}.{self.dataset_name}.{table_name}"
            self._big_query_client.create_table_if_not_exists(table_id)

        for view_name in required_views:
            view_id = f"{self.project}.{self.dataset_name}.{view_name}"
            self._big_query_client.create_view_if_not_exists(view_id)

    def fetch_resources(
        self,
    ) -> tuple[list[EntryGroup], list[TagTemplate]]:
        """
        Fetches resources from tables and creating tasks
        """
        entry_groups = self._big_query_client.select_entry_groups()
        tag_templates = self._big_query_client.select_tag_templates()

        return entry_groups, tag_templates

    def create_cloud_tasks(
        self,
        resources: list[EntryGroup | TagTemplate],
    ):
        """
        Create cloud tasks for further processing
        """
        if not self._cloud_task_client.check_queue_exists():
            self._cloud_task_client.create_queue()

        tasks = []
        results = []

        for resource in resources:
            payload_data = {
                "resource_type": type(resource).__name__,
                "resource": {
                    "resource_name": resource.id,
                    "location": resource.location,
                    "project_id": resource.project_id,
                },
            }

            payload = FindResourceNamesTaskData(**payload_data).model_dump(
                mode="json"
            )

            with ThreadPoolExecutor(max_workers=100) as executor:
                tasks.append(executor.submit(
                    self.create_cloud_task,
                    payload,
                    self.handler_name,
                    self.project,
                    self.location,
                ))
                for future in as_completed(tasks):
                    try:
                        results.append(future.result())
                    except Exception as exc:
                        results.append(exc)

        errors = list(filter(lambda t: isinstance(t, Exception), results))

        if len(errors) == 0:
            self._logger.info("All tasks created")
        else:
            self._logger.info(f"{len(errors)} errors occurred"
                              f" during tasks creation")

    def create_cloud_task(
        self,
        payload: dict,
        handler_name: str,
        project: str,
        location: str
    ):
        try:
            return self._cloud_task_client.create_task(
                payload,
                handler_name,
                project,
                location
            )
        except GoogleAPICallError as e:
            return e
