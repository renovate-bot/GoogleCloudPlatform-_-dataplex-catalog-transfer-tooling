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
from google.cloud.tasks_v2 import Task

from common.api import QuotaInfoAdapter, Services, Quotas
from common.big_query import BigQueryAdapter, ViewNames, TableNames
from common.cloud_task import CloudTaskPublisher
from common.api.resource_manager_api_adapter import ResourceManagerApiAdapter
from common.entities import EntryGroup, TagTemplate, ResourceTaskData
from common.utils import get_logger


class TransferController:
    """
    A controller class for managing the transfer of data from the Google Cloud
    Data Catalog to BigQuery. It handles the retrieval of tag
    templates, and entry groups, finding new names
    and writes them to BigQuery tables.
    """

    def __init__(self, app_config: dict) -> None:
        """
        Initializes the TransferController with the specified project.
        """
        self.project = app_config["project_name"]
        self.location = app_config["service_location"]
        self.handler_name = app_config["handler_name"]
        self.queue = app_config["queue"]
        self.dataset_name = app_config["dataset_name"]
        self.quota_consumption = app_config["quota_consumption"]
        self._quota_client = QuotaInfoAdapter()
        self.default_dataplex_quota = self._get_default_dataplex_quota()
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

    def _get_default_dataplex_quota(self) -> int:
        """
        Retrieves the default Dataplex quota for the project.
        """
        dataplex_quota_per_min = (
                self._quota_client.get_default_quota_value(
                    self.project,
                    Services.DATAPLEX,
                    Quotas.CATALOG_MANAGEMENT_READS,
                )
            )
        dataplex_quota_per_min_per_user = (
            self._quota_client.get_default_quota_value(
                self.project,
                Services.DATAPLEX,
                Quotas.CATALOG_MANAGEMENT_PER_USER_READS,
            )
        )

        dataplex_quota = min(
            dataplex_quota_per_min, dataplex_quota_per_min_per_user
        )
        return dataplex_quota

    def start_transfer(self) -> None:
        """
        Initiates the data transfer process by fetching resources
        from tables and creating tasks
        """
        self._setup_tables_and_views()

        entry_groups, tag_templates = self.fetch_resources()

        self._logger.info("Fetched %d entry groups", len(entry_groups))
        self._logger.info("Fetched %d tag templates", len(tag_templates))

        self.create_cloud_tasks(entry_groups + tag_templates)

    def _setup_tables_and_views(self) -> None:
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
    ) -> None:
        """
        Create cloud tasks for further processing
        """
        locations = set(map(lambda x: x.location, resources))
        self._cloud_task_client.prepare_queues_for_locations(
            locations, self.default_dataplex_quota, self.quota_consumption
        )

        tasks = []
        error_counter = 0

        def process_futures(futures: list) -> None:
            """
            Handle errors for a list of futures.
            """
            nonlocal error_counter
            for future in as_completed(futures):
                try:
                    result = future.result()
                    if isinstance(result, Exception):
                        raise result
                except Exception as exc:
                    error_counter += 1
                    self._logger.error(exc)

        with ThreadPoolExecutor(max_workers=10) as executor:
            chunk_size = 10_000
            cur_chunk_size = 0

            for resource in resources:
                payload = ResourceTaskData(
                    **{
                        "resource_type": type(resource).__name__,
                        "resource": {
                            "resource_name": resource.id,
                            "location": resource.location,
                            "project_id": resource.project_id,
                        },
                    }
                ).model_dump(mode="json")

                tasks.append(
                    executor.submit(
                        self.create_cloud_task,
                        payload,
                        self.handler_name,
                        self.project,
                        self.location,
                        resource.location,
                    )
                )

                cur_chunk_size += 1

                if cur_chunk_size == chunk_size:
                    cur_chunk_size = 0
                    process_futures(tasks)
                    tasks = []

            if tasks:
                process_futures(tasks)

        if error_counter == 0:
            self._logger.info("All tasks created")
        else:
            self._logger.error(
                "%d errors occurred during tasks creation", error_counter
            )

    def create_cloud_task(
        self,
        payload: dict,
        handler_name: str,
        project: str,
        location: str,
        msg_location: str,
    ) -> Task | GoogleAPICallError:
        """
        Creates a single Cloud Task with the specified payload and
        configuration.
        """
        try:
            return self._cloud_task_client.create_task_by_message_location(
                payload, handler_name, msg_location, project, location
            )
        except GoogleAPICallError as e:
            return e
