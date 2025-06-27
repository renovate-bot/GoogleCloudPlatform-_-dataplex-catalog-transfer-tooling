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
This module defines the `TransferController` class, which manages the data
transfer process. It fetches resources from BigQuery tables and creates
Cloud Tasks to process them.
"""

from datetime import date
from concurrent.futures import ThreadPoolExecutor, as_completed

from google.api_core.exceptions import GoogleAPICallError
from google.cloud.tasks_v2 import Task

from common.big_query import BigQueryAdapter
from common.cloud_task import CloudTaskPublisher
from common.entities import EntryGroup, TagTemplate, ResourceTaskData
from common.utils import get_logger


class TransferController:
    """
    Manages the data transfer process by fetching resources and creating
    Cloud Tasks for processing.
    """

    def __init__(self, app_config: dict) -> None:
        """
        Initializes the TransferController with the specified project.
        """
        self.project_name = app_config["project_name"]
        self.resource_types = app_config["resource_types"]
        self.location = app_config["service_location"]
        self.handler_name = app_config["handler_name"]
        self.queue = app_config["queue"]
        self.scope = app_config["scope"]
        self._big_query_client = BigQueryAdapter(
            self.project_name,
            app_config["dataset_location"],
            app_config["dataset_name"],
        )
        self._cloud_task_client = CloudTaskPublisher(
            self.project_name, self.location, self.queue, max_rps=2
        )
        self._logger = get_logger()

    def start_transfer(self) -> None:
        """
        Initiates the data transfer process by fetching resources
        from tables and creating tasks
        """
        entry_groups, tag_templates = self.fetch_resources(
            self.resource_types,
            ["DATAPLEX"],
            self.scope,
        )
        if entry_groups is not None:
            self.create_cloud_tasks(entry_groups)
        if tag_templates is not None:
            self.create_cloud_tasks(tag_templates)

    def fetch_resources(
        self,
        resource_types: list[str],
        managing_systems: list[str],
        scope: dict,
    ) -> tuple[
        tuple[list[EntryGroup], date] | None,
        tuple[list[TagTemplate], date] | None,
    ]:
        """
        Fetches entry groups and tag templates from BigQuery tables."
        """
        entry_groups = (
            self._big_query_client.get_entry_groups_within_scope(
                scope, managing_systems
            )
            if "entry_group" in resource_types
            else None
        )
        tag_templates = (
            self._big_query_client.get_tag_templates_within_scope(
                scope, managing_systems
            )
            if "tag_template" in resource_types
            else None
        )

        return entry_groups, tag_templates

    def create_cloud_tasks(
        self,
        resources_date: tuple[list[EntryGroup | TagTemplate], date],
    ) -> None:
        """
        Creates Cloud Tasks for the given resources.
        """
        resources, _ = resources_date

        if not self._cloud_task_client.check_queue_exists():
            self._cloud_task_client.create_queue()

        tasks = []
        error_counter = 0

        def process_futures(tasks: list) -> None:
            """
            Handle errors for a list of futures.
            """
            nonlocal error_counter
            for future in as_completed(tasks):
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
                            "system": resource.managing_system,
                        },
                    }
                ).model_dump(mode="json")

                tasks.append(
                    executor.submit(
                        self.create_cloud_task,
                        payload,
                        self.handler_name,
                        self.project_name,
                        self.location,
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
    ) -> Task | GoogleAPICallError:
        """
        Creates a single Cloud Task with the given payload.
        """
        try:
            return self._cloud_task_client.create_task(
                payload, handler_name, project, location
            )
        except GoogleAPICallError as e:
            return e
