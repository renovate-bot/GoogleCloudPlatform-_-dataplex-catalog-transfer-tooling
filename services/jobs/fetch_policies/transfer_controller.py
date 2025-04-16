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

from common.big_query import BigQueryAdapter
from common.cloud_task import CloudTaskPublisher
from common.entities import EntryGroup, TagTemplate, FetchPoliciesTaskData
from common.api.resource_manager_api_adapter import ResourceManagerApiAdapter
from common.utils import get_logger


class TransferController:
    """
    Manages the data transfer process by fetching resources and creating
    Cloud Tasks for processing.
    """

    def __init__(self, app_config: dict):
        """
        Initializes the TransferController with the specified project.
        """
        self.project_name = app_config["project_name"]
        self.resource_types = app_config["resource_types"]
        self.managing_systems = app_config["managing_systems"]
        self.location = app_config["service_location"]
        self.handler_name = app_config["handler_name"]
        self.queue = app_config["queue"]
        self._resource_manager_client = ResourceManagerApiAdapter()
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
            self.managing_systems,
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
        scope: tuple[str, int]
    ) -> tuple[tuple[list[EntryGroup], date], tuple[list[TagTemplate], date]]:
        """
        Fetches entry groups and tag templates from BigQuery tables."
        """
        entry_groups = self._big_query_client.get_entry_groups_for_policies(
            scope, managing_systems
        ) if "entry_group" in resource_types else None
        tag_templates = self._big_query_client.get_tag_templates_for_policies(
            scope, managing_systems
        ) if "tag_template" in resource_types else None

        return entry_groups, tag_templates

    def create_cloud_tasks(
        self,
        resources_date: tuple[list[EntryGroup | TagTemplate], date],
    ) -> None:
        """
        Creates Cloud Tasks for the given resources.
        """
        resources, created_at = resources_date

        if not self._cloud_task_client.check_queue_exists():
            self._cloud_task_client.create_queue()

        tasks = []
        results = []

        for resource in resources:
            payload_data = {
                "resource_type": type(resource).__name__,
                "created_at": created_at,
                "resource": {
                    "resource_name": resource.id,
                    "location": resource.location,
                    "project_id": resource.project_id,
                    "system": resource.managing_system,
                },
            }

            payload = FetchPoliciesTaskData(**payload_data).model_dump(
                mode="json"
            )

            with ThreadPoolExecutor(max_workers=100) as executor:
                tasks.append(executor.submit(
                    self.create_cloud_task,
                    payload,
                    self.handler_name,
                    self.project_name,
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
        location: str,
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
