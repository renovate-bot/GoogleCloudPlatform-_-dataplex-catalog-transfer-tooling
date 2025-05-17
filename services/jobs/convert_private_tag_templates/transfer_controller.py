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
This module implements the `TransferController` class to fetch private
tag templates from BigQuery and create Cloud Tasks for processing.
"""

from concurrent.futures import ThreadPoolExecutor, as_completed

from google.api_core.exceptions import GoogleAPICallError
from google.cloud.tasks_v2 import Task

from common.big_query import BigQueryAdapter
from common.cloud_task import CloudTaskPublisher
from common.entities import TagTemplate, ConvertPrivateTagTemplatesTaskData
from common.utils import get_logger


class TransferController:
    """
    A controller for fetching private tag templates from BigQuery and creating
    Cloud Tasks for their conversion and processing.
    """

    def __init__(self, app_config: dict) -> None:
        """
        Initializes the TransferController with the specified project.
        """
        self.project = app_config["project_name"]
        self.location = app_config["service_location"]
        self.handler_name = app_config["handler_name"]
        self.queue = app_config["queue"]
        self.scope = app_config["scope"]
        self.dataset_name = app_config["dataset_name"]
        self._big_query_client = BigQueryAdapter(
            self.project,
            app_config["dataset_location"],
            app_config["dataset_name"],
        )
        self._cloud_task_client = CloudTaskPublisher(
            self.project, self.location, self.queue, max_rps=5
        )
        self._logger = get_logger()

    def start_transfer(self) -> None:
        """
        Initiates the data transfer process by fetching private tag templates
        from BigQuery and creating Cloud Tasks for processing them.
        """
        private_tag_templates = (
            self._big_query_client.get_private_tag_templates(
                self.scope,
            )
        )

        if private_tag_templates is not None:
            self.create_cloud_tasks(private_tag_templates)

    def create_cloud_tasks(
        self,
        tag_templates: list[TagTemplate],
    ) -> None:
        """
        Creates Cloud Tasks for the given list of tag templates.
        """
        if not self._cloud_task_client.check_queue_exists():
            self._cloud_task_client.create_queue()

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

            for tt in tag_templates:
                payload = ConvertPrivateTagTemplatesTaskData(
                    **{
                        "resource_name": tt.id,
                        "location": tt.location,
                        "project_id": tt.project_id,
                    }
                ).model_dump(mode="json")

                tasks.append(
                    executor.submit(
                        self.create_cloud_task,
                        payload,
                        self.handler_name,
                        self.project,
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
        Creates a single Cloud Task with the specified payload and
        configuration.
        """
        try:
            return self._cloud_task_client.create_task(
                payload, handler_name, project, location
            )
        except GoogleAPICallError as e:
            return e
