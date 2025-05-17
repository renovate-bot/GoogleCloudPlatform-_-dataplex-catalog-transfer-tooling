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
This module provides a Fastapi handler that handles cloud tasks related to
project management and data processing. It integrates with
ResourceManagerApiAdapter and BigQueryAdapter to manage project data and
store it in BigQuery.
"""

from common.api import ResourceManagerApiAdapter
from common.entities import Project, FetchProjectsTaskData
from common.big_query import BigQueryAdapter, TableNames


class CloudTaskHandler:
    """
    A handler class for processing cloud tasks related to project management.
    """

    def __init__(self, app_config: dict) -> None:
        """
        Initializes the CloudTaskHandler with ResourceManagerApiAdapter and
        BigQueryAdapter clients.
        """
        self.project_name = app_config["project_name"]
        self.dataset_name = app_config["dataset_name"]
        self._resource_manager_client = ResourceManagerApiAdapter()
        self._big_query_client = BigQueryAdapter(
            self.project_name, app_config["dataset_location"], self.dataset_name
        )

    def handle_cloud_task(
        self, task_data: FetchProjectsTaskData
    ) -> tuple[dict[str, str], int]:
        """
        Processes a single cloud task by extracting project information,
        fetching project ancestry, and writing the project data to a BigQuery
        table.
        """
        project = Project(task_data.project_id, task_data.project_number)
        project.set_dataplex_api_enabled(task_data.dataplex_api_enabled)
        project.set_data_catalog_api_enabled(task_data.data_catalog_api_enabled)

        ancestors = self._resource_manager_client.get_project_ancestry(
            project.project_id
        )
        project.set_ancestry(ancestors)

        self._big_query_client.write_entities_to_table(
            TableNames.PROJECTS,
            [project],
            creation_date=task_data.created_at,
        )

        return {"message": "Task processed"}, 200
