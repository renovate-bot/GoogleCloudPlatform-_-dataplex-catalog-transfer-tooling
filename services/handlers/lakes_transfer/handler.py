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
This module provides a FastAPI handler for cloud tasks.
Its purpose is to restore the ability to search for Dataplex Catalog entries
by their parent lake or zone. It achieves this by processing tasks that add a
'lake-details' aspect to entries.
"""
import re

from common.api import DataplexApiAdapter, ResourceManagerApiAdapter
from common.entities import DataplexEntries
from common.utils import get_logger
from common.big_query import BigQueryAdapter


class CloudTaskHandler:
    """
    Handles cloud tasks to restore search functionality for Dataplex entries.
    """

    def __init__(self) -> None:
        """
        Initializes the CloudTaskHandler.
        """
        self._dataplex_client = DataplexApiAdapter()
        self._rm_adapter = ResourceManagerApiAdapter()
        self._logger = get_logger()

    def handle_cloud_task(
        self, task_data: DataplexEntries
    ) -> tuple[dict[str, str], int]:
        """
        Processes a cloud task to make Dataplex entries searchable.
        """
        match task_data.type:
            case "TABLE":
                self.handle_table_entity(task_data)
            case "ASSET":
                self.handle_bigquery_dataset(task_data)
            case _:
                raise ValueError(f"Unknown entry type: {task_data.type}")

        return {"message": "Task processed"}, 200

    def handle_table_entity(self, task_data: DataplexEntries) -> None:
        """
        Processes a Dataplex table entity and updates its entry
        with lake and zone details.
        """
        pattern = (
            r"projects/(?P<project_number>[^/]+)/"
            r"locations/(?P<location>[^/]+)/"
            r"lakes/(?P<lake_id>[^/]+)/"
            r"zones/(?P<zone_id>[^/]+)/"
            r"entities/(?P<entity_id>[^/]+)"
        )
        match = re.match(pattern, task_data.fqn)

        if match is None:
            raise ValueError(f"Incorrect entity name: {task_data.fqn}")

        lake = match.group("lake_id")
        zone = match.group("zone_id")
        entity_id = match.group("entity_id")
        entity_project_id = self._rm_adapter.get_project_id(
            match.group("project_number")
        )

        data_path = task_data.data_path
        parsed_data = BigQueryAdapter.parse_bigquery_table_path(data_path)

        if parsed_data:
            project_id = parsed_data["projectId"]
            dataset_id = parsed_data["datasetId"].replace("-", "_")
            table_id = parsed_data["tableId"]

            location = BigQueryAdapter.get_dataset_location(
                project_id, dataset_id
            )

            bigquery_resource_name = (
                f"bigquery.googleapis.com/projects/{project_id}/"
                f"datasets/{dataset_id}/tables/{table_id}"
            )
        else:
            dataset_id = zone.replace("-", "_")

            location = BigQueryAdapter.get_dataset_location(
                entity_project_id, dataset_id
            )

            bigquery_resource_name = (
                f"bigquery.googleapis.com/projects/{entity_project_id}/"
                f"datasets/{dataset_id}/tables/{entity_id}"
            )

        dataplex_entry_prefix = (
            f"projects/{entity_project_id}/locations/"
            f"{location}/entryGroups/@bigquery/entries/"
        )

        entry_resource_name = dataplex_entry_prefix + bigquery_resource_name

        self._dataplex_client.update_entry(
            entry_resource_name,
            task_data.aspect_type_project,
            lake,
            zone,
        )

    def handle_bigquery_dataset(self, task_data: DataplexEntries) -> None:
        """
        Processes a Dataplex BigQuery dataset entity and updates its entry
        with lake and zone details.
        """
        pattern = (
            r"projects/(?P<project_number>[^/]+)/"
            r"locations/(?P<location>[^/]+)/"
            r"lakes/(?P<lake_id>[^/]+)/"
            r"zones/(?P<zone_id>[^/]+)/"
            r"assets/(?P<assets_id>[^/]+)"
        )
        match = re.match(pattern, task_data.fqn)

        if match is None:
            raise ValueError(f"Incorrect entity name: {task_data.fqn}")

        zone_id = match.group("zone_id")
        lake_id = match.group("lake_id")

        project, dataset_name = BigQueryAdapter.parse_dataset_path(
            task_data.data_path
        )

        location = BigQueryAdapter.get_dataset_location(project, dataset_name)

        dataplex_entry_prefix = (
            f"projects/{project}/locations/"
            f"{location}/entryGroups/@bigquery/entries/"
        )

        entry_resource_name = (
            dataplex_entry_prefix
            + "bigquery.googleapis.com/"
            + task_data.data_path
        )

        self._dataplex_client.update_entry(
            entry_resource_name,
            task_data.aspect_type_project,
            lake_id,
            zone_id,
        )
