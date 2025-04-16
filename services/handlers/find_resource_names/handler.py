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
A handler class for processing cloud tasks related to resource name mapping
between Data Catalog and Dataplex.
"""

from common.api import DataplexApiAdapter
from common.big_query import BigQueryAdapter, TableNames
from common.entities import (
    EntryGroup,
    TagTemplate,
    FindResourceNamesTaskData,
)
from common.utils import get_logger

class CloudTaskHandler:
    """
    A handler class for processing cloud tasks related to project management.
    """

    def __init__(self, app_config):
        """
        Initializes the CloudTaskHandler with ResourceManagerApiAdapter and
        BigQueryAdapter clients.
        """
        self.project_name = app_config["project_name"]
        self.dataset_name = app_config["dataset_name"]
        self._dataplex_client = DataplexApiAdapter()
        self._big_query_client = BigQueryAdapter(
            self.project_name,
            app_config["dataset_location"],
            self.dataset_name,
        )
        self._logger = get_logger()

    def handle_cloud_task(self, task_data: FindResourceNamesTaskData):
        """
        Processes a single cloud task by extracting project information,
        fetching new name, and writing the project data to a BigQuery
        table.
        """
        resource_type = task_data.resource_type
        resource_data = task_data.resource

        project_id = resource_data.project_id
        location = resource_data.location
        resource_name = resource_data.resource_name

        if resource_type == EntryGroup.__name__:
            table = TableNames.ENTRY_GROUPS_RESOURCE_MAPPING
            old_fqn = EntryGroup.get_old_fqn(
                project_id, location, resource_name
            )
            dataplex_name = self.find_new_entry_group_name(
                project_id, resource_name, location
            )
            data = {
                "dataCatalogResourceName": old_fqn,
                "dataplexResourceName": dataplex_name,
            }
        elif resource_type == TagTemplate.__name__:
            table = TableNames.TAG_TEMPLATES_RESOURCE_MAPPING
            old_fqn = TagTemplate.get_old_fqn(
                project_id, location, resource_name
            )
            dataplex_name = self.find_new_tag_template_name(
                project_id, resource_name, location
            )
            data = {
                "dataCatalogResourceName": old_fqn,
                "dataplexResourceName": dataplex_name,
            }
        else:
            self._logger.error(f"Invalid resource type: {resource_type}")
            return {"message": f"Invalid resource type: {resource_type}"}, 400

        if dataplex_name is None:
            self._logger.error(
                f"No dataplex resource found for {resource_name}"
            )
            return {"message": "Resource not found"}, 200

        self.write_to_table(table, data)
        return {"message": "Task processed"}, 200

    def find_new_entry_group_name(self, project_id, resource_name, location):
        fqn = EntryGroup.get_new_fqn(project_id, location, resource_name)
        entry_group = self._dataplex_client.get_entry_group(fqn=fqn)

        if entry_group is not None and entry_group.transfer_status is not None:
            return fqn

        self._logger.info(
            f"Didn't find entry group named {fqn}."
            f"Try to find with different name"
        )

        new_name = f"{resource_name}_{location}"
        fqn = EntryGroup.get_new_fqn(project_id, location, new_name)

        entry_group = self._dataplex_client.get_entry_group(fqn=fqn)
        if entry_group is not None and entry_group.transfer_status is not None:
            return fqn

        self._logger.info(f"No entry group named {fqn}.")

        return None

    def find_new_tag_template_name(self, project_id, resource_name, location):
        fqn = TagTemplate.get_new_fqn(project_id, location, resource_name)

        aspect_type = self._dataplex_client.get_aspect_type(fqn=fqn)

        if aspect_type is not None and "transferStatus" in aspect_type:
            return fqn

        self._logger.info(
            f"Didn't find aspect type named {fqn}."
            f"Try to find with different name"
        )

        new_name = f"{resource_name}_{location}"
        fqn = TagTemplate.get_new_fqn(project_id, location, new_name)

        aspect_type = self._dataplex_client.get_aspect_type(fqn=fqn)

        if aspect_type is not None and "transferStatus" in aspect_type:
            return fqn

        self._logger.info(f"No aspect type named {fqn}.")

        return None

    def write_to_table(
        self,
        table_name: str,
        row,
    ):
        table_id = f"{self.project_name}.{self.dataset_name}.{table_name}"
        self._big_query_client.write_to_table(table_id, [row])
