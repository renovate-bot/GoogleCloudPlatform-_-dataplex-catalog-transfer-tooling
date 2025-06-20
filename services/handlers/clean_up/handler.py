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

from google.api_core.exceptions import PermissionDenied
from google.cloud.datacatalog_v1.types.tags import TagTemplate as DataCatalogTagTemplate

from common.api import DatacatalogApiAdapter
from common.entities import ResourceTaskData, EntryGroup, TagTemplate
from common.exceptions import IncorrectTypeException
from common.utils import get_logger


class CloudTaskHandler:
    """
    A handler class for processing cloud tasks related to project management.
    """

    def __init__(self) -> None:
        """
        Initializes the CloudTaskHandler
        """
        self._datacatalog_client = DatacatalogApiAdapter()
        self._logger = get_logger()

    def handle_cloud_task(
        self, task_data: ResourceTaskData
    ) -> tuple[dict[str, str], int]:
        """
        Processes a single cloud task and transfer resource to Dataplex
        """
        resource_type = task_data.resource_type
        resource_data = task_data.resource

        project_id = resource_data.project_id
        location = resource_data.location
        resource_name = resource_data.resource_name

        try:
            if resource_type == EntryGroup.__name__:
                fqn = EntryGroup.get_old_fqn(
                    project_id, location, resource_name
                )
                entry_group = self._datacatalog_client.get_entry_group(
                    project_id, location, resource_name
                )
                if not entry_group.transferred_to_dataplex:
                    self._logger.info("Entry group %s not transferred", fqn)
                    return {
                        "message": f"Entry group {fqn} not transferred"
                    }, 200

                self._datacatalog_client.delete_entry_group(
                    project_id, location, resource_name, True
                )
            elif resource_type == TagTemplate.__name__:
                fqn = TagTemplate.get_old_fqn(
                    project_id, location, resource_name
                )
                tag_template = self._datacatalog_client.get_tag_template(
                    project_id, location, resource_name
                )
                if (tag_template.dataplex_transfer_status
                != DataCatalogTagTemplate.DataplexTransferStatus.TRANSFERRED):
                    self._logger.info("Tag template %s not transferred", fqn)
                    return {
                        "message": f"Tag template {fqn} not transferred"
                    }, 200

                self._datacatalog_client.delete_tag_template(
                    project_id, location, resource_name, True
                )
            else:
                raise IncorrectTypeException(
                    f"Unknown resource type: " f"{task_data.resource_type}"
                )
        except PermissionDenied:
            return {"message": f"Resource {fqn} not found"}, 200

        return {"message": "Task processed"}, 200
