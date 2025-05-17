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

from google.api_core.exceptions import PermissionDenied, InvalidArgument

from common.api import DatacatalogApiAdapter
from common.big_query import BigQueryAdapter
from common.entities import ResourceTaskData, EntryGroup, TagTemplate
from common.exceptions import IncorrectTypeException
from common.utils import get_logger


class CloudTaskHandler:
    """
    A handler class for processing cloud tasks related to project management.
    """

    def __init__(self, app_config: dict) -> None:
        """
        Initializes the CloudTaskHandler
        """
        self.project_name = app_config["project_name"]
        self.dataset_name = app_config["dataset_name"]
        self._datacatalog_client = DatacatalogApiAdapter()
        self._big_query_client = BigQueryAdapter(
            self.project_name,
            app_config["dataset_location"],
            self.dataset_name,
        )
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

        if resource_type == EntryGroup.__name__:
            fqn = EntryGroup.get_old_fqn(project_id, location, resource_name)
            response = self._datacatalog_client.transfer_entry_group(fqn)
        elif resource_type == TagTemplate.__name__:
            fqn = TagTemplate.get_old_fqn(project_id, location, resource_name)
            response = self._datacatalog_client.transfer_tag_template(fqn)
        else:
            raise IncorrectTypeException(
                f"Unknown resource type: " f"{task_data.resource_type}"
            )

        if not isinstance(response, Exception):
            return {"message": "Task processed"}, 200
        elif isinstance(response, PermissionDenied):
            return {"message": f"Resource {fqn} not found"}, 200
        elif isinstance(response, InvalidArgument):
            return {"message": f"Resource {fqn} already transferred"}, 200
        else:
            return {"message": f"Error occurred {response}"}, 500
