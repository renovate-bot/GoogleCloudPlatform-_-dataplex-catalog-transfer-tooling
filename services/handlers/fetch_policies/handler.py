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
from common.api import DataplexApiAdapter, DatacatalogApiAdapter
from common.big_query import BigQueryAdapter, TableNames
from common.entities import FetchPoliciesTaskData, EntryGroup, TagTemplate, ManagingSystem
from common.exceptions import IncorrectTypeException
from common.utils import get_logger


class CloudTaskHandler:
    """
    A handler class for processing cloud tasks related to project management.
    """

    def __init__(self, app_config):
        """
        Initializes the CloudTaskHandler
        """
        self.project_name = app_config["project_name"]
        self.dataset_name = app_config["dataset_name"]
        self._dataplex_client = DataplexApiAdapter()
        self._datacatalog_client = DatacatalogApiAdapter()
        self._big_query_client = BigQueryAdapter(
            self.project_name,
            app_config["dataset_location"],
            self.dataset_name,
        )
        self._logger = get_logger()

    def handle_cloud_task(self, task_data: FetchPoliciesTaskData):
        """
        Processes a single cloud task by extracting project information,
        fetching iam policies, and writing the project data to a BigQuery
        table.
        """
        policies = self.get_policies(task_data)
        resource = task_data.resource

        if task_data.resource_type == EntryGroup.__name__:
            fqn = EntryGroup.get_old_fqn(
                resource.project_id,
                resource.location,
                resource.resource_name,
            )
        elif task_data.resource_type == TagTemplate.__name__:
            fqn = TagTemplate.get_old_fqn(
                resource.project_id,
                resource.location,
                resource.resource_name,
            )
        else:
            raise IncorrectTypeException(f"Unknown resource type: "
                                         f"{task_data.resource_type}")
        data = {
            "resourceName": fqn,
            "system": task_data.resource.system,
            "bindings": policies
        }

        table_name = (f"{self.project_name}."
                      f"{self.dataset_name}."
                      f"{TableNames.IAM_POLICIES}")
        self._big_query_client.write_to_table(
            table_name,
            [data]
        )
        return {"message": "Task processed"}, 200

    def get_policies(self, task_data):
        resource_type = task_data.resource_type
        system = task_data.resource.system
        location = task_data.resource.location
        resource_id = task_data.resource.resource_name
        project_id = task_data.resource.project_id

        if system == ManagingSystem.DATA_CATALOG:
            return self._datacatalog_client.get_resource_policy(
                resource_type,
                project_id,
                location,
                resource_id,
            )
        elif system == ManagingSystem.DATAPLEX:
            return self._dataplex_client.get_resource_policy(
                resource_type,
                project_id,
                location,
                resource_id,
            )
        else:
            raise IncorrectTypeException(f"Unknown managing system: "
                                         f"{system}")
