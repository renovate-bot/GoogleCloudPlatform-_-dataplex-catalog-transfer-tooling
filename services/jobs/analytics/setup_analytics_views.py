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
This module defines the `TransferController` class, which is responsible for
managing the creation of analytical views in BigQuery. It interacts with the
BigQueryAdapter to ensure views are created if they do not already exist.

The `TransferController` is initialized with application configuration settings
and provides functionality to create predefined analytical views.
"""

from common.big_query import BigQueryAdapter, ViewNames, TableNames
from common.exceptions import MissingTablesOrViewsError
from common.utils import get_logger


class TransferController:
    """
    The `TransferController` class is responsible for managing the creation of
    analytical views in BigQuery. It uses the `BigQueryAdapter` to interact with
    BigQuery and ensures that required views are created if they do not already
    exist.
    """

    def __init__(self, app_config: dict) -> None:
        """
        Initializes the TransferController with the specified project.
        """
        self.project = app_config["project_name"]
        self.location = app_config["dataset_location"]
        self.dataset_name = app_config["dataset_name"]
        self._big_query_client = BigQueryAdapter(
            self.project,
            self.location,
            self.dataset_name,
        )
        self._logger = get_logger()

    def create_analytical_views(self) -> None:
        """
        Creates predefined analytical views in BigQuery if they do not
        already exist.
        """

        required_tables_and_views = [
            TableNames.CLOUDAUDIT_GOOGLEAPIS_DATA_ACCESS,
            TableNames.IAM_POLICIES,
            ViewNames.TAG_TEMPLATES_VIEW,
            ViewNames.ENTRY_GROUPS_VIEW,
        ]

        views_to_create = [
            ViewNames.RESOURCE_INTERACTIONS,
            ViewNames.RESOURCE_INTERACTIONS_SUMMARY,
            ViewNames.IAM_POLICIES_COMPARISON,
        ]

        missing_tables_or_views = []

        for table_id in required_tables_and_views:
            ref = self._big_query_client._get_table_ref(table_id)
            if not self._big_query_client.check_if_table_or_view_exists(
                ref
            ):
                missing_tables_or_views.append(ref)

        if missing_tables_or_views:
            for name in missing_tables_or_views:
                self._logger.warning(
                    "Required table or view is missing: %s.",
                    name,
                )
            raise MissingTablesOrViewsError(
                "The following required tables or views are missing: "
                f"{', '.join(missing_tables_or_views)}. "
                "Please ensure they exist before proceeding."
            )

        for view_id in views_to_create:
            self._logger.info(
                "Creating view: %s.%s.%s",
                self.project,
                self.dataset_name,
                view_id,
            )
            view_ref = self._big_query_client._get_table_ref(view_id)
            self._big_query_client.create_view_if_not_exists(view_ref)
