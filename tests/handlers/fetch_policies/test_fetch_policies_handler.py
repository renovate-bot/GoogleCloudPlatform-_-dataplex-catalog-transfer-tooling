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
Fetch Policies handler tests
"""

import datetime
import random

from typing import Generator
import pytest
from google.cloud.bigquery import table

from services.handlers.fetch_policies.handler import CloudTaskHandler
from tests.mocks.api.datacatalog_api_mock import DatacatalogApiMock
from tests.mocks.api.dataplex_api_mock import DataplexApiMock
from common.entities.request_models import (
    FetchPoliciesTaskData,
    ExtendedResourceData,
)
from common.big_query import BigQueryAdapter, TableNames


class TestFetchPoliciesHandler:
    """
    Fetch Policies handler tests
    """

    @pytest.fixture(scope="class")
    def full_config(self, basic_config: dict) -> dict:
        """
        Generates a full configuration with a unique dataset name.
        """
        suffix = random.randint(1, 1000000)
        basic_config["dataset_name"] += "_" + str(suffix)

        return basic_config

    @pytest.fixture(scope="class", autouse=True)
    def big_query_client(self, full_config: dict) -> Generator:
        """
        Provides a BigQuery client and ensures cleanup after tests.
        """
        big_query_client = BigQueryAdapter(
            full_config["project_name"],
            full_config["dataset_location"],
            full_config["dataset_name"],
        )
        yield big_query_client
        big_query_client.delete_dataset()

    @pytest.fixture(scope="class")
    def handler(self, full_config: dict) -> CloudTaskHandler:
        """
        Provides a CloudTaskHandler instance with mocked clients.
        """
        handler = CloudTaskHandler(full_config)
        test_data = {
            "projects/p1/locations/l1/entryGroups/e1": {
                "role1": ["m1", "m2"],
                "role2": ["m3"],
            }
        }

        handler._datacatalog_client._client = DatacatalogApiMock(test_data)
        handler._dataplex_client._plain_client = DataplexApiMock(test_data)

        return handler

    @pytest.mark.parametrize(
        "system, resource",
        [
            ("DATA_CATALOG", "e1"),
            ("DATA_CATALOG", "e2"),
            ("DATAPLEX", "e1"),
            ("DATAPLEX", "e2"),
        ],
    )
    def test_fetch_policies_handler(
        self,
        handler: CloudTaskHandler,
        big_query_client: BigQueryAdapter,
        system: str,
        resource: str,
    ) -> None:
        """
        Tests the Fetch Policies handler for various systems and resources.
        """
        request = FetchPoliciesTaskData(
            resource_type="EntryGroup",
            created_at=datetime.date.fromisoformat("2025-01-01"),
            resource=ExtendedResourceData(
                location="l1",
                resource_name=resource,
                project_id="p1",
                system=system,
            ),
        )

        response = handler.handle_cloud_task(request)
        assert response == ({"message": "Task processed"}, 200)

        table_name = TableNames.IAM_POLICIES
        rows = list(
            big_query_client._select_all_data_from_table(
                big_query_client._get_table_ref(table_name)
            ).result()
        )

        resource_name = f"projects/p1/locations/l1/entryGroups/{resource}"

        def filter_fun(x: table.Row) -> bool:
            return x.resourceName == resource_name and x.system == system

        row = next(filter(filter_fun, rows))
        bindings = row.bindings

        assert row.resourceName == resource_name
        assert row.system == system
        assert bindings == (
            [
                {
                    "role": "role1",
                    "members": ["m1", "m2"],
                },
                {
                    "role": "role2",
                    "members": ["m3"],
                },
            ]
            if resource == "e1"
            else []
        )
