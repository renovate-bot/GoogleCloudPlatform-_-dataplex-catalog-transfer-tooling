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
Fetch Projects handler tests
"""

import datetime
import random
from typing import Generator

import pytest
from googleapiclient.errors import HttpError

from services.handlers.fetch_projects.handler import CloudTaskHandler
from tests.mocks.api.resource_manager_api_mock import ResourceManagerApiMock
from common.entities.request_models import FetchProjectsTaskData
from common.big_query import BigQueryAdapter


class TestFetchProjectsHandler:
    """
    Fetch Projects handler tests
    """

    @pytest.fixture(scope="class")
    def full_config(self, basic_config: dict) -> dict:
        """
        Generates a full configuration by appending a random suffix
        to the dataset name.
        """
        suffix = random.randint(1, 1000000)
        basic_config["dataset_name"] += "_" + str(suffix)

        return basic_config

    @pytest.fixture(scope="class", autouse=True)
    def big_query_client(self, full_config: dict) -> Generator:
        """
        Provides a BigQuery client for testing, and ensures cleanup after tests.
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
        Creates a CloudTaskHandler instance with a mocked Resource
        Manager API client.
        """
        handler = CloudTaskHandler(full_config)
        handler._resource_manager_client._plain_api_client = (
            ResourceManagerApiMock()
        )

        return handler

    def test_fetch_projects_handler_org(
        self, handler: CloudTaskHandler, big_query_client: BigQueryAdapter
    ) -> None:
        """
        Tests the handler for processing projects under an organization.
        """
        request = FetchProjectsTaskData(
            project_id="prj1",
            project_number="111111",
            dataplex_api_enabled=False,
            data_catalog_api_enabled=False,
            created_at=datetime.date.fromisoformat("2025-01-01"),
        )
        table_name = "projects"
        response = handler.handle_cloud_task(request)
        assert response == ({"message": "Task processed"}, 200)

        rows = list(
            big_query_client._select_data_from_partition(
                big_query_client._get_table_ref(table_name),
                big_query_client._get_target_creation_date(
                    big_query_client._get_table_ref(table_name)
                ),
            ).result()
        )

        assert len(rows) == 1

        row = rows[0]
        assert row.projectId == "prj1"
        assert row.projectNumber == 111111
        assert row.isDataplexApiEnabled is False
        assert row.isDataCatalogApiEnabled is False
        assert row.createdAt == datetime.date.fromisoformat("2025-01-01")
        assert row.ancestry == [
            {"type": "FOLDER", "id": "folder11"},
            {"type": "ORGANIZATION", "id": "org111"},
        ]

    def test_fetch_projects_handler_folder(
        self, handler: CloudTaskHandler, big_query_client: BigQueryAdapter
    ) -> None:
        """
        Tests the handler for processing projects under a folder.
        """
        request = FetchProjectsTaskData(
            project_id="prj2",
            project_number="222222",
            dataplex_api_enabled=True,
            data_catalog_api_enabled=False,
            created_at=datetime.date.fromisoformat("2025-02-02"),
        )

        table_name = "projects"
        response = handler.handle_cloud_task(request)
        assert response == ({"message": "Task processed"}, 200)

        rows = list(
            big_query_client._select_data_from_partition(
                big_query_client._get_table_ref(table_name),
                big_query_client._get_target_creation_date(
                    big_query_client._get_table_ref(table_name)
                ),
            ).result()
        )

        assert len(rows) == 1

        row = rows[0]
        assert row.projectId == "prj2"
        assert row.projectNumber == 222222
        assert row.isDataplexApiEnabled is True
        assert row.isDataCatalogApiEnabled is False
        assert row.createdAt == datetime.date.fromisoformat("2025-02-02")
        assert row.ancestry == [{"type": "ORGANIZATION", "id": "org222"}]

    def test_fetch_projects_handler_project(
        self, handler: CloudTaskHandler, big_query_client: BigQueryAdapter
    ) -> None:
        """
        Tests the handler for processing individual projects.
        """
        request = FetchProjectsTaskData(
            project_id="prj3",
            project_number="333333",
            dataplex_api_enabled=False,
            data_catalog_api_enabled=True,
            created_at=datetime.date.fromisoformat("2025-03-03"),
        )

        table_name = "projects"
        response = handler.handle_cloud_task(request)
        assert response == ({"message": "Task processed"}, 200)

        rows = list(
            big_query_client._select_data_from_partition(
                big_query_client._get_table_ref(table_name),
                big_query_client._get_target_creation_date(
                    big_query_client._get_table_ref(table_name)
                ),
            ).result()
        )

        assert len(rows) == 1

        row = rows[0]
        assert row.projectId == "prj3"
        assert row.projectNumber == 333333
        assert row.isDataplexApiEnabled is False
        assert row.isDataCatalogApiEnabled is True
        assert row.createdAt == datetime.date.fromisoformat("2025-03-03")
        assert row.ancestry == []

    def test_fetch_projects_handler_404(
        self, handler: CloudTaskHandler
    ) -> None:
        """
        Tests the handler's behavior when a project is not found (HTTP 404).
        """
        request = FetchProjectsTaskData(
            project_id="prj4",
            project_number="444444",
            dataplex_api_enabled=True,
            data_catalog_api_enabled=True,
            created_at=datetime.date.fromisoformat("2025-01-01"),
        )
        try:
            _ = handler.handle_cloud_task(request)
        except HttpError as e:
            assert e.status_code == 403
