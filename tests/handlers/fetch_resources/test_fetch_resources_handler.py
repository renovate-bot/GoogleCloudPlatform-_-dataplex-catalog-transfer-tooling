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
Fetch Resources handler tests
"""

import json
import random
from typing import Generator
from datetime import date
from unittest.mock import MagicMock

import pytest

from common.big_query import BigQueryAdapter, TableNames
from common.cloud_task import CloudTaskPublisher
from common.entities import FetchResourcesTaskData, EntryGroup, TagTemplate
from services.handlers.fetch_resources.handler import CloudTaskHandler


class TestCloudTaskHandler:
    """
    Tests for the CloudTaskHandler class.
    """

    @pytest.fixture(scope="class")
    def full_config(self, basic_config: dict) -> None:
        """
        Extends the basic configuration with a unique queue name by
        appending a random suffix.
        """
        suffix = random.randint(1, 1000000)
        basic_config["queue"] += "-" + str(suffix)
        basic_config["dataset_name"] += "_" + str(suffix)

        return basic_config

    @pytest.fixture
    def mock_api_client(self) -> MagicMock:
        """
        Provides a mock for the DatacatalogApiAdapter.
        """
        return MagicMock()

    @pytest.fixture(scope="class", autouse=True)
    def big_query_client(self, full_config: dict) -> Generator:
        """
        Sets up a BigQuery client for the test environment and ensures
        cleanup after tests.
        """
        big_query_client = BigQueryAdapter(
            full_config["project_name"],
            full_config["dataset_location"],
            full_config["dataset_name"],
        )
        yield big_query_client
        big_query_client.delete_dataset()

    @pytest.fixture(scope="class")
    def cloud_task_client(self, full_config) -> Generator:
        """
        Sets up a Cloud Task client for the test environment and ensures
        cleanup after tests.
        """
        cloud_task_client = CloudTaskPublisher(
            full_config["project_name"],
            full_config["service_location"],
            full_config["queue"],
        )
        cloud_task_client.create_queue()
        yield cloud_task_client
        cloud_task_client.delete_queue()

    @pytest.fixture
    def handler(
        self,
        full_config: dict,
        big_query_client: BigQueryAdapter,
        mock_api_client: MagicMock,
        cloud_task_client: CloudTaskPublisher,
    ) -> CloudTaskHandler:
        """
        Provides an instance of CloudTaskHandler with mocked dependencies.
        """
        handler = CloudTaskHandler(full_config)
        handler.api_client = mock_api_client
        handler._big_query_client = big_query_client
        handler.cloud_task_publisher = cloud_task_client
        handler.cloud_task_publisher._wait_after_queue_creation = 5
        return handler

    def test_handle_cloud_task_entry_group(
        self,
        handler: CloudTaskHandler,
        mock_api_client: MagicMock,
        big_query_client: BigQueryAdapter,
        cloud_task_client: CloudTaskPublisher,
    ) -> None:
        """
        Tests the handle_cloud_task method for the 'entry_group' resource type.
        """
        task_data = FetchResourcesTaskData(
            scope="project1",
            resource_type="entry_group",
            is_transferred=False,
            next_page_token=None,
            created_at="2025-01-01",
        )

        table_name = TableNames.ENTRY_GROUPS

        mock_api_client.search_entry_groups.return_value = (
            [EntryGroup("project1", "us-west1", "eg1", False)],
            None,
        )

        response, status_code = handler.handle_cloud_task(task_data)

        assert status_code == 200
        assert response["message"] == "Task processed"

        mock_api_client.search_entry_groups.assert_called_once_with(
            ["project1"], False, page_token=None
        )

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
        assert (
            row.resourceName
            == "projects/project1/locations/us-west1/entryGroups/eg1"
        )
        assert row.projectId == "project1"
        assert row.location == "us-west1"
        assert row.entryGroupId == "eg1"
        assert row.managingSystem == "DATA_CATALOG"
        assert row.createdAt == date.fromisoformat("2025-01-01")

        assert cloud_task_client.check_queue_exists()
        messages = list(cloud_task_client.get_messages())
        assert len(messages) == 0

    def test_handle_cloud_task_tag_template_with_next_page_token(
        self,
        handler: CloudTaskHandler,
        mock_api_client: MagicMock,
        big_query_client: BigQueryAdapter,
        cloud_task_client: CloudTaskPublisher,
    ) -> None:
        """
        Tests the handle_cloud_task method for the 'tag_template' resource type
        with next_page_token.
        """
        task_data = FetchResourcesTaskData(
            scope="project1",
            resource_type="tag_template",
            next_page_token=None,
            is_transferred=False,
            created_at="2025-01-01",
            is_public=True,
        )

        table_name = TableNames.TAG_TEMPLATES

        next_page_token = "some_token"
        mock_api_client.search_tag_templates.return_value = (
            [
                TagTemplate(
                    project_id="project1",
                    location="us-west1",
                    tag_template_id="tt1",
                    public=True,
                    transferred=False,
                )
            ],
            next_page_token,
        )

        response, status_code = handler.handle_cloud_task(task_data)

        assert status_code == 200
        assert response["message"] == "Task processed"

        mock_api_client.search_tag_templates.assert_called_once_with(
            ["project1"], True, False, page_token=None
        )

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
        assert (
            row.resourceName
            == "projects/project1/locations/us-west1/tagTemplates/tt1"
        )
        assert row.projectId == "project1"
        assert row.location == "us-west1"
        assert row.tagTemplateId == "tt1"
        assert row.managingSystem == "DATA_CATALOG"
        assert row.isPubliclyReadable is True
        assert row.createdAt == date.fromisoformat("2025-01-01")

        assert cloud_task_client.check_queue_exists()
        messages = list(cloud_task_client.get_messages())
        assert len(messages) == 1

        results = [json.loads(msg.http_request.body) for msg in messages]
        result = results[0]
        assert result["scope"] == task_data.scope
        assert result["resource_type"] == task_data.resource_type
        assert result["next_page_token"] == next_page_token
        assert result["is_transferred"] == task_data.is_transferred
        assert result["created_at"] == task_data.created_at.isoformat()
        assert result["is_public"] == task_data.is_public
