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
Find Resource names handler tests
"""

import random
from datetime import date
from unittest.mock import MagicMock
from typing import Generator

import pytest
from google.cloud.dataplex_v1.types import catalog

from common.big_query import BigQueryAdapter, TableNames, ViewNames
from common.entities import (
    ResourceTaskData,
    ResourceData,
    EntryGroup,
    TagTemplate,
)
from services.handlers.find_resource_names.handler import CloudTaskHandler


class TestFindResourceNamesHandler:
    """
    Tests for the CloudTaskHandler class.
    """

    @pytest.fixture(scope="function")
    def full_config(self, basic_config: dict) -> dict:
        """
        Extends the basic configuration with a unique queue name by
        appending a random suffix.
        """
        suffix = random.randint(1, 1000000)
        basic_config["dataset_name"] += "_" + str(suffix)

        return basic_config

    @pytest.fixture
    def mock_dataplex_client(self) -> MagicMock:
        """
        Provides a mock for the DatacatalogApiAdapter.
        """
        return MagicMock()

    @pytest.fixture(scope="class")
    def test_data_eg(self) -> EntryGroup:
        """
        Provides test data for the entry groups.
        """
        return EntryGroup("project1", "us-west1", "eg1", False)

    @pytest.fixture(scope="class")
    def test_data_tt(self) -> TagTemplate:
        """
        Provides test data for the tag templates.
        """
        return TagTemplate("project1", "us-west1", "tt1", True, False)

    @pytest.fixture(scope="function")
    def big_query_client(
        self,
        full_config: dict,
        test_data_eg: EntryGroup,
        test_data_tt: TagTemplate,
    ) -> Generator:
        """
        Sets up a BigQuery client for the test environment and ensures
        cleanup after tests.
        """
        big_query_client = BigQueryAdapter(
            full_config["project_name"],
            full_config["dataset_location"],
            full_config["dataset_name"],
        )

        big_query_client.write_entities_to_table(
            TableNames.ENTRY_GROUPS,
            [test_data_eg],
            date.fromisoformat("2025-01-01"),
        )
        big_query_client.write_entities_to_table(
            TableNames.TAG_TEMPLATES,
            [test_data_tt],
            date.fromisoformat("2025-01-01"),
        )

        required_tables = [
            TableNames.ENTRY_GROUPS,
            TableNames.TAG_TEMPLATES,
            TableNames.ENTRY_GROUPS_RESOURCE_MAPPING,
            TableNames.TAG_TEMPLATES_RESOURCE_MAPPING,
        ]
        required_views = [
            ViewNames.ENTRY_GROUPS_VIEW,
            ViewNames.TAG_TEMPLATES_VIEW,
        ]
        for table_name in required_tables:
            table_id = (
                f"{big_query_client._project}."
                f"{big_query_client._dataset_name}.{table_name}"
            )
            big_query_client.create_table_if_not_exists(table_id)

        for view_name in required_views:
            view_id = (
                f"{big_query_client._project}."
                f"{big_query_client._dataset_name}.{view_name}"
            )
            big_query_client.create_view_if_not_exists(view_id)

        yield big_query_client
        big_query_client.delete_dataset()

    @pytest.fixture(scope="function")
    def handler(
        self,
        full_config: dict,
        big_query_client: BigQueryAdapter,
        mock_dataplex_client: MagicMock,
    ) -> CloudTaskHandler:
        """
        Provides an instance of CloudTaskHandler with mocked dependencies.
        """
        handler = CloudTaskHandler(full_config)
        handler._dataplex_client = mock_dataplex_client
        handler._big_query_client = big_query_client
        return handler

    def generate_task_data(
        self, entity: EntryGroup | TagTemplate
    ) -> ResourceTaskData:
        """
        Generates task data for a given entity.
        """
        return ResourceTaskData(
            resource_type=type(entity).__name__,
            resource=ResourceData(
                project_id=entity.project_id,
                location=entity.location,
                resource_name=entity.id,
            ),
        )

    def test_handle_cloud_task_entry_group(
        self, handler: CloudTaskHandler, test_data_eg: EntryGroup
    ) -> None:
        """
        Test handling a cloud task for an entry group.
        """
        task_data = self.generate_task_data(test_data_eg)
        expected_dataplex_resource_name = (
            "projects/project1/locations/us-west1/entryGroups/eg1"
        )

        table_name = TableNames.ENTRY_GROUPS_RESOURCE_MAPPING
        view_name = ViewNames.ENTRY_GROUPS_VIEW

        handler._dataplex_client.get_entry_group.return_value = (
            catalog.EntryGroup(
                name="dataplex resource name",
                transfer_status=False,
            )
        )

        response, status_code = handler.handle_cloud_task(task_data)

        assert status_code == 200
        assert response["message"] == "Task processed"

        table_rows = list(
            handler._big_query_client._select_all_data_from_table(
                handler._big_query_client._get_table_ref(table_name)
            ).result()
        )

        assert len(table_rows) == 1

        table_row = table_rows[0]
        assert table_row.dataCatalogResourceName == test_data_eg.resource_name
        assert table_row.dataplexResourceName == expected_dataplex_resource_name

        view_rows = list(
            handler._big_query_client._select_data_from_partition(
                handler._big_query_client._get_table_ref(view_name),
                handler._big_query_client._get_target_creation_date(
                    handler._big_query_client._get_table_ref(view_name)
                ),
            ).result()
        )

        assert len(view_rows) == 1

        view_row = view_rows[0]
        assert view_row.resourceName == test_data_eg.resource_name
        assert view_row.dataplexResourceName == expected_dataplex_resource_name
        assert view_row.projectId == test_data_eg.project_id
        assert view_row.location == test_data_eg.location
        assert view_row.entryGroupId == test_data_eg.id
        assert view_row.managingSystem == test_data_eg.managing_system
        assert view_row.createdAt == date.fromisoformat("2025-01-01")

    def test_handle_cloud_task_entry_group_with_new_name(
        self, handler: CloudTaskHandler, test_data_eg: EntryGroup
    ) -> None:
        """
        Test handling a cloud task for an entry group with a new name.
        """
        task_data = self.generate_task_data(test_data_eg)
        expected_dataplex_resource_name = (
            "projects/project1/locations/us-west1/entryGroups/eg1_us-west1"
        )

        table_name = TableNames.ENTRY_GROUPS_RESOURCE_MAPPING
        view_name = ViewNames.ENTRY_GROUPS_VIEW

        handler._dataplex_client.get_entry_group.side_effect = [
            None,
            catalog.EntryGroup(
                name="dataplex resource name",
                transfer_status=False,
            ),
        ]

        response, status_code = handler.handle_cloud_task(task_data)

        assert status_code == 200
        assert response["message"] == "Task processed"

        table_rows = list(
            handler._big_query_client._select_all_data_from_table(
                handler._big_query_client._get_table_ref(table_name)
            ).result()
        )

        assert len(table_rows) == 1

        table_row = table_rows[0]
        assert table_row.dataCatalogResourceName == test_data_eg.resource_name
        assert table_row.dataplexResourceName == expected_dataplex_resource_name

        view_rows = list(
            handler._big_query_client._select_data_from_partition(
                handler._big_query_client._get_table_ref(view_name),
                handler._big_query_client._get_target_creation_date(
                    handler._big_query_client._get_table_ref(view_name)
                ),
            ).result()
        )

        assert len(view_rows) == 1

        view_row = view_rows[0]
        assert view_row.resourceName == test_data_eg.resource_name
        assert view_row.dataplexResourceName == expected_dataplex_resource_name
        assert view_row.projectId == test_data_eg.project_id
        assert view_row.location == test_data_eg.location
        assert view_row.entryGroupId == test_data_eg.id
        assert view_row.managingSystem == test_data_eg.managing_system
        assert view_row.createdAt == date.fromisoformat("2025-01-01")

    def test_handle_cloud_task_entry_group_not_found(
        self, handler: CloudTaskHandler, test_data_eg: EntryGroup
    ) -> None:
        """
        Test handling a cloud task for an entry group that is not found.
        """
        task_data = self.generate_task_data(test_data_eg)

        table_name = TableNames.ENTRY_GROUPS_RESOURCE_MAPPING
        view_name = ViewNames.ENTRY_GROUPS_VIEW

        handler._dataplex_client.get_entry_group.side_effect = [None, None]

        response, status_code = handler.handle_cloud_task(task_data)

        assert status_code == 200
        assert response["message"] == "Resource not found"

        table_rows = list(
            handler._big_query_client._select_all_data_from_table(
                handler._big_query_client._get_table_ref(table_name)
            ).result()
        )

        assert len(table_rows) == 0

        view_rows = list(
            handler._big_query_client._select_data_from_partition(
                handler._big_query_client._get_table_ref(view_name),
                handler._big_query_client._get_target_creation_date(
                    handler._big_query_client._get_table_ref(view_name)
                ),
            ).result()
        )

        assert len(view_rows) == 1

        view_row = view_rows[0]

        assert view_row.resourceName == test_data_eg.resource_name
        assert view_row.dataplexResourceName is None
        assert view_row.projectId == test_data_eg.project_id
        assert view_row.location == test_data_eg.location
        assert view_row.entryGroupId == test_data_eg.id
        assert view_row.managingSystem == test_data_eg.managing_system
        assert view_row.createdAt == date.fromisoformat("2025-01-01")

    def test_handle_cloud_task_tag_template(
        self, handler: CloudTaskHandler, test_data_tt: TagTemplate
    ) -> None:
        """
        Test handling a cloud task for a tag template.
        """
        task_data = self.generate_task_data(test_data_tt)
        expected_dataplex_resource_name = (
            "projects/project1/locations/global/aspectTypes/tt1"
        )

        table_name = TableNames.TAG_TEMPLATES_RESOURCE_MAPPING
        view_name = ViewNames.TAG_TEMPLATES_VIEW

        handler._dataplex_client.get_aspect_type.return_value = {
            "name": "dataplex resource name",
            "transferStatus": "TRANSFER_STATUS_MIGRATED",
        }
        response, status_code = handler.handle_cloud_task(task_data)

        assert status_code == 200
        assert response["message"] == "Task processed"

        table_rows = list(
            handler._big_query_client._select_all_data_from_table(
                handler._big_query_client._get_table_ref(table_name)
            ).result()
        )

        assert len(table_rows) == 1

        table_row = table_rows[0]
        assert table_row.dataCatalogResourceName == test_data_tt.resource_name
        assert table_row.dataplexResourceName == expected_dataplex_resource_name

        view_rows = list(
            handler._big_query_client._select_data_from_partition(
                handler._big_query_client._get_table_ref(view_name),
                handler._big_query_client._get_target_creation_date(
                    handler._big_query_client._get_table_ref(view_name)
                ),
            ).result()
        )

        assert len(view_rows) == 1

        view_row = view_rows[0]
        assert view_row.resourceName == test_data_tt.resource_name
        assert view_row.dataplexResourceName == expected_dataplex_resource_name
        assert view_row.projectId == test_data_tt.project_id
        assert view_row.location == test_data_tt.location
        assert view_row.tagTemplateId == test_data_tt.id
        assert view_row.managingSystem == test_data_tt.managing_system
        assert view_row.isPubliclyReadable == test_data_tt.public
        assert view_row.createdAt == date.fromisoformat("2025-01-01")

    def test_handle_cloud_task_tag_template_with_new_name(
        self, handler: CloudTaskHandler, test_data_tt: TagTemplate
    ) -> None:
        """
        Test handling a cloud task for a tag template with a new name.
        """
        task_data = self.generate_task_data(test_data_tt)
        expected_dataplex_resource_name = (
            "projects/project1/locations/global/aspectTypes/tt1_us-west1"
        )

        table_name = TableNames.TAG_TEMPLATES_RESOURCE_MAPPING
        view_name = ViewNames.TAG_TEMPLATES_VIEW

        handler._dataplex_client.get_aspect_type.side_effect = [
            None,
            {
                "name": "dataplex resource name",
                "transferStatus": "TRANSFER_STATUS_MIGRATED",
            },
        ]

        response, status_code = handler.handle_cloud_task(task_data)

        assert status_code == 200
        assert response["message"] == "Task processed"

        table_rows = list(
            handler._big_query_client._select_all_data_from_table(
                handler._big_query_client._get_table_ref(table_name)
            ).result()
        )

        assert len(table_rows) == 1

        table_row = table_rows[0]
        assert table_row.dataCatalogResourceName == test_data_tt.resource_name
        assert table_row.dataplexResourceName == expected_dataplex_resource_name

        view_rows = list(
            handler._big_query_client._select_data_from_partition(
                handler._big_query_client._get_table_ref(view_name),
                handler._big_query_client._get_target_creation_date(
                    handler._big_query_client._get_table_ref(view_name)
                ),
            ).result()
        )

        assert len(view_rows) == 1

        view_row = view_rows[0]
        assert view_row.resourceName == test_data_tt.resource_name
        assert view_row.dataplexResourceName == expected_dataplex_resource_name
        assert view_row.projectId == test_data_tt.project_id
        assert view_row.location == test_data_tt.location
        assert view_row.tagTemplateId == test_data_tt.id
        assert view_row.managingSystem == test_data_tt.managing_system
        assert view_row.isPubliclyReadable == test_data_tt.public
        assert view_row.createdAt == date.fromisoformat("2025-01-01")

    def test_handle_cloud_task_tag_template_not_found(
        self, handler: CloudTaskHandler, test_data_tt: TagTemplate
    ) -> None:
        """
        Test handling a cloud task for a tag template that is not found.
        """
        task_data = self.generate_task_data(test_data_tt)

        table_name = TableNames.TAG_TEMPLATES_RESOURCE_MAPPING
        view_name = ViewNames.TAG_TEMPLATES_VIEW

        handler._dataplex_client.get_aspect_type.side_effect = [
            None,
            None,
        ]

        response, status_code = handler.handle_cloud_task(task_data)

        assert status_code == 200
        assert response["message"] == "Resource not found"

        table_rows = list(
            handler._big_query_client._select_all_data_from_table(
                handler._big_query_client._get_table_ref(table_name)
            ).result()
        )

        assert len(table_rows) == 0

        view_rows = list(
            handler._big_query_client._select_data_from_partition(
                handler._big_query_client._get_table_ref(view_name),
                handler._big_query_client._get_target_creation_date(
                    handler._big_query_client._get_table_ref(view_name)
                ),
            ).result()
        )

        assert len(view_rows) == 1

        view_row = view_rows[0]
        assert view_row.resourceName == test_data_tt.resource_name
        assert view_row.dataplexResourceName is None
        assert view_row.projectId == test_data_tt.project_id
        assert view_row.location == test_data_tt.location
        assert view_row.tagTemplateId == test_data_tt.id
        assert view_row.managingSystem == test_data_tt.managing_system
        assert view_row.isPubliclyReadable == test_data_tt.public
        assert view_row.createdAt == date.fromisoformat("2025-01-01")
