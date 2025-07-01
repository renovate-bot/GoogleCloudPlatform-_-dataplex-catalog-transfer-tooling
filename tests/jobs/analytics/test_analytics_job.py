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
Analytics job tests
"""

import random
from typing import Generator

import pytest

from common.big_query import BigQueryAdapter, ViewNames, TableNames
from common.exceptions import MissingTablesOrViewsError
from services.jobs.analytics.setup_analytics_views import TransferController


class TestAnaluticsViewsCreation:
    """
    Analytics job tests
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

    @pytest.fixture(scope="function", autouse=True)
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

    @pytest.fixture(scope="function", autouse=True)
    def setup_bigquery_table(
        self,
        big_query_client: BigQueryAdapter,
    ) -> None:
        """
        Fixture to set up required BigQuery tables and views for
        the test environment.
        """
        required_tables = [
            TableNames.ENTRY_GROUPS,
            TableNames.TAG_TEMPLATES,
            TableNames.ENTRY_GROUPS_RESOURCE_MAPPING,
            TableNames.TAG_TEMPLATES_RESOURCE_MAPPING,
            TableNames.IAM_POLICIES,
        ]
        required_views = [
            ViewNames.TAG_TEMPLATES_VIEW,
            ViewNames.ENTRY_GROUPS_VIEW,
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

    @pytest.fixture
    def setup_data_access_table(
        self,
        big_query_client: BigQueryAdapter,
    ) -> None:
        """
        Fixture to set up the `cloudaudit_googleapis_com_data_access`
        table in BigQuery.
        """
        table_fqn = (
            f"{big_query_client._project}.{big_query_client._dataset_name}"
            f".{TableNames.CLOUDAUDIT_GOOGLEAPIS_DATA_ACCESS}"
        )
        query_job = big_query_client._client.query(
            f"""
            CREATE TABLE `{table_fqn}` (
                protopayload_auditlog STRUCT<
                    resourceName STRING,
                    methodName STRING,
                    authenticationInfo STRUCT<
                        principalSubject STRING,
                        principalEmail STRING
                    >,
                    authorizationInfo ARRAY<STRUCT<
                        permissionType STRING
                    >>
                >
            );
        """
        )
        query_job.result()

    @pytest.mark.usefixtures("setup_data_access_table")
    def test_create_analytics_views(self, full_config: dict) -> None:
        """
        Test case to validate the creation of analytics views
        when all required tables exist.
        """

        controller = TransferController(full_config)

        controller.create_analytical_views()

        expected_schemas = {
            ViewNames.RESOURCE_INTERACTIONS: [
                {"name": "resourceName", "type": "STRING"},
                {"name": "resourceType", "type": "STRING"},
                {"name": "methodName", "type": "STRING"},
                {"name": "principal", "type": "STRING"},
                {"name": "permissionType", "type": "STRING"},
            ],
            ViewNames.RESOURCE_INTERACTIONS_SUMMARY: [
                {"name": "principal", "type": "STRING"},
                {"name": "totalCalls", "type": "INTEGER"},
                {"name": "adminReadCalls", "type": "INTEGER"},
                {"name": "dataReadCalls", "type": "INTEGER"},
                {"name": "dataWriteCalls", "type": "INTEGER"},
                {"name": "tagTemplateCalls", "type": "INTEGER"},
                {"name": "entryGroupCalls", "type": "INTEGER"},
                {"name": "entryCalls", "type": "INTEGER"},
            ],
            ViewNames.IAM_POLICIES_COMPARISON: [
                {"name": "resourceType", "type": "STRING"},
                {"name": "resourceName", "type": "STRING"},
                {"name": "dataplexResourceName", "type": "STRING"},
                {
                    "name": "dataCatalogBindings",
                    "type": "RECORD",
                    "fields": [
                        {"name": "role", "type": "STRING"},
                        {"name": "members", "type": "STRING"},
                    ],
                },
                {
                    "name": "dataplexBindings",
                    "type": "RECORD",
                    "fields": [
                        {"name": "role", "type": "STRING"},
                        {"name": "members", "type": "STRING"},
                    ],
                },
            ],
        }

        views_to_check = [
            ViewNames.RESOURCE_INTERACTIONS,
            ViewNames.RESOURCE_INTERACTIONS_SUMMARY,
            ViewNames.IAM_POLICIES_COMPARISON,
        ]

        for view_id in views_to_check:
            ref = controller._big_query_client._get_table_ref(view_id)
            table = controller._big_query_client.check_if_table_or_view_exists(
                ref
            )

            expected_schema = expected_schemas[view_id]
            actual_schema = [
                {
                    "name": field.name,
                    "type": field.field_type,
                    "fields": getattr(field, "fields", None),
                }
                for field in table.schema
            ]

            assert len(expected_schema) == len(actual_schema)
            for expected_field, actual_field in zip(
                expected_schema, actual_schema
            ):
                assert expected_field["name"] == actual_field["name"]
                assert expected_field["type"] == actual_field["type"]

                if expected_field["type"] == "RECORD":
                    assert "fields" in expected_field
                    assert "fields" in actual_field
                    expected_nested_fields = expected_field["fields"]
                    actual_nested_fields = [
                        {
                            "name": nested_field.name,
                            "type": nested_field.field_type,
                        }
                        for nested_field in actual_field["fields"]
                    ]
                    assert len(expected_nested_fields) == len(
                        actual_nested_fields
                    )
                    for expected_nested, actual_nested in zip(
                        expected_nested_fields, actual_nested_fields
                    ):
                        assert expected_nested == actual_nested

    def test_create_analytics_views_without_data_access_table(
        self, full_config: dict
    ) -> None:
        """
        Test case to validate the behavior when the data access table
        is missing.
        """
        controller = TransferController(full_config)

        with pytest.raises(MissingTablesOrViewsError) as exc_info:
            controller.create_analytical_views()

        assert (
            "The following required tables or views are missing: "
            f"{full_config["project_name"]}.{full_config["dataset_name"]}."
            f"{TableNames.CLOUDAUDIT_GOOGLEAPIS_DATA_ACCESS}. "
            "Please ensure they exist before proceeding."
        ) in str(exc_info.value)
