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
Clean up job test
"""

import json
import datetime
import random
from typing import Generator

import pytest

from common.cloud_task import CloudTaskPublisher
from common.entities import EntryGroup, TagTemplate, Project
from common.big_query import BigQueryAdapter, TableNames, ViewNames
from services.jobs.clean_up.transfer_controller import TransferController

class TestCleanUpJob:
    """
    Clean up job tests
    """

    @pytest.fixture(scope="function")
    def full_config(self, basic_config: dict) -> dict:
        """
        Extends the basic configuration with a unique queue name by
        appending a random suffix.
        """
        suffix = random.randint(1, 1000000)
        basic_config["dataset_name"] += "_" + str(suffix)
        basic_config["queue"] += "-" + str(suffix)

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

    @pytest.fixture(scope="class")
    def test_resources(
        self,
    ) -> tuple[list[EntryGroup], list[TagTemplate], list[Project]]:
        """
        Provides test resources including EntryGroups,
        TagTemplates, and Projects.
        """
        entries = [
            EntryGroup("project1", "us-central1", "eg_tr_1", True),
            EntryGroup("project1", "us-central1", "eg_1", False),
            EntryGroup("project2", "us-central1", "eg_2", False),
        ]

        tags_templates = [
            TagTemplate("project1", "global", "tt_tr_1", True, True),
            TagTemplate("project2", "us-central1", "tt_2", True, False),
        ]

        projects = [
            Project("project1", 1),
            Project("project2", 2),
        ]

        projects[0].set_ancestry([("ORGANIZATION", 1)])
        projects[1].set_ancestry([("FOLDER", 2), ("ORGANIZATION", 1)])

        return entries, tags_templates, projects

    @pytest.fixture(scope="function", autouse=True)
    def setup_bigquery_table(
        self,
        big_query_client: BigQueryAdapter,
        full_config: dict,
        test_resources: tuple[
            list[EntryGroup], list[TagTemplate], list[Project]
        ],
    ) -> None:
        """
        Sets up a BigQuery table with test data for the `policies` table.
        """
        entry_table_id = TableNames.ENTRY_GROUPS
        tag_table_id = TableNames.TAG_TEMPLATES
        project_table_id = TableNames.PROJECTS
        entry_resource_mapping_id = TableNames.ENTRY_GROUPS_RESOURCE_MAPPING
        tag_resource_mapping_id = TableNames.TAG_TEMPLATES_RESOURCE_MAPPING

        entries, tags_templates, projects = test_resources

        big_query_client.write_entities_to_table(
            entry_table_id, entries, datetime.date.fromisoformat("2025-01-01")
        )
        big_query_client.write_entities_to_table(
            tag_table_id,
            tags_templates,
            datetime.date.fromisoformat("2025-01-01"),
        )
        big_query_client.write_entities_to_table(
            project_table_id,
            projects,
            datetime.date.fromisoformat("2025-01-01"),
        )

        big_query_client.write_to_table(
            (
                f"{full_config["project_name"]}.{full_config["dataset_name"]}."
                f"{entry_resource_mapping_id}"
            ),
            [
                {
                    "dataCatalogResourceName": EntryGroup.get_old_fqn(
                        entries[0].project_id,
                        entries[0].location,
                        entries[0].id,
                    ),
                    "dataplexResourceName": EntryGroup.get_new_fqn(
                        entries[0].project_id,
                        entries[0].location,
                        entries[0].id,
                    ),
                },
                {
                    "dataCatalogResourceName": EntryGroup.get_old_fqn(
                        entries[1].project_id,
                        entries[1].location,
                        entries[1].id,
                    ),
                    "dataplexResourceName": EntryGroup.get_new_fqn(
                        entries[1].project_id,
                        entries[1].location,
                        entries[1].id,
                    ),
                },
                {
                    "dataCatalogResourceName": EntryGroup.get_old_fqn(
                        entries[2].project_id,
                        entries[2].location,
                        entries[2].id,
                    ),
                    "dataplexResourceName": EntryGroup.get_new_fqn(
                        entries[2].project_id,
                        entries[2].location,
                        entries[2].id,
                    ),
                }
            ],
        )
        big_query_client.write_to_table(
            (
                f"{full_config["project_name"]}.{full_config["dataset_name"]}."
                f"{tag_resource_mapping_id}"
            ),
            [
                {
                    "dataCatalogResourceName": TagTemplate.get_old_fqn(
                        tags_templates[0].project_id,
                        tags_templates[0].location,
                        tags_templates[0].id,
                    ),
                    "dataplexResourceName": TagTemplate.get_new_fqn(
                        tags_templates[0].project_id,
                        tags_templates[0].location,
                        tags_templates[0].id,
                    ),
                },
                {
                    "dataCatalogResourceName": TagTemplate.get_old_fqn(
                        tags_templates[1].project_id,
                        tags_templates[1].location,
                        tags_templates[1].id,
                    ),
                    "dataplexResourceName": TagTemplate.get_new_fqn(
                        tags_templates[1].project_id,
                        tags_templates[1].location,
                        tags_templates[1].id,
                    ),
                }
            ],
        )

        required_views = [
            ViewNames.ENTRY_GROUPS_VIEW,
            ViewNames.TAG_TEMPLATES_VIEW,
        ]

        for view_name in required_views:
            view_id = (
                f"{big_query_client._project}."
                f"{big_query_client._dataset_name}.{view_name}"
            )
            big_query_client.create_view_if_not_exists(view_id)

    @pytest.fixture(scope="function")
    def cloud_task_client(self, full_config: dict) -> Generator:
        """
        Sets up a Cloud Task client for the test environment and ensures
        cleanup after tests.
        """
        cloud_task_client = CloudTaskPublisher(
            full_config["project_name"],
            full_config["service_location"],
            full_config["queue"],
        )
        yield cloud_task_client
        cloud_task_client.delete_queue()

    @staticmethod
    def generate_result(
        resource_types: list,
        scope: tuple[str, int],
        test_resources: tuple[
            list[EntryGroup], list[TagTemplate], list[Project]
        ],
    ) -> list[dict, dict]:
        """
        Generates expected results based on the provided resource types
        and scope.
        """
        scope_type, scope_id = scope
        entry_groups, tag_templates, projects = test_resources

        project_to_scope = {
            project.project_id: {
                "ORGANIZATION": any(
                    ancestor[0] == "ORGANIZATION" and ancestor[1] == scope_id
                    for ancestor in project.ancestry
                ),
                "FOLDER": any(
                    ancestor[0] == "FOLDER" and ancestor[1] == scope_id
                    for ancestor in project.ancestry
                ),
                "PROJECT": project.project_number == scope_id,
            }
            for project in projects
        }

        def is_in_scope(resource: EntryGroup | TagTemplate) -> bool:
            """
            Determines if a resource is within the given scope.
            """
            match scope_type:
                case "ORGANIZATION":
                    in_scope = project_to_scope.get(
                        resource.project_id, {}
                    ).get("ORGANIZATION", False)
                case "FOLDER":
                    in_scope = project_to_scope.get(
                        resource.project_id, {}
                    ).get("FOLDER", False)
                case "PROJECT":
                    in_scope = project_to_scope.get(
                        resource.project_id, {}
                    ).get("PROJECT", False)
                case _:
                    raise ValueError(f"Unsupported scope type: {scope_type}")
            return in_scope and resource.managing_system == "DATA_CATALOG"

        filtered_resources = []
        if "entry_group" in resource_types:
            filtered_resources.extend(
                {
                    "resource_type": "EntryGroup",
                    "resource": {
                        "resource_name": entry.id,
                        "location": entry.location,
                        "project_id": entry.project_id,
                    },
                }
                for entry in entry_groups
                if is_in_scope(entry)
            )

        if "tag_template" in resource_types:
            filtered_resources.extend(
                {
                    "resource_type": "TagTemplate",
                    "resource": {
                        "resource_name": tag.id,
                        "location": tag.location,
                        "project_id": tag.project_id,
                    },
                }
                for tag in tag_templates
                if is_in_scope(tag)
            )

        return filtered_resources

    @pytest.mark.parametrize(
        "resource_types, scope",
        [
            (
                ["entry_group", "tag_template"],
                ("ORGANIZATION", 1),
            ),
            (["entry_group"], ("ORGANIZATION", 1)),
            (["tag_template"], ("FOLDER", 2)),
            (
                ["entry_group", "tag_template"],
                ("PROJECT", 1),
            ),
        ],
    )
    def test_clean_up_job(
        self,
        full_config: dict,
        cloud_task_client: CloudTaskPublisher,
        resource_types: list,
        scope: tuple[str, int],
        test_resources: tuple[
            list[EntryGroup], list[TagTemplate], list[Project]
        ],
    ) -> None:
        """
        Tests the Clean up job and publish tasks.
        """
        full_config["resource_types"] = resource_types
        full_config["scope"] = {"scope_type": scope[0], "scope_id": scope[1]}
        controller = TransferController(full_config)
        controller._cloud_task_client._wait_after_queue_creation = 5

        controller.start_transfer()

        messages = list(cloud_task_client.get_messages())
        test_data = self.generate_result(resource_types, scope, test_resources)

        tasks_data = [json.loads(msg.http_request.body) for msg in messages]

        assert len(tasks_data) == len(test_data)
        for task in tasks_data:
            assert task in test_data
