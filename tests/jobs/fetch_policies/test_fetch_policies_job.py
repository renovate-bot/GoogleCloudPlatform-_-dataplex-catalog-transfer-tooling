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
Fetch Policies job tests
"""

import datetime
import json
import random
from typing import Generator

import pytest

from common.cloud_task import CloudTaskPublisher
from common.entities import EntryGroup, TagTemplate, Project
from common.big_query import BigQueryAdapter, TableNames, ViewNames
from services.jobs.fetch_policies.transfer_controller import TransferController


class TestFetchPoliciesJob:
    """
    Fetch Policies job tests
    """

    @pytest.fixture(scope="function")
    def full_config(self, basic_config: dict) -> dict:
        """
        Provides a full configuration dictionary with unique suffixes for
        dataset and queue names.
        """
        suffix = random.randint(1, 1000000)
        basic_config["dataset_name"] += "_" + str(suffix)
        basic_config["queue"] += "-" + str(suffix)
        basic_config["quota_consumption"] = 20

        return basic_config

    @pytest.fixture(scope="function", autouse=True)
    def big_query_client(self, full_config: dict) -> Generator:
        """
        Sets up a BigQuery client for the test environment and ensures cleanup
        after tests.
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
            EntryGroup("pDATAPLEX", "us-central1", "eDATAPLEX", True),
            EntryGroup("pDATA_CATALOG", "us-central1", "eDATA_CATALOG", False),
            EntryGroup("pDATAPLEX", "us-central1", "eDATAPLEX_wo_name", True),
        ]

        tags_templates = [
            TagTemplate("pDATAPLEX", "global", "tDATAPLEX", True, True),
            TagTemplate(
                "pDATA_CATALOG", "us-central1", "tDATA_CATALOG", True, False
            ),
            TagTemplate("pDATAPLEX", "global", "tDATAPLEX_wo_name", True, True),
        ]

        projects = [
            Project("pDATAPLEX", 1),
            Project("pDATA_CATALOG", 2),
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
                },
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

        try:
            cloud_task_client.delete_queue()
        except Exception:
            pass

        try:
            cloud_task_client.delete_queue(
                queue_name=full_config["queue"] + "-us-central1"
            )
        except Exception:
            pass

    @staticmethod
    def generate_result(
        resource_types: list,
        managing_systems,
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
            return in_scope

        filtered_resources = []
        if "entry_group" in resource_types:
            filtered_resources.extend(
                {
                    "resource_type": "EntryGroup",
                    "created_at": "2025-01-01",
                    "resource": {
                        "resource_name": entry.id,
                        "location": entry.location,
                        "project_id": entry.project_id,
                        "system": entry.managing_system.value,
                    },
                }
                for entry in entry_groups
                if is_in_scope(entry)
                and entry.id != "eDATAPLEX_wo_name"
                and entry.managing_system.value in managing_systems
            )

        if "tag_template" in resource_types:
            filtered_resources.extend(
                {
                    "resource_type": "TagTemplate",
                    "created_at": "2025-01-01",
                    "resource": {
                        "resource_name": tag.id,
                        "location": tag.location,
                        "project_id": tag.project_id,
                        "system": tag.managing_system.value,
                    },
                }
                for tag in tag_templates
                if is_in_scope(tag)
                and tag.id != "tDATAPLEX_wo_name"
                and tag.managing_system.value in managing_systems
            )

        return filtered_resources

    @pytest.mark.parametrize(
        "resource_types, managing_systems, scope",
        [
            (
                ["entry_group", "tag_template"],
                ["DATA_CATALOG", "DATAPLEX"],
                ("ORGANIZATION", 1),
            ),
            (["entry_group"], ["DATAPLEX"], ("ORGANIZATION", 1)),
            (["entry_group"], ["DATAPLEX"], ("PROJECT", 1)),
            (["tag_template"], ["DATA_CATALOG", "DATAPLEX"], ("FOLDER", 2)),
            (
                ["entry_group", "tag_template"],
                ["DATA_CATALOG"],
                ("ORGANIZATION", 1),
            ),
            (
                ["entry_group", "tag_template"],
                ["DATAPLEX"],
                ("PROJECT", 1),
            ),
        ],
    )
    def test_fetch_policies_job(
        self,
        full_config: dict,
        cloud_task_client: CloudTaskPublisher,
        resource_types: list,
        managing_systems: list,
        scope: tuple[str, int],
        test_resources: tuple[
            list[EntryGroup], list[TagTemplate], list[Project]
        ],
    ) -> None:
        """
        Tests the Fetch Policies job for various configurations.
        """
        full_config["resource_types"] = resource_types
        full_config["managing_systems"] = managing_systems
        full_config["scope"] = {"scope_type": scope[0], "scope_id": scope[1]}
        controller = TransferController(full_config)
        controller._cloud_task_client._wait_after_queue_creation = 5

        controller.start_transfer()
        tasks_data = []

        try:
            messages_from_queue = list(cloud_task_client.get_messages())
            if messages_from_queue:
                tasks_data.extend(
                    [
                        json.loads(msg.http_request.body)
                        for msg in messages_from_queue
                    ]
                )
        except Exception:
            pass

        try:
            additional_messages = list(
                cloud_task_client.get_messages(
                    queue_name=full_config["queue"] + "-us-central1"
                )
            )
            if additional_messages:
                tasks_data.extend(
                    [
                        json.loads(msg.http_request.body)
                        for msg in additional_messages
                    ]
                )
        except Exception:
            pass

        test_data = self.generate_result(
            resource_types, managing_systems, scope, test_resources
        )

        assert len(tasks_data) == len(test_data)
        for task in tasks_data:
            assert task in test_data
