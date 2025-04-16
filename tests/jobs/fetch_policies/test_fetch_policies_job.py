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

import pytest
import random

from common.cloud_task import CloudTaskPublisher
from common.entities import EntryGroup, TagTemplate, Project
from common.big_query import BigQueryAdapter, TableNames, ViewNames
from services.jobs.fetch_policies.transfer_controller import TransferController


class TestFetchPoliciesJob:
    """
    Fetch Policies job tests
    """

    @pytest.fixture(scope="class")
    def basic_config(self):
        return {
            "project_name": "hl2-gogl-dapx-t1iylu",
            "service_location": "us-west1",
            "dataset_location": "us-west1",
            "dataset_name": "transfer_tooling_test",
            "queue": "transfer-tooling-test",
            "handler_name": "dummy",
        }

    @pytest.fixture(scope="function")
    def full_config(self, basic_config):
        suffix = random.randint(1, 1000000)
        basic_config["dataset_name"] += "_" + str(suffix)
        basic_config["queue"] += "-" + str(suffix)

        return basic_config

    @pytest.fixture(scope="function", autouse=True)
    def big_query_client(self, full_config):
        big_query_client = BigQueryAdapter(
            full_config["project_name"],
            full_config["dataset_location"],
            full_config["dataset_name"],
        )
        yield big_query_client
        big_query_client.delete_dataset()

    @pytest.fixture(scope="function", autouse=True)
    def setup_bigquery_table(self, big_query_client, full_config):
        """
        Sets up a BigQuery table with test data for the `policies` table.
        """
        entry_table_id = TableNames.ENTRY_GROUPS
        tag_table_id = TableNames.TAG_TEMPLATES
        project_table_id = TableNames.PROJECTS
        entry_resource_mapping_id = TableNames.ENTRY_GROUPS_RESOURCE_MAPPING
        tag_resource_mapping_id = TableNames.TAG_TEMPLATES_RESOURCE_MAPPING

        entries = [
            EntryGroup("pDATAPLEX", "l", "eDATAPLEX", True),
            EntryGroup("pDATA_CATALOG", "l", "eDATA_CATALOG", False),
        ]

        tags_templates = [
            TagTemplate("pDATAPLEX", "global", "tDATAPLEX", True, True),
            TagTemplate("pDATA_CATALOG", "l", "tDATA_CATALOG", True, False),
        ]

        projects = [
            Project("pDATAPLEX", 1),
            Project("pDATA_CATALOG", 2),
        ]

        projects[0].set_ancestry([("ORGANIZATION", 1)])
        projects[1].set_ancestry([("FOLDER", 2), ("ORGANIZATION", 1)])

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
    def cloud_task_client(self, full_config):
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
    def generate_result(resource_types, managing_systems, scope):
        resource_types = [
            "EntryGroup" if x == "entry_group" else "TagTemplate"
            for x in resource_types
        ]

        if scope == ("FOLDER", 2):
            managing_systems.remove("DATAPLEX")

        all_data = {
            f"{"e" if r_type == "EntryGroup" else "t"}{system}": {
                "resource_type": r_type,
                "created_at": "2025-01-01",
                "resource": {
                    "resource_name": f"{"e" if r_type == "EntryGroup" else "t"}"
                    f"{system}",
                    "location": (
                        "global"
                        if r_type == "TagTemplate" and system == "DATAPLEX"
                        else "l"
                    ),
                    "project_id": f"p{system}",
                    "system": system,
                },
            }
            for r_type in resource_types
            for system in managing_systems
        }

        return all_data

    @pytest.mark.parametrize(
        "resource_types, managing_systems, scope",
        [
            (
                ["entry_group", "tag_template"],
                ["DATA_CATALOG", "DATAPLEX"],
                ("ORGANIZATION", 1),
            ),
            (["entry_group"], ["DATAPLEX"], ("ORGANIZATION", 1)),
            (["tag_template"], ["DATA_CATALOG", "DATAPLEX"], ("FOLDER", 2)),
            (
                ["entry_group", "tag_template"],
                ["DATA_CATALOG"],
                ("ORGANIZATION", 1),
            ),
        ],
    )
    def test_fetch_policies_job(
        self,
        full_config,
        cloud_task_client,
        resource_types,
        managing_systems,
        scope,
    ):
        full_config["resource_types"] = resource_types
        full_config["managing_systems"] = managing_systems
        full_config["scope"] = {"scope_type": scope[0], "scope_id": scope[1]}
        controller = TransferController(full_config)
        controller._cloud_task_client._wait_after_queue_creation = 5

        controller.start_transfer()

        messages = list(cloud_task_client.get_messages())
        test_data = self.generate_result(
            resource_types, managing_systems, scope
        )

        tasks_data = [json.loads(msg.http_request.body) for msg in messages]
        assert len(tasks_data) == len(test_data)
        for task in tasks_data:
            assert task == test_data[task["resource"]["resource_name"]]
