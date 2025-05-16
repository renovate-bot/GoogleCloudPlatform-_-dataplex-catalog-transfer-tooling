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
Convert private tag templates job tests
"""

import datetime
import json
import random
from typing import Generator

import pytest

from common.cloud_task import CloudTaskPublisher
from common.entities import TagTemplate, Project
from common.big_query import BigQueryAdapter, TableNames
from services.jobs.convert_private_tag_templates.transfer_controller import (
    TransferController,
)


class TestConvertPrivateTagTemplatesJob:
    """
    Test suite for the Convert Private Tag Templates job.
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
    def test_data(self) -> tuple[list[TagTemplate], list[Project]]:
        """
        Provides test data for tag templates and projects.
        """

        tag_templates = [
            TagTemplate("project1", "us-central1", "public1", True, False),
            TagTemplate("project1", "us-east1", "private1", False, False),
            TagTemplate("project2", "us-west3", "private2", False, False),
            TagTemplate("project2", "us-west2", "private3", False, False),
            TagTemplate("project3", "us-central1", "private4", False, False),
        ]

        projects = [
            Project("project1", 111),
            Project("project2", 222),
            Project("project3", 333),
        ]

        projects[0].set_ancestry([("FOLDER", 1), ("ORGANIZATION", 1)])
        projects[1].set_ancestry([("FOLDER", 1), ("ORGANIZATION", 1)])
        projects[2].set_ancestry([("ORGANIZATION", 1)])

        return tag_templates, projects

    @pytest.fixture(scope="function", autouse=True)
    def setup_bigquery_table(
        self,
        big_query_client: BigQueryAdapter,
        test_data: tuple[list[TagTemplate], list[Project]],
    ) -> None:
        """
        Sets up a BigQuery table with test data.
        """
        tt_table_id = TableNames.TAG_TEMPLATES
        project_table_id = TableNames.PROJECTS

        tag_templates, projects = test_data

        big_query_client.write_entities_to_table(
            tt_table_id,
            tag_templates,
            datetime.date.fromisoformat("2025-01-01"),
        )
        big_query_client.write_entities_to_table(
            project_table_id,
            projects,
            datetime.date.fromisoformat("2025-01-01"),
        )

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
        scope, test_data: tuple[list[TagTemplate], list[Project]]
    ) -> list[dict, dict]:
        """
        Generates expected results based on the provided scope and
        tag templates.
        """
        scope_type, scope_id = scope
        tag_templates, projects = test_data

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
            }
            for project in projects
        }

        def is_in_scope(template: TagTemplate) -> bool:
            """
            Determines if a resource is within the given scope.
            """
            match scope_type:
                case "ORGANIZATION":
                    return project_to_scope.get(template.project_id, {}).get(
                        "ORGANIZATION", False
                    )
                case "FOLDER":
                    return project_to_scope.get(template.project_id, {}).get(
                        "FOLDER", False
                    )
                case "PROJECT":
                    return any(
                        project.project_number == scope_id
                        and project.project_id == template.project_id
                        for project in projects
                    )
                case _:
                    raise ValueError(f"Unsupported scope type: {scope_type}")

        filtered_templates = [
            tt for tt in tag_templates if not tt.public and is_in_scope(tt)
        ]

        result = [
            {
                "project_id": tt.project_id,
                "location": tt.location,
                "resource_name": tt.id,
            }
            for tt in filtered_templates
        ]

        return result

    @pytest.mark.parametrize(
        "scope",
        [
            ("ORGANIZATION", 1),
            ("FOLDER", 1),
            ("PROJECT", 222),
        ],
    )
    def test_convert_private_tag_templates_job(
        self,
        full_config: dict,
        cloud_task_client: CloudTaskPublisher,
        scope: tuple[str, int],
        test_data: tuple[list[TagTemplate], list[Project]],
    ) -> None:
        """
        Tests the Convert Private Tag Templates job for various scopes.
        """
        full_config["scope"] = {"scope_type": scope[0], "scope_id": scope[1]}
        controller = TransferController(full_config)
        controller._cloud_task_client._wait_after_queue_creation = 5

        controller.start_transfer()

        messages = list(cloud_task_client.get_messages())
        test_data = self.generate_result(scope, test_data)

        tasks_data = [json.loads(msg.http_request.body) for msg in messages]

        assert len(tasks_data) == len(test_data)
        for task in tasks_data:
            assert task in test_data
