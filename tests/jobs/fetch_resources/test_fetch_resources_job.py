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
Fetch Resources job tests
"""

import json
import random
import pytest
import datetime

from services.jobs.fetch_resources.transfer_controller import TransferController
from common.cloud_task import CloudTaskPublisher
from common.big_query import BigQueryAdapter, TableNames
from common.entities import Project


class TestFetchResources:
    """
    Fetch Resources job tests
    """

    @pytest.fixture(scope="class")
    def basic_config(self):
        """
        Provides a basic configuration dictionary for the test environment.
        """
        return {
            "project_name": "hl2-gogl-dapx-t1iylu",
            "service_location": "us-west1",
            "handler_name": "test-fetch-resources-handler",
            "queue": "test-resource-discovery",
            "dataset_location": "US",
            "dataset_name": "test_fetch_resources_job",
        }

    @pytest.fixture(scope="class")
    def full_config(self, basic_config):
        """
        Extends the basic configuration with a unique queue name by
        appending a random suffix.
        """
        suffix = random.randint(1, 1000000)
        basic_config["queue"] += "-" + str(suffix)
        basic_config["dataset_name"] += "_" + str(suffix)

        return basic_config

    @pytest.fixture(scope="class", autouse=True)
    def big_query_client(self, full_config):
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
    def setup_bigquery_table(self, big_query_client):
        """
        Sets up a BigQuery table with test data for the `projects` table.
        """
        table_id = TableNames.PROJECTS

        entities = [
            Project("project1", "12345678"),
            Project("project2", "87654321"),
        ]

        big_query_client.write_entities_to_table(table_id, entities)

    @pytest.fixture()
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

    def generate_expected_results(self, projects: list[str]) -> list[dict]:
        """
        Generates the expected results for the test based on the
        provided project list.
        """
        results = []
        for project in projects:
            for transferred in [True, False]:
                results.append(
                    {
                        "scope": project,
                        "resource_type": "entry_group",
                        "next_page_token": None,
                        "is_transferred": transferred,
                        "created_at": str(datetime.date.today()),
                    }
                )
                for public in [True, False]:
                    results.append(
                        {
                            "scope": project,
                            "resource_type": "tag_template",
                            "next_page_token": None,
                            "is_transferred": transferred,
                            "is_public": public,
                            "created_at": str(datetime.date.today()),
                        }
                    )
        return results

    @pytest.mark.usefixtures("setup_bigquery_table")
    @pytest.mark.parametrize("project_list", [["project1", "project2"]])
    def test_fetch_resources(
        self, full_config, big_query_client, cloud_task_client, project_list
    ):
        """
        Tests the Fetch Resources functionality by verifying the transfer of
        resource data from BigQuery to Cloud Tasks."
        """
        expected_results = self.generate_expected_results(project_list)

        controller = TransferController(full_config)
        controller.big_query_client = big_query_client
        controller._cloud_task_client._wait_after_queue_creation = 5

        controller.start_transfer()

        projects = controller.get_projects_to_fetch()
        assert sorted(projects) == project_list
        assert cloud_task_client.check_queue_exists()

        messages = list(cloud_task_client.get_messages())
        assert len(messages) == len(expected_results)

        tasks_data = [json.loads(msg.http_request.body) for msg in messages]

        for expected_json in expected_results:
            assert expected_json in tasks_data
