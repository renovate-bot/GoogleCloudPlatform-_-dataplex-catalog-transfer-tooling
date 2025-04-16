"""
Find resource names job test
"""

import json
import random

import pytest

from common.big_query import BigQueryAdapter, TableNames, ViewNames
from common.cloud_task import CloudTaskPublisher
from common.entities import EntryGroup, TagTemplate
from services.jobs.find_resource_names.transfer_controller import (
    TransferController,
)


class TestFindResourceNames:
    """
    Find resource names job test
    """

    @pytest.fixture(scope="class")
    def basic_config(self):
        """
        Provides a basic configuration dictionary for the test environment.
        """
        return {
            "project_name": "hl2-gogl-dapx-t1iylu",
            "service_location": "us-west1",
            "handler_name": "test-find-resource-names-job",
            "queue": "test-find-resource-names",
            "dataset_location": "US",
            "dataset_name": "test_find_resource_names_job",
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
    def test_data(self):
        """
        Provides test data for the entry groups and tag templates.
        """
        entry_groups = [
            EntryGroup("project1", "us-west1", "eg1", False),
            EntryGroup("project2", "us-west2", "eg2", False),
        ]

        tag_templates = [
            TagTemplate("project1", "us-west1", "tt1", True, False),
            TagTemplate("project2", "us-west2", "tt2", True, False),
        ]
        return entry_groups, tag_templates

    @pytest.fixture(scope="class")
    def setup_bigquery_table(self, big_query_client, test_data):
        """
        Sets up a BigQuery table with test data for the 'entry_groups'
        and `tag_templates` tables.
        """

        entry_groups, tag_templates = test_data

        big_query_client.write_entities_to_table(
            TableNames.ENTRY_GROUPS, entry_groups
        )
        big_query_client.write_entities_to_table(
            TableNames.TAG_TEMPLATES, tag_templates
        )

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

    @pytest.fixture()
    def expected_result(self, test_data):
        """
        Generates the expected results for the test based on the
        provided test data.
        """
        entry_groups, tag_tempates = test_data
        entities = entry_groups + tag_tempates
        results = []
        for entity in entities:
            results.append(
                {
                    "resource_type": type(entity).__name__,
                    "resource": {
                        "resource_name": entity.id,
                        "location": entity.location,
                        "project_id": entity.project_id,
                    },
                }
            )
        return results

    @pytest.mark.usefixtures("setup_bigquery_table")
    def test_find_resource_names(
        self, full_config, big_query_client, cloud_task_client, expected_result
    ):
        """
        Tests the Find Resource Names job functionality.
        """

        required_resources = [
            TableNames.ENTRY_GROUPS_RESOURCE_MAPPING,
            TableNames.TAG_TEMPLATES_RESOURCE_MAPPING,
            ViewNames.ENTRY_GROUPS_VIEW,
            ViewNames.TAG_TEMPLATES_VIEW,
        ]

        controller = TransferController(full_config)
        controller._big_query_client = big_query_client
        controller._cloud_task_client._wait_after_queue_creation = 5
        controller.start_transfer()

        for resource_name in required_resources:
            resource_id = (f"{full_config['project_name']}."
                           f"{full_config['dataset_name']}.{resource_name}")
            assert controller._big_query_client._client.get_table(resource_id)

        assert cloud_task_client.check_queue_exists()

        messages = list(cloud_task_client.get_messages())
        assert len(messages) == len(expected_result)

        tasks_data = [json.loads(msg.http_request.body) for msg in messages]
        for expected_json in expected_result:
            assert expected_json in tasks_data
