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
Fetch Projects job tests
"""

import json

import pytest
import random
from services.jobs.fetch_projects.transfer_controller import TransferController
from tests.mocks.api.cloud_asset_api_mock import CloudAssetApiMock
from common.cloud_task import CloudTaskPublisher


class TestFetchProjectsJob:
    """
    Fetch Projects job tests
    """
    @pytest.fixture(scope='class')
    def basic_config(self):
        return {
            "project_name": "hl2-gogl-dapx-t1iylu",
            "service_location": "us-west1",
            "handler_name": "dummy",
            "queue": "test-fetch-projects",
            "dataset_location": "us-west1",
            "dataset_name": "test-fetch-projects"
        }

    @pytest.fixture()
    def full_config(self, basic_config):
        suffix = random.randint(1, 1000000)
        basic_config["queue"] += "-" + str(suffix)

        return basic_config

    @pytest.fixture()
    def cloud_task_client(self, full_config):
        cloud_task_client = CloudTaskPublisher(
            full_config["project_name"],
            full_config["service_location"],
            full_config["queue"]
        )
        yield cloud_task_client
        cloud_task_client.delete_queue()

    def test_fetch_projects_job(self, full_config, cloud_task_client):
        controller = TransferController(full_config)
        cloud_asset_mock_client = CloudAssetApiMock()
        controller._cloud_asset_api_client = cloud_asset_mock_client
        controller._cloud_task_client._wait_after_queue_creation = 5

        controller.start_transfer()
        assert cloud_task_client.check_queue_exists()

        messages = list(cloud_task_client.get_messages())
        assert len(messages) == 4

        tasks_data = [
            json.loads(msg.http_request.body)
            for msg
            in messages
        ]

        test_names = {
            prj.project_id
            for prj
            in cloud_asset_mock_client.fetch_projects()
        }

        real_names = {
            msg["project_id"]
            for msg
            in tasks_data
        }

        assert test_names == real_names

        for data in tasks_data:
            match data["project_id"]:
                case "test_prj1":
                    assert data["data_catalog_api_enabled"] is False
                    assert data["dataplex_api_enabled"] is False
                case "test_prj2":
                    assert data["data_catalog_api_enabled"] is False
                    assert data["dataplex_api_enabled"] is True
                case "test_prj3":
                    assert data["data_catalog_api_enabled"] is True
                    assert data["dataplex_api_enabled"] is False
                case "test_prj4":
                    assert data["data_catalog_api_enabled"] is True
                    assert data["dataplex_api_enabled"] is True
