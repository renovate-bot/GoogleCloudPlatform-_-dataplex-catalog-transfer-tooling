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
Transfer Resources handler tests
"""

from typing import Generator
import pytest

from google.api_core.exceptions import AlreadyExists
from google.cloud import datacatalog_v1

from common.api import DataplexApiAdapter
from common.entities.request_models import ResourceTaskData, ResourceData
from services.handlers.transfer_resources.handler import CloudTaskHandler


class TestTransferResourcesHandler:
    """
    Fetch Policies handler tests
    """

    @pytest.fixture(scope="class")
    def handler(self, basic_config: dict) -> CloudTaskHandler:
        """
        Provides a CloudTaskHandler instance for testing.
        """
        handler = CloudTaskHandler(basic_config)
        return handler

    @pytest.fixture(scope="class", autouse=True)
    def create_test_data(self, handler: CloudTaskHandler) -> Generator:
        """
        Creates test data in Data Catalog and Dataplex for testing.
        Ensures cleanup after tests.
        """
        dataplex_client = DataplexApiAdapter()
        try:
            handler._datacatalog_client.create_entry_group(
                handler.project_name, "us-west1", "test_e1"
            )
        except AlreadyExists:
            pass
        try:
            field = datacatalog_v1.TagTemplateField(
                {
                    "display_name": "f1",
                    "type_": datacatalog_v1.FieldType(
                        {
                            "primitive_type": (
                                datacatalog_v1.FieldType.PrimitiveType.STRING
                            )
                        }
                    ),
                }
            )
            handler._datacatalog_client.create_tag_template(
                handler.project_name, "us-west1", "test_t1", {"f1": field}
            )
        except AlreadyExists:
            pass

        yield None

        try:
            handler._datacatalog_client.delete_entry_group(
                handler.project_name, "us-west1", "test_e1"
            )
        except Exception:
            pass
        try:
            handler._datacatalog_client.delete_tag_template(
                handler.project_name, "us-west1", "test_t1"
            )
        except Exception:
            pass
        try:
            dataplex_client.delete_entry_group(
                handler.project_name, "us-west1", "test_e1"
            )
        except Exception:
            pass
        try:
            dataplex_client.delete_aspect_type(
                handler.project_name, "global", "test_t1"
            )
        except Exception:
            pass

    @pytest.mark.parametrize(
        "rtype, resource",
        [
            ("EntryGroup", "test_e1"),
            ("EntryGroup", "test_e2"),
            # ("TagTemplate", "test_t1"),
            ("TagTemplate", "test_t2"),
        ],
    )
    def test_transfer_resources_handler(
        self, handler: CloudTaskHandler, rtype: str, resource: str
    ) -> None:
        """
        Tests the Transfer Resources handler for various resource
        types and names.
        """
        request = ResourceTaskData(
            resource_type=rtype,
            resource=ResourceData(
                location="us-west1",
                resource_name=resource,
                project_id=handler.project_name,
            ),
        )

        response = handler.handle_cloud_task(request)
        if resource in ["test_e1", "test_t1"]:
            assert response == ({"message": "Task processed"}, 200)
        elif rtype == "EntryGroup":
            assert response == (
                {
                    'message': f'Resource '
                               f'projects/{handler.project_name}'
                               f'/locations/us-west1'
                               f'/entryGroups/test_e2 '
                               f'not found'
                },
                200
            )
        else:
            assert response == (
                {
                    "message": f"Error occurred 404 Template "
                    f"{handler.project_name}.{resource}@us-west1 "
                    f"not found."
                },
                500,
            )
