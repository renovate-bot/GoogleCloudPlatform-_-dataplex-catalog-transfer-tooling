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
Clean up handler tests
"""

from typing import Generator
from unittest import mock

import pytest
from google.api_core.exceptions import AlreadyExists
from google.cloud import datacatalog_v1

from common.entities.request_models import ResourceTaskData, ResourceData
from services.handlers.clean_up.handler import CloudTaskHandler


class TestCleanUpHandler:
    """
    Clean up handler tests
    """

    @pytest.fixture(scope="class")
    def handler(self) -> CloudTaskHandler:
        """
        Provides a CloudTaskHandler instance for testing.
        """
        handler = CloudTaskHandler()
        return handler

    @pytest.fixture(scope="class", autouse=True)
    def create_test_data(
        self, handler: CloudTaskHandler, basic_config: dict
    ) -> Generator:
        """
        Creates test data in Data Catalog for testing.
        Ensures cleanup after tests.
        """
        try:
            handler._datacatalog_client.create_entry_group(
                basic_config["project_name"], "us-west1", "test_e1"
            )
        except AlreadyExists:
            pass
        try:
            handler._datacatalog_client.create_entry_group(
                basic_config["project_name"], "us-west1", "test_e2"
            )
        except AlreadyExists:
            pass

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

        try:
            handler._datacatalog_client.create_tag_template(
                basic_config["project_name"],
                "us-west1",
                "test_t1",
                {"f1": field},
            )
        except AlreadyExists:
            pass

        try:
            handler._datacatalog_client.create_tag_template(
                basic_config["project_name"],
                "us-west1",
                "test_t2",
                {"f1": field},
            )
        except AlreadyExists:
            pass

        yield None

        try:
            handler._datacatalog_client.delete_entry_group(
                basic_config["project_name"], "us-west1", "test_e1"
            )
        except Exception:
            pass
        try:
            handler._datacatalog_client.delete_entry_group(
                basic_config["project_name"], "us-west1", "test_e2"
            )
        except Exception:
            pass
        try:
            handler._datacatalog_client.delete_tag_template(
                basic_config["project_name"], "us-west1", "test_t1"
            )
        except Exception:
            pass
        try:
            handler._datacatalog_client.delete_tag_template(
                basic_config["project_name"], "us-west1", "test_t2"
            )
        except Exception:
            pass

    @pytest.mark.parametrize(
        "rtype, resource, expected_message, expected_code",
        [
            (
                "EntryGroup",
                "test_e1",
                (
                    "Entry group projects/hl2-gogl-dapx-t1iylu/locations"
                    "/us-west1/entryGroups/test_e1 not transferred"
                ),
                200,
            ),
            ("EntryGroup", "test_e2", "Task processed", 200),
            (
                "TagTemplate",
                "test_t1",
                (
                    "Tag template projects/hl2-gogl-dapx-t1iylu/locations"
                    "/us-west1/tagTemplates/test_t1 not transferred"
                ),
                200,
            ),
            ("TagTemplate", "test_t2", "Task processed", 200),
            (
                "EntryGroup",
                "test_e100",
                (
                    "Resource projects/hl2-gogl-dapx-t1iylu/locations"
                    "/us-west1/entryGroups/test_e100 not found"
                ),
                200,
            ),
        ],
    )
    def test_clean_up_handler(
        self,
        handler: CloudTaskHandler,
        basic_config: dict,
        rtype: str,
        resource: str,
        expected_message: str,
        expected_code: int,
    ) -> None:
        """
        Tests the Clean up handler for various resource
        types and names.
        """
        request = ResourceTaskData(
            resource_type=rtype,
            resource=ResourceData(
                location="us-west1",
                resource_name=resource,
                project_id=basic_config["project_name"],
            ),
        )
        if resource == "test_e2":
            with mock.patch.object(
                handler._datacatalog_client, "get_entry_group"
            ) as get_entry_group_mocked:
                get_entry_group_mocked.return_value = datacatalog_v1.EntryGroup(
                    transferred_to_dataplex=True
                )
                response, status_code = handler.handle_cloud_task(request)
        elif resource == "test_t2":
            with mock.patch.object(
                handler._datacatalog_client, "get_tag_template"
            ) as get_tag_template_mocked:
                get_tag_template_mocked.return_value = (
                    datacatalog_v1.TagTemplate(dataplex_transfer_status=2)
                )
                response, status_code = handler.handle_cloud_task(request)
        else:
            response, status_code = handler.handle_cloud_task(request)

        assert response["message"].strip() == expected_message
        assert status_code == expected_code
