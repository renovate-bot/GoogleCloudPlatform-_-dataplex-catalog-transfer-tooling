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
Convert private tag templates handler tests
"""

from typing import Generator

import pytest
from google.api_core.exceptions import AlreadyExists
from google.cloud import datacatalog_v1

from common.entities.request_models import ConvertPrivateTagTemplatesTaskData
from services.handlers.convert_private_tag_templates.handler import (
    CloudTaskHandler,
)


class TestConvertPrivateTagTemplatesHandler:
    """
    Convert private tag templates handler tests
    """

    @pytest.fixture(scope="class")
    def handler(self) -> CloudTaskHandler:
        """
        Fixture to initialize the CloudTaskHandler instance.
        """
        handler = CloudTaskHandler()

        return handler

    @pytest.fixture(scope="class", autouse=True)
    def create_test_data(
        self, basic_config: dict, handler: CloudTaskHandler
    ) -> Generator:
        """
        Fixture to create and clean up test data for the tests.
        """
        field = datacatalog_v1.TagTemplateField(
            {
                "display_name": "test field",
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
            handler.api_client.create_tag_template(
                project=basic_config["project_name"],
                location="us-west1",
                name="tt_test_1",
                fields={"field": field},
                public=False,
            )
        except AlreadyExists:
            pass

        try:
            handler.api_client.create_tag_template(
                project=basic_config["project_name"],
                location="us-west1",
                name="tt_test_2",
                fields={"field": field},
                public=True,
            )
        except AlreadyExists:
            pass

        yield

        try:
            handler.api_client.delete_tag_template(
                project=basic_config["project_name"],
                location="us-west1",
                name="tt_test_1",
                force=True,
            )
        except Exception:
            pass

        try:
            handler.api_client.delete_tag_template(
                project=basic_config["project_name"],
                location="us-west1",
                name="tt_test_2",
                force=True,
            )
        except Exception:
            pass

    @pytest.mark.parametrize(
        "name, expected_message, expected_code",
        [
            ("tt_test_1", "Task processed.", 200),
            ("tt_test_2", "Tag template is already public.", 200),
            (
                "tt_test_3",
                (
                    "Permission denied for hl2-gogl-dapx-t1iylu."
                    "tt_test_3@us-west1, or resource doesn't exist."
                ),
                400,
            ),
        ],
    )
    def test_convert_private_tag_templates_handler(
        self,
        basic_config: dict,
        handler: CloudTaskHandler,
        name: str,
        expected_message: str,
        expected_code: int,
    ) -> None:
        """
        Tests the handler for converting private tag templates.
        """
        request = ConvertPrivateTagTemplatesTaskData(
            project_id=basic_config["project_name"],
            location="us-west1",
            resource_name=name,
        )

        response, status_code = handler.handle_cloud_task(request)
        assert response["message"].strip() == expected_message
        assert status_code == expected_code
