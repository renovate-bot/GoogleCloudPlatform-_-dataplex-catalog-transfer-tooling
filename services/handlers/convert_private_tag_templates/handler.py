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
Module for handling Cloud Tasks related to tag template conversion.
"""

from typing import Any

from google.api_core.exceptions import GoogleAPICallError

from common.api import DatacatalogApiAdapter
from common.entities import TagTemplate, ConvertPrivateTagTemplatesTaskData
from common.utils import get_logger


class CloudTaskHandler:
    """
    Handles Cloud Tasks for converting private tag templates to public
    in Google Data Catalog.
    """

    def __init__(self) -> None:
        """
        Initializes the CloudTaskHandler with application configuration.
        """
        self.api_client = DatacatalogApiAdapter()
        self._logger = get_logger()

    def handle_cloud_task(
        self, task_data: ConvertPrivateTagTemplatesTaskData
    ) -> tuple[dict[str, Any], int]:
        """
        Processes a Cloud Task to convert a private tag template to public.
        """
        tt_fqn = TagTemplate.get_old_fqn(
            project_id=task_data.project_id,
            location=task_data.location,
            name=task_data.resource_name,
        )
        try:
            tag_template = self.api_client.get_tag_template(
                project=task_data.project_id,
                location=task_data.location,
                name=task_data.resource_name,
            )

            if tag_template.is_publicly_readable:
                self._logger.info(
                    "Tag template %s is already publicly readable.",
                    tt_fqn,
                )
                return {"message": "Tag template is already public."}, 200

            response = self.api_client.convert_private_tag_template(tt_fqn)

            self._logger.info(
                "Successfully converted tag template: %s to public.",
                response.name,
            )
            return {"message": "Task processed."}, 200
        except GoogleAPICallError as e:
            self._logger.error(
                "Failed to process tag template %s due to error: %s.",
                tt_fqn,
                str(e.message),
            )
            return {"message": e.message}, 400
