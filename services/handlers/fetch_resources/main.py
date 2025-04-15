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
This module defines a server application using the Flask framework.
It is designed to handle cloud tasks related to Google Cloud Platform (GCP)
resources, specifically for managing entry groups and tag templates in a data
catalog.
"""

from flask import Flask, request, jsonify
from asgiref.wsgi import WsgiToAsgi
import uvicorn
import json
from datetime import datetime
from googleapiclient.errors import HttpError
from common.api.resource_manager_api_adapter import ResourceManagerApiAdapter
from common.exceptions.exceptions import ValidationError
from common.api import DatacatalogApiAdapter
from common.big_query import BigQueryAdapter
from common.cloud_task import CloudTaskPublisher
from common.utils import get_logger
from config import get_application_config


app = Flask(__name__)
asgi_app = WsgiToAsgi(app)
logger = get_logger()


class CloudTaskHandler:
    """
    Handles cloud tasks, validates task data, and manages the lifecycle of
    the application. This class interacts with GCP resources to manage entry
    groups and tag templates.
    """

    def __init__(self, app_config):
        """
        Initializes the CloudTaskHandler with configuration for the GCP project
        and other settings.
        """
        self.project = app_config["project_name"]
        self.service_location = app_config["service_location"]
        self.queue = app_config["queue"]
        self.handler_name = app_config["handler_name"]
        self.dataset_name = app_config["dataset_name"]
        self._resource_manager_client = ResourceManagerApiAdapter()
        self.api_client = DatacatalogApiAdapter()
        self.big_query_client = BigQueryAdapter(app_config)
        self.cloud_task_publisher = CloudTaskPublisher(
            self.project, self.service_location, self.queue
        )


    def _validate_task_data(self, task_data):
        """
        Validates the incoming task data against required fields and types.
        """
        required_fields = {
            "scope": str,
            "type": str,
            "nextPageToken": (str, type(None)),
            "is_transferred": bool,
            "createdAt": str,
        }

        missing_fields = [
            field for field in required_fields if field not in task_data
        ]
        if missing_fields:
            raise ValidationError(
                f"Missing required fields: {', '.join(missing_fields)}"
            )

        for field, expected_types in required_fields.items():
            if isinstance(expected_types, tuple):
                if not isinstance(task_data[field], expected_types):
                    expected_type_names = ", ".join(
                        t.__name__ for t in expected_types
                    )
                    raise ValidationError(
                        f"Invalid type for '{field}', "
                        f"expected one of: {expected_type_names}"
                    )
            else:
                if not isinstance(task_data[field], expected_types):
                    raise ValidationError(
                        f"Invalid type for '{field}', "
                        f"expected {expected_types.__name__}"
                    )
        try:
            self._resource_manager_client.get_project_number(self.project)
        except HttpError as e:
            raise ValidationError(
                f"Not enough permissions or {self.project} doesn't exists"
            ) from e

        if task_data["type"] not in ["entry_group", "tag_template"]:
            raise ValidationError("Invalid entry type for 'type'")

        date_format = "%Y-%m-%d"
        try:
            datetime.strptime(task_data["createdAt"], date_format)
        except ValueError as exc:
            raise ValidationError(
                "Invalid date format for 'createdAt'. "
                f"Expected format: {date_format}"
            ) from exc

        if task_data["type"] == "tag_template":
            if "is_public" not in task_data:
                raise ValidationError(
                    "Missing required field: 'is_public' for tag_template"
                )
            if not isinstance(task_data["is_public"], bool):
                raise ValidationError(
                    "Invalid type for 'is_public', expected bool"
                )

    async def handle_cloud_task(self, task_data):
        """
        Processes the task data, interacts with the Datacatalog API, and writes
        results to BigQuery.
        """
        try:
            self._validate_task_data(task_data)
        except ValidationError as e:
            return jsonify({"error": str(e)}), 400

        match task_data["type"]:
            case "entry_group":
                api_result, next_page_token = (
                    self.api_client.search_entry_groups(
                        [task_data["scope"]],
                        task_data["is_transferred"],
                        page_token=task_data["nextPageToken"],
                    )
                )
            case "tag_template":
                api_result, next_page_token = (
                    self.api_client.search_tag_templates(
                        [task_data["scope"]],
                        task_data["is_public"],
                        task_data["is_transferred"],
                        page_token=task_data["nextPageToken"],
                    )
                )
            case _:
                api_result, next_page_token = (None, None)

        if api_result:
            converted_date = datetime.strptime(
                task_data["createdAt"], "%Y-%m-%d"
            ).date()

            self.big_query_client.write_to_table(
                f"{self.project}.{self.dataset_name}.{task_data["type"]}s",
                api_result,
                converted_date,
            )

        if next_page_token:
            payload = task_data.copy()
            payload["nextPageToken"] = next_page_token

            self.cloud_task_publisher.create_task(
                payload,
                self.handler_name,
                self.project,
                self.service_location
            )

        return jsonify({"message": "Task processed"}), 200

@app.route('/', methods=['POST', 'PUT'])
async def process_task():
    """
    Route to process cloud tasks.
    """
    task_data = json.loads(request.data)
    config = get_application_config()
    handler = CloudTaskHandler(config)
    return  await handler.handle_cloud_task(task_data)

if __name__ == "__main__":
    uvicorn.run(asgi_app, host="0.0.0.0", port=8080)
