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
This module provides a Flask application that handles cloud tasks related to
project management and data processing. It integrates with 
ResourceManagerApiAdapter and BigQueryAdapter to manage project data and
store it in BigQuery.
"""

from flask import Flask, request, jsonify
from asgiref.wsgi import WsgiToAsgi
import json
import datetime
import uvicorn
from common.api import ResourceManagerApiAdapter
from common.entities import Project
from common.big_query import BigQueryAdapter
from common.utils import get_logger
from config import get_application_config


app = Flask(__name__)
asgi_app = WsgiToAsgi(app)
logger = get_logger()


class CloudTaskHandler:
    """
    A handler class for processing cloud tasks related to project management.
    """

    def __init__(self, app_config):
        """
        Initializes the CloudTaskHandler with ResourceManagerApiAdapter and
        BigQueryAdapter clients.
        """
        self.project_name = app_config["project_name"]
        self.dataset_name = app_config["dataset_name"]
        self._resource_manager_client = ResourceManagerApiAdapter()
        self._big_query_client = BigQueryAdapter(app_config)

    async def handle_cloud_task(self, task_data):
        """
        Processes a single cloud task by extracting project information,
        fetching project ancestry, and writing the project data to a BigQuery
        table.
        """
        project = Project(task_data["project_id"], task_data["project_number"])
        project.set_dataplex_api_enabled(
            task_data.get("dataplex_api_enabled", False)
        )
        project.set_data_catalog_api_enabled(
            task_data.get("data_catalog_api_enabled", False)
        )

        ancestors = self._resource_manager_client.get_project_ancestry(
            project.project_id
        )
        project.set_ancestry(ancestors)

        self._big_query_client.write_to_table(
            f"{self.project_name}.{self.dataset_name}.projects",
            [project],
            creation_date=datetime.datetime.fromisoformat(
                task_data["created_at"]
            ).date(),
        )

        return jsonify({"message": "Task processed"}), 200


@app.route("/", methods=["POST", "PUT"])
async def process_task():
    """
    Route to process cloud tasks.
    """
    task_data = json.loads(request.data)
    config = get_application_config()
    handler = CloudTaskHandler(config)
    return await handler.handle_cloud_task(task_data)


if __name__ == "__main__":
    uvicorn.run(asgi_app, host="0.0.0.0", port=8080)
