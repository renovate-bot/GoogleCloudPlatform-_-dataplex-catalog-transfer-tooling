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
This module provides functionality to create and publish tasks to
Google Cloud Tasks.
The module uses Google Cloud Tasks API to manage task creation and dispatch.
"""

from common.utils import get_logger
from common.api.resource_manager_api_adapter import ResourceManagerApiAdapter
import google.cloud.tasks_v2 as tasks
from google.cloud.tasks_v2 import Queue, RateLimits
from google.api_core.exceptions import NotFound
import google.auth.transport.requests
import google.oauth2.id_token
import json
import time


class CloudTaskPublisher(object):
    """
    A publisher class for creating and submitting tasks to Google Cloud Tasks.
    """

    def __init__(self, project: str, location: str, queue: str, max_rps: int = 60):
        """
        Initializes the CloudTaskPublisher with the necessary configuration.
        """
        self.project = project
        self.location = location
        self.queue_name = queue
        self.max_rps = max_rps
        self._wait_after_queue_creation = 30
        self._cloud_task_client = tasks.CloudTasksClient()
        self._resource_manager_client = ResourceManagerApiAdapter()
        self._queue_fqn = self._cloud_task_client.queue_path(
            self.project, self.location, self.queue_name
        )
        self._logger = get_logger()

    def create_task(
        self,
        json_payload: dict | list,
            service_name: str,
            project: str = None,
            location: str = None,
    ) -> tasks.Task:
        """
        Creates a task with a JSON payload and adds it to the specified queue.
        """
        project = project or self.project
        location = location or self.location

        url = self._form_service_url(service_name, project, location)

        auth_req = google.auth.transport.requests.Request()
        id_token = google.oauth2.id_token.fetch_id_token(auth_req, url)

        http_request = tasks.HttpRequest(
            {
                "http_method": tasks.HttpMethod.POST,
                "url": url,
                "headers": {
                    "Content-type": "application/json",
                    "Authorization": f"Bearer {id_token}",
                },
                "body": json.dumps(json_payload).encode(),
            }
        )

        task = tasks.Task({"http_request": http_request})

        create_request = tasks.CreateTaskRequest(
            {
                "parent": self._queue_fqn,
                "task": task,
            }
        )

        try:
            task = self._cloud_task_client.create_task(create_request)
            self._logger.info(f"Created task. "
                              f"Endpoint: {url}, "
                              f"payload: {json.dumps(json_payload)}")
        except NotFound as e:
            self._logger.info(f"Queue {self._queue_fqn} does not exist")
            raise e

        return task

    def create_queue(self) -> Queue:
        """
        Creates a queue with Google Cloud Queues.
        """
        parent = f"projects/{self.project}/locations/{self.location}"
        rate_limits = RateLimits({
            "max_dispatches_per_second": self.max_rps,
        })
        queue = Queue({
            "name": self._queue_fqn,
            "rate_limits": rate_limits
        })
        result = self._cloud_task_client.create_queue(
            request={"parent": parent, "queue": queue}
        )
        time.sleep(self._wait_after_queue_creation)

        self._logger.info(f"Created queue: {self._queue_fqn}")

        return result

    def check_queue_exists(self) -> bool:
        """
        Checks if a queue exists.
        """
        try:
            self._cloud_task_client.get_queue(name=self._queue_fqn)
            return True
        except NotFound:
            self._logger.info(f"Queue {self._queue_fqn} does not exist. "
                              f"Queue will be created automatically.")
            return False

    def _form_service_url(
            self,
            service_name: str,
            project: str,
            location: str
    ) -> str:
        """
        Form a service URL for cloud task.
        """
        project_number = self._get_project_number(project)

        return (f"https://{service_name}-"
                f"{project_number}."
                f"{location}.run.app")

    def _get_project_number(self, project) -> str:
        """
        Get the project number using project_id.
        """
        return self._resource_manager_client.get_project_number(project)
