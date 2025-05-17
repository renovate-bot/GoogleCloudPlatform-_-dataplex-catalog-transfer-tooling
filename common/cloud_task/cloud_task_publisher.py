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

import json
import os
import time
from functools import cache
from typing import Iterable
from math import ceil

import google.auth.transport.requests
import google.cloud.tasks_v2 as tasks
from google.api_core.exceptions import NotFound, AlreadyExists
from google.cloud.tasks_v2 import Queue, RateLimits
from google.cloud.tasks_v2.services.cloud_tasks.pagers import ListTasksPager
from google.cloud.tasks_v2.types import OidcToken

from common.api.resource_manager_api_adapter import ResourceManagerApiAdapter
from common.utils import get_logger


class CloudTaskPublisher(object):
    """
    A publisher class for creating and submitting tasks to Google Cloud Tasks.
    """

    def __init__(
        self,
        project: str,
        location: str,
        queue: str,
        max_rps: int = 60,
        wait_after_queue_creation: int = 60,
    ) -> None:
        """
        Initializes the CloudTaskPublisher with the necessary configuration.
        """
        self.project = project
        self.location = location
        self.queue_name = queue
        self.max_rps = max_rps
        self._wait_after_queue_creation = wait_after_queue_creation
        self._cloud_task_client = tasks.CloudTasksClient()
        self._resource_manager_client = ResourceManagerApiAdapter()
        self._queue_fqn = self.get_queue_fqn(
            self.project, self.location, self.queue_name
        )
        self._logger = get_logger()

    @cache
    def _get_service_account_email(self) -> str:
        """
        Retrieves the service account email associated with the
        current Google Cloud credentials.
        """
        credentials, _ = google.auth.default(
            scopes=["https://www.googleapis.com/auth/cloud-platform"]
        )
        request = google.auth.transport.requests.Request()
        credentials.refresh(request=request)
        # Local testing
        if "CLOUDTASK_OIDC_SERVICE_ACCOUNT" in os.environ:
            return os.environ["CLOUDTASK_OIDC_SERVICE_ACCOUNT"]
        else:
            return credentials.service_account_email

    def get_queue_fqn(
        self, project: str, location: str, queue_name: str
    ) -> str:
        """
        Constructs the fully qualified name (FQN) of the queue.
        """
        return self._cloud_task_client.queue_path(project, location, queue_name)

    def create_task(
        self,
        json_payload: dict | list,
        service_name: str,
        project: str = None,
        location: str = None,
        queue_name: str = None,
    ) -> tasks.Task:
        """
        Creates a task with a JSON payload and adds it to the specified queue.
        """
        project = project or self.project
        location = location or self.location
        queue_name = queue_name or self.queue_name

        url = self._form_service_url(service_name, project, location)

        http_request = tasks.HttpRequest(
            {
                "http_method": tasks.HttpMethod.POST,
                "url": url,
                "headers": {
                    "Content-type": "application/json",
                },
                "body": json.dumps(json_payload).encode(),
                "oidc_token": OidcToken(
                    service_account_email=self._get_service_account_email()
                ),
            }
        )

        task = tasks.Task({"http_request": http_request})
        queue_fqn = self._cloud_task_client.queue_path(
            project, location, queue_name
        )

        create_request = tasks.CreateTaskRequest(
            {
                "parent": queue_fqn,
                "task": task,
            }
        )

        if not self.check_queue_exists(project, location, queue_name):
            self._logger.info(
                "Queue %s does not exist. "
                "Queue will be created automatically.",
                queue_fqn,
            )
            self.create_queue(project, location, queue_name)

        try:
            task = self._cloud_task_client.create_task(create_request)
            self._logger.info(
                "Created task. Endpoint: %s, payload: %s",
                url,
                json.dumps(json_payload),
            )
        except NotFound as e:
            self._logger.info("Queue %s does not exist", self._queue_fqn)
            raise e

        return task

    def create_task_by_message_location(
        self,
        json_payload: dict | list,
        service_name: str,
        message_location: str,
        project: str = None,
        location: str = None,
    ) -> tasks.Task:
        queue_name = self.queue_name + "-" + message_location

        return self.create_task(
            json_payload, service_name, project, location, queue_name
        )

    def create_queue(
        self,
        project: str = None,
        location: str = None,
        queue_name: str = None,
        max_rps: int = None,
    ) -> Queue:
        """
        Creates a queue with Google Cloud Queues.
        """
        project = project or self.project
        location = location or self.location
        queue_name = queue_name or self.queue_name
        max_rps = max_rps or self.max_rps

        queue_fqn = self.get_queue_fqn(project, location, queue_name)
        parent = f"projects/{project}/locations/{location}"

        rate_limits = RateLimits(
            {
                "max_dispatches_per_second": max_rps,
            }
        )
        queue = Queue({"name": queue_fqn, "rate_limits": rate_limits})

        try:
            result = self._cloud_task_client.create_queue(
                request={"parent": parent, "queue": queue}
            )
            time.sleep(self._wait_after_queue_creation)
            self._logger.info("Created queue: %s", queue_fqn)
        except AlreadyExists:
            result = self._cloud_task_client.get_queue(name=queue_fqn)

        return result

    def update_queue(
        self,
        project: str = None,
        location: str = None,
        queue_name: str = None,
        max_rps: int = None,
    ):
        """
        Updates a queue.
        """
        project = project or self.project
        location = location or self.location
        queue_name = queue_name or self.queue_name
        max_rps = max_rps or self.max_rps

        queue_fqn = self.get_queue_fqn(project, location, queue_name)

        rate_limits = RateLimits(
            {
                "max_dispatches_per_second": max_rps,
            }
        )
        queue = Queue({"name": queue_fqn, "rate_limits": rate_limits})

        result = self._cloud_task_client.update_queue(queue=queue)
        self._logger.info("Updated queue: %s", queue_fqn)

        return result

    def check_queue_exists(
        self,
        project: str = None,
        location: str = None,
        queue_name: str = None,
    ) -> bool:
        """
        Checks if a queue exists.
        """
        project = project or self.project
        location = location or self.location
        queue_name = queue_name or self.queue_name

        queue_fqn = self.get_queue_fqn(project, location, queue_name)

        try:
            self._cloud_task_client.get_queue(name=queue_fqn)
            return True
        except NotFound:
            return False

    def _form_service_url(
        self, service_name: str, project: str, location: str
    ) -> str:
        """
        Form a service URL for cloud task.
        """
        project_number = self._get_project_number(project)

        return f"https://{service_name}-{project_number}.{location}.run.app"

    def _get_project_number(self, project: str) -> str:
        """
        Get the project number using project_id.
        """
        return self._resource_manager_client.get_project_number(project)

    def delete_queue(
        self,
        project: str = None,
        location: str = None,
        queue_name: str = None,
    ) -> None:
        """
        Deletes the queue.
        """
        project = project or self.project
        location = location or self.location
        queue_name = queue_name or self.queue_name

        queue_fqn = self.get_queue_fqn(project, location, queue_name)

        self._logger.info("Deleting queue: %s", queue_fqn)
        self._cloud_task_client.delete_queue(name=queue_fqn)
        self._logger.info("Deleted queue: %s", queue_fqn)

    def purge_queue(
        self,
        project: str = None,
        location: str = None,
        queue_name: str = None,
    ) -> None:
        """
        Purges the queue.
        """
        project = project or self.project
        location = location or self.location
        queue_name = queue_name or self.queue_name

        queue_fqn = self.get_queue_fqn(project, location, queue_name)

        self._logger.info("Purging queue: %s", queue_fqn)
        self._cloud_task_client.purge_queue(name=queue_fqn)
        self._logger.info("Purged queue: %s", queue_fqn)

    def get_messages(
        self,
        project: str = None,
        location: str = None,
        queue_name: str = None,
    ) -> ListTasksPager:
        """
        Get messages from the queue.
        """
        project = project or self.project
        location = location or self.location
        queue_name = queue_name or self.queue_name

        queue_fqn = self.get_queue_fqn(project, location, queue_name)

        return self._cloud_task_client.list_tasks(
            request={"parent": queue_fqn, "response_view": 2}
        )

    def prepare_queues_for_locations(
        self, msg_locations: Iterable[str], quota: int, quota_consumption: int
    ) -> None:
        """
        Creates queues for the specified message locations.
        """
        for msg_location in msg_locations:
            new_queue_name = self.queue_name + "-" + msg_location
            if self.check_queue_exists(queue_name=new_queue_name):
                self.purge_queue(queue_name=new_queue_name)
            else:
                self.create_queue(
                    queue_name=new_queue_name,
                    max_rps=ceil(quota * (quota_consumption / 100)),
                )
