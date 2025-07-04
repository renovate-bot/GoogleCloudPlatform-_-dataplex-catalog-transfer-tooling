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
This module provides a class for project setup.
With this class you can create jobs, handlers and schedulers
"""
from google.cloud.run import (
    JobsClient,
    Job,
    ExecutionTemplate,
    TaskTemplate,
    RevisionTemplate,
    Container,
    ResourceRequirements,
    ServicesClient,
    Service,
)
from google.cloud.scheduler import (
    CloudSchedulerClient,
    Job as SchedulerJob,
    HttpTarget
)


class CloudSetup:
    """
    This class is for project setup.
    With this class you can create jobs, handlers and schedulers
    """

    def __init__(
            self,
            project_id: str,
            location: str,
            service_account_email: str
    ):
        self.project_id = project_id
        self.location = location
        self.service_account_email = service_account_email
        self._job_client = JobsClient()
        self._scheduler_client = CloudSchedulerClient()
        self._service_client = ServicesClient()

    def create_job(
            self,
            job_name: str,
            image: str,
            cli_args: list[str],
            resources: dict[str, str] = None
    ):
        """
        Create a job
        """
        container = Container({
            "name": "test",
            "image": image,
            "command": ["python", "main.py"],
            "args": cli_args,
        })

        if resources is not None:
            container.resources = ResourceRequirements({
                "limits": resources
            })

        task_template = TaskTemplate({
            "containers": [container],
            "service_account": self.service_account_email
        })
        job_template = ExecutionTemplate({
            "template": task_template,
        })
        job = Job({
            "template": job_template,
        })
        self._job_client.create_job(
            parent=f"projects/{self.project_id}/locations/{self.location}",
            job=job,
            job_id=job_name,
        )

        return job

    def create_scheduler(
            self,
            job_name: str,
            cron_expression: str
    ):
        """
        Create a scheduler
        """
        http_target = HttpTarget({
            "uri": f"https://{self.location}-run.googleapis.com/apis/"
                   f"run.googleapis.com/v1/namespaces/"
                   f"{self.project_id}/jobs/{job_name}:run",
            "http_method": "POST",
        })
        job = SchedulerJob({
            "name": f"projects/{self.project_id}/"
                    f"locations/{self.location}/"
                    f"jobs/{job_name}",
            "http_target": http_target,
            "schedule": cron_expression,
        })
        scheduler = self._scheduler_client.create_job(
            parent=f"projects/{self.project_id}/locations/{self.location}",
            job=job,
        )

        return scheduler

    def create_service(
            self,
            service_name: str,
            image: str,
            cli_args: list[str],
            resources: dict[str, str] = None
    ):
        """
        Create a service
        """
        container = Container({
            "name": "test",
            "image": image,
            "command": ["python", "main.py"],
            "args": cli_args,
        })

        if resources is not None:
            container.resources = ResourceRequirements({
                "limits": resources
            })

        revision_template = RevisionTemplate({
            "containers": [container],
            "service_account": self.service_account_email
        })
        service = Service({
            "template": revision_template,
        })
        self._service_client.create_service(
            parent=f"projects/{self.project_id}/locations/{self.location}",
            service=service,
            service_id=service_name,
        )

        return service
