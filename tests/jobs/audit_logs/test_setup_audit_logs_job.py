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
Setup audit logs job tests
"""

import random
from typing import Generator

import pytest

from services.jobs.audit_logs.setup_audit_log import AuditLogsSetup


class TestAuditLogsSetup:
    """
    Setup audit logs job tests
    """

    @pytest.fixture(scope="class")
    def full_config(self, basic_config: dict) -> dict:
        """
        Extends the basic configuration with a unique queue name by
        appending a random suffix.
        """
        suffix = random.randint(1, 1000000)
        basic_config["dataset_name"] += "_" + str(suffix)

        return basic_config

    @pytest.fixture(scope="class")
    def log_filter(self) -> str:
        """
        Fixture to provide a log filter.
        """
        return """
            protoPayload.serviceName="datacatalog.googleapis.com"
            """

    @pytest.fixture(scope="class")
    def audit_log_job(self, full_config: dict) -> Generator:
        """
        Fixture to create an AuditLogsSetup instance and ensure
        cleanup after the test.
        """
        audit_log_job = AuditLogsSetup(full_config)
        yield audit_log_job
        audit_log_job.delete_sink()

    def test_create_sink(
        self, audit_log_job: AuditLogsSetup, log_filter: str, full_config: dict
    ) -> None:
        """
        Test case to validate the creation of a log sink.
        """
        expected_destination = (
            f"bigquery.googleapis.com/projects/{full_config['project_name']}"
            f"/datasets/{full_config['dataset_name']}"
        )

        audit_log_job.create_sink(log_filter)

        sink = audit_log_job.get_sink()

        assert sink.name == full_config["log_sink_name"]
        assert sink.destination == expected_destination
        assert sink.writer_identity is not None
        assert sink.filter.strip() == log_filter.strip()
