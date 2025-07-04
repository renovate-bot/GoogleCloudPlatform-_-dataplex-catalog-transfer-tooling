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
This module provides functionality to configure and manage Google Cloud
Logging sinks for exporting audit logs to BigQuery. It includes the
creation of a log sink with partitioned tables and handles cases where
the sink already exists.
"""

from google.cloud import logging_v2
from google.cloud.logging_v2.types import (
    LogSink,
    BigQueryOptions,
    CreateSinkRequest,
    DeleteSinkRequest,
)
from google.cloud.logging_v2 import _gapic as gapic
from google.api_core.exceptions import AlreadyExists

from common.big_query import BigQueryAdapter
from common.utils import get_logger


class AuditLogsSetup:
    """
    A class to set up and manage Google Cloud Logging sinks for exporting
    audit logs.
    """

    def __init__(self, app_config: dict) -> None:
        """
        Initializes the AuditLogsSetup instance with the given application
        configuration.
        """
        self.project = app_config["project_name"]
        self.dataset = app_config["dataset_name"]
        self.dataset_location = app_config["dataset_location"]
        self.log_sink_name = app_config["log_sink_name"]
        self.logging_client = logging_v2.Client()
        self._big_query_client = BigQueryAdapter(
            self.project,
            self.dataset_location,
            self.dataset,
        )
        self._logger = get_logger()
        self.grpc_client = gapic.make_sinks_api(client=self.logging_client)

    def create_sink(self, log_filter: str) -> None:
        """
        Creates a Google Cloud Logging sink to export logs to BigQuery. If
        the sink already exists, it retrieves the existing sink.
        """
        try:
            self._big_query_client.ensure_dataset_exists()
            log_sink = LogSink(
                name=self.log_sink_name,
                destination=(
                    "bigquery.googleapis.com/projects/"
                    f"{self.project}/datasets/{self.dataset}"
                ),
                filter=log_filter,
                description="",
                include_children=True,
                bigquery_options=BigQueryOptions(use_partitioned_tables=True),
            )

            request = CreateSinkRequest(
                parent=f"projects/{self.project}",
                sink=log_sink,
                unique_writer_identity=True,
            )

            sink = self.grpc_client._gapic_api.create_sink(request=request)
            self._logger.info("Sink %s has been created.", sink.name)
        except AlreadyExists:
            sink = self.get_sink()
            self._logger.info("Sink %s already exists.", sink.name)

        self._logger.info(
            "Sink name: %s, sink destination: %s, sink writer_identity: %s",
            sink.name,
            sink.destination,
            sink.writer_identity,
        )

    def get_sink(self) -> LogSink:
        """
        Retrieves the details of the Google Cloud Logging sink with
        the specified name.
        """
        return self.grpc_client._gapic_api.get_sink(
            sink_name=f"projects/{self.project}/sinks/{self.log_sink_name}"
        )

    def delete_sink(self) -> None:
        """
        Deletes the Google Cloud Logging sink with the specified name.
        """
        request = DeleteSinkRequest(
            sink_name=f"projects/{self.project}/sinks/{self.log_sink_name}"
        )
        self.grpc_client._gapic_api.delete_sink(request=request)
        self._logger.info("Sink %s has been deleted.", request.sink_name)
