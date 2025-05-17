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
Module for configuring test environment variables.
"""

import os
import pytest


@pytest.fixture(scope="class")
def basic_config():
    """
    Provides a basic configuration dictionary for the test environment.
    """
    return {
        "project_name": os.environ.get("PROJECT"),
        "service_location": os.environ.get("SERVICE_LOCATION", "us-west1"),
        "dataset_location": os.environ.get("DATASET_LOCATION", "US"),
        "dataset_name": os.environ.get("DATASET_NAME", "transfer_tooling_test"),
        "queue": os.environ.get("QUEUE", "transfer-tooling-test"),
        "handler_name": os.environ.get(
            "HANDLER_NAME", "transfer-tooling-handler-test"
        ),
        "log_sink_name": os.environ.get("LOG_SINK_NAME", "test-sink"),
    }
