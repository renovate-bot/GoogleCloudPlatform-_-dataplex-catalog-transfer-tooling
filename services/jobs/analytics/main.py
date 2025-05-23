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
This module serves as the entry point for setting up analytical views
in BigQuery.

It initializes the `TransferController` with application configuration settings
and invokes the process to create predefined analytical views in the specified
BigQuery dataset.
"""

from setup_analytics_views import TransferController
from config import get_application_config


def main(app_config: dict) -> None:
    """
    Main function to set up analytical views in BigQuery.

    This function initializes the `TransferController` with the provided
    application configuration and invokes the `create_analytical_views`
    method to create the required views in the specified BigQuery dataset.
    """

    controller = TransferController(app_config)
    controller.create_analytical_views()


if __name__ == "__main__":
    config = get_application_config()
    main(config)
