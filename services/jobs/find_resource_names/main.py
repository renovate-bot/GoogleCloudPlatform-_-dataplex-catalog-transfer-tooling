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
This script initializes and starts a cloud task consumer server and a data
transfer process.

The script uses threading to run an ASGI server for handling cloud tasks
concurrently with a data transfer operation. The data transfer is managed
by the TransferController, which orchestrates the retrieval and storage of
data from the Google Cloud Data Catalog to BigQuery.

Functions:
- main: Initializes and starts the cloud task consumer and data transfer
  process.
"""

from transfer_controller import TransferController
from config import get_application_config


def main(app_config: dict):
    """
    Starts the cloud task consumer server and initiates the data transfer
    process.
    """

    controller = TransferController(app_config)
    controller.start_transfer()


if __name__ == "__main__":
    config = get_application_config()
    main(config)
