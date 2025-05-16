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
This module defines a server application using the Fastapi framework.
It is designed to handle cloud tasks related to Google Cloud Platform (GCP)
resources, specifically for managing entry groups and tag templates in a data
catalog.
"""

from typing import Any
from fastapi import FastAPI, Response
import uvicorn

from handler import CloudTaskHandler
from common.entities import ConvertPrivateTagTemplatesTaskData


app = FastAPI()
handler = CloudTaskHandler()


@app.api_route("/", methods=["POST", "PUT"])
async def process_task(
    task_data: ConvertPrivateTagTemplatesTaskData, response: Response
) -> dict[str, Any]:
    """
    Route to process cloud tasks.
    """
    body, status_code = handler.handle_cloud_task(task_data)
    response.status_code = status_code

    return body


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8080)
