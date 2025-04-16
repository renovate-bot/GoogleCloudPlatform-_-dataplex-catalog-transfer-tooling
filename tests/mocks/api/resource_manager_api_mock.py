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
Mock for resource manager api
"""
from googleapiclient.errors import HttpError


class ResourceManagerApiMock:
    """
    Mock for resource manager api
    """
    class HttpErrorContent:
        pass

    def __init__(self):
        self.result = []
        self._projects = {
            "prj1": {
                "id": "prj1",
                "type": "project",
                "ancestor": {
                    "id": "folder11",
                    "type": "folder",
                    "ancestor": {
                        "id": "org111",
                        "type": "organization",
                    }
                }
            },
            "prj2": {
                "id": "prj2",
                "type": "project",
                "ancestor": {
                    "id": "org222",
                    "type": "organization",
                }
            },
            "prj3": {
                "id": "prj3",
                "type": "project",
            }
        }

    def projects(self):
        return self

    def execute(self):
        if self.result == []:
            err_content = self.HttpErrorContent()
            err_content.status = 403
            err_content.reason = "403"
            raise HttpError(err_content, "403".encode("utf8"))

        result = self.result
        self.result = []
        return result

    def getAncestry(self, projectId):
        if projectId not in self._projects:
            return self

        result = []
        item = self._projects[projectId]

        while item:
            result.append({
                "resourceId": {
                    "type": item["type"],
                    "id": item["id"],
                }
            })
            item = item.get("ancestor")

        self.result = {
            "ancestor": result
        }

        return self
