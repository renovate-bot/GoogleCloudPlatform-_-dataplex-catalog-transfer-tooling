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
Mock for datacatalog client
"""
from google.api_core.exceptions import NotFound


class Bindings:
    def __init__(self, role: str, members: list[str]):
        self.role = role
        self.members = members

class Response:
    def __init__(self, bindings: list[Bindings]):
        self.bindings = bindings


class DatacatalogApiMock:
    """
    Mock for datacatalog client
    """
    def __init__(self, bindings: dict):
        self.resources = {
            resource: Response([
                Bindings(role, members)
                for role, members
                in roles.items()
            ])
            for resource, roles
            in bindings.items()
        }

    def get_iam_policy(self, resource: str):
        if resource not in self.resources:
            raise NotFound(f"{resource} not found")
        return self.resources.get(resource, Response([]))
