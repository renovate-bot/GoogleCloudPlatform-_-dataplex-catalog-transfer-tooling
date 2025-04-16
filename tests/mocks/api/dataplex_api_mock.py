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
Mock for dataplex client
"""

class DataplexApiMock:
    """
    Mock for dataplex client
    """
    def __init__(self, bindings):
        self.bindings = bindings
        self.result = None

    def projects(self):
        return self

    def locations(self):
        return self

    def aspectTypes(self):
        return self

    def entryGroups(self):
        return self

    def getIamPolicy(self, resource):
        self.result = {
            "bindings": [
                {
                    "role": key,
                    "members": value
                }
                for key, value
                in self.bindings.get(resource, {}).items()
            ]
        }
        return self

    def execute(self, **_):
        result = self.result
        self.result = None
        return result
