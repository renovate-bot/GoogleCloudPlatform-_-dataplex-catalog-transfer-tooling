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
Mock for cloud asset api
"""

from common.entities import Project

class CloudAssetApiMock:
    """
    Mock for cloud asset api
    """
    def __init__(self):
        self.p1 = Project("test_prj1", 111111)
        self.p2 = Project("test_prj2", 222222)
        self.p3 = Project("test_prj3", 333333)
        self.p4 = Project("test_prj4", 444444)

        self.p2.set_dataplex_api_enabled(True)
        self.p3.set_data_catalog_api_enabled(True)
        self.p4.set_dataplex_api_enabled(True)
        self.p4.set_data_catalog_api_enabled(True)


    def fetch_projects(self):
        return [
            self.p1,
            self.p2,
            self.p1,
            self.p3,
            self.p4,
            self.p2
        ]
