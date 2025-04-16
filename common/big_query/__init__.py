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
This module provides utilities for interacting with Google BigQuery through
a high-level adapter. It is designed to simplify the process of querying,
inserting, and managing data within BigQuery.
"""

from common.big_query.big_query_adapter import BigQueryAdapter
from common.big_query.schema_provider import TableNames
from common.big_query.view_provider import ViewNames
