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
This module provides adapters for interacting with Google Cloud's Data Catalog
and Cloud Asset services.
"""

from common.api.data_catalog_api_adapter import DatacatalogApiAdapter
from common.api.cloud_asset_api_adapter import CloudAssetApiAdapter
from common.api.resource_manager_api_adapter import ResourceManagerApiAdapter
from common.api.dataplex_api_adapter import DataplexApiAdapter
