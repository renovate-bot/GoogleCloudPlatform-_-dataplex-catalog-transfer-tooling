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
This module provides an adapter for interacting with Google Cloud Quotas API.
"""

from enum import StrEnum
from math import ceil

from google.cloud import cloudquotas_v1
from google.api_core.exceptions import GoogleAPICallError, NotFound

from common.utils import get_logger


class Services(StrEnum):
    """
    Define a class for service constants
    """

    DATAPLEX = "dataplex.googleapis.com"
    DATA_CATALOG = "datacatalog.googleapis.com"


class Quotas(StrEnum):
    """
    Define a class for quota constants
    """

    CATALOG_MANAGEMENT_READS = (
        "CatalogManagementReadsPerMinutePerProjectPerRegion"
    )
    DATAPLEX_IAM_POLICY_REQUESTS = (
        "DefaultIamPolicyRequestsPerMinutePerProjectPerRegion"
    )
    DATA_CATALOG_READ_REQUESTS = "ReadRequestsPerMinutePerProject"
    CATALOG_MANAGEMENT_PER_USER_READS = (
        "CatalogManagementReadsPerMinutePerProjectPerUserPerRegion"
    )


class QuotaInfoAdapter:
    """
    Adapter for interacting with the Google Cloud Quotas API.
    """

    def __init__(self) -> None:
        """
        Initialize the QuotaInfoAdapter.
        """
        self._client = cloudquotas_v1.CloudQuotasClient()
        self._logger = get_logger()

    def get_default_quota_value(
        self, project: str, service: str, quota: str
    ) -> int | None:
        """
        Get the quota value for a specific quota and region.
        """
        resource_name = (
            f"projects/{project}/locations/global/"
            f"services/{service}/quotaInfos/{quota}"
        )

        request = cloudquotas_v1.GetQuotaInfoRequest({"name": resource_name})

        try:
            response = self._client.get_quota_info(request=request)
        except NotFound:
            self._logger.error(
                "Quota information not found for resource: %s", resource_name
            )
            return None
        except GoogleAPICallError as e:
            self._logger.error(
                "API call failed while retrieving quota info: %s", str(e)
            )
            return None

        quota_values = [
            dimension_info.details.value
            for dimension_info in response.dimensions_infos
        ]
        quota_value = ceil(min(quota_values) / 60)
        return quota_value

    def list_all_quotas_for_service(self, project: str, service: str) -> list:
        """
        List all quotas for a specific service in the project.
        """
        parent = f"projects/{project}/locations/global/services/{service}"

        request = cloudquotas_v1.ListQuotaInfosRequest({"parent": parent})

        try:
            page_result = self._client.list_quota_infos(request=request)
        except NotFound:
            self._logger.error(
                "Service quotas not found for project '%s' and service '%s'.",
                project,
                service,
            )
            return []
        except GoogleAPICallError as e:
            self._logger.error(
                "API call failed while listing quotas: %s", str(e)
            )
            return []

        quotas = []

        for quota_info in page_result:
            quota_data = {
                "quota_id": quota_info.quota_id,
                "display_name": quota_info.metric_display_name,
                "values": {},
            }

            for dimension_info in quota_info.dimensions_infos:
                if dimension_info.applicable_locations:
                    for region in dimension_info.applicable_locations:
                        quota_data["values"][
                            region
                        ] = dimension_info.details.value
                else:
                    quota_data["values"][
                        "common"
                    ] = dimension_info.details.value

            quotas.append(quota_data)

        self._logger.info(
            "Retrieved %d quotas for service '%s' in project '%s'.",
            len(quotas),
            service,
            project,
        )

        return quotas
