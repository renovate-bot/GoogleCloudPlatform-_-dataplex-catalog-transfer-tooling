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
This module defines the TransferController for restoring Dataplex search.

Its primary purpose is to orchestrate the discovery of all relevant Dataplex
assets and entities across an organization. For each discovered item, it creates
a Cloud Task to trigger a separate process that will enrich the entry with
lake and zone information, thereby restoring parent-based search functionality.

Classes:
- TransferController: Orchestrates the discovery of assets and structured
  entities and the creation of processing tasks.
"""

from typing import Iterable, Generator
from math import ceil
from concurrent.futures import ThreadPoolExecutor

from google.api_core.exceptions import PermissionDenied, GoogleAPIError

from common.api import (
    CloudAssetApiAdapter,
    DataplexApiAdapter,
    QuotaInfoAdapter,
    Services,
    Quotas,
)
from common.entities import DataplexEntries
from common.cloud_task import CloudTaskPublisher
from common.api.resource_manager_api_adapter import ResourceManagerApiAdapter
from common.utils import get_logger, rate_limiter


class TransferController:
    """
    This controller discovers all relevant Dataplex assets and entities across
    an organization. It then dispatches a Cloud Task for each resource, which
    triggers a separate handler to attach the necessary 'lake-details' aspect,
    making the entries searchable by their parent lake and zone.
    """

    def __init__(self, app_config: dict) -> None:
        """
        Initializes the TransferController with application configuration.
        """
        self.project_name = app_config["transfer_tooling_project_id"]
        self.aspect_type_project = app_config["aspect_type_project_id"]
        self.location = app_config["service_location"]
        self.handler_name = app_config["handler_name"]
        self.queue = app_config["queue"]
        self.quota_consumption = app_config["quota_consumption"]
        self._resource_manager_client = ResourceManagerApiAdapter()
        self.organization_number = self._get_organization_number(
            self.aspect_type_project
        )
        self._cloud_asset_api_client = CloudAssetApiAdapter(
            self.organization_number
        )
        self._cloud_task_client = CloudTaskPublisher(
            self.project_name, self.location, self.queue
        )
        self._dataplex_adapter = DataplexApiAdapter()
        self._quota_client = QuotaInfoAdapter()
        self._logger = get_logger()

    def _get_organization_number(self, project: str) -> str:
        """
        Retrieves the organization number for the given project.
        """
        try:
            return self._resource_manager_client.get_organization_number(
                project
            )
        except PermissionDenied as e:
            raise PermissionDenied(
                f"Not enough permissions or {project} doesn't exists"
            ) from e

    def _get_allowed_requests(
        self, service: Services, quota: Quotas, per_min: bool = True
    ) -> int:
        """
        Calculates allowed requests based on quota and consumption percentage.
        """
        quota_value = self._quota_client.get_default_quota_value(
            self.aspect_type_project, service, quota, per_min=per_min
        )
        return ceil(quota_value * (self.quota_consumption / 100))

    def start_transfer(self) -> None:
        """
        Initiates the discovery and task creation process.
        """
        self.create_aspect_type()
        self.setup_queue()

        # Create shared rate limiters for different APIs
        cloud_storage_requests = self._get_allowed_requests(
            Services.DATAPLEX, Quotas.METADATA_LIST_REQUESTS
        )
        cloud_storage_limiter = rate_limiter(
            request_limit=cloud_storage_requests
        )

        asset_requests = self._get_allowed_requests(
            Services.CLOUD_ASSET, Quotas.ASSET_LIST
        )
        asset_limiter = rate_limiter(request_limit=asset_requests)

        dataplex_mgmt_requests = self._get_allowed_requests(
            Services.DATAPLEX, Quotas.MANAGEMENT_READS
        )
        dataplex_mgmt_limiter = rate_limiter(
            request_limit=dataplex_mgmt_requests
        )

        self._logger.info(
            f"Cloud storage entities limiter: {cloud_storage_requests} req/min"
        )
        self._logger.info(f"Asset limiter: {asset_requests} req/min")
        self._logger.info(
            f"Dataplex management limiter: {dataplex_mgmt_requests} req/min"
        )

        with ThreadPoolExecutor(max_workers=10) as executor:
            for entity_page in self.cloud_storage_entities(
                cloud_storage_limiter
            ):
                executor.submit(self.create_cloud_tasks, entity_page)

            for asset_page in self.big_query_assets(asset_limiter):
                bigquery_assets = []
                for asset in asset_page:
                    dataset_asset_path = self._dataplex_adapter.get_bq_asset(
                        asset["fqn"], dataplex_mgmt_limiter
                    )
                    if dataset_asset_path:
                        asset["data_path"] = dataset_asset_path
                        bigquery_assets.append(asset)
                    else:
                        self._logger.info(
                            f"Asset {asset["fqn"]} is not a BIGQUERY_DATASET."
                        )
                executor.submit(self.create_cloud_tasks, bigquery_assets)

    def create_aspect_type(self):
        """
        Creates a new aspect type for storing references to
        Dataplex Lakes and Zones.
        """
        metadata = {
            "name": "LakeDetails",
            "type": "record",
            "annotations": {
                "display_name": "Lake details",
                "description": (
                    "Stores reference to Dataplex Lakes and Zones "
                    "that managed the entry"
                ),
            },
            "record_fields": [
                {
                    "name": "lake",
                    "type": "string",
                    "index": 1,
                    "constraints": {"required": True},
                    "annotations": {
                        "display_name": "Dataplex Lake",
                        "description": "Name of Dataplex Lake",
                    },
                },
                {
                    "name": "zone",
                    "type": "string",
                    "index": 2,
                    "constraints": {"required": True},
                    "annotations": {
                        "display_name": "Dataplex Zone",
                        "description": "Name of Dataplex Zone",
                    },
                },
            ],
        }

        try:
            self._dataplex_adapter.create_aspect_type(
                self.aspect_type_project,
                "global",
                "lake-details",
                "Lake details",
                (
                    "Stores reference to Dataplex Lakes and Zones "
                    "that managed the entry."
                ),
                metadata,
            )
            self._logger.info('Aspect type "Lake details" created successfully')
        except GoogleAPIError as e:
            if "already exists" in str(e):
                self._logger.info('Aspect type "Lake details" already exists')
            else:
                raise e

    def cloud_storage_entities(self, limiter: Generator) -> Generator[list]:
        """
        Finds cloud storage structured entities.
        """
        parent = f"organizations/{self.organization_number}"
        zones = self._cloud_asset_api_client.fetch_zones(parent)

        for zone in zones:
            for entity_page in self._dataplex_adapter.list_entities(
                zone, limiter
            ):
                yield entity_page

    def big_query_assets(self, limiter: Generator) -> Iterable:
        """
        Find BigQuery assets.
        """
        parent = f"organizations/{self.organization_number}"

        for asset_page in self._cloud_asset_api_client.find_assets(
            parent, limiter
        ):
            yield asset_page

    def setup_queue(self) -> None:
        """
        Ensures the queue exists and has the correct max_rps configuration.
        """

        max_rps = self._get_allowed_requests(
            Services.DATAPLEX, Quotas.METADATA_WRITES, per_min=False
        )

        if not self._cloud_task_client.check_queue_exists():
            self._logger.info(
                "Queue does not exist. Creating queue with max_rps: %d",
                max_rps,
            )
            self._cloud_task_client.create_queue(max_rps=max_rps)
        else:
            self._logger.info(
                "Queue exists. Ensuring correct max_rps: %d", max_rps
            )
            self._cloud_task_client.ensure_correct_rps(max_rps=max_rps)

    def create_cloud_tasks(self, entities: Iterable) -> None:
        """
        Create cloud tasks for further processing.
        """
        for entity_dict in entities:
            payload = DataplexEntries(
                aspect_type_project=self.aspect_type_project, **entity_dict
            ).model_dump(mode="json")

            self._cloud_task_client.create_task(
                payload,
                self.handler_name,
                self.project_name,
                self.location,
            )
