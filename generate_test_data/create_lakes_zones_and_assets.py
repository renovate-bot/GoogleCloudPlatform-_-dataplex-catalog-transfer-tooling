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
This script creates lakes, zones and assets:
- Lakes: 15 lakes in each of 42 regions
- Zones: 75 zones in the first lake of each region + 10 zones each in 8 regions
- Assets: 500 assets per region (only in 8 regions), total 4000 assets
  with total 400,000 tables

Usage: python generate_test_data/generate_lakes_zones_and_assets.py
        -p <your project>
"""

import argparse
import asyncio
import time
from google.cloud import dataplex_v1
from google.api_core.retry_async import AsyncRetry
from google.api_core.exceptions import GoogleAPICallError

# All regions
regions = [
    "europe-west1",
    "us-east1",
    "us-east4",
    "us-west1",
    "us-west2",
    "us-west4",
    "africa-south1",
    "asia-east1",
    "asia-east2",
    "asia-northeast1",
    "asia-northeast2",
    "asia-northeast3",
    "asia-south1",
    "asia-south2",
    "asia-southeast1",
    "asia-southeast2",
    "australia-southeast1",
    "australia-southeast2",
    "europe-central2",
    "europe-north1",
    "europe-north2",
    "europe-southwest1",
    "europe-west10",
    "europe-west12",
    "europe-west2",
    "europe-west3",
    "europe-west4",
    "europe-west6",
    "europe-west8",
    "europe-west9",
    "me-central1",
    "me-central2",
    "me-west1",
    "northamerica-northeast1",
    "northamerica-northeast2",
    "northamerica-south1",
    "southamerica-east1",
    "southamerica-west1",
    "us-central1",
    "us-east5",
    "us-south1",
    "us-west3",
]

dataset_regions = {
    "us-central1": (1, 500),
    "us-west3": (501, 1000),
    "us-west4": (1001, 1500),
    "us-east4": (1501, 2000),
    "europe-west1": (2001, 2500),
    "us-west1": (2501, 3000),
    "us-east1": (3001, 3500),
    "us-west2": (3501, 4000),
}

custom_timeout = 600

lake_semaphore = asyncio.Semaphore(90)
zone_semaphore = asyncio.Semaphore(30)
asset_semaphore = asyncio.Semaphore(50)


async def create_lake(project_id, region, lake_id, is_retry=False):
    """
    Creates a single Dataplex lake with retry logic on failure.
    """
    async with lake_semaphore:
        try:
            parent = f"projects/{project_id}/locations/{region}"
            lake = dataplex_v1.Lake(
                display_name=lake_id, description=f"Lake {lake_id}"
            )

            operation = await dataplex_client.create_lake(
                parent=parent,
                lake_id=lake_id,
                lake=lake,
                retry=AsyncRetry(),
                timeout=custom_timeout,
            )

            await asyncio.sleep(300)
            await operation.result()
            return f"{parent}/lakes/{lake_id}"

        except GoogleAPICallError as e:
            if e.code == 409 or "already exists" in str(e).lower():
                return f"{parent}/lakes/{lake_id}"
            if not is_retry:
                return await create_lake(
                    project_id, region, lake_id, is_retry=True
                )
            return f"{parent}/lakes/{lake_id}"
        except TimeoutError:
            if not is_retry:
                return await create_lake(
                    project_id, region, lake_id, is_retry=True
                )
            return f"{parent}/lakes/{lake_id}"


async def create_zone(lake_id, zone_id, is_retry=False):
    """
    Creates a single Dataplex zone with retry logic on failure.
    """
    async with zone_semaphore:
        try:
            zone = dataplex_v1.Zone(
                display_name=zone_id,
                description=f"Zone {zone_id}",
                type_=dataplex_v1.Zone.Type.RAW,
                resource_spec=dataplex_v1.Zone.ResourceSpec(
                    location_type=(
                        dataplex_v1.Zone.ResourceSpec.LocationType.SINGLE_REGION
                    )
                ),
            )

            operation = await dataplex_client.create_zone(
                parent=lake_id,
                zone_id=zone_id,
                zone=zone,
                retry=AsyncRetry(),
                timeout=custom_timeout,
            )

            await asyncio.sleep(30)
            await operation.result()
            return f"{lake_id}/zones/{zone_id}"

        except GoogleAPICallError as e:
            if e.code == 409 or "already exists" in str(e).lower():
                return f"{lake_id}/zones/{zone_id}"
            if not is_retry:
                return await create_zone(lake_id, zone_id, is_retry=True)
            return None
        except TimeoutError:
            if not is_retry:
                return await create_zone(lake_id, zone_id, is_retry=True)
            return f"{lake_id}/zones/{zone_id}"


async def create_asset(zone_name, asset_id, dataset_id, is_retry=False):
    """
    Creates a single Dataplex asset linked to BigQuery dataset.
    """
    async with asset_semaphore:

        try:
            bigquery_table = (
                f"projects/{PROJECT_ID}/datasets/dataset_{dataset_id}"
            )
            asset = dataplex_v1.Asset(
                display_name=asset_id,
                description=f"Asset {asset_id}",
                resource_spec=dataplex_v1.Asset.ResourceSpec(
                    type_=dataplex_v1.Asset.ResourceSpec.Type.BIGQUERY_DATASET,
                    name=bigquery_table,
                ),
                discovery_spec=dataplex_v1.Asset.DiscoverySpec(
                    enabled=True,
                ),
            )

            operation = await dataplex_client.create_asset(
                parent=zone_name,
                asset_id=asset_id,
                asset=asset,
                retry=AsyncRetry(),
                timeout=custom_timeout,
            )
            await asyncio.sleep(15)
            await operation.result()
            return f"{zone_name}/assets/{asset_id}"

        except GoogleAPICallError as e:
            if e.code == 409 or "already exists" in str(e).lower():
                return f"{zone_name}/assets/{asset_id}"
            if not is_retry:
                return await create_asset(
                    zone_name, asset_id, dataset_id, is_retry=True
                )
            return f"{zone_name}/assets/{asset_id}"
        except TimeoutError:
            if not is_retry:
                return await create_asset(
                    zone_name, asset_id, dataset_id, is_retry=True
                )
            return f"{zone_name}/assets/{asset_id}"


async def create_all_lakes_for_region(region):
    """
    Creates all 15 lakes for a specific region in parallel.
    """
    tasks = []
    for lake_num in range(1, 16):
        lake_id = f"lake-{lake_num}-{region}"
        tasks.append(create_lake(PROJECT_ID, region, lake_id))

    results = await asyncio.gather(*tasks, return_exceptions=True)

    successful_lakes = []
    for i, result in enumerate(results):
        if result and not isinstance(result, Exception):
            lake_num = i + 1
            successful_lakes.append((lake_num, result))

    print(f"{region}: {len(successful_lakes)}/15 lakes")
    return successful_lakes


async def create_zones_for_region(region):
    """
    Creates zones for a region.
    """
    tasks = []
    zone_info = []
    parent = f"projects/{PROJECT_ID}/locations/{region}"

    for lake_num in range(1, 12):
        lake_id = f"{parent}/lakes/lake-{lake_num}-{region}"
        if lake_num == 1:
            for zone_num in range(1, 76):
                zone_id = f"l-{lake_num}-{region}-z-{zone_num}"
                tasks.append(create_zone(lake_id, zone_id))
                zone_info.append((lake_num, zone_id, "standard"))

        elif region in dataset_regions and 2 <= lake_num <= 11:
            zone_id = f"l-{lake_num}-{region}-zone-with-assets"
            tasks.append(create_zone(lake_id, zone_id))
            zone_info.append((lake_num, zone_id, "dataset"))

    if not tasks:
        return []

    results = await asyncio.gather(*tasks, return_exceptions=True)

    successful_zones = []
    for i, result in enumerate(results):
        if result and not isinstance(result, Exception):
            lake_num, zone_id, zone_type = zone_info[i]
            successful_zones.append((lake_num, zone_id, zone_type, result))

    print(f"{region}: {len(successful_zones)}/{len(tasks)} zones")
    return successful_zones


async def create_assets_for_region(region):
    """
    Creates assets for a region.
    """
    start_id, _ = dataset_regions[region]
    tasks = []

    parent_project = f"projects/{PROJECT_ID}/locations/{region}"

    for lake_num in range(2, 12):
        lake_id = f"lake-{lake_num}-{region}"
        zone_id = f"l-{lake_num}-{region}-zone-with-assets"
        zone_name = f"{parent_project}/lakes/{lake_id}/zones/{zone_id}"

        dataset_lake_index = lake_num - 2
        dataset_start = start_id + (dataset_lake_index * 50)

        for i in range(50):
            dataset_id = dataset_start + i
            asset_id = f"asset-{dataset_id}"
            tasks.append(create_asset(zone_name, asset_id, dataset_id))

    if not tasks:
        return []

    results = await asyncio.gather(*tasks, return_exceptions=True)

    successful_assets = []
    for i, result in enumerate(results):
        if result and not isinstance(result, Exception):
            successful_assets.append(result)

    print(f"{region}: {len(successful_assets)}/{len(tasks)} assets")
    return successful_assets


async def main():
    """
    Orchestrates creation of lakes, zones, and assets across all regions.
    """
    start_time = time.time()
    print(f"Starting creation across {len(regions)} regions")
    print(
        f"Semaphores: Lakes={lake_semaphore._value}, "
        f"Zones={zone_semaphore._value}, Assets={asset_semaphore._value}"
    )

    # PHASE 1: Lakes
    print("\n=== PHASE 1: Lakes ===")
    phase1_tasks = [create_all_lakes_for_region(region) for region in regions]
    phase1_results = await asyncio.gather(*phase1_tasks, return_exceptions=True)

    all_lakes_info = {}
    for i, result in enumerate(phase1_results):
        if result and not isinstance(result, Exception):
            all_lakes_info[regions[i]] = result

    print(f"Phase 1: {len(all_lakes_info)}/{len(regions)} regions")

    # PHASE 2: Zones
    print("\n=== PHASE 2: Zones ===")
    phase2_tasks = [create_zones_for_region(region) for region in regions]

    phase2_results = await asyncio.gather(*phase2_tasks, return_exceptions=True)

    all_zones_info_by_region = {}
    for i, result in enumerate(phase2_results):
        region = regions[i]
        if result and not isinstance(result, Exception):
            all_zones_info_by_region[region] = result
            print(
                f"Region '{region}': {len(result)} "
                "zones are ready for asset creation."
            )
        else:
            print(
                f"Region '{region}': No zones were created or an "
                "error occurred. It will be skipped in Phase 3."
            )

    # PHASE 3: Assets
    print("\n=== PHASE 3: Assets ===")

    phase3_tasks = [
        create_assets_for_region(region) for region in dataset_regions
    ]
    phase3_results = await asyncio.gather(*phase3_tasks, return_exceptions=True)

    all_assets_info = {}
    for i, result in enumerate(phase3_results):
        region = list(dataset_regions.keys())[i]
        if result and not isinstance(result, Exception):
            all_assets_info[region] = result

    print(f"Phase 3: {len(all_assets_info)}/{len(dataset_regions)} regions")

    total_elapsed = time.time() - start_time
    print(f"Total time: {total_elapsed/60:.1f} minutes")
    print("\nCreation completed!")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Create lakes, zones and assets"
    )
    parser.add_argument(
        "-p", "--project-id", required=True, help="Google Cloud Project ID"
    )
    args = parser.parse_args()

    PROJECT_ID = args.project_id

    loop = asyncio.get_event_loop()
    dataplex_client = dataplex_v1.DataplexServiceAsyncClient()

    try:
        loop.run_until_complete(main())
    except KeyboardInterrupt:
        print("Interrupted")
    except Exception as e:
        print(f"Failed: {e}")
    finally:
        loop.close()
