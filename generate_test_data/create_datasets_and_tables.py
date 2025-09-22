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
Google Cloud BigQuery Datasets and Tables Creator

This script creates datasets and tables as preparation for Dataplex
resources creation.
By default, it creates 4000 datasets with a total of 400,000 tables\
to later create lakes, zones, and assets.

Usage:
    python generate_test_data/create_datasets_and_tables.py
            -p <project_id> [-c <number_of_datasets>]

Arguments:
    --project (-p): Google Cloud project ID where the datasets and
      tables will be created
    --count (-c): Number of datasets to create (default: 4000, optional
      for testing purposes)

Example:
    python generate_test_data/create_datasets_and_tables.py -p my-gcp-project
"""

import os
import asyncio
import time
from concurrent.futures import ThreadPoolExecutor
import argparse
from google.cloud import bigquery

# Dataset region mapping
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

bigquery_dataset_semaphore = asyncio.Semaphore(100)
bigquery_table_semaphore = asyncio.Semaphore(200)

dataset_progress = 0
table_progress = 0
progress_lock = asyncio.Lock()


def get_region(dataset_id):
    """
    Returns the appropriate region for a dataset based on its ID.
    """
    for region, (start, end) in dataset_regions.items():
        if start <= dataset_id <= end:
            return region
    return "us-west2"


async def update_dataset_progress(total_count):
    """
    Updates and prints dataset creation progress.
    """
    global dataset_progress
    async with progress_lock:
        dataset_progress += 1
        if dataset_progress % 100 == 0 or dataset_progress == total_count:
            print(f"Datasets: {dataset_progress}/{total_count} completed")


async def update_table_progress(total_count):
    """
    Updates and prints table creation progress.
    """
    global table_progress
    async with progress_lock:
        table_progress += 1
        if table_progress % 1000 == 0 or table_progress == total_count:
            print(f"Tables: {table_progress}/{total_count} completed")


async def create_single_dataset(
    client, project_id, dataset_id, region, executor, total_count
):
    """
    Creates a single BigQuery dataset in the specified region.
    """
    async with bigquery_dataset_semaphore:
        dataset_ref = f"{project_id}.dataset_{dataset_id}"

        def _create_dataset():
            try:
                dataset = bigquery.Dataset(dataset_ref)
                dataset.location = region
                client.create_dataset(dataset, exists_ok=True)
                return True
            except Exception:
                return False

        try:
            success = await asyncio.get_running_loop().run_in_executor(
                executor, _create_dataset
            )
            if success:
                await update_dataset_progress(total_count)
            return success
        except Exception:
            return False


async def create_all_datasets(project_id, count):
    """
    Creates all datasets for the project using parallel processing.
    """
    global dataset_progress
    dataset_progress = 0

    print(f"Phase 1: Creating {count} datasets...")
    start_time = time.time()

    num_clients = 20
    clients = [bigquery.Client(project=project_id) for _ in range(num_clients)]
    executor = ThreadPoolExecutor(max_workers=50)

    tasks = []
    for dataset_id in range(1, count + 1):
        region = get_region(dataset_id)
        client = clients[dataset_id % num_clients]

        tasks.append(
            create_single_dataset(
                client, project_id, dataset_id, region, executor, count
            )
        )

    results = await asyncio.gather(*tasks, return_exceptions=True)
    successful_datasets = sum(1 for r in results if r is True)

    elapsed = time.time() - start_time
    print(
        f"Phase 1 completed: {successful_datasets}/{count}"
        f"datasets in {elapsed/60:.1f} minutes"
    )
    print(f"   Rate: {successful_datasets/(elapsed/60):.0f} datasets/minute")

    return successful_datasets, clients, executor


async def create_single_table(
    client, project_id, dataset_id, table_id, executor, total_tables
):
    """
    Creates a single BigQuery table with basic schema.
    """
    async with bigquery_table_semaphore:
        table_ref = f"{project_id}.dataset_{dataset_id}.{table_id}"

        def _create_table():
            try:
                schema = [
                    bigquery.SchemaField("id", "STRING"),
                    bigquery.SchemaField("name", "STRING"),
                    bigquery.SchemaField("timestamp", "TIMESTAMP"),
                ]
                table = bigquery.Table(table_ref, schema=schema)
                client.create_table(table, exists_ok=True)
                return True
            except Exception:
                return False

        try:
            success = await asyncio.get_running_loop().run_in_executor(
                executor, _create_table
            )
            if success:
                await update_table_progress(total_tables)
            return success
        except Exception:
            return False


async def create_all_tables(
    project_id, dataset_count, table_count, clients, executor
):
    """
    Creates all tables for all datasets.
    """
    global table_progress
    table_progress = 0

    total_tables = dataset_count * table_count
    print(
        f"Phase 2: Creating {total_tables} tables"
        f"({table_count} per dataset)..."
    )
    start_time = time.time()

    tasks = []
    for dataset_id in range(1, dataset_count + 1):
        client = clients[dataset_id % len(clients)]

        for table_num in range(1, table_count + 1):
            table_id = f"table_{dataset_id}_{table_num}"
            tasks.append(
                create_single_table(
                    client,
                    project_id,
                    dataset_id,
                    table_id,
                    executor,
                    total_tables,
                )
            )

    print(f"Starting {len(tasks)} table creation tasks...")
    results = await asyncio.gather(*tasks, return_exceptions=True)
    successful_tables = sum(1 for r in results if r is True)

    elapsed = time.time() - start_time
    print(
        f"Phase 2 completed: {successful_tables}/{total_tables}"
        f"tables in {elapsed/60:.1f} minutes"
    )
    print(f"   Rate: {successful_tables/(elapsed/60):.0f} tables/minute")

    return successful_tables


async def main(project_id, count, table_count=100):
    """
    Main function that creates datasets first, then all tables.
    """
    total_start_time = time.time()
    print(f"Starting BigQuery creation for project: {project_id}")
    print(
        f"Target: {count} datasets with {table_count} tables "
        f"each = {count * table_count} total tables"
    )

    # Phase 1: Create all datasets
    successful_datasets, clients, executor = await create_all_datasets(
        project_id, count
    )

    if successful_datasets == 0:
        print("No datasets were created successfully. Stopping.")
        os._exit(1)

    print()

    # Phase 2: Create all tables
    successful_tables = await create_all_tables(
        project_id, successful_datasets, table_count, clients, executor
    )

    # Statistics
    total_elapsed = time.time() - total_start_time
    expected_tables = successful_datasets * table_count

    print("\n Creation completed! \n")
    print(f"Total time: {total_elapsed/60:.1f} minutes")
    print("Statistics:")
    print(
        f"   • Datasets: {successful_datasets}/{count}"
        f"({successful_datasets/count*100:.1f}%)"
    )
    print(
        f"   • Tables: {successful_tables}/{expected_tables}"
        f"({successful_tables/expected_tables*100:.1f}%)"
    )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Create BigQuery datasets and tables efficiently."
    )
    parser.add_argument(
        "-c",
        "--count",
        type=int,
        default=4000,
        help="Number of datasets to create.",
    )
    parser.add_argument(
        "-p",
        "--project",
        type=str,
        required=True,
        help="Google Cloud project ID where resources will be created.",
    )

    args = parser.parse_args()

    count_ = args.count
    project_id_ = args.project
    table_count_ = 100

    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)

    try:
        loop.run_until_complete(main(project_id_, count_, table_count_))
        print("Script completed successfully!")
    except KeyboardInterrupt:
        print("Operation interrupted")
    except Exception as e:
        print(f"Operation failed: {e}")
    finally:
        loop.close()
