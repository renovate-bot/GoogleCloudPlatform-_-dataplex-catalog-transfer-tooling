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
This module provides classes and functions for interacting with Google BigQuery
and transforming entities for data insertion. It includes:

Classes:
- RowTransformer: A utility class for converting TagTemplate and EntryGroup
  entities into a dictionary format suitable for BigQuery.
- BigQueryAdapter: An adapter class for managing datasets and tables in
  Google BigQuery, including creating tables, writing data,
  and retrieving partitions.
"""

import time
from datetime import date
from typing import Mapping, Any
from google.cloud import bigquery
from google.api_core.exceptions import NotFound
from common.utils import get_logger
from common.entities import TagTemplate, EntryGroup, Project, Entity
from common.big_query.schema_provider import SchemaProvider
from common.big_query.big_query_exceptions import (
    google_api_exception_shield,
    BigQueryDataRetrievalError,
)


logger = get_logger()


class RowTransformer:
    """
    A utility class for transforming entities into a dictionary
    format suitable for BigQuery insertion.
    """

    @classmethod
    def from_entity(
        cls, entity: Entity, creation_date: date
    ) -> Mapping[str, Any]:
        """
        Transforms an entity into a dictionary format based on its type.
        """
        match entity:
            case TagTemplate():
                return cls.from_tag_template(entity, creation_date)
            case EntryGroup():
                return cls.from_entry_group(entity, creation_date)
            case Project():
                return cls.from_project(entity, creation_date)

    @staticmethod
    def from_entry_group(
        entity: EntryGroup, creation_date: date
    ) -> Mapping[str, Any]:
        """
        Transforms an EntryGroup entity into a dictionary format.
        """
        return {
            "resourceName": entity.resource_name,
            "dataplexResourceName": entity.dataplex_resource_name,
            "projectId": entity.project_id,
            "location": entity.location,
            "entryGroupId": entity.id,
            "managingSystem": entity.managing_system,
            "createdAt": creation_date,
        }

    @staticmethod
    def from_tag_template(
        entity: TagTemplate, creation_date: date
    ) -> Mapping[str, Any]:
        """
        Transforms a TagTemplate entity into a dictionary format.
        """
        return {
            "resourceName": entity.resource_name,
            "dataplexResourceName": entity.dataplex_resource_name,
            "projectId": entity.project_id,
            "location": entity.location,
            "tagTemplateId": entity.id,
            "isPubliclyReadable": entity.public,
            "managingSystem": entity.managing_system,
            "createdAt": creation_date,
        }

    @staticmethod
    def from_project(
        project: Project, creation_date: date
    ) -> Mapping[str, Any]:
        """
        Transforms a Project entity into a dictionary format.
        """
        return {
            "projectId": project.project_id,
            "projectNumber": project.project_number,
            "isDataCatalogApiEnabled": project.data_catalog_api_enabled,
            "isDataplexApiEnabled": project.dataplex_api_enabled,
            "ancestry": project.ancestry,
            "createdAt": creation_date,
        }


class BigQueryAdapter:
    """
    An adapter class for interacting with Google BigQuery,
    providing methods to manage datasets and tables.
    """

    def __init__(
        self,
        app_config: dict,
        partition_column="createdAt",
        retry_count=5,
        retry_delay=2,
    ):
        self._project = app_config["project_name"]
        self._client = bigquery.Client(self._project)
        self._dataset_location = app_config["dataset_location"]
        self._dataset_name = app_config["dataset_name"]
        self._partition_column = partition_column
        self.retry_count = retry_count
        self.retry_delay = retry_delay

    def _get_target_creation_date(self, table_ref: str) -> date:
        """
        Retrieves the most recent partition date from the specified table.
        """
        last_date_response = self._client.query(
            f"""
            SELECT max({self._partition_column}) as max_date
            FROM `{table_ref}`
            WHERE {self._partition_column} <= CURRENT_DATE()
            """
        ).result()

        target_creation_date = next(last_date_response).max_date
        return target_creation_date

    def _execute_query(
        self, table_ref: str, target_creation_date: date, fields: str = "*"
    ) -> bigquery.QueryJob:
        """
        Executes a query to retrieve data from a specific partition of a table.
        """
        return self._client.query(
            f"""
                SELECT {fields}
                FROM `{table_ref}`
                WHERE {self._partition_column} = \"{target_creation_date}\"
                """
        )

    @google_api_exception_shield
    def get_projects_to_fetch(self) -> list[str]:
        """
        Retrieves a list of project IDs to fetch from the projects table.
        """
        table_ref = f"{self._project}.{self._dataset_name}.projects"
        target_creation_date = self._get_target_creation_date(table_ref)
        if not target_creation_date:
            error_message = (
                "Unable to identify the most recent data "
                f"partition in table: {table_ref}."
            )
            logger.error(
                "Failed to identify recent data partition for table %s. %s",
                table_ref,
                error_message,
            )
            raise BigQueryDataRetrievalError(error_message)

        project_ids_query_job = self._execute_query(
            table_ref, target_creation_date, "projectId"
        )

        project_ids = [
            project_id.projectId
            for project_id in project_ids_query_job.result()
        ]
        return project_ids

    def _ensure_dataset_exists(self) -> bigquery.Dataset:
        """
        Retrieves the dataset, creating it if it does not exist.
        """
        try:
            dataset_ref = bigquery.DatasetReference(
                self._project, self._dataset_name
            )
            return self._client.get_dataset(dataset_ref)
        except NotFound:
            dataset = bigquery.Dataset(dataset_ref)
            dataset.location = self._dataset_location
            return self._client.create_dataset(dataset)

    def _ensure_table_exist(
        self, table_ref: bigquery.TableReference
    ) -> bigquery.Table:
        """
        Creates a table in the dataset with the specified name,
        using schema information from a SchemaProvider.
        """
        self._ensure_dataset_exists()

        try:
            return self._client.get_table(table_ref)
        except NotFound:
            schema_provider = SchemaProvider()
            table_metadata = schema_provider.get_table_metadata(
                table_ref.table_id
            )
            table = bigquery.Table(table_ref, table_metadata["schema"])
            if table_metadata["is_partitioned"]:
                table.time_partitioning = bigquery.TimePartitioning(
                    field=table_metadata["partition_column"],
                    require_partition_filter=table_metadata[
                        "require_partition_filter"
                    ],
                )
            return self._client.create_table(table)

    @google_api_exception_shield
    def write_to_table(
        self,
        table_id: str,
        rows: list[Entity],
        creation_date: date = None,
    ) -> None:
        """
        Writes a list of entities to the specified table.
        """
        table_reference = bigquery.TableReference.from_string(table_id)

        table = self._ensure_table_exist(table_reference)
        creation_date = date.today() if creation_date is None else creation_date

        retries = self.retry_count
        delay = self.retry_delay

        while retries > 0:
            try:
                errors = self._client.insert_rows(
                    table,
                    [
                        RowTransformer.from_entity(row, creation_date)
                        for row in rows
                    ],
                )
                if errors:
                    logger.info(
                        "Errors occurred while inserting data: %s", errors
                    )
                else:
                    logger.info(
                        "Data inserted successfully into %s.", table_reference
                    )
                return
            except NotFound:
                logger.info(
                    "Table not found, retrying... (%s retries left)", retries
                )
                time.sleep(delay)
                delay *= 2
                retries -= 1
        raise BigQueryDataRetrievalError(
            "Data insertion failed: table not found."
        )

    @google_api_exception_shield
    def get_last_partition(self, table_ref: str) -> list[dict]:
        """
        Retrieves the data from the most recent partition of
        the specified table.
        """
        last_partition_date = self._get_target_creation_date(table_ref)
        last_partition_query_job = self._execute_query(
            table_ref, last_partition_date
        )

        last_partition = [
            dict(row.items()) for row in last_partition_query_job.result()
        ]
        return last_partition
