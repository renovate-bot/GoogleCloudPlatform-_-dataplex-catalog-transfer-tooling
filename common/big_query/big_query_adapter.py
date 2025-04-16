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
from typing import Any
from google.cloud import bigquery
from google.api_core.exceptions import NotFound

from common.utils import get_logger
from common.entities import (
    TagTemplate,
    EntryGroup,
    Project,
    Entity,
    ManagingSystem,
)
from common.big_query.schema_provider import SchemaProvider, TableNames
from common.big_query.view_provider import ViewSQLStatements, ViewNames
from common.big_query.big_query_exceptions import (
    google_api_exception_shield,
    BigQueryDataRetrievalError,
)


class RowTransformer:
    """
    A utility class for transforming entities into a dictionary
    format suitable for BigQuery insertion.
    """

    @classmethod
    def from_entity(cls, entity: Entity, creation_date: date) -> dict[str, Any]:
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
    ) -> dict[str, Any]:
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
    ) -> dict[str, Any]:
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
    def from_project(project: Project, creation_date: date) -> dict[str, Any]:
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
        project: str,
        dataset_location: str,
        dataset_name: str,
        partition_column: str = "createdAt",
        retry_count: int = 5,
        retry_delay: int = 2,
    ):
        self._project = project
        self._client = bigquery.Client(self._project)
        self._dataset_location = dataset_location
        self._dataset_name = dataset_name
        self._partition_column = partition_column
        self.retry_count = retry_count
        self.retry_delay = retry_delay
        self._logger = get_logger()

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

        if not target_creation_date:
            error_message = (
                "Unable to identify the most recent data "
                f"partition in table: {table_ref}."
            )
            self._logger.error(
                "Failed to identify recent data partition for table %s. %s",
                table_ref,
                error_message,
            )
            raise BigQueryDataRetrievalError(error_message)

        return target_creation_date

    def _select_data_from_partition(
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

    def _select_all_data_from_table(
        self, table_ref: str, fields: str = "*"
    ) -> bigquery.QueryJob:
        """
        Executes a query to retrieve data from a specific table.
        """

        return self._client.query(
            f"""
                SELECT {fields}
                FROM `{table_ref}`
                """
        )

    def select_entry_groups(self) -> list[EntryGroup]:
        table_ref = self._get_table_ref(ViewNames.ENTRY_GROUPS_VIEW)
        target_creation_date = self._get_target_creation_date(table_ref)
        query = f"""SELECT
                        projectId, 
                        location, 
                        entryGroupId, 
                        managingSystem
                    FROM `{table_ref}`
                    WHERE {self._partition_column} = \"{target_creation_date}\"
                    AND dataplexResourceName IS NULL
                """

        query_result = self._client.query(query).result()

        entry_groups = [
            EntryGroup(
                eg.projectId,
                eg.location,
                eg.entryGroupId,
                True if eg.managingSystem == ManagingSystem.DATAPLEX else False,
            )
            for eg in query_result
        ]

        return entry_groups

    def select_tag_templates(self) -> list[TagTemplate]:
        # TODO create simple ORM
        table_ref = self._get_table_ref(ViewNames.TAG_TEMPLATES_VIEW)
        target_creation_date = self._get_target_creation_date(table_ref)
        query = f"""SELECT
                        projectId, 
                        location, 
                        tagTemplateId,
                        isPubliclyReadable,
                        managingSystem
                    FROM `{table_ref}`
                    WHERE {self._partition_column} = \"{target_creation_date}\"
                    AND dataplexResourceName IS NULL
                    AND isPubliclyReadable = TRUE
                """

        query_result = self._client.query(query).result()

        tag_templates = [
            TagTemplate(
                tt.projectId,
                tt.location,
                tt.tagTemplateId,
                tt.isPubliclyReadable,
                True if tt.managingSystem == ManagingSystem.DATAPLEX else False,
            )
            for tt in query_result
        ]

        return tag_templates

    def get_entry_groups_for_policies(self, scope, managing_systems: list):
        """
        Fetch entry groups matching scope criteria.
        """
        eg_table_ref = self._get_table_ref(ViewNames.ENTRY_GROUPS_VIEW)
        projects_table_ref = self._get_table_ref(TableNames.PROJECTS)

        target_creation_date_for_eg = self._get_target_creation_date(
            eg_table_ref
        )
        target_creation_date_for_projects = self._get_target_creation_date(
            projects_table_ref
        )
        query = f"""
            SELECT
                resourceName as dataCatalogResourceName,
                dataplexResourceName,
                managingSystem
            FROM 
                `{eg_table_ref}` AS entry_groups
            JOIN 
                `{projects_table_ref}` AS projects
            ON 
                entry_groups.projectId = projects.projectId,
            UNNEST(projects.ancestry) AS ancestryItem
            WHERE 
                entry_groups.createdAt = \"{target_creation_date_for_eg}\"
                AND projects.createdAt = \"{target_creation_date_for_projects}\"
                AND entry_groups.managingSystem IN 
                    ({",".join([f"\"{v}\"" for v in managing_systems])})
                AND ancestryItem.type = \"{scope["scope_type"]}\"
                AND ancestryItem.id = \"{scope["scope_id"]}\"
            """

        query_result = self._client.query(query).result()

        entry_groups = []

        for eg in query_result:

            match eg.managingSystem:
                case ManagingSystem.DATA_CATALOG:
                    resource_name = eg.dataCatalogResourceName
                case ManagingSystem.DATAPLEX:
                    resource_name = eg.dataplexResourceName

            if resource_name:
                parse_resource_name = EntryGroup.parse_entry_group_resource(
                    resource_name
                )
            else:
                self._logger.info("Could not find resource name for %s", eg)

            entry_groups.append(
                EntryGroup(
                    project_id=parse_resource_name["project_id"],
                    location=parse_resource_name["location"],
                    entry_group_id=parse_resource_name["entry_group_id"],
                    transferred=(
                        True
                        if eg.managingSystem == ManagingSystem.DATAPLEX
                        else False
                    ),
                )
            )
        return entry_groups, target_creation_date_for_eg

    def get_tag_templates_for_policies(self, scope, managing_systems):
        """
        Fetch tag templates matching scope criteria.
        """
        tt_table_ref = self._get_table_ref(ViewNames.TAG_TEMPLATES_VIEW)
        projects_table_ref = self._get_table_ref(TableNames.PROJECTS)

        target_creation_date_for_tt = self._get_target_creation_date(
            tt_table_ref
        )
        target_creation_date_for_projects = self._get_target_creation_date(
            projects_table_ref
        )
        query = f"""
            SELECT 
                resourceName as dataCatalogResourceName, 
                dataplexResourceName,
                isPubliclyReadable,
                managingSystem
            FROM 
                `{tt_table_ref}` AS tag_templates
            JOIN 
                `{projects_table_ref}` AS projects
            ON 
                tag_templates.projectId = projects.projectId,
            UNNEST(projects.ancestry) AS ancestryItem
            WHERE 
                tag_templates.createdAt = \"{target_creation_date_for_tt}\"
                AND projects.createdAt = \"{target_creation_date_for_projects}\"
                AND tag_templates.managingSystem IN 
                    ({",".join([f"\"{v}\"" for v in managing_systems])})
                AND ancestryItem.type = \"{scope["scope_type"]}\"
                AND ancestryItem.id = \"{scope["scope_id"]}\"
            """

        query_result = self._client.query(query).result()

        tag_templates = []

        for tt in query_result:

            match tt.managingSystem:
                case ManagingSystem.DATA_CATALOG:
                    resource_name = tt.dataCatalogResourceName
                case ManagingSystem.DATAPLEX:
                    resource_name = tt.dataplexResourceName

            if resource_name:
                parse_resource_name = TagTemplate.parse_tag_template_resource(
                    resource_name
                )
            else:
                self._logger.info("Could not find resource name for %s", tt)

            tag_templates.append(
                TagTemplate(
                    project_id=parse_resource_name["project_id"],
                    location=parse_resource_name["location"],
                    tag_template_id=parse_resource_name["tag_template_id"],
                    public=tt.isPubliclyReadable,
                    transferred=(
                        True
                        if tt.managingSystem == ManagingSystem.DATAPLEX
                        else False
                    ),
                )
            )

        return tag_templates, target_creation_date_for_tt

    @google_api_exception_shield
    def get_projects_to_fetch(self) -> list[str]:
        """
        Retrieves a list of project IDs to fetch from the projects table.
        """
        table_ref = self._get_table_ref(TableNames.PROJECTS)
        target_creation_date = self._get_target_creation_date(table_ref)

        project_ids_query_job = self._select_data_from_partition(
            table_ref, target_creation_date, "projectId"
        )

        project_ids = [
            project_id.projectId
            for project_id in project_ids_query_job.result()
        ]
        return project_ids

    def _get_dataset_ref(self) -> bigquery.DatasetReference:
        return bigquery.DatasetReference(self._project, self._dataset_name)

    def _get_table_ref(self, table_name: str):
        return f"{self._project}.{self._dataset_name}.{table_name}"

    def _ensure_dataset_exists(self) -> bigquery.Dataset:
        """
        Retrieves the dataset, creating it if it does not exist.
        """
        dataset_ref = self._get_dataset_ref()
        try:
            return self._client.get_dataset(dataset_ref)
        except NotFound:
            dataset = bigquery.Dataset(dataset_ref)
            dataset.location = self._dataset_location
            return self._client.create_dataset(dataset)

    @google_api_exception_shield
    def create_table_if_not_exists(
        self, table_ref: bigquery.TableReference | str
    ) -> bigquery.Table:
        """
        Creates a table in the dataset with the specified name,
        using schema information from a SchemaProvider.
        """
        if isinstance(table_ref, str):
            table_ref = bigquery.TableReference.from_string(table_ref)

        self._ensure_dataset_exists()

        try:
            return self._client.get_table(table_ref)
        except NotFound:
            schema_provider = SchemaProvider()
            table_metadata = schema_provider.get_table_metadata(
                table_ref.table_id
            )
            table = bigquery.Table(table_ref, table_metadata["schema"])
            if table_metadata.get("is_partitioned", False):
                table.time_partitioning = bigquery.TimePartitioning(
                    field=table_metadata["partition_column"]
                )
                table.require_partition_filter = table_metadata[
                    "require_partition_filter"
                ]
            return self._client.create_table(table)

    @google_api_exception_shield
    def create_view_if_not_exists(
        self, view_ref: bigquery.TableReference | str
    ) -> None:
        """
        Ensures that a view exists in the dataset. If the view does not exist,
        it creates the view using SQL statements from ViewSQLStatements.
        """
        if isinstance(view_ref, str):
            view_ref = bigquery.TableReference.from_string(view_ref)

        self._ensure_dataset_exists()

        try:
            return self._client.get_table(view_ref)
        except NotFound:
            sql = ViewSQLStatements.get_sql(view_ref)
            job = self._client.query(sql)
            job.result()

            self._logger.info(
                'Created new view "%s.%s.%s".',
                job.destination.project,
                job.destination.dataset_id,
                job.destination.table_id,
            )

    def write_entities_to_table(
        self,
        table_id: str,
        entities: list[Entity],
        creation_date: date = None,
    ) -> None:
        """
        Writes a list of entities to the specified table.
        """
        table_str_ref = self._get_table_ref(table_id)
        creation_date = date.today() if creation_date is None else creation_date
        rows = [
            RowTransformer.from_entity(entity, creation_date)
            for entity in entities
        ]
        self.write_to_table(table_str_ref, rows)

    @google_api_exception_shield
    def write_to_table(self, table_id: str, rows: list[dict[str, Any]]) -> None:
        """
        Writes a list of rows to the specified table.
        """
        table_reference = bigquery.TableReference.from_string(table_id)
        table = self.create_table_if_not_exists(table_reference)

        retries = self.retry_count
        delay = self.retry_delay

        while retries > 0:
            try:
                errors = self._client.insert_rows(table, rows)
                if errors:
                    self._logger.info(
                        "Errors occurred while inserting data: %s", errors
                    )
                else:
                    self._logger.info(
                        "Data inserted successfully into %s.", table_reference
                    )
                return
            except NotFound:
                self._logger.info(
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
        last_partition_query_job = self._select_data_from_partition(
            table_ref, last_partition_date
        )

        last_partition = [
            dict(row.items()) for row in last_partition_query_job.result()
        ]
        return last_partition

    def delete_dataset(self):
        dataset_ref = self._get_dataset_ref()
        self._client.delete_dataset(dataset_ref, True)
