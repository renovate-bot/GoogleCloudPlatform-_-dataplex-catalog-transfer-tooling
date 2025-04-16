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
This module provides a SchemaProvider class for managing and retrieving schema
information for BigQuery tables. It includes predefined schemas for specific
tables and methods to access these schemas.

Classes:
- SchemaProvider: A class for managing and retrieving schema information
  for BigQuery tables.
"""

from enum import StrEnum
from typing import Dict, Any
from google.cloud import bigquery
from common.big_query.big_query_exceptions import BigQuerySchemaNotFoundError


class TableNames(StrEnum):
    TAG_TEMPLATES = "tag_templates_table"
    ENTRY_GROUPS = "entry_groups_table"
    PROJECTS = "projects"
    IAM_POLICIES = "iam_policies"
    TAG_TEMPLATES_RESOURCE_MAPPING = "tag_templates_resource_mapping"
    ENTRY_GROUPS_RESOURCE_MAPPING = "entry_groups_resource_mapping"


class SchemaProvider:
    """
    A provider class for managing and retrieving schema information
    for BigQuery tables.
    """

    def __init__(self):
        """
        Initializes the SchemaProvider with predefined schemas
        for specific tables.
        """
        self.tables = {
            TableNames.TAG_TEMPLATES: {
                "schema": [
                    bigquery.SchemaField(
                        name="resourceName",
                        field_type="STRING",
                        mode="REQUIRED",
                        description=(
                            "Format: projects/:project/locations/"
                            ":location/tagTemplates/:tagTemplateId"
                        ),
                    ),
                    bigquery.SchemaField(
                        name="dataplexResourceName",
                        field_type="STRING",
                        description=(
                            "Format: projects/:project/locations/"
                            "global/aspectTypes/:aspectTypeId"
                        ),
                    ),
                    bigquery.SchemaField(
                        name="projectId", field_type="STRING", mode="REQUIRED"
                    ),
                    bigquery.SchemaField(
                        name="location", field_type="STRING", mode="REQUIRED"
                    ),
                    bigquery.SchemaField(
                        name="tagTemplateId",
                        field_type="STRING",
                        mode="REQUIRED",
                    ),
                    bigquery.SchemaField(
                        name="managingSystem",
                        field_type="STRING",
                        mode="REQUIRED",
                        description="Either DATA_CATALOG or DATAPLEX",
                    ),
                    bigquery.SchemaField(
                        name="isPubliclyReadable", field_type="BOOL"
                    ),
                    bigquery.SchemaField(
                        name="createdAt", field_type="DATE", mode="REQUIRED"
                    ),
                ],
                "is_partitioned": True,
                "partition_column": "createdAt",
                "require_partition_filter": True,
            },
            TableNames.TAG_TEMPLATES_RESOURCE_MAPPING: {
                "schema": [
                    bigquery.SchemaField(
                        name="dataCatalogResourceName",
                        field_type="STRING",
                        mode="REQUIRED",
                        description=(
                            "Format: projects/:project/locations/"
                            ":location/tagTemplates/:tagTemplateId"
                        ),
                    ),
                    bigquery.SchemaField(
                        name="dataplexResourceName",
                        field_type="STRING",
                        mode="REQUIRED",
                        description=(
                            "Format: projects/:project/locations/"
                            "global/aspectTypes/:aspectTypeId"
                        ),
                    ),
                ],
            },
            TableNames.ENTRY_GROUPS: {
                "schema": [
                    bigquery.SchemaField(
                        name="resourceName",
                        field_type="STRING",
                        mode="REQUIRED",
                        description=(
                            "Format: projects/:project/locations/"
                            ":location/entryGroups/:entryGroupId"
                        ),
                    ),
                    bigquery.SchemaField(
                        name="dataplexResourceName",
                        field_type="STRING",
                        description=(
                            "Format: projects/:project/locations/"
                            ":location/entryGroups/:entryGroupId"
                        ),
                    ),
                    bigquery.SchemaField(
                        name="projectId", field_type="STRING", mode="REQUIRED"
                    ),
                    bigquery.SchemaField(
                        name="location", field_type="STRING", mode="REQUIRED"
                    ),
                    bigquery.SchemaField(
                        name="entryGroupId",
                        field_type="STRING",
                        mode="REQUIRED",
                    ),
                    bigquery.SchemaField(
                        name="managingSystem",
                        field_type="STRING",
                        mode="REQUIRED",
                        description="Either DATA_CATALOG or DATAPLEX",
                    ),
                    bigquery.SchemaField(
                        name="createdAt", field_type="DATE", mode="REQUIRED"
                    ),
                ],
                "is_partitioned": True,
                "partition_column": "createdAt",
                "require_partition_filter": True,
            },
            TableNames.ENTRY_GROUPS_RESOURCE_MAPPING: {
                "schema": [
                    bigquery.SchemaField(
                        name="dataCatalogResourceName",
                        field_type="STRING",
                        mode="REQUIRED",
                        description=(
                            "Format: projects/:project/locations/"
                            ":location/entryGroups/:entryGroupId"
                        ),
                    ),
                    bigquery.SchemaField(
                        name="dataplexResourceName",
                        field_type="STRING",
                        mode="REQUIRED",
                        description=(
                            "Format: projects/:project/locations/"
                            ":location/entryGroups/:entryGroupId"
                        ),
                    ),
                ],
            },
            TableNames.PROJECTS: {
                "schema": [
                    bigquery.SchemaField(
                        name="projectId",
                        field_type="STRING",
                        mode="REQUIRED",
                    ),
                    bigquery.SchemaField(
                        name="projectNumber",
                        field_type="INTEGER",
                        mode="REQUIRED",
                    ),
                    bigquery.SchemaField(
                        name="isDataCatalogApiEnabled",
                        field_type="BOOLEAN",
                        mode="REQUIRED",
                    ),
                    bigquery.SchemaField(
                        name="isDataplexApiEnabled",
                        field_type="BOOLEAN",
                        mode="REQUIRED",
                    ),
                    bigquery.SchemaField(
                        name="ancestry",
                        field_type="RECORD",
                        mode="REPEATED",
                        fields=[
                            bigquery.SchemaField(
                                name="type",
                                field_type="STRING",
                                mode="REQUIRED",
                            ),
                            bigquery.SchemaField(
                                name="id",
                                field_type="STRING",
                                mode="REQUIRED",
                            ),
                        ],
                    ),
                    bigquery.SchemaField(
                        name="createdAt", field_type="DATE", mode="REQUIRED"
                    ),
                ],
                "is_partitioned": True,
                "partition_column": "createdAt",
                "require_partition_filter": True,
            },
            TableNames.IAM_POLICIES: {
                "schema": [
                    bigquery.SchemaField(
                        name="resourceName",
                        field_type="STRING",
                        mode="REQUIRED",
                    ),
                    bigquery.SchemaField(
                        name="system",
                        field_type="STRING",
                        mode="REQUIRED",
                    ),
                    bigquery.SchemaField(
                        name="bindings",
                        field_type="RECORD",
                        mode="REPEATED",
                        fields=[
                            bigquery.SchemaField(
                                name="role",
                                field_type="STRING",
                                mode="REQUIRED",
                            ),
                            bigquery.SchemaField(
                                name="members",
                                field_type="STRING",
                                mode="REPEATED",
                            ),
                        ],
                    ),
                ]
            },
        }

    def get_table_metadata(self, table_name: str) -> Dict[str, Any]:
        """
        Retrieves the metadata for a specified table.
        """
        table_metadata = self.tables.get(table_name)

        if table_metadata:
            return table_metadata
        raise BigQuerySchemaNotFoundError(
            f"Schema not found for table {table_name}"
        )
