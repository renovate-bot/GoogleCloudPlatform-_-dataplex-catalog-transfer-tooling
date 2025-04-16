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
This module provides functionality for dynamically generating SQL statements
to create BigQuery views based on predefined templates. It supports views
such as `entry_groups_view` and `tag_templates_view`.
"""

from enum import StrEnum
from google.cloud import bigquery
from common.big_query.big_query_exceptions import BigQueryViewSQLNotFoundError
from common.big_query.big_query_adapter import TableNames


class ViewNames(StrEnum):
    TAG_TEMPLATES_VIEW = "tag_templates"
    ENTRY_GROUPS_VIEW = "entry_groups"


class ViewSQLStatements:
    """
    Class for managing SQL templates and generating SQL statements for
    BigQuery views.
    """
    ENTRY_GROUPS_VIEW_SQL = """
        CREATE VIEW `{project_id}.{dataset_id}.{view_name}` AS
        SELECT
            eg.resourceName,
            egrm.dataplexResourceName,
            eg.projectId,
            eg.location,
            eg.entryGroupId,
            eg.managingSystem,
            eg.createdAt
        FROM
            `{project_id}.{dataset_id}.{entity_table}` AS eg
        LEFT JOIN
            `{project_id}.{dataset_id}.{entity_mapping_table}` AS egrm
        ON
            eg.resourceName = egrm.dataCatalogResourceName
        """

    TAG_TEMPLATES_VIEW_SQL = """
        CREATE VIEW `{project_id}.{dataset_id}.{view_name}` AS
        SELECT
            tt.resourceName,
            ttrm.dataplexResourceName,
            tt.projectId,
            tt.location,
            tt.tagTemplateId,
            tt.managingSystem,
            tt.isPubliclyReadable,
            tt.createdAt
        FROM
            `{project_id}.{dataset_id}.{entity_table}` AS tt
        LEFT JOIN
            `{project_id}.{dataset_id}.{entity_mapping_table}` AS ttrm
        ON
            tt.resourceName = ttrm.dataCatalogResourceName
        """

    @classmethod
    def get_sql(cls, view_ref: bigquery.TableReference) -> str:
        """
        Generates the SQL statement for the specified view reference.
        """
        if view_ref.table_id == ViewNames.ENTRY_GROUPS_VIEW:
            sql_template =  cls.ENTRY_GROUPS_VIEW_SQL
            entity_table = TableNames.ENTRY_GROUPS
            entity_mapping_table = TableNames.ENTRY_GROUPS_RESOURCE_MAPPING
        elif view_ref.table_id == ViewNames.TAG_TEMPLATES_VIEW:
            sql_template = cls.TAG_TEMPLATES_VIEW_SQL
            entity_table = TableNames.TAG_TEMPLATES
            entity_mapping_table = TableNames.TAG_TEMPLATES_RESOURCE_MAPPING
        else:
            raise BigQueryViewSQLNotFoundError(
                f"Unknown view name: {view_ref.table_id}"
            )

        return sql_template.format(
            project_id=view_ref.project,
            dataset_id=view_ref.dataset_id,
            view_name=view_ref.table_id,
            entity_table=entity_table,
            entity_mapping_table=entity_mapping_table
        )
