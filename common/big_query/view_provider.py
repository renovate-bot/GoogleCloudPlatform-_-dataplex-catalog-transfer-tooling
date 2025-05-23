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
    """
    Predefined view names used in the ViewProvider.
    """

    TAG_TEMPLATES_VIEW = "tag_templates"
    ENTRY_GROUPS_VIEW = "entry_groups"
    RESOURCE_INTERACTIONS = "resource_interactions"
    RESOURCE_INTERACTIONS_SUMMARY = "resource_interactions_summary"
    IAM_POLICIES_COMPARISON = "iam_policies_comparison"


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

    RESOURCE_INTERACTIONS = """
        CREATE MATERIALIZED VIEW `{project_id}.{dataset_id}.{view_name}`
        OPTIONS (
            enable_refresh = true, 
            refresh_interval_minutes = 60 * 12, 
            max_staleness = INTERVAL "24:0:0" HOUR TO SECOND
        ) AS (
        SELECT
            protopayload_auditlog.resourceName,
            CASE
            When ENDS_WITH(protopayload_auditlog.methodName, 'TagTemplate') Then 'TAG_TEMPLATE'
            When ENDS_WITH(protopayload_auditlog.methodName, 'TagTemplates') Then 'TAG_TEMPLATE'
            When ENDS_WITH(protopayload_auditlog.methodName, 'TagTemplateField') Then 'TAG_TEMPLATE'
            When ENDS_WITH(protopayload_auditlog.methodName, 'EntryGroup') Then 'ENTRY_GROUP'
            When ENDS_WITH(protopayload_auditlog.methodName, 'EntryGroups') Then 'ENTRY_GROUP'
            When ENDS_WITH(protopayload_auditlog.methodName, 'ListEntries') Then 'ENTRY_GROUP'
            When ENDS_WITH(protopayload_auditlog.methodName, 'Tag') Then 'TAG'
            When ENDS_WITH(protopayload_auditlog.methodName, 'Tags') Then 'TAG'
            When ENDS_WITH(protopayload_auditlog.methodName, 'Entry') Then 'ENTRY'
            ELSE ''
            END AS resourceType,
            protopayload_auditlog.methodName,
            CASE
            WHEN protopayload_auditlog.authenticationInfo.principalSubject is null Then CONCAT('user:', protopayload_auditlog.authenticationInfo.principalEmail)
            ELSE protopayload_auditlog.authenticationInfo.principalSubject
            END as principal,
            authorizationInfo.permissionType,
        FROM
            `{project_id}.{dataset_id}.{cloudaudit_googleapis_data_access}`
        INNER JOIN UNNEST(protopayload_auditlog.authorizationInfo) as authorizationInfo
        );       
        """

    RESOURCE_INTERACTIONS_SUMMARY = """
        CREATE VIEW `{project_id}.{dataset_id}.{view_name}` (
        `principal` OPTIONS(description="IAM principal"),
        `totalCalls` OPTIONS(description="Total amount of API calls"),
        `adminReadCalls` OPTIONS(description="Total amount of ADMIN_READ calls"),
        `dataReadCalls` OPTIONS(description="Total amount of DATA_READ calls"),
        `dataWriteCalls` OPTIONS(description="Total amount of DATA_WRITE calls"),
        `tagTemplateCalls` OPTIONS(description="Total amount of calls related to tag templates"),
        `entryGroupCalls` OPTIONS(description="Total amount of calls related to entry groups"),
        `entryCalls` OPTIONS(description="Total amount of calls related to entries")
        ) AS (
        SELECT 
            principal,
            COUNT(principal) as total_calls,
            COUNT(case permissionType when 'ADMIN_READ' then 1 else null end) as adminReadCalls,
            COUNT(case permissionType when 'DATA_READ' then 1 else null end) as dataReadCalls,
            COUNT(case permissionType when 'DATA_WRITE' then 1 else null end) as dataWriteCalls,
            COUNT(case resourceType when 'TAG_TEMPLATE' then 1 else null end) as tagTemplateCalls,
            COUNT(case resourceType when 'ENTRY_GROUP' then 1 else null end) as entryGroupCalls,
            COUNT(case resourceType when 'ENTRY' then 1 else null end) as entryCalls
        FROM
            `{project_id}.{dataset_id}.{resource_interactions_view}`
        GROUP BY principal
        );
        """

    IAM_POLICIES_COMPARISON = """
        CREATE MATERIALIZED VIEW `{project_id}.{dataset_id}.{view_name}`
        OPTIONS (
            enable_refresh = true, 
            refresh_interval_minutes = 60 * 12, 
            max_staleness = INTERVAL "24:0:0" HOUR TO SECOND,
            allow_non_incremental_definition = true
        ) AS (
        SELECT 
            res.resourceType,
            res.resourceName,
            res.dataplexResourceName,
            dc_iam.bindings as dataCatalogBindings,
            dp_iam.bindings as dataplexBindings
        FROM (
            SELECT 
            "ENTRY_GROUP" as resourceType,
            eg.resourceName, 
            eg.dataplexResourceName
            FROM `{project_id}.{dataset_id}.{entry_groups_view}` eg
            WHERE eg.createdAt > "1990-01-01" AND eg.createdAt = (SELECT MAX(createdAt) FROM `{project_id}.{dataset_id}.{entry_groups_view}` WHERE createdAt > "1990-01-01")
            UNION ALL

            SELECT 
            "TAG_TEMPLATE" as resourceType, 
            tt.resourceName,
            tt.dataplexResourceName
            FROM `{project_id}.{dataset_id}.{tag_templates_view}` tt
            WHERE tt.createdAt > "1990-01-01" AND tt.createdAt = (SELECT MAX(createdAt) FROM `{project_id}.{dataset_id}.{tag_templates_view}` WHERE createdAt > "1990-01-01")
        ) res
        LEFT JOIN `{project_id}.{dataset_id}.{iam_policies_table}` dc_iam 
            ON res.resourceName = dc_iam.resourceName 
            AND dc_iam.system = "DATA_CATALOG"
        LEFT JOIN `{project_id}.{dataset_id}.{iam_policies_table}` dp_iam 
            ON res.dataplexResourceName = dp_iam.resourceName 
            AND dp_iam.system = "DATAPLEX"
        )
    """

    @classmethod
    def get_sql(cls, view_ref: bigquery.TableReference) -> str:
        """
        Generates the SQL statement for the specified view reference.
        """
        format_params = {
            "project_id": view_ref.project,
            "dataset_id": view_ref.dataset_id,
            "view_name": view_ref.table_id,
            "entity_table": "",
            "entity_mapping_table": "",
            "cloudaudit_googleapis_data_access": "",
            "resource_interactions_view": "",
            "iam_policies_table": "",
            "tag_templates_view": "",
            "entry_groups_view": "",
        }

        match view_ref.table_id:
            case ViewNames.ENTRY_GROUPS_VIEW:
                sql_template = cls.ENTRY_GROUPS_VIEW_SQL
                format_params.update(
                    {
                        "entity_table": TableNames.ENTRY_GROUPS,
                        "entity_mapping_table": (
                            TableNames.ENTRY_GROUPS_RESOURCE_MAPPING
                        ),
                    }
                )
            case ViewNames.TAG_TEMPLATES_VIEW:
                sql_template = cls.TAG_TEMPLATES_VIEW_SQL
                format_params.update(
                    {
                        "entity_table": TableNames.TAG_TEMPLATES,
                        "entity_mapping_table": (
                            TableNames.TAG_TEMPLATES_RESOURCE_MAPPING
                        ),
                    }
                )
            case ViewNames.RESOURCE_INTERACTIONS:
                sql_template = cls.RESOURCE_INTERACTIONS
                format_params.update(
                    {
                        "cloudaudit_googleapis_data_access": (
                            TableNames.CLOUDAUDIT_GOOGLEAPIS_DATA_ACCESS
                        ),
                    }
                )
            case ViewNames.RESOURCE_INTERACTIONS_SUMMARY:
                sql_template = cls.RESOURCE_INTERACTIONS_SUMMARY
                format_params.update(
                    {
                        "resource_interactions_view": (
                            ViewNames.RESOURCE_INTERACTIONS
                        ),
                    }
                )
            case ViewNames.IAM_POLICIES_COMPARISON:
                sql_template = cls.IAM_POLICIES_COMPARISON
                format_params.update(
                    {
                        "iam_policies_table": TableNames.IAM_POLICIES,
                        "tag_templates_view": ViewNames.TAG_TEMPLATES_VIEW,
                        "entry_groups_view": ViewNames.ENTRY_GROUPS_VIEW,
                    }
                )
            case _:
                raise BigQueryViewSQLNotFoundError(
                    f"Unknown view name: {view_ref.table_id}"
                )

        return sql_template.format(**format_params)
