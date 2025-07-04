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
Module to manage application configuration settings based on command-line
inputs.Utilizes `common.utils` for argument parsing.
"""

from argparse import ArgumentParser

from common.utils import parse_common_args


def parse_service_args(parser: ArgumentParser) -> None:
    """
    Adds service-specific arguments to the provided ArgumentParser.
    """
    parser.add_argument(
        "-sn",
        "--log-sink-name",
        default="data-catalog-audit-logs",
        type=str,
        help=(
            "Name of the log sink for exporting logs to a BigQuery dataset. "
            "Default is 'data-catalog-audit-logs'."
        ),
    )


def get_application_config() -> dict:
    """
    Combines common and service-specific arguments into a unified configuration.
    """
    parser = ArgumentParser(description="CLI for audit logs service")

    parse_service_args(parser)
    parse_common_args(parser)

    args = parser.parse_args()

    if args.dataset_name is None:
        args.dataset_name = (
            "transfer_tooling_dry_run" if args.dry_run else "transfer_tooling"
        )

    return {
        "project_name": args.project,
        "dataset_name": args.dataset_name,
        "dataset_location": args.dataset_location,
        "log_sink_name": args.log_sink_name,
    }


def get_log_filter() -> str:
    """
    Returns the log filter string for filtering audit logs.

    The filter is designed to capture specific Data Catalog API calls while
    excluding certain methods and versions.
    """
    log_filter = """
    protoPayload.serviceName="datacatalog.googleapis.com"
    AND protoPayload.methodName=("google.cloud.datacatalog.v1.DataCatalog.ListEntryGroups" OR
        "google.cloud.datacatalog.v1.DataCatalog.GetEntryGroup" OR
        "google.cloud.datacatalog.v1.DataCatalog.CreateEntryGroup" OR
        "google.cloud.datacatalog.v1.DataCatalog.UpdateEntryGroup" OR
        "google.cloud.datacatalog.v1.DataCatalog.DeleteEntryGroup" OR
        "google.cloud.datacatalog.v1.DataCatalog.ListEntries" OR
        "google.cloud.datacatalog.v1.DataCatalog.GetEntry" OR
        "google.cloud.datacatalog.v1.DataCatalog.LookupEntry" OR
        "google.cloud.datacatalog.v1.DataCatalog.CreateEntry" OR
        "google.cloud.datacatalog.v1.DataCatalog.UpdateEntry" OR
        "google.cloud.datacatalog.v1.DataCatalog.DeleteEntry" OR
        "google.cloud.datacatalog.v1.DataCatalog.GetTagTemplate" OR
        "google.cloud.datacatalog.v1.DataCatalog.CreateTagTemplate" OR
        "google.cloud.datacatalog.v1.DataCatalog.UpdateTagTemplate" OR
        "google.cloud.datacatalog.v1.DataCatalog.DeleteTagTemplate" OR
        "google.cloud.datacatalog.v1.DataCatalog.CreateTagTemplateField" OR
        "google.cloud.datacatalog.v1.DataCatalog.UpdateTagTemplateField" OR
        "google.cloud.datacatalog.v1.DataCatalog.RenameTagTemplateField" OR
        "google.cloud.datacatalog.v1.DataCatalog.DeleteTagTemplateField" OR
        "google.cloud.datacatalog.v1.DataCatalog.RenameTagTemplateFieldEnumValue" OR
        "google.cloud.datacatalog.v1.DataCatalog.ListTags" OR
        "google.cloud.datacatalog.v1.DataCatalog.CreateTag" OR
        "google.cloud.datacatalog.v1.DataCatalog.UpdateTag" OR
        "google.cloud.datacatalog.v1.DataCatalog.DeleteTag" OR
        "google.cloud.datacatalog.v1.DataCatalog.ReconcileTags" OR
        "google.cloud.datacatalog.v1.DataCatalog.GetIamPolicy" OR
        "google.cloud.datacatalog.v1.DataCatalog.SetIamPolicy" OR
        "google.cloud.datacatalog.v1.DataCatalog.ModifyEntryOverview" OR
        "google.cloud.datacatalog.v1.DataCatalog.ModifyEntryContacts" OR
        "google.cloud.datacatalog.v1.DataCatalog.SearchEntries")
    AND protoPayload.methodName!=("google.cloud.datacatalog.v1.DataCatalog.StarEntry" OR
        "google.cloud.datacatalog.v1.DataCatalog.UnstarEntry" OR
        "google.cloud.datacatalog.v1.DataCatalog.TestIamPermission" OR
        "google.cloud.datacatalog.v1.DataCatalog.ListRelationships" OR
        "google.cloud.datacatalog.v1.DataCatalog.TestUpdateTagPermission" OR
        "google.cloud.datacatalog.v1.DataCatalog.ExportMetadata" OR
        "google.cloud.datacatalog.v1.DataCatalog.ImportEntries" OR
        "google.cloud.datacatalog.v1.DataCatalog.SetConfig" OR
        "google.cloud.datacatalog.v1.DataCatalog.RetrieveConfig" OR
        "google.cloud.datacatalog.v1.DataCatalog.RetrieveEffectiveConfig"    
    )
    AND NOT protoPayload.methodName:"google.cloud.datacatalog.v2"
    """
    return log_filter
