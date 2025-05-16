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
inputs. Utilizes `common.utils` for argument parsing.
"""

import re
from typing import Any
from argparse import ArgumentParser, Action as argparseAction

from common.utils import parse_common_args


class ValidateScope(argparseAction):
    """
    Validates scope format and extracts type and ID.
    """

    def __call__(
        self,
        parser: ArgumentParser,
        namespace: Any,
        values: str,
        option_string: str = None,
    ) -> None:
        """
        Validates the scope format and sets the parsed scope in the namespace.
        """
        pattern = r"^(organizations|folders|projects)/(\d+)$"
        match = re.match(pattern, values)

        if match:
            scope_type = match.group(1).rstrip("s")
            scope_id = int(match.group(2))
        else:
            parser.error(
                "Invalid scope format. Expected 'organizations/{orgID}', "
                "'folders/{folderID}', or 'projects/{projectId}'."
            )

        setattr(
            namespace,
            self.dest,
            {"scope_type": scope_type.upper(), "scope_id": scope_id},
        )


def parse_service_args(parser: ArgumentParser) -> None:
    """
    Adds service-specific arguments to the argument parser.
    """
    parser.add_argument(
        "-s",
        "--scope",
        type=str,
        required=True,
        action=ValidateScope,
        help=(
            "Should be formatted as organizations/{orgId}, folders/{folderId},"
            "or 'projects/{projectId}', defining the scope for the process."
        ),
    )
    parser.add_argument(
        "-l",
        "--service-location",
        type=str,
        default="us-central1",
        help=(
            "The location/region where the job is running. "
            "(default: 'us-central1')."
        ),
    )
    parser.add_argument(
        "-q",
        "--queue",
        default="resource-visibility",
        type=str,
        help=(
            "The name of the queue to use for converting private tag templates"
            "(default: 'resource-visibility')."
        ),
    )
    parser.add_argument(
        "-hn",
        "--handler-name",
        default="convert-private-tag-templates-handler",
        type=str,
        help=(
            "The name of the handler responsible for converting private "
            "tag templates (default: 'convert-private-tag-templates-handler')."
        ),
    )


def get_application_config() -> dict:
    """
    Combines common and service-specific arguments into a unified configuration.
    """
    parser = ArgumentParser(
        description="CLI for converting private tag templates"
    )

    parse_service_args(parser)
    parse_common_args(parser)

    args = parser.parse_args()

    if args.dataset_name is None:
        args.dataset_name = (
            "transfer_tooling_dry_run" if args.dry_run else "transfer_tooling"
        )

    return {
        "scope": args.scope,
        "project_name": args.project,
        "dataset_name": args.dataset_name,
        "service_location": args.service_location,
        "queue": args.queue,
        "handler_name": args.handler_name,
        "dataset_location": args.dataset_location,
    }
