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

from argparse import ArgumentParser

from common.utils import parse_common_args, percent


def parse_service_args(parser: ArgumentParser) -> None:
    """
    Adds service-specific arguments to the argument parser.
    """
    parser.add_argument(
        "-atp",
        "--aspect_type_project",
        type=str,
        required=True,
        help=(
            "Project id where process should create 3P aspect "
            "type to store reference to lake & zone."
        ),
    )
    parser.add_argument(
        "-l",
        "--service-location",
        default="us-central1",
        type=str,
        help=(
            "The location/region where the job is running "
            "(default: 'us-central1')."
        ),
    )
    parser.add_argument(
        "-q",
        "--queue",
        default="lakes-discovery",
        type=str,
        help=(
            "The queue for tasks that attach lake and zone metadata to entries "
            "(default: 'lakes-discovery')."
        ),
    )
    parser.add_argument(
        "-hn",
        "--handler-name",
        default="lakes-handler",
        type=str,
        help=(
            "The name of the handler responsible for processing entries "
            "(default: 'lakes-handler')."
        ),
    )

    parser.add_argument(
        "-qc",
        "--quota-consumption",
        default=20,
        type=percent,
        help=("Percentage of dataplex quota to use (default: 20)."),
    )


def get_application_config() -> dict:
    """
    Combines common and service-specific arguments into a unified configuration.
    """
    parser = ArgumentParser(description="CLI for fetch projects job")

    parse_service_args(parser)
    parse_common_args(parser)

    args = parser.parse_args()

    if args.project == args.aspect_type_project:
        parser.error(
            "'aspect_type_project_id' must be different from "
            "'transfer_tooling_project_id'."
        )

    # Cap quota consumption at 90% to prevent API rate limit issues
    # if >90% consumption is specified.
    args.quota_consumption = min(args.quota_consumption, 90)

    return {
        "transfer_tooling_project_id": args.project,
        "aspect_type_project_id": args.aspect_type_project,
        "service_location": args.service_location,
        "queue": args.queue,
        "handler_name": args.handler_name,
        "quota_consumption": args.quota_consumption,
    }
