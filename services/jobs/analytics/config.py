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


def get_application_config() -> dict:
    """
    Combines common and service-specific arguments into a unified configuration.
    """
    parser = ArgumentParser(description="CLI for creating analytics views")

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
    }
