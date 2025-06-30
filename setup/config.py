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
"""
import argparse
import re


def scope(s: str) -> str:
    pattern = r"^(organizations|folders|projects)/(\d+)$"
    match = re.match(pattern, s)

    if match:
        return s
    else:
        raise argparse.ArgumentTypeError(
            "Invalid scope format'{}'".format(s)
        )


def get_config():
    """
    Parse input and get application config
    """
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "-p",
        "--project",
        type=str,
        required=True,
        help="The name of the project in which the service operates.",
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
        "-sa",
        "--service-account",
        type=str,
        required=True,
        help=(
            "The service account used to authenticate with the Cloud Platform."
        ),
    )
    parser.add_argument(
        "-s",
        "--scope",
        type=scope,
        required=True,
        help=(
            "Should be formatted as 'organizations/{orgNumber}', "
            "'folders/{folderNumber}' or 'projects/{projectNumber}', "
            "defining the scope of projects for the process."
        ),
    )

    return parser.parse_args()
