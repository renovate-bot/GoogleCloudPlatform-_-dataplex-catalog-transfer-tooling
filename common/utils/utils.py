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
This module provides essential tools for command-line interface operations
and logging setup.
"""

import logging
from argparse import ArgumentParser, Namespace, ArgumentTypeError


def str2bool(v):
    """
    Converts a string representation of truth to a boolean value.
    """
    if isinstance(v, bool):
        return v
    if v.lower() in ("yes", "true", "t", "y", "1"):
        return True
    elif v.lower() in ("no", "false", "f", "n", "0"):
        return False
    else:
        raise ArgumentTypeError("Boolean value expected.")


def parse_common_args(parser: ArgumentParser) -> Namespace:
    """
    Parses command-line arguments for the data transfer CLI tool.
    """
    parser.add_argument(
        "-d",
        "--dry-run",
        default=False,
        type=str2bool,
        help=(
            "Dry run mode: If True, the dataset name defaults to "
            "'transfer_tooling_dry_run' unless explicitly specified. If False, "
            "the dataset name defaults to 'transfer_tooling'. (default: False)"
        ),
    )
    parser.add_argument(
        "-p",
        "--project",
        type=str,
        required=True,
        help="The name of the project in which the service operates.",
    )
    parser.add_argument(
        "-dn",
        "--dataset-name",
        type=str,
        help=(
            "The name of the dataset to use for storing or processing data. "
            "If not specified, defaults to 'transfer_tooling' or "
            "'transfer_tooling_dry_run' based on the dry-run flag."
        ),
    )
    parser.add_argument(
        "-dl",
        "--dataset-location",
        default="US",
        type=str,
        help=(
            "The location/region where the dataset should be created. "
            "This is only required if the dataset does not already exist. "
            "(default: 'US' if not specified)."
        ),
    )


def get_logger():
    """
    Configures and retrieves the root logger with a specified logging
    level and format.
    """
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(filename)s - %(levelname)s - %(message)s",
    )
    logger = logging.getLogger()
    return logger
