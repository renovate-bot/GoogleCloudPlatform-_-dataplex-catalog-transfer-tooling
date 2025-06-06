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
This module defines custom exception classes for handling specific error
conditions related to data processing and schema management.
"""


class IncorrectTypeException(Exception):
    """
    Exception raised when an incorrect type is encountered during processing.
    """


class ValidationError(Exception):
    """
    Exception raised for errors in the validation of data.
    """


class FormatException(Exception):
    """
    Exception raised when a string is parsed incorrectly.
    """


class MissingTablesOrViewsError(Exception):
    """
    Exception for missing required tables or views.
    """
