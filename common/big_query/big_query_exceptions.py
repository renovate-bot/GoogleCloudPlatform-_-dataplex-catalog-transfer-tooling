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
This module defines custom exceptions for handling specific BigQuery errors and
provides a decorator to shield functions from common Google API exceptions by
translating them into more specific BigQuery exceptions.
"""

from functools import wraps
from google.api_core.exceptions import GoogleAPICallError
import concurrent.futures


class BigQuerySchemaNotFoundError(Exception):
    """
    Exception raised when the expected schema is not found in BigQuery.
    This can occur if the schema is missing or incorrectly defined.
    """

class BigQueryViewSQLNotFoundError(Exception):
    """
    Raised when the SQL definition for a BigQuery view is not found.
    """


class BigQueryExecutionError(Exception):
    """
    Exception raised for errors that occur during the execution of
    operations on BigQuery.
    """


class BigQueryTimeoutError(Exception):
    """
    Exception raised when a BigQuery operation exceeds the allotted time limit.
    """


class BigQueryDataRetrievalError(Exception):
    """
    Exception raised when there is an issue retrieving data from BigQuery.
    """


def google_api_exception_shield(target):
    """
    Decorator to shield a function from exceptions raised by the Google API.
    Converts specific Google API exceptions into more specific BigQuery
    exceptions.
    """

    @wraps(target)
    def inner(*args, **kwargs):
        try:
            return target(*args, **kwargs)
        except GoogleAPICallError as e:
            raise BigQueryExecutionError(f"Error: {e}") from e
        except concurrent.futures.TimeoutError as e:
            raise BigQueryTimeoutError(f"Error: {e}") from e

    return inner
