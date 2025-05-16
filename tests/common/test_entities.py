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
Module for testing the parsing and conversion functionality of
resource names and proto messages.
"""

from typing import Any

import pytest

from common.exceptions import FormatException
from common.entities import TagTemplate, EntryGroup, Project


@pytest.mark.parametrize(
    "resource_name, expected",
    [
        (
            (
                "projects/my-project/locations/us-central1/"
                "tagTemplates/my-template"
            ),
            {
                "project_id": "my-project",
                "location": "us-central1",
                "tag_template_id": "my-template",
            },
        ),
        (
            "projects/my-project/locations/europe-west1/aspectTypes/my-aspect",
            {
                "project_id": "my-project",
                "location": "europe-west1",
                "tag_template_id": "my-aspect",
            },
        ),
    ],
)
def test_parse_tag_template_resource_valid(
    resource_name: str, expected: dict[str, str]
) -> None:
    """
    Test that valid tag template resource names are correctly parsed.
    """
    result = TagTemplate.parse_tag_template_resource(resource_name)
    assert result == expected


@pytest.mark.parametrize(
    "resource_name",
    [
        "projects/my-project/locations/us-central1/invalidType/my-template",
        "projects/my-project/locations//tagTemplates/my-template",
        "invalid-resource-name",
    ],
)
def test_parse_tag_template_resource_invalid(resource_name: str) -> None:
    """
    Test that invalid tag template resource names raise a FormatException.
    """
    with pytest.raises(FormatException):
        TagTemplate.parse_tag_template_resource(resource_name)


@pytest.mark.parametrize(
    "resource_name, expected",
    [
        (
            "projects/my-project/locations/us-central1/entryGroups/my-group",
            {
                "project_id": "my-project",
                "location": "us-central1",
                "entry_group_id": "my-group",
            },
        ),
        (
            (
                "projects/another-project/locations/europe-west1/"
                "entryGroups/another-group"
            ),
            {
                "project_id": "another-project",
                "location": "europe-west1",
                "entry_group_id": "another-group",
            },
        ),
    ],
)
def test_parse_entry_group_resource_valid(
    resource_name: str, expected: dict[str, str]
) -> None:
    """
    Test that valid entry group resource names are correctly parsed.
    """
    result = EntryGroup.parse_entry_group_resource(resource_name)
    assert result == expected


@pytest.mark.parametrize(
    "resource_name",
    [
        "projects/my-project/locations/us-central1/invalidType/my-group",
        "projects/my-project/locations//entryGroups/my-group",
        "invalid-resource-name",
    ],
)
def test_parse_entry_group_resource_invalid(resource_name: str) -> None:
    """
    Test that invalid entry group resource names raise a FormatException.
    """
    with pytest.raises(FormatException):
        EntryGroup.parse_entry_group_resource(resource_name)


@pytest.mark.parametrize(
    "parent_full_resource_name, project, display_name, expected",
    [
        (
            "projects/my-project",
            "projects/123456789",
            "datacatalog.googleapis.com",
            {
                "project_id": "my-project",
                "project_number": "123456789",
                "data_catalog_api_enabled": True,
                "dataplex_api_enabled": False,
                "ancestry": [],
            },
        ),
        (
            "projects/another-project",
            "projects/987654321",
            "dataplex.googleapis.com",
            {
                "project_id": "another-project",
                "project_number": "987654321",
                "data_catalog_api_enabled": False,
                "dataplex_api_enabled": True,
                "ancestry": [],
            },
        ),
    ],
)
def test_proto_to_project_valid(
    parent_full_resource_name: str,
    project: str,
    display_name: str,
    expected: dict[str, Any],
) -> None:
    """
    Test that valid proto messages are correctly converted to Project objects.
    """

    class MockResourceSearchResult:
        def __init__(
            self,
            parent_full_resource_name: str,
            project: str,
            display_name: str,
        ) -> None:
            self.parent_full_resource_name = parent_full_resource_name
            self.project = project
            self.display_name = display_name

    msg = MockResourceSearchResult(
        parent_full_resource_name, project, display_name
    )
    result = Project.proto_to_project(msg)
    assert result.to_dict() == expected


@pytest.mark.parametrize(
    "parent_full_resource_name, project, display_name",
    [
        (
            "invalid-resource-name",
            "projects/123456789",
            "datacatalog.googleapis.com",
        ),
        (
            "projects/my-project",
            "invalid-project-name",
            "datacatalog.googleapis.com",
        ),
    ],
)
def test_proto_to_project_invalid(
    parent_full_resource_name: str, project: str, display_name: str
) -> None:
    """
    Test that invalid proto messages raise a FormatException.
    """

    class MockResourceSearchResult:
        def __init__(
            self,
            parent_full_resource_name: str,
            project: str,
            display_name: str,
        ) -> None:
            self.parent_full_resource_name = parent_full_resource_name
            self.project = project
            self.display_name = display_name

    msg = MockResourceSearchResult(
        parent_full_resource_name, project, display_name
    )
    with pytest.raises(FormatException):
        Project.proto_to_project(msg)
