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
This script initializes the audit log setup process by creating a log
sink for exporting logs to BigQuery. It uses the application configuration
to set up the required resources.
"""

from setup_audit_log import AuditLogsSetup
from config import get_application_config, get_log_filter


def main(app_config: dict, log_filter: str) -> None:
    """
    Main function to set up the audit log sink.
    """
    audit_log_job = AuditLogsSetup(app_config)
    audit_log_job.create_sink(log_filter)


if __name__ == "__main__":
    config = get_application_config()
    log_filter_param = get_log_filter()
    main(config, log_filter_param)
