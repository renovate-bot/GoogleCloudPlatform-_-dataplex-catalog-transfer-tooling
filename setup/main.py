
"""
This module provides a setup script for creating jobs and handlers
"""
import random
from cloud_setup import CloudSetup
from config import get_config

config = get_config()

project_setup = CloudSetup(
    config.project, config.service_location, config.service_account
)

hour = random.randint(0, 3)


#fetch project
print("Creating fetch project services")
print("Job")
project_setup.create_job(
    "fetch-projects",
    "us-docker.pkg.dev/dataplex-transfer-tooling/dataplex-transfer-tooling/"
    "fetch-projects-job:latest",
    ["-p", config.project]
)
print("Scheduler")
project_setup.create_scheduler(
    "fetch-projects",
    f"0 {hour} * * *"
)
print("Handler")
project_setup.create_service(
    "fetch-projects-handler",
    "us-docker.pkg.dev/dataplex-transfer-tooling/dataplex-transfer-tooling/"
    "fetch-projects-handler:latest",
    ["-p", config.project]
)

#fetch resources
print("Creating fetch resources services")
print("Job")
project_setup.create_job(
    "fetch-resources",
    "us-docker.pkg.dev/dataplex-transfer-tooling/dataplex-transfer-tooling/"
    "fetch-resources-job:latest",
    ["-p", config.project]
)
print("Scheduler")
project_setup.create_scheduler(
    "fetch-resources",
    f"0 {hour + 1} * * *"
)
print("Handler")
project_setup.create_service(
    "fetch-resources-handler",
    "us-docker.pkg.dev/dataplex-transfer-tooling/dataplex-transfer-tooling/"
    "fetch-resources-handler:latest",
    ["-p", config.project]
)

#find resource names
print("Creating find resource names services")
print("Job")
project_setup.create_job(
    "findd-resource-names",
    "us-docker.pkg.dev/dataplex-transfer-tooling/dataplex-transfer-tooling/"
    "find-resource-names-job:latest",
    ["-p", config.project],
    {"memory": "2G"}
)
print("Scheduler")
project_setup.create_scheduler(
    "find-resource-names",
    f"0 {hour + 2} * * *"
)
print("Handler")
project_setup.create_service(
    "find-resource-names-handler",
    "us-docker.pkg.dev/dataplex-transfer-tooling/dataplex-transfer-tooling/"
    "find-resource-names-handler:latest",
    ["-p", config.project]
)

#fetch policies
print("Creating fetch policies services")
print("Job")
project_setup.create_job(
    "fetch-policies",
    "us-docker.pkg.dev/dataplex-transfer-tooling/dataplex-transfer-tooling/"
    "fetch-policies-job:latest",
    ["-p", config.project, "-s", config.scope],
    {"memory": "2G"}
)
print("Scheduler")
project_setup.create_scheduler(
    "fetch-policies",
    f"0 {hour + 14} * * *"
)
print("Handler")
project_setup.create_service(
    "fetch-policies-handler",
    "us-docker.pkg.dev/dataplex-transfer-tooling/dataplex-transfer-tooling/"
    "fetch-policies-handler:latest",
    ["-p", config.project]
)

#audit logs
print("Creating audit logs services")
project_setup.create_job(
    "audit-logs",
    "us-docker.pkg.dev/dataplex-transfer-tooling/dataplex-transfer-tooling/"
    "audit-logs-job:latest",
    ["-p", config.project, "-s", config.scope]
)
print("Job")
project_setup.create_scheduler(
    "audit-logs",
    f"0 {hour + 17} * * *"
)

#analytics
print("Creating analytics services")
print("Job")
project_setup.create_job(
    "analytics",
    "us-docker.pkg.dev/dataplex-transfer-tooling/dataplex-transfer-tooling/"
    "analytics-job:latest",
    ["-p", config.project]
)
print("Scheduler")
project_setup.create_scheduler(
    "analytics",
    f"0 {hour + 18} * * *"
)

#convert private tag templates
print("Creating convert private tag templates services")
print("Job")
project_setup.create_job(
    "convert-private-tag-templates",
    "us-docker.pkg.dev/dataplex-transfer-tooling/dataplex-transfer-tooling/"
    "convert-private-tag-templates-job:latest",
    ["-p", config.project, "-s", config.scope]
)
print("Handler")
project_setup.create_service(
    "convert-private-tag-templates-handler",
    "us-docker.pkg.dev/dataplex-transfer-tooling/dataplex-transfer-tooling/"
    "convert-private-tag-templates-handler:latest",
    ["-p", config.project]
)

#transfer resources
print("Creating transfer resources services")
print("Job")
project_setup.create_job(
    "transfer-resources",
    "us-docker.pkg.dev/dataplex-transfer-tooling/dataplex-transfer-tooling/"
    "transfer-resources-job:latest",
    ["-p", config.project, "-s", config.scope]
)
print("Handler")
project_setup.create_service(
    "transfer-resources-handler",
    "us-docker.pkg.dev/dataplex-transfer-tooling/dataplex-transfer-tooling/"
    "transfer-resources-handler:latest",
    ["-p", config.project]
)

#clean up
print("Creating clean up services")
print("Job")
project_setup.create_job(
    "clean-up",
    "us-docker.pkg.dev/dataplex-transfer-tooling/dataplex-transfer-tooling/"
    "clean-up-job:latest",
    ["-p", config.project, "-s", config.scope]
)
print("Handler")
project_setup.create_service(
    "clean-up-handler",
    "us-docker.pkg.dev/dataplex-transfer-tooling/dataplex-transfer-tooling/"
    "clean-up-handler:latest",
    ["-p", config.project]
)
