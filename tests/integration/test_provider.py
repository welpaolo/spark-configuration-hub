import asyncio
import json
import logging
import subprocess
from pathlib import Path

import pytest
import yaml
from pytest_operator.plugin import OpsTest

logger = logging.getLogger(__name__)

METADATA = yaml.safe_load(Path("./metadata.yaml").read_text())
APP_NAME = METADATA["name"]
DUMMY_APP_NAME = "app"

REL_NAME_A = "spark-account-a"
REL_NAME_B = "spark-account-b"


def check_service_account_existance(namespace: str, service_account_name) -> bool:
    """Retrieve secret data for a given namespace and secret."""
    command = ["kubectl", "get", "sa", "-n", namespace, "--output", "json"]
    try:
        output = subprocess.run(command, check=True, capture_output=True)
        # output.stdout.decode(), output.stderr.decode(), output.returncode
        result = output.stdout.decode()
        logger.info(f"Command: {command}")
        logger.info(f"Service accounts for namespace: {namespace}")
        logger.info(f"results: {str(result)}")
        accounts = json.loads(result)
        for sa in accounts["items"]:
            name = sa["metadata"]["name"]
            logger.info(f"\t secretName: {name}")
            if name == service_account_name:
                return True
        return False
    except subprocess.CalledProcessError as e:
        logger.error(e.stdout.decode(), e.stderr.decode(), e.returncode)
        return False

@pytest.mark.group(1)
@pytest.mark.abort_on_fail
async def test_build_and_deploy_test_app(ops_test: OpsTest):
    """Build the charm-under-test and deploy it together with related charms.

    Assert on the unit status before any relations/configurations take place.
    """
    logger.info("Building charm")
    # Build and deploy charm from local source folder

    charm = await ops_test.build_charm(".")

    test_charm = await ops_test.build_charm("tests/integration/app-charm", verbosity="trace")

    image_version = METADATA["resources"]["configuration-hub-image"]["upstream-source"]

    logger.info(f"Image version: {image_version}")

    resources = {"configuration-hub-image": image_version}

    logger.info("Deploying Spark Configuration hub charm")

    # Deploy the charm and wait for waiting status
    await asyncio.gather(
        ops_test.model.deploy(
            test_charm, application_name=DUMMY_APP_NAME, num_units=1, series="jammy"
        ),
        ops_test.model.deploy(
            charm,
            resources=resources,
            application_name=APP_NAME,
            num_units=1,
            series="jammy",
            trust=True,
        ),
    )

    await ops_test.model.wait_for_idle(apps=[APP_NAME, DUMMY_APP_NAME], timeout=600)

    async with ops_test.fast_forward():
        await ops_test.model.wait_for_idle(apps=[APP_NAME, DUMMY_APP_NAME], status="active")

@pytest.mark.group(1)
@pytest.mark.abort_on_fail
async def test_build_and_deploy(ops_test: OpsTest, namespace):

    logger.info(f"Add namespace: {namespace}")
    configuration_parameters = {"namespace": namespace}
    # apply new configuration options
    await ops_test.model.applications[DUMMY_APP_NAME].set_config(configuration_parameters)

    await ops_test.model.add_relation(APP_NAME, f"{DUMMY_APP_NAME}:{REL_NAME_A}")

    async with ops_test.fast_forward(fast_interval="60s"):
        await ops_test.model.wait_for_idle(
            apps=[APP_NAME, DUMMY_APP_NAME], idle_period=30, status="active", timeout=2000
        )

    assert check_service_account_existance(namespace, "sa1")

    await ops_test.model.add_relation(APP_NAME, f"{DUMMY_APP_NAME}:{REL_NAME_B}")

    async with ops_test.fast_forward(fast_interval="60s"):
        await ops_test.model.wait_for_idle(
            apps=[APP_NAME, DUMMY_APP_NAME], idle_period=30, status="active", timeout=2000
        )
    assert check_service_account_existance(namespace, "sa1")

    await ops_test.model.applications[APP_NAME].remove_relation(
        f"{APP_NAME}:spark-service-account", f"{DUMMY_APP_NAME}:{REL_NAME_A}"
    )

    await ops_test.model.wait_for_idle(apps=[APP_NAME, DUMMY_APP_NAME], timeout=600)
    assert not check_service_account_existance(namespace, "sa1")
