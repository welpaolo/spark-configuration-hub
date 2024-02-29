# Copyright 2023 Canonical Limited
# See LICENSE file for licensing details.

import pytest
from scenario import Context, Model, Relation
from scenario.state import next_relation_id

from charm import SparkConfigurationHubCharm
from constants import S3_INTEGRATOR_REL


@pytest.fixture
def configuration_hub_charm():
    """Provide fixture for the SparkConfigurationHub charm."""
    yield SparkConfigurationHubCharm


@pytest.fixture
def configuration_hub_charm_ctx(configuration_hub_charm):
    """Provide fixture for scenario context based on the SparkConfigurationHub charm."""
    return Context(charm_type=configuration_hub_charm)


@pytest.fixture
def model():
    """Provide fixture for the testing Juju model."""
    return Model(name="test-model")


@pytest.fixture
def s3_relation():
    """Provide fixture for the S3 relation."""
    relation_id = next_relation_id(update=True)

    return Relation(
        endpoint=S3_INTEGRATOR_REL,
        interface="s3",
        remote_app_name="s3-integrator",
        relation_id=relation_id,
        local_app_data={"bucket": f"relation-{relation_id}"},
        remote_app_data={
            "access-key": "access-key",
            "bucket": "my-bucket",
            "data": f'{{"bucket": "relation-{relation_id}"}}',
            "endpoint": "https://s3.endpoint",
            "path": "spark-events",
            "secret-key": "secret-key",
        },
    )
