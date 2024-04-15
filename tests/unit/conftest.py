# Copyright 2024 Canonical Limited
# See LICENSE file for licensing details.

import pytest
from ops import pebble
from scenario import Container, Context, Model, Mount, Relation
from scenario.state import next_relation_id

from charm import SparkConfigurationHub
from constants import CONTAINER
from core.context import S3


@pytest.fixture
def configuration_hub_charm():
    """Provide fixture for the SparkConfigurationHub charm."""
    yield SparkConfigurationHub


@pytest.fixture
def configuration_hub_ctx(configuration_hub_charm):
    """Provide fixture for scenario context based on the SparkConfigurationHub charm."""
    return Context(charm_type=configuration_hub_charm)


@pytest.fixture
def model():
    """Provide fixture for the testing Juju model."""
    return Model(name="test-model")


@pytest.fixture
def configuration_hub_container(tmp_path):
    """Provide fixture for the Configuration Hub workload container."""
    layer = pebble.Layer(
        {
            "summary": "Charmed Spark Configuration Hub",
            "description": "Pebble base layer in Charmed Spark Configuration Hub",
            "services": {
                "configuration-hub": {
                    "override": "replace",
                    "summary": "This is the Spark Configuration Hub service",
                    "command": "/bin/bash /opt/hub/monitor_sa.sh",
                    "startup": "disabled",
                    "environment": {
                        "SPARK_PROPERTIES_FILE": "/etc/hub/conf/spark-properties.conf"
                    },
                },
            },
        }
    )

    etc = Mount("/etc/", tmp_path)

    return Container(
        name=CONTAINER,
        can_connect=True,
        layers={"base": layer},
        service_status={"configuration-hub": pebble.ServiceStatus.ACTIVE},
        mounts={"etc": etc},
    )


@pytest.fixture
def s3_relation():
    """Provide fixture for the S3 relation."""
    relation_id = next_relation_id(update=True)

    return Relation(
        endpoint=S3,
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


@pytest.fixture
def s3_relation_tls():
    """Provide fixture for the S3 relation."""
    relation_id = next_relation_id(update=True)

    return Relation(
        endpoint=S3,
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
            "tls-ca-chain": '["certificate"]',
        },
    )
