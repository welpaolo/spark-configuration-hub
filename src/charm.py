#!/usr/bin/env python3
# Copyright 2024 Canonical Limited
# See LICENSE file for licensing details.

"""Charmed Kubernetes Operator for the Spark Configuration Hub Charm."""

from ops import CharmBase
from ops.main import main

from common.utils import WithLogging
from constants import CONTAINER, PEBBLE_USER
from core.context import Context
from core.domain import User
from events.configuration_hub import ConfigurationHubEvents
from events.pushgateway import PushgatewayEvents
from events.s3 import S3Events
from workload import ConfigurationHub


class SparkConfigurationHub(CharmBase, WithLogging):
    """Charm the service."""

    def __init__(self, *args):
        super().__init__(*args)

        context = Context(self)
        workload = ConfigurationHub(
            self.unit.get_container(CONTAINER), User(name=PEBBLE_USER[0], group=PEBBLE_USER[1])
        )

        self.s3 = S3Events(self, context, workload)
        self.pushgateway = PushgatewayEvents(self, context, workload)
        self.configuration_hub = ConfigurationHubEvents(self, context, workload)


if __name__ == "__main__":  # pragma: nocover
    main(SparkConfigurationHub)
