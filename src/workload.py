#!/usr/bin/env python3
# Copyright 2024 Canonical Limited
# See LICENSE file for licensing details.

"""Module containing all business logic related to the workload."""
import json

import ops.pebble
from ops.model import Container

from common.k8s import K8sWorkload
from common.utils import WithLogging
from core.domain import User
from core.workload import HistoryServerPaths, SparkHistoryWorkloadBase


class SparkHistoryServer(SparkHistoryWorkloadBase, K8sWorkload, WithLogging):
    """Class representing Workload implementation for the Configuration Hub charm on K8s."""

    CONTAINER = "spark-configuration-hub"
    CONTAINER_LAYER = "spark-configuration-hub"

    CONFIGURATION_HUB_SERVICE = "configuration-hub"

    CONFS_PATH = "/etc/spark/conf"
    ENV_FILE = "/etc/spark/environment"

    def __init__(self, container: Container, user: User):
        self.container = container
        self.user = user

        self.paths = HistoryServerPaths(conf_path=self.CONFS_PATH, keytool="keytool")

        self._envs = None

    @property
    def envs(self):
        """Return current environment."""
        if self._envs is not None:
            return self._envs

        self._envs = self.from_env(self.read(self.ENV_FILE)) if self.exists(self.ENV_FILE) else {}

        return self._envs

    @property
    def _spark_configuration_hub_layer(self):
        """Return a dictionary representing a Pebble layer."""
        layer = {
            "summary": "spark history server layer",
            "description": "pebble config layer for spark history server",
            "services": {
                self.CONFIGURATION_HUB_SERVICE: {
                    # "override": "merge",
                    # "summary": "spark history server",
                    # "startup": "enabled",
                    "environment": self.envs,
                }
            },
        }
        self.logger.info(f"Layer: {json.dumps(layer)}")
        return layer

    def start(self):
        """Execute business-logic for starting the workload."""
        layer = dict(self.container.get_plan().to_dict())

        layer["services"][self.CONFIGURATION_HUB_SERVICE] = (
            layer["services"][self.CONFIGURATION_HUB_SERVICE]
            | self._spark_history_server_layer["services"][self.CONFIGURATION_HUB_SERVICE]
        )

        self.container.add_layer(self.CONTAINER_LAYER, layer, combine=True)

        if not self.exists(str(self.paths.spark_properties)):
            self.logger.error(f"{self.paths.spark_properties} not found")
            raise FileNotFoundError(self.paths.spark_properties)

        # Push an updated layer with the new config
        # self.container.replan()
        self.container.restart(self.CONFIGURATION_HUB_SERVICE)

    def stop(self):
        """Execute business-logic for stopping the workload."""
        self.container.stop(self.CONFIGURATION_HUB_SERVICE)

    def ready(self) -> bool:
        """Check whether the service is ready to be used."""
        return self.container.can_connect()

    def active(self) -> bool:
        """Return the health of the service."""
        try:
            service = self.container.get_service(self.CONFIGURATION_HUB_SERVICE)
        except ops.pebble.ConnectionError:
            self.logger.debug(f"Service {self.CONFIGURATION_HUB_SERVICE} not running")
            return False

        return service.is_running()

    def set_environment(self, env: dict[str, str | None]):
        """Set environment for workload."""
        merged_envs = self.envs | env

        self._envs = {k: v for k, v in merged_envs.items() if v is not None}

        self.write("\n".join(self.to_env(self.envs)), self.ENV_FILE)
