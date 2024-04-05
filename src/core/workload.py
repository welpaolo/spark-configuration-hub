#!/usr/bin/env python3
# Copyright 2024 Canonical Limited
# See LICENSE file for licensing details.

"""Implementation and blue-print for Spark Configuration Hub workloads."""

import secrets
import string
from abc import abstractmethod
from pathlib import Path

from common.workload import AbstractWorkload
from core.domain import User


class ConfigurationHubPaths:
    """Object to store common paths for Kafka."""

    def __init__(self, conf_path: Path | str, keytool: str):
        self.conf_path = conf_path if isinstance(conf_path, Path) else Path(conf_path)
        self.keytool = keytool

    @property
    def spark_properties(self) -> Path:
        """Return the path of the spark-properties file."""
        return self.conf_path / "spark-properties.conf"

    @property
    def cert(self):
        """Return the path of the certificate file."""
        return self.conf_path / "ca.pem"

    @property
    def truststore(self):
        """Return the path of the truststore."""
        return self.conf_path / "truststore.jks"


class ConfigurationHubWorkloadBase(AbstractWorkload):
    """Base interface for common workload operations."""

    paths: ConfigurationHubPaths
    user: User

    def restart(self) -> None:
        """Restarts the workload service."""
        self.stop()
        self.start()

    @abstractmethod
    def set_environment(self, env: dict[str, str | None]):
        """Set the environment."""
        ...

    @abstractmethod
    def active(self) -> bool:
        """Checks that the workload is active."""
        ...

    @abstractmethod
    def ready(self) -> bool:
        """Checks that the container/snap is ready."""
        ...

    @staticmethod
    def generate_password() -> str:
        """Creates randomized string for use as app passwords.

        Returns:
            String of 32 randomized letter+digit characters
        """
        return "".join([secrets.choice(string.ascii_letters + string.digits) for _ in range(32)])
