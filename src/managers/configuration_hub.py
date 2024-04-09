#!/usr/bin/env python3
# Copyright 2024 Canonical Limited
# See LICENSE file for licensing details.

"""Configuration Hub manager."""

import re

from common.utils import WithLogging
from core.context import S3ConnectionInfo
from core.workload import ConfigurationHubWorkloadBase


class ConfigurationHubConfig(WithLogging):
    """Class representing the Spark Properties configuration file."""

    _ingress_pattern = re.compile("http://.*?/|https://.*?/")

    _base_conf: dict[str, str] = {}

    def __init__(self, s3: S3ConnectionInfo | None):
        self.s3 = s3

    @staticmethod
    def _ssl_enabled(endpoint: str | None) -> str:
        """Check if ssl is enabled."""
        if not endpoint or endpoint.startswith("https:") or ":443" in endpoint:
            return "true"

        return "false"

    @property
    def _s3_conf(self) -> dict[str, str]:
        if s3 := self.s3:
            return {
                "spark.hadoop.fs.s3a.endpoint": s3.endpoint or "https://s3.amazonaws.com",
                "spark.hadoop.fs.s3a.access.key": s3.access_key,
                "spark.hadoop.fs.s3a.secret.key": s3.secret_key,
                "spark.eventLog.dir": s3.log_dir,
                "spark.history.fs.logDirectory": s3.log_dir,
                "spark.hadoop.fs.s3a.aws.credentials.provider": "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
                "spark.hadoop.fs.s3a.connection.ssl.enabled": self._ssl_enabled(s3.endpoint),
            }
        return {}

    def to_dict(self) -> dict[str, str]:
        """Return the dict representation of the configuration file."""
        return self._base_conf | self._s3_conf

    @property
    def contents(self) -> str:
        """Return configuration contents formatted to be consumed by pebble layer."""
        dict_content = self.to_dict()

        return "\n".join(
            [
                f"{key}={value}"
                for key in sorted(dict_content.keys())
                if (value := dict_content[key])
            ]
        )


class ConfigurationHubManager(WithLogging):
    """Class exposing general functionalities of the ConfigurationHub workload."""

    def __init__(self, workload: ConfigurationHubWorkloadBase):
        self.workload = workload

    def update(self, s3: S3ConnectionInfo | None) -> None:
        """Update the Configuration Hub service if needed."""
        self.logger.debug("Update")
        self.workload.stop()

        config = ConfigurationHubConfig(s3)
        self.workload.write(config.contents, str(self.workload.paths.spark_properties))
        self.workload.set_environment(
            {"SPARK_PROPERTIES_FILE": str(self.workload.paths.spark_properties)}
        )    
        self.logger.info("Start service")
        self.workload.start()
