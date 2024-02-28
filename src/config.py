#!/usr/bin/env python3
# Copyright 2024 Canonical Limited
# See LICENSE file for licensing details.

"""Spark Configuration Hub configuration."""

import re
from typing import Optional

from models import S3ConnectionInfo
from utils import WithLogging


class SparkConfigurationHubConfig(WithLogging):
    """Spark Confiuration Hub Configuration."""

    _ingress_pattern = re.compile("http://.*?/|https://.*?/")

    def __init__(
        self,
        s3_connection_info: Optional[S3ConnectionInfo],
    ):
        self.s3_connection_info = s3_connection_info

    _base_conf: dict[str, str] = {
        "spark.hadoop.fs.s3a.connection.ssl.enabled": "false",
        "spark.hadoop.fs.s3a.path.style.access": "true",
        "spark.eventLog.enabled": "true",
    }

    @property
    def _s3_conf(self) -> dict[str, str]:
        return (
            {
                "spark.hadoop.fs.s3a.endpoint": self.s3_connection_info.endpoint
                or "https://s3.amazonaws.com",
                "spark.hadoop.fs.s3a.access.key": self.s3_connection_info.access_key,
                "spark.hadoop.fs.s3a.secret.key": self.s3_connection_info.secret_key,
                "spark.eventLog.dir": self.s3_connection_info.log_dir,
                "spark.history.fs.logDirectory": self.s3_connection_info.log_dir,
                "spark.hadoop.fs.s3a.aws.credentials.provider": "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
            }
            if self.s3_connection_info
            else {}
        )

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
