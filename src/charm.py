#!/usr/bin/env python3
# Copyright 2024 Canonical Limited
# See LICENSE file for licensing details.

"""Charmed Kubernetes Operator for Spark Configuration Hub."""

import shlex
import subprocess
from time import time
from typing import Optional

from charms.data_platform_libs.v0.s3 import (
    CredentialsChangedEvent,
    CredentialsGoneEvent,
    S3Requirer,
)
from ops.charm import CharmBase, ConfigChangedEvent, InstallEvent
from ops.main import main
from ops.model import StatusBase

from config import SparkConfigurationHubConfig
from constants import (
    CONF_FILE_PATH,
    PEER,
    S3_INTEGRATOR_REL,
)
from models import S3ConnectionInfo, Status
from utils import WithLogging


class SparkConfigurationHubCharm(CharmBase, WithLogging):
    """Charm the service."""

    def __init__(self, *args):
        super().__init__(*args)

        self.framework.observe(self.on.install, self._update_event)
        self.framework.observe(self.on.update_status, self._update_event)
        self.framework.observe(self.on.install, self._on_install)
        self.s3_requirer = S3Requirer(self, S3_INTEGRATOR_REL)
        self.framework.observe(
            self.s3_requirer.on.credentials_changed, self._on_s3_credential_changed
        )
        self.framework.observe(self.s3_requirer.on.credentials_gone, self._on_s3_credential_gone)

        self.framework.observe(self.on.config_changed, self._on_config_changed)

    @property
    def peers(self):
        """Fetch the peer relation."""
        return self.model.get_relation(PEER)

    def set_peer_data(self, key: str, data: Optional[str]) -> None:
        """Set peer data."""
        if data:
            self.peers.data[self.app][key] = data
        else:
            del self.peers.data[self.app][key]

    def get_peer_data(self, key: str) -> Optional[str]:
        """Get peer data."""
        if not self.peers:
            return None
        data = self.peers.data[self.app].get(key, None)
        return data if data else None

    @property
    def s3_connection_info(self) -> Optional[S3ConnectionInfo]:
        """Parse a S3ConnectionInfo object from relation data."""
        if not self.s3_requirer.relations:
            return None

        raw = self.s3_requirer.get_s3_connection_info()

        return S3ConnectionInfo(
            **{key.replace("-", "_"): value for key, value in raw.items() if key != "data"}
        )

    def get_status(
        self,
        s3: Optional[S3ConnectionInfo],
    ) -> StatusBase:
        """Compute and return the status of the charm."""
        if not s3:
            return Status.MISSING_S3_RELATION.value

        if not s3.verify():
            return Status.INVALID_CREDENTIALS.value

        return Status.ACTIVE.value

    def update_service(self, s3: Optional[S3ConnectionInfo]) -> bool:
        """Update the Spark Configuration hub service if needed."""
        status = self.log_result(lambda _: f"Status: {_}")(self.get_status(s3))

        self.unit.status = status

        # write configuration to peer relation databag

        # compare it to avoid unnecessary updates

        # write to configuration file

        spark_config = SparkConfigurationHubConfig(s3)
        self.logger.info(f"Current configuration: {spark_config.contents}")
        # fid.write(spark_config.contents)

        with open(CONF_FILE_PATH, "w") as f:
            f.write(spark_config.contents)
            f.close()

        if status is not Status.ACTIVE.value:
            self.logger.info(f"Cannot start service because of status {status}")
            return False
        if pid := self.get_peer_data("pid"):
            self._stop_process(int(pid))
        self._start_process()
        return True

    def _on_install(self, _: InstallEvent) -> None:
        """Handle the `on_install` event."""
        self.unit.status = Status.INSTALL.value

    def _on_s3_credential_changed(self, _: CredentialsChangedEvent):
        """Handle the `CredentialsChangedEvent` event from S3 integrator."""
        self.logger.info("S3 Credentials changed")
        self.update_service(self.s3_connection_info)

    def _on_s3_credential_gone(self, _: CredentialsGoneEvent):
        """Handle the `CredentialsGoneEvent` event for S3 integrator."""
        self.logger.info("S3 Credentials gone")
        self.update_service(None)

    def _update_event(self, _):
        """Handle the update event hook."""
        self.unit.status = self.get_status(self.s3_connection_info)

    def _on_config_changed(self, event: ConfigChangedEvent):
        """Handle the on config changed event."""
        self.update_service(self.s3_connection_info)

    def _start_process(
        self,
    ) -> int:
        """Handle the start of the process for consumption or production of messagges."""
        t0 = int(time())
        my_cmd = (
            f"nohup python3 -m src.monitor_sa --app-name={self.app.name} --config={CONF_FILE_PATH}"
        )
        self.logger.info(my_cmd)
        process = subprocess.Popen(
            shlex.split(my_cmd),
            stdout=open(f"/tmp/{t0}_monitor_sa.log", "w"),
            stderr=open(f"/tmp/{t0}_monitor_sa.err", "w"),
        )
        self.set_peer_data(key="pid", data=str(process.pid))
        self.logger.info(f"START PROCESS: {process.pid}")
        return process.pid

    def _stop_process(self, pid: int):
        """Handle the stop of the producer/consumer process."""
        self.logger.info(f"Killing process with pid: {pid}")
        subprocess.Popen(["kill", "-9", str(pid)])
        self.set_peer_data(key="pid", data=None)
        return pid


if __name__ == "__main__":  # pragma: nocover
    main(SparkConfigurationHubCharm)
