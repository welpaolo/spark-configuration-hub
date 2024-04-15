#!/usr/bin/env python3
# Copyright 2024 Canonical Limited
# See LICENSE file for licensing details.

"""Spark Configuration Hub workload related event handlers."""

from ops.charm import CharmBase

from common.utils import WithLogging
from constants import CONFIGURATION_HUB_LABEL
from core.context import Context
from core.workload import ConfigurationHubWorkloadBase
from events.base import BaseEventHandler, compute_status
from managers.configuration_hub import ConfigurationHubManager


class ConfigurationHubEvents(BaseEventHandler, WithLogging):
    """Class implementing Spark Configuration Hub event hooks."""

    def __init__(self, charm: CharmBase, context: Context, workload: ConfigurationHubWorkloadBase):
        super().__init__(charm, "configuration-hub")
        self.charm = charm
        self.context = context
        self.workload = workload

        self.configuration_hub = ConfigurationHubManager(self.workload)

        self.framework.observe(
            self.charm.on.configuration_hub_pebble_ready,
            self._on_configuration_hub_pebble_ready,
        )
        self.framework.observe(self.charm.on.update_status, self._update_event)
        self.framework.observe(self.charm.on.install, self._update_event)
        self.framework.observe(self.charm.on.stop, self._remove_resources)

    def _remove_resources(self, _):
        """Handle the stop event."""
        self.configuration_hub.workload.exec(
            f"kubectl delete secret -l {CONFIGURATION_HUB_LABEL} --all-namespaces"
        )

    @compute_status
    def _on_configuration_hub_pebble_ready(self, _):
        """Handle on Pebble ready event."""
        self.configuration_hub.update(self.context.s3)

    @compute_status
    def _update_event(self, _):
        pass
