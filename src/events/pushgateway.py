#!/usr/bin/env python3
# Copyright 2024 Canonical Limited
# See LICENSE file for licensing details.

"""S3 Integration related event handlers."""


from charms.prometheus_pushgateway_k8s.v0.pushgateway import PrometheusPushgatewayRequirer
from ops import CharmBase, RelationBrokenEvent, RelationChangedEvent

from common.utils import WithLogging
from core.context import PUSHGATEWAY, Context
from core.workload import ConfigurationHubWorkloadBase
from events.base import BaseEventHandler, compute_status
from managers.configuration_hub import ConfigurationHubManager


class PushgatewayEvents(BaseEventHandler, WithLogging):
    """Class implementing PushGateway event hooks."""

    def __init__(self, charm: CharmBase, context: Context, workload: ConfigurationHubWorkloadBase):
        super().__init__(charm, "pushgateway")

        self.charm = charm
        self.context = context
        self.workload = workload

        self.configuration_hub = ConfigurationHubManager(self.workload)

        self.pushgateway = PrometheusPushgatewayRequirer(self.charm, PUSHGATEWAY)

        self.framework.observe(self.charm.on[PUSHGATEWAY].relation_changed, self._on_pushgateway_changed)
        self.framework.observe(self.charm.on[PUSHGATEWAY].relation_broken, self._on_pushgateway_gone)

    @compute_status
    def _on_pushgateway_changed(self, _: RelationChangedEvent):
        """Handle the `RelationChanged` event from the PushGateway."""
        self.logger.info("PushGateway relation changed")
        self.logger.info(f"PushGateway ready: {self.pushgateway.is_ready()}")
        if self.pushgateway.is_ready():
            self.configuration_hub.update(self.context.s3, self.context.pushgateway)

    def _on_pushgateway_gone(self, _: RelationBrokenEvent):
        """Handle the `RelationBroken` event for PushGateway."""
        self.logger.info("PushGateway relation broken")
        self.configuration_hub.update(self.context.s3, None)

        self.charm.unit.status = self.get_app_status(self.context.s3, None)
        if self.charm.unit.is_leader():
            self.charm.app.status = self.get_app_status(self.context.s3, None)
