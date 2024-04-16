#!/usr/bin/env python3
# Copyright 2024 Canonical Limited
# See LICENSE file for licensing details.

"""Spark Service accounts related event handlers."""

from ops import CharmBase

from common.utils import WithLogging
from constants import SERVICE_ACCOUNT
from core.context import Context
from core.workload import ConfigurationHubWorkloadBase
from events.base import BaseEventHandler
from managers.configuration_hub import ConfigurationHubManager
from relations.spark_sa import (
    ServiceAccountProvider,
    ServiceAccountReleasedEvent,
    ServiceAccountRequestedEvent,
)


class ServiceAccountsEvents(BaseEventHandler, WithLogging):
    """Class implementing Spark Service Account Integration event hooks."""

    def __init__(self, charm: CharmBase, context: Context, workload: ConfigurationHubWorkloadBase):
        super().__init__(charm, "service-account")

        self.charm = charm
        self.context = context
        self.workload = workload

        self.configuration_hub = ConfigurationHubManager(self.workload)

        self.sa = ServiceAccountProvider(self.charm, SERVICE_ACCOUNT)
        self.framework.observe(self.sa.on.account_requested, self._on_service_account_requested)
        self.framework.observe(self.sa.on.account_released, self._on_service_account_released)

    def _on_service_account_requested(self, event: ServiceAccountRequestedEvent):
        """Handle the `ServiceAccountRequested` event for the Spark Integrator hub."""
        self.logger.info("Service account requested.")

        if not self.charm.unit.is_leader():
            return
        relation_id = event.relation.id

        service_account = event.service_account
        namespace = event.namespace
        self.logger.debug(
            f"Desired service account name: {service_account} in namespace: {namespace}"
        )
        assert service_account is not None
        assert namespace is not None

        # Try to create service account

        try:
            self.workload.exec(
                f"python3 -m spark8t.cli.service_account_registry create --username={service_account} --namespace={namespace}"
            )
        except Exception as e:
            self.logger.error(e)
            raise RuntimeError(
                f"Impossible to create service account: {service_account} in namespace: {namespace}"
            )

        sa_configuration = {"service-account": service_account, "namespace": namespace}

        # update connection parameters in the relation data bug
        self.sa.update_connection_info(relation_id, sa_configuration)

    def _on_service_account_released(self, event: ServiceAccountReleasedEvent):
        """Handle the `ServiceAccountReleased` event for the Spark Integrator hub."""
        self.logger.info("Service account released.")

        if not self.charm.unit.is_leader():
            return

        service_account = event.service_account
        namespace = event.namespace
        self.logger.debug(
            f"The service account name: {service_account} in namespace: {namespace} should be deleted"
        )
        assert service_account is not None
        assert namespace is not None

        # Try to create service account
        try:
            self.workload.exec(
                f"python3 -m spark8t.cli.service_account_registry delete --username={service_account} --namespace={namespace}"
            )
        except Exception as e:
            self.logger.error(e)
            raise RuntimeError(
                f"Failed to delete service account: {service_account} in namespace: {namespace}"
            )
