#!/usr/bin/env python3
# Copyright 2024 Canonical Limited
# See LICENSE file for licensing details.

"""Base utilities exposing common functionalities for all Events classes."""

from functools import wraps
from typing import Callable

from ops import CharmBase, EventBase, Object, StatusBase

from core.context import Context, S3ConnectionInfo, Status
from core.domain import PushGatewayInfo
from core.workload import ConfigurationHubWorkloadBase
from managers.k8s import KubernetesManager
from managers.s3 import S3Manager


class BaseEventHandler(Object):
    """Base class for all Event Handler classes in the Spark Configuration Hub."""

    workload: ConfigurationHubWorkloadBase
    charm: CharmBase
    context: Context

    def get_app_status(
        self, s3: S3ConnectionInfo | None, pushgateway: PushGatewayInfo | None
    ) -> StatusBase:
        """Return the status of the charm."""
        if not self.workload.ready():
            return Status.WAITING_PEBBLE.value

        k8s_manager = KubernetesManager(self.charm.model.app.name)
        if not k8s_manager.trusted():
            return Status.NOT_TRUSTED.value

        s3 = s3

        if s3:
            s3_manager = S3Manager(s3)
            if not s3_manager.verify():
                return Status.INVALID_CREDENTIALS.value

        if not self.workload.active():
            return Status.NOT_RUNNING.value

        return Status.ACTIVE.value


def compute_status(
    hook: Callable[[BaseEventHandler, EventBase], None]
) -> Callable[[BaseEventHandler, EventBase], None]:
    """Decorator to automatically compute statuses at the end of the hook."""

    @wraps(hook)
    def wrapper_hook(event_handler: BaseEventHandler, event: EventBase):
        """Return output after resetting statuses."""
        res = hook(event_handler, event)
        if event_handler.charm.unit.is_leader():
            event_handler.charm.app.status = event_handler.get_app_status(
                event_handler.context.s3,
                event_handler.context.pushgateway,
            )
        event_handler.charm.unit.status = event_handler.get_app_status(
            event_handler.context.s3,
            event_handler.context.pushgateway,
        )
        return res

    return wrapper_hook
