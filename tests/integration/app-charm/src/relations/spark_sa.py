# Copyright 2024 Canonical Ltd.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

r"""A library for creating service accounts that are configured to run Spark jobs."""
import json
import logging
from collections import namedtuple
from typing import Dict, List, Optional, Union

import ops.charm
import ops.framework
import ops.model
from ops import RelationDepartedEvent
from ops.charm import (
    CharmBase,
    CharmEvents,
    RelationBrokenEvent,
    RelationChangedEvent,
    RelationEvent,
    RelationJoinedEvent,
)
from ops.framework import EventSource, Object, ObjectEvents
from ops.model import Application, Relation, RelationDataContent, Unit

# The unique Charmhub library identifier, never change it
LIBID = ""

# Increment this major API version when introducing breaking changes
LIBAPI = 0

# Increment this PATCH version before using `charmcraft publish-lib` or reset
# to 0 if you are raising the major API version
LIBPATCH = 1


logger = logging.getLogger(__name__)

Diff = namedtuple("Diff", "added changed deleted")
Diff.__doc__ = """
A tuple for storing the diff between two data mappings.

added - keys that were added
changed - keys that still exist but have new values
deleted - key that were deleted"""


def diff(event: RelationChangedEvent, bucket: Union[Unit, Application]) -> Diff:
    """Retrieves the diff of the data in the relation changed databag.

    Args:
        event: relation changed event.
        bucket: bucket of the databag (app or unit)

    Returns:
        a Diff instance containing the added, deleted and changed
            keys from the event relation databag.
    """
    # Retrieve the old data from the data key in the application relation databag.
    old_data = json.loads(event.relation.data[bucket].get("data", "{}"))
    # Retrieve the new data from the event relation databag.
    new_data = (
        {key: value for key, value in event.relation.data[event.app].items() if key != "data"}
        if event.app
        else {}
    )

    # These are the keys that were added to the databag and triggered this event.
    added = new_data.keys() - old_data.keys()
    # These are the keys that were removed from the databag and triggered this event.
    deleted = old_data.keys() - new_data.keys()
    # These are the keys that already existed in the databag,
    # but had their values changed.
    changed = {key for key in old_data.keys() & new_data.keys() if old_data[key] != new_data[key]}

    # TODO: evaluate the possibility of losing the diff if some error
    # happens in the charm before the diff is completely checked (DPE-412).
    # Convert the new_data to a serializable format and save it for a next diff check.
    event.relation.data[bucket].update({"data": json.dumps(new_data)})

    # Return the diff with all possible changes.
    return Diff(added, changed, deleted)


class ServiceAccountEvent(RelationEvent):
    """Base class for Service account events."""

    @property
    def service_account(self) -> Optional[str]:
        """Returns the service account was requested."""
        if not self.relation.app:
            return None

        return self.relation.data[self.relation.app].get("service-account", "")

    @property
    def namespace(self) -> Optional[str]:
        """Returns the namespace that was requested."""
        if not self.relation.app:
            return None

        return self.relation.data[self.relation.app].get("namespace", "")


class ServiceAccountRequestedEvent(ServiceAccountEvent):
    """Event emitted when a set of service account/namespace is requested for use on this relation."""


class ServiceAccountReleasedEvent(ServiceAccountEvent):
    """Event emitted when a set of service account/namespace is released."""


class IntegratorHubServiceAccountEvents(CharmEvents):
    """Event descriptor for events raised by ServiceAccountProvider."""

    account_requested = EventSource(ServiceAccountRequestedEvent)
    account_released = EventSource(ServiceAccountReleasedEvent)


class ServiceAccountProvider(Object):
    """A provider handler for communicating ServiceAccount details to consumers."""

    on = (
        IntegratorHubServiceAccountEvents()
    )  # pyright: ignore [reportAssignmentType, reportGeneralTypeIssues]

    def __init__(
        self,
        charm: CharmBase,
        relation_name: str,
    ):
        super().__init__(charm, relation_name)
        self.charm = charm
        self.local_app = self.charm.model.app
        self.local_unit = self.charm.unit
        self.relation_name = relation_name

        # monitor relation changed event for changes in the provided fields
        self.framework.observe(charm.on[relation_name].relation_changed, self._on_relation_changed)
        self.framework.observe(
            charm.on[relation_name].relation_departed, self._on_relation_departed
        )

    def _on_relation_changed(self, event: RelationChangedEvent) -> None:
        """React to the relation changed event by consuming data."""
        if not self.charm.unit.is_leader():
            return
        diff = self._diff(event)
        # emit on account requested if service account name is provided by the requirer application
        if "service-account" in diff.added:
            getattr(self.on, "account_requested").emit(
                event.relation, app=event.app, unit=event.unit
            )

    def _on_relation_departed(self, event: RelationDepartedEvent) -> None:
        """React to the relation changed event by consuming data."""
        if not self.charm.unit.is_leader():
            return

        getattr(self.on, "account_released").emit(event.relation, app=event.app, unit=event.unit)

    def _load_relation_data(self, raw_relation_data: dict) -> dict:
        """Loads relation data from the relation data bag.

        Args:
            raw_relation_data: Relation data from the databag
        Returns:
            dict: Relation data in dict format.
        """
        connection_data = {}
        for key in raw_relation_data:
            try:
                connection_data[key] = json.loads(raw_relation_data[key])
            except (json.decoder.JSONDecodeError, TypeError):
                connection_data[key] = raw_relation_data[key]
        return connection_data

    def _diff(self, event: RelationChangedEvent) -> Diff:
        """Retrieves the diff of the data in the relation changed databag.

        Args:
            event: relation changed event.

        Returns:
            a Diff instance containing the added, deleted and changed
                keys from the event relation databag.
        """
        return diff(event, self.local_app)

    def fetch_relation_data(self) -> dict:
        """Retrieves data from relation.

        This function can be used to retrieve data from a relation
        in the charm code when outside an event callback.

        Returns:
            a dict of the values stored in the relation data bag
                for all relation instances (indexed by the relation id).
        """
        data = {}
        for relation in self.relations:
            data[relation.id] = (
                {key: value for key, value in relation.data[relation.app].items() if key != "data"}
                if relation.app
                else {}
            )
        return data

    def update_connection_info(self, relation_id: int, connection_data: dict) -> None:
        """Updates the fields in the databag as set of key-value pairs in the relation.

        This function writes in the application data bag, therefore,
        only the leader unit can call it.

        Args:
            relation_id: the identifier for a particular relation.
            connection_data: dict containing the key-value pairs
                that should be updated.
        """
        # check and write changes only if you are the leader
        if not self.local_unit.is_leader():
            return

        relation = self.charm.model.get_relation(self.relation_name, relation_id)

        if not relation:
            return

        # update the databag, if connection data did not change with respect to before
        # the relation changed event is not triggered
        updated_connection_data = {}
        for configuration_option, configuration_value in connection_data.items():
            updated_connection_data[configuration_option] = configuration_value

        relation.data[self.local_app].update(updated_connection_data)
        logger.debug(f"Updated service account info: {updated_connection_data}")

    @property
    def relations(self) -> List[Relation]:
        """The list of Relation instances associated with this relation_name."""
        return list(self.charm.model.relations[self.relation_name])

    def set_service_account(self, relation_id: int, service_account: str) -> None:
        """Sets service account name in application databag.

        This function writes in the application data bag, therefore,
        only the leader unit can call it.

        Args:
            relation_id: the identifier for a particular relation.
            service_account: the service account name.
        """
        self.update_connection_info(relation_id, {"service-account": service_account})

    def set_namespace(self, relation_id: int, namespace: str) -> None:
        """Sets access-key value in application databag.

        This function writes in the application data bag, therefore,
        only the leader unit can call it.

        Args:
            relation_id: the identifier for a particular relation.
            namespace: the name of the service account.
        """
        self.update_connection_info(relation_id, {"namespace": namespace})


class ServiceAccountGrantedEvent(ServiceAccountEvent):
    """Event emitted when service account are granted on this relation."""


class ServiceAccountGoneEvent(RelationEvent):
    """Event emitted when service account are removed from this relation."""


class ServiceAccountRequiresEvents(ObjectEvents):
    """Event descriptor for events raised by the ServiceAccountProvider."""

    account_granted = EventSource(ServiceAccountGrantedEvent)
    account_gone = EventSource(ServiceAccountGoneEvent)


REQUIRED_OPTIONS = ["service-account", "namespace"]


class ServiceAccountRequirer(Object):
    """Requires-side of the Service Account relation."""

    on = (
        ServiceAccountRequiresEvents()
    )  # pyright: ignore[reportAssignmentType, reportGeneralTypeIssues]

    def __init__(
        self, charm: ops.charm.CharmBase, relation_name: str, service_account: str, namespace: str
    ):
        """Manager of the ServiceAccount client relations."""
        super().__init__(charm, relation_name)

        self.relation_name = relation_name
        self.charm = charm
        self.local_app = self.charm.model.app
        self.local_unit = self.charm.unit
        self.service_account = service_account
        self.namespace = namespace

        self.framework.observe(
            self.charm.on[self.relation_name].relation_changed, self._on_relation_changed
        )

        self.framework.observe(
            self.charm.on[self.relation_name].relation_joined, self._on_relation_joined
        )

        self.framework.observe(
            self.charm.on[self.relation_name].relation_broken,
            self._on_relation_broken,
        )

    def _on_relation_joined(self, event: RelationJoinedEvent) -> None:
        """Event emitted when the application joins the service account relation."""
        self.update_connection_info(
            event.relation.id,
            {"service-account": self.service_account, "namespace": self.namespace},
        )

    def fetch_relation_data(self) -> dict:
        """Retrieves data from relation.

        This function can be used to retrieve data from a relation
        in the charm code when outside an event callback.

        Returns:
            a dict of the values stored in the relation data bag
                for all relation instances (indexed by the relation id).
        """
        data = {}

        for relation in self.relations:
            data[relation.id] = self._load_relation_data(relation.data[self.charm.app])
        return data

    def update_connection_info(self, relation_id: int, connection_data: dict) -> None:
        """Updates the databag as set of key-value pairs in the relation.

        This function writes in the application data bag, therefore,
        only the leader unit can call it.

        Args:
            relation_id: the identifier for a particular relation.
            connection_data: dict containing the key-value pairs
                that should be updated.
        """
        # check and write changes only if you are the leader
        if not self.local_unit.is_leader():
            return

        relation = self.charm.model.get_relation(self.relation_name, relation_id)

        if not relation:
            return

        # update the databag, if connection data did not change with respect to before
        # the relation changed event is not triggered
        updated_data = {}
        for configuration_option, configuration_value in connection_data.items():
            updated_data[configuration_option] = configuration_value

        relation.data[self.local_app].update(updated_data)
        logger.debug(f"Updated service-account data: {updated_data}")

    def _load_relation_data(self, raw_relation_data: RelationDataContent) -> Dict[str, str]:
        """Loads relation data from the relation data bag.

        Args:
            raw_relation_data: Relation data from the databag
        Returns:
            dict: Relation data in dict format.
        """
        connection_data = {}
        for key in raw_relation_data:
            try:
                connection_data[key] = json.loads(raw_relation_data[key])
            except (json.decoder.JSONDecodeError, TypeError):
                connection_data[key] = raw_relation_data[key]
        return connection_data

    def _diff(self, event: RelationChangedEvent) -> Diff:
        """Retrieves the diff of the data in the relation changed databag.

        Args:
            event: relation changed event.

        Returns:
            a Diff instance containing the added, deleted and changed
                keys from the event relation databag.
        """
        return diff(event, self.local_unit)

    def _on_relation_changed(self, event: RelationChangedEvent) -> None:
        """Notify the charm about the presence of service-account info."""
        # check if the mandatory options are in the relation data
        contains_required_options = True
        # get current service account info
        infos = self.get_service_account_info()
        # records missing options
        missing_options = []
        for configuration_option in REQUIRED_OPTIONS:
            if configuration_option not in infos:
                contains_required_options = False
                missing_options.append(configuration_option)
        # emit account change event only if all mandatory fields are present
        if contains_required_options:
            getattr(self.on, "account_granted").emit(
                event.relation, app=event.app, unit=event.unit
            )
        else:
            logger.warning(
                f"Some mandatory fields: {missing_options} are not present, do not emit service account change event!"
            )

    def get_service_account_info(self) -> Dict[str, str]:
        """Return the service account info as a dictionary."""
        for relation in self.relations:
            if relation and relation.app:
                return self._load_relation_data(relation.data[relation.app])

        return {}

    def _on_relation_broken(self, event: RelationBrokenEvent) -> None:
        """Notify the charm about a broken service account relation."""
        getattr(self.on, "account_gone").emit(event.relation, app=event.app, unit=event.unit)

    @property
    def relations(self) -> List[Relation]:
        """The list of Relation instances associated with this relation_name."""
        return list(self.charm.model.relations[self.relation_name])
