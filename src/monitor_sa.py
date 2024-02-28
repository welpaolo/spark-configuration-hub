#!/usr/bin/env python3
# Copyright 2024 Canonical Ltd.
# See LICENSE file for licensing details.

"""Routine that updates secrets in Spark service accounts."""

import argparse
from typing import Dict, Optional

from lightkube.core.client import Client
from lightkube.core.exceptions import ApiError
from lightkube.resources.core_v1 import Secret, ServiceAccount


def read_configuration_file(file_path: str) -> Optional[Dict[str, str]]:
    """Read spark configuration file."""
    with open(file_path, "r") as f:
        lines = f.readlines()
        confs = {}
        for line in lines:
            if "=" in line:
                key_value = line.split("=")
                confs[key_value[0]] = key_value[1]
        return confs


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Handler for running a Kafka client")
    parser.add_argument(
        "-a",
        "--app-name",
        help="The name of the application",
        required=True,
        type=str,
        default="",
    )
    parser.add_argument(
        "-c",
        "--config",
        help="The configuration path.",
        type=str,
    )
    args = parser.parse_args()
    client = Client(field_manager=args.app_name)  # type: ignore
    label_selector = {"app.kubernetes.io/managed-by": "spark8t"}

    for op, sa in client.watch(ServiceAccount, namespace="*", labels=label_selector):
        print(f"Operation: {op}")
        print(f"Service Account: {sa}")
        sa_name = sa.metadata.name
        namespace = sa.metadata.namespace
        print(f"Name: {sa_name} --- namespace: {namespace}")
        #
        #
        options = read_configuration_file(args.config)
        if options:
            try:
                s = client.get(
                    Secret, name=f"configuration-hub-conf-{sa_name}", namespace=namespace
                )
                print(f"retrieved secrets: {s}")
                client.delete(
                    Secret, name=f"configuration-hub-conf-{sa_name}", namespace=namespace
                )
            except ApiError:
                pass
            print("HERE")
            s = Secret.from_dict(
                {
                    "apiVersion": "v1",
                    "kind": "Secret",
                    "metadata": {
                        "name": f"configuration-hub-conf-{sa_name}",
                        "namespace": namespace,
                    },
                    "stringData": options,
                }
            )
            client.create(s)

        #
        print("--------------------------------------------------------")
