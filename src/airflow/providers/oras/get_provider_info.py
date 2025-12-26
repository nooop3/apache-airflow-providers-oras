"""Provider metadata for Apache Airflow."""

from __future__ import annotations

from airflow.providers.oras.__version__ import __version__


def get_provider_info() -> dict:
    """Return provider info for Airflow."""
    return {
        "package-name": "apache-airflow-providers-oras",
        "name": "ORAS",
        "description": "DAG bundle backend for ORAS/OCI registries.",
        "versions": [__version__],
        "integrations": [],
        "hooks": [],
        "operators": [],
        "sensors": [],
        "asset-uris": [],
        "connection-types": [],
        "extra-links": [],
        "dag-bundles": [
            {
                "name": "oras",
                "class-name": "airflow.providers.oras.bundles.oras.OrasDagBundle",
            }
        ],
    }
