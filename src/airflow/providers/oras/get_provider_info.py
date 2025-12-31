"""Provider metadata for Apache Airflow."""

from __future__ import annotations

from airflow.providers.oras.__version__ import __version__


def get_provider_info() -> dict:
    """Return provider info for Airflow."""
    return {
        "package-name": "apache-airflow-providers-oras",
        "name": "ORAS",
        "description": "[ORAS](https://oras.land/) - OCI Registry As Storage.",
        "versions": [__version__],
        "integrations": [],
        "hooks": [
            {
                "integration-name": "ORAS",
                "python-modules": ["airflow.providers.oras.hooks.oras"],
            }
        ],
        "operators": [],
        "sensors": [],
        "connection-types": [
            {
                "connection-type": "oras",
                "hook-class-name": "airflow.providers.oras.hooks.oras.OrasHook",
            }
        ],
        "extra-links": [],
        "dag-bundles": [
            {
                "name": "oras",
                "class-name": "airflow.providers.oras.bundles.oras.OrasDagBundle",
            }
        ],
    }
