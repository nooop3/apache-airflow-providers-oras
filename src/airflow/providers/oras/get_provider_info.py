"""Provider metadata for Apache Airflow."""

from __future__ import annotations


def get_provider_info() -> dict:
    """Return provider info for Airflow."""
    return {
        "package-name": "apache-airflow-providers-oras",
        "name": "ORAS",
        "description": "[ORAS](https://oras.land/) - OCI Registry As Storage.",
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
        "bundles": [
            {
                "integration-name": "oras",
                "python-modules": "airflow.providers.oras.bundles.oras.OrasDagBundle",
            }
        ],
    }
