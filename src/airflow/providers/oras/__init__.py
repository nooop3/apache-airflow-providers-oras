"""Apache Airflow provider for ORAS."""

from __future__ import annotations

import packaging.version

from airflow import __version__ as airflow_version
from airflow.providers.oras.__version__ import __version__

__all__ = ["__version__"]

if packaging.version.parse(packaging.version.parse(airflow_version).base_version) < packaging.version.parse(
    "3.0.0"
):
    raise RuntimeError(
        f"The package `apache-airflow-providers-oras:{__version__}` needs Apache Airflow 3.0.0+"
    )
