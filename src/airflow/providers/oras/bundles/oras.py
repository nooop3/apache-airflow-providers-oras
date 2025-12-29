"""ORAS-based DAG bundle backend."""

from __future__ import annotations

import os
from pathlib import Path

import oras.client
import oras.auth
import structlog

from airflow.dag_processing.bundles.base import BaseDagBundle
from airflow.exceptions import AirflowException

log = structlog.get_logger(__name__)


class OrasDagBundle(BaseDagBundle):
    """
    ORAS DAG bundle - exposes an OCI artifact as a DAG bundle.

    Materialize DAGs from OCI registry using ORAS.

    :param image: The OCI image reference with the DAG bundle.
    :param tag: Optional tag or digest to pull. If not provided, the latest version
    :param subdir: Optional subdirectory within the pulled artifact where the DAGs are located.
    """

    supports_versioning = False

    def __init__(
        self,
        *,
        image: str,
        tag: str | None = None,
        subdir: str | None = None,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.image = image
        self.tag = tag or "latest"
        self.subdir = subdir

        self.oras_dags_dir: Path = self.base_dir
        self.oras_client = oras.client.OrasClient()

        self._log = log.bind(
            bundle_name=self.name,
            version=self.version,
            oras_dags_dir=self.oras_dags_dir,
            image=self.image,
            tag=self.tag,
            subdir=self.subdir,
        )
        self._log.debug("bundle configured")

    def _initialize(self) -> None:
        with self.lock():
            if not self.oras_dags_dir.exists():
                self._log.info("Creating local DAGs directory", path=self.oras_dags_dir)
                os.makedirs(self.oras_dags_dir)

            if not self.oras_dags_dir.is_dir():
                raise AirflowException(
                    f"Local DAGs path: {self.oras_dags_dir} is not a directory."
                )

            # TODO: check we can reach the registry
            auth_backend = oras.auth.get_auth_backend()
            if not auth_backend:
                raise AirflowException(
                    f"Cannot get auth backend to reach OCI registry for image {self.image}"
                )

            self.refresh()

    def initialize(self) -> None:
        self._initialize()
        super().initialize()

    def __repr__(self):
        return (
            f"<OrasDagBundle("
            f"name={self.name!r}, "
            f"image={self.image!r}, "
            f"tag={self.tag!r}, "
            f"subdir={self.subdir!r}, "
            f"version={self.version!r}"
            f"oras_dags_dir={self.oras_dags_dir!r}, "
            f")>"
        )

    def get_current_version(self) -> str | None:
        """Return the current version of the DAG bundle. Currently not supported."""
        return None

    @property
    def path(self) -> Path:
        """Return the local path to the bundle."""
        return self.oras_dags_dir

    def refresh(self) -> None:
        """Refresh the DAG bundles by re-pulling from the OCI registry."""
        if self.version:
            raise AirflowException("Refreshing a specific version is not supported")

        with self.lock():
            self._log.info("Refreshing bundle", path=self.oras_dags_dir)
            # TODO: refresh logic
            self.oras_client.pull(
                target=f"{self.image}:{self.tag}",
                outdir=str(self.oras_dags_dir),
            )

    def view_url_template(self) -> str | None:
        """Return a URL template to view the bundle in a registry web UI, if available."""
        if self.version:
            raise AirflowException("View URL for specific versions is not supported")
        url = f"https://{self.image}"
        return url
