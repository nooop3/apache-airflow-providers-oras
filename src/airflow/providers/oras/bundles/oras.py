# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

from __future__ import annotations

import os
from pathlib import Path

import structlog

from airflow.dag_processing.bundles.base import BaseDagBundle
from airflow.exceptions import AirflowException
from airflow.providers.oras.hooks.oras import OrasHook


class OrasDagBundle(BaseDagBundle):
    """
    ORAS DAG bundle - exposes an OCI artifact as a DAG bundle.

    Materialize DAGs from OCI registry using ORAS.

    :param image: The OCI image reference with the DAG bundle.
    :param tag: Optional tag or digest to pull. If not provided, using the latest version.
    :param subdir: Optional subdirectory within the pulled artifact where the DAGs are located.
    :param oras_conn_id: Airflow connection ID for the ORAS registry.
    :param disable_refresh: When True, skip periodic refresh() calls after initialize().
    """

    supports_versioning = False

    def __init__(
        self,
        *,
        image: str,
        tag: str | None = None,
        subdir: str | None = None,
        oras_conn_id: str = OrasHook.default_conn_name,
        disable_refresh: bool = True,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.image = image
        self.tag = tag or "latest"
        self.subdir = subdir
        self.oras_conn_id = oras_conn_id
        self.disable_refresh = disable_refresh

        self.oras_dags_dir: Path = self.base_dir

        log = structlog.get_logger(__name__)
        self._log = log.bind(
            bundle_name=self.name,
            version=self.version,
            image=self.image,
            tag=self.tag,
            subdir=self.subdir,
            oras_conn_id=self.oras_conn_id,
        )
        self._oras_hook: OrasHook | None = None

    def _initialize(self) -> None:
        with self.lock():
            if not self.oras_dags_dir.exists():
                self._log.info("Creating local DAGs directory", path=self.oras_dags_dir)
                os.makedirs(self.oras_dags_dir)

            if not self.oras_dags_dir.is_dir():
                raise AirflowException(
                    f"Local DAGs path: {self.oras_dags_dir} is not a directory."
                )

            self._refresh(force=True)

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
            f"version={self.version!r}, "
            f"oras_conn_id={self.oras_conn_id!r}"
            f")>"
        )

    def get_current_version(self) -> str | None:
        """Return the current version of the DAG bundle. Currently not supported."""
        return None

    @property
    def path(self) -> Path:
        """Return the local path to the bundle."""
        if self.subdir:
            return self.oras_dags_dir / self.subdir
        return self.oras_dags_dir

    @property
    def oras_hook(self) -> OrasHook | None:
        if self._oras_hook is None:
            try:
                self._oras_hook = OrasHook(oras_conn_id=self.oras_conn_id)
            except AirflowException as exc:
                self._log.warning(
                    "Could not create OrasHook for connection %s: %s",
                    self.oras_conn_id,
                    exc,
                )
        return self._oras_hook

    def refresh(self) -> None:
        """Refresh the DAG bundles by re-pulling from the OCI registry."""
        self._refresh()

    def _refresh(self, *, force: bool = False) -> None:
        if self.version:
            raise AirflowException("Refreshing a specific version is not supported")
        if self.disable_refresh and not force:
            self._log.debug("Refresh disabled for bundle %s", self.name)
            return

        with self.lock():
            if self.oras_hook is None:
                raise AirflowException(
                    "ORAS hook is unavailable; cannot refresh DAG bundle."
                )
            image = self._ensure_image_has_hostname(self.image, self.oras_hook.hostname)
            self._log.debug(
                "Pulling DAG bundle from %s:%s to %s",
                image,
                self.tag,
                self.oras_dags_dir,
            )
            self.oras_hook.pull(
                target=f"{image}:{self.tag}",
                outdir=str(self.oras_dags_dir),
                overwrite=True,
            )

    @staticmethod
    def _ensure_image_has_hostname(image: str, hostname: str | None) -> str:
        if not hostname:
            raise AirflowException(
                "ORAS hostname is required to build image reference."
            )
        if image.startswith(f"{hostname}/"):
            return image
        if hostname in image:
            return image
        return f"{hostname}/{image}"

    def view_url_template(self) -> str | None:
        """Return a URL template to view the bundle in a registry web UI, if available."""
        if self.version:
            raise AirflowException("View URL for specific versions is not supported")
        if hasattr(self, "_view_url_template") and self._view_url_template:
            # Backward compatibility for Airflow 3.0 where view_url_template is new.
            return self._view_url_template
        url = f"https://{self.image}"
        return url

    def view_url(self, version: str | None = None) -> str | None:
        """
        Return a URL for viewing the bundle in a registry web UI.

        This method is deprecated and will be removed when the minimum supported Airflow version is 3.1.
        Use `view_url_template` instead.
        """
        if version:
            raise AirflowException("View URL for specific versions is not supported")
        return self.view_url_template()
