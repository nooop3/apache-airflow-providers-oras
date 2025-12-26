"""ORAS-based DAG bundle backend."""

from __future__ import annotations

import os
import time
from pathlib import Path

import structlog
from oras.provider import Registry

from airflow.dag_processing.bundles.base import BaseDagBundle
from airflow.exceptions import AirflowException

log = structlog.get_logger(__name__)


class OrasDagBundle(BaseDagBundle):
    """Materialize DAGs from OCI registry using ORAS."""

    supports_versioning = True

    def __init__(
        self,
        *,
        image: str,
        tag: str | None = None,
        subdir: str | None = None,
        max_retries: int = 0,
        retry_delay: int = 5,
        env: dict[str, str] | None = None,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.image = image
        self.tag = tag
        self.subdir = subdir
        self.env = env or {}
        self.max_retries = max_retries
        self.retry_delay = retry_delay

        if self.version:
            self.download_path = self.versions_dir / self.version
        else:
            self.download_path = self.base_dir / "latest"

        self._log = log.bind(
            bundle_name=self.name,
            version=self.version,
            versions_path=self.versions_dir,
            download_path=self.download_path,
        )
        self._log.debug("bundle configured")

    def __repr__(self):
        return (
            f"<OrasDagBundle("
            f"name={self.name!r}, "
            f"version={self.version!r}"
            f"versions_path={self.versions_dir!r}, "
            f"download_path={self.download_path!r}, "
            f"subdir={self.subdir!r}, "
            f")>"
        )

    @property
    def path(self) -> Path:
        """Return the local path to the bundle."""
        if self.subdir:
            return self.download_path / self.subdir
        return self.download_path

    def get_current_version(self) -> str:
        """Return the current version of the bundle (OCI Digest)."""
        registry = Registry()
        self._authenticate(registry)

        # HEAD request to get digest from headers
        # We need to handle potential 401/403 here? Registry.do_request handles it?
        # get_manifest logic in oras-py usually does a GET.
        # We want HEAD for efficiency if supported.
        try:
            # Look up the tag -> digest
            # Using internal method or recreating logic?
            # registry.get_manifest returns dict.
            # We can use registry.get_tags? No.
            # Let's use `do_request` to get manifest headers.
            # Depending on registry support, HEAD might not work for manifest.
            # Fallback to GET if needed.
            # Docker-Content-Digest header is standard.
            headers = {
                "Accept": "application/vnd.oci.image.manifest.v1+json,application/vnd.docker.distribution.manifest.v2+json"
            }
            response = registry.do_request(self.image, "HEAD", headers=headers)
            digest = response.headers.get("Docker-Content-Digest")
            if digest:
                return digest

            # Fallback to GET if HEAD didn't return digest
            response = registry.do_request(self.image, "GET", headers=headers)
            digest = response.headers.get("Docker-Content-Digest")
            if digest:
                return digest

            raise AirflowException(f"Could not determine digest for {self.image}")
        except Exception as e:
            raise AirflowException(
                f"Failed to get current version for {self.image}: {e}"
            ) from e

    def refresh(self) -> None:
        """Pull the OCI artifact to the local DAG folder."""
        bundle_path = self.path

        # If versioned and exists, we might skip?
        # User said "should not remove the exist directory".
        # But if it exists and corrupt?
        # For now, let's assume if it exists it's good for immutable versions.
        if self.version and bundle_path.exists() and any(bundle_path.iterdir()):
            self._log.info(
                "Version already exists, skipping pull",
                path=bundle_path,
                version=self.version,
            )
            return

        self._prepare_directory(bundle_path)

        registry = Registry()
        self._authenticate(registry)

        target = f"{self.image}@{self.version}" if self.version else self.image

        self._log.info("Pulling ORAS bundle", path=bundle_path, target=target)

        attempt = 0
        while True:
            try:
                registry.pull(target=target, outdir=str(bundle_path))
                return
            except Exception as exc:
                attempt += 1
                self._log.warning(
                    "ORAS pull failed",
                    error=str(exc),
                    attempt=attempt,
                    max_retries=self.max_retries + 1,
                )
                if attempt > self.max_retries:
                    raise AirflowException("ORAS pull failed after retries.") from exc
                time.sleep(self.retry_delay)

    def _authenticate(self, registry: Registry) -> None:
        # Simple auth from env if present
        username = self.env.get("ORAS_USER") or os.environ.get("ORAS_USER")
        password = self.env.get("ORAS_PASS") or os.environ.get("ORAS_PASS")
        if username and password:
            registry.login(username, password)

    @staticmethod
    def _prepare_directory(path: Path) -> None:
        # Don't ensure empty if tracking?
        # User said "should not remove the exist directory".
        # If tracking, we might be overwriting. oras pull handles overwrite usually.
        # So we just ensure dir exists.
        path.mkdir(parents=True, exist_ok=True)
