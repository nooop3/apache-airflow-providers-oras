"""ORAS-based DAG bundle backend."""

from __future__ import annotations

import os
import shutil
import time
from pathlib import Path

import structlog
from oras.provider import Registry

from airflow.dag_processing.bundles.base import BaseDagBundle
from airflow.exceptions import AirflowException

log = structlog.get_logger(__name__)


class OrasDagBundle(BaseDagBundle):
    """Materialize DAGs from OCI registry using ORAS."""

    def __init__(
        self,
        *,
        image: str,
        max_retries: int = 0,
        retry_delay: int = 5,
        env: dict[str, str] | None = None,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.image = image
        self.env = env or {}
        self.max_retries = max_retries
        self.retry_delay = retry_delay

        self.log = log.bind(bundle_name=self.name)

    @property
    def path(self) -> Path:
        """Return the local path to the bundle."""
        return self.base_dir

    def get_current_version(self) -> str:
        """Return the current version of the bundle (OCI Digest)."""
        registry = Registry()
        return registry.get_digest(self.image)

    def refresh(self) -> None:
        """Pull the OCI artifact to the local DAG folder."""
        bundle_path = self.path
        self._prepare_directory(bundle_path)

        registry = Registry()
        self.log.info("Pulling ORAS bundle", path=bundle_path, image=self.image)
        
        # Simple retry loop
        attempt = 0
        while True:
            try:
                registry.pull(target=self.image, outdir=str(bundle_path))
                return
            except Exception as exc:
                attempt += 1
                self.log.warning(
                    "ORAS pull failed",
                    error=str(exc),
                    attempt=attempt,
                    max_retries=self.max_retries + 1,
                )
                if attempt > self.max_retries:
                    raise AirflowException("ORAS pull failed after retries.") from exc
                time.sleep(self.retry_delay)
    
    @staticmethod
    def _prepare_directory(path: Path) -> None:
        if path.exists():
            shutil.rmtree(path)
        path.mkdir(parents=True, exist_ok=True)
