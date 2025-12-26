"""ORAS-based DAG bundle backend."""

from __future__ import annotations

import os
import shutil
import subprocess
import time
from pathlib import Path
from typing import Mapping, Sequence

from airflow.dag_processing.bundles.base import BaseDagBundle
from airflow.exceptions import AirflowException


class OrasDagBundle(BaseDagBundle):
    """Materialize DAGs from an OCI registry using ORAS."""

    def __init__(
        self,
        *,
        image: str,
        oras_cmd: str = "oras",
        pull_args: list[str] | None = None,
        max_retries: int = 0,
        retry_delay: int = 5,
        env: dict[str, str] | None = None,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.image = image
        self.oras_cmd = oras_cmd
        self.pull_args = pull_args or []
        self.env = env or {}
        self.max_retries = max_retries
        self.retry_delay = retry_delay

        self._validate_oras_cmd()

    @property
    def path(self) -> Path:
        """Return the local path to the bundle."""
        return self.base_dir

    def get_current_version(self) -> str | None:
        """Return the current version of the bundle."""
        return self.version

    def _validate_oras_cmd(self) -> None:
        if not shutil.which(self.oras_cmd):
            raise AirflowException(
                f"The command '{self.oras_cmd}' was not found. "
                "Please ensure it is installed and in your PATH."
            )

    def refresh(self) -> None:
        """Pull the OCI artifact to the local DAG folder."""
        bundle_path = self.path
        self._prepare_directory(bundle_path)

        command = self._build_pull_command(bundle_path)
        env = os.environ.copy()
        env.update(self.env)

        self.log.info("Pulling ORAS bundle into %s", bundle_path)
        self._run_with_retries(command, env)

    def _build_pull_command(self, output_dir: Path) -> list[str]:
        command = [self.oras_cmd, "pull"]
        command.extend(self.pull_args)
        command.extend([self.image, "--output", str(output_dir)])
        return command

    @staticmethod
    def _prepare_directory(path: Path) -> None:
        if path.exists():
            shutil.rmtree(path)
        path.mkdir(parents=True, exist_ok=True)

    def _run_with_retries(self, command: Sequence[str], env: Mapping[str, str]) -> None:
        attempt = 0
        while True:
            try:
                subprocess.run(
                    list(command),
                    check=True,
                    env=env,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                    text=True,
                )
                return
            except subprocess.CalledProcessError as exc:
                attempt += 1
                self.log.warning(
                    "ORAS pull failed with exit code %s (attempt %s/%s).",
                    exc.returncode,
                    attempt,
                    self._max_retries + 1,
                )
                if attempt > self.max_retries:
                    raise AirflowException("ORAS pull failed after retries.") from exc
                time.sleep(self.retry_delay)
