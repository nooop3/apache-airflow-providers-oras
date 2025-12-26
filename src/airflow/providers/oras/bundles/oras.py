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

    def __init__(self, name: str, config: Mapping[str, object]):
        super().__init__(name, config)
        self._config = dict(config)
        self._image = self._require_str("image")
        self._oras_cmd = self._coerce_str(self._config.get("oras_cmd", "oras"), "oras_cmd")
        self._pull_args = self._coerce_str_sequence(self._config.get("pull_args", []))
        self._bundle_root = self._config.get("bundle_root")
        self._max_retries = self._coerce_non_negative_int(
            self._config.get("max_retries", 0), "max_retries"
        )
        self._retry_delay = self._coerce_non_negative_int(
            self._config.get("retry_delay", 5), "retry_delay"
        )
        self._env = self._coerce_env(self._config.get("env", {}))
        self._validate_oras_cmd()

    def _validate_oras_cmd(self) -> None:
        if not shutil.which(self._oras_cmd):
            raise AirflowException(
                f"The command '{self._oras_cmd}' was not found. "
                "Please ensure it is installed and in your PATH."
            )

    def refresh(self) -> str:
        """Pull the OCI artifact and return the local DAG folder path."""
        bundle_path = self._resolve_bundle_path()
        self._prepare_directory(bundle_path)

        command = self._build_pull_command(bundle_path)
        env = os.environ.copy()
        env.update(self._env)

        self.log.info("Pulling ORAS bundle into %s", bundle_path)
        self._run_with_retries(command, env)

        return str(bundle_path)

    def _require_str(self, key: str) -> str:
        value = self._config.get(key)
        if not isinstance(value, str) or not value.strip():
            raise AirflowException(f"Config value '{key}' must be a non-empty string.")
        return value

    @staticmethod
    def _coerce_str(value: object, key: str) -> str:
        if isinstance(value, str) and value.strip():
            return value
        raise AirflowException(f"Config value '{key}' must be a non-empty string.")

    @staticmethod
    def _coerce_str_sequence(value: object) -> list[str]:
        if value is None:
            return []
        if isinstance(value, (list, tuple)):
            if not all(isinstance(item, str) for item in value):
                raise AirflowException("pull_args must be a list of strings.")
            return list(value)
        raise AirflowException("pull_args must be a list of strings.")

    @staticmethod
    def _coerce_env(value: object) -> dict[str, str]:
        if value is None:
            return {}
        if isinstance(value, dict):
            env = {}
            for key, val in value.items():
                if not isinstance(key, str) or not isinstance(val, str):
                    raise AirflowException("env must be a mapping of string keys to string values.")
                env[key] = val
            return env
        raise AirflowException("env must be a mapping of string keys to string values.")

    @staticmethod
    def _coerce_non_negative_int(value: object, key: str) -> int:
        try:
            parsed = int(value)
        except (TypeError, ValueError) as exc:
            raise AirflowException(f"Config value '{key}' must be an integer.") from exc
        if parsed < 0:
            raise AirflowException(f"Config value '{key}' must be non-negative.")
        return parsed

    def _resolve_bundle_path(self) -> Path:
        if self._bundle_root:
            root = Path(str(self._bundle_root))
        else:
            airflow_home = os.environ.get("AIRFLOW_HOME", os.path.expanduser("~/airflow"))
            root = Path(airflow_home) / "dag_bundles" / "oras"
        return root / self.name

    def _build_pull_command(self, output_dir: Path) -> list[str]:
        command = [str(self._oras_cmd), "pull"]
        command.extend(self._pull_args)
        command.extend([self._image, "--output", str(output_dir)])
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
                if attempt > self._max_retries:
                    raise AirflowException("ORAS pull failed after retries.") from exc
                time.sleep(self._retry_delay)
