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

from typing import Any, Iterable

import oras.client

from airflow.exceptions import AirflowException
from airflow.sdk import BaseHook


class OrasHook(BaseHook):
    """Interact with OCI registries via oras-py."""

    conn_name_attr = "oras_conn_id"
    default_conn_name = "oras_default"
    conn_type = "oras"
    hook_name = "ORAS"

    def __init__(
        self,
        oras_conn_id: str = default_conn_name,
        registry: str | None = None,
        insecure: bool | None = None,
        tls_verify: bool | str | None = None,
        auth_backend: str | None = None,
        config_path: str | None = None,
    ) -> None:
        super().__init__()
        self.oras_conn_id = oras_conn_id
        self.registry = registry
        self.insecure = insecure
        self.tls_verify = tls_verify
        self.auth_backend = auth_backend
        self.config_path = config_path

    def get_conn(self) -> oras.client.OrasClient:
        """Return an authenticated oras-py client."""
        return self.get_client()

    def get_client(self) -> oras.client.OrasClient:
        """Create an oras-py client using the Airflow connection."""
        conn = self.get_connection(self.oras_conn_id)
        extras = conn.extra_dejson or {}

        registry = self.registry or conn.host or extras.get("registry")
        if not registry:
            raise AirflowException("ORAS connection requires a registry host or 'registry' extra.")

        insecure = self._resolve_insecure(conn.schema, extras)
        tls_verify = self._resolve_tls_verify(extras)
        auth_backend = self.auth_backend or extras.get("auth_backend") or "token"

        client = oras.client.OrasClient(
            hostname=registry,
            insecure=insecure,
            tls_verify=tls_verify,
            auth_backend=auth_backend,
        )

        if conn.login or conn.password:
            if not conn.login or not conn.password:
                raise AirflowException(
                    "ORAS connection requires both login and password when using basic auth."
                )
            try:
                client.login(
                    username=conn.login,
                    password=conn.password,
                    hostname=registry,
                    tls_verify=tls_verify if isinstance(tls_verify, bool) else True,
                )
            except Exception as exc:
                raise AirflowException("Failed to authenticate to ORAS registry.") from exc

        return client

    def pull(
        self,
        *,
        target: str,
        outdir: str | None = None,
        allowed_media_type: list[str] | None = None,
        overwrite: bool = True,
        config_path: str | None = None,
    ) -> list[str]:
        """Pull an OCI artifact and return the downloaded file list."""
        client = self.get_client()
        return client.pull(
            target=target,
            outdir=outdir,
            allowed_media_type=allowed_media_type,
            overwrite=overwrite,
            config_path=config_path or self._get_config_path(),
        )

    def push(
        self,
        *,
        target: str,
        files: Iterable[str] | None = None,
        config_path: str | None = None,
        **kwargs: Any,
    ) -> Any:
        """Push files to a target OCI reference."""
        client = self.get_client()
        return client.push(
            target=target,
            files=list(files) if files else None,
            config_path=config_path or self._get_config_path(),
            **kwargs,
        )

    def _get_config_path(self) -> str | None:
        conn = self.get_connection(self.oras_conn_id)
        extras = conn.extra_dejson or {}
        return self.config_path or extras.get("config_path")

    def _resolve_insecure(self, schema: str | None, extras: dict) -> bool:
        override = self.insecure
        if override is None:
            override = self._as_bool(extras.get("insecure"))
        if override is None and schema:
            override = schema.lower() == "http"
        return bool(override) if override is not None else False

    def _resolve_tls_verify(self, extras: dict) -> bool | str:
        override = self.tls_verify
        if override is None:
            override = extras.get("tls_verify", True)
        return self._normalize_tls_verify(override)

    @staticmethod
    def _as_bool(value: Any) -> bool | None:
        if isinstance(value, bool):
            return value
        if value is None:
            return None
        if isinstance(value, (int, float)):
            return bool(value)
        if isinstance(value, str):
            lowered = value.strip().lower()
            if lowered in {"true", "1", "yes", "y"}:
                return True
            if lowered in {"false", "0", "no", "n"}:
                return False
        return None

    def _normalize_tls_verify(self, value: Any) -> bool | str:
        bool_value = self._as_bool(value)
        if bool_value is not None:
            return bool_value
        if isinstance(value, str):
            return value.strip()
        return True
