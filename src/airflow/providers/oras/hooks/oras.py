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

import json
import logging
import socket
from typing import Any, Iterable

import oras.client

from airflow.exceptions import AirflowException
from airflow.sdk import BaseHook

log = logging.getLogger(__name__)


class OrasHook(BaseHook):
    """
    Hook for OCI registries via oras-py.

    :param oras_conn_id: Connection ID for ORAS registry
    :param hostname: Optional registry host override
    :param insecure: Allow plain HTTP for registry
    :param tls_verify: TLS verification toggle or CA bundle path
    :param auth_backend: oras auth backend
    :param config_path: Optional oras config path
    """

    conn_name_attr = "oras_conn_id"
    default_conn_name = "oras_default"
    conn_type = "oras"
    hook_name = "ORAS"

    @classmethod
    def get_ui_field_behaviour(cls) -> dict[str, Any]:
        return {
            "hidden_fields": ["schema"],
            "relabeling": {
                "login": "Username",
                "host": "Registry",
                "password": "Password or Token",
            },
            "placeholders": {
                "extra": json.dumps(
                    {
                        "hostname": "registry.example.com",
                        "insecure": False,
                        "tls_verify": True,
                        "auth_backend": "token",
                        "config_path": "optional/path/to/config.json",
                    }
                )
            },
        }

    def __init__(
        self,
        oras_conn_id: str = default_conn_name,
        hostname: str | None = None,
        insecure: bool | None = None,
        tls_verify: bool | str | None = None,
        auth_backend: str | None = None,
        config_path: str | None = None,
    ) -> None:
        super().__init__()
        connection = self.get_connection(oras_conn_id)
        extras = connection.extra_dejson or {}

        self.oras_conn_id = oras_conn_id
        self._login = connection.login
        self._password = connection.password

        self.hostname = self._resolve_hostname(connection.host, extras, hostname)
        if not self.hostname:
            raise AirflowException(
                "ORAS connection requires a registry host or 'hostname' extra."
            )
        self.insecure = self._resolve_insecure(connection.schema, extras, insecure)
        self.tls_verify = self._resolve_tls_verify(extras, tls_verify)
        self.auth_backend = self._resolve_auth_backend(extras, auth_backend)
        self.config_path = self._resolve_config_path(extras, config_path)

    def get_conn(self) -> oras.client.OrasClient:
        """Return an authenticated oras-py client."""
        return self.get_client()

    def get_client(self) -> oras.client.OrasClient:
        """Create an oras-py client using the Airflow connection."""
        client = oras.client.OrasClient(
            hostname=self.hostname,
            insecure=bool(self.insecure) if self.insecure is not None else False,
            tls_verify=self.tls_verify,
            auth_backend=self.auth_backend,
        )

        if self._login or self._password:
            if not self._login or not self._password:
                raise AirflowException(
                    "ORAS connection requires both login and password when using basic auth."
                )
            try:
                client.login(
                    username=self._login,
                    password=self._password,
                    hostname=self.hostname,
                    tls_verify=self.tls_verify
                    if isinstance(self.tls_verify, bool)
                    else True,
                )
            except Exception as exc:
                raise AirflowException(
                    "Failed to authenticate to ORAS registry."
                ) from exc

        return client

    def test_connection(self) -> tuple[bool, str]:
        """Test ORAS connection by initializing the client and optional login."""
        try:
            if self.hostname:
                port = 80 if self.insecure else 443
                with socket.create_connection((self.hostname, port), timeout=5):
                    pass
            self.get_client()
        except Exception as exc:
            log.exception("ORAS connection test failed.")
            return False, f"Connection test failed: {exc}"
        return True, "Connection successfully tested."

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
            config_path=config_path or self.config_path,
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
            config_path=config_path or self.config_path,
            **kwargs,
        )

    @staticmethod
    def _resolve_hostname(
        host: str | None, extras: dict, override: str | None
    ) -> str | None:
        return override or host or extras.get("hostname")

    def _resolve_insecure(
        self, schema: str | None, extras: dict, override: bool | None
    ) -> bool:
        if override is None:
            override = self._as_bool(extras.get("insecure"))
        if override is None and schema:
            override = schema.lower() == "http"
        return bool(override) if override is not None else False

    def _resolve_tls_verify(
        self, extras: dict, override: bool | str | None
    ) -> bool | str:
        if override is None:
            override = extras.get("tls_verify", True)
        return self._normalize_tls_verify(override)

    @staticmethod
    def _resolve_auth_backend(extras: dict, override: str | None) -> str:
        return override or extras.get("auth_backend") or "token"

    @staticmethod
    def _resolve_config_path(extras: dict, override: str | None) -> str | None:
        return override or extras.get("config_path")

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
