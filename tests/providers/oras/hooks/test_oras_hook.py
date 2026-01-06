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

import unittest
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

from airflow.exceptions import AirflowException
from airflow.providers.oras.hooks.oras import OrasHook


def _make_connection(
    *,
    host: str | None = None,
    schema: str | None = None,
    login: str | None = None,
    password: str | None = None,
    extras: dict | None = None,
) -> SimpleNamespace:
    return SimpleNamespace(
        host=host,
        schema=schema,
        login=login,
        password=password,
        extra_dejson=extras or {},
    )


class TestOrasHook(unittest.TestCase):
    @patch.object(OrasHook, "get_connection")
    def test_hostname_from_param(self, get_connection_mock: MagicMock) -> None:
        get_connection_mock.return_value = _make_connection(
            host="conn-host", extras={"hostname": "extras-host"}
        )
        hook = OrasHook(hostname="param-host")
        self.assertEqual(hook.hostname, "param-host")

    @patch.object(OrasHook, "get_connection")
    def test_hostname_from_connection(self, get_connection_mock: MagicMock) -> None:
        get_connection_mock.return_value = _make_connection(host="conn-host")
        hook = OrasHook()
        self.assertEqual(hook.hostname, "conn-host")

    @patch.object(OrasHook, "get_connection")
    def test_missing_hostname_raises(self, get_connection_mock: MagicMock) -> None:
        get_connection_mock.return_value = _make_connection()
        with self.assertRaises(AirflowException):
            OrasHook()

    @patch.object(OrasHook, "get_connection")
    def test_insecure_from_schema(self, get_connection_mock: MagicMock) -> None:
        get_connection_mock.return_value = _make_connection(host="host", schema="http")
        hook = OrasHook()
        self.assertTrue(hook.insecure)

    @patch.object(OrasHook, "get_connection")
    def test_insecure_override(self, get_connection_mock: MagicMock) -> None:
        get_connection_mock.return_value = _make_connection(host="host", schema="http")
        hook = OrasHook(insecure=False)
        self.assertFalse(hook.insecure)

    @patch.object(OrasHook, "get_connection")
    def test_tls_verify_from_extra(self, get_connection_mock: MagicMock) -> None:
        get_connection_mock.return_value = _make_connection(
            host="host", extras={"tls_verify": "/path/ca.pem"}
        )
        hook = OrasHook()
        self.assertEqual(hook.tls_verify, "/path/ca.pem")

    @patch.object(OrasHook, "get_connection")
    @patch("airflow.providers.oras.hooks.oras.oras.client.OrasClient")
    def test_get_client_login(self, client_mock: MagicMock, get_connection_mock: MagicMock) -> None:
        get_connection_mock.return_value = _make_connection(
            host="host",
            login="user",
            password="pass",
            extras={"tls_verify": "/path/ca.pem"},
        )
        client_instance = MagicMock()
        client_mock.return_value = client_instance

        hook = OrasHook()
        hook.get_client()

        client_mock.assert_called_once_with(
            hostname="host",
            insecure=False,
            tls_verify="/path/ca.pem",
            auth_backend="token",
        )
        client_instance.login.assert_called_once_with(
            username="user",
            password="pass",
            hostname="host",
            tls_verify=True,
        )

    @patch("airflow.providers.oras.hooks.oras.socket.create_connection")
    @patch("airflow.providers.oras.hooks.oras.log")
    @patch.object(OrasHook, "get_client")
    def test_test_connection_failure(
        self,
        get_client_mock: MagicMock,
        log_mock: MagicMock,
        create_connection_mock: MagicMock,
    ) -> None:
        get_client_mock.side_effect = AirflowException("boom")
        hook = OrasHook.__new__(OrasHook)
        hook.hostname = "registry.example.com"
        hook.insecure = False
        success, message = hook.test_connection()
        self.assertFalse(success)
        self.assertIn("Connection test failed", message)
        log_mock.exception.assert_called_once()

    @patch("airflow.providers.oras.hooks.oras.socket.create_connection")
    @patch.object(OrasHook, "get_client")
    def test_test_connection_success(
        self, get_client_mock: MagicMock, create_connection_mock: MagicMock
    ) -> None:
        get_client_mock.return_value = MagicMock()
        hook = OrasHook.__new__(OrasHook)
        hook.hostname = "registry.example.com"
        hook.insecure = False
        success, message = hook.test_connection()
        self.assertTrue(success)
        self.assertEqual(message, "Connection successfully tested.")
        create_connection_mock.assert_called_once_with(
            ("registry.example.com", 443), timeout=5
        )
