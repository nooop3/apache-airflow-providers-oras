import unittest
from unittest import mock

from tests._fakes import install_fake_airflow

install_fake_airflow()

from airflow.exceptions import AirflowException
from airflow.hooks.base import BaseHook
from airflow.models.connection import Connection
from airflow.providers.oras.hooks.oras import OrasHook


class TestOrasHook(unittest.TestCase):
    @mock.patch("airflow.providers.oras.hooks.oras.oras.client.OrasClient")
    def test_get_client_uses_connection_fields(self, mock_client_cls):
        conn = Connection(
            host="registry.example.com",
            schema="https",
            login="user",
            password="pass",
            extra_dejson={"auth_backend": "token", "tls_verify": "false"},
        )
        with mock.patch.object(BaseHook, "get_connection", return_value=conn):
            hook = OrasHook()
            client = hook.get_client()

        self.assertIs(client, mock_client_cls.return_value)
        mock_client_cls.assert_called_with(
            hostname="registry.example.com",
            insecure=False,
            tls_verify=False,
            auth_backend="token",
        )
        mock_client_cls.return_value.login.assert_called_with(
            username="user",
            password="pass",
            hostname="registry.example.com",
            tls_verify=False,
        )

    def test_get_client_requires_registry(self):
        conn = Connection(host=None, extra_dejson={})
        with mock.patch.object(BaseHook, "get_connection", return_value=conn):
            hook = OrasHook()
            with self.assertRaises(AirflowException):
                hook.get_client()

    @mock.patch("airflow.providers.oras.hooks.oras.oras.client.OrasClient")
    def test_pull_uses_config_path(self, mock_client_cls):
        conn = Connection(
            host="registry.example.com",
            extra_dejson={"config_path": "/tmp/oras.json"},
        )
        with mock.patch.object(BaseHook, "get_connection", return_value=conn):
            hook = OrasHook()
            hook.pull(target="registry.example.com/demo:latest", outdir="/tmp/out")

        mock_client_cls.return_value.pull.assert_called_with(
            target="registry.example.com/demo:latest",
            outdir="/tmp/out",
            allowed_media_type=None,
            overwrite=True,
            config_path="/tmp/oras.json",
        )


if __name__ == "__main__":
    unittest.main()
