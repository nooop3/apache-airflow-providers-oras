import os
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest import mock

from tests._fakes import install_fake_airflow


install_fake_airflow()

from airflow.providers.oras.bundles.oras import OrasDagBundle


class TestOrasDagBundle(unittest.TestCase):
    def test_init(self):
        bundle = OrasDagBundle(name="oras_test", image="example.com/demo")
        self.assertEqual(bundle.image, "example.com/demo")
        self.assertEqual(bundle.tag, "latest")

    def test_get_current_version_returns_none(self):
        bundle = OrasDagBundle(name="oras_test", image="example.com/demo")
        version = bundle.get_current_version()
        self.assertIsNone(version)

    @mock.patch("airflow.providers.oras.bundles.oras.oras.client.OrasClient")
    def test_refresh_pulls_artifact(self, mock_registry_cls):
        mock_registry = mock_registry_cls.return_value

        with TemporaryDirectory() as tmp:
            old_home = os.environ.get("AIRFLOW_HOME")
            os.environ["AIRFLOW_HOME"] = tmp
            try:
                bundle = OrasDagBundle(name="oras_test", image="example.com/demo")
                bundle.refresh()

                target_path = Path(tmp) / "dag_bundles" / "oras_test"
                mock_registry.pull.assert_called_with(
                    target="example.com/demo:latest",
                    outdir=str(target_path)
                )

                mock_registry.reset_mock()
                bundle_v = OrasDagBundle(
                    name="oras_test", image="example.com/demo", version="sha256:12345"
                )
                with self.assertRaisesRegex(
                    Exception, "Refreshing a specific version is not supported"
                ):
                    bundle_v.refresh()

            finally:
                if old_home is None:
                    os.environ.pop("AIRFLOW_HOME", None)
                else:
                    os.environ["AIRFLOW_HOME"] = old_home

    def test_path_property(self):
        with TemporaryDirectory() as tmp:
            old_home = os.environ.get("AIRFLOW_HOME")
            os.environ["AIRFLOW_HOME"] = tmp
            try:
                bundle = OrasDagBundle(name="oras_test", image="example.com/demo")
                expected = Path(tmp) / "dag_bundles" / "oras_test"
                self.assertEqual(bundle.path, expected)
            finally:
                if old_home is None:
                    os.environ.pop("AIRFLOW_HOME", None)
                else:
                    os.environ["AIRFLOW_HOME"] = old_home

if __name__ == "__main__":
    unittest.main()
