import os
import sys
import types
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest import mock

ROOT = Path(__file__).resolve().parents[1]
SRC_DIR = ROOT / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))


def _install_fake_airflow() -> None:
    if "airflow" in sys.modules:
        return

    airflow = types.ModuleType("airflow")
    airflow.__version__ = "3.0.0"
    airflow.__path__ = [str(SRC_DIR / "airflow")]
    exceptions = types.ModuleType("airflow.exceptions")

    # Mock packaging
    packaging = types.ModuleType("packaging")
    version = types.ModuleType("packaging.version")

    class MockVersion:
        def __init__(self, v_str):
            self.base_version = v_str

        def __lt__(self, other):
            return self.base_version < other.base_version

    version.parse = MockVersion
    packaging.version = version
    sys.modules["packaging"] = packaging
    sys.modules["packaging.version"] = version

    class AirflowException(Exception):
        pass

    exceptions.AirflowException = AirflowException

    # Mock structlog
    structlog = types.ModuleType("structlog")

    class MockLogger:
        def bind(self, **kwargs):
            return self

        def info(self, *args, **kwargs):
            pass

        def warning(self, *args, **kwargs):
            pass

    def get_logger(name):
        return MockLogger()

    structlog.get_logger = get_logger
    sys.modules["structlog"] = structlog

    # Mock oras
    oras = types.ModuleType("oras")
    oras_provider = types.ModuleType("oras.provider")
    
    class MockRegistry:
        def pull(self, target, outdir):
            pass
        def get_digest(self, target):
            return "sha256:mockdigest"

    oras_provider.Registry = MockRegistry
    oras.provider = oras_provider
    sys.modules["oras"] = oras
    sys.modules["oras.provider"] = oras_provider

    # Mock dag_processing.bundles.base
    dag_processing = types.ModuleType("airflow.dag_processing")
    bundles = types.ModuleType("airflow.dag_processing.bundles")
    base = types.ModuleType("airflow.dag_processing.bundles.base")

    class FakeDagBundle:
        def __init__(self, **kwargs):
            self.name = kwargs.get("name")
            self.version = kwargs.get("version")
            # Mimic BaseDagBundle calculation (simplified for test)
            airflow_home = os.environ.get("AIRFLOW_HOME", os.path.expanduser("~/airflow"))
            self.base_dir = Path(airflow_home) / "dag_bundles" / self.name
            import logging
            self.log = logging.getLogger("fake")

    base.BaseDagBundle = FakeDagBundle
    bundles.base = base
    dag_processing.bundles = bundles

    airflow.dag_processing = dag_processing
    airflow.exceptions = exceptions

    sys.modules["airflow"] = airflow
    sys.modules["airflow.exceptions"] = exceptions
    sys.modules["airflow.dag_processing"] = dag_processing
    sys.modules["airflow.dag_processing.bundles"] = bundles
    sys.modules["airflow.dag_processing.bundles.base"] = base


_install_fake_airflow()

from airflow.providers.oras.bundles.oras import OrasDagBundle


class TestOrasDagBundle(unittest.TestCase):

    def test_init(self):
        bundle = OrasDagBundle(name="oras_test", image="example.com/demo:1")
        self.assertEqual(bundle.image, "example.com/demo:1")

    def test_path_property_uses_base_dir(self):
        with TemporaryDirectory() as tmp:
            old_home = os.environ.get("AIRFLOW_HOME")
            os.environ["AIRFLOW_HOME"] = tmp
            try:
                bundle = OrasDagBundle(name="oras_test", image="example.com/demo:1")
                # Our fake base class sets base_dir to AIRFLOW_HOME/dag_bundles/name
                expected = Path(tmp) / "dag_bundles" / "oras_test"
                self.assertEqual(bundle.path, expected)
            finally:
                if old_home is None:
                    os.environ.pop("AIRFLOW_HOME", None)
                else:
                    os.environ["AIRFLOW_HOME"] = old_home

    @mock.patch("airflow.providers.oras.bundles.oras.Registry")
    def test_get_current_version_returns_digest(self, mock_registry_cls):
        mock_registry = mock_registry_cls.return_value
        mock_registry.get_digest.return_value = "sha256:12345"
        
        bundle = OrasDagBundle(name="oras_test", image="example.com/demo:1")
        version = bundle.get_current_version()
        
        self.assertEqual(version, "sha256:12345")
        mock_registry.get_digest.assert_called_with("example.com/demo:1")

    @mock.patch("airflow.providers.oras.bundles.oras.Registry")
    def test_refresh_pulls_artifact(self, mock_registry_cls):
        mock_registry = mock_registry_cls.return_value
        
        with TemporaryDirectory() as tmp:
            old_home = os.environ.get("AIRFLOW_HOME")
            os.environ["AIRFLOW_HOME"] = tmp
            try:
                bundle = OrasDagBundle(name="oras_test", image="example.com/demo:1")
                bundle.refresh()
                
                target_path = Path(tmp) / "dag_bundles" / "oras_test"
                mock_registry.pull.assert_called_with(
                    target="example.com/demo:1",
                    outdir=str(target_path)
                )
            finally:
                if old_home is None:
                    os.environ.pop("AIRFLOW_HOME", None)
                else:
                    os.environ["AIRFLOW_HOME"] = old_home

if __name__ == "__main__":
    unittest.main()
