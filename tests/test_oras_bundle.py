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
    airflow.__path__ = [str(SRC_DIR / "airflow")]
    exceptions = types.ModuleType("airflow.exceptions")

    class AirflowException(Exception):
        pass

    exceptions.AirflowException = AirflowException

    dag_bundles = types.ModuleType("airflow.dag_bundles")
    base = types.ModuleType("airflow.dag_bundles.base")

    class FakeDagBundle:
        def __init__(self, name, config):
            self.name = name
            self.config = config
            import logging

            self.log = logging.getLogger("fake")

    base.DagBundle = FakeDagBundle
    base.BaseDagBundle = FakeDagBundle
    dag_bundles.base = base

    airflow.dag_bundles = dag_bundles
    airflow.exceptions = exceptions

    sys.modules["airflow"] = airflow
    sys.modules["airflow.exceptions"] = exceptions
    sys.modules["airflow.dag_bundles"] = dag_bundles
    sys.modules["airflow.dag_bundles.base"] = base


_install_fake_airflow()

from airflow.providers.oras.bundles.oras import OrasDagBundle


class TestOrasDagBundle(unittest.TestCase):
    def setUp(self):
        self.which_patcher = mock.patch("airflow.providers.oras.bundles.oras.shutil.which")
        self.mock_which = self.which_patcher.start()
        self.mock_which.return_value = "/usr/bin/oras"

    def tearDown(self):
        self.which_patcher.stop()

    def test_init_raises_if_oras_not_found(self):
        self.mock_which.return_value = None
        with self.assertRaisesRegex(
            Exception, "The command 'oras' was not found"
        ):  # AirflowException is strictly checked in some envs
            OrasDagBundle("oras_test", {"image": "example.com/demo:1"})

    def test_resolve_bundle_path_defaults_to_airflow_home(self):
        with TemporaryDirectory() as tmp:
            old_home = os.environ.get("AIRFLOW_HOME")
            os.environ["AIRFLOW_HOME"] = tmp
            try:
                bundle = OrasDagBundle("oras_test", {"image": "example.com/demo:1"})
                expected = Path(tmp) / "dag_bundles" / "oras" / "oras_test"
                self.assertEqual(bundle._resolve_bundle_path(), expected)
            finally:
                if old_home is None:
                    os.environ.pop("AIRFLOW_HOME", None)
                else:
                    os.environ["AIRFLOW_HOME"] = old_home

    def test_build_pull_command(self):
        bundle = OrasDagBundle(
            "oras_test",
            {"image": "example.com/demo:1", "pull_args": ["--plain-http"]},
        )
        command = bundle._build_pull_command(Path("/tmp/out"))
        self.assertEqual(
            command,
            ["oras", "pull", "--plain-http", "example.com/demo:1", "--output", "/tmp/out"],
        )

    def test_refresh_runs_oras_and_returns_path(self):
        with TemporaryDirectory() as tmp:
            old_home = os.environ.get("AIRFLOW_HOME")
            os.environ["AIRFLOW_HOME"] = tmp
            try:
                bundle = OrasDagBundle("oras_test", {"image": "example.com/demo:1"})
                target = Path(tmp) / "dag_bundles" / "oras" / "oras_test"
                target.mkdir(parents=True)
                (target / "old.py").write_text("print('old')", encoding="ascii")

                with mock.patch(
                    "airflow.providers.oras.bundles.oras.subprocess.run"
                ) as run:
                    run.return_value = mock.Mock(returncode=0, stdout="", stderr="")
                    result = bundle.refresh()

                self.assertEqual(result, str(target))
                self.assertTrue(target.exists())
                self.assertFalse((target / "old.py").exists())
                run.assert_called_once()
            finally:
                if old_home is None:
                    os.environ.pop("AIRFLOW_HOME", None)
                else:
                    os.environ["AIRFLOW_HOME"] = old_home


if __name__ == "__main__":
    unittest.main()
