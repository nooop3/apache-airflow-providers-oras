import os
import sys
import types
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SRC_DIR = ROOT / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))


def install_fake_airflow() -> None:
    if "airflow" in sys.modules:
        return

    airflow = types.ModuleType("airflow")
    airflow.__version__ = "3.0.0"
    airflow.__path__ = [str(SRC_DIR / "airflow")]

    exceptions = types.ModuleType("airflow.exceptions")

    class AirflowException(Exception):
        pass

    exceptions.AirflowException = AirflowException

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

    # Mock structlog
    structlog = types.ModuleType("structlog")

    class MockLogger:
        def bind(self, **kwargs):
            return self

        def info(self, *args, **kwargs):
            pass

        def warning(self, *args, **kwargs):
            pass

        def debug(self, *args, **kwargs):
            pass

    def get_logger(name):
        return MockLogger()

    structlog.get_logger = get_logger
    sys.modules["structlog"] = structlog

    # Mock hooks
    hooks = types.ModuleType("airflow.hooks")
    hooks_base = types.ModuleType("airflow.hooks.base")

    class FakeBaseHook:
        conn_name_attr = "conn_id"
        default_conn_name = "default"
        conn_type = "generic"
        hook_name = "Hook"

        def __init__(self, *args, **kwargs):
            self.log = MockLogger()

        @classmethod
        def get_connection(cls, conn_id):
            raise NotImplementedError("Override get_connection in tests.")

    hooks_base.BaseHook = FakeBaseHook
    hooks.base = hooks_base

    # Mock models/connection
    models = types.ModuleType("airflow.models")
    connection = types.ModuleType("airflow.models.connection")

    class FakeConnection:
        def __init__(
            self,
            host=None,
            schema=None,
            login=None,
            password=None,
            extra_dejson=None,
        ):
            self.host = host
            self.schema = schema
            self.login = login
            self.password = password
            self.extra_dejson = extra_dejson or {}

    connection.Connection = FakeConnection
    models.connection = connection

    # Mock dag_processing.bundles.base
    dag_processing = types.ModuleType("airflow.dag_processing")
    bundles = types.ModuleType("airflow.dag_processing.bundles")
    base = types.ModuleType("airflow.dag_processing.bundles.base")

    class FakeDagBundle:
        def __init__(self, **kwargs):
            self.name = kwargs.get("name")
            self.version = kwargs.get("version")
            airflow_home = os.environ.get("AIRFLOW_HOME", os.path.expanduser("~/airflow"))
            self.base_dir = Path(airflow_home) / "dag_bundles" / self.name
            self.versions_dir = self.base_dir / "versions"

        def lock(self):
            return nullcontext()

        def initialize(self) -> None:
            pass

    base.BaseDagBundle = FakeDagBundle
    bundles.base = base
    dag_processing.bundles = bundles

    airflow.dag_processing = dag_processing
    airflow.exceptions = exceptions
    airflow.hooks = hooks
    airflow.models = models

    sys.modules["airflow"] = airflow
    sys.modules["airflow.exceptions"] = exceptions
    sys.modules["airflow.dag_processing"] = dag_processing
    sys.modules["airflow.dag_processing.bundles"] = bundles
    sys.modules["airflow.dag_processing.bundles.base"] = base
    sys.modules["airflow.hooks"] = hooks
    sys.modules["airflow.hooks.base"] = hooks_base
    sys.modules["airflow.models"] = models
    sys.modules["airflow.models.connection"] = connection

    # Mock oras
    oras = types.ModuleType("oras")
    oras_client = types.ModuleType("oras.client")
    oras_auth = types.ModuleType("oras.auth")

    class MockOrasClient:
        def __init__(self, *args, **kwargs):
            pass

        def login(self, *args, **kwargs):
            return {}

        def pull(self, *args, **kwargs):
            return []

        def push(self, *args, **kwargs):
            return {}

    def get_auth_backend(*args, **kwargs):
        return object()

    oras_client.OrasClient = MockOrasClient
    oras_auth.get_auth_backend = get_auth_backend

    oras.client = oras_client
    oras.auth = oras_auth
    sys.modules["oras"] = oras
    sys.modules["oras.client"] = oras_client
    sys.modules["oras.auth"] = oras_auth


class nullcontext:
    def __init__(self, result=None):
        self.result = result

    def __enter__(self):
        return self.result

    def __exit__(self, exc_type, exc, exc_tb):
        return False
