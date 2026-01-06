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

import tempfile
import unittest
from contextlib import contextmanager
from pathlib import Path
from unittest.mock import MagicMock, PropertyMock, patch

from airflow.exceptions import AirflowException

from airflow.providers.oras.bundles.oras import OrasDagBundle


@contextmanager
def _noop_lock(_self):
    yield


class TestOrasDagBundle(unittest.TestCase):
    def _make_bundle(self, tmp_path: Path, **kwargs) -> OrasDagBundle:
        with (
            patch(
                "airflow.dag_processing.bundles.base.get_bundle_base_folder",
                return_value=tmp_path,
            ),
            patch(
                "airflow.dag_processing.bundles.base.get_bundle_versions_base_folder",
                return_value=tmp_path / "versions",
            ),
        ):
            kwargs.setdefault("image", "registry.example.com/dags")
            return OrasDagBundle(name="oras_bundle", **kwargs)

    def test_path_without_subdir(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            bundle = self._make_bundle(Path(tmpdir))
            self.assertEqual(bundle.path, Path(tmpdir))

    def test_path_with_subdir(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            bundle = self._make_bundle(Path(tmpdir), subdir="dags")
            self.assertEqual(bundle.path, Path(tmpdir) / "dags")

    def test_refresh_calls_oras_hook(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            bundle = self._make_bundle(Path(tmpdir), tag="v1", disable_refresh=False)
            hook = MagicMock()
            hook.hostname = "registry.example.com"
            with (
                patch.object(OrasDagBundle, "lock", _noop_lock),
                patch.object(
                    OrasDagBundle,
                    "oras_hook",
                    new_callable=PropertyMock,
                    return_value=hook,
                ),
            ):
                bundle.refresh()
            hook.pull.assert_called_once_with(
                target="registry.example.com/dags:v1",
                outdir=str(Path(tmpdir)),
                overwrite=True,
            )

    def test_refresh_noop_when_disabled(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            bundle = self._make_bundle(Path(tmpdir))
            hook = MagicMock()
            with patch.object(
                OrasDagBundle, "oras_hook", new_callable=PropertyMock, return_value=hook
            ):
                bundle.refresh()
            hook.pull.assert_not_called()

    def test_refresh_prefixes_hostname(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            bundle = self._make_bundle(Path(tmpdir), image="dags", disable_refresh=False)
            hook = MagicMock()
            hook.hostname = "registry.example.com"
            with (
                patch.object(OrasDagBundle, "lock", _noop_lock),
                patch.object(
                    OrasDagBundle,
                    "oras_hook",
                    new_callable=PropertyMock,
                    return_value=hook,
                ),
            ):
                bundle.refresh()
            hook.pull.assert_called_once_with(
                target="registry.example.com/dags:latest",
                outdir=str(Path(tmpdir)),
                overwrite=True,
            )

    def test_initialize_forces_refresh(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            bundle = self._make_bundle(Path(tmpdir), disable_refresh=True)
            with (
                patch.object(OrasDagBundle, "lock", _noop_lock),
                patch.object(bundle, "_refresh") as refresh_mock,
            ):
                bundle.initialize()
            refresh_mock.assert_called_once_with(force=True)

    def test_refresh_version_not_supported(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            bundle = self._make_bundle(Path(tmpdir), version="v1")
            with self.assertRaises(AirflowException):
                bundle.refresh()

    def test_view_url_template_prefers_configured_value(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            bundle = self._make_bundle(
                Path(tmpdir),
                view_url_template="https://example.test/{version}",
            )
            self.assertEqual(
                bundle.view_url_template(), "https://example.test/{version}"
            )

    def test_view_url_template_defaults(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            bundle = self._make_bundle(Path(tmpdir), image="registry.example.com/dags")
            self.assertEqual(
                bundle.view_url_template(), "https://registry.example.com/dags"
            )

    def test_view_url_delegates(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            bundle = self._make_bundle(Path(tmpdir))
            with patch.object(
                bundle, "view_url_template", return_value="url"
            ) as view_mock:
                self.assertEqual(bundle.view_url(), "url")
            view_mock.assert_called_once_with()
