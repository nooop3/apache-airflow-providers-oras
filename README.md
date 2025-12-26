# Airflow Provider for ORAS

This provider adds an Airflow DAG bundle backend that pulls DAGs from an OCI registry
using the ORAS CLI.

## Requirements

- Apache Airflow 3.x
- ORAS CLI available on the PATH of your scheduler/triggerer

## Install

```bash
pip install apache-airflow-providers-oras
```

## Local development with uv

```bash
uv venv
uv pip install -r requirements-dev.txt
```

Run tests:

```bash
uv run python -m unittest discover -s tests
```

## Configuration

This provider registers `airflow.providers.oras.bundles.oras.OrasDagBundle`.
The DAG bundle config is a JSON list; each entry defines a bundle name and backend
configuration.

`airflow.cfg` example:

```ini
[dag_bundles]
bundle_config = [
  {
    "name": "oras_dags",
    "bundle_backend": "airflow.providers.oras.bundles.oras.OrasDagBundle",
    "bundle_backend_kwargs": {
      "image": "ghcr.io/acme/airflow-dags:latest",
      "pull_args": ["--plain-http"],
      "bundle_root": "/opt/airflow/dag_bundles",
      "max_retries": 2,
      "retry_delay": 5
    }
  }
]
```

Helm values example:

```yaml
env:
  - name: AIRFLOW__DAG_BUNDLES__BUNDLE_CONFIG
    value: >
      [{"name":"oras_dags","bundle_backend":"airflow.providers.oras.bundles.oras.OrasDagBundle",
      "bundle_backend_kwargs":{"image":"ghcr.io/acme/airflow-dags:latest","pull_args":["--plain-http"]}}]
```

### Bundle backend options

- `image` (required): OCI reference or digest to pull.
- `oras_cmd`: ORAS binary to execute (default: `oras`).
- `pull_args`: List of extra ORAS `pull` flags.
- `bundle_root`: Local root directory for bundle materialization.
- `env`: Mapping of environment variables to pass to ORAS.
- `max_retries`: Retry count on pull failures (default: `0`).
- `retry_delay`: Seconds between retries (default: `5`).

## Security Notes

- Credentials are supplied via ORAS-compatible auth (env vars, helpers, or IRSA).
- ORAS output is captured; only the first line of errors is logged to avoid leaking secrets.
