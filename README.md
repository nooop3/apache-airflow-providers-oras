# Apache Airflow Provider for ORAS

Release: `0.1.0`

[ORAS](https://oras.land/) - OCI Registry As Storage.

## Provider package

This is a provider package for `oras` provider. All classes for this provider package
are in `airflow.providers.oras` python package.

## Installation

You can install this package on top of an existing Airflow installation (see `Requirements` below
for the minimum Airflow version supported) via:

```bash
pip install apache-airflow-providers-oras
```

## Requirements

| Package | Version required |
|---------|------------------|
| `apache-airflow` | `>=3.0.0` |
| `oras` (CLI) | Available on PATH |

## Configuration

This provider registers `airflow.providers.oras.bundles.oras.OrasDagBundle`.

### DAG Bundles

To use the ORAS bundle backend, configure the `dag_bundles` section in `airflow.cfg` or via environment variables.

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

### Bundle backend options

- `image` (required): OCI reference or digest to pull.
- `oras_cmd`: ORAS binary to execute (default: `oras`).
- `pull_args`: List of extra ORAS `pull` flags.
- `bundle_root`: Local root directory for bundle materialization.
- `env`: Mapping of environment variables to pass to ORAS.
- `max_retries`: Retry count on pull failures (default: `0`).
- `retry_delay`: Seconds between retries (default: `5`).

## Local development

```bash
# Create virtual environment
uv venv
uv sync --extra dev
uv pip install -e .

# Run tests
uv run python -m unittest discover -s tests
```
