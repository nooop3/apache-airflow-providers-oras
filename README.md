# Apache Airflow Provider for ORAS

Release: `0.1.0`

[ORAS](https://oras.land/) - OCI Registry As Storage.

## Provider package

This is a provider package for `oras` provider. All classes for this provider package are in `airflow.providers.oras` python package.

## Installation

You can install this package on top of an existing Airflow installation (see `Requirements` below for the minimum Airflow version supported) via:

```bash
pip install apache-airflow-providers-oras
```

Optional dependencies (for ORAS features that need Docker or ECR support):

```bash
pip install apache-airflow-providers-oras[docker]
pip install apache-airflow-providers-oras[ecr]
pip install apache-airflow-providers-oras[all]
```

## Requirements

| Package | Version required |
|---------|------------------|
| `apache-airflow` | `>=3.0.0` |
| `oras` (Python SDK) | `>=0.2.8` |

## Configuration

### Connections

The ORAS hook uses the `oras` connection type. Configure the registry host and optional
credentials in Airflow Connections.

Connection fields:

- Host: Registry host (for example, `registry.example.com`).
- Login / Password: Optional basic auth credentials.
- Schema: Optional `https` or `http` to set `insecure`.

Extras:

- `hostname`: Override registry host.
- `insecure`: Boolean to use HTTP instead of HTTPS.
- `tls_verify`: Boolean or CA bundle path.
- `auth_backend`: ORAS auth backend name (default: `token`).
- `config_path`: Optional ORAS config path to load credentials from.

### DAG Bundles

To use the ORAS bundle backend, configure `dag_bundle_config_list` in `airflow.cfg` (JSON list) or via environment variables.

`airflow.cfg` JSON example:

```ini
[dag_processor]
dag_bundle_config_list = [
    {
      "name": "oci-dag-bundles",
      "classpath": "airflow.providers.oras.bundles.oras.OrasDagBundle",
      "kwargs": {
        "image": "nooop3/apache-airflow-providers-oras/oci-dag-bundles",
        "tag": "v0.0.2",
        "subdir": "dag-bundles/oci-examples"
      }
    }
  ]
```

### Bundle Backend Parameters

The bundle accepts the following parameters:

- `image` (required): OCI image reference to pull. If it omits the registry hostname, the connection hostname is prefixed.
- `tag`: Tag or digest to pull (default: `latest`).
- `subdir`: Optional subdirectory inside the artifact that contains DAGs.
- `oras_conn_id`: Airflow connection ID for ORAS (default: `oras_default`).
- `disable_refresh`: Skip periodic refresh calls after initialize (default: `true`).

## Local development

```bash
# Create virtual environment
export UV_NO_SOURCES=1
uv venv --extra all
uv sync --no-sources

# Run tests
uv run python -m unittest discover -s tests
```
