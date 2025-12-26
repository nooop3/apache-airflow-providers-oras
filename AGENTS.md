# AGENTS.md

## Agent Guide for `apache-airflow-providers-oras`

This document defines how human contributors and AI agents should collaborate on this repository.
It sets expectations for scope, code quality, and project standards.

---

## Project Overview

**Repository:** `apache-airflow-providers-oras`
**Purpose:** Provide an Apache Airflow provider that integrates ORAS / OCI registries with Airflow.

Initial scope includes:

* An ORAS-based DAG Bundle backend (`airflow.dag_bundles` entrypoint)
* OCI authentication helpers (ECR, GHCR, Harbor, etc.)
* Extensible structure for future hooks or operators if needed

This is not an official Apache project, but follows Apache Airflow provider conventions.

---

## Goals

* Seamless integration with Airflow 3.x DAG Bundles
* Production-grade OCI pulls using `oras` or compatible tooling
* Clear logging, retries, and failure modes
* Minimal dependencies and fast startup
* Easy packaging as a pip-installable provider

---

## Responsibilities of Agents

Agents (including AI assistants) working on this repo should:

* Follow Airflow provider patterns and conventions
* Keep implementations small, explicit, and testable
* Prefer standard libraries and simple subprocess wrappers
* Avoid introducing unnecessary frameworks or abstractions
* Not broaden scope without discussion (for example, unrelated operators)

When in doubt:
Optimize for clarity, maintainability, and Airflow compatibility.

---

## Code Structure Expectations

Expected layout:

```bash
src/airflow/providers/oras/
├── __init__.py
├── __version__.py
├── get_provider_info.py
└── bundles/
    └── oras.py
```

Key principles:

* One backend per module
* Explicit class names (for example, `OrasDagBundle` for `airflow.providers.oras.bundles.oras.OrasDagBundle`)
* No side effects at import time
* All Airflow integration via entrypoints

---

## Airflow Integration

Must be registered via:

```toml
[project.entry-points."apache_airflow_provider"]
provider_info = "airflow.providers.oras.get_provider_info:get_provider_info"
```

Implementations must:

* Subclass the appropriate Airflow base class
* Implement `refresh()` to materialize DAGs locally
* Return a filesystem path containing DAG files

---

## Testing

Agents should:

* Add unit tests for core logic such as parsing config, path handling, and command building
* Mock subprocess and ORAS calls
* Avoid requiring real OCI registries in tests

Tests should live under:

```bash
tests/
```

---

## Code Style

* Python 3.9 or newer (aligned with Airflow 3.x)
* Follow PEP8
* Use type hints where helpful
* Keep functions small and readable
* Use Airflow’s logging (`self.log`)

If adding tooling:

* Prefer ruff or flake8 for linting
* Prefer black for formatting (optional but consistent)

---

## Packaging and Versioning

* Use Semantic Versioning: MAJOR.MINOR.PATCH
* Versions live in `__version__.py`
* Provider package name: `apache-airflow-providers-oras`
* Python module: `airflow_provider_oras`

Agents should not:

* Hardcode versions in multiple places
* Break backward compatibility without bumping MAJOR

---

## Security and Supply Chain

Given OCI and registry usage, agents should:

* Avoid logging credentials or tokens
* Support external authentication (environment variables, helpers, IRSA)
* Keep subprocess usage explicit and auditable
* Make it easy to integrate with:

  * cosign verification
  * immutable digests

Never introduce:

* Inline secrets
* Shell injection risks
* Silent failures

---

## Documentation

All new features must include:

* README updates
* Configuration examples
* Airflow `airflow.cfg` or Helm snippets
* Clear usage notes

Docstrings are required for public classes and methods.

---

## CI and Automation

Agents may help define:

* Wheel build pipelines
* Lint and test jobs
* Release tagging

But should:

* Keep CI minimal
* Avoid vendor lock-in where possible

---

## Out of Scope (for now)

Unless explicitly requested, agents should avoid adding:

* Kubernetes operators
* Generic OCI hooks unrelated to DAG bundles
* Task execution features
* Non-ORAS registry abstractions

Focus: DAG Bundles via ORAS.

---

## Collaboration

When unsure:

* Ask for clarification
* Propose changes before large refactors
* Prefer incremental improvements

The goal is to make this provider boring, predictable, and rock-solid.

---

## Summary for Agents

If you are contributing here:

* You are building an Airflow provider
* For OCI DAG Bundles
* With a production-first mindset
* Keeping things simple, explicit, and testable

Thank you for helping improve this project.
