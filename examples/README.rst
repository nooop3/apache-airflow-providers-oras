Examples
========

OCI DAG bundle image
--------------------

When you push a git tag prefixed with ``oci-`` (for example, ``oci-0.1.0``), the
GitHub Actions workflow publishes an OCI artifact to GHCR built from
``examples/dags/oci``. Update the example DAGs in that folder before pushing a
new ``oci-`` tag.
