from __future__ import annotations

from pprint import pformat

import pendulum
from airflow.models.taskinstance import TaskInstance
from airflow.sdk import dag, task

from airflow.providers.oras.hooks.oras import OrasHook


@dag(
    schedule=None,
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
    catchup=False,
    tags=["example", "dag-bundles", "oci", "oras"],
)
def demo_oci_oras_hook():
    @task
    def inspect_bundle_image(task_instance: TaskInstance | None = None) -> None:
        if task_instance is None:
            raise ValueError("Missing TaskInstance for bundle inspection.")

        bundle_instance = getattr(task_instance, "bundle_instance", None)
        if bundle_instance is None:
            raise ValueError("Bundle instance not found on TaskInstance.")

        print(f"Bundle instance: {bundle_instance}")

        image = getattr(bundle_instance, "image", None)
        tag = getattr(bundle_instance, "tag", None)
        if not image:
            raise ValueError("Bundle instance is missing the image name.")

        target = f"{image}:{tag}" if tag else image
        hook = OrasHook()
        client = hook.get_client()

        print(f"Inspecting OCI image: {target}")
        try:
            if hasattr(client, "get_manifest"):
                manifest = client.get_manifest(target)
            else:
                raise ValueError("Oras client does not support manifest inspection.")
        except Exception as exc:
            print(f"Failed to inspect OCI image: {exc}")
            raise

        print("Manifest:")
        print(pformat(manifest))

    inspect_bundle_image()


demo_oci_oras_hook()
