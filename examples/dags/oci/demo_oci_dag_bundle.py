from __future__ import annotations

from pathlib import Path
from pprint import pformat

import pendulum
from airflow.models.dag import DAG
from airflow.models.dagrun import DagRun
from airflow.models.taskinstance import TaskInstance
from airflow.sdk import dag, task


@dag(
    schedule=None,
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
    catchup=False,
    tags=["example", "dag-bundles", "oci"],
)
def demo_oci_dag_bundle():
    @task
    def show_bundle_context(
        dag: DAG | None = None,
        task_instance: TaskInstance | None = None,
        dag_run: DagRun | None = None,
    ) -> None:
        if dag is None or task_instance is None or dag_run is None:
            raise ValueError("Missing Airflow context objects for this task.")

        print("Dag object:")
        print(pformat(dag))

        print("TaskInstance object:")
        print(pformat(task_instance))

        print("DagRun object:")
        print(pformat(dag_run))

        dag_file = Path(__file__).resolve()
        print(f"DAG file path: {dag_file}")
        print(f"DAG folder: {dag_file.parent}")

    show_bundle_context()


demo_oci_dag_bundle()
