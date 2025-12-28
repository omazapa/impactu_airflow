import os
import sys

import pytest
from airflow.models import DagBag


def test_dag_bag_integrity():
    """
    Test that all DAGs in the dags/ folder are valid and can be loaded by Airflow.
    """
    # Set PYTHONPATH to include the root so imports work during test
    root_path = os.getcwd()
    os.environ["PYTHONPATH"] = root_path
    if root_path not in sys.path:
        sys.path.insert(0, root_path)

    dag_bag = DagBag(dag_folder="dags/", include_examples=False)

    # Check for import errors
    # We format the error message to be more readable in CI
    if len(dag_bag.import_errors) > 0:
        errors = "\n".join([f"{file}: {error}" for file, error in dag_bag.import_errors.items()])
        pytest.fail(f"DAG import errors detected:\n{errors}")

    # Check that we have at least one DAG
    assert len(dag_bag.dags) > 0, "No DAGs found in the dags/ folder"
