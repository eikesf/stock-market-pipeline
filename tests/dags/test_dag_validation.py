from airflow.models import DagBag


def test_dagbag_imports():
    """Verify that there are no import errors in any of the DAG files."""
    dagbag = DagBag(dag_folder="airflow/dags", include_examples=False)

    # Assert that there are no import errors
    assert len(dagbag.import_errors) == 0, f"DAG import errors found: {dagbag.import_errors}"


def test_dagbag_cycles():
    """Verify that all DAGs in the bag are free from cycles."""
    dagbag = DagBag(dag_folder="airflow/dags", include_examples=False)

    # Check each DAG for cycles
    for dag in dagbag.dags.values():
        dag.check_cycle()


def test_dag_standards():
    """Verify that all DAGs follow defined project standards."""
    dagbag = DagBag(dag_folder="airflow/dags", include_examples=False)

    for dag_id, dag in dagbag.dags.items():
        owner = dag.default_args.get("owner", "airflow")
        assert owner != "airflow", f"DAG {dag_id} needs to have a custom owner."
        assert dag.tags, f"DAG {dag_id} needs to have at least one tag."
        assert dag.catchup is False, f"DAG {dag_id} needs to have catchup set to False."
        assert dag.schedule is not None, f"DAG {dag_id} needs to have a schedule set."
