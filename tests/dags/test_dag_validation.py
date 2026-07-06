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
        assert dag.start_date is not None, f"DAG {dag_id} needs to have a start_date set."


def test_all_contracts_integrated_in_dags():
    """Verify that every YAML contract in soda/contracts is referenced and tested in the DAG files."""
    from pathlib import Path

    # Find all contracts
    contracts_dir = Path("soda/contracts")
    contract_files = [p.name for p in contracts_dir.glob("*.yml")]
    assert len(contract_files) > 0, "No contract files found in soda/contracts"

    # Read all DAG file contents
    dags_dir = Path("airflow/dags")
    dag_contents = ""
    for df in dags_dir.glob("*.py"):
        with open(df, encoding="utf-8") as f:
            dag_contents += f.read()

    # Assert each contract is referenced in at least one DAG
    for contract in contract_files:
        assert contract in dag_contents, (
            f"Contract file '{contract}' is defined in soda/contracts but not referenced/tested in any DAG!"
        )
