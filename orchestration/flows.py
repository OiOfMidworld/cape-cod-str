import subprocess
import sys
from pathlib import Path

# Add repo root to path
repo_root = Path(__file__).parent.parent
sys.path.insert(0, str(repo_root))

from prefect import flow, task

DBT_PROJECT_DIR = repo_root / 'cape_cod_str_dbt'

@task
def run_str_registry():
    from ingestion.dor_str_registry import run
    return run()

@task
def run_massgis():
    from ingestion.massgis_parcels import run
    return run()

@task
def run_census():
    from ingestion.census_api import run
    return run()

@task
def run_dbt():
    result = subprocess.run(
        ['dbt', 'run'],
        cwd=DBT_PROJECT_DIR,
        capture_output=True,
        text=True
    )
    print(result.stdout)
    if result.returncode != 0:
        raise Exception(f"dbt run failed:\n{result.stderr}")

@flow
def cape_cod_str_pipeline():
    """Run the full STR pipeline"""
    run_str_registry()
    run_massgis()
    run_census()
    run_dbt()
    print("Pipeline complete!")

if __name__ == "__main__":
    cape_cod_str_pipeline()