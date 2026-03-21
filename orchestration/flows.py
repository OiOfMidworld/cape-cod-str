import logging
import subprocess
from pathlib import Path
from prefect import flow, task
from prefect.tasks import task_input_hash
from datetime import timedelta

logger = logging.getLogger(__name__)

DBT_PROJECT_DIR = Path(__file__).parent.parent / 'cape_cod_str_dbt'


@task(retries=2, retry_delay_seconds=60, cache_key_fn=task_input_hash, cache_expiration=timedelta(hours=23))
def run_census():
    from ingestion.census_api import run
    logger.info("Running Census ACS ingestion...")
    run()


@task(retries=3, retry_delay_seconds=120)
def run_str_registry():
    from ingestion.dor_str_registry import run
    logger.info("Running DOR STR Registry scrape...")
    run()


@task(retries=1, retry_delay_seconds=60)
def run_massgis():
    from ingestion.massgis_parcels import run as parcels_run
    from pathlib import Path
    logger.info("Running MassGIS parcel ingestion...")
    data_dir = Path('data/raw/massgis')
    for town_dir in sorted(data_dir.iterdir()):
        if town_dir.is_dir():
            parcels_run(town_dir)


@task(retries=1, retry_delay_seconds=30)
def run_address_matcher():
    from transform.address_matcher import run
    logger.info("Running address matcher...")
    run()


@task
def run_dbt():
    logger.info("Running dbt models...")
    result = subprocess.run(
        ['dbt', 'run'],
        cwd=DBT_PROJECT_DIR,
        capture_output=True,
        text=True
    )
    logger.info(result.stdout)
    if result.returncode != 0:
        raise Exception(f"dbt run failed:\n{result.stderr}")


@flow(name="monthly_str_pipeline")
def monthly_pipeline():
    """Runs monthly - scrapes STR registry and refreshes mart models."""
    run_str_registry()
    run_address_matcher()
    run_dbt()


@flow(name="annual_pipeline")
def annual_pipeline():
    """Runs annually - refreshes Census and parcel data then full pipeline."""
    run_census()
    run_massgis()
    run_str_registry()
    run_address_matcher()
    run_dbt()


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO, format='%(levelname)s - %(message)s')
    monthly_pipeline()