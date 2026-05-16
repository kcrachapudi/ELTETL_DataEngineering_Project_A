from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator

default_args = {
    'owner': 'pipeline',
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'email_on_failure': False,
}

with DAG(
    dag_id='healthcare_pipeline',
    default_args=default_args,
    description='End-to-end healthcare data pipeline',
    schedule_interval='0 6 * * *',
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['healthcare', 'edi', 'pipeline'],
) as dag:

    def extract_and_load():
        import sys
        import os
        sys.path.insert(0, '/opt/airflow/project')
        os.chdir('/opt/airflow/project')
        from tests.integration_test_parsers import (
            test_edi_834, test_edi_837, test_edi_835,
            test_edi_270, test_edi_271,
            test_csv_weather_stations, test_csv_member_eligibility,
        )
        from tests.test_load_db import load_to_db

        sources = [
            (test_edi_834,                "edi_834",            ["member_id"],                    "edi_834"),
            (test_edi_837,                "edi_837",            ["claim_id", "procedure_code"],   "edi_837"),
            (test_edi_835,                "edi_835",            ["claim_id", "procedure_code"],   "edi_835"),
            (test_edi_270,                "edi_270",            ["subscriber_id"],                "edi_270"),
            (test_edi_271,                "edi_271",            ["subscriber_id", "benefit_seq"], "edi_271"),
            (test_csv_weather_stations,   "weather_stations",   ["station_id"],                  "csv"),
            (test_csv_member_eligibility, "member_eligibility", ["member_id"],                   "csv"),
        ]

        for fn, table, pkeys, fmt in sources:
            df = fn()
            load_to_db(df, table=table, primary_keys=pkeys,
                      mode="upsert", source_format=fmt,
                      source_file=f"sample_data/{fmt}/{table}")
            print(f"Loaded {len(df)} rows into raw.{table}")

    extract_load = PythonOperator(
        task_id='extract_and_load',
        python_callable=extract_and_load,
    )

    dbt_run = BashOperator(
        task_id='dbt_run',
        bash_command='cd /opt/airflow/project/dbt_king && dbt run --profiles-dir /opt/airflow/project/dbt_king',
    )

    dbt_test = BashOperator(
        task_id='dbt_test',
        bash_command='cd /opt/airflow/project/dbt_king && dbt test --profiles-dir /opt/airflow/project/dbt_king',
    )

    extract_load >> dbt_run >> dbt_test